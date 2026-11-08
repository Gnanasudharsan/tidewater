/*
 * "Try to break it": an in-browser simulation of the pipeline's delivery semantics.
 *
 * Source table -> Kafka log (at-least-once: events can be redelivered and reordered)
 * -> two sinks fed the same deliveries:
 *   naive     applies each delivery in arrival order: c appends, u overwrites, d removes
 *   tidewater applies each micro-batch with TidewaterMerge.applyBatch (js/merge.js), the
 *             port of the production MERGE rule that CI checks against PySpark + Delta
 *
 * Every random choice comes from one seeded RNG consumed in action order, so the action
 * log in the URL hash replays the exact same run.
 */
(function () {
  "use strict";

  const Merge = window.TidewaterMerge;
  const BATCH = 8; // deliveries per micro-batch
  const MAX_ROWS = 12; // auto traffic stops inserting past this many source rows
  const LSN_BASE = 0x16b3a28;
  const STATUSES = ["placed", "paid", "shipped", "delivered"];
  const INTRO = ["i", "i", "i", "i", "b"];

  // ---------- seeded RNG (mulberry32) ----------
  function rngFrom(seed) {
    let a = seed >>> 0;
    return function () {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  // ---------- state ----------
  let S;

  function fresh(seed) {
    return {
      seed: seed,
      rng: rngFrom(seed),
      actions: [],
      nextId: 1001,
      lsn: LSN_BASE,
      source: new Map(), // id -> {status, amount}
      log: [], // produced events; index = Kafka offset
      pending: [], // offsets awaiting delivery, in arrival order
      naive: [], // rows {pk, status, amount}, duplicates possible
      silver: {}, // pk -> row from merge.js
      trace: {}, // pk -> [{batch, offset, delivery, outcome}]
      batchNo: 0,
    };
  }

  const pick = (arr) => arr[Math.floor(S.rng() * arr.length)];
  const randInt = (lo, hi) => lo + Math.floor(S.rng() * (hi - lo + 1));
  const money = () => Math.round((10 + S.rng() * 490) * 100) / 100;

  // ---------- source changes (each produces one CDC event) ----------
  function produce(op, pk, row) {
    S.lsn += randInt(8, 96);
    const e = { pk: pk, op: op, lsn: S.lsn, offset: S.log.length, status: row.status, amount: row.amount, deliveries: 0, processed: 0 };
    S.log.push(e);
    enqueue(e.offset);
    return e;
  }

  function enqueue(offset) {
    S.pending.push(offset);
    S.log[offset].deliveries += 1;
  }

  function insert() {
    const id = S.nextId++;
    const row = { status: "placed", amount: money() };
    S.source.set(id, row);
    produce("c", id, row);
    return "Inserted order " + id + ".";
  }

  function update(id) {
    const cur = S.source.get(id);
    if (!cur) return "Order " + id + " no longer exists.";
    const i = STATUSES.indexOf(cur.status);
    const row =
      i < STATUSES.length - 1 && S.rng() < 0.7
        ? { status: STATUSES[i + 1], amount: cur.amount }
        : { status: cur.status, amount: money() };
    S.source.set(id, row);
    produce("u", id, row);
    return "Updated order " + id + " to " + row.status + ", $" + row.amount.toFixed(2) + ".";
  }

  function remove(id) {
    const cur = S.source.get(id);
    if (!cur) return "Order " + id + " no longer exists.";
    S.source.delete(id);
    produce("d", id, cur); // REPLICA IDENTITY FULL: the delete carries the before image
    return "Deleted order " + id + ".";
  }

  function randomChange() {
    const ids = Array.from(S.source.keys());
    const r = S.rng();
    if (!ids.length || (r < 0.4 && ids.length < MAX_ROWS)) return insert();
    if (r < 0.85 || ids.length < 3) return update(pick(ids));
    return remove(pick(ids));
  }

  // ---------- delivery ----------
  function applyNaive(e) {
    const rows = S.naive.filter((r) => r.pk === e.pk);
    if (e.op === "c") S.naive.push({ pk: e.pk, status: e.status, amount: e.amount });
    else if (e.op === "d") S.naive = S.naive.filter((r) => r.pk !== e.pk);
    else if (rows.length) rows.forEach((r) => Object.assign(r, { status: e.status, amount: e.amount }));
    else S.naive.push({ pk: e.pk, status: e.status, amount: e.amount }); // update of a missing row: upsert
  }

  function applyTidewater(offsets) {
    const events = offsets.map((o) => S.log[o]);
    const res = Merge.applyBatch(S.silver, events);
    S.silver = res.silver;
    S.batchNo += 1;
    const counts = {};
    events.forEach((e, i) => {
      e.processed += 1;
      const out = res.outcomes[i];
      counts[out] = (counts[out] || 0) + 1;
      (S.trace[e.pk] = S.trace[e.pk] || []).push({ batch: S.batchNo, offset: e.offset, delivery: e.processed, outcome: out });
    });
    return counts;
  }

  function describe(counts) {
    const parts = [];
    if (counts.applied) parts.push(counts.applied + " applied");
    if (counts.superseded) parts.push(counts.superseded + " superseded in batch");
    if (counts.duplicate) parts.push(counts.duplicate + " duplicate skipped");
    if (counts.stale) parts.push(counts.stale + " stale skipped");
    return parts.join(", ") || "nothing to do";
  }

  function batch() {
    if (!S.pending.length) return "Micro-batch: no events in flight.";
    const offsets = S.pending.splice(0, BATCH);
    offsets.forEach((o) => applyNaive(S.log[o]));
    return "Micro-batch " + (S.batchNo + 1) + " (" + offsets.length + " events): " + describe(applyTidewater(offsets)) + ".";
  }

  function kill() {
    if (S.pending.length < 2) return "Kill Spark: needs at least 2 events in flight. Pause the stream and make some changes first.";
    const offsets = S.pending.splice(0, BATCH);
    const k = randInt(1, offsets.length - 1);
    // Spark dies after committing part of the batch; the checkpoint was not advanced.
    offsets.slice(0, k).forEach((o) => applyNaive(S.log[o]));
    applyTidewater(offsets.slice(0, k));
    // On restart the whole batch is read again from the last checkpoint.
    offsets.slice(0, k).forEach((o) => (S.log[o].deliveries += 1));
    offsets.forEach((o) => applyNaive(S.log[o]));
    const counts = applyTidewater(offsets);
    return "Spark killed after committing " + k + " of " + offsets.length + " events, restarted, and replayed the batch from the checkpoint: " + describe(counts) + ".";
  }

  function duplicate(n) {
    const last = S.log.slice(-n);
    if (!last.length) return "Nothing to duplicate yet.";
    last.forEach((e) => enqueue(e.offset));
    return "Redelivered the last " + last.length + " events (at-least-once delivery).";
  }

  function shuffle() {
    for (let i = S.pending.length - 1; i > 0; i--) {
      const j = Math.floor(S.rng() * (i + 1));
      [S.pending[i], S.pending[j]] = [S.pending[j], S.pending[i]];
    }
    return S.pending.length > 1 ? "Shuffled " + S.pending.length + " in-flight events." : "Shuffle: fewer than 2 events in flight. Pause the stream first.";
  }

  function replay() {
    S.log.forEach((e) => enqueue(e.offset));
    return "Re-queued all " + S.log.length + " events from bronze, oldest first.";
  }

  // ---------- actions (the share log) ----------
  function run(code) {
    S.actions.push(code);
    const arg = parseInt(code.slice(1), 10);
    switch (code[0]) {
      case "i": return insert();
      case "u": return update(arg);
      case "d": return remove(arg);
      case "x": return randomChange();
      case "b": return batch();
      case "k": return kill();
      case "n": return duplicate(arg || 3);
      case "s": return shuffle();
      case "r": return replay();
      default: S.actions.pop(); return "";
    }
  }

  // ---------- reconciliation (same idea as spark/jobs/reconcile.py) ----------
  const rowHash = (r) => r.status + "|" + r.amount.toFixed(2);

  function reconcile(rows) {
    const byPk = new Map();
    rows.forEach((r) => byPk.set(r.pk, (byPk.get(r.pk) || []).concat([r])));
    const keys = new Set(Array.from(S.source.keys()).concat(Array.from(byPk.keys())));
    let match = 0;
    keys.forEach((k) => {
      const src = S.source.get(k);
      const got = byPk.get(k) || [];
      if (src && got.length === 1 && rowHash(got[0]) === rowHash(src)) match += 1;
    });
    return {
      rows: rows.length,
      source: S.source.size,
      dupes: rows.length - byPk.size,
      pct: keys.size ? (100 * match) / keys.size : 100,
    };
  }

  function rowState(r, count) {
    const src = S.source.get(r.pk);
    if (count > 1) return ["dup", "duplicate"];
    if (!src) return ["bad", "not in source"];
    if (rowHash(src) !== rowHash(r)) return ["bad", "wrong values"];
    return ["", ""];
  }

  // ---------- rendering ----------
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  const fmtLsn = (n) => "0/" + n.toString(16).toUpperCase();
  const fmtAmt = (a) => "$" + a.toFixed(2);

  function renderSource() {
    const rows = Array.from(S.source.entries());
    $("src-count").textContent = rows.length + " rows";
    $("src-body").innerHTML = rows.length
      ? rows
          .map(
            ([id, r]) =>
              `<tr><td>${id}</td><td>${r.status}</td><td class="num">${fmtAmt(r.amount)}</td>` +
              `<td class="acts"><button type="button" class="mini" data-act="u${id}" aria-label="Update order ${id}">Update</button>` +
              `<button type="button" class="mini danger" data-act="d${id}" aria-label="Delete order ${id}">Delete</button></td></tr>`
          )
          .join("")
      : `<tr><td colspan="4" class="empty">No rows. Insert one.</td></tr>`;
  }

  function renderKafka() {
    const inFlight = new Set(S.pending);
    $("kafka-count").textContent = S.log.length + " events · " + S.pending.length + " in flight";
    $("pending").innerHTML = S.pending.length
      ? S.pending.slice(0, 24).map((o) => `<li class="chip op-${S.log[o].op}">#${o}</li>`).join("") + (S.pending.length > 24 ? `<li class="chip more">+${S.pending.length - 24}</li>` : "")
      : `<li class="muted">none: sinks are caught up</li>`;
    $("kafka-body").innerHTML = S.log
      .slice(-40)
      .reverse()
      .map(
        (e) =>
          `<li class="${inFlight.has(e.offset) ? "flight" : ""}"><span class="off">#${e.offset}</span>` +
          `<span class="op op-${e.op}" title="${{ c: "create", u: "update", d: "delete" }[e.op]}">${e.op}</span>` +
          `<span>pk ${e.pk}</span><span class="lsn">${fmtLsn(e.lsn)}</span>` +
          `<span class="payload">${e.status} · ${fmtAmt(e.amount)}</span>` +
          (e.deliveries > 1 ? `<span class="badge warn" title="delivered ${e.deliveries} times">×${e.deliveries}</span>` : `<span class="badge">×1</span>`) +
          `</li>`
      )
      .join("") || `<li class="muted">No events yet.</li>`;
  }

  function renderRecon(prefix, rec) {
    const ok = rec.pct === 100 && rec.dupes === 0 && rec.rows === rec.source;
    const el = $(prefix + "-recon");
    el.className = "recon " + (ok ? "ok" : "bad");
    el.innerHTML =
      `<div class="recon-bar" role="meter" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${rec.pct.toFixed(1)}" aria-label="Row hash match">` +
      `<span style="width:${rec.pct}%"></span></div>` +
      `<dl><div><dt>Rows</dt><dd>${rec.rows} / ${rec.source}</dd></div>` +
      `<div><dt>Hash match</dt><dd>${rec.pct.toFixed(1)}%</dd></div>` +
      `<div><dt>Duplicates</dt><dd>${rec.dupes}</dd></div></dl>`;
  }

  function renderNaive() {
    const counts = new Map();
    S.naive.forEach((r) => counts.set(r.pk, (counts.get(r.pk) || 0) + 1));
    renderRecon("naive", reconcile(S.naive));
    $("naive-body").innerHTML =
      S.naive
        .map((r) => {
          const [cls, why] = rowState(r, counts.get(r.pk));
          return `<tr class="${cls}"><td>${r.pk}</td><td>${r.status}</td><td class="num">${fmtAmt(r.amount)}</td><td>${why ? `<span class="tag ${cls}">${why}</span>` : ""}</td></tr>`;
        })
        .join("") || `<tr><td colspan="4" class="empty">Empty.</td></tr>`;
  }

  function renderSilver() {
    const rows = Object.values(S.silver).sort((a, b) => a.pk - b.pk);
    const live = rows.filter((r) => !r.is_deleted);
    renderRecon("tw", reconcile(live));
    $("tw-body").innerHTML =
      rows
        .map((r) => {
          const [cls, why] = r.is_deleted ? (S.source.has(r.pk) ? ["bad", "behind"] : ["del", "soft-deleted"]) : rowState(r, 1);
          const label = why === "not in source" || why === "wrong values" ? "behind" : why;
          return (
            `<tr class="${cls}"><td><button type="button" class="linkish" data-trace="${r.pk}" aria-label="Show event trace for order ${r.pk}">${r.pk}</button></td>` +
            `<td>${r.status}</td><td class="num">${fmtAmt(r.amount)}</td><td class="lsn">${fmtLsn(r._lsn)}</td>` +
            `<td>${label ? `<span class="tag ${cls}">${label}</span>` : ""}</td></tr>`
          );
        })
        .join("") || `<tr><td colspan="5" class="empty">Empty.</td></tr>`;
  }

  function render(msg, quiet) {
    // Tables are re-rendered on every action (timers included): keep keyboard focus.
    const f = document.activeElement;
    const key = f && f.dataset && (f.dataset.act ? "act" : f.dataset.trace ? "trace" : null);
    const val = key && f.dataset[key];
    renderSource();
    renderKafka();
    renderNaive();
    renderSilver();
    if (key && !document.body.contains(f)) {
      const again = $("sim").querySelector(`[data-${key}="${val}"]`);
      if (again) again.focus();
    }
    if (msg) {
      // Timer-driven updates stay visible but are not announced every second.
      $("sim-status").setAttribute("aria-live", quiet ? "off" : "polite");
      $("sim-status").textContent = msg;
    }
    $("lag-note").hidden = S.pending.length === 0;
  }

  const shareUrl = () => location.href.split("#")[0] + "#seed=" + S.seed + "&a=" + S.actions.join(".");

  // ---------- trace drawer ----------
  const OUTCOME = {
    applied: ["Applied", "newer than what silver held"],
    superseded: ["Superseded", "a newer change for this key was in the same batch"],
    duplicate: ["Skipped: duplicate", "the same record (same _lsn and offset) was already seen"],
    stale: ["Skipped: stale LSN", "older than the row in silver; recorded as a late arrival"],
  };

  function openTrace(pk) {
    const row = S.silver[pk];
    const items = S.trace[pk] || [];
    $("trace-title").textContent = "Order " + pk + ": every delivery that touched it";
    $("trace-final").innerHTML =
      `Final state: <strong>${row.status}</strong>, ${fmtAmt(row.amount)}${row.is_deleted ? ", <strong>soft-deleted</strong>" : ""}. ` +
      `<code>_lsn</code> = <code>${fmtLsn(row._lsn)}</code>, <code>_source_offset</code> = ${row._source_offset}.`;
    $("trace-list").innerHTML = items
      .map((t) => {
        const e = S.log[t.offset];
        const [name, why] = OUTCOME[t.outcome];
        return (
          `<li class="out-${t.outcome}"><div><span class="op op-${e.op}">${e.op}</span> offset #${e.offset} · LSN <code>${fmtLsn(e.lsn)}</code> · ${e.status}, ${fmtAmt(e.amount)}</div>` +
          `<div class="why"><strong>${name}</strong> (batch ${t.batch}, delivery ${t.delivery}): ${why}</div></li>`
        );
      })
      .join("");
    $("trace").showModal();
  }

  // ---------- controls ----------
  let autoTimer = null;
  let streamTimer = null;

  function act(code, quiet) {
    render(run(code), quiet);
  }

  function setAuto(on) {
    clearInterval(autoTimer);
    autoTimer = on ? setInterval(() => act("x", true), 700) : null;
    $("btn-auto").setAttribute("aria-pressed", String(on));
    $("btn-auto").textContent = on ? "Stop traffic" : "Auto traffic";
  }

  function setStream(on) {
    clearInterval(streamTimer);
    streamTimer = on ? setInterval(() => S.pending.length && act("b", true), 1800) : null;
    $("btn-stream").setAttribute("aria-pressed", String(!on));
    $("btn-stream").textContent = on ? "Pause stream" : "Resume stream";
    $("stream-state").textContent = on ? "Stream running: a micro-batch every 1.8 s" : "Stream paused: events pile up in flight";
  }

  function start(seed, actions) {
    S = fresh(seed);
    let msg = "";
    actions.forEach((a) => (msg = run(a)));
    render(msg || "Ready.");
  }

  function parseHash() {
    const p = new URLSearchParams(location.hash.slice(1));
    const seed = parseInt(p.get("seed"), 10);
    const a = (p.get("a") || "").split(".").filter((x) => /^[iudxbknsr]\d*$/.test(x));
    return Number.isFinite(seed) && a.length ? { seed: seed >>> 0, actions: a } : null;
  }

  function init() {
    if (!Merge || !$("sim")) return;
    const shared = parseHash();
    start(shared ? shared.seed : (Math.random() * 2 ** 32) >>> 0, shared ? shared.actions : INTRO);
    if (shared) {
      $("sim-status").textContent = "Replayed a shared scenario (" + shared.actions.length + " actions).";
      $("try").scrollIntoView();
    }

    $("sim").addEventListener("click", (ev) => {
      const b = ev.target.closest("button");
      if (!b) return;
      if (b.dataset.act) act(b.dataset.act);
      else if (b.dataset.trace) openTrace(b.dataset.trace);
    });
    $("btn-insert").onclick = () => act("i");
    $("btn-auto").onclick = () => setAuto(!autoTimer);
    $("btn-stream").onclick = () => setStream(!streamTimer);
    $("btn-batch").onclick = () => act("b");
    $("btn-dup").onclick = () => act("n" + $("dup-n").value);
    $("btn-shuffle").onclick = () => act("s");
    $("btn-kill").onclick = () => act("k");
    $("btn-replay").onclick = () => act("r");
    $("btn-reset").onclick = () => {
      setAuto(false);
      start(S.seed, INTRO);
    };
    $("btn-share").onclick = () => {
      const url = shareUrl();
      try {
        history.replaceState(null, "", url);
      } catch (_) {
        /* sandboxed: the copied link still works */
      }
      const done = () => ($("sim-status").textContent = "Link to this exact scenario copied.");
      const manual = () => ($("sim-status").textContent = "Copy the address bar to share this scenario.");
      if (navigator.clipboard) navigator.clipboard.writeText(url).then(done, manual);
      else manual();
    };
    $("trace-close").onclick = () => $("trace").close();
    $("trace").addEventListener("click", (ev) => ev.target === $("trace") && $("trace").close());
    setStream(true);
  }

  window.TidewaterSim = { rngFrom: rngFrom }; // exposed for debugging
  document.addEventListener("DOMContentLoaded", init);
})();
