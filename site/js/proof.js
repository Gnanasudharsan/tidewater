/*
 * Live proof: renders data/latest.json and data/history.json, written by the showcase
 * workflow (.github/workflows/showcase.yml) from a real end-to-end run. Missing values
 * are shown as "Not yet measured"; nothing here is a hard-coded number.
 */
(function () {
  "use strict";

  const REPO = "https://github.com/Gnanasudharsan/tidewater";
  const STALE_HOURS = 48;
  const NA = '<span class="na">Not yet measured</span>';
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  const num = (v) => (v === null || v === undefined ? null : Number(v));
  const fmtInt = (v) => (num(v) === null ? NA : num(v).toLocaleString("en-US"));
  const fmtSec = (ms) => (num(ms) === null ? null : (num(ms) / 1000).toFixed(1) + " s");

  async function getJSON(path) {
    try {
      const r = await fetch(path, { cache: "no-store" });
      return r.ok ? await r.json() : null;
    } catch (_) {
      return null;
    }
  }

  function ago(iso) {
    const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
    if (s < 90) return "just now";
    if (s < 5400) return Math.round(s / 60) + " min ago";
    if (s < 129600) return Math.round(s / 3600) + " h ago";
    return Math.round(s / 86400) + " days ago";
  }

  function badge(latest) {
    const el = $("run-badge");
    if (!el) return;
    if (!latest) {
      el.className = "run-badge";
      el.innerHTML = `No verified run published yet. <a href="${REPO}/actions/workflows/showcase.yml">Showcase workflow</a>`;
      return;
    }
    const hours = (Date.now() - Date.parse(latest.timestamp_utc)) / 3.6e6;
    const link = latest.run_url ? ` · <a href="${esc(latest.run_url)}">View the CI run</a>` : "";
    if (latest.status !== "passed") {
      el.className = "run-badge bad";
      el.innerHTML = `Last verified run: ${ago(latest.timestamp_utc)} · <span aria-hidden="true">❌</span> failed${link}`;
    } else if (hours > STALE_HOURS) {
      el.className = "run-badge stale";
      el.innerHTML = `Last verified run: ${ago(latest.timestamp_utc)} · <span aria-hidden="true">⚠️</span> passed, but stale (older than ${STALE_HOURS} h)${link}`;
    } else {
      el.className = "run-badge ok";
      el.innerHTML = `Last verified run: ${ago(latest.timestamp_utc)} · <span aria-hidden="true">✅</span> passed${link}`;
    }
  }

  function parityNote(latest) {
    const el = $("parity-note");
    if (!el) return;
    const test = `<a href="${REPO}/tree/main/tests/parity">tests/parity</a>`;
    if (latest && latest.parity_passed === true) {
      el.innerHTML = `Parity with the production merge: <span aria-hidden="true">✅</span> verified in <a href="${esc(latest.run_url)}">the latest CI run</a> by ${test} (same fixtures through <code>merge.js</code> and the PySpark merge on Delta Lake).`;
    } else if (latest && latest.parity_passed === false) {
      el.innerHTML = `Parity with the production merge: <span aria-hidden="true">❌</span> failed in <a href="${esc(latest.run_url)}">the latest CI run</a> (${test}).`;
    } else {
      el.innerHTML = `Parity with the production merge is checked in CI by ${test}: the same fixtures run through <code>merge.js</code> and the PySpark merge on Delta Lake.`;
    }
  }

  function card(label, value, sub) {
    return `<div class="card"><dt>${label}</dt><dd>${value}</dd>${sub ? `<dd class="sub">${sub}</dd>` : ""}</div>`;
  }

  function cards(latest) {
    const el = $("proof-cards");
    if (!el) return;
    if (!latest) {
      el.innerHTML = card("Latest CI run", NA, "The showcase workflow has not published a run yet; the cards appear once it does.");
      return;
    }
    const lat = latest.latency_ms;
    const chaos = latest.chaos || [];
    const okChaos = chaos.filter((c) => c.passed).length;
    const pct = num(latest.hash_match_pct);
    el.innerHTML = [
      card("Events processed", fmtInt(latest.events_processed), "business events from the simulator, end to end"),
      card("Rows reconciled", fmtInt(latest.rows_reconciled), "silver vs source, every table"),
      card("Duplicates in silver", fmtInt(latest.duplicates_in_silver), "expected: 0"),
      card("Row hash match", pct === null ? NA : pct.toFixed(pct === 100 ? 0 : 3) + "%", "SHA-256 of every column of every row"),
      card("Latency p50 / p95", lat ? `${fmtSec(lat.p50)} / ${fmtSec(lat.p95)}` : NA, lat ? `source commit → silver commit; median p50, worst p95 over ${lat.batches} micro-batches` : "source commit → silver commit"),
      card("Chaos tests", chaos.length ? `${okChaos} / ${chaos.length} passed` : NA, chaos.map((c) => `${c.passed ? "✅" : "❌"} ${esc(c.name.replace(/_/g, " "))}`).join("<br>")),
      card("Commit", latest.commit_sha ? `<a href="${REPO}/commit/${esc(latest.commit_sha)}"><code>${esc(latest.commit_sha.slice(0, 7))}</code></a>` : NA),
      card("Run time", `<time datetime="${esc(latest.timestamp_utc)}">${esc(latest.timestamp_utc.replace("T", " ").replace("Z", " UTC"))}</time>`, latest.run_url ? `<a href="${esc(latest.run_url)}">View this CI run</a>` : ""),
    ].join("");
  }

  // ---------- trend chart: p50/p95 latency line + pass/fail strip, last 30 runs ----------
  function chart(history) {
    const el = $("trend");
    if (!el) return;
    const runs = (history || []).slice(-30);
    if (runs.length < 2) {
      el.innerHTML = `<p class="muted">The trend appears after two published runs${runs.length ? " (one so far)" : ""}.</p>`;
      return;
    }
    const W = 600, H = 250, L = 48, R = 14, T = 16, B = 58;
    const pts = runs.map((r, i) => ({ i, r, p50: r.latency_ms ? r.latency_ms.p50 / 1000 : null, p95: r.latency_ms ? r.latency_ms.p95 / 1000 : null }));
    const maxY = Math.max(10, ...pts.map((p) => p.p95 || 0));
    const niceMax = Math.ceil(maxY / 10) * 10;
    const x = (i) => L + (runs.length === 1 ? 0 : (i * (W - L - R)) / (runs.length - 1));
    const y = (v) => T + (H - T - B) * (1 - v / niceMax);
    const line = (key) =>
      pts
        .filter((p) => p[key] !== null)
        .map((p, k) => (k ? "L" : "M") + x(p.i).toFixed(1) + " " + y(p[key]).toFixed(1))
        .join(" ");
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => f * niceMax);
    const passed = runs.filter((r) => r.status === "passed").length;
    el.innerHTML =
      `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Latency of the last ${runs.length} verified runs; ${passed} of ${runs.length} passed. Data table below.">` +
      ticks.map((t) => `<g class="tick"><line x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}"/><text x="${L - 8}" y="${y(t) + 4}">${t}s</text></g>`).join("") +
      `<path class="l95" d="${line("p95")}"/><path class="l50" d="${line("p50")}"/>` +
      pts.map((p) => (p.p95 === null ? "" : `<circle class="d95" cx="${x(p.i)}" cy="${y(p.p95)}" r="3.5"><title>${esc(p.r.timestamp_utc)}: p95 ${p.p95.toFixed(1)} s</title></circle>`)).join("") +
      pts.map((p) => `<rect class="${p.r.status === "passed" ? "pass" : "fail"}" x="${x(p.i) - 5}" y="${H - B + 18}" width="10" height="14" rx="2"><title>${esc(p.r.timestamp_utc)}: ${esc(p.r.status)}</title></rect>`).join("") +
      `<text class="axis" x="${L}" y="${H - 6}">older</text><text class="axis end" x="${W - R}" y="${H - 6}">latest</text>` +
      `<text class="axis" x="${L}" y="${H - B + 12}">pass / fail</text>` +
      `</svg>` +
      `<p class="legend"><span class="k95"></span> p95 latency <span class="k50"></span> p50 latency <span class="kpass"></span> passed <span class="kfail"></span> failed</p>` +
      `<details><summary>Show the data table</summary><div class="scroll"><table><thead><tr><th scope="col">Run (UTC)</th><th scope="col">Status</th><th scope="col" class="num">p50</th><th scope="col" class="num">p95</th><th scope="col">Commit</th></tr></thead><tbody>` +
      runs
        .slice()
        .reverse()
        .map((r) => `<tr><td>${r.run_url ? `<a href="${esc(r.run_url)}">${esc(r.timestamp_utc)}</a>` : esc(r.timestamp_utc)}</td><td>${esc(r.status)}</td><td class="num">${fmtSec(r.latency_ms && r.latency_ms.p50) || "n/a"}</td><td class="num">${fmtSec(r.latency_ms && r.latency_ms.p95) || "n/a"}</td><td><code>${esc((r.commit_sha || "").slice(0, 7))}</code></td></tr>`)
        .join("") +
      `</tbody></table></div></details>`;
  }

  async function init() {
    const [latest, history] = await Promise.all([getJSON("data/latest.json"), getJSON("data/history.json")]);
    badge(latest);
    parityNote(latest);
    cards(latest);
    chart(history);
    // Sections above grow as they render, so re-apply a #section deep link afterwards.
    const target = /^#[a-z-]+$/.test(location.hash) && document.querySelector(location.hash);
    if (target) target.scrollIntoView();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
