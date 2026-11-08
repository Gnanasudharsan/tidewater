/*
 * Results from real runs: renders data/results.json, which scripts/build_site_data.py
 * builds from result files committed in the repo (bench/results/, results/). The chaos
 * table also shows the latest CI outcome for the tests CI re-runs (data/latest.json).
 */
(function () {
  "use strict";

  const REPO = "https://github.com/Gnanasudharsan/tidewater";
  const NA = '<p class="na">Not yet measured.</p>';
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  const blob = (path, text) => `<a href="${REPO}/blob/main/${esc(path)}">${esc(text || path)}</a>`;
  const n0 = (v) => Math.round(v).toLocaleString("en-US");

  async function getJSON(path) {
    try {
      const r = await fetch(path, { cache: "no-store" });
      return r.ok ? await r.json() : null;
    } catch (_) {
      return null;
    }
  }

  function chaosTable(chaos, latest) {
    const el = $("chaos-table");
    if (!chaos) return (el.innerHTML = NA);
    const ci = {};
    ((latest && latest.chaos) || []).forEach((c) => (ci[c.name.slice(0, 2)] = c));
    el.innerHTML =
      `<div class="scroll wide"><table><caption class="sr-only">Chaos test outcomes</caption><thead><tr>` +
      `<th scope="col">#</th><th scope="col">What was broken, under live load</th><th scope="col">Local run</th><th scope="col">Latest CI</th><th scope="col">Recovery and finding</th></tr></thead><tbody>` +
      chaos.runs
        .map((r) => {
          const c = ci[r.id];
          const ciCell = c
            ? `<a href="${esc(latest.run_url)}" class="pill ${c.passed ? "ok" : "bad"}">${c.passed ? "PASS" : "FAIL"}</a>`
            : `<span class="muted">not run in CI</span>`;
          return (
            `<tr><td>${blob(r.script, r.id)}</td><td class="wrap-cell">${esc(r.broken)}</td>` +
            `<td><span class="pill ${r.result === "PASS" ? "ok" : "bad"}">${esc(r.result)}</span></td><td>${ciCell}</td>` +
            `<td class="wrap-cell">${esc(r.recovery)} · ${blob(r.runbook, "runbook")}</td></tr>`
          );
        })
        .join("") +
      `</tbody></table></div>` +
      `<p class="source">Local runs: ${blob(chaos.source.split("#")[0], "README chaos suite")}, findings in the ${blob(chaos.devlog, "dev log")}. CI re-runs the CI-safe tests on every showcase run.</p>`;
  }

  function benchChart(rows) {
    const W = 500, H = 300, L = 46, R = 26, T = 16, B = 48;
    const maxX = Math.ceil(Math.max(...rows.map((r) => r.achieved_events_per_s)) / 500) * 500;
    const maxY = Math.ceil(Math.max(60, ...rows.map((r) => r.latency_p95_s)) / 20) * 20;
    const x = (v) => L + ((W - L - R) * v) / maxX;
    const y = (v) => T + (H - T - B) * (1 - v / maxY);
    const path = (k) => rows.map((r, i) => (i ? "L" : "M") + x(r.achieved_events_per_s).toFixed(1) + " " + y(r[k]).toFixed(1)).join(" ");
    const xt = [];
    for (let v = 0; v <= maxX; v += 500) xt.push(v);
    const yt = [];
    for (let v = 0; v <= maxY; v += 20) yt.push(v);
    return (
      `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Latency versus throughput: p95 stays between ${Math.min(...rows.map((r) => r.latency_p95_s))} and ${Math.max(...rows.map((r) => r.latency_p95_s))} seconds from ${rows[0].achieved_events_per_s} to ${rows[rows.length - 1].achieved_events_per_s} events per second, above the 60 second SLO.">` +
      yt.map((v) => `<g class="tick"><line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}"/><text x="${L - 8}" y="${y(v) + 4}">${v}s</text></g>`).join("") +
      xt.map((v) => `<text class="xt" x="${x(v)}" y="${H - B + 18}">${n0(v)}</text>`).join("") +
      `<text class="axis-label" x="${(L + W - R) / 2}" y="${H - 6}">business events per second (achieved)</text>` +
      `<line class="slo" x1="${L}" x2="${W - R}" y1="${y(60)}" y2="${y(60)}"/><text class="slo-label" x="${W - R}" y="${y(60) - 6}">SLO: p95 &lt; 60 s</text>` +
      `<path class="l95" d="${path("latency_p95_s")}"/><path class="l50" d="${path("latency_p50_s")}"/>` +
      rows.map((r) => `<circle class="d95" cx="${x(r.achieved_events_per_s)}" cy="${y(r.latency_p95_s)}" r="4"><title>${n0(r.achieved_events_per_s)} ev/s: p95 ${r.latency_p95_s} s</title></circle>`).join("") +
      rows.map((r) => `<circle class="d50" cx="${x(r.achieved_events_per_s)}" cy="${y(r.latency_p50_s)}" r="3"><title>${n0(r.achieved_events_per_s)} ev/s: p50 ${r.latency_p50_s} s</title></circle>`).join("") +
      `</svg><p class="legend"><span class="k95"></span> p95 latency <span class="k50"></span> p50 latency <span class="kslo"></span> SLO</p>`
    );
  }

  function benchmark(b) {
    const el = $("bench");
    if (!b) return (el.innerHTML = NA);
    el.innerHTML =
      `<div class="split"><div class="chart-box">${benchChart(b.rows)}</div><div class="scroll wide"><table><caption class="sr-only">Throughput sweep</caption><thead><tr>` +
      `<th scope="col" class="num">events/s</th><th scope="col" class="num">CDC records/s</th><th scope="col" class="num">p50</th><th scope="col" class="num">p95</th></tr></thead><tbody>` +
      b.rows
        .map((r) => `<tr><td class="num">${n0(r.achieved_events_per_s)}</td><td class="num">${n0(r.cdc_records_per_s)}</td><td class="num">${r.latency_p50_s} s</td><td class="num">${r.latency_p95_s} s</td></tr>`)
        .join("") +
      `</tbody></table></div></div>` +
      `<p class="source">Events/s is achieved business events (target ${b.rows.map((r) => n0(r.target_events_per_s)).join(", ")}). Sources keep up at every rate (max Kafka lag ${n0(Math.max(...b.rows.map((r) => r.bronze_max_offsets_behind)))} offsets); silver is the bottleneck, bounded by MERGE cost at about 250k rows per table, so the 60 s SLO is missed at this size (it held at 59.7 s when tables were around 50k rows). Laptop, every service in one 8 GB Docker VM. Source: ${blob(b.source)} · ${blob("bench/README.md", "analysis")}</p>`;
  }

  function warehouse(w) {
    const el = $("warehouse");
    if (!w) return (el.innerHTML = NA);
    const [pg, sf] = [w.rows.find((r) => r.warehouse.startsWith("postgres")), w.rows.find((r) => r.warehouse.startsWith("snowflake"))];
    if (!pg || !sf) return (el.innerHTML = NA);
    const max = Math.max(pg.full_refresh_s, sf.full_refresh_s);
    const bar = (r, label) => `<div class="hbar"><span class="hlabel">${label}</span><span class="htrack"><span class="hfill" style="width:${(100 * r.full_refresh_s) / max}%"></span></span><span class="hval">${r.full_refresh_s} s</span></div>`;
    const row = (label, k, unit, better) => {
      const a = pg[k], b = sf[k];
      // Highlight a winner only when the gap is real (more than 10%).
      const win = better && Math.abs(a - b) > 0.1 * Math.max(a, b) ? (a < b ? "pg" : "sf") : "";
      return `<tr><th scope="row">${label}</th><td class="num ${win === "pg" ? "win" : ""}">${a}${unit}</td><td class="num ${win === "sf" ? "win" : ""}">${b}${unit}</td></tr>`;
    };
    el.innerHTML =
      `<div class="split"><div class="chart-box"><p class="mini-title">Full dbt rebuild, wall clock</p>${bar(pg, "Postgres 16")}${bar(sf, "Snowflake XS")}</div>` +
      `<div class="scroll wide"><table><caption class="sr-only">Postgres versus Snowflake, same dbt project and data</caption><thead><tr><th scope="col"></th><th scope="col" class="num">Postgres 16</th><th scope="col" class="num">Snowflake X-Small</th></tr></thead><tbody>` +
      row("dbt nodes", "nodes", "", false) +
      row("Full rebuild", "full_refresh_s", " s", true) +
      row("fct_orders", "fct_orders_s", " s", true) +
      row("fct_order_items", "fct_order_items_s", " s", true) +
      row("Data test, average", "test_avg_s", " s", true) +
      `<tr><th scope="row">Live orders (both)</th><td class="num" colspan="2">${n0(pg.live_orders)}</td></tr>` +
      `</tbody></table></div></div>` +
      `<p class="source">Identical data in both warehouses, verified per table. Level on the heavy models; Snowflake loses overall on per-query overhead at this size. Source: ${blob(w.source)} · ${blob("bench/README.md", "analysis")}</p>`;
  }

  function dashboard(m) {
    const el = $("dashboard");
    if (!m || !m.dashboard_gif) return (el.hidden = true);
    el.innerHTML =
      `<img src="${esc(m.dashboard_gif)}" width="800" height="500" loading="lazy" decoding="async" alt="Grafana dashboard during a real run: load arrives, bronze is killed mid-batch (a gap in the lag line), and the pipeline recovers.">` +
      `<figcaption>Grafana during a real local run: bronze is SIGKILLed mid-batch and recovers. Source: ${blob(m.dashboard_gif_source)}</figcaption>`;
  }

  async function init() {
    if (!$("results")) return;
    const [data, latest] = await Promise.all([getJSON("data/results.json"), getJSON("data/latest.json")]);
    const d = data || {};
    chaosTable(d.chaos, latest);
    benchmark(d.benchmark);
    warehouse(d.warehouse);
    dashboard(d.media);
  }

  document.addEventListener("DOMContentLoaded", init);
})();
