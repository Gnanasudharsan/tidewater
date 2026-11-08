// Runs tests/parity/fixtures.json through site/js/merge.js and prints the result as JSON.
// test_parity.py compares it with the PySpark merge on Delta Lake.
//   node tests/parity/run_merge.js > tests/parity/out/js_result.json
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { applyBatch } = require("../../site/js/merge.js");

const raw = fs.readFileSync(path.join(__dirname, "fixtures.json"));
const { scenarios } = JSON.parse(raw);

const result = { fixtures_sha256: crypto.createHash("sha256").update(raw).digest("hex"), scenarios: {} };
for (const s of scenarios) {
  let silver = {};
  const late = [];
  s.batches.forEach((batch, b) => {
    const res = applyBatch(silver, batch);
    silver = res.silver;
    batch.forEach((e, i) => res.outcomes[i] === "stale" && late.push([b, e.pk, e.lsn, e.offset]));
  });
  const state = {};
  for (const [pk, r] of Object.entries(silver)) {
    state[pk] = { status: r.status, amount: r.amount, is_deleted: r.is_deleted, _lsn: r._lsn, _source_offset: r._source_offset };
  }
  result.scenarios[s.name] = { state, late };
}
process.stdout.write(JSON.stringify(result, null, 2) + "\n");
