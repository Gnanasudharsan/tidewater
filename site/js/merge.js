/*
 * The silver MERGE rule, ported from spark/tidewater_spark/merge.py as a pure function.
 *
 * Per micro-batch, exactly as the Spark job does it:
 *   1. latest_per_key: keep the newest change per primary key, where "newest" is the
 *      order key (_lsn, _source_offset). Every other change for that key is superseded.
 *   2. classify_against_target: compare each survivor with the row silver holds.
 *        newer  -> merged (insert, or update every column)
 *        older  -> late: recorded, never applied ("stale")
 *        equal  -> an exact replay, dropped quietly ("duplicate")
 *   3. merge: deletes are soft. op "d" sets is_deleted = true and keeps the row, so a
 *      late update for a deleted key is still older than the delete and is rejected.
 *
 * tests/parity runs the same fixtures through this file and through the real PySpark
 * merge on Delta Lake, and CI fails if the final state or the late set differ.
 */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.TidewaterMerge = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /** Order key comparison: _lsn first, then _source_offset breaks ties. */
  function compareKey(a, b) {
    if (a.lsn !== b.lsn) return a.lsn < b.lsn ? -1 : 1;
    if (a.offset !== b.offset) return a.offset < b.offset ? -1 : 1;
    return 0;
  }

  function toRow(e) {
    return {
      pk: e.pk,
      status: e.status,
      amount: e.amount,
      is_deleted: e.op === "d",
      _op: e.op,
      _lsn: e.lsn,
      _source_offset: e.offset,
    };
  }

  /**
   * Apply one micro-batch to silver.
   * @param {Object<string, Object>} silver  rows keyed by primary key (not mutated)
   * @param {Array<{pk, op, lsn, offset, status, amount}>} batch  events in arrival order
   * @returns {{silver: Object, outcomes: string[]}}  outcomes[i] is one of
   *   "applied" | "superseded" | "duplicate" | "stale" for batch[i]
   */
  function applyBatch(silver, batch) {
    const outcomes = new Array(batch.length);
    const winner = new Map(); // pk -> index of the newest change in this batch

    // 1. latest_per_key
    batch.forEach(function (e, i) {
      const k = String(e.pk);
      if (!winner.has(k)) return winner.set(k, i);
      const best = winner.get(k);
      const c = compareKey(e, batch[best]);
      if (c > 0) {
        outcomes[best] = "superseded";
        winner.set(k, i);
      } else {
        // An identical order key in one batch is the same record delivered twice.
        outcomes[i] = c === 0 ? "duplicate" : "superseded";
      }
    });

    // 2 + 3. classify against the target, then merge the newer changes
    const next = Object.assign({}, silver);
    winner.forEach(function (i, k) {
      const e = batch[i];
      const t = silver[k];
      const c = t ? compareKey(e, { lsn: t._lsn, offset: t._source_offset }) : 1;
      if (c > 0) {
        next[k] = toRow(e);
        outcomes[i] = "applied";
      } else {
        outcomes[i] = c < 0 ? "stale" : "duplicate";
      }
    });
    return { silver: next, outcomes: outcomes };
  }

  return { compareKey: compareKey, applyBatch: applyBatch };
});
