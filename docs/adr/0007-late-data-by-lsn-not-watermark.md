# ADR 0007: Detect late data by LSN instead of dropping it by watermark

Status: Accepted · Date: 2026-11-01

## Context
The plan called for an event-time watermark with late rows routed to a
`_late_arrivals` table. A watermark is built for stateful aggregations: once it
passes, Spark drops anything older. Silver is not an aggregation. It is a
per-key upsert, and the SLO is **100% reconciliation** with the source.

## Decision
- Silver never drops data based on event time.
- An event is **late** when its order key `(_lsn, _source_offset)` is strictly
  older than the version silver already holds for that key. Late events are
  appended to `silver/_late_arrivals` (with both LSNs and both source
  timestamps) and are not merged.
- Exact replays (equal order key) are duplicates, not late data, and are dropped
  quietly.

## Rationale
- A watermark would silently discard a legitimate change that arrives after a
  long Debezium outage (chaos test #2), which breaks reconciliation.
- LSN lateness is exact: it says "this change is superseded", which is the only
  lateness that matters for an upsert. How late it was in wall-clock terms
  (`_target_source_ts - _source_ts`) is still recorded for monitoring.
- The MERGE guard rejects these rows anyway. Splitting them out first turns a
  silent no-op into an auditable record.

## Consequences
- Classifying late rows costs one join with the target per batch (key and order
  columns only). That is acceptable at this scale; at larger scale the split
  can be sampled or dropped, because correctness never depends on it.
- If a watermark is needed later (for example, gold-layer streaming
  aggregations), it belongs there, not in silver.
