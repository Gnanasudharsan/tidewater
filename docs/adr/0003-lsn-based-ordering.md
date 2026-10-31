# ADR 0003: LSN-based ordering for idempotent merges

Status: Accepted · Date: 2026-10-31

## Context
Kafka guarantees order only within a partition, and only for one delivery
attempt. Restarts, replays from bronze, and backfills mean silver can see:
the same event twice, or an older version of a row after a newer one.
Candidates for the "which version wins" rule: Kafka offset, `ts_ms`,
an `updated_at` column, or the Postgres WAL log sequence number (LSN).

## Decision
Order changes by **`(source.lsn, source.txId, event position)`** from the Debezium
envelope. Silver stores `_lsn` per row, and the merge only applies a change when
`incoming._lsn > target._lsn`.

## Rationale
- The LSN is assigned by Postgres at commit and is strictly increasing across
  the whole database. It is the source's own definition of order.
- Kafka offsets are per partition and change if data is re-produced. They are
  useless for backfills, which do not come from Kafka at all.
- `ts_ms` has millisecond resolution, so two updates in the same millisecond tie.
- `updated_at` columns are application-controlled and can be wrong or missing.

## Consequences
- Within one batch, rows are deduplicated with
  `row_number() over (partition by pk order by _lsn desc)` before the `MERGE`.
- The backfill job must also produce an LSN. It reads the source inside a
  snapshot and stamps rows with `pg_current_wal_lsn()` taken at snapshot start,
  so any streamed change committed after the snapshot correctly wins.
- Debezium snapshot events (`op='r'`) carry the snapshot LSN, which follows the same rule.
