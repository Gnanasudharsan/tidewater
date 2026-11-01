# ADR 0003: LSN-based ordering for idempotent merges

Status: Accepted · Date: 2026-10-31

## Context
Kafka guarantees order only within a partition, and only for one delivery
attempt. Restarts, replays from bronze, and backfills mean silver can see:
the same event twice, or an older version of a row after a newer one.
Candidates for the "which version wins" rule: Kafka offset, `ts_ms`,
an `updated_at` column, or the Postgres WAL log sequence number (LSN).

## Decision
Order changes by **`(source.lsn, Kafka offset)`**. Silver stores `_lsn` and
`_source_offset` per row, and the merge only applies a change when
`(incoming._lsn, incoming._source_offset) > (target._lsn, target._source_offset)`.

## Rationale
- Debezium's `source.lsn` is the WAL position of each change record. For a
  given row it is strictly increasing: a second writer must wait for the first
  writer's row lock, so its change is written to the WAL after the first commits.
  Changes to *different* rows can interleave, but they never need ordering.
- Ties happen only for snapshot reads (`op='r'`, all at the snapshot LSN, one per
  key). The Kafka offset breaks them, because one key always maps to one partition.
- Kafka offsets are per partition and change if data is re-produced. They are
  useless for backfills, which do not come from Kafka at all.
- `ts_ms` has millisecond resolution, so two updates in the same millisecond tie.
- `updated_at` columns are application-controlled and can be wrong or missing.

## Consequences
- Within one batch, rows are deduplicated with
  `row_number() over (partition by pk order by _lsn desc, _source_offset desc)`.
- The backfill job stamps rows with `_lsn = L0`, `_source_offset = -1`, where L0 is
  `pg_current_wal_lsn()` captured *before* the read. Then it waits until every
  transaction whose xid was assigned before the capture has finished. Without that
  wait, a transaction that wrote a change with LSN < L0 but committed after the
  read would lose to the backfill's older image, and silver would go stale. With
  it, every row read reflects all changes below L0, and any change above L0 wins
  over the backfill by the normal guard.
- Debezium snapshot events (`op='r'`) carry the snapshot LSN, which follows the same rule.
