# ADR 0004: Medallion layering (bronze → silver → gold)

Status: Accepted · Date: 2026-10-31

## Context
The pipeline needs to be replayable, correct, and easy to model. Mixing raw
ingestion, deduplication, and business logic in one job makes failures hard to
isolate and makes rebuilds impossible.

## Decision

| Layer | Where | Contents | Write mode |
|---|---|---|---|
| Bronze | Delta on S3 (SeaweedFS) | Raw Kafka records + `_topic, _partition, _offset, _ingested_at, _batch_id` | Append-only |
| Silver | Delta on S3 (SeaweedFS) | One row per source PK, typed, deduped, soft-deleted, plus `_lsn` | `MERGE` |
| Quarantine | Delta on S3 (SeaweedFS) | Rows that failed DQ rules, with the failure reason | Append-only |
| Gold | Warehouse (dbt) | Staging → dims (SCD2) → facts → metrics | dbt incremental |

## Rationale
- Bronze is the replay log. If silver logic has a bug, fix it and rebuild silver
  from bronze (chaos test #5) without touching the source or Kafka retention.
- Silver is where correctness guarantees live, and where reconciliation runs.
- Business logic stays in dbt, which is versioned, tested, and documented.

## Consequences
- Storage is roughly doubled between bronze and silver. That is acceptable at this scale.
- Each layer has its own checkpoint, so jobs fail and recover independently.
