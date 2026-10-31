# ADR 0001: Delta Lake over Apache Iceberg

Status: Accepted · Date: 2026-10-31

## Context
Silver needs ACID `MERGE`, schema evolution, time travel, and streaming writes
from Spark into S3-compatible storage (SeaweedFS locally), on one laptop.

## Decision
Use **Delta Lake** (OSS `delta-spark` 3.x) as the table format.

## Rationale
- `MERGE` inside `foreachBatch` is mature and well documented in Delta.
- No catalog service needed. Delta stores its transaction log next to the data,
  which saves RAM compared with running a Hive Metastore or REST catalog for Iceberg.
- `mergeSchema` / `autoMerge` covers the "add a source column live" scenario.
- `OPTIMIZE` and `VACUUM` are available in OSS for the nightly maintenance DAG.

## Consequences
- Iceberg's engine neutrality (Trino, Flink, Snowflake) is lost for now.
  Snowflake loads come from the warehouse loader, not from reading Delta directly.
- Concurrent writers (stream + backfill) rely on Delta optimistic concurrency.
  Conflicts must be retried. This is tested in chaos test #6.
- Revisit if the project adds Flink or needs multi-engine reads (future work).
