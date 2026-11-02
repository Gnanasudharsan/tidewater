# Tidewater — Design Doc

Status: Draft v1 · Owner: Gnanasudharsan Ashokumar · Last updated: 2026-10-31

## 1. Problem

Operational databases are the source of truth for business events, but analytics
needs that data in a lakehouse and a warehouse, **fresh** and **correct**. Batch
`SELECT *` extracts are slow, miss deletes, and hammer the source. Log-based
change data capture (CDC) fixes freshness, but introduces the hard parts:

- Kafka delivers **at least once**, so every event may arrive more than once.
- Events for the same key can arrive **out of order** (retries, rebalances,
  backfills racing the live stream).
- Source schemas **change** while the pipeline is running.
- Deletes must propagate, but history must stay auditable.

Tidewater is a CDC lakehouse that turns an at-least-once stream into
**exactly-once results**, and proves it with automated failure injection.

## 2. Scope

Domain: a simulated e-commerce OLTP database with five tables:
`customers`, `products`, `orders`, `order_items`, `payments`.

```
Postgres (WAL) ─▶ Debezium ─▶ Kafka (+ Schema Registry)
                                 │
                                 ▼
                     Spark Structured Streaming
                   bronze (raw, append-only Delta on S3 (SeaweedFS))
                                 │
                                 ▼
                 silver (deduped, typed, merged Delta tables)
                                 │
                                 ▼
             Postgres warehouse / Snowflake ─▶ dbt marts ─▶ Metabase
```

Orchestration: Airflow. Observability: Prometheus + Grafana. Lineage:
OpenLineage + Marquez.

## 3. SLOs

| SLO | Target | How it is measured |
|---|---|---|
| Freshness | p95 source-commit → silver-commit latency **< 60 s** at 500 events/s | `source.ts_ms` vs. silver commit timestamp, exported to Prometheus |
| Uniqueness | **0** duplicate primary keys in silver, ever | dbt `unique` tests + reconciliation script |
| Completeness | **100%** row-count and per-row hash match, silver vs. source | `scripts/reconcile.py` after every chaos test |
| Recoverability | Any single component crash recovers with no manual data fixes | `tests/chaos/*` |

An SLO that is not measured automatically does not count.

## 4. Key design decisions

Each has an ADR in [`docs/adr/`](adr/):

1. [Delta Lake over Iceberg](adr/0001-delta-lake-over-iceberg.md)
2. [Soft deletes in silver](adr/0002-soft-deletes.md)
3. [LSN-based ordering for idempotent merges](adr/0003-lsn-based-ordering.md)
4. [Medallion layering](adr/0004-medallion-layering.md)
5. [Postgres for dev, Snowflake for benchmarks](adr/0005-postgres-dev-snowflake-bench.md)
6. [SeaweedFS instead of MinIO for local S3](adr/0006-seaweedfs-over-minio.md)
7. [Detect late data by LSN instead of dropping it by watermark](adr/0007-late-data-by-lsn-not-watermark.md)
8. [Load the warehouse by Delta version, not by timestamp](adr/0008-warehouse-load-by-delta-version.md)
9. [Airflow runs jobs as containers through a Docker socket proxy](adr/0009-orchestrate-containers-via-socket-proxy.md)

### Correctness model (summary)

- **Bronze** stores every Kafka record with `(_topic, _partition, _offset)`.
  That triple is unique, so replays are detectable and bronze is a complete,
  replayable log.
- **Silver** is a `MERGE` keyed on the primary key that only applies an event
  when `incoming.lsn > target.lsn` (with a tiebreak inside a transaction).
  Applying the same batch twice, or an older event after a newer one, is a no-op.
  This makes the merge **idempotent and order-insensitive**, which is what turns
  at-least-once delivery into exactly-once results.
- The **backfill job** calls the same merge function, so it is safe to run
  concurrently with the stream (Delta optimistic concurrency + LSN guard).
  It waits for a transaction horizon before reading so its LSN stamp is honest
  (ADR 0003), merges in small chunks, and retries write conflicts.

## 5. Non-goals

- Multi-node clusters, Kubernetes, or high availability of the infrastructure itself.
- Sub-second latency. The target is under a minute, which fits analytics.
- Flink, Iceberg, or a second CDC source (listed as future work).
- Real customer data. Everything is synthetic (Faker).
- Paid cloud services, except a time-boxed Snowflake trial for benchmarks.

## 6. Risks

| Risk | Mitigation |
|---|---|
| Laptop RAM (8 GB) | Compose profiles; per-container memory limits |
| Replication slot fills disk when Debezium is down | Debezium heartbeats; chaos test #2; runbook |
| Schema IDs lost on registry restart | Registry persists to a Kafka topic (KafkaSQL storage) |
| Snowflake trial expires | Export all benchmark results to `bench/results/` during the trial |
