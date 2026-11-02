# Tidewater

[![ci](https://github.com/Gnanasudharsan/tidewater/actions/workflows/ci.yml/badge.svg)](https://github.com/Gnanasudharsan/tidewater/actions/workflows/ci.yml)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

**A CDC lakehouse that turns at-least-once Kafka delivery into exactly-once results, and proves it with automated failure injection.**

Postgres → Debezium → Kafka (+ Schema Registry) → Spark Structured Streaming → Delta Lake (bronze/silver) → dbt → Metabase. Runs entirely on a laptop for $0.

```
Postgres 16 (WAL) ──▶ Debezium 2.7 ──▶ Kafka 3.7 (KRaft) ──▶ Spark 3.5 ──▶ Delta on S3 ──▶ Postgres / Snowflake ──▶ dbt ──▶ Metabase
        ▲                              Apicurio Registry       bronze → silver                     staging → marts
   simulator                           (Avro, BACKWARD)
```

> 🚧 **Work in progress.** Weeks 0–8 are done (CDC through marts, orchestration, CI/CD, IaC, observability, lineage, 6 chaos tests); the hour-long soak and Snowflake benchmarks are next. See [Roadmap](#roadmap).

## Results

_Benchmarks land in week 9. No numbers are claimed here until they are measured._

| Metric | Target (SLO) | Measured |
|---|---|---|
| p95 source → silver latency | < 60 s | — |
| Duplicate or missing offsets in bronze after crashes | 0 | **0** across 76,789 events, 3 mid-batch SIGKILLs + 1 post-commit crash ([chaos 01](tests/chaos/01_kill_bronze_mid_batch.sh)) |
| Silver vs. source reconciliation (counts + per-row SHA-256) | 100% | **100%** on all 5 tables, 162k rows, including after a [total Kafka data loss](docs/incidents/2026-11-01-kafka-log-loss.md) |
| Duplicate PKs in silver after chaos tests | 0 | **0**: all 6 chaos tests end in 100% reconciliation ([results](#chaos-suite)) |
| Source reconciliation | 100% | — |
| Throughput | — | — |
| Snowflake credits per 1M events | — | — |

## Quickstart

Requires Docker with about 6 GB of RAM available.

```bash
git clone https://github.com/Gnanasudharsan/tidewater.git && cd tidewater
make up-core              # Postgres, Kafka, Debezium, schema registry, S3, Kafka UI, bronze stream
make register             # BACKWARD compatibility rule + Debezium connector
make sim RATE=50          # generate live e-commerce traffic (Ctrl-C to stop)
make verify-bronze        # prove bronze holds every Kafka offset exactly once
make reconcile            # prove silver matches the source DB: counts + per-row hashes
make backfill ARGS="--table orders"   # backfill from the source while the stream runs
make chaos-bronze         # SIGKILL the stream mid-batch under load, then verify
make soak DURATION=3600   # an hour of load + a concurrent backfill, then reconcile

make up-wh                # warehouse, continuous loader, Metabase
make dbt-build            # snapshot + staging + dims + incremental facts + 65 data tests
make metrics              # query GMV, orders, AOV, refund rate via MetricFlow
make dashboard            # provision the Metabase dashboard as code
```

Then open **http://localhost:3000** for the live dashboard (admin login is in your `.env`).

```bash
make up-orch              # Airflow at http://localhost:8085 (user admin, `make airflow-password`)
make integration          # 1,000 events end to end, asserted in the marts
make tf-apply             # Terraform-managed S3 buckets
```

| Airflow DAG | Schedule | What it does |
|---|---|---|
| `dbt_build_hourly` | hourly | silver → warehouse load, source freshness, `dbt build` |
| `backfill_range` | on demand | backfill one table for a validated date window, then reload and rebuild |
| `lakehouse_maintenance_nightly` | 03:00 | Delta OPTIMIZE + VACUUM (bronze: 2,110 → 15 files) |
| `dq_daily_report` | 06:00 | per-table DQ summary to `marts.dq_daily_report`; fails above a quarantine threshold |

Then open **Kafka UI at http://localhost:8080** and watch change events stream into
`tidewater.public.*`. `make help` lists every command.

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark UI (bronze / silver streams) | http://localhost:4040 / http://localhost:4041 |
| Kafka Connect REST | http://localhost:8083 |
| Schema registry (Apicurio) | http://localhost:8081 |
| S3 endpoint / filer UI | http://localhost:8333 / http://localhost:8888 |
| Source Postgres | `localhost:5432` (`make psql-src`) |
| Warehouse Postgres | `localhost:5434` (`make psql-wh`) |
| Metabase dashboard | http://localhost:3000 |
| Airflow | http://localhost:8085 |
| Grafana | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| Marquez (lineage) | http://localhost:3002 |
| dbt docs (lineage) | https://gnanasudharsan.github.io/tidewater/ |

### Compose profiles (8 GB laptops)

| Command | Services | Approx. RAM |
|---|---|---|
| `make up-core` | postgres-src, kafka, connect, schema-registry, object-store, kafka-ui, spark-bronze | ~6 GB limits, ~3 GB actual |
| `make up-wh` | postgres-wh, metabase | ~2 GB |
| `make up-all` | everything | 16 GB machines |

## Chaos suite

Each test injects a failure under live load and passes only if bronze has every
Kafka offset exactly once and silver reconciles 100% with the source (row counts +
SHA-256 of every column). Runbook for each: [docs/runbooks](docs/runbooks/).

| # | Failure injected | Result | Found along the way |
|---|---|---|---|
| [01](tests/chaos/01_kill_bronze_mid_batch.sh) | SIGKILL bronze mid-batch ×3, plus a crash right after the Delta commit | PASS | replay skipped by Delta `txnVersion` |
| [02](tests/chaos/02_debezium_outage.sh) | Debezium down (ran 180 s; default 600 s) | PASS | slot retained 14 MB of WAL |
| [03](tests/chaos/03_add_column_live.sh) | `ADD COLUMN ... DEFAULT` live | PASS | **metadata-only DDL emits no CDC for existing rows; procedure is ALTER + backfill** |
| [04](tests/chaos/04_malformed_events.sh) | 4 malformed Kafka records + a DQ-violating row | PASS | each quarantined with its exact reason; streams never stopped |
| [05](tests/chaos/05_rebuild_silver_from_bronze.sh) | drop all silver, rebuild from bronze (both ingest runs) | PASS | needed the **schema archive** ([ADR 0010](docs/adr/0010-schema-archive-in-bronze.md)) and a delete-repair backfill |
| [06](tests/chaos/06_concurrent_backfill.sh) | full backfill of 341k rows concurrent with live CDC | PASS | **unpartitioned MERGE targets livelock**; fixed with retry budget + pacing |

## Observability and lineage

```bash
make up-obs               # Prometheus :9090, Grafana http://localhost:3001 (anonymous read-only)
make up-lineage           # Marquez UI http://localhost:3002 (set OPENLINEAGE_URL=http://marquez:5000)
make audit-trace ID=1     # one order: mart -> warehouse -> silver -> Kafka offsets -> WAL LSN
```

- **Grafana** (provisioned as code): CDC produce rate, bronze consumer lag, input vs
  processed rows/s, batch duration, source→silver p50/p95 latency against the 60 s
  SLO, rows merged, DQ failures, merge-conflict retries, dbt run times, warehouse freshness.
- **Alerts** (to Slack/Discord or the local sink): latency SLO breach, bronze falling
  behind, stream stalled or down, rows quarantined. Each was fired and resolved in testing.
- **Lineage:** one OpenLineage graph in Marquez from the Kafka topics to `fct_orders`
  (25 datasets, 24 jobs), from the Spark listener, `dbt-ol`, and explicit events for
  what Spark cannot see (MERGE in `foreachBatch`, Change Data Feed reads).
- **Audit trace:** [notebooks/audit_trace.ipynb](notebooks/audit_trace.ipynb) follows
  order 1 through every layer, including its pre-incident events (schema 9) and its
  post-incident re-snapshot (schema 7).

## Guarantees and how they are proven

| Guarantee | Mechanism | Proof |
|---|---|---|
| Bronze has every Kafka record exactly once | Streaming checkpoint + Delta idempotent writes (`txnAppId`, `txnVersion = batch_id`) | [chaos 01](tests/chaos/01_kill_bronze_mid_batch.sh) kills mid-batch and right after the Delta commit, then [`verify_bronze.py`](spark/jobs/verify_bronze.py) checks every partition for duplicates and gaps |
| One row per key, newest version wins | Silver `MERGE` applies a change only if `(lsn, offset)` is newer than the stored row | [merge tests](spark/tests/test_silver_merge.py): dedup, out-of-order, replay-twice, ties |
| Correct under replays and concurrent backfills | WAL LSN ordering, plus a transaction horizon before the backfill reads ([ADR 0003](docs/adr/0003-lsn-based-ordering.md)) | [soak](scripts/soak.sh): full backfill during live load, then reconcile _(soak run pending; see devlog)_ |
| Deletes propagate, history kept | Soft deletes; a stale update after a delete is rejected ([ADR 0002](docs/adr/0002-soft-deletes.md)) | delete tests + [reconcile](spark/jobs/reconcile.py) |
| Silver matches the source exactly | Row counts + SHA-256 of every column of every row | [`reconcile.py`](spark/jobs/reconcile.py) |
| Bad data never reaches silver silently | YAML DQ rules ([contracts](spark/conf/silver_tables.yml)); undecodable or failing rows → `_quarantine` with reasons | [DQ](spark/tests/test_dq.py) + [decode](spark/tests/test_cdc_decode.py) tests |
| Warehouse never skips or regresses a row | Loads by Delta version via Change Data Feed; upsert + bookmark in one transaction, LSN-guarded ([ADR 0008](docs/adr/0008-warehouse-load-by-delta-version.md)) | [loader SQL](tests/unit/test_warehouse_sql.py) + [CDF](spark/tests/test_cdf.py) tests; dbt tests |
| Late data is recorded, not dropped | Lateness by LSN, not by watermark ([ADR 0007](docs/adr/0007-late-data-by-lsn-not-watermark.md)) | `classify` tests |
| Safe schema evolution | Avro + registry in BACKWARD mode | chaos test "add column live" _(week 8)_ |

## Design docs

- [Design doc: problem, SLOs, non-goals](docs/design.md)
- ADRs: [Delta vs Iceberg](docs/adr/0001-delta-lake-over-iceberg.md) ·
  [Soft deletes](docs/adr/0002-soft-deletes.md) ·
  [LSN ordering](docs/adr/0003-lsn-based-ordering.md) ·
  [Medallion layering](docs/adr/0004-medallion-layering.md) ·
  [Postgres dev / Snowflake bench](docs/adr/0005-postgres-dev-snowflake-bench.md) ·
  [SeaweedFS over MinIO](docs/adr/0006-seaweedfs-over-minio.md) ·
  [Late data by LSN](docs/adr/0007-late-data-by-lsn-not-watermark.md) ·
  [Warehouse load by Delta version](docs/adr/0008-warehouse-load-by-delta-version.md) ·
  [Containers via socket proxy](docs/adr/0009-orchestrate-containers-via-socket-proxy.md) ·
  [Schema archive in bronze](docs/adr/0010-schema-archive-in-bronze.md)
- [Runbooks](docs/runbooks/) · [Dev log](docs/devlog.md) · [Postmortem: Kafka log loss](docs/incidents/2026-11-01-kafka-log-loss.md)

## Repository layout

```
connect/      Debezium connector config
docs/         design doc, ADRs, runbooks, dev log
infra/        Postgres init SQL (source + warehouse)
scripts/      connector registration, soak test, Metabase provisioning
simulator/    Python + Faker transaction generator
spark/        Spark image; bronze + silver streams, backfill, reconcile; DQ contracts in conf/
dbt/          staging, SCD2 snapshot, dims, incremental facts, semantic layer, tests
airflow/      DAGs: hourly build, backfill, maintenance, DQ report
terraform/    S3 buckets (Snowflake objects in week 9)
tests/        unit, Airflow DAG, chaos and end-to-end integration tests
```

## Roadmap

- [x] **Week 0:** design doc, ADRs, repo tooling
- [x] **Week 1:** source schema, simulator, Debezium CDC, schema registry (BACKWARD)
- [x] **Week 2:** bronze (Kafka → Delta, append-only, idempotent writes, offset-gap checker, chaos test 01)
- [~] **Weeks 3–4:** silver built and reconciled 100% on a 77k-event backlog; the hour-long soak with a concurrent backfill is still to run
- [x] **Week 5:** incremental warehouse load (Delta CDF), dbt staging models, source freshness, 29 data tests
- [x] **Week 6:** SCD2 customer dimension, incremental facts, dbt-expectations, MetricFlow metrics (GMV, AOV, refund rate, repeat-customer rate), docs site, Metabase dashboard as code
- [x] **Week 7:** 4 Airflow DAGs (via a Docker socket proxy), dbt Slim CI, DAG tests, Compose integration test in CI, Terraform for S3
- [x] **Week 8:** Prometheus + Grafana + alerts, OpenLineage/Marquez, 6 chaos tests with runbooks, audit trace, schema archive
- [ ] **Week 9:** Snowflake benchmarks and cost per million events
- [ ] **Week 10:** packaging and launch

## Trade-offs and future work

Single-node everything; no HA. **Silver tables are unpartitioned**, so concurrent
MERGEs (stream + backfill) conflict on every commit; partitioning by key range would
let writers touch disjoint files. The full stack needs more than an 8 GB Docker VM to
run everything at once comfortably. Flink, Iceberg, and Kubernetes are deliberately out
of scope until the core guarantees are proven ([non-goals](docs/design.md#5-non-goals)).

## License

[MIT](LICENSE)
