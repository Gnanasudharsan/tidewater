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

> 🚧 **Work in progress.** Weeks 0–2 (foundation, CDC, bronze layer) are done. See [Roadmap](#roadmap).

## Results

_Benchmarks land in week 9. No numbers are claimed here until they are measured._

| Metric | Target (SLO) | Measured |
|---|---|---|
| p95 source → silver latency | < 60 s | — |
| Duplicate or missing offsets in bronze after crashes | 0 | **0** across 76,789 events, 3 mid-batch SIGKILLs + 1 post-commit crash ([chaos 01](tests/chaos/01_kill_bronze_mid_batch.sh)) |
| Duplicate PKs in silver after chaos tests | 0 | — |
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
make chaos-bronze         # SIGKILL the stream mid-batch under load, then verify
```

Then open **Kafka UI at http://localhost:8080** and watch change events stream into
`tidewater.public.*`. `make help` lists every command.

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark UI (bronze stream) | http://localhost:4040 |
| Kafka Connect REST | http://localhost:8083 |
| Schema registry (Apicurio) | http://localhost:8081 |
| S3 endpoint / filer UI | http://localhost:8333 / http://localhost:8888 |
| Source Postgres | `localhost:5432` (`make psql-src`) |

### Compose profiles (8 GB laptops)

| Command | Services | Approx. RAM |
|---|---|---|
| `make up-core` | postgres-src, kafka, connect, schema-registry, object-store, kafka-ui, spark-bronze | ~6 GB limits, ~3 GB actual |
| `make up-wh` | postgres-wh, metabase | ~2 GB |
| `make up-all` | everything | 16 GB machines |

## Guarantees and how they are proven

| Guarantee | Mechanism | Proof |
|---|---|---|
| Bronze has every Kafka record exactly once | Streaming checkpoint + Delta idempotent writes (`txnAppId`, `txnVersion = batch_id`) | [chaos 01](tests/chaos/01_kill_bronze_mid_batch.sh) kills mid-batch and right after the Delta commit, then [`verify_bronze.py`](spark/jobs/verify_bronze.py) checks every partition for duplicates and gaps |
| No duplicates | Silver `MERGE` on PK, guarded by `incoming.lsn > target.lsn` | unit tests + chaos suite _(weeks 3–8)_ |
| Correct order under replays and backfills | WAL LSN ordering ([ADR 0003](docs/adr/0003-lsn-based-ordering.md)) | out-of-order unit tests _(weeks 3–4)_ |
| Deletes propagate, history kept | Soft deletes ([ADR 0002](docs/adr/0002-soft-deletes.md)) | reconciliation _(weeks 3–4)_ |
| Safe schema evolution | Avro + registry in BACKWARD mode | chaos test "add column live" _(week 8)_ |

## Design docs

- [Design doc: problem, SLOs, non-goals](docs/design.md)
- ADRs: [Delta vs Iceberg](docs/adr/0001-delta-lake-over-iceberg.md) ·
  [Soft deletes](docs/adr/0002-soft-deletes.md) ·
  [LSN ordering](docs/adr/0003-lsn-based-ordering.md) ·
  [Medallion layering](docs/adr/0004-medallion-layering.md) ·
  [Postgres dev / Snowflake bench](docs/adr/0005-postgres-dev-snowflake-bench.md) ·
  [SeaweedFS over MinIO](docs/adr/0006-seaweedfs-over-minio.md)
- [Runbooks](docs/runbooks/) · [Dev log](docs/devlog.md)

## Repository layout

```
connect/      Debezium connector config
docs/         design doc, ADRs, runbooks, dev log
infra/        Postgres init SQL (source + warehouse)
scripts/      operational scripts (connector registration, reconciliation)
simulator/    Python + Faker transaction generator
spark/        Spark image, bronze stream, verifier    (silver in weeks 3–4)
dbt/          staging, dims, facts, metrics           (week 5+)
airflow/      DAGs                                    (week 7)
terraform/    S3 buckets, Snowflake objects           (week 7+)
tests/        unit and chaos tests (integration in week 7)
```

## Roadmap

- [x] **Week 0:** design doc, ADRs, repo tooling
- [x] **Week 1:** source schema, simulator, Debezium CDC, schema registry (BACKWARD)
- [x] **Week 2:** bronze (Kafka → Delta, append-only, idempotent writes, offset-gap checker, chaos test 01)
- [ ] **Weeks 3–4:** silver (dedup, LSN-guarded MERGE, soft deletes, DQ, backfill, reconciliation)
- [ ] **Week 5:** warehouse load and dbt staging
- [ ] **Week 6:** dimensional model, SCD2, MetricFlow, Metabase
- [ ] **Week 7:** Airflow, CI integration test, Terraform
- [ ] **Week 8:** Grafana, lineage, chaos suite, runbooks
- [ ] **Week 9:** Snowflake benchmarks and cost per million events
- [ ] **Week 10:** packaging and launch

## Trade-offs and future work

Single-node everything; no HA. Flink, Iceberg, and Kubernetes are deliberately out
of scope until the core guarantees are proven ([non-goals](docs/design.md#5-non-goals)).

## License

[MIT](LICENSE)
