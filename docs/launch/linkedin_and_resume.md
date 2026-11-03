# LinkedIn post and resume bullets

Every number below is measured in the repo. Don't round them up.

## LinkedIn post (draft)

> I spent 10 weeks building a data pipeline and then trying to break it.
>
> **Tidewater** copies every change from a Postgres database into a lakehouse and a
> warehouse (Debezium → Kafka → Spark → Delta Lake → dbt), and guarantees the results are
> exactly right even though Kafka can deliver the same event twice.
>
> How I know: 6 automated chaos tests. Kill Spark mid-batch, take Debezium down,
> change the schema live, inject garbage, rebuild everything from raw data, run a
> backfill during live traffic. Every one ends with a check of row counts plus a hash
> of every row against the source: 0 duplicates, 100% match.
>
> Things I didn't expect:
> • Adding a column with a DEFAULT in Postgres produces no change events for existing rows. Hashes caught it; counts wouldn't have.
> • I accidentally wiped Kafka. The pipeline refused to continue instead of silently losing data, and I recovered it to 100%. Postmortem is in the repo.
> • My latency target holds on small tables and misses at scale. I documented why instead of hiding it.
>
> Architecture, results, design decisions and the postmortem:
> github.com/Gnanasudharsan/tidewater
>
> #DataEngineering #ApacheKafka #Debezium #DeltaLake #dbt #ApacheSpark

Attach `docs/architecture.svg` (export as PNG) or the dashboard GIF.

## Resume bullets

Pick 2–3; lead with the strongest for the role.

- **Built Tidewater, a CDC lakehouse** (Debezium, Kafka, Spark Structured Streaming,
  Delta Lake, dbt, Airflow) sustaining **2,000 events/s** at the source with **zero
  duplicates and 100% hash-level reconciliation across 6 automated failure-injection
  tests**.
- **Designed LSN-guarded idempotent MERGEs and transaction-horizon backfills**, making
  replays, out-of-order events and concurrent backfills safe; proven by a 341k-row
  backfill during live CDC reconciling 100%.
- **Recovered a total Kafka data loss to 100% reconciliation** with zero data lost
  (replication-slot replay plus delete-detecting backfill); wrote the postmortem and
  added a bronze schema archive so replays no longer depend on the schema registry.
- **Ran one dbt project on Postgres and Snowflake** (81/81 models and tests);
  provisioned Snowflake with Terraform (key-pair auth, credit-capped resource monitor);
  benchmarked at ~0.059 credits per million rows.
- **Built observability and lineage as code**: Prometheus/Grafana with 4 alert rules,
  one OpenLineage graph from Kafka topic to mart in Marquez, and 7 CI jobs including a
  1,000-event end-to-end integration test.

**Interview stories** (each is in [docs/devlog.md](../devlog.md)): the Kafka log
loss; "a filter is not a guard in Spark"; the metadata-only DDL; the registry ID
collision; why a benchmark harness lied twice before the pipeline did.
