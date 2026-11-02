# Runbooks

One runbook per failure scenario, each paired with an automated chaos test in
`tests/chaos/` that injects the failure and asserts recovery (bronze verifier and
100% reconciliation).

| Scenario | Runbook | Chaos test |
|---|---|---|
| Spark stream killed mid-batch | [spark-killed-mid-batch.md](spark-killed-mid-batch.md) | [01](../../tests/chaos/01_kill_bronze_mid_batch.sh) |
| Debezium down for 10 min | [debezium-outage.md](debezium-outage.md) | [02](../../tests/chaos/02_debezium_outage.sh) |
| Source column added live | [schema-change.md](schema-change.md) | [03](../../tests/chaos/03_add_column_live.sh) |
| Malformed events / DQ failures | [malformed-events.md](malformed-events.md) | [04](../../tests/chaos/04_malformed_events.sh) |
| Silver rebuilt from bronze | [rebuild-silver.md](rebuild-silver.md) | [05](../../tests/chaos/05_rebuild_silver_from_bronze.sh) |
| Backfill concurrent with stream | [concurrent-backfill.md](concurrent-backfill.md) | [06](../../tests/chaos/06_concurrent_backfill.sh) |
| Kafka lost its log (offsets reset) | [kafka-data-loss.md](kafka-data-loss.md) | [postmortem](../incidents/2026-11-01-kafka-log-loss.md) |
