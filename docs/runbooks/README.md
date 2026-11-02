# Runbooks

One runbook per failure scenario, each paired with a chaos test in `tests/chaos/`.
Each runbook covers: symptoms, dashboards to check, diagnosis, recovery, and verification.

| Scenario | Runbook | Chaos test |
|---|---|---|
| Kafka lost its log (offsets reset) | [kafka-data-loss.md](kafka-data-loss.md) | _to do_ ([postmortem](../incidents/2026-11-01-kafka-log-loss.md)) |
| Spark killed mid-batch | _week 8_ | [01_kill_bronze_mid_batch.sh](../../tests/chaos/01_kill_bronze_mid_batch.sh) (bronze) |
| Debezium down for 10 min | _week 8_ | _week 8_ |
| Source column added live | _week 8_ | _week 8_ |
| Malformed events | _week 8_ | _week 8_ |
| Silver rebuilt from bronze | _week 8_ | _week 8_ |
| Backfill concurrent with stream | _week 8_ | _week 8_ |
