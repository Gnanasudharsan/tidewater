# Runbooks

One runbook per failure scenario, each paired with a chaos test in `tests/chaos/`.
Each runbook covers: symptoms, dashboards to check, diagnosis, recovery, and verification.

| Scenario | Runbook | Chaos test |
|---|---|---|
| Spark killed mid-batch | _week 8_ | [01_kill_bronze_mid_batch.sh](../../tests/chaos/01_kill_bronze_mid_batch.sh) (bronze) |
| Debezium down for 10 min | _week 8_ | _week 8_ |
| Source column added live | _week 8_ | _week 8_ |
| Malformed events | _week 8_ | _week 8_ |
| Silver rebuilt from bronze | _week 8_ | _week 8_ |
| Backfill concurrent with stream | _week 8_ | _week 8_ |
