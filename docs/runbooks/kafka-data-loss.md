# Runbook: Kafka lost its log (offsets reset)

**Symptoms:** bronze crash-loops with `OffsetOutOfRangeException` / "Cannot fetch
offset N"; topics exist but start at offset 0; `curl localhost:8083/connectors`
returns `[]`; the registry's subject list is empty.

**Do not** set `failOnDataLoss=false`. It hides the gap instead of repairing it.

1. **Stop consumers:** `docker compose --profile wh stop spark-loader spark-silver spark-bronze`
2. **Fix persistence, then recreate Kafka:** confirm `KAFKA_LOG_DIRS` points at
   the mounted volume. Recreate `kafka`, then `schema-registry`, `connect` and
   `kafka-ui` with `--force-recreate`.
3. **Check the slot still exists:**
   `select slot_name, confirmed_flush_lsn from pg_replication_slots;`
   If it is gone, changes since the loss are only recoverable via step 7.
4. **Re-register CDC:** `make register`. With no stored offsets, Debezium
   re-snapshots every table, then streams.
5. **Start bronze as a new run:** set `BRONZE_TXN_APP_ID=bronze_cdc_v<N+1>` in
   `.env` (new checkpoint, new idempotency ID; rows are tagged `_ingest_run`).
   Then `docker compose up -d spark-bronze`.
6. **Restart silver** (drops cached schema IDs from the old registry):
   `docker compose up -d spark-silver`. The first restart after bronze gains
   columns may stop once with `DELTA_SCHEMA_CHANGED`; the restart policy resumes it.
7. **Repair deletes missed during the outage:** `make backfill ARGS="--detect-deletes"`.
8. **Verify:**
   - `make verify-bronze ARGS=--require-caught-up` must pass for the new run.
   - `make reconcile` must pass at 100%.
   - `make dbt-build` must be green.

**Known risk until schemas are archived by fingerprint:** do not rebuild silver
from pre-incident bronze. Its schema IDs refer to the lost registry.
