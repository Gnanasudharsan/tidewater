# Runbook: a Spark stream dies mid-batch

**Chaos test:** [01_kill_bronze_mid_batch.sh](../../tests/chaos/01_kill_bronze_mid_batch.sh)

**Symptoms:** Grafana "Seconds since last progress" climbs; alert *A streaming job
stopped reporting progress* fires; the container shows `Restarting` or exited.

**What protects you:** bronze replays the checkpointed offsets of the failed batch
and Delta's `txnAppId/txnVersion` skips it if it had already committed. Silver
re-runs the batch, and the LSN-guarded MERGE makes it a no-op for rows already applied.

1. Check `docker compose ps`: `restart: unless-stopped` normally brings it back.
2. If it crash-loops, read the root error:
   `docker compose logs --since 10m spark-bronze | grep -E "Exception|Error" | sort | uniq -c`
3. Common causes: S3 store down or out of memory (check `object-store`), Kafka
   offsets gone (see [kafka-data-loss.md](kafka-data-loss.md)), schema change
   (`DELTA_SCHEMA_CHANGED`: one restart resolves it).
4. **Verify:** `make verify-bronze ARGS=--require-caught-up`, then `make reconcile`.

**Never** delete a checkpoint to "unstick" a stream without bumping its
`*_TXN_APP_ID`. Delta would silently skip the re-numbered batches.
