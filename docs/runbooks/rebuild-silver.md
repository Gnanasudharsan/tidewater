# Runbook: rebuild silver from bronze

**Chaos test:** [05_rebuild_silver_from_bronze.sh](../../tests/chaos/05_rebuild_silver_from_bronze.sh)

**When:** a silver bug corrupted data, or a table was dropped. Bronze is the replay log.

1. `docker compose stop spark-silver`
2. Drop silver: `spark-submit jobs/drop_silver.py --yes-really` (bronze untouched).
3. Set a new `SILVER_TXN_APP_ID` in `.env` (a fresh checkpoint and idempotency id),
   then `docker compose up -d spark-silver`. It replays every bronze run, decoding
   with the **schema archive** (`bronze/_schemas`), never the live registry, so
   pre-incident data still decodes even though registry IDs were reassigned.
4. **Repair what bronze cannot know:** deletes that happened while CDC was down
   (they never reached Kafka) were fixed by backfills, which write to silver, not
   bronze. Run `make backfill ARGS="--detect-deletes"`.
5. **Verify:** `make reconcile`. The warehouse loader detects the recreated tables
   (its CDF range is gone) and falls back to a full load automatically.
