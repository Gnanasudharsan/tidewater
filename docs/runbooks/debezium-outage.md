# Runbook: Debezium / Kafka Connect is down

**Chaos test:** [02_debezium_outage.sh](../../tests/chaos/02_debezium_outage.sh)

**Symptoms:** "CDC events produced per second" drops to 0 while the source is busy;
`curl localhost:8083/connectors/tidewater-postgres-cdc/status` fails or shows FAILED.

**What protects you:** the replication slot `tidewater_slot` retains WAL from the
last confirmed LSN, so nothing is lost while Connect is down. The cost is disk:
`max_slot_wal_keep_size=2GB` caps it. Past that, the slot is invalidated and only a
re-snapshot plus a delete-detecting backfill can recover.

1. Watch slot growth:
   `select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) from pg_replication_slots;`
2. Restart: `docker compose up -d --wait connect`. If the connector is FAILED,
   read the task trace in the status output and `make register` to re-apply config.
3. Confirm slot lag drains back to ~0 and bronze offsets-behind returns to 0.
4. **Verify:** `make reconcile`.

If the slot was invalidated (`wal_status = 'lost'`): drop and recreate it, then
`make register` (Debezium re-snapshots), then `make backfill ARGS="--detect-deletes"`.
