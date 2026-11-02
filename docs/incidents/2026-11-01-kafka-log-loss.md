# Postmortem: Kafka log lost on container recreation

Date: 2026-11-01 · Severity: data-loss risk (no data was lost) · Status: resolved

## Summary
While adding restart policies, the Kafka, Connect and schema-registry containers
were recreated. Kafka had never been writing to its volume, so every topic was
lost: the CDC topics, Debezium's connector config and offsets, and the schema
registry's journal (stored in Kafka). The pipeline stopped itself instead of
corrupting data. Recovery took about 15 minutes and ended at 100% reconciliation
against the source.

## Timeline (UTC)
- 23:31: the object store is OOM-killed (512 MB limit, no restart policy). All
  Spark jobs block on S3.
- 23:34: Compose is hardened (restart policies, 1 GB store limit). Recreating the
  containers wipes Kafka's log.
- 23:35 onwards: bronze crash-loops with `OffsetOutOfRangeException`: its
  checkpoint asks for offset 5356, which no longer exists. `failOnDataLoss=true`
  refuses to skip it. **This is the guardrail working.**
- 23:52: root cause found: `apache/kafka` logs to `/tmp/kraft-combined-logs`
  unless `KAFKA_LOG_DIRS` is set; the mounted volume was empty.
- 23:53–23:56: fix and recovery (below). Debezium re-snapshots 162,411 rows.
- 00:00: reconciliation finds 536 orders (with their items and payments) still
  live in silver but deleted at the source during the outage.
- 00:01: `backfill --detect-deletes` soft-deletes them. Reconciliation: 100%.

## Root cause
`KAFKA_LOG_DIRS` was not set, so the Kafka volume was never used (a Week 1
configuration bug). Kafka's durability is everything downstream depends on:
consumer checkpoints, connector offsets, and schema IDs.

## What went right
- Bronze refused to continue on missing offsets (`failOnDataLoss=true`).
- Postgres's replication slot kept every change since the last confirmed LSN.
- Silver's LSN guard made the full re-snapshot (162k events overlapping data it
  already had) safe to apply.
- The backfill's delete detection repaired the one gap a re-snapshot can't:
  deletes that happened while CDC was down.
- Reconciliation proved the result instead of assuming it.

## What went wrong
- No persistence test: nobody checked that the Kafka volume held data.
- The schema registry's state lives in Kafka, so it was lost too. New
  registrations reused low IDs. Old bronze records reference the old IDs, so
  **replaying pre-incident bronze through silver is unsafe** until schemas are
  archived by fingerprint (action item).
- The object store had no restart policy and too small a memory limit.
- Metabase kept its own app database inside the container (dashboard config was
  lost on recreation; re-provisioned from code in seconds).

## Action items
- [x] Set `KAFKA_LOG_DIRS`; confirmed data lands in the volume.
- [x] `restart: unless-stopped` on every long-running service; store limit 1 GB.
- [x] Bronze rows carry `_ingest_run`; the verifier checks only the current run,
      since Kafka offsets are unique only within one run.
- [x] Metabase app DB on a volume; dashboards are provisioned as code.
- [ ] Archive each Avro schema by fingerprint in the lakehouse when first seen, so
      bronze replays never depend on registry IDs (week 8, with chaos test 5).
- [ ] Chaos test: recreate every stateful container and assert nothing is lost.

See the runbook: [Kafka data loss](../runbooks/kafka-data-loss.md).
