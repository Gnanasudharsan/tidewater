# Runbook: running a backfill alongside the live stream

**Chaos test:** [06_concurrent_backfill.sh](../../tests/chaos/06_concurrent_backfill.sh)

Backfills are safe while the stream runs: both use the same LSN-guarded MERGE, and
the backfill stamps rows with a transaction-horizon LSN (ADR 0003). The cost is
write contention. Silver tables are unpartitioned, so every MERGE conflicts with
any concurrent commit, and both sides retry.

1. Prefer narrow backfills: `make backfill ARGS="--table orders --where ..."` or
   the `backfill_range` DAG.
2. Watch "merge conflict retries" and silver latency in Grafana. Latency rises
   during a backfill; that is expected.
3. If the stream approaches its retry limit (50, with backoff capped at 30 s),
   pause the backfill or raise `--chunk-size`/pause between chunks.
4. **Verify:** `make reconcile` after the backfill.
