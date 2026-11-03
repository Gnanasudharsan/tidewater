# Exactly-once results from at-least-once Kafka: idempotent CDC merges with concurrent backfills

*Draft for Medium / Hashnode. About 1,800 words. All numbers are from the
[Tidewater repo](https://github.com/Gnanasudharsan/tidewater).*

---

Kafka gives you at-least-once delivery. Your stream processor gives you
at-least-once processing. Your boss wants the revenue dashboard to be *exactly*
right. Here is how I closed that gap in a change-data-capture pipeline, and how I
proved it by deliberately breaking things, including one break I didn't plan.

## The problem with "exactly-once"

A CDC pipeline copies every insert, update and delete from an operational database
into analytics storage. In Tidewater that's Postgres → Debezium → Kafka → Spark
Structured Streaming → Delta Lake → a warehouse → dbt.

At every hop, the same change can arrive twice (a retry after a crash), late (a
partition lags), or out of order (a backfill racing the live stream). "Exactly-once
delivery" across all of those hops is a fight you lose. **Exactly-once *results*** is
winnable: make every write idempotent and order-insensitive, so duplicates and
reordering simply don't change the outcome.

## Rule 1: order by the source's clock, not yours

The question every merge must answer is: *is this change newer than what I have?*
Timestamps are the obvious answer and the wrong one: millisecond ties, clock skew,
application-controlled `updated_at` columns.

Postgres already has a perfect ordering: the **WAL log sequence number (LSN)**. For
any single row, LSNs strictly increase, because a second writer has to wait for the
first writer's row lock. Debezium puts the LSN on every change event. Silver stores it
on every row, and the MERGE only applies a change if it's newer:

```sql
MERGE INTO silver.orders t USING changes s ON t.order_id = s.order_id
WHEN MATCHED AND (s._lsn > t._lsn OR (s._lsn = t._lsn AND s._source_offset > t._source_offset))
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

That one condition buys three properties:
- **Idempotent:** replaying a batch changes nothing (equal keys aren't newer).
- **Order-insensitive:** an older event arriving late is rejected.
- **Retry-safe:** if two writers conflict, retrying is harmless.

Deletes are soft (`is_deleted = true`) for the same reason: if you physically delete
a row, a late update can resurrect it. A tombstone keeps its LSN, so the stale update loses.

## Rule 2: make the raw layer replayable, and exactly-once on its own

Bronze stores raw Kafka bytes plus `(topic, partition, offset)`. A crash between
writing data and committing the stream checkpoint means the same batch is replayed.
Delta handles this natively: write with `txnAppId` and `txnVersion = batch_id`, and a
second write of the same batch is skipped.

Random kills rarely land in that window, so the chaos test crashes deliberately
*after* the Delta commit and *before* the checkpoint, and asserts the replay was
skipped. A verifier then checks every Kafka partition: no duplicate offsets, no gaps.

## Rule 3: backfills need a transaction horizon

Backfills read the source directly and stamp rows with an LSN, so the guard decides
between them and live events. The naive approach, "take `pg_current_wal_lsn()` and
read", has a hole: a transaction that wrote a change *below* that LSN but hasn't
committed yet is invisible to your read. When its event arrives, it loses to your
stamp, and silver is silently stale.

The fix: capture the LSN, *then* the next transaction id, and wait until every older
transaction has finished before reading. Now the stamp is honest: everything below it
is visible, and anything above it wins by the normal rule. Backfills can run during
live traffic. Chaos test 06 ran a 341k-row backfill under load and reconciled 100%.

## Rule 4: load the warehouse by version, not by timestamp

The classic incremental load, `WHERE updated_at > :watermark`, skips rows whenever two
writers commit out of timestamp order. With a stream and a backfill writing silver,
that happens. Tidewater bookmarks **Delta table versions** and reads the Change Data
Feed between them; the upsert and the bookmark commit in one transaction. One more
lesson found later: versions restart at 0 when a table is recreated, so the bookmark
also stores the Delta table id and forces a full reload when it changes.

## Proving it: six chaos tests and a reconciler

None of this counts unless it's measured. Every chaos test ends with the same two
checks: bronze has every Kafka offset exactly once, and silver matches the source on
row counts **and a SHA-256 of every column of every row**.

| Failure injected under live load | What it taught |
|---|---|
| SIGKILL mid-batch, plus a crash right after the Delta commit | the Delta transaction id skips the replay |
| Debezium down | the replication slot retains the WAL |
| `ADD COLUMN ... DEFAULT` live | **this one failed the first time**, see below |
| Malformed Kafka records, a row violating a quality rule | each is quarantined with its exact reason |
| Drop all of silver, rebuild from bronze | needed a schema archive (see below) |
| Full backfill during live CDC | unpartitioned tables conflict on every commit |

**The DDL surprise.** In Postgres 11+, `ADD COLUMN ... DEFAULT` is a metadata-only
change: existing rows get the default *without being rewritten*, so no WAL is written
and CDC never sees their new value. Reconciliation caught 12,924 mismatched rows. The
fix isn't code, it's procedure: after that DDL, backfill the table.

## The incident I didn't plan

While adding restart policies, I recreated the Kafka container, and every topic
vanished. `KAFKA_LOG_DIRS` wasn't set, so Kafka had been writing inside the container
the whole time. The pipeline did the right thing: bronze refused to continue
(`failOnDataLoss=true`) rather than skip missing offsets. Recovery: fix persistence,
let Debezium re-snapshot from the replication slot (the LSN guard made the 162k-event
overlap harmless), and run a delete-detecting backfill for orders deleted while
Kafka was down. Reconciliation: 100%.

The incident left a nastier problem. The schema registry also stored its state in
Kafka. Rebuilt, it reassigned IDs: **ID 9 had meant `orders` and now meant
`order_items`.** Bronze stores schema IDs, so replaying old bronze would have decoded
orders with the wrong schema. The fix: bronze archives every writer schema it sees,
keyed by ingest run and fingerprint, and silver decodes from the archive. The chaos
test that rebuilds silver from bronze then passed with zero quarantined rows.

## The numbers, including the bad ones

- Sources sustain **2,000 business events/s** (5,232 CDC records/s) with 0 Kafka lag.
- The p95 source-to-silver latency SLO (< 60 s) was **met at 100 events/s on small
  tables (59.7 s) and missed at scale (92–122 s)**. MERGE into unpartitioned Delta
  tables rewrites large files, so latency grows with table size, not load.
- An optimization I expected to work (concurrent per-table MERGEs) was
  **inconclusive** in an A/B/A test: the two identical baselines differed 2.5×. It
  ships behind a flag, off.
- The same dbt project runs on Postgres and Snowflake. Over identical data, a full
  rebuild took 16.3 s on local Postgres and 31.7 s on Snowflake X-Small: level on the
  heavy models, with Snowflake paying per-query overhead on 65 small tests. Cost:
  about 0.059 credits per million rows.

## Takeaways

1. Use the source's own ordering (the LSN), not timestamps.
2. Make every write idempotent, then *test the window where it matters*, not just random crashes.
3. Backfills need a transaction horizon to be safe alongside a stream.
4. Load incrementally by version, not by timestamp.
5. Reconcile with hashes, not counts. Counts would have missed the DDL bug.
6. Report the misses. The SLO miss and the inconclusive experiment are the most
   useful parts of the benchmark.

Code, ADRs, runbooks and the postmortem: **github.com/Gnanasudharsan/tidewater**
