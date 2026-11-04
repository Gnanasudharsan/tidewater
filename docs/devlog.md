# Dev log

Bugs hit, root causes, and fixes. Newest first.

## 2026-11-03: Week 10, packaging
- **The first `make demo` claimed success it hadn't earned.** Its reconciliation was
  OOM-killed after two tables (exit 137): one-off jobs reuse the bronze service's
  memory limit, lowered to 1.5 GB in week 8, and memory piled up across 4.5M rows.
  Reconcile now releases its cache between tables, the limit is back to 2 GB, and the
  demo fails loudly unless reconciliation passes. The rerun passed end to end.
- **The intermittent CI stall finally left evidence.** The Week 7 diagnostics showed
  bronze's `_delta_log/00000000000000000000.*` committed at 04:04:55 while silver
  logged "waiting for bronze table" until 04:11:53: `DeltaTable.isDeltaTable` kept
  returning False for a table that existed. The trigger (Delta/S3A on SeaweedFS on
  the CI runner) is still not reproduced locally, so silver now waits for the
  concrete fact it needs: a committed `.json` in the transaction log, checked
  through the Hadoop FileSystem directly.
  Since the fix: 4 of 4 integration runs passed (the original run plus 3 reruns of
  run 41576893536). Before it, the stall hit 2 of about 6 runs. Encouraging, but at
  that failure rate 4 straight passes still happen ~20% of the time by luck: keep
  watching CI before calling it closed.
- **Follow-up (2026-11-04): 15 of 15 since the fix.** Two more batches of reruns
  (6 on run 41901339036, 5 on run 41908053104) all passed. If the old 1-in-3 failure
  rate were still there, 15 straight passes would happen about 0.2% of the time, so I
  now count the stall as fixed. The new logging also tested my second theory, that
  Hadoop's cached S3A client was returning stale listings. It didn't hold: silver
  saw the table after 40–60 s in every run (bronze's first commit really does take
  that long). In the 2 polls where both clients were asked, they agreed. No run
  logged a filesystem error. What's still unexplained is *why* `isDeltaTable` said
  False for 7 minutes in the original failure. The diagnostics stay in place, so if
  it ever comes back the log will say which client disagreed, or which error was
  being swallowed.
- The README now leads with measured results, including the misses (latency SLO at
  scale, the inconclusive tuning experiment), because those are what reviewers trust.

## 2026-11-02: Week 9, Snowflake first contact
- **Google sign-in means no password for Terraform.** Terraform logs in as a
  key-pair-only `TERRAFORM_ADMIN` service user instead, so there's no password anywhere.
- **The plan review caught a missing grant.** The loader owns the SILVER tables, and
  dbt's role could only `USAGE` the schema. A future `SELECT` grant fixed it before
  the first dbt run could fail.
- **`toPandas` + `write_pandas` died at scale** (exit 137) once benchmarks had grown
  order_items to 2.6M rows. The loader now writes Parquet with Spark, `PUT`s it to
  an internal stage and `COPY`s it: nothing is collected on the driver. Spark's
  default INT96 timestamps were switched to TIMESTAMP_MICROS for Snowflake.
- **Portability gap that SQL macros can't catch:** staging models select
  `_wh_loaded_at`, which only the Postgres loader stamped. The Snowflake MERGE now
  stamps it too. Afterwards dbt passed 81/81 on Snowflake in 33.5 s.
- **A key that worked stopped working (`JWT token is invalid`).** The Docker VM's
  clock was 5 minutes behind the Mac after a sleep; key-pair JWTs are time-bound.

## 2026-11-02: Week 9, benchmarks
- **The first sweep's "achieved rate" was wrong.** Parallel simulators raced: one
  deleted an order between another picking it and locking it (`fetchone()` was
  `None`), and FK races or deadlocks killed others before they logged their totals.
  The simulator now skips such events like a real client; 8 of them reach 1,999 ev/s.
- **"Silver is quiet" is not "silver is drained"** (again). The harness now
  compares rows silver has read against rows bronze ingested.
- **A restarted stream exports no counter until its first data batch.** The harness
  waited forever for a series that wasn't there. The listener now initialises its
  series at startup, and the harness uses `or vector(0)`.
- **The tuning experiment was inconclusive, and that is the result.** A/B/A runs of
  sequential vs concurrent per-table MERGEs: the two identical baselines differed
  2.5×, more than the effect. The change ships behind a flag, off by default. A
  JVM thread dump (`kill -3`) proved a suspected hang was just an idle stream.
- **Real finding:** silver latency tracks table size, not load. A 193-row batch takes
  44 s because MERGE into unpartitioned tables rewrites large files. That is the next
  tuning target (deletion vectors, partitioning).

## 2026-11-02: Week 8, observability, chaos suite, schema archive
- **Schema archive closed the incident's open risk.** The recovery output shows
  why it mattered: ID 9 was `orders` before the incident and `order_items` after.
  Chaos 05 then rebuilt silver from both bronze runs with 0 quarantined rows. (ADR 0010.)
- **Chaos 03 found a real CDC gap.** `ADD COLUMN ... DEFAULT` is metadata-only in
  Postgres 11+, so existing rows never emit WAL events: 12,924 customers kept NULL
  in silver while the source showed `'standard'`. The procedure is now
  "ALTER, then backfill the table", and the test asserts it.
- **Concurrent writers on an unpartitioned table livelock.** Under chaos 06 the
  stream crashed 4 times on `ConcurrentAppendException`: a silver MERGE takes about
  10 s, the backfill commits every few seconds, and Delta must treat every MERGE as
  reading every file. Fixes: the stream retries up to 50 times (backoff capped at
  30 s), the backfill pauses between chunks, and all six Delta conflict exceptions
  are retryable (a concurrent schema evolution raises `MetadataChangedException`,
  which my "Concurrent*" check missed). Real fix, future work: partition silver
  so writers touch disjoint files.
- **Alerting on a dead process: its series vanishes.** "Time since last progress"
  never fired when silver was stopped, because Prometheus stopped scraping it and
  the series disappeared. The rule now also fires on `up == 0`.
- **The loader would have silently skipped data after a rebuild.** Bookmarks are
  Delta versions, which restart at 0 for a recreated table. Bookmarks now store the
  Delta table id and force a full reload when it changes.
- **"Quiet logs" is not "replay finished".** Replay batches ran for minutes without
  logging, so chaos 05 started its repair backfill mid-replay and the two fought for
  an hour. It now waits for Prometheus to show 0 rows read for 2 minutes.
- **Laptop limits:** with every service up, the 8 GB Docker VM runs out of headroom
  (object store at 1,012/1,024 MiB, silver at 1.94/2 GB). The object store now gets
  1.5 GB and bronze gives back 0.5 GB. More Docker memory is the real fix.

## 2026-11-02: Week 7, orchestration, CI/CD, Terraform
- **Compaction fixed the small-files problem streaming created:** bronze went
  from 2,110 files to 15 (`jobs/maintenance.py`, run nightly by Airflow).
- **Backfill params go into SQL, so they are schema-validated.** The table is an
  enum and the dates use `format: date`. Triggering with
  `"2026-11-01' or 1=1 --"` is rejected by Airflow before any task runs.
- **dbt Slim CI needs data, not a prod database.** CI seeds a throwaway Postgres
  with a 70 KB fixture: 6 orders per status, 3 deleted orders, and all their
  lines, payments and customers (`scripts/make_ci_fixture.sh`). On PRs it builds
  `+state:modified+` against main's manifest artifact.
- **Ruff sorted `import airflow` as first-party**, because the repo has an
  `airflow/` folder. Fixed with `known-third-party`.
- **Shell default-value gotcha:** `${2:-{\}}` expands to `{\}`, not `{}`, which
  broke `airflow dags trigger --conf`. Fixed with an explicit fallback.
- **Integration test passed locally, failed in CI: the bind-mount trap.** Compose
  mounts `./dbt` over the image's baked-in project; a fresh checkout has no
  `dbt_packages/`, so dbt couldn't find `dbt_utils`. It only worked locally
  because my folder already had packages installed. Reproduced by deleting the
  local `dbt_packages/`, then fixed by running `dbt deps` first.
- **Open issue: intermittent silver stall in CI (1 of 3 runs).** Silver logged
  "waiting for bronze table" for 8 minutes while bronze had already committed
  1,960 rows. I tested the obvious theory, that `DeltaTable.isDeltaTable` caches
  a negative result, with two containers (one polling, one creating the table,
  including under a brand-new top-level prefix): it picks up the new table within
  one poll, so that theory is wrong. Not reproduced locally. CI now captures the
  bronze `_delta_log` listing, the lakehouse listing and full service logs on
  failure, so the next occurrence will show whether the table really existed.
- **Terraform against SeaweedFS works** with the stock AWS provider (path-style,
  skip_* flags). The existing bucket is adopted with an `import` block, and a
  re-plan shows no changes.

## 2026-11-01: Week 6, dimensional model, metrics, dashboard
- **Incident: Kafka lost every topic when its container was recreated.** The
  volume was never used (`KAFKA_LOG_DIRS` unset). The pipeline stopped itself, was
  recovered to 100% reconciliation, and got a postmortem:
  [incidents/2026-11-01-kafka-log-loss.md](incidents/2026-11-01-kafka-log-loss.md).
- **Object store OOM-killed at 512 MB with no restart policy**, which froze every
  Spark job at once (0% CPU everywhere was the clue). Now 1 GB, leveldb volume
  index, and every long-running service restarts on failure.
- **Ruff "fixed" code into a crash.** Lint targeted Python 3.11, so UP017 rewrote
  `datetime.timezone.utc` as `datetime.UTC`, which doesn't exist in the Spark
  image's Python 3.10. The lint target now matches the runtime.
- **A latency metric read from an evicted cache.** `persist()` is a hint: after
  the MERGE, Spark recomputed `to_merge`, whose filter ("newer than silver?") now
  matched nothing, and the metric code crashed unpacking `None`. Metrics are now
  computed before the merge.
- **An SCD2 test was wrong, not the model.** "Exactly one current version per
  customer" failed for 55 customers deleted at the source: the snapshot closes them
  out, as it should. The test now says "at most one, and exactly one if live".
- **`dbt build` skips downstream models when a test fails**, so the next
  incremental run had a stale high-water mark and reprocessed more than expected.
  Correct, just surprising.
- Delta's expired-log cleanup throws `MatchError` on SeaweedFS (`_delta_log/_commits`
  is reported as a file). It is caught and logged as a WARN; checkpoints still work.
- Known limitation: dbt snapshots record a customer's state at snapshot time, so
  two address changes between runs collapse into one version. A CDC-native SCD2
  built from silver's change feed would keep every version (future work).

## 2026-11-01: Week 5, warehouse load and dbt staging
- **Avoided a classic incremental-load bug by design.** A watermark on
  `_silver_updated_at` can skip rows when two writers (stream and backfill)
  commit out of timestamp order. The loader bookmarks Delta versions and reads the
  Change Data Feed instead. (ADR 0008.)
- **A test caught a fallback that could never run.** When the CDF range is
  unavailable, the loader is supposed to fall back to a full load. But Spark
  plans lazily, so the "change data not recorded" error was raised later, at
  `count()`, outside the `try`. The loader would have crashed instead.
  Fix: resolve the range inside the `try` with `limit(1).collect()`.
- **"Missing" warehouse rows were only lag.** Right after a burst, the warehouse
  had ~2% fewer live rows than the source. Reconciliation showed silver at 100%,
  and the missing keys were all created in the last seconds of the burst, so
  silver hadn't merged them yet when the loader ran. The next run matched exactly.
  Freshness thresholds in dbt now watch for exactly this lag.
- Port 5433 was taken by another local Postgres, so the warehouse host port is
  now configurable (`WH_PG_HOST_PORT`, default 5434).

## 2026-11-01: Weeks 3–4, silver layer
- **The soak test didn't finish: the laptop kept sleeping.** A 10-minute soak
  stretched over 8 hours of wall time as macOS slept and froze the Docker VM
  (simulator logs show gaps like 05:27 → 06:11 UTC). Every component resumed
  cleanly after each freeze, and the concurrent backfill got through its
  transaction-horizon wait and merged `customers` with 0 conflicts, but the run
  never reached reconciliation. Its latency numbers would be meaningless anyway.
  To do: rerun `make soak DURATION=3600` on a machine that stays awake.
- **Silver crash-looped with `OutOfMemoryError` inside Avro's `readString`, on a
  77k-event backlog.** Every bronze record decoded fine on its own with FAILFAST,
  so the data was not the problem. Cause: decoding was written as
  `filter(schema_id == 9)` and then `from_avro(...)`, followed by a filter on a
  decoded field. Spark combined the two filters and did not evaluate the schema
  check first, so `from_avro` ran on *other tables'* bytes with the orders schema,
  read a garbage string length, and tried to allocate gigabytes. Single-table
  probes never showed it because no foreign bytes were present. Fix: put the
  guard inside the expression, `CASE WHEN schema_id = 9 THEN from_avro(...)`,
  which Spark does evaluate lazily. Lesson: a filter is not a guard in Spark.
- **The Avro envelope references the Debezium `Source` record by name** (Apicurio
  stores it as its own artifact, a schema reference). `from_avro` needs one
  self-contained schema, so the registry client fetches references and inlines
  the first use of each type.
- **The backfill LSN stamp needs a transaction horizon.** Capturing
  `pg_current_wal_lsn()` and reading is not enough: a transaction that wrote a
  change below that LSN but had not committed yet would be invisible to the read,
  and its stream event would then lose to the backfill's stamp. The backfill
  captures the LSN, then the next xid, and waits until every older transaction has
  finished. (ADR 0003.)
- **No watermark in silver.** A watermark drops late data, which contradicts the
  100% reconciliation SLO. Late data is now detected exactly, by LSN, and
  recorded. (ADR 0007.)

## 2026-10-31: Week 2, bronze layer
- **Checkpoint writes failed with `ParentNotDirectoryException: parent is not a dir`.**
  A probe showed that after `mkdirs("dir1")`, S3A's `getFileStatus("dir1")` reports
  `isFile=true` on SeaweedFS (AWS S3 returns 404 for the bare key, so S3A falls back
  to a directory listing). Plain `FileSystem.create` and Delta writes worked; only
  Spark's default FileContext-based checkpoint manager checks the parent and fails.
  Fix: `spark.sql.streaming.checkpointFileManagerClass =
  FileSystemBasedCheckpointFileManager`, which also works on AWS S3.
- **The chaos test reported "crash never triggered" while the logs showed it had.**
  `docker compose logs | grep -q` under `set -o pipefail`: `grep -q` exits on the
  first match, `compose logs` dies of SIGPIPE, and the pipeline counts as failed.
  Fix: use `grep -c`, which reads all of its input.
- **Random SIGKILLs alone don't prove the idempotent write matters.** The dangerous
  window is after the Delta commit and before the checkpoint commit, and random
  kills rarely land in it. Added a chaos hook (`BRONZE_CHAOS_CRASH_AFTER_COMMIT=1`)
  that exits exactly there. The restarted stream replays that batch id, and the
  job logs that Delta skipped it via `txnAppId`/`txnVersion`.
- **Gotcha to remember:** `txnAppId` must change whenever the checkpoint is reset.
  Batch ids restart at 0, and Delta would silently skip them as already committed.
  The checkpoint path is derived from `BRONZE_TXN_APP_ID`, so bumping it resets both.
- `ENV PYTHONPATH=/opt/tidewater` in the Spark Dockerfile replaced the base image's
  path, so `import pyspark` failed outside `spark-submit`. Added `/opt/spark/python`
  and the py4j zip back.

## 2026-10-31: Week 0/1 scaffolding
- **`docker pull minio/minio` failed: "repository does not exist".** MinIO no
  longer publishes free images on Docker Hub or Quay. Switched to SeaweedFS's
  S3 gateway (ADR 0006). Second bug on the way: the SeaweedFS image runs as
  non-root, so writing the S3 identity file to `/etc` failed with
  "Permission denied" and the S3 server crashed on a missing config. Fixed by
  writing it to `/tmp`.
- **Kafka refused to start with `CLUSTER_ID: tidewater-kraft-cluster-01`.**
  KRaft needs the base64 encoding of exactly 16 bytes (22 chars).
- Chose Apicurio Registry with **KafkaSQL storage** instead of the in-memory
  image. With in-memory storage a registry restart reassigns schema IDs, and
  every Avro record already in bronze would point at an ID that no longer exists.
- Debezium writes Avro in the **Confluent wire format** (`as-confluent=true`)
  so Spark can strip the 5-byte header and use `from_avro`, and Kafka UI can
  decode messages through the registry's `ccompat` API.
