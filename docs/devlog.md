# Dev log

Bugs hit, root causes, and fixes. Newest first.

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
