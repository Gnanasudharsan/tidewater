"""Backfill silver straight from the source database, safely alongside the live stream.

    spark-submit jobs/backfill.py --table orders [--where "updated_at >= '2026-10-01'"]
                                 [--detect-deletes] [--chunk-size 5000]

Correctness argument (see ADR 0003):
  1. Capture L0 = pg_current_wal_lsn(), THEN X = the next unassigned transaction id.
  2. Wait until every transaction with id < X has finished. Any change with
     LSN < L0 was written by such a transaction, so it is now committed or aborted.
  3. Read the table. Every row reflects all changes with LSN < L0 (and maybe some
     later ones). Stamp every row with _lsn = L0, _source_offset = -1.
  4. MERGE with the same LSN-guarded function the stream uses. A streamed change
     newer than L0 always wins; the backfill never overwrites newer data. A row
     the backfill read "too new" is simply re-applied by its stream event.
Chunks are small so each MERGE is short, and Delta write conflicts with the
stream are retried (safe because the merge is idempotent).
"""

from __future__ import annotations

import argparse
import logging
import time

import psycopg
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tidewater_spark.cdc import business_columns
from tidewater_spark.config import Settings
from tidewater_spark.dq import DQ_REASONS, split_by_rules
from tidewater_spark.merge import merge_into_silver
from tidewater_spark.session import build_spark
from tidewater_spark.tables import TableContract, load_contracts

log = logging.getLogger("backfill")


def consistent_lsn(settings: Settings, timeout_s: float = 300) -> int:
    with psycopg.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_db,
        user=settings.pg_user,
        password=settings.pg_password,
        autocommit=True,
    ) as conn:
        # Order matters: LSN first, then the xid horizon (separate statements).
        lsn = conn.execute("select (pg_current_wal_lsn() - '0/0')::bigint").fetchone()[0]
        xmax = conn.execute("select txid_snapshot_xmax(txid_current_snapshot())").fetchone()[0]
        deadline = time.monotonic() + timeout_s
        while True:
            xmin = conn.execute("select txid_snapshot_xmin(txid_current_snapshot())").fetchone()[0]
            if xmin >= xmax:
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"transactions older than xid {xmax} still running")
            time.sleep(0.2)
    log.info("consistent LSN %s (all transactions below xid %s finished)", lsn, xmax)
    return lsn


def read_source(
    spark: SparkSession, settings: Settings, table: str, pk: str, where: str | None
) -> DataFrame:
    predicate = f"where {where}" if where else ""
    bounds = spark.read.jdbc(
        settings.jdbc_url,
        f"(select min({pk}) lo, max({pk}) hi from public.{table} {predicate}) b",
        properties=settings.jdbc_props,
    ).first()
    if bounds["lo"] is None:
        return None
    return spark.read.jdbc(
        settings.jdbc_url,
        f"(select * from public.{table} {predicate}) src",
        column=pk,
        lowerBound=int(bounds["lo"]),
        upperBound=int(bounds["hi"]) + 1,
        numPartitions=4,
        properties=settings.jdbc_props,
    )


def stamp(df: DataFrame, lsn: int, op: str, deleted: bool) -> DataFrame:
    now = F.current_timestamp()
    return df.select(
        *business_columns(df),
        F.lit(deleted).alias("is_deleted"),
        (now if deleted else F.lit(None).cast("timestamp")).alias("deleted_at"),
        F.lit(op).alias("_op"),
        F.lit(lsn).cast("long").alias("_lsn"),
        F.lit(None).cast("long").alias("_tx_id"),
        now.alias("_source_ts"),
        F.lit("backfill").alias("_source_topic"),
        F.lit(-1).cast("int").alias("_source_partition"),
        F.lit(-1).cast("long").alias("_source_offset"),
        F.lit(None).cast("int").alias("_schema_id"),
    )


def backfill_table(
    spark: SparkSession,
    settings: Settings,
    contract: TableContract,
    where: str | None,
    detect_deletes: bool,
    chunk_size: int,
    pause_s: float = 2.0,
) -> None:
    if len(contract.primary_key) != 1:
        raise ValueError("chunked backfill needs a single-column primary key")
    pk = contract.primary_key[0]
    path = settings.silver_path(contract.name)
    lsn = consistent_lsn(settings)

    source = read_source(spark, settings, contract.name, pk, where)
    if source is None:
        log.info("%s: no source rows matched", contract.name)
        return
    stamped = stamp(source, lsn, "backfill", deleted=False)
    good, failed = split_by_rules(stamped, contract.rules)
    bad = failed.count()
    if bad:
        log.warning("%s: %d rows failed DQ; written to quarantine", contract.name, bad)
        failed.select(
            F.lit(contract.name).alias("source_table"),
            F.col(DQ_REASONS).alias("reasons"),
            F.to_json(F.struct(*business_columns(failed))).alias("record_json"),
            F.lit(-1).cast("long").alias("_batch_id"),
            F.current_timestamp().alias("_recorded_at"),
        ).write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(
            "source_table"
        ).save(
            settings.quarantine_path
        )

    changes = good
    if detect_deletes and DeltaTable.isDeltaTable(spark, path):
        if where:
            raise ValueError("--detect-deletes needs a full-table backfill (no --where)")
        silver_live = (
            spark.read.format("delta")
            .load(path)
            .where(~F.col("is_deleted") & (F.col("_lsn") < lsn))
        )
        gone = silver_live.join(source.select(pk), pk, "left_anti")
        changes = changes.unionByName(stamp(gone, lsn, "backfill_delete", deleted=True))

    changes = changes.persist()
    lo, hi, total = changes.select(F.min(pk), F.max(pk), F.count("*")).first()
    log.info("%s: %d rows stamped with LSN %s, pk %s..%s", contract.name, total, lsn, lo, hi)
    merged, retries = 0, 0
    for start in range(lo, hi + 1, chunk_size):
        chunk = changes.where((F.col(pk) >= start) & (F.col(pk) < start + chunk_size))
        attempts = merge_into_silver(
            spark,
            path,
            chunk,
            contract.primary_key,
            what=f"backfill.{contract.name}[{start}]",
            max_attempts=30,
        )
        retries += attempts - 1
        merged += 1
        # Leave a commit window for the live stream between chunks: both MERGE into
        # the same unpartitioned table, so every commit conflicts with the other.
        time.sleep(pause_s)
    changes.unpersist()
    log.info(
        "%s: backfill done, %d chunks merged, %d conflict retries", contract.name, merged, retries
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--table", action="append", help="table to backfill (repeatable); default all"
    )
    parser.add_argument("--where", help="SQL predicate to backfill a range, e.g. on updated_at")
    parser.add_argument("--detect-deletes", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    contracts = load_contracts()
    spark = build_spark("tidewater-backfill", settings)
    for name in args.table or sorted(contracts):
        backfill_table(
            spark, settings, contracts[name], args.where, args.detect_deletes, args.chunk_size
        )
    spark.stop()


if __name__ == "__main__":
    main()
