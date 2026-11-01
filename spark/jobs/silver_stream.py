"""Bronze -> silver: decode, validate, dedupe, and MERGE every CDC change into per-table Delta.

Per micro-batch, in this order (the order is what makes crash recovery safe):
  1. decode each table's Avro envelopes; undecodable records -> quarantine
  2. apply the table's DQ rules; failing rows -> quarantine
  3. keep the newest change per primary key (by _lsn, _source_offset)
  4. split out late events (older than what silver holds) -> _late_arrivals
  5. append quarantine and late rows, each as an idempotent Delta write (txnVersion = batch id)
  6. MERGE the rest with the LSN guard
A crash anywhere replays the batch: steps 5 are skipped by Delta, step 6 is a no-op
for rows already merged.
"""

from __future__ import annotations

import datetime as dt
import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tidewater_spark.cdc import LINEAGE, business_columns, decode_table, silver_columns
from tidewater_spark.config import Settings
from tidewater_spark.dq import DQ_REASONS, split_by_rules
from tidewater_spark.merge import classify_against_target, latest_per_key, merge_into_silver
from tidewater_spark.registry import SchemaRegistry
from tidewater_spark.session import build_spark
from tidewater_spark.tables import TableContract, load_contracts

log = logging.getLogger("silver_stream")


def quarantine_rows(df: DataFrame, table: str, record_columns: list[str] | None) -> DataFrame:
    record = F.to_json(F.struct(*record_columns)) if record_columns else F.lit(None)
    return df.select(
        F.lit(table).alias("source_table"),
        F.col(DQ_REASONS).alias("reasons"),
        record.cast("string").alias("record_json"),
        *LINEAGE,
    )


def late_rows(df: DataFrame, table: str, pk: tuple[str, ...]) -> DataFrame:
    return df.select(
        F.lit(table).alias("source_table"),
        F.to_json(F.struct(*pk)).alias("pk_json"),
        "_op",
        "_lsn",
        "_target_lsn",
        "_source_ts",
        "_target_source_ts",
        "_source_topic",
        "_source_partition",
        "_source_offset",
    )


def append_idempotent(df: DataFrame, path: str, app_id: str, batch_id: int) -> int:
    rows = df.count()
    if rows:
        (
            df.withColumn("_batch_id", F.lit(batch_id).cast("long"))
            .withColumn("_recorded_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", batch_id)
            .option("mergeSchema", "true")
            .partitionBy("source_table")
            .save(path)
        )
    return rows


def process_batch(
    spark: SparkSession,
    settings: Settings,
    contracts: dict[str, TableContract],
    registry: SchemaRegistry,
    bronze_batch: DataFrame,
    batch_id: int,
) -> dict:
    started = time.monotonic()
    bronze_batch = bronze_batch.persist()
    tables = sorted(r[0] for r in bronze_batch.select("source_table").distinct().collect())
    quarantined, late, pending, stats = [], [], [], {"batch": batch_id}

    for table in tables:
        contract = contracts.get(table)
        if contract is None:
            log.warning("no contract for table %s; leaving its events in bronze only", table)
            continue
        changes, undecodable = decode_table(
            bronze_batch.where(F.col("source_table") == table), registry
        )
        if undecodable is not None:
            quarantined.append(quarantine_rows(undecodable, table, None))
        if changes is None:
            continue
        good, failed = split_by_rules(changes, contract.rules)
        quarantined.append(quarantine_rows(failed, table, business_columns(failed)))
        deduped = latest_per_key(good.select(*silver_columns(good)), contract.primary_key)
        to_merge, too_old = classify_against_target(
            spark, settings.silver_path(table), deduped, contract.primary_key
        )
        late.append(late_rows(too_old, table, contract.primary_key))
        pending.append((contract, to_merge.persist()))

    def union(frames):
        out = None
        for f in frames:
            out = f if out is None else out.unionByName(f, allowMissingColumns=True)
        return out

    app = settings.silver_txn_app_id
    if quarantined:
        stats["quarantined"] = append_idempotent(
            union(quarantined), settings.quarantine_path, f"{app}_quarantine", batch_id
        )
    if late:
        stats["late"] = append_idempotent(
            union(late), settings.late_arrivals_path, f"{app}_late", batch_id
        )

    merged_frames = []
    for contract, to_merge in pending:
        n = to_merge.count()
        if n:
            attempts = merge_into_silver(
                spark,
                settings.silver_path(contract.name),
                to_merge,
                contract.primary_key,
                what=f"silver.{contract.name}",
            )
            stats[contract.name] = n if attempts == 1 else f"{n} ({attempts} attempts)"
            merged_frames.append(to_merge.select("_source_ts"))

    # Source-commit -> silver-commit latency for rows merged in this batch.
    if merged_frames:
        committed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        lag_s = (F.unix_micros(F.lit(committed_at)) - F.unix_micros("_source_ts")) / 1e6
        p50, p95 = (
            union(merged_frames)
            .select(F.percentile_approx(lag_s, [0.5, 0.95]).alias("p"))
            .first()["p"]
        )
        stats["latency_p50_s"], stats["latency_p95_s"] = round(p50, 1), round(p95, 1)

    for _, df in pending:
        df.unpersist()
    bronze_batch.unpersist()
    stats["duration_s"] = round(time.monotonic() - started, 1)
    log.info("silver batch %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    contracts = load_contracts()
    registry = SchemaRegistry(settings.schema_registry_url)
    spark = build_spark("tidewater-silver-stream", settings)

    while not DeltaTable.isDeltaTable(spark, settings.bronze_path):
        log.info("waiting for bronze table at %s", settings.bronze_path)
        time.sleep(10)

    query = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 1000)
        .load(settings.bronze_path)
        .writeStream.queryName("silver")
        .foreachBatch(
            lambda df, batch_id: process_batch(spark, settings, contracts, registry, df, batch_id)
        )
        .option("checkpointLocation", settings.silver_checkpoint)
        .trigger(processingTime=settings.silver_trigger)
        .start()
    )
    log.info("silver stream started for tables %s", sorted(contracts))
    query.awaitTermination()


if __name__ == "__main__":
    main()
