"""Stream every CDC topic from Kafka into one append-only bronze Delta table.

Exactly-once into bronze comes from two pieces working together:
  1. The streaming checkpoint records which Kafka offsets each batch id covers,
     so after a crash the same batch id is replayed with the same offsets.
  2. Delta idempotent writes (txnAppId + txnVersion=batch_id): if that batch had
     already committed to Delta before the crash, the replayed write is skipped.
"""

from __future__ import annotations

import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from tidewater_spark import lineage, metrics
from tidewater_spark.bronze import BRONZE_PARTITION_COLUMNS, to_bronze
from tidewater_spark.config import Settings
from tidewater_spark.registry import SchemaRegistry
from tidewater_spark.schema_archive import SchemaArchive
from tidewater_spark.session import build_spark
from tidewater_spark.tables import load_contracts

log = logging.getLogger("bronze_ingest")


def table_version(spark: SparkSession, path: str) -> int | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    spark = build_spark("tidewater-bronze-ingest", settings)
    metrics.watch(spark, "bronze")
    lineage.emit(
        settings,
        "bronze_ingest",
        [lineage.kafka(f"tidewater.public.{t}") for t in sorted(load_contracts())],
        [lineage.lake(settings, settings.bronze_path)],
    )
    registry = SchemaRegistry(settings.schema_registry_url)
    archive = SchemaArchive(spark, settings.schema_archive_path)

    def archive_schemas(rows: DataFrame) -> None:
        """Record writer schemas for IDs this run has not archived yet. Best effort:
        if the registry is unreachable, the next batch retries."""
        ids = {r[0] for r in rows.select("_value_schema_id").distinct().collect() if r[0]}
        try:
            fetched = {sid: registry.schema_json(sid) for sid in ids}
            if added := archive.archive(settings.bronze_txn_app_id, fetched):
                log.info(
                    "archived %d new writer schema(s) for %s", added, settings.bronze_txn_app_id
                )
        except Exception as exc:  # noqa: BLE001 - never fail ingestion over the archive
            log.warning("schema archiving deferred: %s", exc)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        before = table_version(spark, settings.bronze_path)
        rows = to_bronze(batch_df, batch_id, settings.bronze_txn_app_id).persist()
        (
            rows.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("txnAppId", settings.bronze_txn_app_id)
            .option("txnVersion", batch_id)
            .partitionBy(*BRONZE_PARTITION_COLUMNS)
            .save(settings.bronze_path)
        )
        archive_schemas(rows)
        rows.unpersist()
        if before is not None and table_version(spark, settings.bronze_path) == before:
            log.warning(
                "batch %s was already committed to Delta before a crash; "
                "idempotent write (txnVersion) skipped the replay",
                batch_id,
            )
        elif settings.chaos_crash_after_commit and not batch_df.isEmpty():
            log.error("CHAOS: crashing after Delta commit of batch %s, before checkpoint", batch_id)
            os._exit(1)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribePattern", settings.cdc_topic_pattern)
        .option("startingOffsets", "earliest")
        # Fail loudly if Kafka retention deleted offsets we never read.
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", settings.bronze_max_offsets_per_trigger)
        .load()
    )

    query = (
        source.writeStream.queryName("bronze_cdc")
        .foreachBatch(write_batch)
        .option("checkpointLocation", settings.bronze_checkpoint)
        .trigger(processingTime=settings.bronze_trigger)
        .start()
    )
    log.info("bronze stream started: %s -> %s", settings.cdc_topic_pattern, settings.bronze_path)

    while not query.awaitTermination(timeout=30):
        progress = query.lastProgress
        if progress:
            log.info(
                "batch=%s rows=%s input/s=%.1f processed/s=%.1f duration_ms=%s",
                progress["batchId"],
                progress["numInputRows"],
                progress.get("inputRowsPerSecond") or 0.0,
                progress.get("processedRowsPerSecond") or 0.0,
                progress["durationMs"].get("triggerExecution"),
            )


if __name__ == "__main__":
    main()
