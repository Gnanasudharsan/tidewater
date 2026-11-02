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

from tidewater_spark.bronze import BRONZE_PARTITION_COLUMNS, to_bronze
from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark

log = logging.getLogger("bronze_ingest")


def table_version(spark: SparkSession, path: str) -> int | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    spark = build_spark("tidewater-bronze-ingest", settings)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        before = table_version(spark, settings.bronze_path)
        (
            to_bronze(batch_df, batch_id, settings.bronze_txn_app_id)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("txnAppId", settings.bronze_txn_app_id)
            .option("txnVersion", batch_id)
            .partitionBy(*BRONZE_PARTITION_COLUMNS)
            .save(settings.bronze_path)
        )
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
