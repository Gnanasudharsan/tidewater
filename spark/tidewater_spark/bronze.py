"""Bronze layer: raw Kafka records plus the lineage columns needed to prove completeness."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BRONZE_PARTITION_COLUMNS = ["source_table", "ingest_date"]


def confluent_schema_id(value: Column) -> Column:
    """Schema id from the Confluent wire format: magic byte 0x00 + 4-byte big-endian id.

    Returns null for anything that is not in that format (null values, malformed
    events), so bronze never drops or fails on a bad record; silver quarantines it.
    """
    is_wire_format = (F.length(value) >= 5) & (F.hex(F.substring(value, 1, 1)) == F.lit("00"))
    return F.when(is_wire_format, F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int"))


def to_bronze(kafka_df: DataFrame, batch_id: int, ingest_run: str = "bronze_cdc_v1") -> DataFrame:
    """Map a micro-batch from the Kafka source to the bronze schema. Payloads stay as raw bytes.

    `ingest_run` identifies the checkpoint lineage. Kafka offsets are only unique within
    one run: after a Kafka rebuild, offsets restart at 0 under a new run.
    """
    return kafka_df.select(
        F.col("key"),
        F.col("value"),
        F.col("topic").alias("_topic"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        confluent_schema_id(F.col("value")).alias("_value_schema_id"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(batch_id).cast("long").alias("_batch_id"),
        F.lit(ingest_run).alias("_ingest_run"),
        # tidewater.public.orders -> orders
        F.element_at(F.split(F.col("topic"), r"\."), -1).alias("source_table"),
    ).withColumn("ingest_date", F.to_date("_ingested_at"))
