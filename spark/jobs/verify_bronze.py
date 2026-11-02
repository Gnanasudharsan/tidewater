"""Prove bronze holds every Kafka offset exactly once: no duplicates, no gaps.

    spark-submit jobs/verify_bronze.py [--require-caught-up]

Exit code 0 means every check passed; 1 means at least one fatal finding.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from tidewater_spark.config import Settings
from tidewater_spark.offsets import BronzeStats, KafkaRange, check_partitions
from tidewater_spark.session import build_spark


def current_run(spark: SparkSession, path: str, run: str):
    """Rows from this ingestion run (rows written before runs were tagged count as v1)."""
    df = spark.read.format("delta").load(path)
    tag = F.col("_ingest_run") if "_ingest_run" in df.columns else F.lit(None)
    return df.where(F.coalesce(tag, F.lit("bronze_cdc_v1")) == run)


def bronze_stats(spark: SparkSession, path: str, run: str) -> dict:
    rows = (
        current_run(spark, path, run)
        .groupBy("_topic", "_partition")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("_offset").alias("distinct_offsets"),
            F.min("_offset").alias("min_offset"),
            F.max("_offset").alias("max_offset"),
        )
        .collect()
    )
    return {
        (r["_topic"], r["_partition"]): BronzeStats(
            r["rows"], r["distinct_offsets"], r["min_offset"], r["max_offset"]
        )
        for r in rows
    }


def kafka_ranges(spark: SparkSession, settings: Settings) -> dict:
    rows = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribePattern", settings.cdc_topic_pattern)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .groupBy("topic", "partition")
        .agg(F.min("offset").alias("earliest"), F.max("offset").alias("latest"))
        .collect()
    )
    return {(r["topic"], r["partition"]): KafkaRange(r["earliest"], r["latest"]) for r in rows}


def show_gaps(spark: SparkSession, path: str, run: str, limit: int = 20) -> None:
    w = Window.partitionBy("_topic", "_partition").orderBy("_offset")
    (
        current_run(spark, path, run)
        .select("_topic", "_partition", "_offset")
        .distinct()
        .withColumn("prev", F.lag("_offset").over(w))
        .where(F.col("_offset") - F.col("prev") > 1)
        .select(
            "_topic",
            "_partition",
            (F.col("prev") + 1).alias("gap_from"),
            (F.col("_offset") - 1).alias("gap_to"),
        )
        .show(limit, truncate=False)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-caught-up",
        action="store_true",
        help="also fail if bronze has not ingested up to Kafka's latest offset",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    spark = build_spark("tidewater-verify-bronze", settings)
    bronze = bronze_stats(spark, settings.bronze_path, settings.bronze_txn_app_id)
    kafka = kafka_ranges(spark, settings)
    findings = check_partitions(bronze, kafka, args.require_caught_up)

    total_rows = sum(s.rows for s in bronze.values())
    run = settings.bronze_txn_app_id
    print(f"\nbronze run {run}: {total_rows} rows across {len(bronze)} topic-partitions")
    print(f"{'topic':<32}{'part':>5}{'rows':>9}{'min':>9}{'max':>9}{'kafka_max':>11}")
    for tp in sorted(set(bronze) | set(kafka)):
        b, k = bronze.get(tp), kafka.get(tp)
        print(
            f"{tp[0]:<32}{tp[1]:>5}{b.rows if b else 0:>9}"
            f"{b.min_offset if b else '-':>9}{b.max_offset if b else '-':>9}"
            f"{k.latest if k else '-':>11}"
        )

    for f in findings:
        level = "FAIL" if f.fatal else "WARN"
        print(f"[{level}] {f.partition[0]}[{f.partition[1]}] {f.kind}: {f.detail}")
    if any(f.kind == "gap" for f in findings):
        show_gaps(spark, settings.bronze_path, settings.bronze_txn_app_id)

    failed = any(f.fatal for f in findings)
    print("\nRESULT:", "FAIL" if failed else "PASS (no duplicates, no gaps)")
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
