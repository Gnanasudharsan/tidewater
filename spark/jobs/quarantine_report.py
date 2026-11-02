"""Print quarantine counts by table and reason since a timestamp (chaos-test helper).

    spark-submit jobs/quarantine_report.py --since '2026-11-02 01:00:00'
"""

import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark

parser = argparse.ArgumentParser()
parser.add_argument("--since", required=True, help="UTC timestamp")
args = parser.parse_args()
settings = Settings.from_env()
spark = build_spark("tidewater-quarantine-report", settings)
if DeltaTable.isDeltaTable(spark, settings.quarantine_path):
    rows = (
        spark.read.format("delta")
        .load(settings.quarantine_path)
        .where(F.col("_recorded_at") >= F.lit(args.since).cast("timestamp"))
        .select("source_table", F.explode("reasons").alias("reason"))
        .groupBy("source_table", "reason")
        .count()
        .orderBy("source_table", "reason")
        .collect()
    )
    for r in rows:
        print(f"QUARANTINE {r.source_table} {r.reason} {r['count']}")
