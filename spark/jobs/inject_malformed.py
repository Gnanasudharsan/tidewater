"""Chaos helper: write deliberately malformed records to a CDC topic.

    spark-submit jobs/inject_malformed.py [--topic tidewater.public.orders]

Writes one record of each kind bronze/silver must survive without stopping:
  not_confluent_avro  plain JSON, no wire-format header
  not_confluent_avro  two stray bytes
  unknown_schema_id   valid header, schema id 999999
  undecodable         valid header with the topic's real schema id, garbage body
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="tidewater.public.orders")
    args = parser.parse_args()
    settings = Settings.from_env()
    spark = build_spark("tidewater-inject-malformed", settings)

    # The topic's current schema id, read from the newest bronze record of that topic.
    real_id = (
        spark.read.format("delta")
        .load(settings.bronze_path)
        .where(F.col("_topic") == args.topic)
        .where(F.col("_value_schema_id").isNotNull())
        .orderBy(F.col("_ingested_at").desc())
        .select("_value_schema_id")
        .first()[0]
    )
    header = lambda sid: bytes([0]) + int(sid).to_bytes(4, "big")  # noqa: E731
    records = [
        (b"chaos-1", b'{"order_id": 1, "status": "paid"}'),
        (b"chaos-2", b"\x07\x07"),
        (b"chaos-3", header(999999) + b"\x02\x04\x06"),
        (b"chaos-4", header(real_id) + b"\xff" * 64),
    ]
    spark.createDataFrame(records, "key binary, value binary").write.format("kafka").option(
        "kafka.bootstrap.servers", settings.kafka_bootstrap
    ).option("topic", args.topic).save()
    print(f"INJECTED {len(records)} malformed records into {args.topic} (real schema id {real_id})")


if __name__ == "__main__":
    main()
