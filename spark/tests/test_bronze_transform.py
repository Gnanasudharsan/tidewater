"""Spark-level tests for the bronze transform. Run with `make test-spark` (needs Java)."""

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from tidewater_spark.bronze import to_bronze

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def kafka_row(value, topic="tidewater.public.orders", offset=0):
    return (b"k", value, topic, 0, offset, dt.datetime(2026, 10, 31, 12, 0))


def test_lineage_columns_and_source_table(spark):
    wire = bytes([0, 0, 0, 0, 9]) + b"avro-payload"
    df = spark.createDataFrame([kafka_row(wire, offset=42)], KAFKA_SCHEMA)
    row = to_bronze(df, batch_id=7).collect()[0]

    assert row["_topic"] == "tidewater.public.orders"
    assert row["_partition"] == 0
    assert row["_offset"] == 42
    assert row["_batch_id"] == 7
    assert row["source_table"] == "orders"
    assert row["_value_schema_id"] == 9
    assert bytes(row["value"]) == wire  # payload kept byte-for-byte
    assert row["ingest_date"] == row["_ingested_at"].date()


@pytest.mark.parametrize(
    "value",
    [None, b"", b"\x00\x00", b"{not avro}", bytes([1, 0, 0, 0, 9]) + b"x"],
    ids=["null", "empty", "short", "json", "wrong-magic"],
)
def test_malformed_values_are_kept_with_null_schema_id(spark, value):
    df = spark.createDataFrame([kafka_row(value)], KAFKA_SCHEMA)
    rows = to_bronze(df, batch_id=0).collect()
    assert len(rows) == 1
    assert rows[0]["_value_schema_id"] is None


def test_large_schema_ids_decode_big_endian(spark):
    wire = bytes([0]) + (70000).to_bytes(4, "big") + b"x"
    df = spark.createDataFrame([kafka_row(wire)], KAFKA_SCHEMA)
    assert to_bronze(df, 0).collect()[0]["_value_schema_id"] == 70000
