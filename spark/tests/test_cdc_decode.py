"""Decode real Debezium-shaped Avro (built with to_avro) the way bronze stores it."""

import datetime as dt
import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

from tidewater_spark.cdc import decode_table
from tidewater_spark.registry import SchemaRegistry
from tidewater_spark.schema_archive import RegistryOnly

VALUE = {
    "type": "record",
    "name": "Value",
    "namespace": "tidewater.public.orders",
    "fields": [
        {"name": "order_id", "type": "long"},
        {"name": "status", "type": "string"},
        {
            "name": "order_total",
            "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2},
        },
        {
            "name": "created_at",
            "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"},
        },
    ],
}
SOURCE = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium.connector.postgresql",
    "fields": [
        {"name": "ts_ms", "type": "long"},
        {"name": "txId", "type": ["null", "long"]},
        {"name": "lsn", "type": ["null", "long"]},
    ],
}


def envelope(source_type):
    return {
        "type": "record",
        "name": "Envelope",
        "namespace": "tidewater.public.orders",
        "fields": [
            {"name": "before", "type": ["null", VALUE]},
            {"name": "after", "type": ["null", "Value"]},
            {"name": "source", "type": source_type},
            {"name": "op", "type": "string"},
        ],
    }


FULL = json.dumps(envelope(SOURCE))


def fake_registry():
    # The envelope refers to Source by name, exactly like Debezium + Apicurio do.
    docs = {
        "http://r/schemas/ids/7": {
            "schema": json.dumps(envelope("io.debezium.connector.postgresql.Source")),
            "references": [
                {"name": "io.debezium.connector.postgresql.Source", "subject": "src", "version": 1}
            ],
        },
        "http://r/subjects/src/versions/1": {"schema": json.dumps(SOURCE)},
    }
    return RegistryOnly(SchemaRegistry("http://r", fetch=lambda url: docs[url]))


def bronze_rows(spark, events, schema_id=7):
    row = "struct<order_id:bigint,status:string,order_total:decimal(12,2),created_at:string>"
    df = spark.createDataFrame(
        events,
        f"before {row}, after {row}, source struct<ts_ms:bigint,txId:bigint,lsn:bigint>, "
        "op string, _offset bigint",
    )
    header = bytes([0]) + schema_id.to_bytes(4, "big")
    return df.select(
        F.concat(F.lit(header), to_avro(F.struct("before", "after", "source", "op"), FULL)).alias(
            "value"
        ),
        F.lit("tidewater.public.orders").alias("_topic"),
        F.lit(0).alias("_partition"),
        "_offset",
        F.lit(schema_id).alias("_value_schema_id"),
    )


ROW = (1, "paid", Decimal("12.50"), "2026-10-31T20:03:43.460123Z")
SRC = (1793476000000, 501, 9000)


def test_update_is_decoded_typed_and_carries_lineage(spark):
    bronze = bronze_rows(spark, [(None, ROW, SRC, "u", 42)])
    changes, bad = decode_table(bronze, fake_registry())
    r = changes.first()
    assert (r.order_id, r.status, r.order_total) == (1, "paid", Decimal("12.50"))
    assert r.created_at == dt.datetime(2026, 10, 31, 20, 3, 43, 460123)  # cast to timestamp
    assert (r._op, r._lsn, r._tx_id, r._source_offset, r.is_deleted) == ("u", 9000, 501, 42, False)
    assert bad.count() == 0


def test_delete_uses_before_image(spark):
    bronze = bronze_rows(spark, [(ROW, None, SRC, "d", 43)])
    r = decode_table(bronze, fake_registry())[0].first()
    assert (r.order_id, r.status, r.is_deleted) == (1, "paid", True)
    assert r.deleted_at is not None


def test_garbage_and_unknown_schemas_are_quarantined(spark):
    good = bronze_rows(spark, [(None, ROW, SRC, "c", 1)])
    garbage = good.select(
        F.lit(bytes([0, 0, 0, 0, 7]) + b"\xff\xff not avro").alias("value"),
        "_topic",
        "_partition",
        F.lit(2).cast("long").alias("_offset"),
        "_value_schema_id",
    )
    unknown = bronze_rows(spark, [(None, ROW, SRC, "c", 3)], schema_id=99)
    plain = good.select(
        F.lit(b"{}").alias("value"),
        "_topic",
        "_partition",
        F.lit(4).cast("long").alias("_offset"),
        F.lit(None).cast("int").alias("_value_schema_id"),
    )
    changes, bad = decode_table(good.union(garbage).union(unknown).union(plain), fake_registry())
    assert changes.count() == 1
    reasons = {r._offset: r._dq_failures[0] for r in bad.collect()}
    assert reasons == {2: "undecodable", 3: "unknown_schema_id:99", 4: "not_confluent_avro"}


@pytest.mark.parametrize("op", ["c", "r"])
def test_inserts_and_snapshot_reads(spark, op):
    r = decode_table(bronze_rows(spark, [(None, ROW, SRC, op, 5)]), fake_registry())[0].first()
    assert r._op == op and not r.is_deleted
