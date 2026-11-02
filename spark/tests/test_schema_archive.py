import json

import pytest

from tidewater_spark.schema_archive import LEGACY_RUN, SchemaArchive, fingerprint

ORDERS = json.dumps(
    {"type": "record", "name": "Orders", "fields": [{"name": "id", "type": "long"}]}
)
PRODUCTS = json.dumps(
    {"type": "record", "name": "P", "fields": [{"name": "sku", "type": "string"}]}
)


class FakeRegistry:
    def __init__(self, schemas):
        self.schemas, self.calls = schemas, 0

    def schema_json(self, sid):
        self.calls += 1
        return self.schemas[sid]


def test_fingerprint_ignores_key_order_and_whitespace():
    a = '{"type": "record", "name": "X", "fields": []}'
    b = '{"fields":[],"name":"X","type":"record"}'
    assert fingerprint(a) == fingerprint(b) and len(fingerprint(a)) == 64
    assert fingerprint(a) != fingerprint(PRODUCTS)


def test_same_id_in_two_runs_resolves_to_each_runs_schema(spark, tmp_path):
    # The incident case: the rebuilt registry reused id 9 for a different schema.
    path = str(tmp_path / "schemas")
    archive = SchemaArchive(spark, path)
    assert archive.archive("bronze_cdc_v1", {9: ORDERS}) == 1
    assert archive.archive("bronze_cdc_v2", {9: PRODUCTS}) == 1
    fresh = SchemaArchive(spark, path)  # reloads from the Delta table
    assert fresh.lookup("bronze_cdc_v1", 9) == ORDERS
    assert fresh.lookup("bronze_cdc_v2", 9) == PRODUCTS
    assert fresh.lookup(None, 9) == ORDERS  # untagged rows are the legacy run
    assert LEGACY_RUN == "bronze_cdc_v1"


def test_archiving_is_idempotent(spark, tmp_path):
    archive = SchemaArchive(spark, str(tmp_path / "schemas"))
    archive.archive("r", {1: ORDERS})
    assert archive.archive("r", {1: ORDERS}) == 0
    assert spark.read.format("delta").load(str(tmp_path / "schemas")).count() == 1


def test_registry_is_only_a_fallback(spark, tmp_path):
    reg = FakeRegistry({5: PRODUCTS})
    archive = SchemaArchive(spark, str(tmp_path / "schemas"), registry=reg)
    archive.archive("r", {1: ORDERS})
    assert archive.lookup("r", 1) == ORDERS and reg.calls == 0
    assert archive.lookup("r", 5) == PRODUCTS and reg.calls == 1


def test_unknown_schema_without_registry_raises(spark, tmp_path):
    with pytest.raises(KeyError):
        SchemaArchive(spark, str(tmp_path / "schemas")).lookup("r", 42)
