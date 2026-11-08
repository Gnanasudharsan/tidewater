"""Parity: the site's merge.js must behave exactly like the production silver MERGE.

Every scenario in fixtures.json runs through the real PySpark path the stream uses
(latest_per_key -> classify_against_target -> merge_into_silver on a Delta table), and
the result is compared with merge.js's output (run_merge.js): final silver state and
the set of late events, batch by batch. Both must also match the hand-checked 'expect'.

Runs inside the Spark image (CI job spark-tests):
    node tests/parity/run_merge.js > tests/parity/out/js_result.json
    docker compose run --rm --no-deps -v "$PWD/tests/parity:/opt/tidewater/parity" \
        spark-bronze python3 -m pytest -q -p no:cacheprovider parity
"""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from tidewater_spark.merge import classify_against_target, latest_per_key, merge_into_silver

HERE = Path(__file__).parent
FIXTURES_RAW = (HERE / "fixtures.json").read_bytes()
SCENARIOS = json.loads(FIXTURES_RAW)["scenarios"]
JS_RESULT = Path(os.environ.get("PARITY_JS_RESULT", HERE / "out" / "js_result.json"))
PK = ("order_id",)
TS = dt.datetime(2026, 10, 31, 12, 0)

# The decoded-change schema silver receives (see spark/tests/test_silver_merge.py).
SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("is_deleted", T.BooleanType()),
        T.StructField("deleted_at", T.TimestampType()),
        T.StructField("_op", T.StringType()),
        T.StructField("_lsn", T.LongType()),
        T.StructField("_tx_id", T.LongType()),
        T.StructField("_source_ts", T.TimestampType()),
        T.StructField("_source_topic", T.StringType()),
        T.StructField("_source_partition", T.IntegerType()),
        T.StructField("_source_offset", T.LongType()),
        T.StructField("_schema_id", T.IntegerType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tidewater-merge-parity")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def js_result():
    if not JS_RESULT.exists():
        pytest.fail(f"{JS_RESULT} missing: run `node tests/parity/run_merge.js` first")
    result = json.loads(JS_RESULT.read_text())
    assert (
        result["fixtures_sha256"] == hashlib.sha256(FIXTURES_RAW).hexdigest()
    ), "merge.js ran on different fixtures"
    return result["scenarios"]


def row(e):
    deleted = e["op"] == "d"
    return (
        e["pk"],
        e["status"],
        float(e["amount"]),
        deleted,
        TS if deleted else None,
        e["op"],
        e["lsn"],
        None,
        TS,
        "tidewater.public.orders",
        0,
        e["offset"],
        1,
    )


def run_pyspark(spark, path, batches):
    late = []
    for b, batch in enumerate(batches):
        changes = spark.createDataFrame([row(e) for e in batch], SCHEMA)
        to_merge, too_old = classify_against_target(spark, path, latest_per_key(changes, PK), PK)
        late += [[b, r.order_id, r._lsn, r._source_offset] for r in too_old.collect()]
        if to_merge.count():
            merge_into_silver(spark, path, to_merge, PK, what="parity")
    state = {
        str(r.order_id): {
            "status": r.status,
            "amount": r.amount,
            "is_deleted": r.is_deleted,
            "_lsn": r._lsn,
            "_source_offset": r._source_offset,
        }
        for r in spark.read.format("delta").load(path).collect()
    }
    return state, late


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
def test_merge_js_matches_pyspark(spark, js_result, tmp_path, scenario):
    py_state, py_late = run_pyspark(spark, str(tmp_path / "orders"), scenario["batches"])
    js = js_result[scenario["name"]]

    assert js["state"] == py_state
    assert sorted(js["late"]) == sorted(py_late)
    for pk, want in scenario["expect"].items():
        assert {k: py_state[pk][k] for k in want} == want
    assert set(py_state) == set(scenario["expect"])
