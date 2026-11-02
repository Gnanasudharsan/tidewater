"""The silver MERGE guarantees, each as a test: dedup, ordering, replay, deletes, new columns."""

import datetime as dt

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql import types as T

from tidewater_spark.merge import (
    classify_against_target,
    latest_per_key,
    merge_into_silver,
    with_conflict_retries,
)

PK = ("order_id",)
TS = dt.datetime(2026, 10, 31, 12, 0)

SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.LongType()),
        T.StructField("status", T.StringType()),
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


def change(order_id, status, lsn, op="u", offset=None):
    deleted = op == "d"
    return (
        order_id,
        status,
        deleted,
        TS if deleted else None,
        op,
        lsn,
        None,
        TS,
        "tidewater.public.orders",
        0,
        lsn if offset is None else offset,
        1,
    )


def apply(spark, path, rows, schema=SCHEMA):
    df = spark.createDataFrame(rows, schema)
    merge_into_silver(spark, path, latest_per_key(df, PK), PK, what="test")


def silver(spark, path):
    return spark.read.format("delta").load(path)


def statuses(spark, path):
    return {r.order_id: (r.status, r.is_deleted) for r in silver(spark, path).collect()}


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "orders")


def test_dedup_keeps_highest_lsn_in_batch(spark, path):
    apply(
        spark, path, [change(1, "placed", 10, "c"), change(1, "paid", 30), change(1, "shipped", 20)]
    )
    assert statuses(spark, path) == {1: ("paid", False)}
    assert silver(spark, path).count() == 1


def test_older_event_in_later_batch_is_ignored(spark, path):
    apply(spark, path, [change(1, "paid", 20)])
    apply(spark, path, [change(1, "placed", 10)])
    assert statuses(spark, path) == {1: ("paid", False)}


def test_replaying_a_batch_changes_nothing(spark, path):
    batch = [change(1, "placed", 10, "c"), change(2, "paid", 11, "c")]
    apply(spark, path, batch)
    before = silver(spark, path)
    snapshot = spark.createDataFrame(before.collect(), before.schema)
    apply(spark, path, batch)
    # Even _silver_updated_at is unchanged: the guard rejected every replayed row.
    assert_df_equality(silver(spark, path), snapshot, ignore_row_order=True)


def test_delete_is_soft_and_blocks_stale_update(spark, path):
    apply(spark, path, [change(1, "placed", 10, "c")])
    apply(spark, path, [change(1, "placed", 30, "d")])
    apply(spark, path, [change(1, "paid", 20)])  # late update from before the delete
    row = silver(spark, path).first()
    assert (row.is_deleted, row.status, row._lsn) == (True, "placed", 30)
    assert row.deleted_at is not None


def test_create_and_delete_in_one_batch_lands_as_deleted(spark, path):
    apply(spark, path, [change(1, "placed", 10, "c"), change(1, "placed", 11, "d")])
    assert statuses(spark, path) == {1: ("placed", True)}


def test_equal_lsn_is_ordered_by_offset(spark, path):
    apply(spark, path, [change(1, "a", 10, offset=5)])
    apply(spark, path, [change(1, "b", 10, offset=7)])
    apply(spark, path, [change(1, "c", 10, offset=6)])
    assert statuses(spark, path) == {1: ("b", False)}


def test_new_source_column_is_added_to_silver(spark, path):
    apply(spark, path, [change(1, "placed", 10, "c")])
    wider = T.StructType(
        SCHEMA.fields[:2] + [T.StructField("coupon", T.StringType())] + SCHEMA.fields[2:]
    )
    row = change(2, "placed", 11, "c")
    apply(spark, path, [row[:2] + ("FALL10",) + row[2:]], schema=wider)
    got = {r.order_id: r.coupon for r in silver(spark, path).collect()}
    assert got == {1: None, 2: "FALL10"}


def test_classify_splits_late_from_replays_and_new(spark, path):
    apply(spark, path, [change(1, "paid", 20), change(2, "paid", 20)])
    incoming = spark.createDataFrame(
        [change(1, "placed", 10), change(2, "paid", 20), change(3, "placed", 5, "c")], SCHEMA
    )
    to_merge, late = classify_against_target(spark, path, incoming, PK)
    assert {r.order_id for r in to_merge.collect()} == {3}  # replay of id 2 dropped quietly
    late_rows = late.collect()
    assert [(r.order_id, r._lsn, r._target_lsn) for r in late_rows] == [(1, 10, 20)]


def test_classify_before_table_exists_returns_everything(spark, path):
    incoming = spark.createDataFrame([change(1, "placed", 10, "c")], SCHEMA)
    to_merge, late = classify_against_target(spark, path, incoming, PK)
    assert to_merge.count() == 1 and late.count() == 0
    assert "_target_source_ts" in late.columns


class ConcurrentAppendException(Exception):
    pass


def test_conflicts_are_retried_then_succeed():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConcurrentAppendException("Files were added by a concurrent update")

    assert with_conflict_retries(flaky, what="t", sleep=lambda s: None) == 3


@pytest.mark.parametrize(
    "message",
    [
        "io.delta.exceptions.ConcurrentAppendException: Files were added",
        "io.delta.exceptions.MetadataChangedException: The metadata of the Delta table changed",
        "io.delta.exceptions.ProtocolChangedException: protocol version changed",
    ],
)
def test_every_delta_conflict_type_is_retryable(message):
    from tidewater_spark.merge import is_write_conflict

    assert is_write_conflict(RuntimeError(message))
    assert not is_write_conflict(RuntimeError("AnalysisException: cannot resolve column"))


def test_non_conflict_errors_are_not_retried():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError("bad column")

    with pytest.raises(ValueError):
        with_conflict_retries(broken, what="t", sleep=lambda s: None)
    assert len(calls) == 1


def test_merge_stamps_silver_updated_at(spark, path):
    apply(spark, path, [change(1, "placed", 10, "c")])
    assert silver(spark, path).where(F.col("_silver_updated_at").isNull()).count() == 0
