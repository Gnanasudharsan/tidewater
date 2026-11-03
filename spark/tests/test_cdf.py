"""Change Data Feed reads used by the warehouse loader."""

from delta.tables import DeltaTable

from tidewater_spark.cdf import changed_rows, ensure_cdf

PK = ("id",)


def version(spark, path):
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


def upsert(spark, path, rows):
    df = spark.createDataFrame(rows, "id long, status string")
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def test_full_then_incremental_returns_only_newest_changes(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "a")], "id long, status string").write.format(
        "delta"
    ).save(path)
    ensure_cdf(spark, path)  # existing table: turned on in place
    v0 = version(spark, path)

    full, mode = changed_rows(spark, path, PK, None, v0)
    assert mode.startswith("full") and full.count() == 2

    upsert(spark, path, [(1, "b"), (3, "a")])
    upsert(spark, path, [(1, "c")])
    v2 = version(spark, path)
    inc, mode = changed_rows(spark, path, PK, v0, v2)
    assert mode == f"cdf {v0 + 1}..{v2}"
    # id 1 changed twice: only its newest post-image; id 2 untouched: absent.
    assert sorted((r.id, r.status) for r in inc.collect()) == [(1, "c"), (3, "a")]
    assert "_change_type" not in inc.columns


def test_falls_back_to_full_load_when_cdf_range_is_unavailable(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a")], "id long, status string").write.format("delta").option(
        "delta.enableChangeDataFeed", "false"
    ).save(path)
    upsert(spark, path, [(2, "a")])  # version 1, written without CDF
    rows, mode = changed_rows(spark, path, PK, 0, version(spark, path))
    assert mode.startswith("full") and rows.count() == 2


def test_delta_log_committed_waits_for_a_real_commit(spark, tmp_path):
    from tidewater_spark.session import delta_log_committed

    path = str(tmp_path / "t")
    assert not delta_log_committed(spark, path)  # nothing there yet
    (tmp_path / "t" / "_delta_log").mkdir(parents=True)
    assert not delta_log_committed(spark, path)  # an empty log directory is not a table
    spark.createDataFrame([(1, "a")], "id long, status string").write.format("delta").mode(
        "overwrite"
    ).save(path)
    assert delta_log_committed(spark, path)
