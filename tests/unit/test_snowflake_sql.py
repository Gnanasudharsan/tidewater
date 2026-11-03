from tidewater_spark import snowflake_sql as sql


def test_merge_is_lsn_guarded_without_row_value_comparison():
    stmt = sql.merge("orders", ["order_id", "status", "_lsn", "_source_offset"], ("order_id",))
    assert (
        "merge into SILVER.ORDERS t using SILVER._STG_ORDERS s on t.ORDER_ID = s.ORDER_ID" in stmt
    )
    assert "s._LSN > t._LSN or (s._LSN = t._LSN and s._SOURCE_OFFSET > t._SOURCE_OFFSET)" in stmt
    assert "ORDER_ID = s.ORDER_ID," not in stmt.split("update set")[1]  # key is never updated
    assert "insert (ORDER_ID, STATUS, _LSN, _SOURCE_OFFSET, _WH_LOADED_AT)" in stmt
    assert "_WH_LOADED_AT = current_timestamp()" in stmt  # dbt incremental facts key on it


def test_new_columns_are_added_case_insensitively():
    stg = {"ORDER_ID": "NUMBER(38,0)", "STATUS": "VARCHAR", "COUPON": "VARCHAR"}
    assert sql.add_columns("orders", stg, {"order_id", "status"}) == [
        "alter table SILVER.ORDERS add column COUPON VARCHAR"
    ]


def test_bookmark_and_log_take_bind_parameters():
    assert sql.save_state().count("%s") == 4 and "delta_table_id" in sql.save_state()
    assert sql.log_load().count("%s") == 3


def test_staging_ddl_maps_spark_types():
    ddl = sql.create_staging(
        "orders",
        [("order_id", "bigint"), ("order_total", "decimal(12,2)"), ("ordered_at", "timestamp"),
         ("is_deleted", "boolean"), ("status", "string")],
    )  # fmt: skip
    assert ddl == (
        "create or replace table SILVER._STG_ORDERS (ORDER_ID NUMBER(38,0), "
        "ORDER_TOTAL NUMBER(12,2), ORDERED_AT TIMESTAMP_NTZ, IS_DELETED BOOLEAN, STATUS VARCHAR)"
    )


def test_unknown_spark_types_fail_loudly():
    import pytest

    with pytest.raises(ValueError):
        sql.sf_type("map<string,int>")


def test_copy_purges_staged_files_and_matches_by_name():
    stmt = sql.copy_into("orders", "orders/abc")
    assert "from @SILVER.LOAD_STAGE/orders/abc" in stmt
    assert "match_by_column_name = case_insensitive" in stmt and "purge = true" in stmt
