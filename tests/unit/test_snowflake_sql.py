from tidewater_spark import snowflake_sql as sql


def test_merge_is_lsn_guarded_without_row_value_comparison():
    stmt = sql.merge("orders", ["order_id", "status", "_lsn", "_source_offset"], ("order_id",))
    assert (
        "merge into SILVER.ORDERS t using SILVER._STG_ORDERS s on t.ORDER_ID = s.ORDER_ID" in stmt
    )
    assert "s._LSN > t._LSN or (s._LSN = t._LSN and s._SOURCE_OFFSET > t._SOURCE_OFFSET)" in stmt
    assert "ORDER_ID = s.ORDER_ID," not in stmt.split("update set")[1]  # key is never updated
    assert "insert (ORDER_ID, STATUS, _LSN, _SOURCE_OFFSET)" in stmt


def test_new_columns_are_added_case_insensitively():
    stg = {"ORDER_ID": "NUMBER(38,0)", "STATUS": "VARCHAR", "COUPON": "VARCHAR"}
    assert sql.add_columns("orders", stg, {"order_id", "status"}) == [
        "alter table SILVER.ORDERS add column COUPON VARCHAR"
    ]


def test_bookmark_and_log_take_bind_parameters():
    assert sql.save_state().count("%s") == 4 and "delta_table_id" in sql.save_state()
    assert sql.log_load().count("%s") == 3
