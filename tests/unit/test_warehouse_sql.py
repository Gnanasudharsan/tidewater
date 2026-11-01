from tidewater_spark.warehouse import add_column_sql, create_target_sql, upsert_sql


def test_upsert_is_lsn_guarded_and_never_updates_the_key():
    sql = upsert_sql("orders", ["order_id", "status", "_lsn", "_source_offset"], ("order_id",))
    assert 'on conflict ("order_id") do update set "status" = excluded."status"' in sql
    assert '"order_id" = excluded' not in sql
    assert (
        'where (silver."orders"._lsn, silver."orders"._source_offset) '
        "< (excluded._lsn, excluded._source_offset)"
    ) in sql
    assert "_wh_loaded_at" in sql


def test_new_staging_columns_become_alters():
    stg = {"order_id": "bigint", "status": "text", "coupon": "text"}
    assert add_column_sql("orders", stg, {"order_id", "status", "_wh_loaded_at"}) == [
        'alter table silver."orders" add column "coupon" text'
    ]
    assert add_column_sql("orders", stg, set(stg)) == []


def test_identifiers_are_quoted():
    sql = upsert_sql('we"ird', ["id", "x"], ("id",))
    assert 'silver."we""ird"' in sql


def test_target_gets_primary_key_once():
    stmts = create_target_sql("orders", ("order_id",))
    assert stmts[0] == 'create table if not exists silver."orders" (like silver."_stg_orders")'
    assert "contype = 'p'" in stmts[2] and 'add primary key ("order_id")' in stmts[2]
