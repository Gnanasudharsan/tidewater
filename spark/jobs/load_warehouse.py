"""Incrementally load silver Delta tables into the Postgres warehouse (schema `silver`).

    spark-submit jobs/load_warehouse.py [--table orders ...] [--interval 60]

Per table:
  1. Read the bookmark (last loaded Delta version) from silver._load_state.
  2. No bookmark: load a full snapshot at the current version.
     Bookmark v: read the Change Data Feed for versions v+1..current and keep
     the newest post-image per primary key.
  3. Write those rows to silver._stg_<table> over JDBC.
  4. In one Postgres transaction: create/evolve the target table, upsert from
     staging (LSN-guarded), and move the bookmark to the current version.
"""

from __future__ import annotations

import argparse
import logging
import time

import psycopg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from tidewater_spark.cdf import changed_rows, ensure_cdf
from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark
from tidewater_spark.tables import TableContract, load_contracts
from tidewater_spark.warehouse import (
    CREATE_STATE,
    SCHEMA,
    STATE_TABLE,
    add_column_sql,
    create_target_sql,
    save_state_sql,
    staging_table,
    upsert_sql,
)

log = logging.getLogger("load_warehouse")


def wh_connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.wh_host,
        port=settings.wh_port,
        dbname=settings.wh_db,
        user=settings.wh_user,
        password=settings.wh_password,
    )


def load_table(spark: SparkSession, settings: Settings, contract: TableContract) -> None:
    name, pk = contract.name, contract.primary_key
    path = settings.silver_path(name)
    if not DeltaTable.isDeltaTable(spark, path):
        log.info("%s: no silver table yet", name)
        return
    ensure_cdf(spark, path)
    current = DeltaTable.forPath(spark, path).history(1).first()["version"]

    with wh_connect(settings) as conn:
        conn.execute(CREATE_STATE)
        row = conn.execute(
            f"select delta_version from {STATE_TABLE} where table_name = %s", (name,)
        ).fetchone()
        conn.commit()
    since = row[0] if row else None
    if since == current:
        log.info("%s: up to date at version %s", name, current)
        return

    rows, mode = changed_rows(spark, path, pk, since, current)
    rows = rows.persist()
    n = rows.count()
    if n:
        rows.write.jdbc(
            settings.wh_jdbc_url,
            f"{SCHEMA}.{staging_table(name)}",
            mode="overwrite",
            properties=settings.wh_jdbc_props,
        )

    with wh_connect(settings) as conn, conn.transaction():
        conn.execute("select pg_advisory_xact_lock(hashtext(%s))", (f"tidewater_load:{name}",))
        if n:
            for stmt in create_target_sql(name, pk):
                conn.execute(stmt)
            stg_cols = dict(
                conn.execute(
                    "select attname, format_type(atttypid, atttypmod) from pg_attribute "
                    "where attrelid = %s::regclass and attnum > 0 and not attisdropped "
                    "order by attnum",
                    (f"{SCHEMA}.{staging_table(name)}",),
                ).fetchall()
            )
            target_cols = {
                r[0]
                for r in conn.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema = %s and table_name = %s",
                    (SCHEMA, name),
                ).fetchall()
            }
            for stmt in add_column_sql(name, stg_cols, target_cols):
                log.info("%s: schema evolution: %s", name, stmt)
                conn.execute(stmt)
            conn.execute(upsert_sql(name, list(stg_cols), pk))
        conn.execute(save_state_sql(), (name, current, n))
    rows.unpersist()
    log.info("%s: loaded %d rows (%s)", name, n, mode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append")
    parser.add_argument("--interval", type=int, default=0, help="loop every N seconds; 0 = once")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    contracts = load_contracts()
    spark = build_spark("tidewater-load-warehouse", settings)
    while True:
        started = time.monotonic()
        for name in args.table or sorted(contracts):
            load_table(spark, settings, contracts[name])
        if not args.interval:
            break
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))
    spark.stop()


if __name__ == "__main__":
    main()
