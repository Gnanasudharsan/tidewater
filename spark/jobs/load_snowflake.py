"""Incrementally load silver Delta tables into Snowflake (TIDEWATER.SILVER).

    spark-submit jobs/load_snowflake.py [--table orders ...]

Mirrors load_warehouse.py (ADR 0008) against Snowflake: Change Data Feed by Delta
version with a table-id check, rows staged with write_pandas, then one transaction
that evolves the schema, MERGEs with the (lsn, offset) guard, moves the bookmark
and logs the rows delivered (read by the snowflake_cost dbt model).

Auth is key-pair only (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH).
"""

from __future__ import annotations

import argparse
import logging
import os

import snowflake.connector
from delta.tables import DeltaTable
from snowflake.connector.pandas_tools import write_pandas

from tidewater_spark import snowflake_sql as sql
from tidewater_spark.cdf import changed_rows, ensure_cdf
from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark
from tidewater_spark.tables import TableContract, load_contracts

log = logging.getLogger("load_snowflake")


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("SNOWFLAKE_USER", "SVC_TIDEWATER"),
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "/secrets/snowflake_key.p8"),
        role="TIDEWATER_LOADER",
        warehouse="TIDEWATER_WH",
        database="TIDEWATER",
        schema="SILVER",
        session_parameters={"QUERY_TAG": "tidewater-loader"},
    )


def load_table(spark, settings: Settings, conn, contract: TableContract) -> None:
    name, pk = contract.name, contract.primary_key
    path = settings.silver_path(name)
    if not DeltaTable.isDeltaTable(spark, path):
        return
    ensure_cdf(spark, path)
    table = DeltaTable.forPath(spark, path)
    current = table.history(1).first()["version"]
    table_id = table.detail().first()["id"]

    cur = conn.cursor()
    cur.execute(
        f"select delta_version, delta_table_id from {sql.STATE} where table_name = %s", (name,)
    )
    row = cur.fetchone()
    since = row[0] if row and row[1] == table_id else None
    if since == current:
        log.info("%s: up to date at version %s", name, current)
        return

    rows, mode = changed_rows(spark, path, pk, since, current)
    pdf = rows.toPandas()
    pdf.columns = [c.upper() for c in pdf.columns]
    if len(pdf):
        write_pandas(
            conn, pdf, sql.staging(name), auto_create_table=True, overwrite=True,
            quote_identifiers=False, use_logical_type=True,
        )  # fmt: skip
    try:
        cur.execute("begin")
        if len(pdf):
            cur.execute(sql.create_target(name))
            cur.execute(f"describe table SILVER.{sql.staging(name)}")
            stg_cols = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute(f"describe table SILVER.{name.upper()}")
            target_cols = {r[0] for r in cur.fetchall()}
            for stmt in sql.add_columns(name, stg_cols, target_cols):
                cur.execute(stmt)
            cur.execute(sql.merge(name, list(stg_cols), pk))
        cur.execute(sql.save_state(), (name, current, table_id, len(pdf)))
        cur.execute(sql.log_load(), (name, len(pdf), current))
        cur.execute("commit")
    except Exception:
        cur.execute("rollback")
        raise
    log.info("%s: loaded %d rows into Snowflake (%s)", name, len(pdf), mode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    contracts = load_contracts()
    spark = build_spark("tidewater-load-snowflake", settings)
    conn = connect()
    with conn.cursor() as cur:
        cur.execute(sql.CREATE_STATE)
        cur.execute(sql.CREATE_LOG)
    for name in args.table or sorted(contracts):
        load_table(spark, settings, conn, contracts[name])
    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
