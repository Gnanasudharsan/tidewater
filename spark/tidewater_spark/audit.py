"""Row-level audit trace: follow one order from the dashboard back to the WAL.

marts (dbt)  ->  warehouse silver  ->  silver Delta (+ commit version)  ->
bronze records (Kafka topic/partition/offset)  ->  Postgres WAL LSN  ->  source row
"""

from __future__ import annotations

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tidewater_spark.cdc import decode_table
from tidewater_spark.config import Settings


def _pg_row(conn_kwargs: dict, sql: str, params: tuple) -> dict | None:
    with psycopg.connect(**conn_kwargs) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(zip([d.name for d in cur.description], row, strict=True)) if row else None


def trace_order(spark: SparkSession, settings: Settings, schemas, order_id: int) -> dict:
    src = dict(host=settings.pg_host, port=settings.pg_port, dbname=settings.pg_db,
               user=settings.pg_user, password=settings.pg_password)  # fmt: skip
    wh = dict(host=settings.wh_host, port=settings.wh_port, dbname=settings.wh_db,
              user=settings.wh_user, password=settings.wh_password)  # fmt: skip
    trace: dict = {"order_id": order_id}

    trace["mart"] = _pg_row(
        wh,
        """select f.order_id, f.order_status, f.order_total, f.ordered_at, f.customer_sk,
                  d.city as customer_city_at_order, d.version_number as customer_version
           from marts.fct_orders f
           left join marts.dim_customer d on f.customer_sk = d.customer_sk
           where f.order_id = %s""",
        (order_id,),
    )
    trace["warehouse_silver"] = _pg_row(
        wh,
        "select status, _lsn, _source_topic, _source_partition, _source_offset, _wh_loaded_at "
        "from silver.orders where order_id = %s",
        (order_id,),
    )

    silver_path = settings.silver_path("orders")
    row = spark.read.format("delta").load(silver_path).where(F.col("order_id") == order_id)
    trace["silver"] = (row.select("status", "is_deleted", "_op", "_lsn", "_source_topic",
                                  "_source_partition", "_source_offset", "_silver_updated_at")
                       .first() or {})  # fmt: skip
    trace["silver"] = trace["silver"].asDict() if trace["silver"] else None
    try:
        commit = (
            spark.read.format("delta").option("readChangeFeed", "true")
            .option("startingVersion", 0).load(silver_path)
            .where((F.col("order_id") == order_id) & (F.col("_change_type") != "update_preimage"))
            .orderBy(F.col("_commit_version").desc())
            .select("_change_type", "_commit_version", "_commit_timestamp").first()
        )  # fmt: skip
        trace["silver_commit"] = commit.asDict() if commit else None
    except Exception as exc:  # noqa: BLE001 - CDF may not cover the table's early versions
        trace["silver_commit"] = {"unavailable": str(exc).splitlines()[0][:120]}

    bronze = (
        spark.read.format("delta")
        .load(settings.bronze_path)
        .where(F.col("source_table") == "orders")
    )
    changes, _ = decode_table(bronze, schemas)
    events = (
        changes.where(F.col("order_id") == order_id)
        .select("_op", "status", "order_total", "_lsn", "_source_ts", "_source_topic",
                "_source_partition", "_source_offset", "_schema_id")
        .orderBy("_lsn", "_source_offset").collect()
    )  # fmt: skip
    trace["bronze_events"] = [e.asDict() for e in events]

    trace["source"] = _pg_row(
        src,
        "select order_id, status, order_total, customer_id, created_at, updated_at, "
        "pg_current_wal_lsn() - '0/0' as current_wal_lsn from orders where order_id = %s",
        (order_id,),
    )
    # Which version does silver hold? The newest bronze event by (lsn, offset), unless
    # a backfill stamped a newer horizon LSN (offset -1) over it.
    s = trace["silver"]
    if not s:
        trace["silver_version_from"] = "absent"
    elif s["_source_offset"] == -1:
        trace["silver_version_from"] = f"backfill at horizon LSN {s['_lsn']}"
    else:
        hits = [i for i, e in enumerate(events)
                if (e._lsn, e._source_offset) == (s["_lsn"], s["_source_offset"])]  # fmt: skip
        trace["silver_version_from"] = (
            f"bronze event #{hits[0] + 1} of {len(events)}" if hits else "no matching bronze event"
        )
    return trace
