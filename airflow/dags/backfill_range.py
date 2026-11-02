"""On demand: backfill one silver table for an updated_at window, then refresh the warehouse.

Trigger with params, e.g. {"table": "orders", "start_date": "2026-11-01", "end_date": "2026-11-02"}.
Params are schema-validated (table is an enum, dates are ISO dates) before they are
templated into the job's SQL predicate.
"""

from datetime import date, datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from tidewater_ops import DEFAULT_ARGS, dbt, spark_job

TABLES = ["customers", "products", "orders", "order_items", "payments"]


def validate_window(params: dict) -> None:
    start, end = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    if start >= end:
        raise ValueError(f"start_date {start} must be before end_date {end}")


with DAG(
    dag_id="backfill_range",
    description="Backfill one silver table for a date window (safe alongside the stream)",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "table": Param("orders", type="string", enum=TABLES),
        "start_date": Param("2026-11-01", type="string", format="date"),
        "end_date": Param("2026-11-02", type="string", format="date"),
    },
    tags=["tidewater", "backfill", "silver"],
) as dag:
    validate = PythonOperator(task_id="validate_window", python_callable=validate_window)
    backfill = spark_job(
        "backfill",
        "backfill.py",
        "--table",
        "{{ params.table }}",
        "--where",
        "updated_at >= '{{ params.start_date }}' and updated_at < '{{ params.end_date }}'",
    )
    load = spark_job("load_warehouse", "load_warehouse.py", "--table", "{{ params.table }}")
    build = dbt("dbt_build", "build", record=True)

    validate >> backfill >> load >> build
