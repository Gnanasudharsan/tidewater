"""Hourly: load the warehouse from silver, check freshness, then build and test all models."""

from datetime import datetime

from airflow import DAG

from tidewater_ops import DEFAULT_ARGS, dbt, spark_job

with DAG(
    dag_id="dbt_build_hourly",
    description="Silver -> warehouse load, dbt source freshness, dbt build (models + tests)",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tidewater", "dbt", "warehouse"],
) as dag:
    load = spark_job("load_warehouse", "load_warehouse.py")
    # Stale sources should be visible but must not block the build: the build still
    # brings everything up to date with whatever has arrived.
    freshness = dbt("source_freshness", "source", "freshness")
    build = dbt("dbt_build", "build", record=True, trigger_rule="all_done")

    load >> freshness >> build
