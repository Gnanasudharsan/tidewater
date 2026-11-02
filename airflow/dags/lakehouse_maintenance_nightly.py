"""Nightly: compact small files and vacuum old ones across bronze, silver and audit logs."""

from datetime import datetime

from airflow import DAG

from tidewater_ops import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="lakehouse_maintenance_nightly",
    description="Delta OPTIMIZE (compaction) + VACUUM (168h retention)",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tidewater", "maintenance", "delta"],
) as dag:
    spark_job("optimize_and_vacuum", "maintenance.py", "--retain-hours", "168")
