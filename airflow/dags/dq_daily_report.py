"""Daily: summarize data quality per table into marts.dq_daily_report, then fail the run
if quarantined rows for the day exceed a threshold (so it alerts instead of hiding)."""

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

from tidewater_ops import DEFAULT_ARGS

TABLES = ["customers", "products", "orders", "order_items", "payments"]

ON_DAY = "cast(_recorded_at as date) = '{{ ds }}'"
PER_TABLE = "\nunion all\n".join(
    f"""select '{t}' as source_table,
       (select count(*) from silver.dq_quarantine
         where source_table = '{t}' and {ON_DAY}) as quarantined,
       (select count(*) from silver.late_arrivals
         where source_table = '{t}' and {ON_DAY}) as late_events,
       (select count(*) from silver.{t} where not is_deleted) as live_rows,
       (select max(_silver_updated_at) from silver.{t}) as last_silver_update"""
    for t in TABLES
)

with DAG(
    dag_id="dq_daily_report",
    description="Per-table DQ summary (quarantine, late events, freshness) + threshold check",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"max_quarantined_per_day": Param(100, type="integer", minimum=0)},
    tags=["tidewater", "data-quality"],
) as dag:
    build_report = SQLExecuteQueryOperator(
        task_id="build_report",
        conn_id="warehouse",
        sql=[
            """create table if not exists marts.dq_daily_report (
                report_date date not null, source_table text not null, quarantined bigint,
                late_events bigint, live_rows bigint, last_silver_update timestamp,
                generated_at timestamptz not null default now(),
                primary key (report_date, source_table))""",
            "delete from marts.dq_daily_report where report_date = '{{ ds }}'",
            f"""insert into marts.dq_daily_report
                (report_date, source_table, quarantined, late_events, live_rows, last_silver_update)
                select '{{{{ ds }}}}', * from ({PER_TABLE}) as per_table""",
        ],
    )
    within_threshold = SQLCheckOperator(
        task_id="quarantine_within_threshold",
        conn_id="warehouse",
        sql="""select coalesce(sum(quarantined), 0) <= {{ params.max_quarantined_per_day }}
               from marts.dq_daily_report where report_date = '{{ ds }}'""",
    )

    build_report >> within_threshold
