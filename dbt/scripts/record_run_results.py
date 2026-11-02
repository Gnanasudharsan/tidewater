"""Append dbt's target/run_results.json to warehouse table marts.dbt_run_results
(model timings and statuses), so Grafana can chart dbt run times over time."""

import json
import os
from pathlib import Path

import psycopg2

results = json.loads(Path("target/run_results.json").read_text())
invocation = results["metadata"]["invocation_id"]
generated_at = results["metadata"]["generated_at"]
rows = [
    (
        invocation,
        generated_at,
        r["unique_id"],
        r["unique_id"].split(".")[0],
        r["status"],
        r["execution_time"],
        (r.get("adapter_response") or {}).get("rows_affected"),
    )
    for r in results["results"]
]
conn = psycopg2.connect(
    host=os.environ["WH_PGHOST"],
    port=os.environ.get("WH_PGPORT", "5432"),
    dbname=os.environ["WH_PGDATABASE"],
    user=os.environ["WH_PGUSER"],
    password=os.environ["WH_PGPASSWORD"],
)
with conn, conn.cursor() as cur:
    cur.execute(
        """create table if not exists marts.dbt_run_results (
            invocation_id text, generated_at timestamptz, unique_id text, resource_type text,
            status text, execution_time double precision, rows_affected bigint,
            primary key (invocation_id, unique_id))"""
    )
    cur.executemany(
        "insert into marts.dbt_run_results values (%s, %s, %s, %s, %s, %s, %s) "
        "on conflict do nothing",
        rows,
    )
print(f"recorded {len(rows)} dbt results for invocation {invocation}")
