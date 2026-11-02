"""Generates grafana/dashboards/tidewater.json.

Run: python3 infra/observability/make_dashboard.py
"""

import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "warehouse"}
panels, y = [], 0


def row(title):
    global y
    panels.append(
        {
            "type": "row",
            "title": title,
            "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        }
    )
    y += 1


def panel(kind, title, targets, x, w, h=8, unit=None, ds=PROM, thresholds=None, desc=""):
    defaults = {"unit": unit} if unit else {}
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        defaults["custom"] = (
            {"thresholdsStyle": {"mode": "line+area"}} if kind == "timeseries" else {}
        )
    panels.append(
        {
            "type": kind,
            "title": title,
            "description": desc,
            "datasource": ds,
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": (
                {"legend": {"displayMode": "list", "placement": "bottom"}}
                if kind == "timeseries"
                else {}
            ),
            "targets": targets,
        }
    )


def prom(expr, legend="", ref="A"):
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend}


def sql(query, fmt="table", ref="A"):
    return {
        "refId": ref,
        "datasource": PG,
        "rawSql": query,
        "format": fmt,
        "rawQuery": True,
        "editorMode": "code",
    }


SLO = [{"color": "green", "value": None}, {"color": "red", "value": 60}]

row("Health")
panel(
    "stat",
    "Silver p95 latency (SLO < 60 s)",
    [prom('max(tidewater_silver_latency_seconds{quantile="0.95"})')],
    0,
    6,
    4,
    "s",
    thresholds=SLO,
)
panel(
    "stat",
    "Bronze offsets behind Kafka",
    [prom('max(tidewater_stream_offsets_behind_latest{stream="bronze",stat="max"})')],
    6,
    6,
    4,
    thresholds=[
        {"color": "green", "value": None},
        {"color": "orange", "value": 1000},
        {"color": "red", "value": 5000},
    ],
)
panel(
    "stat",
    "Seconds since last progress",
    [prom("time() - tidewater_stream_last_progress_timestamp_seconds", "{{stream}}")],
    12,
    6,
    4,
    "s",
    thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 120}],
)
panel(
    "stat",
    "Quarantined rows (1 h)",
    [prom("sum(increase(tidewater_silver_quarantined_total[1h]))")],
    18,
    6,
    4,
    thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
)
y += 4

row("Ingestion: Postgres -> Kafka -> bronze")
panel(
    "timeseries",
    "CDC events produced per second (Debezium -> Kafka)",
    [
        prom(
            "sum by (topic) (rate(kafka_topic_partition_current_offset"
            '{topic=~"tidewater\\\\.public\\\\..*"}[1m]))',
            "{{topic}}",
        )
    ],
    0,
    12,
    unit="ops",
)
panel(
    "timeseries",
    "Bronze consumer lag (offsets behind latest)",
    [prom('tidewater_stream_offsets_behind_latest{stream="bronze"}', "{{stat}}")],
    12,
    12,
    desc="Spark keeps offsets in its checkpoint, so lag comes from the Kafka source's "
    "progress metrics.",
)
y += 8
panel(
    "timeseries",
    "Stream input vs processed rows per second",
    [
        prom("tidewater_stream_input_rows_per_second", "{{stream}} input"),
        prom("tidewater_stream_processed_rows_per_second", "{{stream}} processed", "B"),
    ],
    0,
    12,
    unit="rowsps",
)
panel(
    "timeseries",
    "Batch duration",
    [prom("tidewater_stream_batch_duration_seconds", "{{stream}}")],
    12,
    12,
    unit="s",
)
y += 8

row("Silver: correctness and latency")
panel(
    "timeseries",
    "End-to-end latency, source commit -> silver commit",
    [
        prom('tidewater_silver_latency_seconds{quantile="0.95"}', "p95"),
        prom('tidewater_silver_latency_seconds{quantile="0.5"}', "p50", "B"),
    ],
    0,
    12,
    unit="s",
    thresholds=SLO,
)
panel(
    "timeseries",
    "Rows merged into silver per second",
    [prom("sum by (table) (rate(tidewater_silver_rows_merged_total[1m]))", "{{table}}")],
    12,
    12,
    unit="rowsps",
)
y += 8
panel(
    "timeseries",
    "DQ failures and late events (per minute)",
    [
        prom("sum(rate(tidewater_silver_quarantined_total[5m])) * 60", "quarantined"),
        prom("sum(rate(tidewater_silver_late_events_total[5m])) * 60", "late (superseded)", "B"),
        prom(
            "sum(rate(tidewater_silver_merge_conflict_retries_total[5m])) * 60",
            "merge conflict retries",
            "C",
        ),
    ],
    0,
    12,
)
panel(
    "table",
    "Quarantine by rule (warehouse mirror, 24 h)",
    [
        sql(
            "select source_table, reasons, count(*) as failures from silver.dq_quarantine "
            "where _recorded_at > now() at time zone 'utc' - interval '24 hours' "
            "group by 1, 2 order by 3 desc"
        )
    ],
    12,
    12,
    ds=PG,
)
y += 8

row("Warehouse and dbt")
panel(
    "timeseries",
    "dbt build duration by run",
    [
        sql(
            'select generated_at as time, sum(execution_time) as "total seconds" '
            "from marts.dbt_run_results group by invocation_id, generated_at order by 1",
            "time_series",
        )
    ],
    0,
    12,
    unit="s",
    ds=PG,
)
panel(
    "table",
    "Slowest dbt nodes (latest run)",
    [
        sql(
            "select unique_id, resource_type, status, round(execution_time::numeric, 2) as seconds "
            "from marts.dbt_run_results where invocation_id = "
            "(select invocation_id from marts.dbt_run_results order by generated_at desc limit 1) "
            "order by execution_time desc limit 12"
        )
    ],
    12,
    12,
    ds=PG,
)
y += 8
panel(
    "table",
    "Warehouse freshness per table",
    [
        sql(
            "select table_name, delta_version, rows_loaded, loaded_at, "
            "round(extract(epoch from now() - loaded_at)) as seconds_since_load "
            "from silver._load_state order by table_name"
        )
    ],
    0,
    24,
    6,
    ds=PG,
)

dashboard = {
    "uid": "tidewater-pipeline",
    "title": "Tidewater pipeline",
    "tags": ["tidewater"],
    "timezone": "utc",
    "schemaVersion": 39,
    "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"},
    "panels": panels,
}
out = Path(__file__).parent / "grafana" / "dashboards" / "tidewater.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} with {len(panels)} panels")
