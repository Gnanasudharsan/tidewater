"""Collect one benchmark step's measurements from Prometheus and append a CSV row.

    python3 scripts/bench_collect.py --csv out.csv --label baseline --target-rate 500 \
        --business-events 89000 --step-seconds 180 --window 120 --drain-seconds 42

All rates/latencies are measured over the last `window` seconds of the step (after
warm-up). Latency is Tidewater's own source-commit -> silver-commit measurement.
"""

from __future__ import annotations

import argparse
import csv
import json
import urllib.parse
import urllib.request
from pathlib import Path

PROM = "http://localhost:9090/api/v1/query"
FIELDS = [
    "label", "target_events_per_s", "achieved_events_per_s", "cdc_records_per_s",
    "bronze_rows_per_s", "silver_rows_per_s", "latency_p50_s", "latency_p95_s",
    "latency_p95_max_s", "bronze_max_offsets_behind", "bronze_batch_s", "silver_batch_s",
    "drain_s",
]  # fmt: skip


def q(expr: str) -> float | None:
    url = PROM + "?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.load(resp)["data"]["result"]
    return round(float(result[0]["value"][1]), 2) if result else None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--target-rate", type=float, required=True)
    p.add_argument("--business-events", type=int, required=True)
    p.add_argument("--step-seconds", type=int, required=True)
    p.add_argument("--window", type=int, required=True)
    p.add_argument("--drain-seconds", type=int, required=True)
    p.add_argument("--offset-seconds", type=int, default=0, help="how long ago the load stopped")
    a = p.parse_args()
    # Range selector for "the last `window` seconds before the load stopped".
    rng = f"[{a.window}s]" + (f" offset {a.offset_seconds}s" if a.offset_seconds else "")
    topics = 'topic=~"tidewater\\\\.public\\\\..*"'

    def over(fn: str, metric: str) -> float | None:
        return q(f"{fn}({metric}{rng})")

    lat = "tidewater_silver_latency_seconds"
    batch = "tidewater_stream_batch_duration_seconds"
    row = {
        "label": a.label,
        "target_events_per_s": a.target_rate,
        "achieved_events_per_s": round(a.business_events / a.step_seconds, 1),
        "cdc_records_per_s": q(f"sum(rate(kafka_topic_partition_current_offset{{{topics}}}{rng}))"),
        "bronze_rows_per_s": q(f'sum(rate(tidewater_stream_rows_total{{stream="bronze"}}{rng}))'),
        "silver_rows_per_s": q(f'sum(rate(tidewater_stream_rows_total{{stream="silver"}}{rng}))'),
        "latency_p50_s": over("avg_over_time", f'{lat}{{quantile="0.5"}}'),
        "latency_p95_s": over("avg_over_time", f'{lat}{{quantile="0.95"}}'),
        "latency_p95_max_s": over("max_over_time", f'{lat}{{quantile="0.95"}}'),
        "bronze_max_offsets_behind": over(
            "max_over_time", 'tidewater_stream_offsets_behind_latest{stream="bronze",stat="max"}'
        ),
        "bronze_batch_s": over("avg_over_time", f'{batch}{{stream="bronze"}}'),
        "silver_batch_s": over("avg_over_time", f'{batch}{{stream="silver"}}'),
        "drain_s": a.drain_seconds,
    }
    path = Path(a.csv)
    new = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)
    print("BENCH " + json.dumps(row))


if __name__ == "__main__":
    main()
