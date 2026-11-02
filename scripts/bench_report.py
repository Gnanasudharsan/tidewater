"""Render benchmark CSVs as a markdown table (and save it next to the first CSV).

    python3 scripts/bench_report.py bench/results/baseline_*.csv [bench/results/tuned_*.csv]
"""

import csv
import sys
from pathlib import Path

COLS = [
    ("label", "run"), ("target_events_per_s", "target ev/s"),
    ("achieved_events_per_s", "achieved ev/s"), ("cdc_records_per_s", "CDC rec/s"),
    ("silver_rows_per_s", "silver rows/s"), ("latency_p50_s", "p50 s"), ("latency_p95_s", "p95 s"),
    ("bronze_max_offsets_behind", "max lag (offsets)"), ("silver_batch_s", "silver batch s"),
    ("drain_s", "drain s"),
]  # fmt: skip

rows = []
for path in sys.argv[1:]:
    with open(path) as f:
        rows.extend(csv.DictReader(f))
lines = ["| " + " | ".join(h for _, h in COLS) + " |", "|" + "---|" * len(COLS)]
for r in rows:
    lines.append("| " + " | ".join(r.get(k) or "–" for k, _ in COLS) + " |")
table = "\n".join(lines)
print(table)
Path(sys.argv[1]).with_suffix(".md").write_text(table + "\n")
