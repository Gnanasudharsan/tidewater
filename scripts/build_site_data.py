"""Build the showcase site's data from result files committed in this repo.

    python3 scripts/build_site_data.py --out site/data/results.json
        bench/results/baseline_*.csv              -> throughput vs latency table and chart
        bench/results/warehouse_comparison_*.csv  -> Postgres vs Snowflake (if present)
        results/chaos_local_runs.json             -> chaos outcomes table
        docs/media/dashboard.gif                  -> copied to site/media/ (if present)

    python3 scripts/build_site_data.py --append-history HISTORY --latest results/latest.json
        appends one CI run to HISTORY, keeping the last 30 (used by showcase.yml)

Only files that exist are used; a missing one leaves its section out ("Not yet
measured" on the site), it never becomes a placeholder number.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HISTORY_LIMIT = 30


def newest(pattern: str) -> Path | None:
    files = sorted((ROOT / "bench" / "results").glob(pattern))
    return files[-1] if files else None


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return [{k: _num(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _num(v: str):
    try:
        return int(v) if v.isdigit() else float(v)
    except ValueError:
        return v


def benchmark() -> dict | None:
    path = newest("baseline_*.csv")
    if not path:
        return None
    keep = (
        "target_events_per_s",
        "achieved_events_per_s",
        "cdc_records_per_s",
        "latency_p50_s",
        "latency_p95_s",
        "bronze_max_offsets_behind",
        "silver_batch_s",
    )
    return {"source": rel(path), "rows": [{k: r[k] for k in keep} for r in read_csv(path)]}


def warehouse() -> dict | None:
    path = newest("warehouse_comparison_*.csv")
    return {"source": rel(path), "rows": read_csv(path)} if path else None


def chaos() -> dict | None:
    path = ROOT / "results" / "chaos_local_runs.json"
    return json.loads(path.read_text()) if path.exists() else None


def media(site: Path) -> dict:
    gif = ROOT / "docs" / "media" / "dashboard.gif"
    if not gif.exists():
        return {}
    (site / "media").mkdir(parents=True, exist_ok=True)
    shutil.copyfile(gif, site / "media" / gif.name)
    return {"dashboard_gif": f"media/{gif.name}", "dashboard_gif_source": rel(gif)}


def append_history(history: Path, latest: Path) -> None:
    runs = json.loads(history.read_text()) if history.exists() else []
    runs.append(json.loads(latest.read_text()))
    history.write_text(json.dumps(runs[-HISTORY_LIMIT:], indent=1) + "\n")
    print(f"{history}: {len(runs[-HISTORY_LIMIT:])} runs")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, help="write the results data here")
    ap.add_argument("--append-history", type=Path, metavar="HISTORY")
    ap.add_argument("--latest", type=Path, default=ROOT / "results" / "latest.json")
    args = ap.parse_args()

    if args.append_history:
        append_history(args.append_history, args.latest)
    if args.out:
        site = args.out.resolve().parents[1]  # site/data/results.json -> site/
        data = {
            "benchmark": benchmark(),
            "warehouse": warehouse(),
            "chaos": chaos(),
            "media": media(site),
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(data, indent=1) + "\n")
        print(f"{args.out}: " + ", ".join(k for k, v in data.items() if v))
    if not (args.out or args.append_history):
        ap.error("nothing to do: pass --out and/or --append-history")


if __name__ == "__main__":
    main()
