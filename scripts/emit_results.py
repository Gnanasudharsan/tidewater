"""Turn one showcase CI run's raw outputs into results/latest.json for the site.

Every number comes from an artifact of the run; anything the run did not produce is
null (the site shows "not measured"), never a guess or a zero.

  reconcile output  -> rows_reconciled, duplicates_in_silver, hash_match_pct (the warm
                       wave's reconcile when it ran, since it covers every event)
  silver stream log -> latency (p50: median of per-batch p50s; p95: worst per-batch p95)
                       latency_ms: the warm wave, sent once the stream is running
                       latency_cold_ms: the end-to-end test, whose first batch waits for
                       the stream to start
  chaos/<name>.exit -> chaos[] (exit code 0 = passed; the log's last PASS/FAIL line = notes)
  exit codes        -> status ("passed" only if integration, every chaos test and parity pass)

    python3 scripts/emit_results.py --raw results/raw --events 1000 --warm-events 600 \
        --out results/latest.json
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import os
import re
import statistics
from pathlib import Path

RECONCILE_ROW = re.compile(
    r"^(?P<table>[a-z_]+)\s+(?P<source>\d+)\s+(?P<silver>\d+)\s+(?P<missing>\d+)\s+(?P<extra>\d+)"
    r"\s+(?P<diff>\d+)\s+(?P<dup_pk>\d+)\s+(?P<explained>\d+)\s+(?P<result>PASS|FAIL)\s*$"
)


def read(path: Path) -> str | None:
    return path.read_text(errors="replace") if path.exists() else None


def exit_code(path: Path) -> int | None:
    text = read(path)
    return int(text.strip()) if text and text.strip().lstrip("-").isdigit() else None


def parse_reconcile(text: str | None) -> dict:
    rows = [m.groupdict() for line in (text or "").splitlines() if (m := RECONCILE_ROW.match(line))]
    if not rows or "RESULT: PASS" not in text:
        return {"rows_reconciled": None, "duplicates_in_silver": None, "hash_match_pct": None}
    total = {
        k: sum(int(r[k]) for r in rows) for k in ("source", "missing", "extra", "diff", "dup_pk")
    }
    compared = total["source"] + total["extra"]
    matched = total["source"] - total["missing"] - total["diff"]
    return {
        "rows_reconciled": total["source"],
        "duplicates_in_silver": total["dup_pk"],
        "hash_match_pct": round(100 * matched / compared, 3) if compared else None,
    }


def parse_latency(text: str | None) -> dict | None:
    p50s, p95s = [], []
    for line in (text or "").splitlines():
        _, sep, stats = line.partition("silver batch ")
        if not sep:
            continue
        try:
            batch = ast.literal_eval(stats.strip())
        except (ValueError, SyntaxError):
            continue
        if "latency_p95_s" in batch:
            p50s.append(batch["latency_p50_s"])
            p95s.append(batch["latency_p95_s"])
    if not p95s:
        return None
    return {
        "p50": round(statistics.median(p50s) * 1000),
        "p95": round(max(p95s) * 1000),
        "batches": len(p95s),
    }


def parse_chaos(chaos_dir: Path) -> list[dict]:
    out = []
    for exit_file in sorted(chaos_dir.glob("*.exit")):
        name = exit_file.stem
        code = exit_code(exit_file)
        log = read(chaos_dir / f"{name}.log") or ""
        verdicts = [
            line.strip() for line in log.splitlines() if re.search(r"\b(PASS|FAIL)\b", line)
        ]
        seconds = read(chaos_dir / f"{name}.seconds")
        out.append(
            {
                "name": name,
                "passed": code == 0,
                "notes": (
                    re.sub(r"^\[[\d:]+\]\s*", "", verdicts[-1]) if verdicts else f"exit code {code}"
                ),
                "seconds": int(seconds) if seconds and seconds.strip().isdigit() else None,
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw", type=Path, default=Path("results/raw"))
    ap.add_argument("--events", type=int, required=True, help="business events the simulator sent")
    ap.add_argument("--warm-events", type=int, default=0, help="events in the warm wave")
    ap.add_argument("--out", type=Path, default=Path("results/latest.json"))
    args = ap.parse_args()
    raw = args.raw

    integration = exit_code(raw / "integration.exit")
    parity = exit_code(raw / "parity.exit")
    chaos = parse_chaos(raw / "chaos")
    warm_text = read(raw / "reconcile_warm.out")
    warm = parse_reconcile(warm_text)
    warm_ran, warm_ok = warm_text is not None, warm["rows_reconciled"] is not None
    passed = (
        integration == 0
        and parity == 0
        and all(c["passed"] for c in chaos)
        and (warm_ok or not warm_ran)
    )
    events = None
    if integration == 0:
        events = args.events + (args.warm_events if warm_ok else 0)

    server, repo, run = (
        os.environ.get(k, "") for k in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID")
    )
    result = {
        "status": "passed" if passed else "failed",
        "run_url": f"{server}/{repo}/actions/runs/{run}" if run else "",
        "commit_sha": os.environ.get("GITHUB_SHA", ""),
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # Only count events as processed if they made it through end to end.
        "events_processed": events,
        **(warm if warm_ok else parse_reconcile(read(raw / "reconcile.out"))),
        "latency_ms": parse_latency(read(raw / "silver_warm.log")),
        "latency_cold_ms": parse_latency(read(raw / "silver.log")),
        "chaos": chaos,
        "integration_passed": integration == 0,
        "warm_reconcile_passed": warm_ok if warm_ran else None,
        "parity_passed": None if parity is None else parity == 0,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
