"""emit_results.py parses real run outputs; unmeasured values must stay null, never 0."""

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("emit_results", ROOT / "scripts" / "emit_results.py")
emit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(emit)

RECONCILE_PASS = """
table           source   silver  missing  extra  diff  dup_pk  explained  result
customers          120      120        0      0     0       0          0  PASS
orders             400      400        0      0     0       0          0  PASS
payments           380      380        0      0     0       0          0  PASS

RESULT: PASS (100% match: counts and row hashes)
"""


def batch_line(stats: dict) -> str:
    return f"2026-11-07 10:00:41,100 INFO silver batch {stats!r}"  # silver_stream.py's format


SILVER_LOG = "\n".join(
    [
        "2026-11-07 10:00:01,100 INFO waiting for Delta table at s3a://lakehouse/bronze (0s)",
        batch_line({"batch": 0, "orders": 120, "latency_p95_s": 61.5, "latency_p50_s": 40.2}),
        batch_line({"batch": 1, "orders": 80, "latency_p95_s": 14.0, "latency_p50_s": 9.0}),
        batch_line({"batch": 2, "duration_s": 0.4}),
        batch_line({"batch": 3, "orders": 5, "latency_p95_s": 12.0, "latency_p50_s": 8.0}),
    ]
)


def test_reconcile_totals():
    assert emit.parse_reconcile(RECONCILE_PASS) == {
        "rows_reconciled": 900,
        "duplicates_in_silver": 0,
        "hash_match_pct": 100.0,
    }


def test_failed_or_missing_reconcile_is_unmeasured():
    failed = RECONCILE_PASS.replace("RESULT: PASS", "RESULT: FAIL")
    for text in (failed, None, ""):
        assert set(emit.parse_reconcile(text).values()) == {None}


def test_latency_median_p50_and_worst_p95():
    assert emit.parse_latency(SILVER_LOG) == {"p50": 9000, "p95": 61500, "batches": 3}
    assert emit.parse_latency("no batches here") is None


def test_end_to_end_failed_run_keeps_nulls(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    (raw / "chaos").mkdir(parents=True)
    (raw / "integration.exit").write_text("1\n")
    (raw / "parity.exit").write_text("0\n")
    (raw / "chaos" / "01_kill_bronze_mid_batch.exit").write_text("0")
    (raw / "chaos" / "01_kill_bronze_mid_batch.log").write_text("[10:00:00] PASS: 3 kills\n")
    out = tmp_path / "latest.json"
    monkeypatch.setattr(
        sys, "argv", ["x", "--raw", str(raw), "--events", "1000", "--out", str(out)]
    )
    emit.main()
    r = json.loads(out.read_text())
    assert r["status"] == "failed"
    assert r["events_processed"] is None and r["rows_reconciled"] is None
    assert r["latency_ms"] is None and r["latency_cold_ms"] is None
    assert r["warm_reconcile_passed"] is None
    assert r["chaos"] == [
        {
            "name": "01_kill_bronze_mid_batch",
            "passed": True,
            "notes": "PASS: 3 kills",
            "seconds": None,
        }
    ]


def run_emit(tmp_path, monkeypatch, warm_reconcile):
    raw = tmp_path / "raw"
    (raw / "chaos").mkdir(parents=True)
    (raw / "integration.exit").write_text("0")
    (raw / "parity.exit").write_text("0")
    (raw / "reconcile.out").write_text(RECONCILE_PASS)
    (raw / "silver.log").write_text(SILVER_LOG)
    (raw / "reconcile_warm.out").write_text(warm_reconcile)
    (raw / "silver_warm.log").write_text(
        batch_line({"batch": 9, "latency_p95_s": 18.2, "latency_p50_s": 11.0})
    )
    out = tmp_path / "latest.json"
    argv = ["x", "--raw", str(raw), "--events", "1000", "--warm-events", "600", "--out", str(out)]
    monkeypatch.setattr(sys, "argv", argv)
    emit.main()
    return json.loads(out.read_text())


def test_warm_wave_gives_headline_latency_and_totals(tmp_path, monkeypatch):
    r = run_emit(tmp_path, monkeypatch, RECONCILE_PASS.replace("400      400", "700      700"))
    assert r["status"] == "passed"
    assert r["latency_ms"] == {"p50": 11000, "p95": 18200, "batches": 1}
    assert r["latency_cold_ms"] == {"p50": 9000, "p95": 61500, "batches": 3}
    assert r["events_processed"] == 1600 and r["rows_reconciled"] == 1200


def test_failed_warm_reconcile_fails_the_run(tmp_path, monkeypatch):
    r = run_emit(tmp_path, monkeypatch, RECONCILE_PASS.replace("RESULT: PASS", "RESULT: FAIL"))
    assert r["status"] == "failed" and r["warm_reconcile_passed"] is False
    assert r["events_processed"] == 1000 and r["rows_reconciled"] == 900
