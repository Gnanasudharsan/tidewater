"""Nightly lakehouse maintenance: compact small files (OPTIMIZE) and drop unreferenced
files past the retention window (VACUUM), for bronze, silver and the audit logs.

    spark-submit jobs/maintenance.py [--retain-hours 168] [--skip-vacuum]

Streaming appends every few seconds create many small files; compaction keeps
reads fast. OPTIMIZE commits carry dataChange=false, so streaming readers and the
Change Data Feed ignore them. Conflicts with concurrent writers are retried.
"""

from __future__ import annotations

import argparse
import json
import logging

from delta.tables import DeltaTable

from tidewater_spark.config import Settings
from tidewater_spark.merge import with_conflict_retries
from tidewater_spark.session import build_spark
from tidewater_spark.tables import load_contracts

log = logging.getLogger("maintenance")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retain-hours", type=int, default=168)
    parser.add_argument("--skip-vacuum", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    spark = build_spark("tidewater-maintenance", settings)
    paths = {"bronze": settings.bronze_path}
    paths |= {f"silver.{t}": settings.silver_path(t) for t in sorted(load_contracts())}
    paths |= {"quarantine": settings.quarantine_path, "late_arrivals": settings.late_arrivals_path}

    report = {}
    for name, path in paths.items():
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        table = DeltaTable.forPath(spark, path)
        files_before = table.detail().first()["numFiles"]
        with_conflict_retries(lambda t=table: t.optimize().executeCompaction(), what=name)
        if not args.skip_vacuum:
            table.vacuum(args.retain_hours)
        files_after = DeltaTable.forPath(spark, path).detail().first()["numFiles"]
        report[name] = {"files_before": files_before, "files_after": files_after}
        log.info("%s: %d -> %d files", name, files_before, files_after)

    # One JSON line, easy to parse from orchestrator logs.
    print("MAINTENANCE_REPORT " + json.dumps(report))
    spark.stop()


if __name__ == "__main__":
    main()
