"""Trace one order from the dbt mart back to its Kafka offsets and Postgres WAL LSNs.

    spark-submit jobs/audit_trace.py --order-id 1234 [--json]
"""

from __future__ import annotations

import argparse
import json

from tidewater_spark.audit import trace_order
from tidewater_spark.config import Settings
from tidewater_spark.registry import SchemaRegistry
from tidewater_spark.schema_archive import SchemaArchive
from tidewater_spark.session import build_spark


def show(title: str, value) -> None:
    print(f"\n== {title}")
    if isinstance(value, dict):
        for k, v in value.items():
            print(f"   {k:<26} {v}")
    else:
        print(f"   {value}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id", type=int, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    settings = Settings.from_env()
    spark = build_spark("tidewater-audit-trace", settings)
    schemas = SchemaArchive(
        spark, settings.schema_archive_path, SchemaRegistry(settings.schema_registry_url)
    )
    t = trace_order(spark, settings, schemas, args.order_id)
    if args.json:
        print("TRACE_JSON " + json.dumps(t, default=str))
        return
    show("1. dbt mart (marts.fct_orders + SCD2 dim_customer as of order time)", t["mart"])
    show("2. warehouse silver (silver.orders in Postgres)", t["warehouse_silver"])
    show("3. silver Delta row", t["silver"])
    show("   silver Delta commit that last changed it (Change Data Feed)", t["silver_commit"])
    print(f"\n== 4. bronze CDC events for this order ({len(t['bronze_events'])}), oldest first")
    print(
        f"   {'op':<3}{'status':<11}{'lsn':>12}  {'kafka (topic/partition/offset)':<38}{'schema'}"
    )
    for e in t["bronze_events"]:
        coords = f"{e['_source_topic']}/{e['_source_partition']}/{e['_source_offset']}"
        print(f"   {e['_op']:<3}{e['status']:<11}{e['_lsn']:>12}  {coords:<38}{e['_schema_id']}")
    show("5. source row (Postgres) and current WAL position", t["source"])
    show("silver holds", t["silver_version_from"])
    spark.stop()


if __name__ == "__main__":
    main()
