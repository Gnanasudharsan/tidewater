"""Seed the schema archive for a bronze ingest run whose schemas were never archived.

    spark-submit jobs/seed_schema_archive.py --run bronze_cdc_v2
    spark-submit jobs/seed_schema_archive.py --run bronze_cdc_v1 --from-subject-latest

Default: look each schema id up in the current registry (right for the current run).
--from-subject-latest: for a run whose registry is gone, use the latest schema of the
table's subject instead. That is only valid if the table's schema has not changed
since, so every candidate is VERIFIED by decoding real bronze records with it
(FAILFAST); nothing is archived unless every sampled record decodes.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from tidewater_spark.cdc import ingest_run
from tidewater_spark.config import Settings
from tidewater_spark.registry import SchemaRegistry
from tidewater_spark.schema_archive import SchemaArchive, fingerprint
from tidewater_spark.session import build_spark

log = logging.getLogger("seed_schema_archive")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--from-subject-latest", action="store_true")
    parser.add_argument("--sample", type=int, default=500)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    spark = build_spark("tidewater-seed-schema-archive", settings)
    registry = SchemaRegistry(settings.schema_registry_url)
    bronze = spark.read.format("delta").load(settings.bronze_path)
    rows = bronze.where(ingest_run(bronze) == args.run).where(F.col("_value_schema_id").isNotNull())
    pairs = rows.select("source_table", "_value_schema_id").distinct().collect()

    verified = {}
    for table, sid in sorted(pairs):
        if args.from_subject_latest:
            schema = registry.latest_for_subject(f"tidewater.public.{table}-value")
        else:
            schema = registry.schema_json(sid)
        sample = rows.where(
            (F.col("source_table") == table) & (F.col("_value_schema_id") == sid)
        ).limit(args.sample)
        try:
            n = (
                sample.select(
                    from_avro(F.expr("substring(value, 6)"), schema, {"mode": "FAILFAST"}).alias(
                        "e"
                    )
                )
                .where("e.op is not null")
                .count()
            )
        except Exception as exc:  # noqa: BLE001
            log.error("%s id %s: candidate schema does NOT decode its records: %s", table, sid, exc)
            return 1
        log.info(
            "%s id %s: verified on %d records (fingerprint %s)",
            table,
            sid,
            n,
            fingerprint(schema)[:12],
        )
        verified[sid] = schema

    added = SchemaArchive(spark, settings.schema_archive_path).archive(args.run, verified)
    log.info("archived %d schema(s) for %s", added, args.run)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
