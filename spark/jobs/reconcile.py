"""Reconcile silver against the source database: row counts and per-row hashes.

    spark-submit jobs/reconcile.py [--table orders ...]

Compares every live (not soft-deleted) silver row with the source row of the same
primary key, hashing all source columns. Run it when the source is quiet and
silver has caught up. Exit code 0 only if every table matches 100%, apart from
keys explained by quarantined records (bad data deliberately withheld).
"""

from __future__ import annotations

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark
from tidewater_spark.tables import load_contracts


def row_hash(df: DataFrame, columns: list[str]) -> F.Column:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in columns]
    return F.sha2(F.concat_ws("\u0001", *parts), 256)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append")
    args = parser.parse_args()

    settings = Settings.from_env()
    contracts = load_contracts()
    spark = build_spark("tidewater-reconcile", settings)
    quarantine = (
        spark.read.format("delta").load(settings.quarantine_path)
        if DeltaTable.isDeltaTable(spark, settings.quarantine_path)
        else None
    )

    failed = False
    header = f"{'table':<13}{'source':>9}{'silver':>9}{'missing':>9}{'extra':>7}{'diff':>6}"
    print(f"\n{header}{'dup_pk':>8}{'explained':>11}  result")
    for name in args.table or sorted(contracts):
        pk = list(contracts[name].primary_key)
        source = spark.read.jdbc(
            settings.jdbc_url, f"public.{name}", properties=settings.jdbc_props
        )
        columns = source.columns
        path = settings.silver_path(name)
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"{name:<13} silver table missing")
            failed = True
            continue
        silver_all = spark.read.format("delta").load(path)
        dup_pk = silver_all.count() - silver_all.select(*pk).distinct().count()
        silver = silver_all.where(~F.col("is_deleted")).select(*columns)

        s = source.select(*pk, row_hash(source, columns).alias("h_src"))
        t = silver.select(*pk, row_hash(silver, columns).alias("h_silver"))
        cmp = s.join(t, pk, "full_outer").cache()
        missing = cmp.where(F.col("h_silver").isNull()).select(*pk)
        extra = cmp.where(F.col("h_src").isNull()).select(*pk)
        diff = cmp.where(F.col("h_src") != F.col("h_silver")).select(*pk)
        bad_keys = missing.unionByName(extra).unionByName(diff).distinct()

        explained = 0
        if quarantine is not None:
            q_keys = (
                quarantine.where(F.col("source_table") == name)
                .select(*[F.get_json_object("record_json", f"$.{c}").alias(c) for c in pk])
                .distinct()
            )
            cast_keys = bad_keys.select(*[F.col(c).cast("string").alias(c) for c in pk])
            explained = cast_keys.join(q_keys, pk, "left_semi").count()

        n_missing, n_extra, n_diff, n_bad = (
            missing.count(),
            extra.count(),
            diff.count(),
            bad_keys.count(),
        )
        ok = dup_pk == 0 and n_bad == explained
        failed |= not ok
        print(
            f"{name:<13}{source.count():>9}{silver.count():>9}{n_missing:>9}{n_extra:>7}"
            f"{n_diff:>6}{dup_pk:>8}{explained:>11}  {'PASS' if ok else 'FAIL'}"
        )
        if n_bad > explained:
            print(f"  sample unexplained keys in {name}:")
            bad_keys.limit(5).show(truncate=False)
        cmp.unpersist()

    print("\nRESULT:", "FAIL" if failed else "PASS (100% match: counts and row hashes)")
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
