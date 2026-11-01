"""Read what changed in a Delta table between two versions (Change Data Feed)."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
CDF_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


def ensure_cdf(spark: SparkSession, path: str) -> None:
    props = DeltaTable.forPath(spark, path).detail().first()["properties"]
    if props.get("delta.enableChangeDataFeed") != "true":
        spark.sql(
            f"alter table delta.`{path}` set tblproperties (delta.enableChangeDataFeed = true)"
        )
        log.info("enabled change data feed on %s", path)


def changed_rows(
    spark: SparkSession, path: str, pk: tuple[str, ...], since: int | None, current: int
) -> tuple[DataFrame, str]:
    if since is not None:
        try:
            cdf = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", since + 1)
                .option("endingVersion", current)
                .load(path)
                .where(F.col("_change_type").isin("insert", "update_postimage"))
            )
            # Plans are lazy: force the CDF range to be resolved here, so a missing
            # range raises inside this try instead of later in the caller.
            cdf.limit(1).collect()
            w = Window.partitionBy(*pk).orderBy(F.col("_commit_version").desc())
            latest = cdf.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            return latest.drop("_rn", *CDF_COLUMNS), f"cdf {since + 1}..{current}"
        except Exception as exc:  # noqa: BLE001 - e.g. CDF vacuumed or not enabled then
            log.warning("CDF unavailable for %s (%s); falling back to a full load", path, exc)
    snapshot = spark.read.format("delta").option("versionAsOf", current).load(path)
    return snapshot, f"full @ {current}"
