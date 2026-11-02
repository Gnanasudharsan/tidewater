"""The idempotent silver MERGE, shared by the stream and the backfill job.

A change is applied only if it is newer than what silver holds, where "newer" is
the order key (_lsn, _source_offset). That one rule makes the merge:
  - idempotent: replaying a batch changes nothing (equal keys are not newer),
  - order-insensitive: an older event arriving late is rejected,
  - safe to retry after a concurrent-write conflict, and safe to run concurrently
    with a backfill that stamps rows with an older LSN.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

ORDER_COLUMNS = ["_lsn", "_source_offset"]
NEWER = "s._lsn > t._lsn OR (s._lsn = t._lsn AND s._source_offset > t._source_offset)"


def latest_per_key(changes: DataFrame, pk: tuple[str, ...]) -> DataFrame:
    """Keep only the newest change per primary key within a batch."""
    w = Window.partitionBy(*pk).orderBy(*[F.col(c).desc() for c in ORDER_COLUMNS])
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def classify_against_target(
    spark: SparkSession, path: str, changes: DataFrame, pk: tuple[str, ...]
) -> tuple[DataFrame, DataFrame]:
    """Split deduped changes into (to_merge, late).

    late: strictly older than the row already in silver. They would be rejected by
    the MERGE guard anyway; they are split out so late data is recorded, not silent.
    Exact replays (equal order key) are neither: they are dropped quietly.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        late = changes.limit(0).select(
            "*",
            F.lit(None).cast("long").alias("_target_lsn"),
            F.lit(None).cast("timestamp").alias("_target_source_ts"),
        )
        return changes, late
    target = (
        spark.read.format("delta")
        .load(path)
        .select(
            *pk,
            *[F.col(c).alias(f"_t{c}") for c in ORDER_COLUMNS],
            F.col("_source_ts").alias("_t_ts"),
        )
    )
    joined = changes.alias("s").join(target.alias("t"), list(pk), "left")
    t_lsn, t_off = F.col("_t_lsn"), F.col("_t_source_offset")
    newer = (
        t_lsn.isNull()
        | (F.col("_lsn") > t_lsn)
        | ((F.col("_lsn") == t_lsn) & (F.col("_source_offset") > t_off))
    )
    older = (F.col("_lsn") < t_lsn) | ((F.col("_lsn") == t_lsn) & (F.col("_source_offset") < t_off))
    to_merge = joined.where(newer).drop("_t_lsn", "_t_source_offset", "_t_ts")
    late = (
        joined.where(older)
        .select("*", t_lsn.alias("_target_lsn"), F.col("_t_ts").alias("_target_source_ts"))
        .drop("_t_lsn", "_t_source_offset", "_t_ts")
    )
    return to_merge, late


# Every subclass of Delta's DeltaConcurrentModificationException: optimistic-concurrency
# losers that are safe to retry because the MERGE is idempotent. MetadataChanged covers
# a concurrent schema evolution (e.g. silver auto-merging a new source column).
DELTA_CONFLICTS = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "DeltaConcurrentModificationException",
)


def is_write_conflict(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}"
    return any(name in text for name in DELTA_CONFLICTS)


def with_conflict_retries(
    action: Callable[[], None],
    *,
    what: str,
    max_attempts: int = 10,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run `action`, retrying Delta optimistic-concurrency conflicts with jittered backoff.

    Retrying is only correct because the merge is idempotent. Returns attempts used.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            action()
            return attempt
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is a write conflict
            if not is_write_conflict(exc) or attempt == max_attempts:
                raise
            delay = min(max_delay, base_delay * 2 ** (attempt - 1)) * (0.5 + random.random())
            log.warning("%s: write conflict (attempt %d), retrying in %.1fs", what, attempt, delay)
            sleep(delay)
    raise AssertionError("unreachable")


def merge_into_silver(
    spark: SparkSession,
    path: str,
    changes: DataFrame,
    pk: tuple[str, ...],
    *,
    what: str,
    max_attempts: int = 10,
) -> int:
    """Upsert deduped changes into a silver table with the LSN guard. Returns attempts used."""
    changes = changes.withColumn("_silver_updated_at", F.current_timestamp())
    condition = " AND ".join(f"t.`{c}` = s.`{c}`" for c in pk)

    def run() -> None:
        if not DeltaTable.isDeltaTable(spark, path):
            try:
                changes.write.format("delta").mode("errorifexists").save(path)
                return
            except Exception as exc:  # noqa: BLE001
                # Someone else (stream vs backfill) created it first: fall through to MERGE.
                if "already exists" not in str(exc) and not is_write_conflict(exc):
                    raise
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(changes.alias("s"), condition)
            .whenMatchedUpdateAll(condition=NEWER)
            .whenNotMatchedInsertAll()
            .execute()
        )

    return with_conflict_retries(run, what=what, max_attempts=max_attempts)
