"""Decode raw bronze CDC records (Debezium envelopes in Confluent-framed Avro) into typed rows."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from tidewater_spark.registry import row_temporal_fields
from tidewater_spark.schema_archive import LEGACY_RUN

# Silver bookkeeping columns. Everything else in a silver table is a source column.
META_COLUMNS = [
    "is_deleted",
    "deleted_at",
    "_op",
    "_lsn",
    "_tx_id",
    "_source_ts",
    "_source_topic",
    "_source_partition",
    "_source_offset",
    "_schema_id",
    "_silver_updated_at",
]

# Bronze lineage carried along so quarantined rows can be traced back to Kafka.
LINEAGE = ["_topic", "_partition", "_offset", "_value_schema_id", "value"]


def _temporal(col: Column, kind: str) -> Column:
    if kind == "zoned_timestamp":
        return col.cast("timestamp")
    if kind == "micro_timestamp":
        return F.timestamp_micros(col)
    if kind == "milli_timestamp":
        return F.timestamp_millis(col)
    if kind == "date":
        return F.date_add(F.lit("1970-01-01").cast("date"), col)
    raise ValueError(kind)


def business_columns(df: DataFrame) -> list[str]:
    hidden = set(META_COLUMNS) | set(LINEAGE)
    return [c for c in df.columns if c not in hidden and not c.startswith("_")]


def silver_columns(df: DataFrame) -> list[str]:
    """Columns written to a silver table (source columns + bookkeeping, no raw lineage)."""
    return business_columns(df) + [c for c in META_COLUMNS if c in df.columns]


def ingest_run(df: DataFrame) -> Column:
    """The bronze ingest run of each row (rows from before runs were tagged are v1)."""
    tag = F.col("_ingest_run") if "_ingest_run" in df.columns else F.lit(None).cast("string")
    return F.coalesce(tag, F.lit(LEGACY_RUN))


def decode_schema_group(bronze: DataFrame, run: str, schema_id: int, schema_json: str) -> DataFrame:
    """Decode rows written with one schema id. Returns rows plus `_decoded_ok`.

    The schema-id check lives INSIDE the expression (CASE WHEN), not only in a
    filter: Spark may combine and reorder filters, and from_avro run on another
    table's bytes reads garbage lengths and can exhaust the heap. CASE WHEN is
    the one construct Spark guarantees to evaluate lazily.
    """
    temporal = row_temporal_fields(schema_json)
    decoded = from_avro(F.expr("substring(value, 6)"), schema_json, {"mode": "PERMISSIVE"})
    env = bronze.select(
        *LINEAGE,
        F.when(
            (F.col("_value_schema_id") == F.lit(schema_id)) & (ingest_run(bronze) == F.lit(run)),
            decoded,
        ).alias("e"),
    )
    row_fields = env.schema["e"].dataType["before"].dataType.fieldNames()
    # Deletes carry the last row image in `before` (REPLICA IDENTITY FULL).
    image = F.coalesce(F.col("e.after"), F.col("e.before"))
    columns = [
        _temporal(image[f], temporal[f]).alias(f) if f in temporal else image[f].alias(f)
        for f in row_fields
    ]
    is_delete = F.col("e.op") == "d"
    return env.select(
        *columns,
        F.coalesce(is_delete, F.lit(False)).alias("is_deleted"),
        F.when(is_delete, F.timestamp_millis(F.col("e.source.ts_ms"))).alias("deleted_at"),
        F.col("e.op").alias("_op"),
        F.col("e.source.lsn").alias("_lsn"),
        F.col("e.source.txId").alias("_tx_id"),
        F.timestamp_millis(F.col("e.source.ts_ms")).alias("_source_ts"),
        F.col("_topic").alias("_source_topic"),
        F.col("_partition").alias("_source_partition"),
        F.col("_offset").alias("_source_offset"),
        F.col("_value_schema_id").alias("_schema_id"),
        *LINEAGE,
        (
            F.col("e").isNotNull()
            & F.col("e.op").isin("c", "u", "d", "r")
            & F.col("e.source.lsn").isNotNull()
            & image.isNotNull()
        ).alias("_decoded_ok"),
    )


def decode_table(bronze: DataFrame, schemas) -> tuple[DataFrame | None, DataFrame | None]:
    """Decode one table's bronze rows. Returns (changes, undecodable).

    `schemas.lookup(run, schema_id)` returns the writer schema (normally a
    SchemaArchive). Each (ingest run, schema id) is decoded separately (a column
    added at the source creates a new schema id; IDs are only unique within one
    registry's lifetime) and the results are unioned by name, so new columns flow
    through as nulls for older events.
    """
    keyed = bronze.withColumn("_run", ingest_run(bronze))
    groups = [(r[0], r[1]) for r in keyed.select("_run", "_value_schema_id").distinct().collect()]
    decoded, bad = [], []
    for run, sid in groups:
        in_run = keyed.where(F.col("_run") == run).drop("_run")
        group = in_run.where(
            F.col("_value_schema_id").isNull() if sid is None else F.col("_value_schema_id") == sid
        )
        reason = None
        if sid is None:
            reason = "not_confluent_avro"
        else:
            try:
                schema_json = schemas.lookup(run, sid)
            except Exception:  # noqa: BLE001 - registry 404s and network errors both quarantine
                reason = f"unknown_schema_id:{sid}"
        if reason:
            bad.append(group.select(*LINEAGE, F.array(F.lit(reason)).alias("_dq_failures")))
            continue
        rows = decode_schema_group(group, run, sid, schema_json)
        decoded.append(rows.where("_decoded_ok").drop("_decoded_ok"))
        bad.append(
            rows.where(~F.col("_decoded_ok")).select(
                *LINEAGE, F.array(F.lit("undecodable")).alias("_dq_failures")
            )
        )

    def union(frames):
        if not frames:
            return None
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f, allowMissingColumns=True)
        return out

    return union(decoded), union(bad)
