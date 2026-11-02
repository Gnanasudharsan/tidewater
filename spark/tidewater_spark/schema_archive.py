"""Bronze's own record of every Avro writer schema it has seen, keyed by (ingest_run, id).

A schema registry ID is only meaningful inside one registry's lifetime: after the
2026-11-01 Kafka log loss the registry was rebuilt and IDs were reassigned. Bronze
rows store the ID, so replaying old bronze through a new registry would decode
bytes with the wrong schema. The archive makes bronze self-describing: silver
decodes from it first and only asks the registry about IDs it has never seen.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

LEGACY_RUN = "bronze_cdc_v1"  # bronze rows written before runs were tagged


def fingerprint(schema_json: str) -> str:
    """Content address of a schema: SHA-256 of its canonical (sorted, compact) JSON."""
    canonical = json.dumps(json.loads(schema_json), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class SchemaArchive:
    def __init__(self, spark: SparkSession, path: str, registry=None):
        self.spark, self.path, self.registry = spark, path, registry
        self._known: dict[tuple[str, int], str] = {}
        self._loaded = False

    def _load(self) -> None:
        if DeltaTable.isDeltaTable(self.spark, self.path):
            rows = self.spark.read.format("delta").load(self.path).collect()
            self._known = {(r.ingest_run, r.schema_id): r.schema_json for r in rows}
        self._loaded = True

    def lookup(self, run: str, schema_id: int) -> str:
        """Writer schema for (run, id): archive first, registry only as a fallback."""
        key = (run or LEGACY_RUN, schema_id)
        if key not in self._known:
            self._load()
        if key in self._known:
            return self._known[key]
        if self.registry is None:
            raise KeyError(f"schema {key} is not archived")
        log.warning("schema %s not archived yet; using the live registry", key)
        return self.registry.schema_json(schema_id)

    def archive(self, run: str, schemas: dict[int, str]) -> int:
        """Record schemas for a run (idempotent: existing (run, id) rows are kept)."""
        if not self._loaded:
            self._load()
        new = {sid: s for sid, s in schemas.items() if (run, sid) not in self._known}
        if not new:
            return 0
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        df = self.spark.createDataFrame(
            [(run, sid, fingerprint(s), s, now) for sid, s in new.items()],
            "ingest_run string, schema_id int, fingerprint string, schema_json string, "
            "archived_at timestamp",
        )
        if DeltaTable.isDeltaTable(self.spark, self.path):
            (
                DeltaTable.forPath(self.spark, self.path)
                .alias("t")
                .merge(df.alias("s"), "t.ingest_run = s.ingest_run AND t.schema_id = s.schema_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").save(self.path)
        self._known.update({(run, sid): s for sid, s in new.items()})
        return len(new)


class RegistryOnly:
    """Adapter for callers (and tests) that resolve schemas from the registry alone."""

    def __init__(self, registry):
        self.registry = registry

    def lookup(self, run: str, schema_id: int) -> str:
        return self.registry.schema_json(schema_id)
