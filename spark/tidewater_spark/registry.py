"""Minimal Confluent-compatible schema registry client. Pure Python (no Spark).

Debezium's Avro converter registers the shared `Source` record as its own schema
and the envelope refers to it by name (a schema reference). Spark's from_avro
needs one self-contained schema, so references are fetched and inlined here.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from functools import lru_cache

# Debezium logical types that Avro (and therefore from_avro) sees as plain primitives.
DEBEZIUM_TEMPORAL = {
    "io.debezium.time.ZonedTimestamp": "zoned_timestamp",  # ISO-8601 string with offset
    "io.debezium.time.MicroTimestamp": "micro_timestamp",  # long, micros since epoch
    "io.debezium.time.Timestamp": "milli_timestamp",  # long, millis since epoch
    "io.debezium.time.Date": "date",  # int, days since epoch
}

Fetch = Callable[[str], dict]


def http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 - internal URL
        return json.load(resp)


def inline_references(schema: dict | list | str, definitions: dict[str, dict]) -> object:
    """Replace the first use of each referenced type name with its full definition.

    Later uses stay as name strings, which is valid Avro once the type is defined.
    """
    defined: set[str] = set()

    def walk(node):
        if isinstance(node, str):
            if node in definitions and node not in defined:
                defined.add(node)
                return walk(definitions[node])
            return node
        if isinstance(node, list):
            return [walk(n) for n in node]
        if isinstance(node, dict):
            out = dict(node)
            if node.get("type") == "record":
                name, ns = node["name"], node.get("namespace")
                fullname = name if "." in name or not ns else f"{ns}.{name}"
                defined.add(fullname)
                out["fields"] = [{**f, "type": walk(f["type"])} for f in node["fields"]]
            elif "type" in node and isinstance(node["type"], dict | list | str):
                out["type"] = walk(node["type"])
            if "items" in node:
                out["items"] = walk(node["items"])
            if "values" in node and node.get("type") == "map":
                out["values"] = walk(node["values"])
            return out
        return node

    return walk(schema)


class SchemaRegistry:
    def __init__(self, base_url: str, fetch: Fetch = http_get_json):
        self.base_url = base_url.rstrip("/")
        self._fetch = fetch
        self.schema_json = lru_cache(maxsize=512)(self._schema_json)

    def _definitions(self, references: list[dict]) -> dict[str, dict]:
        defs: dict[str, dict] = {}
        for ref in references or []:
            doc = self._fetch(
                f"{self.base_url}/subjects/{ref['subject']}/versions/{ref['version']}"
            )
            defs.update(self._definitions(doc.get("references", [])))
            defs[ref["name"]] = json.loads(doc["schema"])
        return defs

    def _schema_json(self, schema_id: int) -> str:
        """Self-contained Avro schema (JSON string) for a registry id."""
        doc = self._fetch(f"{self.base_url}/schemas/ids/{schema_id}")
        schema = json.loads(doc["schema"])
        defs = self._definitions(doc.get("references", []))
        return json.dumps(inline_references(schema, defs) if defs else schema)


def row_temporal_fields(envelope_schema_json: str) -> dict[str, str]:
    """Map column name -> Debezium temporal kind for the row record in a CDC envelope."""
    envelope = json.loads(envelope_schema_json)
    before = next(f for f in envelope["fields"] if f["name"] == "before")
    record = next(t for t in before["type"] if isinstance(t, dict))
    kinds = {}
    for f in record["fields"]:
        types = f["type"] if isinstance(f["type"], list) else [f["type"]]
        for t in types:
            if isinstance(t, dict) and t.get("connect.name") in DEBEZIUM_TEMPORAL:
                kinds[f["name"]] = DEBEZIUM_TEMPORAL[t["connect.name"]]
    return kinds
