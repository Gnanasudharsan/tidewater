# ADR 0010: Bronze archives its own Avro writer schemas

Status: Accepted · Date: 2026-11-02

## Context
Bronze stores raw Confluent-framed Avro: a 5-byte header with a schema registry ID,
then the payload. The ID is only meaningful inside one registry's lifetime. After the
[Kafka log loss](../incidents/2026-11-01-kafka-log-loss.md) the registry was rebuilt
and IDs were reassigned: **ID 9 meant `orders` before the incident and `order_items`
after it.** Replaying pre-incident bronze through the live registry would decode
orders with the order_items schema.

## Decision
- Bronze keeps a Delta table, `bronze/_schemas`, keyed by `(ingest_run, schema_id)`
  and holding the full writer schema and its SHA-256 fingerprint (canonical JSON).
  The bronze stream archives every ID the first time it sees it in a run.
- Silver decodes with `SchemaArchive.lookup(run, id)`: archive first, live registry
  only as a fallback for IDs not yet archived.
- Decoding is grouped by `(ingest_run, schema_id)`, and the `CASE WHEN` guard checks both.
- A one-time job (`jobs/seed_schema_archive.py`) recovered the pre-incident run's
  schemas from each subject's latest version. Each candidate was **verified by
  decoding 500 real records** with FAILFAST before being archived.

## Rationale
- Bronze becomes self-describing: replays depend only on the lakehouse, not on a
  registry that may be rebuilt, migrated or vendor-swapped.
- Fingerprints make identical schemas recognisable across runs (the pre- and
  post-incident schemas for each table have the same fingerprint).
- Archiving is idempotent and best-effort: if the registry is unreachable, the next
  batch retries, and ingestion never fails because of it.

## Consequences
- Chaos test 05 rebuilt silver from both ingest runs with 0 quarantined rows.
- A schema change creates a new archive row the first time the new ID is seen.
- The archive must be backed up with bronze; losing it reintroduces the original problem.
