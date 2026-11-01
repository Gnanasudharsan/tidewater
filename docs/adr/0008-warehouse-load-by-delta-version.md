# ADR 0008: Load the warehouse by Delta version, not by timestamp

Status: Accepted · Date: 2026-11-01

## Context
Silver is copied into the Postgres warehouse (and later Snowflake) for dbt. The
common approach is a high-watermark on an "updated at" column:
`where _silver_updated_at > last_loaded`. Silver has two concurrent writers
(the stream and the backfill). Writer A can stamp `_silver_updated_at = 10:00:01`
and commit *after* writer B committed rows stamped `10:00:02`. A loader that ran
in between and advanced its watermark to 10:00:02 never sees A's rows.

## Decision
- Every silver table has **Delta Change Data Feed** enabled.
- The loader keeps a bookmark per table: the last loaded **Delta version**
  (`silver._load_state`). Each run reads CDF from `bookmark + 1` to the current
  version and keeps the newest post-image per primary key. With no bookmark, or
  when CDF for the range is gone (vacuumed), it does a full snapshot load.
- Rows go to a staging table over JDBC. Then, in **one Postgres transaction**,
  the loader evolves the target schema, upserts from staging, and moves the
  bookmark.

## Rationale
- Delta versions are a total order of commits, so nothing committed can be
  skipped, whatever its timestamps say.
- Bookmark and data commit atomically: a crash before commit replays the range,
  a crash after commit replays nothing. The upsert is idempotent either way.
- The upsert carries the same `(lsn, offset)` guard as silver, so even a full
  reload can never move a warehouse row backwards.

## Consequences
- CDF writes extra `_change_data` files for MERGE commits (more storage, a little
  more write time). It is enabled by default on new tables via the session config.
- Deletes in silver are soft (updates), so the loader never needs CDF `delete`
  rows. If hard deletes are ever added, the loader must handle them.
- The warehouse lags silver by up to one loader interval; dbt freshness
  thresholds (`_sources.yml`) alert when that lag grows.
