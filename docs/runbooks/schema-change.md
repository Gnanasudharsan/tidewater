# Runbook: a source schema changes

**Chaos test:** [03_add_column_live.sh](../../tests/chaos/03_add_column_live.sh)

> **Always backfill after `ADD COLUMN ... DEFAULT`.** Postgres 11+ applies it as a
> metadata-only change: existing rows get the default without being rewritten, so
> no WAL events are emitted and CDC never sees their new value. Chaos test 03
> caught this: 12,924 customer rows mismatched until the table was backfilled.
> Procedure: `ALTER TABLE ...`, then `make backfill ARGS="--table <table>"`.
> (`DROP COLUMN` is metadata-only too; silver keeps the column as history.)

**Expected (compatible) changes:** adding a nullable or defaulted column. The
registry accepts a new version (BACKWARD), bronze archives the new writer schema,
silver decodes each schema id separately and auto-merges the new column, and the
warehouse loader `ALTER`s the target table.

**Incompatible changes** (type changes, renames, dropping a column without a
default) are rejected by the registry. The Debezium task fails with a
compatibility error, which is the data contract working. To ship one on purpose:

1. Agree on the change with consumers; add a new column instead of changing one.
2. If unavoidable, set the subject's compatibility to `NONE` for the change,
   restart the connector, and set it back to `BACKWARD` immediately.
3. Silver keeps old and new columns side by side; update dbt staging models.

**Verify:** `make reconcile` compares every source column, so a missing column fails it.
