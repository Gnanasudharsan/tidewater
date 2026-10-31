# ADR 0002: Soft deletes in silver

Status: Accepted · Date: 2026-10-31

## Context
Debezium emits `op='d'` events when a source row is deleted. Silver can either
remove the row (hard delete) or keep it and mark it deleted (soft delete).

## Decision
Silver applies deletes as **soft deletes**: `is_deleted = true`, `deleted_at`
set from the source `ts_ms`, and the delete's LSN stored like any other change.

## Rationale
- **Ordering safety:** if the row is removed, a late, older `update` for the same
  key would re-insert it. Keeping the row keeps its LSN, so the guard
  `incoming.lsn > target.lsn` rejects the stale update.
- **Auditability:** the audit trace can show that an order existed and when it
  was deleted.
- **Reconciliation:** the reconciler compares `source` rows to
  `silver WHERE NOT is_deleted`, and can still count deletes separately.

## Consequences
- Every downstream consumer must filter `is_deleted = false`. dbt staging models
  do this once so marts never see deleted rows.
- Silver grows without bound. A retention job can hard-delete tombstoned rows
  older than N days (later than any plausible late event).
- Debezium tombstone records are disabled (`tombstones.on.delete=false`) because
  topics are not log-compacted and the delete event carries everything needed.
