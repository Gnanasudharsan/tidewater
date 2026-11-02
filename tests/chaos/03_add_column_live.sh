#!/usr/bin/env bash
# Chaos 03: a column with a DEFAULT is added to a source table under live load.
#
# Postgres (11+) applies `ADD COLUMN ... DEFAULT` as a metadata-only change: existing
# rows get the default WITHOUT being rewritten, so no WAL events are produced for
# them and CDC never sees their new value. Only rows changed afterwards carry it.
# The correct procedure is therefore: ALTER, then backfill the table. This test
# asserts the whole procedure: registry accepts the new schema (BACKWARD), silver
# evolves, the backfill fills the gap, reconciliation includes the new column.
#                                   tests/chaos/03_add_column_live.sh [rate=50]
source "$(dirname "$0")/lib.sh"
RATE="${1:-50}"; COL="loyalty_tier_$(date +%s)"
versions() {
  curl -s "localhost:8081/apis/ccompat/v7/subjects/tidewater.public.customers-value/versions" |
    python3 -c 'import json, sys; print(len(json.load(sys.stdin)))'
}

start_sim "$RATE"; sleep 20
v_before=$(versions)
log "ALTER TABLE customers ADD COLUMN $COL ... DEFAULT 'standard' (live)"
src_sql "alter table customers add column $COL text not null default 'standard'" >/dev/null
src_sql "update customers set $COL = 'gold' where customer_id % 7 = 0" >/dev/null
sleep 45
v_after=$(versions)
log "customers-value schema versions: $v_before -> $v_after"
[ "$v_after" -gt "$v_before" ] || fail "no new schema version was registered"

log "Backfilling customers (rows untouched since the ALTER emitted no CDC events)"
run_backfill --table customers
sleep 20; stop_sim
reconcile_until_pass 20    # compares every source column, including the new one
verify_bronze

log "Cleanup: dropping $COL (also metadata-only; silver keeps the column as history)"
src_sql "alter table customers drop column $COL" >/dev/null
log "PASS: live column $COL propagated end to end (ALTER + backfill procedure)"
