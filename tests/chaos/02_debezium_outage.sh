#!/usr/bin/env bash
# Chaos 02: Debezium (Kafka Connect) is down while the source keeps changing.
# The replication slot must retain the WAL; on restart CDC resumes from the slot and
# silver ends 100% reconciled.      tests/chaos/02_debezium_outage.sh [down_s=600] [rate=50]
source "$(dirname "$0")/lib.sh"
DOWN="${1:-600}"; RATE="${2:-50}"
slot_lag() { src_sql "select pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint from pg_replication_slots where slot_name = 'tidewater_slot'"; }

start_sim "$RATE"; sleep 20
log "Stopping Debezium for ${DOWN}s"
$COMPOSE stop connect >/dev/null
before=$(slot_lag); sleep "$DOWN"; after=$(slot_lag)
log "WAL retained by the slot: ${before} -> ${after} bytes"
[ "$after" -gt "$before" ] || fail "slot did not retain WAL during the outage"

log "Restarting Debezium"
$COMPOSE up -d --wait connect >/dev/null
until curl -sf localhost:8083/connectors/tidewater-postgres-cdc/status | grep -q '"state":"RUNNING".*"state":"RUNNING"'; do sleep 3; done
sleep 30; stop_sim
log "Slot lag after catch-up: $(slot_lag) bytes"
reconcile_until_pass 20
verify_bronze
log "PASS: ${DOWN}s Debezium outage, no data lost"
