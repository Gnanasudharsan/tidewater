#!/usr/bin/env bash
# Chaos 06: a full backfill of every table runs while the stream is merging live
# changes. Both use the same LSN-guarded MERGE; conflicts are retried; the result
# must reconcile 100%.             tests/chaos/06_concurrent_backfill.sh [rate=50]
source "$(dirname "$0")/lib.sh"
RATE="${1:-50}"

start_sim "$RATE"; sleep 20
log "Full backfill (all tables, delete detection) concurrent with live CDC"
run_backfill --detect-deletes
sleep 20; stop_sim
reconcile_until_pass 20
verify_bronze
log "PASS: concurrent backfill + stream reconcile 100%"
