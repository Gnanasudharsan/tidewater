#!/usr/bin/env bash
# Shared helpers for the chaos suite. Source from a test script.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

COMPOSE="docker compose"
SIM=tidewater-chaos-sim
SUBMIT=(/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g)
set -a; . ./.env; set +a

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; exit 1; }
spark_job() { $COMPOSE run --rm --no-deps spark-bronze "${SUBMIT[@]}" "$@"; }
src_sql() { $COMPOSE exec -T postgres-src psql -U "$SRC_PG_USER" -d "$SRC_PG_DB" -Atc "$1"; }

start_sim() {  # start_sim <rate>
  docker rm -f "$SIM" >/dev/null 2>&1 || true
  $COMPOSE --profile sim run -d --name "$SIM" simulator --rate "$1" --duration 0 >/dev/null 2>&1
  trap 'docker rm -f "$SIM" >/dev/null 2>&1 || true' EXIT
  log "simulator running at $1 events/s"
}
stop_sim() { docker stop "$SIM" >/dev/null 2>&1 || true; log "simulator stopped"; }

# Retry until silver matches the source 100% (it may still be catching up).
reconcile_until_pass() {  # reconcile_until_pass [attempts]
  local attempts="${1:-12}"
  for i in $(seq 1 "$attempts"); do
    if spark_job jobs/reconcile.py >/tmp/chaos_reconcile.out 2>/dev/null; then
      grep -E "^[a-z_]+ +[0-9]|^RESULT" /tmp/chaos_reconcile.out
      return 0
    fi
    sleep 15
  done
  grep -E "^[a-z_]+ |^RESULT" /tmp/chaos_reconcile.out
  fail "silver did not reconcile with the source"
}

verify_bronze() {
  spark_job jobs/verify_bronze.py --require-caught-up 2>/dev/null | grep -E "^bronze run|^RESULT" ||
    fail "bronze verification failed"
}

# Run the backfill job; print its summary, or its real errors if it fails.
run_backfill() {
  if ! spark_job jobs/backfill.py "$@" >/tmp/chaos_backfill.log 2>&1; then
    grep -E "Error|Exception" /tmp/chaos_backfill.log | grep -v MatchError | head -5
    fail "backfill failed"
  fi
  grep -E "consistent LSN|backfill done" /tmp/chaos_backfill.log | sed "s/.*INFO //"
}
