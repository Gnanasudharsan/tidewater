#!/usr/bin/env bash
# Chaos test 01: crash the bronze stream under live load, then prove bronze has
# every Kafka offset exactly once.
#   Phase A: SIGKILL while a micro-batch is running (random point in the batch), N times.
#   Phase B: crash deterministically in the worst window, after the Delta commit but
#            before the checkpoint commit, and assert the idempotent write skips the replay.
#
#   tests/chaos/01_kill_bronze_mid_batch.sh [kills=3] [rate=100]
#
# Preconditions: `make up-core && make register` (the stack is healthy).
set -euo pipefail
cd "$(dirname "$0")/../.."

KILLS="${1:-3}"
RATE="${2:-100}"
UI="http://localhost:4040/api/v1"
COMPOSE="docker compose"

# grep -c reads all input. grep -q would exit early, SIGPIPE `compose logs`, and
# under pipefail the pipeline would report failure even on a match.
logs_have() { $COMPOSE logs spark-bronze 2>&1 | grep -c "$1" >/dev/null; }

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

app_id() { curl -fs "$UI/applications" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])' 2>/dev/null; }

wait_for_running_batch() {
  # Poll the Spark REST API until a job is actively running, i.e. we are mid-batch.
  local deadline=$((SECONDS + 120)) id
  while ((SECONDS < deadline)); do
    id="$(app_id || true)"
    if [[ -n "$id" ]] && curl -fs "$UI/applications/$id/jobs?status=running" | grep -q '"jobId"'; then
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for a running Spark job" >&2
  return 1
}

log "Starting simulator at $RATE events/s"
SIM=tidewater-chaos-sim
docker rm -f "$SIM" >/dev/null 2>&1 || true
$COMPOSE --profile sim run -d --build --name "$SIM" simulator --rate "$RATE" --duration 0 >/dev/null
trap 'docker rm -f "$SIM" >/dev/null 2>&1 || true' EXIT
$COMPOSE up -d spark-bronze

for i in $(seq 1 "$KILLS"); do
  wait_for_running_batch
  log "Kill $i/$KILLS: SIGKILL spark-bronze mid-batch"
  $COMPOSE kill -s KILL spark-bronze
  sleep "$((RANDOM % 5 + 1))"
  log "Restarting spark-bronze"
  $COMPOSE up -d spark-bronze
done

log "Phase B: crash after Delta commit, before checkpoint commit"
BRONZE_CHAOS_CRASH_AFTER_COMMIT=1 $COMPOSE up -d spark-bronze
deadline=$((SECONDS + 120))
until logs_have "CHAOS: crashing"; do
  ((SECONDS < deadline)) || { echo "chaos crash never triggered" >&2; exit 1; }
  sleep 1
done
$COMPOSE logs spark-bronze 2>&1 | grep "CHAOS: crashing" | tail -1
log "Restarting spark-bronze without the chaos hook"
$COMPOSE up -d spark-bronze
deadline=$((SECONDS + 120))
until logs_have "idempotent write (txnVersion) skipped"; do
  ((SECONDS < deadline)) || { echo "FAIL: replayed batch was not skipped by Delta" >&2; exit 1; }
  sleep 1
done
$COMPOSE logs spark-bronze 2>&1 | grep "idempotent write" | tail -1

log "Stopping simulator; letting bronze drain"
docker stop "$SIM" >/dev/null
wait_for_running_batch   # the restarted stream is processing again

for attempt in 1 2 3 4 5; do
  sleep 15
  log "Verification attempt $attempt"
  if $COMPOSE run --rm --no-deps spark-bronze \
      /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
      jobs/verify_bronze.py --require-caught-up; then
    log "PASS: $KILLS mid-batch kills + 1 post-commit crash, every offset exactly once"
    exit 0
  fi
done
log "FAIL: bronze did not reconcile with Kafka"
exit 1
