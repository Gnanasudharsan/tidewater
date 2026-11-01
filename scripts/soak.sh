#!/usr/bin/env bash
# Soak test: live load for DURATION seconds, a full concurrent backfill halfway
# through, then prove bronze is complete and silver matches the source 100%.
#
#   scripts/soak.sh [duration_s=600] [rate=50]
#
# Preconditions: `make up-core && make register`.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${1:-600}"
RATE="${2:-50}"
COMPOSE="docker compose"
SIM=tidewater-soak-sim
SUBMIT=(/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g)
log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
spark_job() { $COMPOSE run --rm --no-deps spark-bronze "${SUBMIT[@]}" "$@"; }

$COMPOSE up -d spark-bronze spark-silver >/dev/null
started=$(date -u +%Y-%m-%dT%H:%M:%S)

log "Load: $RATE events/s for ${DURATION}s"
docker rm -f "$SIM" >/dev/null 2>&1 || true
$COMPOSE --profile sim run -d --build --name "$SIM" simulator \
  --rate "$RATE" --duration "$DURATION" >/dev/null
trap 'docker rm -f "$SIM" >/dev/null 2>&1 || true' EXIT

sleep "$((DURATION / 2))"
log "Halfway: full backfill of every table, concurrent with the live stream"
spark_job jobs/backfill.py --detect-deletes 2>&1 | grep -E "INFO|WARNING|ERROR" | grep -v " WARN "

log "Waiting for the simulator to finish"
docker wait "$SIM" >/dev/null
docker logs "$SIM" 2>&1 | tail -1

for attempt in 1 2 3 4 5 6; do
  sleep 20
  log "Verify attempt $attempt: bronze completeness, then silver reconciliation"
  if spark_job jobs/verify_bronze.py --require-caught-up | grep -E "^RESULT" &&
     spark_job jobs/reconcile.py | tee /dev/stderr | grep -q "^RESULT: PASS"; then
    log "Silver latency during the run (per-batch p95, source commit -> silver commit):"
    $COMPOSE logs --since "$started" spark-silver 2>&1 | grep -o "'latency_p95_s': [0-9.]*" |
      awk '{print $2}' | sort -n |
      awk '{v[NR]=$1} END {n=NR; if (n) printf "  batches=%d  median=%.1fs  p95=%.1fs  max=%.1fs\n", n, v[int(n*0.5)+1], v[int(n*0.95)+1], v[n]}'
    log "PASS: soak of ${DURATION}s at ${RATE}/s with a concurrent backfill reconciles 100%"
    exit 0
  fi
done
log "FAIL: silver did not reconcile"
exit 1
