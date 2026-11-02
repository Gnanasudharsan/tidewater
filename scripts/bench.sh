#!/usr/bin/env bash
# Throughput / latency benchmark of the streaming pipeline (Postgres -> silver).
#
#   scripts/bench.sh [label=baseline]       (env: RATES="100 500 1000 2000" STEP=180 WINDOW=120)
#
# For each target rate: run enough parallel simulators to reach it (one tops out
# around 400 tx/s), hold for STEP seconds, measure the last WINDOW seconds from
# Prometheus, then stop the load and time how long silver takes to drain. Results
# go to bench/results/<label>_<timestamp>.csv. Requires `make up-core up-obs`.
set -euo pipefail
cd "$(dirname "$0")/.."
LABEL="${1:-baseline}"
RATES="${RATES:-100 500 1000 2000}"; STEP="${STEP:-180}"; WINDOW="${WINDOW:-120}"
PER_SIM=250
OUT="bench/results/${LABEL}_$(date -u +%Y%m%dT%H%M%S).csv"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
prom() { curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" |
  python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "nan")'; }
# `or vector(0)`: a stream that has not processed data since it started exports no series.
total() { prom "sum(tidewater_stream_rows_total{stream=\"$1\"}) or vector(0)"; }
# A restarted stream has no counter series until its first progress event: wait for it.
wait_counters() {
  until [ "$(total bronze)" != "nan" ] && [ "$(total silver)" != "nan" ]; do sleep 5; done
}
# Drained = silver has read every row bronze ingested since the step began. (A silent
# silver is not proof: a single long batch emits no progress until it finishes.)
drained() {
  local b s; b=$(total bronze); s=$(total silver)
  python3 -c "import sys; b,s,b0,s0=map(float,sys.argv[1:]); sys.exit(0 if s - s0 >= b - b0 - 1 else 1)" \
    "$b" "$s" "$b0" "$s0" 2>/dev/null
}

curl -sf localhost:9090/-/ready >/dev/null || { echo "Prometheus is not running (make up-obs)"; exit 1; }
for rate in $RATES; do
  workers=$(( (rate + PER_SIM - 1) / PER_SIM )); per=$(python3 -c "print(round($rate / $workers, 1))")
  log "rate $rate events/s: $workers simulator(s) at $per/s for ${STEP}s"
  wait_counters; b0=$(total bronze); s0=$(total silver)
  for i in $(seq 1 "$workers"); do
    docker rm -f "bench-sim-$i" >/dev/null 2>&1 || true
    docker compose --profile sim run -d --name "bench-sim-$i" simulator --rate "$per" --duration "$STEP" >/dev/null 2>&1
  done
  for i in $(seq 1 "$workers"); do docker wait "bench-sim-$i" >/dev/null; done
  events=0
  for i in $(seq 1 "$workers"); do
    n=$(docker logs "bench-sim-$i" 2>&1 | sed -n 's/.*done: \([0-9]*\) events.*/\1/p' | tail -1)
    events=$((events + ${n:-0})); docker rm -f "bench-sim-$i" >/dev/null
  done
  t0=$(date +%s)
  sleep 15  # let bronze ingest the tail of the load before comparing totals
  until drained; do sleep 5; [ $(( $(date +%s) - t0 )) -gt 900 ] && break; done
  drain=$(( $(date +%s) - t0 ))
  # Measure the window that ended when the load stopped.
  python3 scripts/bench_collect.py --csv "$OUT" --label "$LABEL" --target-rate "$rate" \
    --business-events "$events" --step-seconds "$STEP" --window "$WINDOW" --drain-seconds "$drain" \
    --offset-seconds "$drain"
done
log "results: $OUT"
python3 scripts/bench_report.py "$OUT"
