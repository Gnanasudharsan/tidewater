#!/usr/bin/env bash
# A/B/A benchmark: baseline, tuned, baseline again, at the same rates, so drift (silver
# tables grow with every run, and MERGE cost grows with them) shows up as the gap
# between the two A runs instead of biasing the comparison.
#
#   scripts/bench_ab.sh          (env: RATES="500 1000" STEP=240 WINDOW=180)
# Variants differ in one thing only: sequential vs concurrent per-table MERGEs
# (SILVER_MERGE_PARALLELISM 1 vs 5). Both run silver on local[4].
set -euo pipefail
cd "$(dirname "$0")/.."
export RATES="${RATES:-500 1000}" STEP="${STEP:-240}" WINDOW="${WINDOW:-180}"
run() {  # run <label> <merge parallelism>
  echo "=== $1 (SILVER_MERGE_PARALLELISM=$2)"
  SILVER_MERGE_PARALLELISM="$2" SILVER_MASTER='local[4]' docker compose up -d spark-silver >/dev/null 2>&1
  sleep 90  # JVM start + first idle batch
  ./scripts/bench.sh "$1" | grep -E "^\[|^\|" | grep -v "^|---" | tail -n +1
}
[ -n "${SKIP_A1:-}" ] || run A1_sequential 1
run B_concurrent 5
run A2_sequential 1
docker compose up -d spark-silver >/dev/null 2>&1
python3 scripts/bench_report.py $(ls -t bench/results/A1_*.csv | head -1) \
  $(ls -t bench/results/B_*.csv | head -1) $(ls -t bench/results/A2_*.csv | head -1)
