#!/usr/bin/env bash
# Trigger a DAG run through the Airflow scheduler and wait for it to finish.
#   scripts/airflow_run.sh <dag_id> ['{"param": "value"}']
# Prints each task's final state; exits non-zero unless the run succeeded.
set -euo pipefail
cd "$(dirname "$0")/.."
DAG="$1"; CONF="${2:-}"; [ -n "$CONF" ] || CONF="{}"
AF=(docker compose --profile wh --profile orch exec -T airflow airflow)
RUN_ID="manual__$(date -u +%Y%m%dT%H%M%S)"
"${AF[@]}" dags unpause "$DAG" >/dev/null
"${AF[@]}" dags trigger "$DAG" --run-id "$RUN_ID" --conf "$CONF" >/dev/null
echo "triggered $DAG ($RUN_ID)"
while true; do
  state=$("${AF[@]}" dags list-runs -d "$DAG" -o plain 2>/dev/null | awk -v r="$RUN_ID" '$2==r {print $3}')
  case "$state" in success|failed) break ;; esac
  sleep 5
done
"${AF[@]}" tasks states-for-dag-run "$DAG" "$RUN_ID" -o plain 2>/dev/null | awk 'NR>1 {printf "  %-28s %s\n", $3, $4}'
echo "$DAG: $state"
[ "$state" = success ]
