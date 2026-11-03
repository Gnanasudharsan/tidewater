#!/usr/bin/env bash
# The 5-minute Tidewater demo: live CDC load, a crash mid-batch, automatic recovery,
# then proof (bronze has every offset exactly once, silver matches the source 100%,
# and one order traced back to its WAL position).
#
#   make demo                 (DEMO_PAUSE=1 to wait for Enter between steps, for recording)
# Needs `make up-core` first. Starts Prometheus/Grafana and registers CDC if needed.
set -euo pipefail
cd "$(dirname "$0")/.."
RATE="${RATE:-50}"; LOAD_S="${LOAD_S:-150}"
SUBMIT=(/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g)
spark_job() { docker compose run --rm --no-deps spark-bronze "${SUBMIT[@]}" "$@" 2>/dev/null; }
step() {
  printf '\n\033[1;36m━━ %s\033[0m\n' "$*"
  if [ -n "${DEMO_PAUSE:-}" ]; then read -r -p "   (Enter to run) " _; fi
}

step "1/6  Stack check: CDC connector, dashboards"
curl -sf localhost:8083/connectors/tidewater-postgres-cdc/status | grep -q RUNNING ||
  ./scripts/register_connector.sh >/dev/null
make -s up-obs >/dev/null 2>&1 || true
echo "   Grafana  http://localhost:3001   Kafka UI http://localhost:8080   Spark http://localhost:4040"

step "2/6  Live e-commerce traffic: $RATE business events/s for ${LOAD_S}s"
docker rm -f tidewater-demo-sim >/dev/null 2>&1 || true
docker compose --profile sim run -d --name tidewater-demo-sim simulator \
  --rate "$RATE" --duration "$LOAD_S" >/dev/null 2>&1
sleep 30
docker logs tidewater-demo-sim 2>&1 | grep overall | tail -1 | sed 's/^.*INFO /   /' | cut -c1-110

step "3/6  Chaos: SIGKILL the bronze stream while a batch is running"
id=""; until [ -n "$id" ]; do
  id=$(curl -s localhost:4040/api/v1/applications 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])' 2>/dev/null || true); sleep 1
done
until curl -s "localhost:4040/api/v1/applications/$id/jobs?status=running" | grep -q jobId; do sleep 0.2; done
docker compose kill -s KILL spark-bronze >/dev/null 2>&1
echo "   killed mid-batch at $(date +%T); restarting (it replays from its checkpoint)"
docker compose up -d spark-bronze >/dev/null 2>&1
until docker compose logs --since 60s spark-bronze 2>&1 | grep -q "batch="; do sleep 3; done
echo "   bronze is streaming again at $(date +%T)"

step "4/6  Let the load finish and the pipeline drain"
docker wait tidewater-demo-sim >/dev/null
docker logs tidewater-demo-sim 2>&1 | grep "done:" | sed 's/^.*INFO /   /' | cut -c1-110
sleep 60

step "5/6  Proof: bronze has every Kafka offset exactly once; silver matches the source"
spark_job jobs/verify_bronze.py --require-caught-up | grep -E "^bronze run|^RESULT" | sed 's/^/   /'
ok=""
for attempt in 1 2 3 4 5 6; do
  if spark_job jobs/reconcile.py > /tmp/demo_reconcile.out; then ok=1; break; fi; sleep 20
done
grep -E "^table|^[a-z_]+ +[0-9]|^RESULT" /tmp/demo_reconcile.out | sed 's/^/   /'
[ -n "$ok" ] || { printf '\n\033[1;31mReconciliation did not pass.\033[0m See /tmp/demo_reconcile.out\n'; exit 1; }

step "6/6  Audit: one order traced from silver back to its Kafka offsets and WAL LSNs"
order=$(docker compose exec -T postgres-src psql -U "${SRC_PG_USER:-tidewater}" -d "${SRC_PG_DB:-shop}" -Atc \
  "select order_id from orders order by (status = 'refunded') desc, order_id desc limit 1")
spark_job jobs/audit_trace.py --order-id "$order" | sed -n '/== 3\./,/== 5\./p' | sed 's/^/ /' | head -24
printf '\n\033[1;32mDemo complete.\033[0m Crashed mid-batch under load; zero duplicates, zero gaps, 100%% reconciled.\n'
