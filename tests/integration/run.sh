#!/usr/bin/env bash
# End-to-end integration test: push 1,000 business events through the whole pipeline
# (Postgres -> Debezium -> Kafka -> bronze -> silver -> warehouse -> dbt marts) and
# assert they land correctly.
#
# Preconditions: core + warehouse running and the connector registered, i.e.
#   docker compose --profile core up -d --wait && make register && \
#   docker compose --profile wh up -d --wait postgres-wh
set -euo pipefail
cd "$(dirname "$0")/../.."

EVENTS="${EVENTS:-1000}"
COMPOSE="docker compose"
SUBMIT=(/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g)
log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
spark_job() { $COMPOSE run --rm --no-deps spark-bronze "${SUBMIT[@]}" "$@"; }
src() { $COMPOSE exec -T postgres-src psql -U "${SRC_PG_USER:-tidewater}" -d "${SRC_PG_DB:-shop}" -Atc "$1"; }
wh() { $COMPOSE exec -T postgres-wh psql -U "${WH_PG_USER:-warehouse}" -d "${WH_PG_DB:-analytics}" -Atc "$1"; }

log "Generating $EVENTS events"
$COMPOSE --profile sim run --rm --build simulator --rate 100 --events "$EVENTS" --random-seed 7 2>&1 | tail -1

log "Waiting for silver to reconcile 100% with the source"
for attempt in $(seq 1 20); do
  if spark_job jobs/reconcile.py 2>/dev/null | tee /tmp/reconcile.out | grep -q "^RESULT: PASS"; then
    grep -E "^[a-z_]+ +[0-9]" /tmp/reconcile.out
    break
  fi
  [ "$attempt" = 20 ] && { cat /tmp/reconcile.out; log "FAIL: silver never reconciled"; exit 1; }
  sleep 15
done

log "Loading the warehouse and building dbt"
spark_job jobs/load_warehouse.py 2>&1 | grep -E "INFO [a-z_]+:" || true
# Compose bind-mounts ./dbt over the image's project, so install packages into it first
# (a fresh checkout has no dbt_packages/).
$COMPOSE --profile wh run --rm --build dbt deps >/dev/null
if ! $COMPOSE --profile wh run --rm dbt build >/tmp/dbt_build.out 2>&1; then
  tail -40 /tmp/dbt_build.out
  log "FAIL: dbt build"
  exit 1
fi
grep -E "Done\." /tmp/dbt_build.out

log "Asserting marts match the source"
fail=0
for pair in "orders:fct_orders" "order_items:fct_order_items" "payments:fct_payments"; do
  source_table="${pair%%:*}"; mart="${pair##*:}"
  s=$(src "select count(*) from $source_table")
  m=$(wh "select count(*) from marts.$mart")
  printf '  %-12s source=%-6s %-16s=%s\n' "$source_table" "$s" "$mart" "$m"
  [ "$s" = "$m" ] || fail=1
done
s=$(src "select coalesce(sum(order_total), 0) from orders where status <> 'cancelled'")
m=$(wh "select coalesce(sum(gmv), 0) from marts.fct_daily_sales")
printf '  %-12s source=%-12s marts=%s\n' "gmv" "$s" "$m"
[ "$s" = "$m" ] || fail=1

log "Silver's wait for bronze (evidence for the intermittent CI stall)"
$COMPOSE logs spark-silver 2>&1 | grep -E "waiting for Delta table|appeared after|failed \(not a 'not found'\)" |
  sed 's/^.*INFO //; s/^.*WARNING /WARNING /' | sed -n '1p;$p' || true

[ "$fail" = 0 ] && log "PASS: $EVENTS events landed correctly in the marts" || { log "FAIL"; exit 1; }
