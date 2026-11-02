#!/usr/bin/env bash
# Chaos 05: drop every silver table and rebuild from bronze alone (both ingest runs,
# including pre-incident data whose registry IDs no longer exist: the schema archive
# must decode them). Deletes that happened while CDC was down are not in bronze, so
# the documented procedure finishes with a delete-detecting backfill.
#                                   tests/chaos/05_rebuild_silver_from_bronze.sh
source "$(dirname "$0")/lib.sh"
NEW_APP="silver_rebuild_$(date +%s)"

log "Stopping silver and dropping all silver tables"
$COMPOSE stop spark-silver >/dev/null
spark_job jobs/drop_silver.py --yes-really 2>/dev/null | grep -E "DROPPED|absent"
log "Restarting silver from scratch as $NEW_APP (new checkpoint)"
sed -i.bak '/^SILVER_TXN_APP_ID=/d' .env && rm -f .env.bak
printf 'SILVER_TXN_APP_ID=%s\n' "$NEW_APP" >> .env
$COMPOSE up -d spark-silver >/dev/null
log "Waiting for silver to replay all of bronze"
until $COMPOSE logs --since 2m spark-silver 2>&1 | grep -q "silver batch {'batch': [0-9]*, 'quarantined'"; do sleep 10; done
# Replay batches can run for minutes without logging, so "quiet logs" is not "done".
# Done = silver read zero rows for 2 minutes (Prometheus), or 3 quiet minutes without it.
silver_idle() {
  if curl -sf localhost:9090/-/ready >/dev/null 2>&1; then
    curl -s localhost:9090/api/v1/query --data-urlencode \
      'query=increase(tidewater_stream_rows_total{stream="silver"}[2m])' |
      python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; sys.exit(0 if r and float(r[0]["value"][1]) == 0 else 1)'
  else
    [ -z "$($COMPOSE logs --since 3m spark-silver 2>&1 | grep 'silver batch')" ]
  fi
}
sleep 120
until silver_idle; do sleep 15; done
$COMPOSE logs spark-silver 2>&1 | grep "silver batch" | head -3 | cut -c40-250
log "Repairing deletes that never reached bronze (Kafka outage window)"
run_backfill --detect-deletes
reconcile_until_pass 12
log "PASS: silver rebuilt from bronze (+ delete repair) reconciles 100%"
