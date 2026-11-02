#!/usr/bin/env bash
# Chaos 04: malformed records land on a CDC topic, and a row violates a DQ rule that
# the database itself does not enforce. Nothing may stop; bad records go to quarantine
# with the right reason; good data is unaffected.   tests/chaos/04_malformed_events.sh
source "$(dirname "$0")/lib.sh"
SINCE=$(date -u '+%Y-%m-%d %H:%M:%S')

log "Injecting 4 malformed Kafka records"
spark_job jobs/inject_malformed.py 2>/dev/null | grep INJECTED
log "Inserting a customer whose email breaks the email_format DQ rule"
BAD=$(src_sql "insert into customers (email, full_name, address_line, city, state, postal_code)
               values ('not-an-email-$(date +%s)', 'Chaos Monkey', '1 Main St', 'Springfield', 'IL', '62701')
               returning customer_id" | head -1)
sleep 45
spark_job jobs/quarantine_report.py --since "$SINCE" 2>/dev/null | grep QUARANTINE | tee /tmp/chaos_q.out
for expect in "orders not_confluent_avro 2" "orders unknown_schema_id:999999 1" "orders undecodable 1" "customers email_format 1"; do
  grep -q "QUARANTINE $expect" /tmp/chaos_q.out || fail "expected quarantine entry: $expect"
done
log "Reconciling while the bad customer is quarantined (expect it to be 'explained')"
reconcile_until_pass 12
grep -E "^customers .* 1  PASS" /tmp/chaos_reconcile.out >/dev/null || fail "bad row was not explained by quarantine"

log "Repairing the row at the source; the valid update must flow through"
src_sql "update customers set email = 'repaired-$BAD@example.com' where customer_id = $BAD" >/dev/null
sleep 30
reconcile_until_pass 12
verify_bronze
log "PASS: malformed records quarantined with reasons; streams never stopped"
