# Runbook: malformed records or data-quality failures

**Chaos test:** [04_malformed_events.sh](../../tests/chaos/04_malformed_events.sh)

**Symptoms:** alert *Rows are being quarantined*; Grafana "Quarantined rows" > 0;
`marts.dq_daily_report` shows quarantined counts.

**What protects you:** bronze stores every record as raw bytes and never fails on
content. Silver sends anything it cannot decode (`not_confluent_avro`,
`unknown_schema_id:N`, `undecodable`) or that fails a YAML rule (rule name) to
`silver/_quarantine` with the raw lineage. Streams keep running.

1. Inspect: `select source_table, reasons, count(*) from silver.dq_quarantine group by 1, 2;`
2. Decode problems: a producer other than Debezium wrote to the topic, or a
   schema id is missing from the archive and the registry. Find the record by
   `(_topic, _partition, _offset)`.
3. Rule failures are real bad data at the source: fix the row there. The
   corrected update has a newer LSN and flows through normally.
4. **Verify:** `make reconcile`. Keys still quarantined show as "explained".
