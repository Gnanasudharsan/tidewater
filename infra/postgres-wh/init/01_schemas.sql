-- Warehouse layout: raw silver loads land in `silver`, dbt builds the rest.
create schema if not exists silver;
create schema if not exists staging;
create schema if not exists marts;
create schema if not exists snapshots;

-- Audit tables mirrored from the lakehouse by the loader (pre-created so dashboards
-- work before the first bad record ever arrives).
create table if not exists silver.dq_quarantine (
    source_table text, reasons text, record_json text, _topic text, _partition int,
    _offset bigint, _value_schema_id int, _batch_id bigint, _recorded_at timestamp
);
create table if not exists silver.late_arrivals (
    source_table text, pk_json text, _op text, _lsn bigint, _target_lsn bigint,
    _source_ts timestamp, _target_source_ts timestamp, _source_topic text,
    _source_partition int, _source_offset bigint, _batch_id bigint, _recorded_at timestamp
);
