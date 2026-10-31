-- Warehouse layout: raw silver loads land in `silver`, dbt builds the rest.
create schema if not exists silver;
create schema if not exists staging;
create schema if not exists marts;
create schema if not exists snapshots;
