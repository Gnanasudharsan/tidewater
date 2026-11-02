-- Airflow's metadata database lives on the warehouse server, isolated in its own
-- database and role (local dev credentials). Idempotent: safe to re-run.
select 'create role airflow login password ''airflow'''
where not exists (select 1 from pg_roles where rolname = 'airflow') \gexec
select 'create database airflow owner airflow'
where not exists (select 1 from pg_database where datname = 'airflow') \gexec
