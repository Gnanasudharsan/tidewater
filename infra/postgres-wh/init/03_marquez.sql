-- Marquez (OpenLineage backend) metadata database, isolated like Airflow's.
select 'create role marquez login password ''marquez'''
where not exists (select 1 from pg_roles where rolname = 'marquez') \gexec
select 'create database marquez owner marquez'
where not exists (select 1 from pg_database where datname = 'marquez') \gexec
