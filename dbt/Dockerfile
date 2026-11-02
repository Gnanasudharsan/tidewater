FROM python:3.11-slim
RUN pip install --no-cache-dir dbt-core==1.8.9 dbt-postgres==1.8.2 "dbt-metricflow[dbt-postgres]==0.7.1"
WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt
ENTRYPOINT ["dbt"]
