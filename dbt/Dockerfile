FROM python:3.11-slim
RUN pip install --no-cache-dir dbt-core==1.8.9 dbt-postgres==1.8.2 "dbt-metricflow[dbt-postgres]==0.7.1" openlineage-dbt==1.40.0 \
    dbt-snowflake==1.8.4
WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt
# The project is baked in so an orchestrator can run this image as-is; Compose
# bind-mounts ./dbt over it for local development.
COPY . /dbt
RUN dbt deps
ENTRYPOINT ["dbt"]
