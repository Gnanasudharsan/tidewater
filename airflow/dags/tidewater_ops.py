"""Shared task builders for the Tidewater DAGs (imported by the DAG files).

Every job runs in the same image it runs in locally (tidewater-spark / tidewater-dbt),
launched through a Docker socket proxy onto the Compose network, so the orchestrator
holds no pipeline code or dependencies of its own.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator

# Pipeline settings forwarded to job containers (values come from the Compose env).
PIPELINE_ENV_KEYS = [
    "S3_ENDPOINT_INTERNAL", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKEHOUSE_BUCKET",
    "KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL", "BRONZE_TXN_APP_ID", "SILVER_TXN_APP_ID",
    "PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD",
    "WH_PGHOST", "WH_PGPORT", "WH_PGDATABASE", "WH_PGUSER", "WH_PGPASSWORD",
]  # fmt: skip

DEFAULT_ARGS = {
    "owner": "tidewater",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_SUBMIT = ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g"]


def pipeline_env() -> dict[str, str]:
    return {k: os.environ[k] for k in PIPELINE_ENV_KEYS if k in os.environ}


def _container_task(task_id: str, image: str, command: list[str], **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=pipeline_env(),
        docker_url=os.environ.get("TIDEWATER_DOCKER_URL", "tcp://docker-proxy:2375"),
        network_mode=os.environ.get("TIDEWATER_NETWORK", "tidewater_default"),
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=False,
        **kwargs,
    )


def spark_job(task_id: str, script: str, *args: str, **kwargs) -> DockerOperator:
    """Run spark/jobs/<script> in the tidewater-spark image."""
    return _container_task(
        task_id,
        "tidewater-spark:local",
        [*SPARK_SUBMIT, f"jobs/{script}", *args],
        mem_limit="2g",
        **kwargs,
    )


def dbt(task_id: str, *args: str, **kwargs) -> DockerOperator:
    """Run a dbt command in the tidewater-dbt image (project baked in)."""
    return _container_task(task_id, "tidewater-dbt:local", list(args), mem_limit="1g", **kwargs)
