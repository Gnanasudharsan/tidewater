"""Explicit OpenLineage events for what Spark's automatic integration cannot see.

The OpenLineage Spark listener captures plain reads and writes, but not a Delta MERGE
inside foreachBatch (silver) or Change Data Feed reads (the warehouse loader), and
the loader's final upsert is SQL that Spark never sees. Each job therefore declares
its logical inputs and outputs once per run, using the same dataset naming as the
Spark and dbt integrations, so Marquez joins everything into one graph:

  kafka topic -> bronze/cdc_events -> silver/<table> -> analytics.silver.<table> -> dbt marts
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from tidewater_spark.config import Settings

log = logging.getLogger(__name__)
NAMESPACE = "tidewater"


def kafka(topic: str) -> tuple[str, str]:
    return "kafka://kafka:9092", topic


def lake(settings: Settings, path: str) -> tuple[str, str]:
    """('s3a://<bucket>', '/<path>') for a full s3a:// path, as Spark's integration names it."""
    prefix = f"s3a://{settings.bucket}"
    return prefix, path[len(prefix) :]


def warehouse(settings: Settings, table: str) -> tuple[str, str]:
    return f"postgres://{settings.wh_host}:{settings.wh_port}", f"{settings.wh_db}.silver.{table}"


def emit(settings: Settings, job: str, inputs: list, outputs: list) -> None:
    """Emit START + COMPLETE for one logical run. Never fails the job."""
    if not settings.openlineage_url:
        return
    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.event_v2 import (
            InputDataset,
            Job,
            OutputDataset,
            Run,
            RunEvent,
            RunState,
        )

        client = OpenLineageClient(url=settings.openlineage_url)
        run = Run(runId=str(uuid.uuid4()))
        ins = [InputDataset(namespace=ns, name=n) for ns, n in inputs]
        outs = [OutputDataset(namespace=ns, name=n) for ns, n in outputs]
        for state in (RunState.START, RunState.COMPLETE):
            client.emit(
                RunEvent(
                    eventType=state,
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=run,
                    job=Job(namespace=NAMESPACE, name=job),
                    producer="https://github.com/Gnanasudharsan/tidewater",
                    inputs=ins,
                    outputs=outs,
                )
            )
        log.info("lineage: %s (%d inputs -> %d outputs)", job, len(ins), len(outs))
    except Exception as exc:  # noqa: BLE001 - lineage must never break the pipeline
        log.warning("lineage emit failed for %s: %s", job, exc)
