"""Prometheus metrics for the streaming jobs, served on :9108 inside each container.

Stream health comes from Spark's own progress events (a StreamingQueryListener):
rows in, rows/sec, batch duration and, for Kafka, how far behind the latest offset
the stream is (Spark tracks offsets in its checkpoint, not in a consumer group, so
consumer-group lag exporters cannot see it). Silver adds pipeline metrics computed
in each batch: rows merged, quarantined, late, and source-to-silver latency.
"""

from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

PORT = 9108
_started = False

ROWS = Counter("tidewater_stream_rows_total", "Rows read by the stream", ["stream"])
INPUT_RATE = Gauge("tidewater_stream_input_rows_per_second", "Arrival rate", ["stream"])
PROCESS_RATE = Gauge("tidewater_stream_processed_rows_per_second", "Processing rate", ["stream"])
BATCH_SECONDS = Gauge("tidewater_stream_batch_duration_seconds", "Last batch duration", ["stream"])
LAST_PROGRESS = Gauge(
    "tidewater_stream_last_progress_timestamp_seconds",
    "Unix time of the last progress event (Spark emits one at least every ~10 s while healthy)",
    ["stream"],
)
OFFSETS_BEHIND = Gauge(
    "tidewater_stream_offsets_behind_latest",
    "Kafka offsets not yet read, per the source's progress metrics",
    ["stream", "stat"],
)

MERGED = Counter("tidewater_silver_rows_merged_total", "Rows merged into silver", ["table"])
QUARANTINED = Counter("tidewater_silver_quarantined_total", "Rows sent to quarantine")
LATE = Counter("tidewater_silver_late_events_total", "Superseded CDC events recorded as late")
CONFLICT_RETRIES = Counter(
    "tidewater_silver_merge_conflict_retries_total", "MERGE retries after Delta write conflicts"
)
LATENCY = Gauge(
    "tidewater_silver_latency_seconds",
    "Source commit -> silver commit latency of the last batch with merges",
    ["quantile"],
)


def start_metrics_server(port: int = PORT) -> None:
    global _started
    if not _started:
        start_http_server(port)
        _started = True


class ProgressMetrics(StreamingQueryListener):
    """Turns Spark streaming progress events into Prometheus metrics."""

    def __init__(self, stream: str):
        self.stream = stream

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark's API
        LAST_PROGRESS.labels(self.stream).set(time.time())

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = event.progress
        LAST_PROGRESS.labels(self.stream).set(time.time())
        ROWS.labels(self.stream).inc(p.numInputRows)
        INPUT_RATE.labels(self.stream).set(p.inputRowsPerSecond or 0.0)
        PROCESS_RATE.labels(self.stream).set(p.processedRowsPerSecond or 0.0)
        if p.batchDuration:
            BATCH_SECONDS.labels(self.stream).set(p.batchDuration / 1000)
        for source in p.sources:
            metrics = source.metrics or {}
            for stat in ("max", "avg", "min"):
                value = metrics.get(f"{stat}OffsetsBehindLatest")
                if value is not None:
                    OFFSETS_BEHIND.labels(self.stream, stat).set(float(value))

    def onQueryIdle(self, event) -> None:  # noqa: N802
        LAST_PROGRESS.labels(self.stream).set(time.time())

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def watch(spark, stream: str) -> None:
    """Start the endpoint and attach the progress listener to this session."""
    start_metrics_server()
    spark.streams.addListener(ProgressMetrics(stream))
