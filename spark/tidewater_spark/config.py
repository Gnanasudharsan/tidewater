"""Runtime settings, all from environment variables so the same code runs locally and on S3."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable {name}")
    return value


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str
    kafka_bootstrap: str
    cdc_topic_pattern: str
    bronze_trigger: str
    bronze_max_offsets_per_trigger: int
    # Delta idempotent-write identity for the bronze stream. Bump the version
    # whenever the checkpoint is reset: batch ids restart at 0 and Delta would
    # otherwise skip them as "already committed".
    bronze_txn_app_id: str
    # Chaos hook: exit right after the Delta commit, before the checkpoint commit.
    chaos_crash_after_commit: bool = False

    @property
    def lakehouse(self) -> str:
        return f"s3a://{self.bucket}"

    @property
    def bronze_path(self) -> str:
        return f"{self.lakehouse}/bronze/cdc_events"

    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.lakehouse}/_checkpoints/{self.bronze_txn_app_id}"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            s3_endpoint=_env("S3_ENDPOINT_INTERNAL", "http://object-store:8333"),
            s3_access_key=_env("S3_ACCESS_KEY"),
            s3_secret_key=_env("S3_SECRET_KEY"),
            bucket=_env("LAKEHOUSE_BUCKET", "lakehouse"),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "kafka:9092"),
            cdc_topic_pattern=_env("CDC_TOPIC_PATTERN", r"tidewater\.public\..*"),
            bronze_trigger=_env("BRONZE_TRIGGER", "5 seconds"),
            bronze_max_offsets_per_trigger=int(_env("BRONZE_MAX_OFFSETS_PER_TRIGGER", "50000")),
            bronze_txn_app_id=_env("BRONZE_TXN_APP_ID", "bronze_cdc_v1"),
            chaos_crash_after_commit=_env("BRONZE_CHAOS_CRASH_AFTER_COMMIT", "0") == "1",
        )
