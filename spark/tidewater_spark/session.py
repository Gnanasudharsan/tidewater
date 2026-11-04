"""SparkSession factory with Delta Lake and S3A wired up."""

from __future__ import annotations

import logging
import time

from pyspark.sql import SparkSession

from tidewater_spark.config import Settings


def build_spark(app_name: str, settings: Settings) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        # Standard Parquet timestamps (not legacy INT96) so external readers (Snowflake
        # COPY) see real TIMESTAMP values.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # FileContext-based checkpointing rejects SeaweedFS directories (it sees them as
        # files). The FileSystem-based manager works there and on AWS S3. See devlog.
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
        )
        # Every new Delta table records row-level changes, so the warehouse loader
        # can read exactly what changed between two versions.
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        # Let MERGE add columns that appear at the source (schema evolution).
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # S3A against any S3-compatible endpoint. On AWS, drop the endpoint and path-style lines.
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(settings.s3_endpoint.startswith("https")).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
    )
    if settings.openlineage_url:
        # Emit OpenLineage run events (inputs, outputs, schemas) to Marquez.
        builder = (
            builder.config(
                "spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener"
            )
            .config("spark.openlineage.transport.type", "http")
            .config("spark.openlineage.transport.url", settings.openlineage_url)
            .config("spark.openlineage.namespace", "tidewater")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def delta_log_committed(spark: SparkSession, path: str, fresh_fs: bool = False) -> bool:
    """True once a Delta table at `path` has at least one committed version.

    Checks the transaction log through the Hadoop FileSystem directly. In CI,
    `DeltaTable.isDeltaTable` kept returning False for minutes after bronze had
    committed version 0. Delta wraps its `exists` call in Try(...).getOrElse(false),
    so *any* error looks like "no table". Here only "not found" means "not yet";
    every other error is logged, and `fresh_fs=True` bypasses Hadoop's per-JVM
    FileSystem cache in case the cached S3A client is the broken part.
    """
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    log_dir = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
    fs = (
        jvm.org.apache.hadoop.fs.FileSystem.newInstance(log_dir.toUri(), conf)
        if fresh_fs
        else log_dir.getFileSystem(conf)
    )
    try:
        return any(s.getPath().getName().endswith(".json") for s in fs.listStatus(log_dir))
    except Exception as exc:  # noqa: BLE001 - classified below
        if "FileNotFoundException" not in str(exc):
            logging.getLogger(__name__).warning(
                "checking %s failed (not a 'not found'): %s", log_dir, str(exc).splitlines()[0]
            )
        return False
    finally:
        if fresh_fs:
            fs.close()


def wait_for_delta_table(spark: SparkSession, path: str, poll_s: float = 10) -> None:
    """Block until `path` has a committed Delta version, logging progress. After a
    minute of waiting, each check also tries an uncached FileSystem client."""
    waited, log = 0.0, logging.getLogger(__name__)
    while True:
        fresh = waited >= 60
        if delta_log_committed(spark, path, fresh_fs=fresh):
            if waited:
                log.info("table at %s appeared after %.0fs (fresh client: %s)", path, waited, fresh)
            return
        log.info("waiting for Delta table at %s (%.0fs)", path, waited)
        time.sleep(poll_s)
        waited += poll_s
