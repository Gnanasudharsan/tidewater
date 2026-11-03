"""SparkSession factory with Delta Lake and S3A wired up."""

from __future__ import annotations

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


def delta_log_committed(spark: SparkSession, path: str) -> bool:
    """True once a Delta table at `path` has at least one committed version.

    Checks the transaction log through the Hadoop FileSystem directly. In CI,
    `DeltaTable.isDeltaTable` kept returning False for minutes after bronze had
    committed version 0 on SeaweedFS (cause not reproduced locally), so code that
    waits for a table to appear waits for this concrete fact instead.
    """
    jvm = spark._jvm
    log = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
    fs = log.getFileSystem(spark._jsc.hadoopConfiguration())
    try:
        return any(s.getPath().getName().endswith(".json") for s in fs.listStatus(log))
    except Exception:  # noqa: BLE001 - FileNotFound and friends mean "not yet"
        return False
