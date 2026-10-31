# Dev log

Bugs hit, root causes, and fixes. Newest first.

## 2026-10-31: Week 0/1 scaffolding
- **`docker pull minio/minio` failed: "repository does not exist".** MinIO no
  longer publishes free images on Docker Hub or Quay. Switched to SeaweedFS's
  S3 gateway (ADR 0006). Second bug on the way: the SeaweedFS image runs as
  non-root, so writing the S3 identity file to `/etc` failed with
  "Permission denied" and the S3 server crashed on a missing config. Fixed by
  writing it to `/tmp`.
- **Kafka refused to start with `CLUSTER_ID: tidewater-kraft-cluster-01`.**
  KRaft needs the base64 encoding of exactly 16 bytes (22 chars).
- Chose Apicurio Registry with **KafkaSQL storage** instead of the in-memory
  image. With in-memory storage a registry restart reassigns schema IDs, and
  every Avro record already in bronze would point at an ID that no longer exists.
- Debezium writes Avro in the **Confluent wire format** (`as-confluent=true`)
  so Spark can strip the 5-byte header and use `from_avro`, and Kafka UI can
  decode messages through the registry's `ccompat` API.
