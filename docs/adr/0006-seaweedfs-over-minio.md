# ADR 0006: SeaweedFS instead of MinIO for local S3

Status: Accepted · Date: 2026-10-31

## Context
The original plan used MinIO as the local S3 stand-in. MinIO has stopped
publishing free container images: `minio/minio` on Docker Hub and the Quay
mirror both refuse pulls, so a fresh `git clone && make up-core` would fail.

## Decision
Use **SeaweedFS** (`chrislusf/seaweedfs`, Apache-2.0) in single-process
`weed server -s3` mode, pinned to a fixed version, as the S3 endpoint.

## Rationale
- Actively maintained, Apache-2.0 licensed, and published to Docker Hub.
- Speaks the S3 API with access-key auth, which is all Spark's S3A connector
  and Delta Lake need.
- One container for master, volume, filer, and S3 gateway fits the 8 GB budget.

## Consequences
- Pipeline code only knows an S3 endpoint and keys (`S3_ENDPOINT`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY`). Pointing at AWS S3 is a config change.
- No MinIO console. The SeaweedFS filer UI (`:8888`) is used to browse files.
- Terraform in week 7 uses the generic AWS provider with a custom S3 endpoint
  instead of the MinIO provider.
- Bucket versioning is not enabled; Delta's transaction log provides time travel.
