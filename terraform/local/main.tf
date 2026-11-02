# Local object storage for the lakehouse, managed with the standard AWS provider
# pointed at SeaweedFS's S3 API. Pointing at real AWS means deleting the
# `endpoints` block and the skip_* flags; the resources stay the same.

provider "aws" {
  region     = "us-east-1"
  access_key = var.s3_access_key
  secret_key = var.s3_secret_key

  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  skip_region_validation      = true

  endpoints {
    s3 = var.s3_endpoint
  }
}

# The lakehouse bucket already exists (created by the Compose bootstrap), so it is
# adopted into state instead of recreated.
import {
  to = aws_s3_bucket.lakehouse
  id = var.lakehouse_bucket
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.lakehouse_bucket

  lifecycle {
    prevent_destroy = true
  }
}

# Benchmark exports (week 9) live in their own bucket so they outlive lakehouse rebuilds.
resource "aws_s3_bucket" "exports" {
  bucket = "${var.lakehouse_bucket}-exports"
}
