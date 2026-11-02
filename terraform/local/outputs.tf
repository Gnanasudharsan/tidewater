output "buckets" {
  value = [aws_s3_bucket.lakehouse.bucket, aws_s3_bucket.exports.bucket]
}
