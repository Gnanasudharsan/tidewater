variable "s3_endpoint" {
  description = "S3 API endpoint (SeaweedFS locally)."
  type        = string
  default     = "http://object-store:8333"
}

variable "s3_access_key" {
  type      = string
  sensitive = true
}

variable "s3_secret_key" {
  type      = string
  sensitive = true
}

variable "lakehouse_bucket" {
  type    = string
  default = "lakehouse"
}
