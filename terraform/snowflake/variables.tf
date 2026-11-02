# Provider credentials come from the environment (never from this repo):
#   SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER and either
#   SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT + SNOWFLAKE_PRIVATE_KEY.

variable "service_user_rsa_public_key" {
  description = "Public key (PEM body, no header/footer) for the pipeline's service user. See scripts/snowflake_keypair.sh."
  type        = string
}

variable "monthly_credit_quota" {
  description = "Hard cap: the warehouse is suspended when this many credits are used in a month."
  type        = number
  default     = 20
}
