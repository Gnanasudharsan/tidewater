#!/usr/bin/env bash
# Generate the RSA key pair for the Snowflake service user (key-pair auth).
# The private key stays in .secrets/ (gitignored, mode 600) and is mounted into the
# job containers; the public key goes to Terraform as service_user_rsa_public_key.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .secrets && chmod 700 .secrets
if [ -f .secrets/snowflake_key.p8 ]; then
  echo "exists: .secrets/snowflake_key.p8 (delete it first to rotate)"
else
  openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out .secrets/snowflake_key.p8 -nocrypt
  chmod 600 .secrets/snowflake_key.p8
  openssl rsa -in .secrets/snowflake_key.p8 -pubout -out .secrets/snowflake_key.pub 2>/dev/null
  echo "created .secrets/snowflake_key.p8 and .secrets/snowflake_key.pub"
fi
