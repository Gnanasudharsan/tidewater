#!/usr/bin/env bash
# Enforce BACKWARD schema compatibility, then create/update the Debezium connector.
# Idempotent: safe to run repeatedly.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a

REGISTRY="${REGISTRY_URL:-http://localhost:8081}"
CONNECT="${CONNECT_URL:-http://localhost:8083}"

echo "==> Setting global compatibility rule to BACKWARD"
if curl -fs "$REGISTRY/apis/registry/v2/admin/rules/COMPATIBILITY" >/dev/null; then
  curl -fsS -X PUT -H 'Content-Type: application/json' \
    -d '{"type":"COMPATIBILITY","config":"BACKWARD"}' \
    "$REGISTRY/apis/registry/v2/admin/rules/COMPATIBILITY" >/dev/null
else
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d '{"type":"COMPATIBILITY","config":"BACKWARD"}' \
    "$REGISTRY/apis/registry/v2/admin/rules" >/dev/null
fi
curl -fsS "$REGISTRY/apis/registry/v2/admin/rules/COMPATIBILITY"; echo

echo "==> Registering connector"
# Render ${VARS} from .env into the connector config, then PUT the config block (upsert).
config="$(python3 -c 'import json,os,sys; print(json.dumps(json.loads(os.path.expandvars(open(sys.argv[1]).read()))["config"]))' connect/debezium-postgres.json)"
curl -fsS -X PUT -H 'Content-Type: application/json' \
  -d "$config" "$CONNECT/connectors/tidewater-postgres-cdc/config" >/dev/null

sleep 3
curl -fsS "$CONNECT/connectors/tidewater-postgres-cdc/status"; echo
