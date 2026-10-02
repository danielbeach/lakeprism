#!/usr/bin/env bash
set -euo pipefail

if [[ "${LAKEPRISM_LIVE_CLOUD:-}" != "1" ]]; then
  echo "Set LAKEPRISM_LIVE_CLOUD=1 to run live cloud acceptance." >&2
  exit 2
fi

: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_CLIENT_ID:?DATABRICKS_CLIENT_ID is required}"
: "${DATABRICKS_CLIENT_SECRET:?DATABRICKS_CLIENT_SECRET is required}"

if [[ ! "$DATABRICKS_HOST" =~ ^https://[^/?#@]+/?$ ]]; then
  echo "DATABRICKS_HOST must be an HTTPS origin without credentials, query, or fragment." >&2
  exit 2
fi

if [[ -n "${DATABRICKS_VOLUME_PATH:-}" ]] &&
  [[ ! "$DATABRICKS_VOLUME_PATH" =~ ^/Volumes/[A-Za-z0-9_]+/[A-Za-z0-9_]+/[A-Za-z0-9_]+/.+ ]]; then
  echo "DATABRICKS_VOLUME_PATH must be /Volumes/catalog/schema/volume/path." >&2
  exit 2
fi

if [[ -n "${DATABRICKS_TABLE:-}" ]]; then
  if [[ ! "$DATABRICKS_TABLE" =~ ^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$ ]]; then
    echo "DATABRICKS_TABLE must be a three-part Unity identifier." >&2
    exit 2
  fi
  token_json="$(curl --fail --silent --show-error --request POST \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "client_id=${DATABRICKS_CLIENT_ID}" \
    --data-urlencode "client_secret=${DATABRICKS_CLIENT_SECRET}" \
    --data-urlencode "scope=all-apis" \
    "${DATABRICKS_HOST%/}/oidc/v1/token")"
  DATABRICKS_TOKEN="$(TOKEN_JSON="$token_json" python3 -c \
    'import json, os; print(json.loads(os.environ["TOKEN_JSON"])["access_token"])')"
  unset token_json
  printf '%s\n' "Authorization: Bearer ${DATABRICKS_TOKEN}" | curl --fail --silent --show-error \
    --header @- \
    --header "Accept: application/json" \
    --output /dev/null \
    "${DATABRICKS_HOST%/}/api/2.1/unity-catalog/tables/${DATABRICKS_TABLE}"
  unset DATABRICKS_TOKEN
fi

echo "Live Databricks OAuth/Unity metadata acceptance completed without persisting credentials."
