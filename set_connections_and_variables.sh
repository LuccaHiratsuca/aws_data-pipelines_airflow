#!/usr/bin/env bash
#
# Bootstraps the two Airflow connections and the two Airflow Variables that the
# `final_project` DAG depends on.
#
# Usage:
#   cp .env.example .env && edit .env
#   ./set_connections_and_variables.sh                 # inside an Airflow shell
#   docker compose run --rm airflow-cli bash /opt/airflow/set_connections_and_variables.sh
#
set -euo pipefail

# Load .env from the repo root if present, without clobbering already-exported vars.
ENV_FILE="$(dirname "$0")/.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

require() {
  if [[ -z "${!1:-}" ]]; then
    echo "error: $1 is not set (check your .env)" >&2
    exit 1
  fi
}

require AWS_ACCESS_KEY_ID
require AWS_SECRET_ACCESS_KEY
require REDSHIFT_HOST
require REDSHIFT_PASSWORD
require S3_BUCKET

echo "--> (re)creating connection 'aws_credentials'"
airflow connections delete aws_credentials >/dev/null 2>&1 || true
airflow connections add aws_credentials \
  --conn-type aws \
  --conn-login "$AWS_ACCESS_KEY_ID" \
  --conn-password "$AWS_SECRET_ACCESS_KEY" \
  --conn-extra "{\"region_name\": \"${REDSHIFT_REGION:-us-east-1}\"}"

echo "--> (re)creating connection 'redshift'"
airflow connections delete redshift >/dev/null 2>&1 || true
airflow connections add redshift \
  --conn-type postgres \
  --conn-host "$REDSHIFT_HOST" \
  --conn-port "${REDSHIFT_PORT:-5439}" \
  --conn-schema "${REDSHIFT_SCHEMA:-dev}" \
  --conn-login "${REDSHIFT_LOGIN:-awsuser}" \
  --conn-password "$REDSHIFT_PASSWORD"

echo "--> setting Variables"
airflow variables set s3_bucket "$S3_BUCKET"
airflow variables set s3_prefix "${S3_PREFIX:-}"

echo "done."
