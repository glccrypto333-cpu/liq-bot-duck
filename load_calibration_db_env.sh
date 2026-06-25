#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REQUESTED_DB_TARGET="${1:-}"
DUCK_VERIFICATION_DB_TARGET="${REQUESTED_DB_TARGET:-${DUCK_VERIFICATION_DB_TARGET:-hot}}"
REMOTE_ENV_PATH="/home/alexey/openclaw/apps/liq-bot-duck/.env.vps_postgres"

if [[ "${DUCK_VERIFICATION_DB_TARGET}" == "hot" ]]; then
  if [[ -f "${SCRIPT_DIR}/.env.vps_postgres" ]]; then
    RAW_URL="$(grep -E '^DATABASE_URL=' "${SCRIPT_DIR}/.env.vps_postgres" | cut -d= -f2-)"
    CLEAN_URL="${RAW_URL#\'}"
    CLEAN_URL="${CLEAN_URL%\'}"
    export DATABASE_URL="${CLEAN_URL}"
  else
    RAW_URL="$(ssh alexey@88.198.159.214 "grep -E '^DATABASE_URL=' ${REMOTE_ENV_PATH} | cut -d= -f2-")"
    CLEAN_URL="${RAW_URL#\'}"
    CLEAN_URL="${CLEAN_URL%\'}"
    CLEAN_URL="${CLEAN_URL/@127.0.0.1:5432/@127.0.0.1:6543}"
    export DATABASE_URL="${CLEAN_URL}"
  fi
elif [[ "${DUCK_VERIFICATION_DB_TARGET}" =~ ^mduck_cal_[0-9]{8}$ ]]; then
  DB_HOST="${DUCK_CAL_DB_HOST:-127.0.0.1}"
  DB_PORT="${DUCK_CAL_DB_PORT:-5432}"
  DB_USER="${DUCK_CAL_DB_USER:-postgres}"
  DB_NAME="${DUCK_VERIFICATION_DB_TARGET}"
  DB_PASSWORD="$(docker inspect openclaw-postgres --format '{{json .Config.Env}}' | jq -r '.[]' | grep '^POSTGRES_PASSWORD=' | cut -d= -f2-)"
  export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
else
  echo "Unknown DUCK_VERIFICATION_DB_TARGET=${DUCK_VERIFICATION_DB_TARGET}" >&2
  exit 1
fi

if [[ -x "${SCRIPT_DIR}/.venv/bin/python" ]]; then
  export DUCK_VERIFICATION_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
else
  export DUCK_VERIFICATION_PYTHON="python3"
fi
