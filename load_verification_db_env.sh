#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_ENV_PATH="${SCRIPT_DIR}/.env.vps_postgres"
REMOTE_ENV_PATH="/home/alexey/openclaw/apps/liq-bot-duck/.env.vps_postgres"

if [[ -f "${LOCAL_ENV_PATH}" ]]; then
  RAW_URL="$(grep -E '^DATABASE_URL=' "${LOCAL_ENV_PATH}" | cut -d= -f2-)"
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

if [[ -x "${SCRIPT_DIR}/.venv/bin/python" ]]; then
  export DUCK_VERIFICATION_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
else
  export DUCK_VERIFICATION_PYTHON="python3"
fi
