#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/load_verification_db_env.sh"

cd "${SCRIPT_DIR}"
"${DUCK_VERIFICATION_PYTHON}" tmp_stage2_probe.py "$@"
