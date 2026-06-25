#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Always pin the default verification contour explicitly so user flags
# (`--mode`, `--hours`, etc.) are not misread by the sourced env-loader.
source "${SCRIPT_DIR}/load_verification_db_env.sh" hot

cd "${SCRIPT_DIR}"
"${DUCK_VERIFICATION_PYTHON}" autonomous_oi_replay.py "$@"
