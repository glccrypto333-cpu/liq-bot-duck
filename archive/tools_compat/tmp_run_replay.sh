#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "tmp_run_replay.sh is deprecated; forwarding to run_verification_replay.sh" >&2
exec "${SCRIPT_DIR}/run_verification_replay.sh" "$@"
