#!/usr/bin/env bash
# B12: ban raw urllib HTTP in backend app modules.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

ALLOWLIST=(
  "backend/core/fastly/client.py"
  "backend/routers/provision.py"
  "backend/routers/services/__init__.py"
  # 002e76e8: _fetch_file_to_zip moved byte-for-byte from admin/health.py.
  "backend/routers/admin/_helpers.py"
  "backend/routers/admin/high_scale.py"
  # 002e76e8: download_file moved byte-for-byte from admin/compaction.py.
  "backend/routers/admin/downloads.py"
  "backend/routers/admin/ingest.py"
  "backend/core/storage/_core.py"
  # 04759853: pinned FOS/CDN fetch + redirect validation moved byte-for-byte
  # from metadata/base.py; originally sanctioned in metadata/state.py (bcb5fe6c).
  "backend/core/metadata/state_sync.py"
  "backend/core/metadata/base.py"
  "backend/utils/http.py"
)

PATTERN='urllib\.request|urllib\.error|\burlopen\('
offenders=$(git grep -lE "$PATTERN" -- 'backend/**/*.py' ':(exclude)backend/**/test_*.py' 2>/dev/null || true)

check_allowlist_offenders \
  "$offenders" \
  "ERROR: raw urllib HTTP found in backend app module(s) not on the allowlist (B12):" \
  "Route Fastly calls through backend.core.fastly.client.fastly() and other external fetches through backend.utils.http.http_get_text(). If an exception is truly needed, add the file to ALLOWLIST in $0 with a reason." \
  "OK: no raw urllib HTTP in backend app modules (outside the documented allowlist)." \
  "${ALLOWLIST[@]}"
