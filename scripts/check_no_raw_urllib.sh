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
  "backend/routers/admin/health.py"
  "backend/routers/admin/high_scale.py"
  "backend/routers/admin/compaction.py"
  "backend/routers/admin/ingest.py"
  "backend/core/iceberg/_core.py"
  "backend/core/metadata/state.py"
  "backend/core/metadata/base.py"
  "backend/utils/http.py"
  "backend/utils/telemetry.py"
)

PATTERN='urllib\.request|urllib\.error|\burlopen\('
offenders=$(git grep -lE "$PATTERN" -- 'backend/**/*.py' ':(exclude)backend/**/test_*.py' 2>/dev/null || true)

check_allowlist_offenders \
  "$offenders" \
  "ERROR: raw urllib HTTP found in backend app module(s) not on the allowlist (B12):" \
  "Route Fastly calls through backend.core.fastly.client.fastly() and other external fetches through backend.utils.http.http_get_text(). If an exception is truly needed, add the file to ALLOWLIST in $0 with a reason." \
  "OK: no raw urllib HTTP in backend app modules (outside the documented allowlist)." \
  "${ALLOWLIST[@]}"
