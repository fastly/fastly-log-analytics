#!/usr/bin/env bash
# B4/B11: ban new hand-rolled retry loops under backend/.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

BASELINE='
backend/config.py 2
backend/core/_duckdb_status.py 1
backend/core/duckdb.py 1
backend/core/iceberg/_core.py 1
backend/core/iceberg/_ducklake.py 1
backend/core/query_registry.py 1
backend/core/request_telemetry.py 1
backend/provision/declarative/iac_reconciler.py 1
backend/provision/declarative/reconciler.py 1
backend/provision/fos_setup.py 1
backend/provision/orchestrator.py 2
backend/routers/admin/health.py 3
backend/routers/admin/high_scale.py 3
backend/routers/admin/compaction.py 3
backend/routers/control_room.py 1
backend/routers/provision.py 1
backend/routers/services/__init__.py 2
backend/routers/services/core.py 1
backend/utils/telemetry.py 1
'

run_ast_ratchet \
  "$REPO_ROOT" \
  "$BASELINE" \
  "backend/core/retry.py" \
  "hand-rolled retry loop(s), baseline allows __ALLOWED__ (B4)." \
  "Use backend/core/retry.py:retry_call instead of for/while + try/except + sleep." \
  "OK: hand-rolled retry loops in backend/ are within the ratchet baseline."
