#!/usr/bin/env bash
# B4/B11: ban new hand-rolled retry loops under backend/.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

# 002e76e8: transfer the three SSE feeder loops from admin/health.py to events.py.
# Loop ASTs are unchanged apart from sampler hook/import-reference replacements;
# exception handling, sleep intervals and retry behavior retain the same allowance.
# 88e3f468 moved the unchanged _pool_query_watchdog into request_telemetry/query_registry.py;
# transfer its existing one-loop allowance without retaining the deleted module entry.
# bef84c8d moved unchanged ValkeyPublisher.subscribe from telemetry/metrics.py
# to telemetry/log_flusher.py; relocate its existing one-loop allowance.
BASELINE='
backend/config.py 2
backend/core/duckdb.py 1
backend/core/storage/_core.py 1
backend/core/request_telemetry/query_registry.py 1
backend/provision/declarative/reconciler.py 1
backend/provision/fos_setup.py 1
backend/provision/orchestrator.py 2
backend/routers/admin/events.py 3
backend/routers/admin/high_scale.py 3
backend/routers/admin/compaction.py 3
backend/routers/control_room.py 1
backend/routers/provision.py 1
backend/routers/services/__init__.py 2
backend/routers/services/crud.py 1
backend/utils/telemetry/log_flusher.py 1
'

run_ast_ratchet \
  "$REPO_ROOT" \
  "$BASELINE" \
  "backend/core/retry.py" \
  "hand-rolled retry loop(s), baseline allows __ALLOWED__ (B4)." \
  "Use backend/core/retry.py:retry_call instead of for/while + try/except + sleep." \
  "OK: hand-rolled retry loops in backend/ are within the ratchet baseline."
