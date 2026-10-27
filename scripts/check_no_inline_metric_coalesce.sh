#!/usr/bin/env bash
# B3/B6/B11: ban new inline COALESCE(...) null-default SQL in repositories and rollup writers.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='COALESCE\('
BASELINE='
backend/core/rollups/origin_dims.py 5
backend/core/rollups/recompute.py 11
backend/repositories/_base.py 4
backend/repositories/assets.py 2
backend/repositories/insights/repository.py 1
backend/repositories/network.py 3
backend/repositories/origin.py 24
backend/repositories/rum.py 28
backend/repositories/security.py 35
backend/repositories/sessions.py 1
'

run_regex_ratchet \
  "$PATTERN" \
  "$BASELINE" \
  "inline COALESCE( site(s), baseline allows __ALLOWED__ (B3/B6)." \
  "Use backend/core/metric_expressions.py helpers (sum_or_zero, column_or_default, coalesce, origin_latency_us)." \
  "OK: inline COALESCE( in repositories + rollup writers is within the ratchet baseline." \
  'backend/core/rollups/*.py' 'backend/repositories/*.py' ':!backend/repositories/_sql/*'
