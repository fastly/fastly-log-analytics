#!/usr/bin/env bash
# B3/B6/B11: ban new inline COALESCE(...) null-default SQL in repositories and
# rollup writers.
#
# NULL-default fragments (sum_or_zero, column_or_default, coalesce, the
# origin-latency ottfb/ttfb fallback, HIT_STATES) live in
# backend/core/metric_expressions.py with golden-SQL tests; re-spelling them
# inline is how the same expression drifted per call site. This is a baseline
# ratchet: the table below is the number of pre-existing non-comment
# ``COALESCE(`` lines per file (writer/reader SQL whose text is not yet
# shared, plus prose mentions in docstrings). Counts may only go DOWN; a new
# file or a higher count fails. ``_sql/`` templates are SQL-by-design and are
# excluded by the pathspec.
#
# When you migrate a site, lower its count (delete the row at zero).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='COALESCE\('

BASELINE='
backend/core/rollups/origin_dims.py 4
backend/core/rollups/origin_latency_ts.py 1
backend/core/rollups/overview.py 4
backend/core/rollups/perf_latency.py 1
backend/core/rollups/rum.py 8
backend/core/rollups/sessions.py 2
backend/core/rollups/slow_urls.py 1
backend/core/rollups/time_series.py 2
backend/repositories/_base.py 4
backend/repositories/assets.py 2
backend/repositories/insights/repository.py 1
backend/repositories/network.py 3
backend/repositories/origin.py 1
backend/repositories/sessions.py 1
'

counts=$(git grep -nE "$PATTERN" -- 'backend/core/rollups/*.py' 'backend/repositories/*.py' ':!backend/repositories/_sql/*' 2>/dev/null \
  | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' | cut -d: -f1 | sort | uniq -c || true)

check_ratchet_counts \
  "$counts" \
  "$BASELINE" \
  "inline COALESCE( site(s), baseline allows __ALLOWED__ (B3/B6)." \
  "Use backend/core/metric_expressions.py helpers (sum_or_zero, column_or_default, coalesce, origin_latency_us)." \
  "OK: inline COALESCE( in repositories + rollup writers is within the ratchet baseline."
