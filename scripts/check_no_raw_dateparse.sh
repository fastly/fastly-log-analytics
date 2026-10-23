#!/usr/bin/env bash
# B5/B11: ban new raw date parsing in backend/core and backend/repositories.
#
# Partition keys (YYYY-MM-DD-HH / YYYY-MM-DD) and naive->UTC normalisation go
# through backend/utils/date_utils.py (parse_hour_key[_strict], parse_day_key
# [_strict], ensure_utc, parse_iso_utc). A hand-rolled strptime / fromisoformat
# / .replace(tzinfo=UTC) re-derives the tz semantics at every site, which is how
# aware-vs-naive drift crept in. This is a baseline ratchet: the table below is
# the number of pre-existing, intentional raw sites per file (each has a format
# or tz-handling rule the shared helpers don't cover). Counts may only go DOWN;
# a new file or a higher count fails.
#
# When you migrate a site, lower its count (delete the row at zero).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='strptime\(|fromisoformat\(|replace\(tzinfo=(UTC|timezone\.utc)'

BASELINE='
backend/core/_duckdb_status.py 1
backend/core/clickhouse_rows.py 1
backend/core/iceberg/manifest.py 2
backend/core/ingest.py 3
backend/core/metadata/usage_log.py 2
backend/core/rollups/rum.py 1
backend/core/rollups/wellknown_bots.py 1
backend/repositories/alerts.py 1
backend/repositories/insights/repository.py 1
'

# Non-comment lines only; tests are excluded by the pathspec.
counts=$(git grep -nE "$PATTERN" -- 'backend/core/*.py' 'backend/repositories/*.py' 2>/dev/null \
  | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' | cut -d: -f1 | sort | uniq -c || true)

check_ratchet_counts \
  "$counts" \
  "$BASELINE" \
  "raw date-parse site(s), baseline allows __ALLOWED__ (B5)." \
  "Use backend/utils/date_utils.py helpers (parse_hour_key, parse_day_key, ensure_utc, parse_iso_utc)." \
  "OK: raw date parsing in backend/core + backend/repositories is within the ratchet baseline."
