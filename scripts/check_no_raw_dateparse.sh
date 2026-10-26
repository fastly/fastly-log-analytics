#!/usr/bin/env bash
# B5/B11: ban new raw date parsing in backend/core and backend/repositories.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='strptime\(|fromisoformat\(|replace\(tzinfo=(UTC|timezone\.utc)'
BASELINE='
backend/core/_duckdb_status.py 1
backend/core/clickhouse.py 1
backend/core/clickhouse_rows.py 1
backend/core/duckdb.py 1
backend/core/iceberg/_core.py 2
backend/core/ingest.py 3
backend/core/metadata/usage_log.py 2
backend/core/rollups/recompute.py 1
backend/core/rollups/rum.py 1
backend/core/rollups/security_dims.py 1
backend/repositories/alerts.py 1
backend/repositories/assets.py 1
backend/repositories/insights/repository.py 1
backend/repositories/rum.py 1
backend/repositories/security.py 1
backend/utils/telemetry.py 2
'

run_regex_ratchet \
  "$PATTERN" \
  "$BASELINE" \
  "raw date-parse site(s), baseline allows __ALLOWED__ (B5)." \
  "Use backend/utils/date_utils.py helpers (parse_hour_key, parse_day_key, ensure_utc, parse_iso_utc)." \
  "OK: raw date parsing in backend/core + backend/repositories is within the ratchet baseline." \
  'backend/core/*.py' 'backend/repositories/*.py'
