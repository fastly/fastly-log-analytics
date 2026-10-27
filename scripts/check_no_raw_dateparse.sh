#!/usr/bin/env bash
# B5/B11: ban new raw date parsing in backend/core and backend/repositories.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='strptime\(|fromisoformat\(|replace\(tzinfo=(UTC|timezone\.utc)'
# 002e76e8: _parse_rum_line moved byte-for-byte from ingest.py to rum_ingest.py;
# transfer its one existing date-parse site, keeping their combined allowance at 3.
BASELINE='
backend/core/clickhouse.py 2
backend/core/duckdb.py 1
backend/core/storage/_core.py 2
backend/core/ingest.py 2
backend/core/metadata/usage_log.py 2
backend/core/rollups/recompute.py 1
backend/core/rollups/security_dims.py 1
backend/core/rum_ingest.py 1
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
