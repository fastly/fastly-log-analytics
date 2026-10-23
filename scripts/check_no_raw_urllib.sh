#!/usr/bin/env bash
# B12: ban raw urllib HTTP in backend app modules.
#
# All Fastly API traffic must go through backend/core/fastly/client.py (the
# central `fastly()` wrapper: pinned host + Fastly-Key auth + retries +
# telemetry); other external fetches go through backend/utils/http.py
# (`http_get_text`); shared httpx clients (RT poller, RUM FOS assets, OAuth
# discovery) come from `make_client` there too. A hand-rolled `urllib.request` / `urlopen` bypasses both,
# losing auth threading, retries, telemetry, and the single choke point — which
# is exactly how NGWAF + bot-source fetches drifted off the client (finding
# B12). This is a baseline ratchet: the allowlist below is the set of
# pre-existing, intentional urllib users; nothing new may be added.
#
# To (rarely) add a legitimate exception: add the file to ALLOWLIST and a short
# inline comment at the call site explaining why the central client can't apply.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

# Files allowed to use raw urllib.request/urlopen, each for a documented reason:
#   - core/fastly/client.py          the central client itself (the sanctioned urllib user)
#   - routers/provision.py           SSRF-safe domain probe only (Fastly-host calls go through fastly())
#   - routers/admin/_helpers.py      pinned CDN download helper
#   - routers/admin/downloads.py     streaming CDN log download (not a Fastly API call)
#   - core/iceberg/lake_info.py      pinned FOS/CDN fetch
#   - core/iceberg/sync.py           pinned FOS/CDN fetch
#   - core/iceberg/_core.py          pinned FOS/CDN metadata fetch
ALLOWLIST=(
  "backend/core/fastly/client.py"
  "backend/routers/provision.py"
  "backend/routers/admin/_helpers.py"
  "backend/routers/admin/downloads.py"
  "backend/core/iceberg/lake_info.py"
  "backend/core/iceberg/sync.py"
  "backend/core/iceberg/_core.py"
)

# Match an import of urllib.request or a bare urlopen( call in backend app code.
# Exclude tests (they may legitimately patch urllib on third-party paths).
PATTERN='urllib\.request|urllib\.error|\burlopen\('

offenders=$(git grep -lE "$PATTERN" -- 'backend/**/*.py' ':(exclude)backend/**/test_*.py' 2>/dev/null || true)

check_allowlist_offenders \
  "$offenders" \
  "ERROR: raw urllib HTTP found in backend app module(s) not on the allowlist (B12):" \
  "Route Fastly calls through backend.core.fastly.client.fastly() and other
external fetches through backend.utils.http.http_get_text(). If an
exception is truly needed, add the file to ALLOWLIST in $0 with a reason." \
  "OK: no raw urllib HTTP in backend app modules (outside the documented allowlist)." \
  "${ALLOWLIST[@]}"
