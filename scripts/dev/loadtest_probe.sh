#!/usr/bin/env bash
# Latency probes for the dashboard read path. Three modes:
#
#   serial:     N sequential queries against /api/dashboard/aggregates with a
#               random end_time jitter to defeat the dashboard's 30s
#               BoundedTTLCache. Reports min / p50 / p95 / max.
#
#   concurrent: N parallel queries (xargs -P N). Useful for exercising the
#               DuckDB connection pool (default size 8) — beyond N=pool
#               you'll see HTTP 503 "pool saturated" responses fire after
#               max_wait=10s, which is the expected behavior.
#
#   endpoints:  Fires one query at each of the 8 dashboard endpoints for a
#               given time range. Smoke test that the full surface works.
#
# Assumes the backend is running at http://127.0.0.1:18002 and that the
# generator has put data in the target hour (see scripts/loadtest_generator.py).
#
# Usage:
#   scripts/dev/loadtest_probe.sh serial     <svc> <hour-start-utc> [iters]
#   scripts/dev/loadtest_probe.sh concurrent <svc> <hour-start-utc> [parallelism]
#   scripts/dev/loadtest_probe.sh endpoints  <svc> <start-utc> <end-utc>

set -euo pipefail

BACKEND="${BACKEND:-http://127.0.0.1:18002}"

usage() {
  sed -n '3,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

_pct() {
  # Read latencies from stdin (one int per line), print min/p50/p95/max.
  python3 -c '
import sys
ts = sorted(int(x) for x in sys.stdin.read().split() if x.strip())
if not ts:
    print("  (no successful samples)")
    sys.exit()
n = len(ts)
p50 = ts[n // 2]
p95 = ts[int(n * 0.95)] if n >= 5 else ts[-1]
print(f"  -> n={n} | min={ts[0]}ms p50={p50}ms p95={p95}ms p99/max={ts[-1]}ms")
'
}

_jitter_end() {
  # Given hour-start, return (hour-start + 1h + uniform[-30, +30]s) as ISO 8601.
  python3 -c '
from datetime import datetime, timezone, timedelta
import random, sys
t = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
t += timedelta(hours=1, seconds=random.randint(-30, 30))
print(t.strftime("%Y-%m-%dT%H:%M:%SZ"))
' "$1"
}

_post_aggregates() {
  # $1=svc, $2=start, $3=end. Echoes <wall_ms>|<http>|<rows>|<cached>
  local svc="$1" start="$2" end="$3"
  local body="{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"chart_interval\":\"1 minute\",\"chart_metric\":\"requests\"}"
  local tmp
  tmp=$(mktemp)
  local t0 t1 http wall rows cached
  t0=$(python3 -c 'import time; print(time.time())')
  http=$(curl -s --max-time 60 -X POST "${BACKEND}/api/dashboard/aggregates" \
    -H 'content-type: application/json' \
    -H "x-fastly-service-id: ${svc}" \
    -d "${body}" -o "${tmp}" -w "%{http_code}")
  t1=$(python3 -c 'import time; print(time.time())')
  wall=$(python3 -c "print(int(($t1 - $t0)*1000))")
  if [ "${http}" = "200" ]; then
    rows=$(python3 -c "import json; r=json.load(open('${tmp}')); print(r.get('total_rows','?'))" 2>/dev/null || echo "?")
    cached=$(python3 -c "import json; r=json.load(open('${tmp}')); print(r.get('_is_cached','?'))" 2>/dev/null || echo "?")
  else
    rows="err"; cached="-"
  fi
  rm -f "${tmp}"
  echo "${wall}|${http}|${rows}|${cached}"
}

cmd_serial() {
  local svc="$1" hour_start="$2" iters="${3:-15}"
  echo "=== serial: ${iters} cache-bust queries against ${svc} hour=${hour_start} ==="
  local results
  results=$(mktemp)
  for i in $(seq 1 "${iters}"); do
    local end
    end=$(_jitter_end "${hour_start}")
    local line
    line=$(_post_aggregates "${svc}" "${hour_start}" "${end}")
    IFS='|' read -r wall http rows cached <<< "${line}"
    echo "  i${i}: wall=${wall}ms http=${http} rows=${rows} cached=${cached} end=${end}"
    if [ "${http}" = "200" ]; then echo "${wall}" >> "${results}"; fi
  done
  _pct < "${results}"
  rm -f "${results}"
}

cmd_concurrent() {
  local svc="$1" hour_start="$2" n="${3:-20}"
  echo "=== concurrent: ${n} parallel queries against ${svc} hour=${hour_start} ==="
  local tmpdir
  tmpdir=$(mktemp -d)

  fire_one() {
    local i="$1" svc="$2" hour_start="$3" tmpdir="$4"
    local end
    end=$(_jitter_end "${hour_start}")
    local line
    line=$(_post_aggregates "${svc}" "${hour_start}" "${end}")
    echo "${i}|${line}" >> "${tmpdir}/results.txt"
  }
  export -f fire_one _post_aggregates _jitter_end
  export BACKEND

  seq 1 "${n}" | xargs -n1 -P "${n}" -I{} bash -c 'fire_one "$@"' _ {} "${svc}" "${hour_start}" "${tmpdir}"

  if [ -f "${tmpdir}/results.txt" ]; then
    sort -t'|' -k1n "${tmpdir}/results.txt" | sed 's/^/  i/'
    echo ""
    echo "  http code counts:"
    awk -F'|' '{print $3}' "${tmpdir}/results.txt" | sort | uniq -c | sed 's/^/   /'
    echo "  latencies (200 only):"
    awk -F'|' '$3==200 {print $2}' "${tmpdir}/results.txt" | _pct
  fi
  rm -rf "${tmpdir}"
}

cmd_endpoints() {
  local svc="$1" start="$2" end="$3"
  echo "=== endpoints: 8 read endpoints against ${svc} window=${start}..${end} ==="
  _probe() {
    local path="$1" body="$2" desc="$3"
    local tmp; tmp=$(mktemp)
    local t0 t1 http
    t0=$(python3 -c 'import time; print(time.time())')
    http=$(curl -s --max-time 60 -X POST "${BACKEND}${path}" \
      -H 'content-type: application/json' \
      -H "x-fastly-service-id: ${svc}" \
      -d "${body}" -o "${tmp}" -w "%{http_code}")
    t1=$(python3 -c 'import time; print(time.time())')
    local ms; ms=$(python3 -c "print(int(($t1 - $t0)*1000))")
    echo "  ${desc}: ${ms}ms http=${http}"
    rm -f "${tmp}"
  }
  _probe "/api/dashboard/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"chart_interval\":\"1 minute\",\"chart_metric\":\"requests\"}" "dashboard/aggregates"
  _probe "/api/dashboard/raw" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"page\":1,\"limit\":50,\"sort\":[]}" "dashboard/raw"
  _probe "/api/dashboard/field-values" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"field\":\"country\",\"limit\":100}" "dashboard/field-values"
  _probe "/api/security/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{}}" "security/aggregates"
  _probe "/api/network-health" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"metric\":\"health_score\",\"bucket_seconds\":60,\"top_n\":30}" "network-health"
  _probe "/api/origin/timeseries" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"timeseries_percentile\":\"p95\"}" "origin/timeseries"
  _probe "/api/origin/slow-urls" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"slow_urls_limit\":50,\"slow_urls_min_requests\":10}" "origin/slow-urls"
  _probe "/api/performance/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{}}" "performance/aggregates"
}

if [ $# -lt 3 ]; then usage; fi

mode="$1"; shift
case "${mode}" in
  serial)     cmd_serial "$@" ;;
  concurrent) cmd_concurrent "$@" ;;
  endpoints)  cmd_endpoints "$@" ;;
  *) usage ;;
esac
