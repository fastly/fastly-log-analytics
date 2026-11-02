#!/usr/bin/env bash
# perf_gate.sh — load-harness CI regression gate + consolidated perf/gateway helpers:
#   default (no flags)          : CI perf regression gate (baseline.json vs latest.json)
#   --validate-admin-gateway    : explicit portable admin-gateway validation
#   --baseline-metrics          : snapshot architectural LOC/lint/mypy metrics to .metrics/baseline/<ts>/
#   --loadtest-probe <mode> ... : latency probes (serial|concurrent|endpoints) for dashboard read path

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

run_validate_admin_gateway() {
  for tool in caddy openssl helm uv node docker; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      printf 'Missing validation tool: %s; install it explicitly and retry.\n' "$tool" >&2
      exit 1
    fi
  done
  export ADMIN_MTLS_TEST_REQUIRED=1
  uv run pytest -n 0 \
    tests/test_admin_certificates.py \
    tests/chart/test_helm.py \
    tests/deploy/test_gateway_deployment.py
  node --test tests/deploy/admin_gateway_client.test.cjs
  helm lint deploy/chart/admin-gateway \
    --set host=admin.example.com --set existingSecret=operator-gateway \
    --set frontendUpstream=frontend:3000 --set backendUpstream=backend:8000 \
    --set image=registry.example.com/fla-admin-gateway:validate
}

run_baseline_metrics() {
  local TS OUT BACKEND_TOTAL FRONTEND_TOTAL BACKEND_OVER_1500 BACKEND_OVER_2500 FRONTEND_OVER_500 TODO_COUNT SECURITY_COMMENT_COUNT MYPY_IGNORE_COUNT
  TS=$(date -u +%Y%m%dT%H%M%SZ)
  OUT=".metrics/baseline/$TS"
  mkdir -p "$OUT"
  echo "→ baseline metrics → $OUT"

  find backend -name "*.py" -print0 | xargs -0 wc -l | sort -rn > "$OUT/backend_loc.txt"
  find frontend -type f \( -name "*.ts" -o -name "*.tsx" \) \
      | grep -v node_modules | grep -v .next | grep -v ".generated" \
      | xargs wc -l 2>/dev/null | sort -rn > "$OUT/frontend_loc.txt"

  {
      echo "=== Backend files > 1500 lines ==="
      awk '$1 > 1500 && $2 != "total" {print}' "$OUT/backend_loc.txt" | head -30
      echo
      echo "=== Backend files > 2500 lines (Phase 5b + 6 + 7 + 10 carve targets) ==="
      awk '$1 > 2500 && $2 != "total" {print}' "$OUT/backend_loc.txt"
      echo
      echo "=== Frontend files > 500 lines (Phase 9b carve targets) ==="
      awk '$1 > 500 && $2 != "total" {print}' "$OUT/frontend_loc.txt" | head -30
  } > "$OUT/large_files.txt"

  grep -rn --include="*.py" --include="*.ts" --include="*.tsx" \
      -E "\\b(TODO|FIXME|XXX|HACK)\\b" backend/ frontend/ 2>/dev/null \
      | grep -v node_modules | grep -v ".next/" | grep -v ".generated" \
      | grep -v "\\\\uXXXX" | grep -v "uXXXX escapes" > "$OUT/todo_grep.txt" || true

  grep -rn "# Security:" backend/ --include="*.py" 2>/dev/null > "$OUT/security_comments.txt"

  awk '/ignore_errors = true/{flag=1; next} /^\[/{flag=0} /^\[\[tool.mypy.overrides\]\]/{capture=1; next} capture && /"backend/{print} /^\]/ && capture{capture=0}' pyproject.toml \
      > "$OUT/mypy_overrides.txt" || true

  {
      echo "=== Baseline counts at $TS ==="
      echo
      BACKEND_TOTAL=$(awk '$2 == "total" {print $1}' "$OUT/backend_loc.txt")
      FRONTEND_TOTAL=$(awk '$2 == "total" {print $1}' "$OUT/frontend_loc.txt")
      BACKEND_OVER_1500=$(awk '$1 > 1500 && $2 != "total"' "$OUT/backend_loc.txt" | wc -l | tr -d ' ')
      BACKEND_OVER_2500=$(awk '$1 > 2500 && $2 != "total"' "$OUT/backend_loc.txt" | wc -l | tr -d ' ')
      FRONTEND_OVER_500=$(awk '$1 > 500 && $2 != "total"' "$OUT/frontend_loc.txt" | wc -l | tr -d ' ')
      TODO_COUNT=$(wc -l < "$OUT/todo_grep.txt" | tr -d ' ')
      SECURITY_COMMENT_COUNT=$(wc -l < "$OUT/security_comments.txt" | tr -d ' ')
      MYPY_IGNORE_COUNT=$(wc -l < "$OUT/mypy_overrides.txt" | tr -d ' ')

      echo "backend total LOC: $BACKEND_TOTAL"
      echo "frontend total LOC: $FRONTEND_TOTAL"
      echo "backend files > 2500 lines: $BACKEND_OVER_2500   (target end-state: 0)"
      echo "backend files > 1500 lines: $BACKEND_OVER_1500   (target end-state: ≤ 2)"
      echo "frontend files > 500 lines: $FRONTEND_OVER_500   (target end-state: 0)"
      echo "TODO/FIXME/XXX/HACK markers: $TODO_COUNT   (target end-state: 0)"
      echo "# Security: source comments: $SECURITY_COMMENT_COUNT   (regression-mark floor)"
      echo "mypy ignore_errors modules:  $MYPY_IGNORE_COUNT   (target end-state: ≤ 3)"
      echo
      echo "Coverage gate (in .github/workflows/ci.yml):"
      grep -E "cov-fail-under|coverage.thresholds.lines" .github/workflows/ci.yml || true
  } > "$OUT/summary.txt"

  cat "$OUT/summary.txt"
  echo
  echo "→ written to $OUT/"
}

run_loadtest_probe() {
  BACKEND="${BACKEND:-http://127.0.0.1:18002}"
  _pct() {
    python3 -c '
import sys
ts = sorted(int(x) for x in sys.stdin.read().split() if x.strip())
if not ts:
    print("  (no successful samples)"); sys.exit()
n = len(ts); p50 = ts[n // 2]; p95 = ts[int(n * 0.95)] if n >= 5 else ts[-1]
print(f"  -> n={n} | min={ts[0]}ms p50={p50}ms p95={p95}ms p99/max={ts[-1]}ms")
'
  }
  _jitter_end() {
    python3 -c '
from datetime import datetime, timedelta
import random, sys
t = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")) + timedelta(hours=1, seconds=random.randint(-30, 30))
print(t.strftime("%Y-%m-%dT%H:%M:%SZ"))
' "$1"
  }
  _post_aggregates() {
    local svc="$1" start="$2" end="$3" tmp t0 t1 http wall rows cached
    local body="{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"chart_interval\":\"1 minute\",\"chart_metric\":\"requests\"}"
    tmp=$(mktemp)
    t0=$(python3 -c 'import time; print(time.time())')
    http=$(curl -s --max-time 60 -X POST "${BACKEND}/api/dashboard/aggregates" \
      -H 'content-type: application/json' -H "x-fastly-service-id: ${svc}" \
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
  [ $# -ge 3 ] || { echo "Usage: $0 --loadtest-probe <serial|concurrent|endpoints> <svc> <start-utc> [arg]" >&2; exit 1; }
  local mode="$1"; shift
  case "${mode}" in
    serial)
      local svc="$1" hour_start="$2" iters="${3:-15}" results end line wall http rows cached
      echo "=== serial: ${iters} cache-bust queries against ${svc} hour=${hour_start} ==="
      results=$(mktemp)
      for i in $(seq 1 "${iters}"); do
        end=$(_jitter_end "${hour_start}")
        line=$(_post_aggregates "${svc}" "${hour_start}" "${end}")
        IFS='|' read -r wall http rows cached <<< "${line}"
        echo "  i${i}: wall=${wall}ms http=${http} rows=${rows} cached=${cached} end=${end}"
        [ "${http}" = "200" ] && echo "${wall}" >> "${results}"
      done
      _pct < "${results}"; rm -f "${results}" ;;
    concurrent)
      local svc="$1" hour_start="$2" n="${3:-20}" tmpdir
      echo "=== concurrent: ${n} parallel queries against ${svc} hour=${hour_start} ==="
      tmpdir=$(mktemp -d)
      fire_one() {
        local i="$1" svc="$2" hour_start="$3" tmpdir="$4" end line
        end=$(_jitter_end "${hour_start}")
        line=$(_post_aggregates "${svc}" "${hour_start}" "${end}")
        echo "${i}|${line}" >> "${tmpdir}/results.txt"
      }
      export -f fire_one _post_aggregates _jitter_end
      export BACKEND
      seq 1 "${n}" | xargs -n1 -P "${n}" -I{} bash -c 'fire_one "$@"' _ {} "${svc}" "${hour_start}" "${tmpdir}"
      if [ -f "${tmpdir}/results.txt" ]; then
        sort -t'|' -k1n "${tmpdir}/results.txt" | sed 's/^/  i/'
        echo -e "\n  http code counts:"
        awk -F'|' '{print $3}' "${tmpdir}/results.txt" | sort | uniq -c | sed 's/^/   /'
        echo "  latencies (200 only):"
        awk -F'|' '$3==200 {print $2}' "${tmpdir}/results.txt" | _pct
      fi
      rm -rf "${tmpdir}" ;;
    endpoints)
      local svc="$1" start="$2" end="$3"
      echo "=== endpoints: 7 read endpoints against ${svc} window=${start}..${end} ==="
      _probe() {
        local path="$1" body="$2" desc="$3" tmp t0 t1 http ms
        tmp=$(mktemp); t0=$(python3 -c 'import time; print(time.time())')
        http=$(curl -s --max-time 60 -X POST "${BACKEND}${path}" \
          -H 'content-type: application/json' -H "x-fastly-service-id: ${svc}" \
          -d "${body}" -o "${tmp}" -w "%{http_code}")
        t1=$(python3 -c 'import time; print(time.time())')
        ms=$(python3 -c "print(int(($t1 - $t0)*1000))")
        echo "  ${desc}: ${ms}ms http=${http}"; rm -f "${tmp}"
      }
      _probe "/api/dashboard/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"chart_interval\":\"1 minute\",\"chart_metric\":\"requests\"}" "dashboard/aggregates"
      _probe "/api/dashboard/field-values" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"field\":\"country\",\"limit\":100}" "dashboard/field-values"
      _probe "/api/security/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{}}" "security/aggregates"
      _probe "/api/network-health" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"metric\":\"health_score\",\"bucket_seconds\":60,\"top_n\":30}" "network-health"
      _probe "/api/origin/timeseries" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"timeseries_percentile\":\"p95\"}" "origin/timeseries"
      _probe "/api/origin/slow-urls" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{},\"slow_urls_limit\":50,\"slow_urls_min_requests\":10}" "origin/slow-urls"
      _probe "/api/performance/aggregates" "{\"start_time\":\"${start}\",\"end_time\":\"${end}\",\"filters\":{}}" "performance/aggregates" ;;
    *) echo "Unknown loadtest-probe mode: ${mode}" >&2; exit 1 ;;
  esac
}

case "${1:-}" in
  --validate-admin-gateway) shift; run_validate_admin_gateway "$@"; exit 0 ;;
  --baseline-metrics)       shift; run_baseline_metrics "$@"; exit 0 ;;
  --loadtest-probe)         shift; run_loadtest_probe "$@"; exit 0 ;;
esac

BASELINE="tests/perf/baseline.json"
LATEST="tests/perf/latest.json"

if [[ ! -f "$BASELINE" ]]; then
    echo "ERROR: baseline file missing at $BASELINE" >&2
    exit 2
fi

if [[ ! -f "$LATEST" ]]; then
    echo "ERROR: latest.json missing at $LATEST" >&2
    echo "   The CI workflow should run scripts/emit_perf_latest.py before this gate." >&2
    exit 2
fi

python3 - <<'PY'
import json, sys

with open("tests/perf/baseline.json") as f:
    base = json.load(f)
with open("tests/perf/latest.json") as f:
    latest = json.load(f)

scale_key = latest.get("scale_key")
scenarios_by_scale = base.get("scenarios_by_scale")
if scenarios_by_scale is not None:
    if scale_key is None:
        print("ERROR: baseline.json is schema 2 but latest.json has no scale_key", file=sys.stderr)
        sys.exit(2)
    scenarios = scenarios_by_scale.get(scale_key)
    if scenarios is None:
        print(f"ERROR: baseline.json has no section for scale_key={scale_key!r}", file=sys.stderr)
        available = [k for k in scenarios_by_scale if not k.startswith("_")]
        print(f"   Available: {sorted(available)}", file=sys.stderr)
        sys.exit(2)
else:
    scenarios = base["scenarios"]

pct = base.get("regression_pct_threshold", 10)
fail = False

print(f"  scale: {scale_key or '(schema 1, single scale)'}")
for name, threshold in scenarios.items():
    if name.startswith("_"):
        continue
    actual = latest.get("scenarios", {}).get(name)
    if actual is None:
        print(f"⚠️  scenario {name!r} missing from latest.json")
        continue
    ceiling = threshold * (1 + pct / 100)
    status = "OK"
    if actual > ceiling:
        status = f"FAIL (>{pct}% over baseline {threshold})"
        fail = True
    print(f"  {name}: actual={actual} baseline={threshold} ceiling={ceiling:.0f} {status}")

sys.exit(1 if fail else 0)
PY
