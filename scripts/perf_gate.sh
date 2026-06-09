#!/usr/bin/env bash
# perf_gate.sh — load-harness CI step (Phase 0.9 scaffolding; wired in Phase 1.6)
#
# Reads tests/perf/baseline.json for the per-scenario thresholds and the
# regression_pct_threshold; reads a freshly-produced tests/perf/latest.json
# (emitted by the load harness in Phase 1.6) and exits non-zero if any
# scenario's measured p-value is > baseline * (1 + threshold/100).
#
# Phase 0 ships this as a no-op (latest.json missing → skip with a warning)
# so the CI job is in place but doesn't fail before Phase 1.6 hooks the
# emitter. Phase 1.6 makes the "skip if missing" branch a hard fail.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BASELINE="tests/perf/baseline.json"
LATEST="tests/perf/latest.json"

if [[ ! -f "$BASELINE" ]]; then
    echo "ERROR: baseline file missing at $BASELINE" >&2
    exit 2
fi

if [[ ! -f "$LATEST" ]]; then
    echo "⚠️  load-harness latest.json not present — Phase 1.6 will wire this."
    echo "   Skipping perf gate for now."
    exit 0
fi

python3 - <<'PY'
import json, sys

with open("tests/perf/baseline.json") as f:
    base = json.load(f)
with open("tests/perf/latest.json") as f:
    latest = json.load(f)

pct = base.get("regression_pct_threshold", 10)
fail = False

for name, threshold in base["scenarios"].items():
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
