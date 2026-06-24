#!/usr/bin/env bash
# perf_gate.sh — load-harness CI regression gate.
#
# Reads tests/perf/baseline.json (schema 2: scenarios_by_scale) for the
# per-scale ceilings and the regression_pct_threshold; reads
# tests/perf/latest.json (emitted by scripts/emit_perf_latest.py with
# its --rows / PERF_NUM_ROWS argument, which sets ``scale_key``) and
# exits non-zero if any scenario's measured p-value is > baseline *
# (1 + threshold/100) for the matching scale section.
#
# Both files MUST exist — the CI workflow runs the emitter immediately
# before this gate, so a missing latest.json is a wiring bug, not a
# soft warning.

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

# Schema 2: baseline.json has ``scenarios_by_scale: {scale_key: {...}}``
# and latest.json reports ``scale_key`` (set by emit_perf_latest.py per
# its --rows argument). Schema 1 fallback: a single flat ``scenarios``
# dict applied to whatever scale produced latest.json. The fallback
# exists only so a baseline.json that hasn't been regenerated since
# the schema bump still works — new edits MUST use schema 2.
scale_key = latest.get("scale_key")
scenarios_by_scale = base.get("scenarios_by_scale")
if scenarios_by_scale is not None:
    if scale_key is None:
        print("ERROR: baseline.json is schema 2 but latest.json has no scale_key", file=sys.stderr)
        sys.exit(2)
    scenarios = scenarios_by_scale.get(scale_key)
    if scenarios is None:
        print(f"ERROR: baseline.json has no section for scale_key={scale_key!r}", file=sys.stderr)
        # _comment keys aren't scales; surface only the real ones.
        available = [k for k in scenarios_by_scale if not k.startswith("_")]
        print(f"   Available: {sorted(available)}", file=sys.stderr)
        sys.exit(2)
else:
    scenarios = base["scenarios"]

pct = base.get("regression_pct_threshold", 10)
fail = False

print(f"  scale: {scale_key or '(schema 1, single scale)'}")
for name, threshold in scenarios.items():
    # Skip JSON-comment keys (``_comment``, ``__notes__``, etc.) so a
    # documented scale section doesn't trigger a spurious warning.
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
