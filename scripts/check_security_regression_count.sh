#!/usr/bin/env bash
# check_security_regression_count.sh
#
# Asserts the @pytest.mark.security_regression count is monotonically
# >= the Phase 0 baseline (24). Phase 0.8 of the v2.0 cleanup plan
# requires this gate so a refactor can't silently drop coverage of a
# verified security fix.
#
# Run locally: bash scripts/check_security_regression_count.sh
# Run in CI:   same; exits 1 if count < floor.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Floor derived from audit-findings/README.md (24 verified findings as of
# 2026-06-08). Bump ONLY when (a) a new fix lands AND its test gets
# the mark added — never to "fix" a regression.
FLOOR=24

# Count: uv run pytest -m security_regression --collect-only
# We use pytest's own collection so module-level pytestmark = ... is
# resolved correctly (a plain grep of decorators would miss those).
# Output ends with "N/M tests collected" — extract N (the matched count).
COUNT=$(uv run pytest -m security_regression --collect-only 2>/dev/null \
    | grep -E "tests? collected" \
    | tail -1 \
    | sed -E 's|^([0-9]+)/.*|\1|')

if [[ -z "$COUNT" || ! "$COUNT" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not parse security_regression test count" >&2
    exit 2
fi

echo "security_regression tests: $COUNT (floor: $FLOOR)"

if (( COUNT < FLOOR )); then
    echo "FAIL: count dropped below floor — a verified security fix lost test coverage." >&2
    echo "If the drop is intentional (e.g., a fix became structurally impossible to regress)," >&2
    echo "lower the FLOOR in this script in the same PR and explain in the commit message." >&2
    exit 1
fi

echo "OK"
