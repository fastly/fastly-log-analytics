#!/usr/bin/env bash
# baseline_metrics.sh
#
# Snapshot the architectural metrics that the v2.0 cleanup plan tracks.
# Run at Phase 0 (now) and again at end of Phase 10. The diff is the
# success criteria scorecard.
#
# Outputs to: pending-docs/baseline/<UTC-timestamp>/
#   - backend_loc.txt          per-file line counts (sorted desc) + total
#   - frontend_loc.txt         same for frontend .ts/.tsx
#   - large_files.txt          backend files > 1500 lines + frontend > 500 lines
#   - todo_grep.txt            TODO/FIXME/XXX/HACK markers
#   - security_comments.txt    # Security: tagged comments (regression count baseline)
#   - mypy_overrides.txt       modules currently under [[tool.mypy.overrides]] ignore_errors
#   - ignore_count.txt         counts (mypy ignores, security tags, todos, large files)
#
# Coverage is captured by CI (uv run pytest --cov, vitest --coverage) and
# not duplicated here — the CI gate ratchets are the authoritative source.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="pending-docs/baseline/$TS"
mkdir -p "$OUT"

echo "→ baseline metrics → $OUT"

# Backend line counts
find backend -name "*.py" -print0 | xargs -0 wc -l | sort -rn > "$OUT/backend_loc.txt"

# Frontend line counts (exclude generated, node_modules, .next)
find frontend -type f \( -name "*.ts" -o -name "*.tsx" \) \
    | grep -v node_modules \
    | grep -v .next \
    | grep -v ".generated" \
    | xargs wc -l 2>/dev/null \
    | sort -rn > "$OUT/frontend_loc.txt"

# Large files (success-criteria-relevant thresholds)
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

# TODO/FIXME/XXX/HACK marker grep (Phase 10.9 must close to zero net new)
grep -rn --include="*.py" --include="*.ts" --include="*.tsx" \
    -E "\\b(TODO|FIXME|XXX|HACK)\\b" backend/ frontend/ \
    2>/dev/null \
    | grep -v node_modules \
    | grep -v ".next/" \
    | grep -v ".generated" \
    | grep -v "\\\\uXXXX" \
    | grep -v "uXXXX escapes" \
    > "$OUT/todo_grep.txt" || true

# Security-tagged comments — the @pytest.mark.security_regression baseline
# uses this AND the audit-findings/ remediation log. Phase 0.8 sets the
# pytest mark up; this is the source-comment counterpart.
grep -rn "# Security:" backend/ --include="*.py" 2>/dev/null > "$OUT/security_comments.txt"

# mypy override list (modules currently under ignore_errors)
awk '/ignore_errors = true/{flag=1; next} /^\[/{flag=0} /^\[\[tool.mypy.overrides\]\]/{capture=1; next} capture && /"backend/{print} /^\]/ && capture{capture=0}' pyproject.toml \
    > "$OUT/mypy_overrides.txt" || true

# Summary counts
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
