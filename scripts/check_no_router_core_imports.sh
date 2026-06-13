#!/usr/bin/env bash
# check_no_router_core_imports.sh
#
# Phase 5b §5b.1 architectural gate: routers must NOT import directly
# from backend.core.*. The repository layer is the only allowed
# consumer of core modules.
#
# Today (Phase 0 baseline) the count is 117 imports across 11 router
# files. This script reports the count; gate-mode enforcement is opt-in
# via CHECK_NO_ROUTER_CORE_GATE=1 so we can ship the script now and flip
# the gate when Phase 5b's repository facades are in place.
#
# Run:    bash scripts/check_no_router_core_imports.sh
# Gate:   CHECK_NO_ROUTER_CORE_GATE=1 bash scripts/check_no_router_core_imports.sh
# Floor:  read from .check_router_core_floor (current actual count); the
# gate is "monotonically downward" — count must <= previous floor.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FLOOR_FILE=".check_router_core_floor"

count=$(grep -rc "from backend\\.core" backend/routers --include="*.py" 2>/dev/null \
    | awk -F: '{ s += $2 } END { print s+0 }')

echo "Router → backend.core imports: $count"

if [[ -f "$FLOOR_FILE" ]]; then
    floor=$(cat "$FLOOR_FILE")
    echo "Previous floor: $floor"
    if [[ -n "${CHECK_NO_ROUTER_CORE_GATE:-}" ]]; then
        if (( count > floor )); then
            echo "FAIL: count went UP (was $floor, now $count)" >&2
            echo "Routers are growing their backend.core dependency, opposite of Phase 5b's direction." >&2
            exit 1
        fi
        # Auto-tighten when count drops — anti-rachet.
        if (( count < floor )); then
            echo "$count" > "$FLOOR_FILE"
            echo "Floor tightened to $count"
        fi
        echo "OK"
    else
        echo "(gate disabled — set CHECK_NO_ROUTER_CORE_GATE=1 to enforce)"
    fi
else
    # First run: write the baseline.
    echo "$count" > "$FLOOR_FILE"
    echo "Baseline floor written: $count"
fi
