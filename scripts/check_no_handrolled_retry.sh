#!/usr/bin/env bash
# B4/B11: ban new hand-rolled retry loops under backend/.
#
# A for/while loop whose body has both an exception handler and a sleep call is
# a bespoke retry loop; each one re-derives attempt counting, backoff, jitter
# and the terminal-exception rule. New retry behaviour goes through
# backend/core/retry.py:retry_call (tenacity-based; predicate + backoff + budget
# + before_retry hook). Detection is AST based (scripts/check_no_handrolled_retry.py).
#
# Baseline ratchet: the table below is the number of pre-existing loops per
# file -- some are polling/SSE loops or have timing retry_call cannot reproduce
# (see AGENTS.md, "Shared helpers"). Counts may only go DOWN; a new file or a
# higher count fails. When you migrate a loop, lower its count.
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE='
backend/config.py 2
backend/core/_duckdb_status.py 1
backend/core/duckdb.py 1
backend/core/iceberg/_ducklake.py 1
backend/core/query_registry.py 1
backend/provision/declarative/iac_reconciler.py 1
backend/provision/declarative/reconciler.py 1
backend/provision/fos_setup.py 1
backend/provision/orchestrator.py 1
backend/routers/admin/events.py 3
backend/routers/control_room.py 1
backend/routers/provision.py 1
backend/routers/services/core.py 1
backend/utils/valkey_publisher.py 1
'

counts=$(python3 -I scripts/check_no_handrolled_retry.py)

fail=0
while read -r count file; do
  [ -z "${file:-}" ] && continue
  allowed=$(printf '%s\n' "$BASELINE" | awk -v f="$file" '$1 == f { print $2 }')
  allowed=${allowed:-0}
  if [ "$count" -gt "$allowed" ]; then
    echo "ERROR: $file has $count hand-rolled retry loop(s), baseline allows $allowed (B4)." >&2
    echo "  Use backend/core/retry.py:retry_call instead of for/while + try/except + sleep." >&2
    fail=1
  fi
done <<< "$counts"

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "OK: hand-rolled retry loops in backend/ are within the ratchet baseline."
