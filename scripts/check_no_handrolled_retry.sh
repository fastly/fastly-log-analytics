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
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

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

check_ratchet_counts \
  "$counts" \
  "$BASELINE" \
  "hand-rolled retry loop(s), baseline allows __ALLOWED__ (B4)." \
  "Use backend/core/retry.py:retry_call instead of for/while + try/except + sleep." \
  "OK: hand-rolled retry loops in backend/ are within the ratchet baseline."
