#!/usr/bin/env bash
# B4/B11: ban new hand-rolled retry loops under backend/.
#
# A for/while loop whose body has both an exception handler and a sleep call is
# a bespoke retry loop; each one re-derives attempt counting, backoff, jitter
# and the terminal-exception rule. New retry behaviour goes through
# backend/core/retry.py:retry_call (tenacity-based; predicate + backoff + budget
# + before_retry hook). Detection is AST based (inlined below).
#
# Baseline ratchet: the table below is the number of pre-existing loops per
# file -- some are polling/SSE loops or have timing retry_call cannot reproduce
# (see AGENTS.md, "Shared helpers"). Counts may only go DOWN; a new file or a
# higher count fails. When you migrate a loop, lower its count.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
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

counts=$(python3 -I - "$REPO_ROOT" <<'PY'
from __future__ import annotations

import ast
import os
import subprocess
import sys
from collections import Counter

SLEEP_NAMES = {"sleep", "_retry_sleep"}
EXEMPT = {"backend/core/retry.py"}


def _walk_same_scope(node: ast.AST):
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            continue
        yield child
        yield from _walk_same_scope(child)


def _is_sleep(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
    return name in SLEEP_NAMES


def retry_loops(source: str) -> int:
    tree = ast.parse(source)
    n = 0
    for loop in ast.walk(tree):
        if not isinstance(loop, ast.For | ast.AsyncFor | ast.While):
            continue
        body = [
            c
            for stmt in loop.body
            if not isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
            for c in (stmt, *_walk_same_scope(stmt))
        ]
        if any(isinstance(c, ast.ExceptHandler) for c in body) and any(_is_sleep(c) for c in body):
            n += 1
    return n


def main() -> int:
    os.chdir(sys.argv[1])
    files = subprocess.run(
        ["git", "ls-files", "backend/*.py"], capture_output=True, text=True, check=True
    ).stdout.split()
    counts: Counter[str] = Counter()
    for path in files:
        if path in EXEMPT:
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                n = retry_loops(fh.read())
        except (OSError, SyntaxError):
            continue
        if n:
            counts[path] = n
    for path, n in sorted(counts.items()):
        print(n, path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
PY
)

check_ratchet_counts \
  "$counts" \
  "$BASELINE" \
  "hand-rolled retry loop(s), baseline allows __ALLOWED__ (B4)." \
  "Use backend/core/retry.py:retry_call instead of for/while + try/except + sleep." \
  "OK: hand-rolled retry loops in backend/ are within the ratchet baseline."
