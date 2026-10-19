#!/usr/bin/env python3
"""B11 helper: find hand-rolled retry loops under backend/ (stdlib only).

A retry loop is a ``for``/``while`` whose body (not counting nested function
bodies) contains BOTH an exception handler and a sleep call -- the shape every
bespoke attempt/backoff loop takes. Use ``backend.core.retry.retry_call``
(or tenacity in ``core/fastly/client.py``) instead.

Prints ``<count> <path>`` per offending file, one per line. The ratchet
baseline lives in ``check_no_handrolled_retry.sh``.
"""

from __future__ import annotations

import ast
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
