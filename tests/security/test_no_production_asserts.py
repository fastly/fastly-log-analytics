"""Production code must not use ``assert`` for runtime guards.

``python -O`` strips ``assert`` statements, silently removing the check (including the
destructive-operation whitelist guards in the declarative reconciler). Use explicit
exceptions instead.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.security_regression

BACKEND = Path(__file__).resolve().parents[2] / "backend"


def test_backend_has_no_assert_statements():
    offenders = []
    for path in BACKEND.rglob("*.py"):
        if "tests" in path.relative_to(BACKEND).parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Assert):
                offenders.append(f"{path.relative_to(BACKEND.parent)}:{node.lineno}")
    assert offenders == []


def test_reconciler_whitelist_guard_survives_python_optimize():
    from backend.provision.declarative import reconciler

    src = Path(reconciler.__file__).read_text()
    assert "MANAGED_BACKEND_NAMES" in src
    tree = ast.parse(src)
    guards = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and isinstance(n.test, ast.Compare)
        and isinstance(n.test.ops[0], ast.NotIn)
        and any(isinstance(b, ast.Raise) for b in n.body)
    ]
    assert len(guards) >= 2
