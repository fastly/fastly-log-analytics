"""``except Exception: pass`` hides failures; best-effort steps must at least log at debug level."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.security_regression

BACKEND = Path(__file__).resolve().parents[2] / "backend"
SCOPED = ("routers", "repositories", "core", "utils")


def _is_silent_exception_handler(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.ExceptHandler)
        and isinstance(node.type, ast.Name)
        and node.type.id == "Exception"
        and len(node.body) == 1
        and isinstance(node.body[0], ast.Pass)
    )


def test_no_silent_except_exception_pass_in_scoped_packages():
    offenders = []
    for pkg in SCOPED:
        for path in (BACKEND / pkg).rglob("*.py"):
            if "tests" in path.parts:
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                if _is_silent_exception_handler(node):
                    offenders.append(f"{path.relative_to(BACKEND.parent)}:{node.lineno}")
    assert offenders == []


def test_guard_detects_a_silent_handler():
    tree = ast.parse("try:\n    x()\nexcept Exception:\n    pass\n")
    assert any(_is_silent_exception_handler(n) for n in ast.walk(tree))
    tree = ast.parse("try:\n    x()\nexcept Exception:\n    log.debug('x', exc_info=True)\n")
    assert not any(_is_silent_exception_handler(n) for n in ast.walk(tree))
