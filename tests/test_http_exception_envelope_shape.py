"""AST linter: every ``HTTPException(detail=...)`` in ``backend/routers/`` must
use one of the canonical envelope helpers (``bad_request``, ``not_found``,
``validation_failed``, ``raise_internal``) or a dict literal whose first key
is ``"error"``.

Why: the frontend's ``extractApiError`` keys on ``detail.error`` (a machine-
readable code) so the UI can pattern-match on the code rather than substring-
matching on free-text. Free-form ``detail="some message"`` strings have
drifted into the router tree historically; this guard keeps the surface
uniform as new routes land.

The linter is deliberately conservative — it permits:
- Helper calls: ``bad_request(...)`` / ``not_found(...)`` / ``validation_failed(...)``
- Dict literals with an ``"error"`` first key
- HTTP-standard reason-phrase strings on 415 (Unsupported Media Type) — the
  framework + clients understand those by code

It flags everything else as a finding the developer should either rewrite or
explicitly grant via the ALLOWED list below.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROUTERS = Path(__file__).resolve().parent.parent / "backend" / "routers"

# Explicit allowlist for HTTP-standard reason-phrase strings that callers may
# want to return verbatim. Add sparingly — prefer the dict envelope where
# possible so the frontend can pattern-match on a machine-readable code.
ALLOWED_STRING_DETAILS: set[tuple[int, str]] = {
    (415, "Unsupported Media Type"),
}

# All four envelope helpers return the canonical `{"error": code, ...}`
# shape; the test treats `HTTPException(detail=<helper>(...))` as the
# preferred form. `make_error` was missed in the initial sweep
# (commit f0f206b) — adding it here so the routers that wrap envelope-
# bearing branches like `db_busy` / `db_locked` round-trip cleanly.
HELPER_FUNCS = frozenset({"bad_request", "not_found", "validation_failed", "make_error"})


def _iter_router_files() -> list[Path]:
    return sorted(p for p in ROUTERS.rglob("*.py") if p.name != "__init__.py")


def _detail_kw(call: ast.Call) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == "detail":
            return kw.value
    # detail could also be the second positional arg
    if len(call.args) >= 2:
        return call.args[1]
    return None


def _status_code_kw(call: ast.Call) -> int | None:
    for kw in call.keywords:
        if kw.arg == "status_code" and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, int):
                return value
    if call.args and isinstance(call.args[0], ast.Constant):
        value = call.args[0].value
        if isinstance(value, int):
            return value
    return None


def _is_canonical_helper_call(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in HELPER_FUNCS


def _is_error_first_dict(node: ast.expr) -> bool:
    """Accept dict literals that lead with ``"error"`` (canonical shape) OR
    ``"errors"`` (legacy multi-message validation shape that extractApiError
    joins for display). A future cleanup pass will migrate the ``errors``
    sites onto ``validation_failed`` once the frontend's extractApiError is
    taught the canonical ``{error, messages}`` shape from that helper."""
    if not isinstance(node, ast.Dict):
        return False
    if not node.keys:
        return False
    first = node.keys[0]
    return isinstance(first, ast.Constant) and first.value in {"error", "errors"}


def test_http_exception_envelope_shape() -> None:
    findings: list[str] = []

    for path in _iter_router_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name != "HTTPException":
                continue

            detail = _detail_kw(node)
            if detail is None:
                continue

            # Helper calls and {"error": ...} dicts are fine.
            if _is_canonical_helper_call(detail) or _is_error_first_dict(detail):
                continue

            # String constant: check the allowlist.
            if isinstance(detail, ast.Constant) and isinstance(detail.value, str):
                status = _status_code_kw(node)
                if status is not None and (status, detail.value) in ALLOWED_STRING_DETAILS:
                    continue
                findings.append(
                    f"{path.relative_to(ROUTERS.parent.parent)}:{node.lineno} — "
                    f"HTTPException(detail={detail.value!r}); "
                    "use bad_request/not_found/validation_failed or raise_internal"
                )
                continue

            # Anything else (f-strings, name references, complex exprs) is
            # also suspect.
            findings.append(
                f"{path.relative_to(ROUTERS.parent.parent)}:{node.lineno} — "
                f"HTTPException(detail=<{type(detail).__name__}>); "
                "use bad_request/not_found/validation_failed or raise_internal"
            )

    assert not findings, "Non-canonical HTTPException envelopes:\n  " + "\n  ".join(findings)
