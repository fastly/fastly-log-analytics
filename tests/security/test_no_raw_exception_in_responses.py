"""Raw exception text must not be interpolated into HTTP error bodies.

``str(e)`` / ``{e}`` from DuckDB, boto3, Fastly or the OS can carry SQL excerpts, absolute paths, upstream
response bodies and URLs. Router/dependency/middleware code must return a static message plus a correlation
id (``raise_internal`` / ``make_error(..., error_id=...)``) or wrap the text in ``redact_error_text``.

An entry in ``ALLOWED`` is an explicit, reviewed exception: the text is authored validation output (not
infrastructure text) or the route is admin-only. New code that needs one must add a reason here.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.security_regression

ROOT = Path(__file__).resolve().parents[2]
SCAN = [
    ROOT / "backend" / "routers",
    ROOT / "backend" / "utils",
    ROOT / "backend" / "deps.py",
    ROOT / "backend" / "main.py",
]

# Calls that build an HTTP error/response body.
_SINKS = {"HTTPException", "JSONResponse", "make_error", "with_telemetry"}
# Wrappers that make an interpolated exception safe.
_SAFE_WRAPPERS = {"redact_error_text", "_redact_paths"}
_EXC_PARAMS = {"e", "exc", "err", "error", "exception"}

PARSE = "validation: parse_period ValueError, authored text, admin-only provisioning"
SHARE = "validation: share_db authored ValueError/typed errors, admin-only share management"
ALLOWED: dict[tuple[str, str], str] = {
    ("backend/routers/provision.py", "provision_execute"): PARSE,
    ("backend/routers/provision.py", "provision_ingest"): PARSE,
    (
        "backend/routers/session_scoring.py",
        "scoring_labels_create",
    ): "validation: labels.ALLOWED_LABELS ValueError, authored",
    (
        "backend/routers/session_scoring.py",
        "scoring_labels_update",
    ): "validation: labels.ALLOWED_LABELS ValueError, authored",
    (
        "backend/main.py",
        "_invalid_service_id_handler",
    ): "validation: 422 body echoing the authored invalid-service-id ValueError",
    ("backend/routers/share_admin.py", "share_start"): SHARE,
    ("backend/routers/share_admin.py", "create_invite"): SHARE,
    ("backend/routers/share_admin.py", "update_invite_pii"): SHARE,
    ("backend/routers/share_admin.py", "update_invite_passcode"): SHARE,
    ("backend/routers/share_admin.py", "backup_import"): SHARE,
    ("backend/routers/share_admin.py", "gdpr_erase"): SHARE,
}


def _shallow(fn: ast.AST):
    """Walk ``fn`` without descending into nested function bodies (those are visited on their own)."""
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        n = stack.pop()
        yield n
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            stack.extend(ast.iter_child_nodes(n))


def _leaks(tree: ast.AST) -> list[tuple[str, int]]:
    """Return (enclosing function name, lineno) for each exception interpolated into a sink call."""
    out: list[tuple[str, int]] = []

    def exc_use(node: ast.AST, names: set[str]) -> bool:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _SAFE_WRAPPERS:
            return False
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"str", "repr"}
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id in names
        ):
            return True
        if isinstance(node, ast.FormattedValue) and isinstance(node.value, ast.Name) and node.value.id in names:
            return True
        return any(exc_use(c, names) for c in ast.iter_child_nodes(node))

    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        names = {n.name for n in _shallow(fn) if isinstance(n, ast.ExceptHandler) and n.name}
        names |= {a.arg for a in fn.args.args if a.arg in _EXC_PARAMS}
        if not names:
            continue
        for n in _shallow(fn):
            if isinstance(n, ast.Call):
                f = n.func
                name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None
                if name in _SINKS and any(exc_use(a, names) for a in [*n.args, *[k.value for k in n.keywords]]):
                    out.append((fn.name, n.lineno))
    return sorted(set(out), key=lambda t: t[1])


def _files() -> list[Path]:
    files: list[Path] = []
    for p in SCAN:
        files.extend(sorted(p.rglob("*.py")) if p.is_dir() else [p])
    return files


def _violations() -> list[str]:
    bad = []
    for path in _files():
        rel = path.relative_to(ROOT).as_posix()
        for func, line in _leaks(ast.parse(path.read_text())):
            if (rel, func) not in ALLOWED:
                bad.append(f"{rel}:{line} in {func}()")
    return bad


def test_no_raw_exception_text_in_http_error_bodies():
    assert _violations() == [], (
        "raw exception text in an HTTP error body; use raise_internal()/make_error(error_id=...) or "
        "redact_error_text(), or add a reasoned entry to ALLOWED"
    )


def test_allowlist_has_no_stale_entries():
    live = set()
    for path in _files():
        rel = path.relative_to(ROOT).as_posix()
        live.update((rel, func) for func, _ in _leaks(ast.parse(path.read_text())))
    assert sorted(set(ALLOWED) - live) == []


def test_detector_flags_the_known_bad_shapes_and_accepts_safe_ones():
    bad = ast.parse(
        "def h():\n"
        "    try:\n        pass\n    except Exception as e:\n"
        "        raise HTTPException(status_code=500, detail={'error': f'x {e}'})\n"
        "def h2():\n"
        "    try:\n        pass\n    except Exception as e:\n"
        "        return JSONResponse(content={'m': str(e)})\n"
    )
    assert len(_leaks(bad)) == 2
    ok = ast.parse(
        "def h():\n"
        "    try:\n        pass\n    except Exception as e:\n"
        "        log(str(e))\n"
        "        raise HTTPException(status_code=500, detail=make_error('c', redact_error_text(str(e))))\n"
    )
    assert _leaks(ok) == []


def test_analyst_reachable_routes_return_no_raw_exception_text():
    """Route-table tripwire: every non-blocked (analyst-reachable) route handler is clean, and any allow-listed
    handler that analysts can reach must be an authored-validation exception."""
    from backend.main import app
    from backend.utils.remote_access import _is_blocked_path

    reachable: dict[str, set[str]] = {}
    for route in app.routes:
        endpoint = getattr(route, "endpoint", None)
        path = getattr(route, "path", None)
        if endpoint is None or path is None:
            continue
        if _is_blocked_path(re.sub(r"\{[^}]+\}", "x1", path)):
            continue
        mod = getattr(endpoint, "__module__", "")
        rel = mod.replace(".", "/") + ".py"
        reachable.setdefault(rel, set()).add(endpoint.__name__)

    assert reachable, "sanity: some routes are analyst-reachable"
    offenders = []
    for rel, funcs in reachable.items():
        path = ROOT / rel
        if not path.exists():
            continue
        for func, line in _leaks(ast.parse(path.read_text())):
            if func in funcs and ALLOWED.get((rel, func), "").split(":")[0] != "validation":
                offenders.append(f"{rel}:{line} in {func}()")
    assert offenders == []
