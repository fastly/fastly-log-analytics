"""Behavior of scripts/check_no_raw_dateparse.sh (B5/B11 ratchet)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_no_raw_dateparse.sh"


def _run_dateparse(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / "check_no_raw_dateparse.sh")], capture_output=True, text=True)


def _repo_dateparse(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    shutil.copy(SCRIPT, tmp_path / "scripts" / SCRIPT.name)
    shutil.copy(SCRIPT.parent / "lib" / "ratchet_common.sh", tmp_path / "scripts" / "lib" / "ratchet_common.sh")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def test_guard_passes_on_this_repo() -> None:
    res = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


def test_guard_flags_new_raw_site_in_new_file(tmp_path: Path) -> None:
    root = _repo_dateparse(tmp_path, {"backend/repositories/new.py": 'x = datetime.strptime(s, "%Y-%m-%d-%H")\n'})
    res = _run_dateparse(root)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    body = "a = datetime.fromisoformat(x)\nb = datetime.fromisoformat(y)\n"
    root = _repo_dateparse(tmp_path, {"backend/core/clickhouse_rows.py": body})
    assert _run_dateparse(root).returncode == 1


def test_guard_ignores_comments_and_tests(tmp_path: Path) -> None:
    root = _repo_dateparse(
        tmp_path,
        {
            "backend/core/ok.py": "# datetime.fromisoformat(x) was the bug\n",
            "backend/utils/date_utils.py": "datetime.fromisoformat(s)\n",
            "tests/test_x.py": "datetime.strptime(s, f)\n",
        },
    )
    assert _run_dateparse(root).returncode == 0


# ── Merged from test_retry_and_coalesce_guards.py ──

from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
RETRY = "check_no_handrolled_retry.sh"
COALESCE = "check_no_inline_metric_coalesce.sh"


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    (tmp_path / "scripts" / "lib").mkdir(parents=True)
    for name in (RETRY, COALESCE, "check_no_handrolled_retry.py", "lib/ratchet_common.sh"):
        shutil.copy(SCRIPTS / name, tmp_path / "scripts" / name)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    for rel, body in files.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=tmp_path, check=True)
    return tmp_path


def _run(root: Path, script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(root / "scripts" / script)], capture_output=True, text=True)


@pytest.mark.parametrize("script", [RETRY, COALESCE])
def test_guards_pass_on_this_repo(script: str) -> None:
    res = subprocess.run(["bash", str(SCRIPTS / script)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


RETRY_LOOP = """
import time

def f(op):
    for attempt in range(3):
        try:
            return op()
        except OSError:
            time.sleep(0.1)
"""


def test_retry_guard_flags_new_loop(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/new_thing.py": RETRY_LOOP})
    res = _run(root, RETRY)
    assert res.returncode == 1
    assert "backend/core/new_thing.py" in res.stderr


def test_retry_guard_flags_while_loop(tmp_path: Path) -> None:
    body = "import time\n\ndef f(op):\n    while True:\n        try:\n            return op()\n        except Exception:\n            time.sleep(1)\n"
    assert _run(_repo(tmp_path, {"backend/utils/x.py": body}), RETRY).returncode == 1


def test_retry_guard_ignores_sleep_without_handler_and_nested_functions(tmp_path: Path) -> None:
    body = (
        "import time\n\n"
        "def poll():\n    for _ in range(3):\n        time.sleep(1)\n\n"
        "def outer():\n    for _ in range(3):\n"
        "        def inner():\n            try:\n                pass\n            except OSError:\n                time.sleep(1)\n"
    )
    assert _run(_repo(tmp_path, {"backend/utils/ok.py": body}), RETRY).returncode == 0


def test_retry_guard_exempts_retry_module_and_tests(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/retry.py": RETRY_LOOP, "tests/test_x.py": RETRY_LOOP})
    assert _run(root, RETRY).returncode == 0


def test_retry_guard_flags_count_above_baseline(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/utils/valkey_publisher.py": RETRY_LOOP + RETRY_LOOP.replace("def f", "def g")})
    assert _run(root, RETRY).returncode == 1


def test_coalesce_guard_flags_new_inline_site(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/repositories/new.py": 'q = "SELECT COALESCE(SUM(x), 0) FROM t"\n'})
    res = _run(root, COALESCE)
    assert res.returncode == 1
    assert "backend/repositories/new.py" in res.stderr


def test_coalesce_guard_flags_rollup_writer(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"backend/core/rollups/new_rollup.py": 'q = "COALESCE(a, 0)"\n'})
    assert _run(root, COALESCE).returncode == 1


def test_coalesce_guard_ignores_comments_sql_templates_and_helpers(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        {
            "backend/repositories/ok.py": "# COALESCE(x, 0) was inline here\n",
            "backend/repositories/_sql/tpl.py": 'T = "COALESCE(a, 0)"\n',
            "backend/core/metric_expressions.py": 'f"COALESCE({a}, {b})"\n',
        },
    )
    assert _run(root, COALESCE).returncode == 0


# ── Merged from test_http_exception_envelope_shape.py ──

import ast
from pathlib import Path

ROUTERS = Path(__file__).resolve().parent.parent / "backend" / "routers"

# Explicit allowlist for HTTP-standard reason-phrase strings that callers may
# want to return verbatim. Add sparingly — prefer the dict envelope where
# possible so the frontend can pattern-match on a machine-readable code.
ALLOWED_STRING_DETAILS: set[tuple[int, str]] = {
    (415, "Unsupported Media Type"),
    (403, "Read-only services cannot run log discovery."),
    (403, "Read-only services cannot run full sweep."),
    (403, "Read-only services cannot run gap heal."),
    (403, "Read-only services cannot run cloud maintenance."),
    (403, "Read-only services cannot run ledger sweep."),
    (400, "Ledger sweep is only supported under high-scale mode."),
    (403, "Read-only services cannot run RUM sync."),
    (400, "RUM sync is only supported in standard deployment mode."),
    (403, "Read-only services cannot run RUM discovery."),
    (400, "RUM discovery is only supported under high-scale mode."),
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
