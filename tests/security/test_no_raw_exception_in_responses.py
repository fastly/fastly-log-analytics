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


# ── Merged from test_error_redaction.py ──

from unittest.mock import patch

import duckdb
import pytest

from backend.core.query_registry import _scrub_secrets
from backend.repositories import query as q
from backend.utils.router_utils import redact_error_text, register_salt
from tests.conftest import MOCK_SERVICE_ID

SALT = "0123456789abcdefSALTVALUE-zz"
SVC = "svc"
WINDOW = ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z")


@pytest.fixture(autouse=True)
def _known_salt():
    register_salt(SALT)


@pytest.mark.parametrize(
    "text",
    [
        f"Binder Error near sha256(CONCAT('{SALT}', cid))",
        f'Binder Error near "sha256"("CONCAT"(\'{SALT}\', cid))',
        f"Conversion Error: '{SALT}' || cid",
        f"concat_ws('', '{SALT}', cid) failed",
        f"first {SALT} and again {SALT} and sha256(CONCAT('{SALT}', x))",
    ],
)
def test_salt_is_redacted_by_value_in_every_embedding(text):
    assert SALT not in redact_error_text(text)
    assert SALT not in _scrub_secrets(text)


def test_unknown_literal_form_still_caught_by_regex_fallback():
    out = redact_error_text("x sha256(CONCAT('someOtherSalt123', cid))")
    assert "someOtherSalt123" not in out


def test_paths_and_salt_both_removed():
    out = redact_error_text(f"IO Error: '/app/data/services/x/y.duckdb' salt={SALT}")
    assert "/app/data" not in out and SALT not in out


@pytest.mark.parametrize(
    "analyst_sql",
    [
        "SELECT CAST(cid AS INTEGER) FROM logs",
        "SELECT nonexistent_col FROM logs",
        "SELECT cid::DATE FROM logs",
        "SELECT * FROM logs WHERE cid = (SELECT 1/0)",
        "SELECT read_text('/etc/passwd')",
    ],
)
def test_crafted_analyst_query_errors_leak_neither_salt_nor_paths(analyst_sql):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR)")
    con.execute("INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4')")
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(Exception) as exc:
            q.execute_query(
                con,
                {"name": SVC, "service_id": SVC},
                analyst_sql,
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
    con.close()
    shown = redact_error_text(exc.value)
    assert SALT not in str(exc.value)
    assert SALT not in shown
    assert "/etc/" not in shown and "/app/" not in shown


@pytest.mark.parametrize("exc_cls", [PermissionError, RuntimeError])
def test_query_route_error_bodies_are_scrubbed(client, exc_cls):
    msg = f"boom '/srv/data/x.parquet' sha256(CONCAT('{SALT}', cid)) {SALT}"
    with patch("backend.repositories.query.execute_query", side_effect=exc_cls(msg)):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code in (400, 403)
    assert SALT not in resp.text
    assert "/srv/" not in resp.text


# ── Merged from test_no_production_asserts.py ──

from pathlib import Path

import pytest

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
    assert "MANAGED_DICTIONARY_NAMES" in src
    tree = ast.parse(src)
    guards = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and any(
            isinstance(sub, ast.Compare) and any(isinstance(op, ast.NotIn) for op in sub.ops)
            for sub in ast.walk(n.test)
        )
        and any(isinstance(b, ast.Raise) for b in n.body)
    ]
    assert len(guards) >= 1


# ── Merged from test_no_silent_exception_swallow.py ──

from pathlib import Path

import pytest

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


# ── Merged from tests/security/test_identifier_trailing_newline.py ──

NL = "\n"


def test_config_service_id_rejects_trailing_newline():
    from backend.config import _validate_service_id

    assert _validate_service_id("svc-1") == "svc-1"
    with pytest.raises(ValueError):
        _validate_service_id("svc-1" + NL)


def test_config_paths_refuse_newline_service_id():
    from backend.config import config_path, duckdb_path

    for fn in (config_path, duckdb_path):
        with pytest.raises(ValueError):
            fn("svc" + NL)


def test_metadata_service_id_rejects_trailing_newline():
    from backend.core.metadata.base import InvalidServiceIdError, _validate_service_id_or_raise

    _validate_service_id_or_raise("svc_1")
    with pytest.raises(InvalidServiceIdError):
        _validate_service_id_or_raise("svc_1" + NL)


def test_rollups_safe_ident_rejects_trailing_newline():
    from backend.core.rollups._common import _is_safe_ident

    assert _is_safe_ident("logs_svc")
    assert not _is_safe_ident("logs_svc" + NL)


def test_lake_ident_rejects_trailing_newline():
    from backend.core.iceberg.buffer import _lake_ident

    assert _lake_ident("client_vitals") == "client_vitals"
    with pytest.raises(ValueError):
        _lake_ident("client_vitals" + NL)


@pytest.mark.parametrize(
    "call",
    [
        lambda m: m._apply_target_file_size(None, "lake" + NL),
        lambda m: m.ducklake_current_snapshot_id(None, "lake" + NL),
    ],
)
def test_ducklake_idents_reject_trailing_newline(call):
    from backend.core.iceberg import _ducklake

    with pytest.raises(ValueError, match="invalid ducklake"):
        call(_ducklake)


def test_custom_field_name_regex_rejects_trailing_newline():
    from backend.core.field_registry import VALID_NAME_RE
    from backend.core.log_fields import validate_custom_field

    field = {
        "name": "my_field" + NL,
        "label": "L",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    errors = validate_custom_field(field, [])
    assert any("lowercase alphanumeric" in e for e in errors)
    assert VALID_NAME_RE.fullmatch("my_field")


def test_custom_field_model_rejects_trailing_newline():
    from pydantic import ValidationError

    from backend.models.services import CustomField

    body_cls = CustomField
    base = {
        "label": "L",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    body_cls(name="my_field", **base)
    with pytest.raises(ValidationError):
        body_cls(name="my_field" + NL, **base)


def test_declarative_feature_state_rejects_newline_custom_field_name():
    from backend.provision.declarative.state import FeatureState, LogFieldsConfig

    base = {
        "service_id": "svc",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "custom_condition": "",
        "fos_prefix": "",
        "fos_endpoint": "",
    }
    FeatureState(**base, log_fields=LogFieldsConfig(custom_fields=[{"name": "ok_name"}]))
    with pytest.raises(ValueError, match="Custom field name"):
        FeatureState(**base, log_fields=LogFieldsConfig(custom_fields=[{"name": "ok_name" + NL}]))


def test_provision_check_domain_rejects_trailing_newline_in_prefix():
    from backend.routers.provision import provision_check_domain

    result = provision_check_domain(prefix="my-prefix" + NL, is_custom=False)
    assert result["available"] is False
    assert "alphanumeric" in result["reason"]


def test_provision_router_has_no_bare_re_match():
    import inspect

    from backend.routers import provision

    assert "re.match(" not in inspect.getsource(provision)


def test_share_db_name_and_email_validators_hold_with_whitespace():
    from backend.core.share_db.validation import (
        InvalidEmailError,
        InvalidNameError,
        validate_email,
        validate_name,
    )

    assert validate_name("  Bob\n") == "Bob"  # surrounding whitespace is stripped, not an injection vector
    for bad in ("Bo\nb", "Bo\rb", "Bob\n\nx", "Bo\tb"):
        with pytest.raises(InvalidNameError):
            validate_name(bad)
    assert validate_email(" A@B.co\n") == "a@b.co"
    for bad in ("a@b.co\nx", "a@b.co\r\nBcc: x@y.z", "a@b.c\u2028o", "a b@c.co"):
        with pytest.raises(InvalidEmailError):
            validate_email(bad)


@pytest.mark.parametrize("value", ["1.2.3.4\n", " 1.2.3.4", "1.2.3.4\r\n", "\t2001:db8::1 \n"])
def test_mask_ip_values_does_not_pass_ips_wrapped_in_whitespace(value):
    from backend.core.share_db.validation import mask_ip_values

    out = mask_ip_values({"cell": value})["cell"]
    assert "1.2.3.4" not in out
    assert "2001:db8::1" not in out


def test_share_db_validator_patterns_use_fullmatch():
    import inspect

    from backend.core.share_db import validation

    src = inspect.getsource(validation)
    for name in ("_NAME_RE", "_EMAIL_RE", "_MAYBE_IP_RE"):
        assert f"{name}.match(" not in src
        assert f"{name}.fullmatch(" in src


def test_no_dollar_anchored_validator_uses_bare_match():
    """``X.match(...)`` / ``re.match(...)`` on a ``^...$`` pattern accepts a trailing newline."""
    root = Path(__file__).resolve().parents[2] / "backend"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        anchored: set[str] = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "compile"
                and node.value.args
                and isinstance(node.value.args[0], ast.Constant)
                and isinstance(node.value.args[0].value, str)
                and node.value.args[0].value.endswith("$")
            ):
                anchored.update(t.id for t in node.targets if isinstance(t, ast.Name))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "match"):
                continue
            recv = node.func.value
            if isinstance(recv, ast.Name) and recv.id in anchored:
                offenders.append(f"{path}:{node.lineno}")
            elif (
                isinstance(recv, ast.Name)
                and recv.id == "re"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and node.args[0].value.endswith("$")
            ):
                offenders.append(f"{path}:{node.lineno}")
    assert offenders == []
