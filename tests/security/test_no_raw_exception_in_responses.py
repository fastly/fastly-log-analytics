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
    ROOT / "backend" / "core" / "request_context.py",
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
        "backend/routers/session_scoring/scoring_analytics.py",
        "scoring_labels_create",
    ): "validation: labels.ALLOWED_LABELS ValueError, authored",
    (
        "backend/routers/session_scoring/scoring_analytics.py",
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

from backend.core.request_telemetry import _scrub_secrets
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
    from backend.core.storage._core import _lake_ident

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
    import backend.core.storage._core as _ducklake

    with pytest.raises(ValueError, match="invalid ducklake"):
        call(_ducklake)


def test_custom_field_name_regex_rejects_trailing_newline():
    from backend.core.log_fields import VALID_NAME_RE, validate_custom_field

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

    from backend.models.common import CustomField

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
    from backend.provision.declarative.reconciler import FeatureState, LogFieldsConfig

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
    from backend.core.share_db import (
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
    from backend.core.share_db import mask_ip_values

    out = mask_ip_values({"cell": value})["cell"]
    assert "1.2.3.4" not in out
    assert "2001:db8::1" not in out


def test_share_db_validator_patterns_use_fullmatch():
    import inspect

    import backend.core.share_db as validation

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


# ── Merged from tests/security/test_live_rbac_probes.py ──

import datetime
import json
import os
import urllib.error
import urllib.request
from typing import Any

_BASE_URL_ENV = "FLA_PROBE_BASE_URL"
_EMAIL_ENV = "FLA_PROBE_EMAIL"
_PASSCODE_ENV = "FLA_PROBE_PASSCODE"

_STABLE_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _env(name: str) -> str | None:
    v = os.getenv(name)
    return v if v else None


@pytest.fixture(scope="session")
def probe_base_url() -> str:
    url = _env(_BASE_URL_ENV)
    if not url:
        pytest.skip(
            f"{_BASE_URL_ENV} not set; skipping live RBAC probes. "
            "Set FLA_PROBE_BASE_URL / FLA_PROBE_EMAIL / FLA_PROBE_PASSCODE to run."
        )
    return url.rstrip("/")


@pytest.fixture(scope="session")
def probe_creds() -> tuple[str, str]:
    email = _env(_EMAIL_ENV)
    passcode = _env(_PASSCODE_ENV)
    if not email or not passcode:
        pytest.skip(f"{_EMAIL_ENV} / {_PASSCODE_ENV} not set; skipping live RBAC probes.")
    return email, passcode


@pytest.fixture(scope="session")
def probe_session(probe_base_url: str, probe_creds: tuple[str, str]) -> dict[str, Any]:
    email, passcode = probe_creds
    req = urllib.request.Request(
        f"{probe_base_url}/api/share/login",
        method="POST",
        data=json.dumps({"email": email, "passcode": passcode}).encode(),
        headers={
            "Content-Type": "application/json",
            "Origin": probe_base_url,
            "Referer": f"{probe_base_url}/share-login",
            "X-Remote-Analyst": "1",
            "User-Agent": _STABLE_UA,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            cookies = resp.headers.get_all("Set-Cookie") or []
            body = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        pytest.skip(
            f"share-login probe failed ({exc.code}); cannot run live RBAC probes. "
            "Verify FLA_PROBE_EMAIL / FLA_PROBE_PASSCODE against {probe_base_url}/api/share/login."
        )
    sid_cookie = next(
        (c.split(";")[0].split("=", 1)[1] for c in cookies if c.startswith("analyst_session_id=")),
        None,
    )
    if not sid_cookie:
        pytest.skip("share-login returned no analyst_session_id cookie; cannot run probes.")
    service_ids = body.get("service_ids") or []
    if not service_ids:
        pytest.skip("share-login returned no service_ids; analyst has no service access.")
    return {
        "cookie": f"analyst_session_id={sid_cookie}",
        "service_id": service_ids[0],
        "base_url": probe_base_url,
    }


def analyst_request(
    session: dict[str, Any],
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 60.0,
) -> tuple[int, bytes]:
    headers = {
        "Cookie": session["cookie"],
        "Origin": session["base_url"],
        "Referer": f"{session['base_url']}/dashboard",
        "User-Agent": _STABLE_UA,
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        session["base_url"] + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read() if hasattr(exc, "read") else b""
        return exc.code, raw


def test_query_response_does_not_leak_debug_queries(probe_session):
    status, body = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SELECT 1 AS x"},
    )
    assert status == 200, f"SELECT 1 should succeed for analyst, got {status}: {body[:200]!r}"
    resp = json.loads(body)
    for key in ("debug_queries", "_debug_queries", "debug_calls", "_debug_calls", "debug_sqlite", "_debug_sqlite"):
        assert key not in resp, f"{key!r} present in /api/query response — strip helper missed it"


def test_dashboard_bundle_does_not_leak_debug_queries(probe_session):
    now = datetime.datetime.now(datetime.UTC)
    body = {
        "service_id": probe_session["service_id"],
        "start_time": (now - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "end_time": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "metric": "requests",
        "interval": "minute",
    }
    status, raw = analyst_request(probe_session, "POST", "/api/dashboard/bundle", body)
    assert status == 200, f"/api/dashboard/bundle failed for analyst: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    for key in ("debug_queries", "_debug_queries", "debug_calls", "_debug_calls", "debug_sqlite", "_debug_sqlite"):
        assert key not in resp, f"{key!r} present in /api/dashboard/bundle response"


_LABEL_PII_FIELDS = ("notes", "flagged_by", "sample_ip", "sample_ua", "sample_url")


def test_scoring_labels_strips_pii_for_analyst(probe_session):
    status, raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/labels",
    )
    if status == 404:
        pytest.skip("no scoring labels configured on this service — nothing to probe")
    assert status == 200, f"/scoring/labels failed: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    labels = resp.get("labels", [])
    if not labels:
        pytest.skip("no scoring labels exist on this service — nothing to probe")
    for row in labels:
        for pii_field in _LABEL_PII_FIELDS:
            assert pii_field not in row, f"PII field {pii_field!r} present in analyst /scoring/labels response: {row!r}"


def test_scoring_analytics_composite_omits_evaluation_for_analyst(probe_session):
    status, raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/analytics",
    )
    assert status == 200, f"/scoring/analytics failed: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    assert "evaluation" not in resp, "composite leaked 'evaluation' to analyst"
    assert "evaluation_per_reason" not in resp, "composite leaked 'evaluation_per_reason' to analyst"
    for safe_key in ("top_flagged", "score_distribution", "compliance_breakdown", "health"):
        assert safe_key in resp, f"analyst-safe composite key {safe_key!r} missing"


def test_scoring_evaluation_per_reason_direct_is_403(probe_session):
    status, _raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/evaluation/per-reason",
    )
    assert status == 403, f"direct /evaluation/per-reason returned {status}, expected 403"


def test_dashboard_aggregates_400s_on_empty_window(probe_session):
    now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/dashboard/aggregates",
        {
            "service_id": probe_session["service_id"],
            "start_time": now,
            "end_time": now,
            "metric": "requests",
            "interval": "minute",
        },
    )
    assert status == 400, f"empty-window request should 400, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    detail = body.get("detail") or {}
    assert detail.get("time_range_empty") is True, f"missing time_range_empty marker in 400 body: {body!r}"


def test_dashboard_aggregates_400s_on_inverted_window(probe_session):
    now = datetime.datetime.now(datetime.UTC)
    later = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    earlier = (now - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/dashboard/aggregates",
        {
            "service_id": probe_session["service_id"],
            "start_time": later,
            "end_time": earlier,
            "metric": "requests",
            "interval": "minute",
        },
    )
    assert status == 400, f"inverted-window should 400, got {status}: {raw[:200]!r}"


def test_query_blocks_show_tables(probe_session):
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SHOW TABLES"},
    )
    assert status == 403, f"SHOW TABLES should 403, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    assert (
        "SHOW" in (body.get("detail", {}).get("error") or "").upper()
        or "DESCRIBE" in (body.get("detail", {}).get("error") or "").upper()
    )


def test_query_blocks_describe(probe_session):
    status, _raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "DESCRIBE logs"},
    )
    assert status == 403


def test_query_blocks_foreign_table_select(probe_session):
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {
            "service_id": probe_session["service_id"],
            "mode": "raw",
            "sql": "SELECT COUNT(*) FROM logs_some_other_service_that_does_not_exist",
        },
    )
    assert status == 403, f"foreign-table SELECT should 403, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    error_str = (body.get("detail") or {}).get("error", "")
    assert "not in the allowed set" in error_str, f"expected allowlist rejection, got: {error_str!r}"


def test_query_allows_canonical_logs(probe_session):
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SELECT COUNT(*) AS n FROM logs"},
    )
    assert status == 200, f"canonical /api/query failed post-fix: {status} {raw[:200]!r}"
    body = json.loads(raw)
    assert "data" in body and len(body["data"]) == 1
    assert "n" in body["data"][0]


def test_query_allows_recursive_cte(probe_session):
    sql = "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<5) SELECT * FROM x"
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": sql},
    )
    assert status == 200, f"recursive CTE should pass post-fix: {status} {raw[:200]!r}"


@pytest.mark.parametrize(
    "path",
    [
        "/api/admin/services",
        "/api/admin/ingest",
        "/api/admin/share/invites",
        "/api/cron-runs",
        "/api/audit-logs",
        "/api/alerts",
        "/api/usage",
        "/api/provision/services",
        "/api/debug/state",
    ],
)
def test_admin_paths_blocked_for_analyst(probe_session, path):
    status, raw = analyst_request(probe_session, "GET", path)
    assert status == 403, f"{path} should 403 for analyst, got {status}: {raw[:200]!r}"


# ── Merged from tests/security/test_adversarial_ast_pii_redaction.py ──

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.share_admin as share_auth
from backend.core import share_db
from backend.repositories.query import execute_query
from backend.routers import share_admin
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

RAW_IP = "198.51.100.42"
RAW_SESSION = "deadbeefcafe1234567890abcdef"
RAW_URL = "/api/v1/sensitive-resource"
SOURCE_NAME = "default"
TIME_FILTER = ("2026-09-19T00:00:00Z", "2026-09-19T23:59:59Z")


@pytest.fixture
def test_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE logs (
            timestamp TIMESTAMPTZ,
            ip VARCHAR,
            client_ip VARCHAR,
            cookie_session VARCHAR,
            status INTEGER,
            url VARCHAR
        );
        INSERT INTO logs VALUES (
            '2026-09-19 12:00:00Z'::TIMESTAMPTZ,
            '198.51.100.42',
            '198.51.100.42',
            'deadbeefcafe1234567890abcdef',
            200,
            '/api/v1/sensitive-resource'
        );
        """
    )
    yield con
    con.close()


def test_admin_and_pii_analyst_sees_unmasked_pii(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT ip, client_ip, cookie_session, url FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=False,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == RAW_IP
    assert rows[0]["client_ip"] == RAW_IP
    assert rows[0]["cookie_session"] == RAW_SESSION
    assert rows[0]["url"] == RAW_URL


def test_no_pii_analyst_direct_projection_redacted(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT ip, client_ip, cookie_session, url FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == "[redacted]"
    assert rows[0]["client_ip"] == "[redacted]"
    assert rows[0]["cookie_session"] == "[redacted]"
    assert rows[0]["url"] == RAW_URL


def test_adversarial_ast_string_concatenation(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT 'prefix_' || ip || '_suffix' AS exfil FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["exfil"] == "prefix_[redacted]_suffix"
    assert RAW_IP not in rows[0]["exfil"]


def test_adversarial_ast_type_cast(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT CAST(ip AS VARCHAR) AS cast_ip, cookie_session::TEXT AS cast_sess FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["cast_ip"] == "[redacted]"
    assert rows[0]["cast_sess"] == "[redacted]"


def test_adversarial_ast_substring_slicing(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT SUBSTR(ip, 1, 4) AS sub_ip FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["sub_ip"] == "[red"
    assert RAW_IP[:4] not in rows[0]["sub_ip"]


def test_adversarial_ast_cte_wrapper(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = """
    WITH attacker_cte AS (
        SELECT ip AS aliased_ip, cookie_session AS aliased_sess FROM logs
    )
    SELECT aliased_ip, aliased_sess FROM attacker_cte
    """
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["aliased_ip"] == "[redacted]"
    assert rows[0]["aliased_sess"] == "[redacted]"


def test_adversarial_ast_subquery_wrapper(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT sub.x, sub.y FROM (SELECT ip AS x, cookie_session AS y FROM logs) AS sub"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["x"] == "[redacted]"
    assert rows[0]["y"] == "[redacted]"


def test_adversarial_ast_conditional_oracle(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = f"""
    SELECT CASE
        WHEN ip = '{RAW_IP}' THEN 'leaked_raw_ip'
        ELSE 'protected_redacted'
    END AS oracle_probe FROM logs
    """
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["oracle_probe"] == "protected_redacted"


def test_adversarial_ast_where_filter(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = f"SELECT url FROM logs WHERE ip = '{RAW_IP}'"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 0


def test_adversarial_ast_group_by_aggregation(test_db: duckdb.DuckDBPyConnection) -> None:
    sql = "SELECT ip, COUNT(*) AS count_rows FROM logs GROUP BY ip"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == "[redacted]"
    assert rows[0]["count_rows"] == 1


@pytest.fixture
def rbac_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/views")
    def _create_view():
        return {"ok": True}

    @app.post("/api/dashboard/aggregates")
    def _aggregates():
        return {"ok": True}

    return app


def test_analyst_rbac_admin_routes_blocked(rbac_app: FastAPI) -> None:
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Admin Test",
        email="rbac-admin-test@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-admin-test@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    r_admin = client.get(
        "/api/admin/share/status",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_admin.status_code == 403
    assert r_admin.json()["error"] == "admin_only"


def test_analyst_rbac_mutation_routes_blocked(rbac_app: FastAPI) -> None:
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Write Test",
        email="rbac-write-test@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-write-test@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    r_mut = client.post(
        "/api/views",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_mut.status_code == 403
    assert r_mut.json()["error"] == "read_only"


def test_analyst_pii_policy_filter_locking(rbac_app: FastAPI) -> None:
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Filter Lock Test",
        email="rbac-filter-lock@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-filter-lock@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    r_filt = client.post(
        "/api/dashboard/aggregates?service=svcA",
        json={"filters": {"ip": "198.51.100.42"}},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_filt.status_code == 403
    assert r_filt.json()["error"] == "pii_policy_violation"
    assert r_filt.json()["field"] == "ip"

    r_ok = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_ok.status_code == 200
    assert r_ok.json()["ok"] is True


# ── Merged from tests/security/test_no_infra_leak_in_tracked_tree.py ──

import base64
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# base64 of deployment-private tokens that must not appear in the tracked tree.
# Stored encoded so this guard file itself contains no plaintext leak. Each was
# confirmed real by matching the operator's gitignored production service config
# (the producer), not guessed. Scan is case-insensitive so the lowercased
# ``logs_<id>`` table-name form is caught too.
#   S0xKUFV0SmtDMVpsTFZjalBHVjFqNQ== -> production logging service ID
#   ZnBSZnlrdTQyNThxdGlmaW9UY2dVbw== -> production CDN service ID
#   ZHJld19jb3JwLnRlc3Q=             -> production NGWAF workspace ID
# Additional encoded entries cover deployment service IDs, project/namespace
# names, an origin address, and a deployment-specific analyst hostname found
# during the beta3 public-release scrub.
_NEEDLES_B64 = (
    "S0xKUFV0SmtDMVpsTFZjalBHVjFqNQ==",
    "ZnBSZnlrdTQyNThxdGlmaW9UY2dVbw==",
    "ZHJld19jb3JwLnRlc3Q=",
    "WlUxNUJ2WTJMWDdXY0VwNDNUOVZ3VQ==",
    "cUk0RDh5WFhGWU9JcFpFTXJrSnk2NQ==",
    "Y1ZudTltWUIzQ3Ztb2IzbHNxalFVMw==",
    "cm1XekNSQTBsa0FPczlHbnh2b2hzNA==",
    "TXZpQ0w5UGQ1dnhCS1lTeFhFaFFSMQ==",
    "ZUhEdDM3UUdTRWZpaFpPdVhKT1JFZQ==",
    "c2UtZGV2ZWxvcG1lbnQtOTU2Ng==",
    "MzQuMTIzLjMwLjE5NQ==",
    "c2UtZGVtbw==",
    "ZmxhLWVsZXZhdGlvbi1hbmFseXN0Lmdsb2JhbC5zc2wuZmFzdGx5Lm5ldA==",
    "WkVaNG1jQWpvU0ZEVGc3dHBrREtWMg==",
    "emdKUWRNYXFWbGVUMlZEZTJFTENFTQ==",
    "ZmFzdGx5LXNlLWRlbW8uZ2xvYmFsLnNzbC5mYXN0bHkubmV0",
    "ZmxhLWs4cy1zY2FsaW5nLXRlc3QtZWxldmF0aW9uLmdsb2JhbC5zc2wuZmFzdGx5Lm5ldA==",
    "ZmxhLWxvY2FsLWhpZ2gtc2NhbGUtdGVzdC5nbG9iYWwuc3NsLmZhc3RseS5uZXQ=",
    "ZmxhLWxvY2FsLXN0YW5kYXJkLXRlc3QuZ2xvYmFsLnNzbC5mYXN0bHkubmV0",
)

# Suffixes that are binary / generated / lock files where a coincidental match
# is noise rather than a human-authored leak.
_SKIP_SUFFIXES = {
    ".lock",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".pdf",
    ".zip",
    ".gz",
    ".duckdb",
    ".db",
    ".wasm",
}


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [_REPO_ROOT / p for p in out.split("\0") if p]


def test_no_production_service_id_in_tracked_tree() -> None:
    needles = [base64.b64decode(b).decode().lower() for b in _NEEDLES_B64]
    self_path = Path(__file__).resolve()

    offenders: list[str] = []
    for path in _tracked_files():
        if path.resolve() == self_path:
            continue  # the guard stores only the base64 form
        if path.suffix.lower() in _SKIP_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except (OSError, UnicodeError):
            continue
        for needle in needles:
            if needle in text:
                rel = path.relative_to(_REPO_ROOT)
                offenders.append(f"{rel} (contains a deployment-private token)")
                break

    assert not offenders, (
        "Deployment-private token(s) found in the tracked, world-readable tree.\n"
        "Scrub these before merging to main (and rotate/retire the pushed branch "
        "ref to un-publish them):\n  " + "\n  ".join(sorted(offenders))
    )


def _configured_oauth_secrets() -> list[str]:
    """Derive OAuth client_id / client_secret / flow-state secret from the
    gitignored provider registry + env (the PRODUCER), so this guard never
    hardcodes a secret (per the ``leak-sweep-producer-derived`` pattern). Returns
    lowercased needles; empty when OAuth is not configured in this environment.
    """
    out: list[str] = []
    try:
        import backend.core.oauth as registry

        registry.reset_cache_for_tests()
        for p in registry.get_all_providers():
            out.extend(v for v in (p.client_id, p.client_secret) if v)
        fss = registry.flow_state_secret()
        if fss:
            out.append(fss)
    except Exception:
        return []
    # Ignore trivially-short values that would produce noisy false positives.
    return [v.lower() for v in out if len(v) >= 8]


def test_no_oauth_client_credentials_in_tracked_tree() -> None:
    """When OAuth is configured (gitignored registry + env), the real client_id /
    client_secret / OAUTH_FLOW_STATE_SECRET must never appear in the tracked
    tree. Dormant (skipped) where OAuth isn't configured, e.g. CI."""
    needles = _configured_oauth_secrets()
    if not needles:
        pytest.skip("OAuth not configured in this environment — no credentials to scan for")

    self_path = Path(__file__).resolve()
    offenders: list[str] = []
    for path in _tracked_files():
        if path.resolve() == self_path:
            continue
        if path.suffix.lower() in _SKIP_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except (OSError, UnicodeError):
            continue
        for needle in needles:
            if needle in text:
                offenders.append(f"{path.relative_to(_REPO_ROOT)} (contains an OAuth credential)")
                break

    assert not offenders, (
        "OAuth client credential(s) found in the tracked, world-readable tree.\n"
        "Move them to the gitignored registry / env and rotate them:\n  " + "\n  ".join(sorted(offenders))
    )
