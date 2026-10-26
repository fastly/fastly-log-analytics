from __future__ import annotations

import inspect
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import backend.provision.declarative.generators as gen
import backend.routers.services.core as sc
import backend.utils.vcl_safety as vs
import pytest
from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.core.field_registry import BY_CODE, BY_GROUP, REGISTRY, LogField
from backend.provision.cmcd_fields import (
    _CMCD_CUSTOM_FIELDS,
    _CMCD_FIELD_NAMES,
    get_cmcd_fields,
    reconcile_cmcd_custom_fields,
)
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import CmcdConfig, FeatureState, LogFieldsConfig
from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX, recv_snippet, resolve_exclude_url_regex
from backend.provision.utils import (
    ANALYTICS_LOG_LAYOUT,
    VclBuilder,
    analytics_log_path,
    draft_version,
    minute_list_prefix,
    rollback_to_version,
    rum_log_path,
    rum_minute_list_prefix,
)
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.vcl_safety import LintResult, VclSafetyError, validate_log_format_macro, validate_quoted_literal
from backend.utils.vcl_utils import lint_log_format, log_format_to_vcl_log
from backend.utils.vcl_validator import (
    MAX_REGEX_BYTES,
    RegexValidationError,
    lint_vcl,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st

import backend.provision.fastly_api as fa
from backend import config
from backend.config import path_within_dir
from backend.core import field_registry as fr
from backend.core import log_fields as lf
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    DEFAULT_OFF_FIELD_IDS,
    INSIGHT_DEFINITIONS,
    LOG_FIELD_CATALOG,
    PRESETS,
    generate_log_format,
    get_required_edge_headers,
    resolve_enabled_fields,
    validate_custom_field,
)
from backend.core.rollups._common import quote_path_list
from backend.main import app
from backend.provision import cmcd_fields as orch
from backend.provision import generate_capture_vcl
from backend.provision.fastly_api import (
    SESSION_COOKIE_ALLOWLIST,
    _session_cookie_hash_vcl,
    delete_remote_frontend,
    deploy_remote_frontend,
)
from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS
from backend.utils import sql_validator as sql_ident

FALCO_INSTALLED = shutil.which("falco") is not None
FALCO_REQUIRED = os.environ.get("FALCO_REQUIRED") == "1"

if FALCO_REQUIRED and not FALCO_INSTALLED:
    raise RuntimeError(
        "FALCO_REQUIRED=1 but the falco binary is not on PATH. Install falco or unset FALCO_REQUIRED to allow skipping."
    )

_FASTLY_STUBS = Path(__file__).resolve().parent.parent / "fixtures" / "fastly_stubs.vcl"
FALCO_PATH = shutil.which("falco")
requires_falco = pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")


@pytest.fixture
def run_falco_test():
    template = _FASTLY_STUBS.read_text()

    def _run(cfg: dict, test_assertions: str) -> subprocess.CompletedProcess:
        snippets = generate_capture_vcl(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            main_vcl_path = os.path.join(tmpdir, "main.vcl")
            test_vcl_path = os.path.join(tmpdir, "suite.test.vcl")
            with open(main_vcl_path, "w") as f:
                f.write(
                    template.replace("//<INJECT_FETCH_SNIPPET>", snippets.get("fetch", "")).replace(
                        "//<INJECT_DELIVER_SNIPPET>", snippets.get("deliver", "")
                    )
                )
            with open(test_vcl_path, "w") as f:
                f.write(test_assertions)
            return subprocess.run(["falco", "test", main_vcl_path], cwd=tmpdir, capture_output=True, text=True)

    return _run


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_origin_field_always_log(run_falco_test):
    cfg = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "x_drew",
                "vcl_log_expression": "beresp.http.x-drew",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
            }
        ],
    }
    test_code = """
// @scope: fetch
sub test_fetch_captures {
    set beresp.http.x-drew = "hello";
    testing.call_subroutine("vcl_fetch");
    assert.equal(beresp.http.x-fos-origin-data:x_drew, "hello");
}
// @scope: deliver
sub test_deliver_promotes_and_cleans {
    set resp.http.x-fos-origin-data:x_drew = "hello";
    testing.call_subroutine("vcl_deliver");
    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    assert.is_notset(resp.http.x-fos-origin-data:x_drew);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\n{result.stdout}\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_shield_interaction(run_falco_test):
    cfg = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "x_drew",
                "vcl_log_expression": "beresp.http.x-drew",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
            }
        ],
    }
    test_code = """
// @scope: deliver
sub test_shield_keeps_header_for_edge {
    set resp.http.x-fos-origin-data:x_drew = "hello";
    set req.http.x-is-cluster-fetch = "1";
    testing.call_subroutine("vcl_deliver");
    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    assert.equal(resp.http.x-fos-origin-data:x_drew, "hello");
}
// @scope: deliver
sub test_edge_strips_header {
    set resp.http.x-fos-origin-data:x_drew = "hello";
    unset req.http.x-is-cluster-fetch;
    testing.call_subroutine("vcl_deliver");
    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    assert.is_notset(resp.http.x-fos-origin-data:x_drew);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\n{result.stdout}\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_group_l_telemetry_cleanup(run_falco_test):
    cfg = {"groups": ["L"], "custom_fields": []}
    test_code = """
// @scope: deliver
sub test_telemetry_headers_stripped {
    set resp.http.x-of-ttfb = "100";
    set resp.http.x-of-ttlb = "200";
    set resp.http.x-of-status = "200";
    set resp.http.x-of-oip = "1.2.3.4";
    set resp.http.x-of-oretries = "0";
    unset req.http.x-is-cluster-fetch;
    testing.call_subroutine("vcl_deliver");
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
    assert.is_notset(resp.http.x-of-status);
    assert.is_notset(resp.http.x-of-oip);
    assert.is_notset(resp.http.x-of-oretries);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\n{result.stdout}\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_origin_timing_ttfb_ttlb(run_falco_test):
    cfg = {"groups": ["L"], "custom_fields": []}
    test_code = """
// @scope: fetch
sub test_fetch_records_ttfb {
    declare local var.start INTEGER;
    set var.start = std.atoi(time.elapsed.usec);
    set var.start -= 500;
    set req.http.x-of-start = "" + var.start;
    set beresp.status = 200;
    testing.call_subroutine("vcl_fetch");
    assert.true(std.atoi(req.http.x-of-ttfb) >= 500);
    assert.equal(req.http.x-of-status, "200");
}
// @scope: deliver
sub test_deliver_records_ttlb_and_promotes {
    declare local var.start INTEGER;
    set var.start = std.atoi(time.elapsed.usec);
    set var.start -= 800;
    set req.http.x-of-start = "" + var.start;
    set req.http.x-of-ttfb = "500";
    set req.http.x-of-status = "200";
    set req.http.x-of-ttlb = "";
    set req.http.x-is-cluster-fetch = "";
    testing.call_subroutine("vcl_deliver");
    assert.true(std.atoi(req.http.x-of-ttlb) >= 800);
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\n{result.stdout}\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_shield_to_edge_timing_promotion(run_falco_test):
    cfg = {"groups": ["L"], "custom_fields": []}
    test_code = """
// @scope: deliver
sub test_edge_promotes_metrics_from_shield_resp {
    set resp.http.x-of-ttfb = "500";
    set resp.http.x-of-ttlb = "800";
    set resp.http.x-of-status = "200";
    set req.http.x-of-start = "";
    set req.http.x-of-ttfb = "";
    set req.http.x-of-ttlb = "";
    set req.http.x-of-status = "";
    set req.http.x-is-cluster-fetch = "";
    testing.call_subroutine("vcl_deliver");
    assert.equal(req.http.x-of-ttfb, "500");
    assert.equal(req.http.x-of-ttlb, "800");
    assert.equal(req.http.x-of-status, "200");
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\n{result.stdout}\n{result.stderr}"


def test_deliver_stage_emits_capture_and_log_format_and_coexistence():
    cfg_deliver = {
        "groups": ["A"],
        "custom_fields": [
            {
                "name": "edge_score",
                "vcl_log_expression": "resp.http.X-Edge-Score",
                "collection_stage": "deliver",
                "value_type": "numeric",
                "enabled": True,
            }
        ],
    }
    deliver = generate_capture_vcl(cfg_deliver).get("deliver", "")
    assert "# --- Custom Deliver Fields Start ---" in deliver
    assert 'if (resp.http.X-Edge-Score != "") {' in deliver
    assert "set req.http.x-fos-edge-data:edge_score = resp.http.X-Edge-Score;" in deliver

    cfg_fmt = {
        "groups": ["A"],
        "custom_fields": [
            {
                "name": "edge_score",
                "vcl_log_expression": "req.http.x-edge-score",
                "collection_stage": "deliver",
                "value_type": "numeric",
                "enabled": True,
            },
            {
                "name": "edge_cookie_compliance",
                "vcl_log_expression": "req.http.x-edge-cookie-compliance",
                "collection_stage": "deliver",
                "value_type": "string",
                "enabled": True,
            },
        ],
    }
    fmt = generate_log_format(cfg_fmt)
    assert (
        '"edge_score":%{if(fastly.ff.visits_this_service == 0 && '
        'substr(req.http.x-edge-score, 0, 20) ~ "^-?[0-9]+(\\\\.[0-9]+)?$"'
        ', substr(req.http.x-edge-score, 0, 20), "null")}V'
    ) in fmt
    assert re.search(
        r'"edge_cookie_compliance":"%\{json\.escape\('
        r"if\(fastly\.ff\.visits_this_service == 0, "
        r'substr\(req\.http\.x-edge-cookie-compliance, 0, \d+\), ""\)\)}V"',
        fmt,
    )

    cfg_disabled = {
        "groups": ["A"],
        "custom_fields": [
            {
                "name": "edge_score",
                "vcl_log_expression": "resp.http.X-Edge-Score",
                "collection_stage": "deliver",
                "enabled": False,
            }
        ],
    }
    assert "edge_score" not in generate_capture_vcl(cfg_disabled).get("deliver", "")

    cfg_coexist = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "f_edge",
                "vcl_log_expression": "req.http.X-Custom-Edge",
                "collection_stage": "edge",
                "enabled": True,
            },
            {
                "name": "f_origin",
                "vcl_log_expression": "beresp.http.X-Custom-Origin",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
                "enabled": True,
            },
            {
                "name": "f_deliver",
                "vcl_log_expression": "resp.http.X-Custom-Deliver",
                "collection_stage": "deliver",
                "enabled": True,
            },
        ],
    }
    snippets = generate_capture_vcl(cfg_coexist)
    assert "f_edge" in snippets["recv"] and "f_origin" in snippets["fetch"] and "f_deliver" in snippets["deliver"]
    assert "unset req.http.x-fos-edge-data:f_origin;" in snippets["recv"]
    assert "unset req.http.x-fos-origin-data:f_origin;" in snippets["recv"]
    assert "  set req.http.x-fos-edge-data:f_origin" not in snippets["recv"]
    assert "  set req.http.x-fos-edge-data:f_deliver" not in snippets["fetch"]
    assert "  set beresp.http.x-fos-origin-data:f_edge" not in snippets["fetch"]
    assert "f_edge" not in snippets["deliver"]


def test_ip_capture_and_recv_reset_and_cookie_session_hash():
    snippets = generate_capture_vcl({"groups": ["A"]})
    assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in snippets["recv"]
    assert "set req.http.x-fos-edge-data:ip = client.ip;" in snippets["recv"]
    reset = snippets["recv_reset"]
    assert "unset req.http.Fastly-Client-IP;" in reset
    assert "req.restarts == 0 && fastly.ff.visits_this_service == 0" in reset
    assert "X-Edge-Shield-Auth" not in reset

    assert "recv_reset" not in generate_capture_vcl({"groups": [], "field_overrides": {"ip": False}})

    snippets_h = generate_capture_vcl({"groups": ["H"]})
    assert "x-fos-edge-data:cookie_session" not in snippets_h["recv"]
    assert "digest.hash_sha256" not in snippets_h["recv"]

    snippets_opt_in = generate_capture_vcl({"groups": ["H"], "field_overrides": {"cookie_session": True}})
    assert "set req.http.x-fos-edge-data:cookie_session = " in snippets_opt_in["recv"]
    assert "digest.hash_sha256" in snippets_opt_in["recv"]


def _if_conditions(expr: str) -> list[str]:
    conds, i = [], 0
    while True:
        j = expr.find("if(", i)
        if j == -1:
            break
        depth, k, start = 1, j + 3, j + 3
        while k < len(expr) and depth > 0:
            c = expr[k]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "," and depth == 1:
                break
            k += 1
        conds.append(expr[start:k])
        i = j + 3
    return conds


def test_cookie_session_hash_has_no_if_nested_in_a_condition():
    expr = _session_cookie_hash_vcl()
    for cond in _if_conditions(expr):
        assert "if(" not in cond, f"if() nested in an if()-condition (Fastly rejects): {cond!r}"
    assert 'digest.hash_sha256("")' not in expr
    assert expr.count("digest.hash_sha256") == len(SESSION_COOKIE_ALLOWLIST)
    first, last = SESSION_COOKIE_ALLOWLIST[0], SESSION_COOKIE_ALLOWLIST[-1]
    assert expr.index(f'"{first}"') < expr.index(f'"{last}"')


# ── Merged from tests/utils/test_vcl_utils.py & tests/provision/test_vcl_builder.py ──


def test_log_format_to_vcl_log_variants():
    assert log_format_to_vcl_log("plain text no braces") == '{"plain text no braces"}'
    out1 = log_format_to_vcl_log('{"ts": %{time.start}V}')
    assert "time.start" in out1 and " + " in out1
    out2 = log_format_to_vcl_log('{"ip":"%{client.ip}V","ua":"%{req.http.User-Agent}V"}')
    assert "client.ip" in out2 and "req.http.User-Agent" in out2 and " + " in out2
    out3 = log_format_to_vcl_log('\\{"raw":"x"\\}')
    assert "\\{" not in out3 and "\\}" not in out3
    assert '"} + {"' in log_format_to_vcl_log('{"a":"b"}{"c":"d"}')


@pytest.mark.parametrize(
    ("fmt", "substr"),
    [
        ("", "empty"),
        ('{"timestamp":%{time.start}V}\n', "newline"),
        ('"just a string"', ""),
        ('{"ip":"%{client.ip}V"}', "timestamp"),
    ],
)
def test_lint_log_format_json_rejections(fmt: str, substr: str):
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, msg = lint_log_format(fmt)
    assert ok is False and (not substr or substr in msg.lower())
    if FALCO_PATH is not None:
        assert lint_log_format(fmt)[0] is False


def test_lint_log_format_json_and_falco_paths():
    valid = '{"timestamp":"%{time.start}V","ip":"%{client.ip}V"}'
    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, msg = lint_log_format(valid)
        assert ok is True and ("falco" in msg.lower() or "valid" in msg.lower())
        assert lint_log_format('\\{"timestamp":"%{time.start}V"\\}')[0] is True
        assert lint_log_format('{"timestamp":"%{this_is_not_valid_vcl}V"}')[0] is True

    if FALCO_PATH is not None:
        assert lint_log_format(valid)[0] is True

    with (
        patch("backend.utils.vcl_utils.shutil.which", return_value="/usr/bin/falco"),
        patch(
            "backend.utils.vcl_utils._run_falco_lint",
            return_value=(1, "falco 0.4.2 linting…", "[ERROR] vcl_log syntax invalid at line 9"),
        ),
    ):
        ok_err, msg_err = lint_log_format('{"timestamp":"%{time.start}V"}')
        assert ok_err is False and "[ERROR]" in msg_err

    with (
        patch("backend.utils.vcl_utils.shutil.which", return_value="/usr/bin/falco"),
        patch("backend.utils.vcl_utils._run_falco_lint", return_value=(2, "falco: unexpected token near 'log'", "")),
    ):
        assert lint_log_format('{"timestamp":"%{time.start}V"}')[0] is False


@pytest.mark.parametrize(
    "bad", ['has"quote', "back\\slash", "tab\tinside", "newline\ninside", "null\x00byte", "del\x7fchar"]
)
def test_validate_quoted_literal_rejects_forbidden(bad: str):
    with pytest.raises(VclSafetyError):
        validate_quoted_literal(bad)


def test_validate_quoted_literal_and_macro_and_lint_doc(monkeypatch: pytest.MonkeyPatch):
    assert validate_quoted_literal("hello-world_123") == "hello-world_123"
    assert validate_quoted_literal("") == ""
    assert validate_quoted_literal(r"\.js$", allow_backslash=True) == r"\.js$"
    for bad in ('re"gex', "re\tgex"):
        with pytest.raises(VclSafetyError):
            validate_quoted_literal(bad, allow_backslash=True)
    with pytest.raises(ValueError):
        validate_quoted_literal('"')

    assert validate_log_format_macro("req.http.Host") == "req.http.Host"
    assert validate_log_format_macro(r'strftime(\{"%Y"\}, now)') == r'strftime(\{"%Y"\}, now)'
    for bad_macro in ("a; b", "a /* c */", "a // c", "close}brace", "open{brace"):
        with pytest.raises(VclSafetyError, match="VCL macro contains invalid characters"):
            validate_log_format_macro(bad_macro)

    monkeypatch.setattr(vs.shutil, "which", lambda _name: None)
    res = vs.lint_document("sub vcl_recv { }")
    assert (
        isinstance(res, LintResult) and res.ok is True and res.skipped is True and "falco" in res.skipped_reason.lower()
    )


def test_path_within_dir_and_sql_quote_helpers(tmp_path, monkeypatch):
    base_dir = str(tmp_path)
    assert path_within_dir(base_dir, str(tmp_path / "subdir" / "file.txt")) is True
    assert path_within_dir(base_dir, base_dir) is True
    assert path_within_dir(base_dir, str(tmp_path / ".." / "other_dir")) is False

    monkeypatch.setattr(os.path, "commonpath", lambda _paths: (_ for _ in ()).throw(ValueError("mismatch")))
    assert path_within_dir("/tmp/base", "/tmp/base/candidate") is False

    assert quote_path_list(["/a/o'brien.parquet", "/b.parquet"]) == "'/a/o''brien.parquet', '/b.parquet'"
    with pytest.raises(TypeError):
        escape_sql_literal(5)  # type: ignore[arg-type]

    needle = 'replace("\'", "\'\'")'
    root = pathlib.Path(__file__).resolve().parents[2] / "backend"
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if needle in p.read_text(encoding="utf-8") and p.name not in {"sql_validator.py", "duckdb.py"}
    ]
    assert offenders == []


def test_validate_url_exclusion_regex_and_lint_vcl():
    assert validate_url_exclusion_regex("") == "" and validate_url_exclusion_regex("   \t  ") == ""
    assert validate_url_exclusion_regex(r"\.(css|js|png)$") == r"\.(css|js|png)$"
    assert validate_url_exclusion_regex(r"^/healthz") == r"^/healthz"
    assert validate_url_exclusion_regex("\\.css$\n") == r"\.css$"
    assert validate_url_exclusion_regex("  \\.css$  ") == r"\.css$"

    for bad_type in (b"bytes", 42):
        with pytest.raises(RegexValidationError) as exc:
            validate_url_exclusion_regex(bad_type)  # type: ignore[arg-type]
        assert exc.value.reason == "type"

    with pytest.raises(RegexValidationError) as exc:
        validate_url_exclusion_regex("a" * (MAX_REGEX_BYTES + 1))
    assert exc.value.reason == "too_long"

    for bad_char in ('foo"bar', *(f"foo{c}bar" for c in ("\x00", "\x01", "\x07", "\x1b"))):
        with pytest.raises(RegexValidationError) as exc:
            validate_url_exclusion_regex(bad_char)
        assert exc.value.reason == "disallowed_char"

    for bad_re in (r"(unclosed", r"[unbalanced"):
        with pytest.raises(RegexValidationError) as exc:
            validate_url_exclusion_regex(bad_re)
        assert exc.value.reason == "invalid_regex"

    with patch("backend.utils.vcl_validator.shutil.which", return_value=None):
        res = lint_vcl("ignore me", snippet_name="test")
        assert res.skipped is True and res.ok is True and "falco" in res.skipped_reason.lower()
        with pytest.raises(RegexValidationError) as exc:
            validate_recv_exclusion_regex_with_lint(
                r"\.css$", build_full_snippet=lambda _: "irrelevant", require_falco=True
            )
        assert exc.value.reason == "falco_unavailable"
        cleaned, lint_res = validate_recv_exclusion_regex_with_lint(
            r"\.css$", build_full_snippet=lambda _: "irrelevant", require_falco=False
        )
        assert cleaned == r"\.css$" and lint_res.skipped

    for empty_val in (None, "", "   "):
        assert resolve_exclude_url_regex(empty_val) == DEFAULT_ASSET_EXT_REGEX
    assert DEFAULT_ASSET_EXT_REGEX in recv_snippet("svc")
    custom_body = recv_snippet("svc", exclude_url_regex=r"\.(css)$")
    assert r"\.(css)$" in custom_body and DEFAULT_ASSET_EXT_REGEX not in custom_body


@requires_falco
def test_lint_vcl_and_e2e_recv_exclusion_with_falco():
    assert lint_vcl(recv_snippet("test_svc"), snippet_name="recv_default").ok
    assert lint_vcl(recv_snippet("test_svc", exclude_url_regex=r"\.(css|js|png)$")).ok
    bad_res = lint_vcl('# defective snippet\nif (std.tolower(req.url) !~ "unclosed[") {\n  return(pass);\n}')
    assert not bad_res.ok and any("regex" in e.lower() for e in bad_res.errors)

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", exclude_url_regex=cleaned or None)

    cleaned, lint = validate_recv_exclusion_regex_with_lint(
        r"\.(jpg|png)$", build_full_snippet=builder, require_falco=True
    )
    assert cleaned == r"\.(jpg|png)$" and lint.ok
    with pytest.raises(RegexValidationError) as exc:
        validate_recv_exclusion_regex_with_lint(r'foo"bar', build_full_snippet=builder, require_falco=True)
    assert exc.value.reason == "disallowed_char"


def test_cmcd_capture_ordering_and_declarative_parity():
    lf_cfg = {"groups": ["A", "B", "C"], "custom_fields": get_cmcd_fields(True), "field_overrides": {}}
    recv = generate_capture_vcl(lf_cfg, cmcd_enabled=True, cmcd_mode="query_string", cmcd_version=1)["recv"]
    for cf in get_cmcd_fields(True):
        name = cf["name"]
        key = name[len("cmcd_") :]
        extract = recv.find(f"set req.http.x-cmcd:{key} ")
        capture = recv.find(f"set req.http.x-fos-edge-data:{name} ")
        assert -1 < extract < capture

    off = generate_capture_vcl(lf_cfg, cmcd_enabled=False)["recv"]
    assert "set req.http.x-cmcd:sid = " not in off and "set req.http.x-cmcd:sid = " in recv
    assert 0 <= recv.rfind('subfield(var.cmcd, "rtp"') < recv.find('querystring.filter(req.url, "CMCD")')

    state = FeatureState(
        service_id="svcCMCDORDER",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=True,
        cmcd=CmcdConfig(enabled=True, mode="query_string", version=1),
        log_fields=LogFieldsConfig(groups=["A", "B", "C"], custom_fields=get_cmcd_fields(True)),
    )
    decl_recv = generate_consolidated_snippet(state, "vcl_recv")
    ext, cap = decl_recv.find("set req.http.x-cmcd:sid "), decl_recv.find("set req.http.x-fos-edge-data:cmcd_sid ")
    if ext != -1 and cap != -1:
        assert ext < cap


def test_vcl_builder_cmcd_removal_and_log_paths():
    assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"
    assert VclBuilder.join_sections(["a", "", "  ", "b"]) == "a\n\nb"

    cfg = {"log_fields": {"custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True)}}
    assert _CMCD_FIELD_NAMES <= {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    orch._remove_cmcd_custom_fields(cfg)
    assert {c["name"] for c in cfg["log_fields"]["custom_fields"]} == {"mine"}
    for empty_cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(empty_cfg)

    dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
    assert analytics_log_path() == ANALYTICS_LOG_LAYOUT == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    assert rum_log_path() == "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    assert minute_list_prefix(dt) == "raw/request/year=2026/month=08/day=27/hour=16/minute=53/"
    assert rum_minute_list_prefix(dt) == "raw/rum/year=2026/month=08/day=27/hour=16/minute=53/"

    for mod in (gen, fa, sc):
        assert "raw/%Y-%m-%d" not in inspect.getsource(mod)
    assert "log_paths" in inspect.getsource(gen)

    for faro_ver in (None, "2.9.0"):
        st_cfg = FeatureState(
            service_id="svc_test",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
            logging_enabled=False,
            rum_enabled=True,
            faro_version=faro_ver,
        )
        shipped = generate_consolidated_snippet(st_cfg, "vcl_recv")
        shared = (
            generate_rum_asset_fetch_vcl("iad-va-us", faro_version=faro_ver)[RUM_ASSET_FETCH_NAME]
            if faro_ver
            else generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]
        )
        assert shared in shipped


class _FastlyDraftFake:
    def __init__(self, *, validate_ok=True, rollback_raises=False):
        self.calls: list[tuple[str, str]] = []
        self.validate_ok = validate_ok
        self.rollback_raises = rollback_raises

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        if path.endswith("/clone"):
            return {"number": 9}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad"]}
        if self.rollback_raises and path == "/service/svc/version/5/activate":
            raise RuntimeError("rollback failed")
        return {}


def test_draft_version_lifecycle_and_rollback(caplog):
    fake = _FastlyDraftFake()
    msgs: list[str] = []
    with draft_version("svc", "t", base_version=5, call=fake, comment="c", status_cb=msgs.append) as new_ver:
        assert new_ver == 9
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", "/service/svc/version/5/clone"),
        ("PUT", "/service/svc/version/9"),
        ("BODY", "mutate"),
        ("GET", "/service/svc/version/9/validate"),
        ("PUT", "/service/svc/version/9/activate"),
    ]
    assert msgs == ["Validating draft version 9...", "Activating version 9..."]

    fake2 = _FastlyDraftFake()
    with draft_version("svc", "t", base_version=5, call=fake2):
        pass
    assert ("PUT", "/service/svc/version/9") not in fake2.calls

    fake3 = _FastlyDraftFake()
    seen_at: list[int] = []
    with pytest.raises(ValueError, match="mutation boom"):
        with draft_version(
            "svc", "t", base_version=5, call=fake3, on_failure=lambda _: seen_at.append(len(fake3.calls))
        ):
            raise ValueError("mutation boom")
    assert seen_at == [1] and fake3.calls[-1] == ("PUT", "/service/svc/version/5/activate")
    assert not any(p.endswith("/validate") for _, p in fake3.calls)

    fake4 = _FastlyDraftFake(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version("svc", "t", base_version=5, call=fake4):
            pass
    assert ("PUT", "/service/svc/version/9/activate") not in fake4.calls
    assert fake4.calls[-1] == ("PUT", "/service/svc/version/5/activate")

    fake5 = _FastlyDraftFake(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version("svc", "t", base_version=5, call=fake5, rollback=False):
            pass
    assert ("PUT", "/service/svc/version/5/activate") not in fake5.calls

    fake6 = _FastlyDraftFake()
    with draft_version("svc", "t", base_version=5, call=fake6, comment="c", comment_before_activate=True):
        fake6.calls.append(("BODY", "mutate"))
    assert fake6.calls == [
        ("PUT", "/service/svc/version/5/clone"),
        ("BODY", "mutate"),
        ("GET", "/service/svc/version/9/validate"),
        ("PUT", "/service/svc/version/9"),
        ("PUT", "/service/svc/version/9/activate"),
    ]

    fake7 = _FastlyDraftFake(validate_ok=False)
    with draft_version("svc", "t", base_version=5, call=fake7, validate=False):
        pass
    assert not any(p.endswith("/validate") for _, p in fake7.calls)
    assert fake7.calls[-1] == ("PUT", "/service/svc/version/9/activate")

    for expect_empty, expected_kw in ((True, {"token": "t", "expect_empty": True}), (False, {"token": "t"})):
        seen: list[tuple[str, dict]] = []

        def _call(_m, path, _b=None, _seen=seen, **kwargs):
            _seen.append((path, kwargs))
            return {"number": 9} if path.endswith("/clone") else {"status": "ok"}

        with draft_version("svc", "t", base_version=5, call=_call, expect_empty_activate=expect_empty):
            pass
        assert [kw for p, kw in seen if p.endswith("/version/9/activate")] == [expected_kw]

    custom_msgs: list[str] = []
    with draft_version(
        "svc",
        "t",
        base_version=5,
        call=_FastlyDraftFake(),
        status_cb=custom_msgs.append,
        validate_msg="V{ver}",
        activate_msg="A{ver}",
    ):
        pass
    assert custom_msgs == ["V9", "A9"]

    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(ValueError, match="original"):
            with draft_version("svc", "t", base_version=5, call=_FastlyDraftFake(rollback_raises=True)):
                raise ValueError("original")
        rollback_to_version("svc", "t", 5, call=_FastlyDraftFake(rollback_raises=True))
    assert sum("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records) >= 2


def test_deploy_and_delete_remote_frontend_variants():
    responses = [
        {"id": "service123", "name": "test-service"},
        {"number": 1, "service_id": "service123"},
        {"name": "test-domain.global.ssl.fastly.net"},
        {"name": "gce_vm_origin"},
        {"number": 1, "active": True},
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        assert token == "fake-token"
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            "test-service", "test-domain.global.ssl.fastly.net", "1.2.3.4", 80, False, "fake-token"
        )
    assert res == {
        "service_id": "service123",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "1.2.3.4",
    }
    assert len(calls) == 5

    responses_ssl = [{"id": "service123"}, {"number": 1}, {}, {}, {"active": True}]
    calls_ssl: list[tuple] = []
    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=lambda m, p, b=None, **kw: (
            calls_ssl.append((m, p, b if b is not None else kw.get("body"))),
            responses_ssl.pop(0),
        )[1],
    ):
        deploy_remote_frontend(
            "test-service",
            "test-domain.global.ssl.fastly.net",
            "1.2.3.4",
            443,
            True,
            "fake-token",
            override_host="my-override.example.com",
        )
    backend_payload = next(c[2] for c in calls_ssl if c[1] == "/service/service123/version/1/backend")
    assert (
        backend_payload["override_host"]
        == backend_payload["ssl_sni_hostname"]
        == backend_payload["ssl_cert_hostname"]
        == "my-override.example.com"
    )

    with patch("backend.provision.fastly_api.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError, match="Failed to retrieve service ID"):
            deploy_remote_frontend("s", "d", "1.2.3.4", 80, False, "fake-token")
    seq = [{"id": "service123"}, {"service_id": "service123"}]
    with patch("backend.provision.fastly_api.fastly", side_effect=lambda *a, **k: seq.pop(0)):
        with pytest.raises(RuntimeError, match="Failed to retrieve version number"):
            deploy_remote_frontend("s", "d", "1.2.3.4", 80, False, "fake-token")

    del_seq = [[{"number": 2, "active": True}, {"number": 1, "active": False}], {}, {"status": "ok"}]
    del_calls: list[tuple] = []
    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=lambda m, p, *a, **k: (del_calls.append((m, p)), del_seq.pop(0))[1],
    ):
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert del_calls == [
        ("GET", "/service/remote123/version"),
        ("PUT", "/service/remote123/version/2/deactivate"),
        ("DELETE", "/service/remote123"),
    ]

    del_seq2 = [[{"number": 1, "active": False}], {"status": "ok"}]
    del_calls2: list[tuple] = []
    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=lambda m, p, *a, **k: (del_calls2.append((m, p)), del_seq2.pop(0))[1],
    ):
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert del_calls2 == [("GET", "/service/remote123/version"), ("DELETE", "/service/remote123")]

    with patch("backend.provision.fastly_api.fastly", side_effect=RuntimeError("HTTP 404 not found")):
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")

    err_seq = [[], RuntimeError("HTTP 500 DELETE /service/remote123\n    boom")]

    def _raise_or_pop(*a, **k):
        r = err_seq.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    with patch("backend.provision.fastly_api.fastly", side_effect=_raise_or_pop):
        with pytest.raises(RuntimeError, match="500"):
            delete_remote_frontend(remote_service_id="remote123", token="fake-token")


@pytest.mark.parametrize(
    ("step_fail", "client_err", "expected_step"),
    [
        (0, "HTTP 400 POST /service\n    Name already taken", "Create Service"),
        (1, "HTTP 400 POST /service/service123/version\n    Failed to create version", "Verify Draft Version"),
        (2, "HTTP 422 POST /service/service123/version/1/domain\n    Invalid domain name", "Attach Domain"),
        (3, "HTTP 400 POST /service/service123/version/1/backend\n    Invalid backend config", "Attach Backend Origin"),
        (4, "HTTP 500 PUT /service/service123/version/1/activate\n    Could not activate", "Activate Version"),
    ],
)
def test_deploy_remote_frontend_failures(step_fail, client_err, expected_step):
    ok_responses = [{"id": "service123"}, {"number": 1}, {}, {}, {"active": True}]
    state = {"i": 0}

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        idx = state["i"]
        state["i"] += 1
        if idx == step_fail:
            raise RuntimeError(client_err)
        return ok_responses[idx]

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                "test-service", "test-domain.global.ssl.fastly.net", "1.2.3.4", 80, False, "fake-token"
            )
    assert f"Fastly API error during {expected_step}:" in str(exc_info.value) and client_err in str(exc_info.value)


# ── Merged from tests/core/test_log_fields.py ──


def test_generate_log_format_and_field_overrides_and_default_off():
    fmt_minimal = generate_log_format({"groups": PRESETS["minimal"]["groups"], "field_overrides": {}})
    assert fmt_minimal.startswith("{") and fmt_minimal.endswith("}")
    assert '"timestamp":' in fmt_minimal and '"ip":' in fmt_minimal and '"url":' not in fmt_minimal

    fmt_standard = generate_log_format({"groups": PRESETS["standard"]["groups"], "field_overrides": {}})
    assert '"url":' in fmt_standard and '"pop":' in fmt_standard and '"country":' in fmt_standard

    fmt_ovr = generate_log_format({"groups": [], "field_overrides": {"url": True, "country": True}})
    assert '"url":' in fmt_ovr and '"country":' in fmt_ovr and '"pop":' not in fmt_ovr

    assert "cookie_session" in DEFAULT_OFF_FIELD_IDS
    enabled = resolve_enabled_fields({"groups": ["H"], "field_overrides": {}})
    assert "cookie_session" not in enabled and {"ja3", "ja4", "tls_ciphers_sha"} <= set(enabled)
    for preset_name in ("security", "full"):
        preset = PRESETS[preset_name]
        assert "cookie_session" not in resolve_enabled_fields(
            {"groups": preset["groups"], "field_overrides": preset.get("field_overrides", {})}
        )

    cfg_opt = {"groups": ["H"], "field_overrides": {"cookie_session": True}}
    assert "cookie_session" in resolve_enabled_fields(cfg_opt)
    assert '"cookie_session":' in generate_log_format(cfg_opt)
    assert "cookie_session" in get_required_edge_headers(cfg_opt)
    assert "cookie_session" in resolve_enabled_fields({"groups": [], "field_overrides": {"cookie_session": True}})
    assert "cookie_session" not in resolve_enabled_fields(
        {"groups": ["H"], "field_overrides": {"cookie_session": False}}
    )


def test_custom_origin_and_edge_fields_and_budget_clamps():
    cfg_orig = {
        "groups": ["L"],
        "custom_fields": [
            {
                "name": "bereq_drew",
                "vcl_log_expression": "beresp.http.x-drew",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
                "enabled": True,
            }
        ],
    }
    deliver = generate_capture_vcl(cfg_orig)["deliver"]
    assert 'if (req.http.x-is-cluster-fetch != "1") {\n  unset resp.http.x-fos-origin-data:bereq_drew;\n}' in deliver
    assert "\nunset resp.http.x-fos-origin-data:bereq_drew;" not in deliver

    snippets_edge = generate_capture_vcl(
        {
            "groups": [],
            "custom_fields": [
                {
                    "name": "edge_randomint",
                    "vcl_log_expression": "randomint(1, 100)",
                    "collection_stage": "edge",
                    "enabled": True,
                }
            ],
        }
    )
    assert "edge_randomint" in snippets_edge["recv"]
    assert "edge_randomint" not in snippets_edge.get("deliver", "")

    cfg_num = {
        "groups": [],
        "custom_fields": [
            {
                "name": f"z_num_{i}",
                "vcl_log_expression": f"req.http.x-edge-score:n{i}",
                "collection_stage": "deliver",
                "value_type": "numeric",
                "enabled": True,
            }
            for i in range(8)
        ]
        + [
            {
                "name": "zzz_tail",
                "vcl_log_expression": "req.http.x-edge-score:tail",
                "collection_stage": "deliver",
                "value_type": "string",
                "enabled": True,
            }
        ],
    }
    fmt_num = generate_log_format(cfg_num)
    num_lens = [int(m) for m in re.findall(r"x-edge-score:n\d+, 0, (\d+)\)", fmt_num)]
    assert num_lens and all(n <= 20 for n in num_lens)
    tail = re.search(r"x-edge-score:tail, 0, (\d+)\)", fmt_num)
    assert tail and int(tail.group(1)) > 0

    fmt_cmcd = generate_log_format(
        {
            "groups": PRESETS["standard"]["groups"],
            "custom_fields": [dict(cf) for cf in _CMCD_CUSTOM_FIELDS] + [dict(cf) for cf in _SCORING_CUSTOM_FIELDS],
        }
    )
    assert int(re.search(r"x-edge-score:sid, 0, (\d+)\)", fmt_cmcd).group(1)) > 0
    assert int(re.search(r"x-edge-score:rtt, 0, (\d+)\)", fmt_cmcd).group(1)) > 0
    assert int(re.search(r"x-fos-edge-data:cmcd_sid, 0, (\d+)\)", fmt_cmcd).group(1)) < 2000

    fmt_ip = generate_log_format({"schema_version": 2, "groups": []})
    assert (
        'if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, '
        'if(req.http.Fastly-Client-IP != "", req.http.Fastly-Client-IP, client.ip))' in fmt_ip
    )


def _valid_field(**overrides) -> dict:
    base = {
        "name": "my_field",
        "label": "My Field",
        "vcl_log_expression": "req.url",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    ("overrides", "existing", "expected_substr"),
    [
        ({"name": "1bad"}, [], "lowercase alphanumeric"),
        ({"name": "bad-name"}, [], "lowercase alphanumeric"),
        ({"name": "select"}, [], "reserved word"),
        ({"name": "timestamp"}, [], "already a built-in"),
        ({"name": "my_field"}, ["my_field"], "already exists"),
        ({"label": ""}, [], "label"),
        ({"label": "x" * 81}, [], "label"),
        ({"description": "x" * 501}, [], "description"),
        ({"vcl_log_expression": ""}, [], "empty"),
        ({"vcl_log_expression": "x" * 513}, [], "512"),
        ({"vcl_log_expression": "req.url\n"}, [], "newlines"),
        ({"vcl_log_expression": "req.url; set req.x = 1"}, [], "semicolons"),
        ({"vcl_log_expression": "req.url // hack"}, [], "comments"),
        ({"vcl_log_expression": "req.url # hack"}, [], "comments"),
        ({"duckdb_type": "QUANTUM"}, [], "duckdb_type"),
        ({"value_type": "unknown"}, [], "value_type"),
        ({"duckdb_type": "INTEGER", "value_type": "ip"}, [], "not compatible"),
        ({"bytes_estimate": 0}, [], "bytes_estimate"),
        ({"bytes_estimate": 2000}, [], "bytes_estimate"),
        ({"bytes_estimate": "not-a-number"}, [], "bytes_estimate"),
        ({"collection_stage": "fetch"}, [], "collection_stage"),
        ({"bytes_estimate": 5}, [], "WARN:"),
    ],
)
def test_validate_custom_field_rules(overrides, existing, expected_substr):
    assert validate_custom_field(_valid_field(), []) == []
    missing_errors = validate_custom_field({"name": "x"}, [])
    assert any("Missing required field" in e for e in missing_errors)
    assert not any("label" in e and "characters" in e for e in missing_errors)

    errors = validate_custom_field(_valid_field(**overrides), existing)
    assert any(expected_substr in e for e in errors), f"Expected {expected_substr!r} in {errors}"


# ── Merged from tests/core/test_log_line_budget.py ──


def _make_custom_fields(count: int, bytes_each: int) -> list[dict]:
    return [
        {
            "name": f"cf{i:03d}",
            "label": f"cf{i:03d}",
            "description": "",
            "vcl_log_expression": "req.http.Host",
            "collection_stage": "edge",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": bytes_each,
            "nullable": True,
            "enabled": True,
            "show_in_dashboard": False,
            "show_in_logs": True,
            "filterable": True,
        }
        for i in range(count)
    ]


def test_log_line_budget_bands_and_clamp():
    assert lf.FASTLY_LOG_LINE_DELIVER_MAX == 16 * 1024
    assert 0.40 < lf.FASTLY_LOG_LINE_SAFE_MAX / lf.FASTLY_LOG_LINE_DELIVER_MAX < 0.80

    cfg_small = {"groups": ["A"], "field_overrides": {}}
    assert lf.estimate_log_line_bytes(cfg_small) < lf.FASTLY_LOG_LINE_SAFE_MAX
    assert lf.check_log_line_budget(cfg_small) is None

    cfg_warn = {
        "groups": ["A", "B", "C", "D"],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=500),
    }
    est_warn = lf.estimate_log_line_bytes(cfg_warn)
    assert lf.FASTLY_LOG_LINE_SAFE_MAX <= est_warn < lf.FASTLY_LOG_LINE_DELIVER_MAX
    w = lf.check_log_line_budget(cfg_warn)
    assert w is not None and w["code"] == "LOG_LINE_APPROACHING_LIMIT" and w["severity"] == "warn"
    assert w["estimate_bytes"] == est_warn and str(est_warn) in w["message"] and "16" in w["message"]

    cfg_err = {"groups": [], "field_overrides": {}, "custom_fields": _make_custom_fields(count=20, bytes_each=1024)}
    est_err = lf.estimate_log_line_bytes(cfg_err)
    assert est_err >= lf.FASTLY_LOG_LINE_DELIVER_MAX
    e = lf.check_log_line_budget(cfg_err)
    assert e is not None and e["code"] == "LOG_LINE_TOO_LARGE" and e["severity"] == "error"
    assert "truncate" in e["message"].lower() and "16" in e["message"]

    cfg_clamp = {
        "field_limits": {"url": 4000, "ua": 4000, "referer": 4000},
        "custom_fields": _make_custom_fields(10, bytes_each=4000),
        "groups": {"core": True, "a": True},
    }
    for cf in cfg_clamp["custom_fields"]:
        cf["byte_limit"] = 4000
    fmt = lf.generate_log_format(cfg_clamp)
    caps = [int(m) for m in re.findall(r"substr\([^)]*, *0, *(\d+)\)", fmt)]
    assert caps and sum(caps) <= lf.FASTLY_LOG_LINE_DELIVER_MAX


def test_log_fields_endpoints_surface_warning_and_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    client = TestClient(app)

    svc_get = "test_svc_log_budget_get"
    config.save_config(
        svc_get,
        {
            "service_id": svc_get,
            "log_fields": {
                "schema_version": 2,
                "groups": ["A", "B", "C", "D"],
                "field_overrides": {},
                "custom_fields": _make_custom_fields(count=20, bytes_each=500),
            },
        },
    )
    resp_get = client.get(f"/api/services/{svc_get}/log-fields")
    assert (
        resp_get.status_code == 200 and resp_get.json()["line_budget_warning"]["code"] == "LOG_LINE_APPROACHING_LIMIT"
    )

    svc_post = "test_svc_log_budget_post"
    config.save_config(svc_post, {"service_id": svc_post, "log_fields": {"schema_version": 2, "groups": ["A"]}})
    payload = {
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        }
    }
    with patch("backend.state_sync.export_admin_state", return_value=None):
        resp_post = client.post(f"/api/services/{svc_post}/log-fields", json=payload)
    assert (
        resp_post.status_code == 200 and resp_post.json()["line_budget_warning"]["code"] == "LOG_LINE_APPROACHING_LIMIT"
    )

    svc_noop = "test_svc_log_budget_noop"
    config.save_config(svc_noop, {"service_id": svc_noop, "log_fields": {"schema_version": 2, "groups": ["A"]}})
    cfg = config.load_config(svc_noop)
    cfg["log_fields"]["format_hash"] = lf.format_hash(cfg["log_fields"])
    config.save_config(svc_noop, cfg)
    resp_noop = client.post(f"/api/services/{svc_noop}/log-fields", json={"log_fields": cfg["log_fields"]})
    assert resp_noop.status_code == 200 and resp_noop.json().get("ok") is True


# ── Merged from tests/core/test_field_registry.py ──


def test_registry_invariants_and_parity() -> None:
    assert isinstance(REGISTRY, tuple) and len(REGISTRY) > 0 and all(isinstance(f, LogField) for f in REGISTRY)
    with pytest.raises((AttributeError, TypeError)):
        REGISTRY[0].code = "mutated"  # type: ignore[misc]
    with pytest.raises(TypeError):
        BY_CODE["bogus"] = REGISTRY[0]  # type: ignore[index]

    for f in REGISTRY:
        assert fr.get(f.code) is f and fr.try_get(f.code) is f
        assert f.is_derived is (f.vcl is None)
        assert isinstance(f.required_by, tuple) and all(isinstance(i, str) and i for i in f.required_by)
    assert fr.try_get("__not_a_real_code__") is None
    with pytest.raises(KeyError):
        fr.get("__not_a_real_code__")

    codes = [f.code for f in REGISTRY]
    assert len(codes) == len(set(codes))
    assert {entry["id"] for entry in LOG_FIELD_CATALOG} == set(BY_CODE) == set(_BUILTIN_FIELD_NAMES)

    default_off_codes = {f.code for f in REGISTRY if f.default_off}
    assert (
        default_off_codes
        == {entry["id"] for entry in LOG_FIELD_CATALOG if entry.get("default_off")}
        == DEFAULT_OFF_FIELD_IDS
    )
    assert "cookie_session" in default_off_codes

    seen: set[str] = set()
    for group, fields in BY_GROUP.items():
        for f in fields:
            assert f.group is group and f.code not in seen
            seen.add(f.code)
    assert seen == set(BY_CODE)

    for insight in INSIGHT_DEFINITIONS:
        for code in insight["required_fields"]:
            assert fr.try_get(code) is not None


@pytest.mark.security_regression
def test_wire_order_matches_legacy_emission_order() -> None:
    legacy_emission_order = tuple(entry["id"] for entry in LOG_FIELD_CATALOG if entry["vcl"] is not None)
    assert tuple(f.code for f in REGISTRY if not f.is_derived) == legacy_emission_order


@given(code=st.sampled_from(sorted(f.code for f in REGISTRY)))
def test_hypothesis_every_code_lookup_succeeds(code: str) -> None:
    f = fr.get(code)
    assert f.code == code
    _ = f.is_derived


_RE_EXPORTED_NAMES = (
    "generate_log_format",
    "format_hash",
    "get_lf_config",
    "estimate_log_line_bytes",
    "resolve_enabled_fields",
    "check_log_line_budget",
    "validate_custom_field",
    "get_required_edge_headers",
    "get_catalog_for_api",
    "get_groups_for_api",
    "LOG_FIELD_CATALOG",
    "GROUP_INFO",
    "PRESETS",
    "INSIGHT_DEFINITIONS",
    "VALID_NAME_RE",
)


@pytest.mark.parametrize("name", _RE_EXPORTED_NAMES)
def test_re_exported_symbol_is_same_object_as_log_fields(name: str) -> None:
    assert getattr(fr, name) is getattr(lf, name)


# ── Merged from tests/core/test_sql_ident.py ──
BACKEND = pathlib.Path(__file__).resolve().parents[2] / "backend"


@pytest.mark.parametrize("name", ["lake", "_x", "a1", "Logs_Foo", "client_vitals", "_"])
def test_accepts_bare_identifiers(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is True
    assert sql_ident.require_safe_ident(name, "bad ident") == name


@pytest.mark.parametrize(
    "name",
    ["", "1abc", "a-b", 'a"b', "a b", "a;DROP", "a\n", "x.y", "é", "lake'--"],
)
def test_rejects_everything_else(name: str) -> None:
    assert sql_ident.is_safe_ident(name) is False
    with pytest.raises(ValueError, match="bad ident"):
        sql_ident.require_safe_ident(name, "bad ident")


def test_rejects_non_strings_and_error_message_shape() -> None:
    assert sql_ident.is_safe_ident(None) is False  # type: ignore[arg-type]
    assert sql_ident.is_safe_ident(5) is False  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        sql_ident.require_safe_ident(None, "bad ident")  # type: ignore[arg-type]
    with pytest.raises(ValueError) as exc:
        sql_ident.require_safe_ident("a-b", "invalid ducklake alias")
    assert str(exc.value) == "invalid ducklake alias: 'a-b'"


def test_bare_identifier_regex_has_one_owner() -> None:
    """No module besides sql_ident may declare the bare-identifier regex literal."""
    needles = ("[A-Za-z_][A-Za-z0-9_]*",)
    offenders = []
    for path in BACKEND.rglob("*.py"):
        if path.name in {"sql_ident.py", "sql_validator.py", "duckdb.py"}:
            continue
        text = path.read_text()
        if path.name in {"pg_connection.py", "clickhouse_client.py", "base.py", "clickhouse.py"}:
            continue
        if any(n in text for n in needles):
            offenders.append(str(path.relative_to(BACKEND)))
    assert offenders == [], f"use backend.core.sql_ident instead of a private identifier regex: {offenders}"
