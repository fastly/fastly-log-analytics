"""Unit tests for VCL semantics, Falco VCL simulation, VCL utils, and VCL builder helpers."""

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

import pytest

import backend.core.log_fields as vs
import backend.provision.declarative.reconciler as gen
import backend.provision.fastly_api as fa
import backend.routers.services.core as sc
from backend.config import path_within_dir
from backend.core.fastly.client import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.core.log_fields import (
    MAX_REGEX_BYTES,
    LintResult,
    RegexValidationError,
    VclSafetyError,
    generate_log_format,
    lint_log_format,
    lint_vcl,
    log_format_to_vcl_log,
    validate_log_format_macro,
    validate_quoted_literal,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)
from backend.core.sql_validator import escape_sql_literal
from backend.provision import cmcd_fields as orch
from backend.provision import generate_capture_vcl
from backend.provision.cli import (
    ANALYTICS_LOG_LAYOUT,
    VclBuilder,
    analytics_log_path,
    draft_version,
    minute_list_prefix,
    rollback_to_version,
    rum_log_path,
    rum_minute_list_prefix,
)
from backend.provision.cmcd_fields import (
    _CMCD_FIELD_NAMES,
    get_cmcd_fields,
    reconcile_cmcd_custom_fields,
)
from backend.provision.declarative.reconciler import (
    CmcdConfig,
    FeatureState,
    LogFieldsConfig,
    generate_consolidated_snippet,
)
from backend.provision.fastly_api import (
    SESSION_COOKIE_ALLOWLIST,
    _session_cookie_hash_vcl,
    delete_remote_frontend,
    deploy_remote_frontend,
)
from backend.provision.session_scoring_orchestrator import (
    DEFAULT_ASSET_EXT_REGEX,
    recv_snippet,
    resolve_exclude_url_regex,
)

FALCO_INSTALLED = shutil.which("falco") is not None
FALCO_REQUIRED = os.environ.get("FALCO_REQUIRED") == "1"

pytestmark = pytest.mark.no_postgres

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
    assert "f_edge" in snippets["recv"]
    assert "f_origin" in snippets["fetch"]
    assert "f_deliver" in snippets["deliver"]
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
    with patch("backend.core.log_fields.shutil.which", return_value=None):
        ok, msg = lint_log_format(fmt)
    assert ok is False
    assert not substr or substr in msg.lower()
    if FALCO_PATH is not None:
        assert lint_log_format(fmt)[0] is False


def test_lint_log_format_json_and_falco_paths():
    valid = '{"timestamp":"%{time.start}V","ip":"%{client.ip}V"}'
    with patch("backend.core.log_fields.shutil.which", return_value=None):
        ok, msg = lint_log_format(valid)
        assert ok is True
        assert "falco" in msg.lower() or "valid" in msg.lower()
        assert lint_log_format('\\{"timestamp":"%{time.start}V"\\}')[0] is True
        assert lint_log_format('{"timestamp":"%{this_is_not_valid_vcl}V"}')[0] is True

    if FALCO_PATH is not None:
        assert lint_log_format(valid)[0] is True

    with (
        patch("backend.core.log_fields.shutil.which", return_value="/usr/bin/falco"),
        patch(
            "backend.core.log_fields._run_falco_lint",
            return_value=(1, "falco 0.4.2 linting…", "[ERROR] vcl_log syntax invalid at line 9"),
        ),
    ):
        ok_err, msg_err = lint_log_format('{"timestamp":"%{time.start}V"}')
        assert ok_err is False
        assert "[ERROR]" in msg_err

    with (
        patch("backend.core.log_fields.shutil.which", return_value="/usr/bin/falco"),
        patch("backend.core.log_fields._run_falco_lint", return_value=(2, "falco: unexpected token near 'log'", "")),
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
    assert isinstance(res, LintResult)
    assert res.ok is True
    assert res.skipped is True
    assert "falco" in res.skipped_reason.lower()


def test_path_within_dir_and_sql_quote_helpers(tmp_path, monkeypatch):
    base_dir = str(tmp_path)
    assert path_within_dir(base_dir, str(tmp_path / "subdir" / "file.txt")) is True
    assert path_within_dir(base_dir, base_dir) is True
    assert path_within_dir(base_dir, str(tmp_path / ".." / "other_dir")) is False

    def _raise_mismatch(_paths):
        raise ValueError("mismatch")

    monkeypatch.setattr(os.path, "commonpath", _raise_mismatch)
    assert path_within_dir("/tmp/base", "/tmp/base/candidate") is False

    with pytest.raises(TypeError):
        escape_sql_literal(5)  # type: ignore[arg-type]

    needle = 'replace("\'", "\'\'")'
    root = pathlib.Path(__file__).resolve().parents[2] / "backend"
    offenders = [
        str(p.relative_to(root))
        for p in root.rglob("*.py")
        if needle in p.read_text(encoding="utf-8") and p.name not in {"sql_validator.py"}
    ]
    assert offenders == []


def test_validate_url_exclusion_regex_and_lint_vcl():
    assert validate_url_exclusion_regex("") == ""
    assert validate_url_exclusion_regex("   \t  ") == ""
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

    with patch("backend.core.log_fields.shutil.which", return_value=None):
        res = lint_vcl("ignore me", snippet_name="test")
        assert res.skipped is True
        assert res.ok is True
        assert "falco" in res.skipped_reason.lower()
        with pytest.raises(RegexValidationError) as exc:
            validate_recv_exclusion_regex_with_lint(
                r"\.css$", build_full_snippet=lambda _: "irrelevant", require_falco=True
            )
        assert exc.value.reason == "falco_unavailable"
        cleaned, lint_res = validate_recv_exclusion_regex_with_lint(
            r"\.css$", build_full_snippet=lambda _: "irrelevant", require_falco=False
        )
        assert cleaned == r"\.css$"
        assert lint_res.skipped

    for empty_val in (None, "", "   "):
        assert resolve_exclude_url_regex(empty_val) == DEFAULT_ASSET_EXT_REGEX
    assert DEFAULT_ASSET_EXT_REGEX in recv_snippet("svc")
    custom_body = recv_snippet("svc", exclude_url_regex=r"\.(css)$")
    assert r"\.(css)$" in custom_body
    assert DEFAULT_ASSET_EXT_REGEX not in custom_body


@requires_falco
def test_lint_vcl_and_e2e_recv_exclusion_with_falco():
    assert lint_vcl(recv_snippet("test_svc"), snippet_name="recv_default").ok
    assert lint_vcl(recv_snippet("test_svc", exclude_url_regex=r"\.(css|js|png)$")).ok
    bad_res = lint_vcl('# defective snippet\nif (std.tolower(req.url) !~ "unclosed[") {\n  return(pass);\n}')
    assert not bad_res.ok
    assert any("regex" in e.lower() for e in bad_res.errors)

    def builder(cleaned: str) -> str:
        return recv_snippet("svc", exclude_url_regex=cleaned or None)

    cleaned, lint = validate_recv_exclusion_regex_with_lint(
        r"\.(jpg|png)$", build_full_snippet=builder, require_falco=True
    )
    assert cleaned == r"\.(jpg|png)$"
    assert lint.ok
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
    assert "set req.http.x-cmcd:sid = " not in off
    assert "set req.http.x-cmcd:sid = " in recv
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
    assert seen_at == [1]
    assert fake3.calls[-1] == ("PUT", "/service/svc/version/5/activate")
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

    with caplog.at_level(logging.WARNING, logger="backend.provision"):
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

    def _record_ssl_call(method, path, body=None, **kw):
        calls_ssl.append((method, path, body if body is not None else kw.get("body")))
        return responses_ssl.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=_record_ssl_call):
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
    assert backend_payload["ssl_check_cert"] is True
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

    def _record_delete_call(method, path, *a, **k):
        del_calls.append((method, path))
        return del_seq.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=_record_delete_call):
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert del_calls == [
        ("GET", "/service/remote123/version"),
        ("PUT", "/service/remote123/version/2/deactivate"),
        ("DELETE", "/service/remote123"),
    ]

    del_seq2 = [[{"number": 1, "active": False}], {"status": "ok"}]
    del_calls2: list[tuple] = []

    def _record_delete_call2(method, path, *a, **k):
        del_calls2.append((method, path))
        return del_seq2.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=_record_delete_call2):
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
    assert f"Fastly API error during {expected_step}:" in str(exc_info.value)
    assert client_err in str(exc_info.value)


# ─── Recorded Fastly API Contracts (unified from compute/cloud/infra/test_contracts.py) ───

import json  # noqa: E402
import os  # noqa: E402

from tests.support.recording_transport import _redact, fetch_contract, install_identifiers  # noqa: E402

RECORDED_FASTLY_API_FIXTURES = (
    Path(__file__).resolve().parents[2] / "compute" / "cloud" / "tests" / "fixtures" / "fastly-api"
)


def test_redaction_removes_credentials_and_identity_before_recording() -> None:
    assert _redact(
        {
            "id": "token-value",
            "customer_id": "customer",
            "services": ["private-service"],
            "secret": "credential",
            "access_key": "credential",
            "_links": {"self": "https://api.fastly.com/customer/private"},
            "scope": "global:read",
            "expires_at": None,
        }
    ) == {
        "id": "REDACTED_ID",
        "customer_id": "REDACTED_ID",
        "services": ["REDACTED_ID"],
        "scope": "global:read",
        "expires_at": None,
    }


def _matches_recorded_schema(recorded: object, current: object) -> bool:
    if recorded is None:
        return current is None or isinstance(current, (str, int, float, bool))
    if type(current) is not type(recorded):
        return False
    if isinstance(recorded, dict) and isinstance(current, dict):
        return current.keys() == recorded.keys() and all(
            _matches_recorded_schema(recorded[key], current[key]) for key in recorded
        )
    elif isinstance(recorded, list) and isinstance(current, list) and recorded:
        return all(any(_matches_recorded_schema(example, value) for example in recorded) for value in current)
    return True


def _assert_recorded_schema(recorded: object, current: object) -> None:
    assert _matches_recorded_schema(recorded, current), "Observed API field/type schema drift"


@pytest.mark.skipif(os.environ.get("FLP_LIVE") != "1", reason="Explicit read-only live gate")
def test_live_recorded_get_contracts_have_not_drifted() -> None:
    service_id = os.environ.get("FLP_TEST_SERVICE_ID", "xa20dYfOsL4MhjIo66mVhh")
    for path in sorted(RECORDED_FASTLY_API_FIXTURES.glob("*.json")):
        fixture = json.loads(path.read_text())
        if fixture.get("recording_group") == "install":
            continue
        projection = {
            "token-self": "token",
            "object-storage": "product",
            "test-service-generated-vcl": "vcl",
        }.get(path.stem, "")
        status, response = fetch_contract(
            fixture["request"]["path"].replace("TEST_SERVICE", service_id),
            projection=projection,
        )
        assert status == fixture["status"], f"Status drift for {path.name}"
        _assert_recorded_schema(fixture["response"], response)


@pytest.mark.skipif(
    os.environ.get("FLP_LIVE") != "1" or not os.environ.get("FLP_INVENTORY"),
    reason="Explicit read-only live install gate and inventory",
)
def test_live_created_install_get_contracts_have_not_drifted() -> None:
    inventory = json.loads(Path(os.environ["FLP_INVENTORY"]).read_text())
    identifiers = install_identifiers(inventory)
    for path in sorted(RECORDED_FASTLY_API_FIXTURES.glob("install-*.json")):
        fixture = json.loads(path.read_text())
        request_path = fixture["request"]["path"]
        for original, replacement in identifiers.items():
            request_path = request_path.replace(replacement, original)
        status, response = fetch_contract(request_path, identifiers=identifiers)
        assert status == fixture["status"], f"Status drift for {path.name}"
        _assert_recorded_schema(fixture["response"], response)


def test_recorded_created_services_have_correct_isolated_store_links() -> None:
    for role, expected_service in (
        ("compute", "INSTALL_COMPUTE_ID"),
        ("worker", "INSTALL_WORKER_ID"),
    ):
        fixture = json.loads((RECORDED_FASTLY_API_FIXTURES / f"install-{role}-resource-links.json").read_text())
        assert fixture["status"] == 200
        assert {entry["name"]: (entry["resource_id"], entry["resource_type"]) for entry in fixture["response"]} == {
            "flp_config": ("STORE_CONFIG_ID", "config"),
            "flp_secrets": ("STORE_SECRET_ID", "secret-store"),
            "flp_state": ("STORE_KV_ID", "kv-store"),
        }
        assert all(entry["service_id"] == expected_service for entry in fixture["response"])


def test_recorded_backend_readbacks_have_matching_tls_and_only_tick_is_shielded() -> None:
    for role in ("compute", "worker", "front"):
        backends = json.loads((RECORDED_FASTLY_API_FIXTURES / f"install-{role}-backends.json").read_text())["response"]
        assert len(backends) == 3
        for origin in backends:
            assert origin["use_ssl"] is True
            assert origin["prefer_ipv6"] is False
            assert (
                origin["address"]
                == origin["override_host"]
                == origin["ssl_sni_hostname"]
                == origin["ssl_cert_hostname"]
            )
            assert origin["shield"] == ("iad-va-us" if origin["name"] == "flp_tick" else None)


def test_recorded_front_clock_and_write_only_dictionary_configuration() -> None:
    dictionaries = json.loads((RECORDED_FASTLY_API_FIXTURES / "install-front-dictionaries.json").read_text())[
        "response"
    ]
    assert {item["name"]: item["write_only"] for item in dictionaries} == {
        "flp_front_auth": True,
        "flp_fos_buckets": False,
        "flp_fos_credentials": True,
    }
    clock = json.loads((RECORDED_FASTLY_API_FIXTURES / "install-front-healthchecks.json").read_text())["response"]
    assert len(clock) == 1
    assert (clock[0]["method"], clock[0]["path"], clock[0]["host"]) == (
        "GET",
        "/_tick",
        "front.example",
    )
    assert clock[0]["expected_response"] == 200


# ─── M2 Security Regressions (SEC-11 / SEC-12 / SEC-14 / SEC-15 + Falco) ─────


def _run_raw_falco_test(main_vcl: str, test_vcl: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        main_path = Path(tmpdir) / "main.vcl"
        test_path = Path(tmpdir) / "suite.test.vcl"
        main_path.write_text(main_vcl)
        test_path.write_text(test_vcl)
        return subprocess.run(
            [FALCO_PATH or "falco", "test", str(main_path)],
            cwd=tmpdir,
            capture_output=True,
            text=True,
            timeout=20,
        )


def _run_raw_falco_lint(main_vcl: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        main_path = Path(tmpdir) / "main.vcl"
        main_path.write_text(main_vcl)
        return subprocess.run(
            [FALCO_PATH or "falco", "lint", str(main_path)],
            cwd=tmpdir,
            capture_output=True,
            text=True,
            timeout=20,
        )


@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec11_x_fos_request_scrubbed_and_sigv4_scoped_to_fos_rum_assets() -> None:
    from backend.core.fastly.client import (
        RUM_DELIVER_SET_COOKIE_NAME,
        RUM_RECV_NAME,
        _generate_sigv4_sign_vcl,
        generate_rum_vcl,
    )

    assert "X-FOS-Request" in fa._SCRUB_HEADERS
    assert fa._SCRUB_HEADERS[-1] == "X-FOS-Request"

    # Both logging+RUM and RUM-only declarative vcl_recv scrub X-FOS-Request at the edge first hop
    for logging_on in (True, False):
        state = FeatureState(
            service_id="svc_sec11",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="raw",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            logging_enabled=logging_on,
            rum_enabled=True,
            faro_version="2.9.0",
        )
        recv_vcl = generate_consolidated_snippet(state, "vcl_recv")
        assert recv_vcl.count("unset req.http.X-FOS-Request;") == 1
        miss_vcl = generate_consolidated_snippet(state, "vcl_miss")
        assert (
            'req.http.X-FOS-Request == "1" && req.backend == F_fos_origin && !req.backend.is_shield '
            '&& (req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js")'
        ) in miss_vcl

    standalone_rum = generate_rum_vcl("svc_sec11")
    assert "unset req.http.X-FOS-Request;" in standalone_rum[RUM_RECV_NAME]
    assert (
        'req.http.X-FOS-Request == "1" && req.backend == F_fos_origin && !req.backend.is_shield '
        '&& req.url.path == "/js/rum.js"'
    ) in _generate_sigv4_sign_vcl(None)
    assert "; Path=/; SameSite=Lax; Secure" in standalone_rum[RUM_DELIVER_SET_COOKIE_NAME]


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec11_falco_simulation_blocks_spoofed_x_fos_request_and_signs_only_rum_assets() -> None:
    state = FeatureState(
        service_id="svc_sec11",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="raw",
        fos_endpoint="us-east-1.object.fastlystorage.app",
        logging_enabled=False,
        rum_enabled=True,
        faro_version="2.9.0",
    )
    recv_body = generate_consolidated_snippet(state, "vcl_recv")
    miss_body = generate_consolidated_snippet(state, "vcl_miss")

    main_vcl = f"""
backend F_customer_origin {{ .host = "origin.example.com"; .port = "443"; }}
backend F_fos_origin {{ .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }}
backend ssl_shield_iad_va_us {{ .host = "shield.fastly.net"; .port = "443"; }}
table fos_credentials {{
  "access_key": "AKIA_TEST_KEY",
  "secret_key": "SECRET_TEST_KEY",
  "bucket": "my-rum-bucket",
  "region": "us-east-1",
}}

sub vcl_recv {{
  set req.backend = F_customer_origin;
{recv_body}
  return(lookup);
}}

sub vcl_miss {{
{miss_body}
  return(fetch);
}}
"""
    lint_res = _run_raw_falco_lint(main_vcl)
    assert lint_res.returncode == 0, f"falco lint failed:\n{lint_res.stdout}\n{lint_res.stderr}"

    test_vcl = """
// @scope: recv
sub test_spoofed_x_fos_request_scrubbed_on_customer_route {
  set req.method = "GET";
  set req.url = "/account/profile";
  set req.http.X-FOS-Request = "1";
  testing.call_subroutine("vcl_recv");
  assert.is_notset(req.http.X-FOS-Request);
  assert.equal(req.backend, F_customer_origin);
}

// @scope: miss
sub test_customer_backend_never_receives_fos_sigv4_even_with_header {
  set req.method = "GET";
  set req.url = "/js/rum.js";
  set bereq.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  set req.backend = F_customer_origin;
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
  assert.is_notset(bereq.http.x-amz-content-sha256);
}

// @scope: miss
sub test_non_rum_path_on_fos_origin_never_signed {
  set req.method = "GET";
  set req.url = "/raw/request/secret.parquet";
  set bereq.url = "/raw/request/secret.parquet";
  set req.http.X-FOS-Request = "1";
  set req.backend = F_fos_origin;
  testing.call_subroutine("vcl_miss");
  assert.is_notset(bereq.http.Authorization);
}

// @scope: miss
sub test_legitimate_rum_and_faro_assets_signed_on_fos_origin {
  set req.method = "GET";
  set req.url = "/js/faro-sdk.js";
  set bereq.url = "/js/faro-sdk.js";
  set req.http.X-FOS-Request = "1";
  set req.backend = F_fos_origin;
  testing.call_subroutine("vcl_miss");
    assert.equal(bereq.url, "/my-rum-bucket/rum/faro-web-sdk-v2.9.0.iife.js");
  assert.match(bereq.http.Authorization, "^AWS4-HMAC-SHA256 Credential=AKIA_TEST_KEY/");
}
"""
    res = _run_raw_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"falco test failed:\n{res.stdout}\n{res.stderr}"


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec12_cdn_load_vcl_deterministic_and_falco_auth_ratelimit_simulation() -> None:
    from backend.core.fastly.client import load_vcl

    vcl_rl = load_vcl(rate_limiting=True)
    assert vcl_rl == load_vcl(rate_limiting=True)
    assert "unprovisioned-fallback" not in vcl_rl
    vcl_no_rl = load_vcl(rate_limiting=False)
    assert vcl_no_rl == load_vcl(rate_limiting=False)

    preamble = """
backend F_fos_origin { .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }
table fos_credentials {
  "access_key": "AKIA_CDN",
  "secret_key": "SECRET_CDN",
  "bucket": "cdn-bucket",
  "region": "us-east-1",
}
table cdn_auth {
  "secret_key": "valid-cdn-secret-key",
}
"""
    for variant in (vcl_rl, vcl_no_rl):
        lint_res = _run_raw_falco_lint(preamble + "\n" + variant)
        assert lint_res.returncode == 0, f"falco lint failed:\n{lint_res.stdout}\n{lint_res.stderr}"

    test_vcl = r"""
// @scope: recv
sub test_valid_header_auth_scrubs_secrets_and_filters_querystring {
  testing.inject_variable("client.ip", "198.51.100.10");
  set req.method = "GET";
  set req.url = "/?list-type=2&prefix=raw%2F&key=leaked&junk=1";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  set req.http.Cookie = "session=secret";
  set req.http.Authorization = "Bearer leaked";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
  assert.is_notset(req.http.x-fastly-key);
  assert.is_notset(req.http.Cookie);
  assert.is_notset(req.http.Authorization);
  assert.equal(req.url, {"/?list-type=2&prefix=raw%2F"});
}

// @scope: recv
sub test_query_param_key_without_header_is_rejected_401 {
  testing.inject_variable("client.ip", "198.51.100.20");
  set req.method = "GET";
  set req.url = "/raw/request/file.zst?key=valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.error(401);
}

// @scope: recv
sub test_legacy_secret_item_fallback_and_fail_closed_403_when_empty {
  testing.inject_variable("client.ip", "198.51.100.30");
  testing.table_set(cdn_auth, "secret_key", "");
  testing.table_set(cdn_auth, "secret", "legacy-cdn-secret");
  set req.method = "GET";
  set req.url = "/meta/admin_state.json";
  set req.http.x-fastly-key = "legacy-cdn-secret";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);

  testing.table_set(cdn_auth, "secret", "");
  set req.http.x-fastly-key = "";
  testing.call_subroutine("vcl_recv");
  assert.error(403);
}

// @scope: recv
sub test_ipv6_slash64_penaltybox_enforced_before_valid_auth_ignoring_xff {
  testing.table_set(cdn_auth, "secret_key", "valid-cdn-secret-key");
  // 10 failures across distinct hosts inside 2001:db8:abcd:0012::/64 with spoofed XFF
  testing.inject_variable("client.ip", "2001:db8:abcd:12::1");
  set req.http.X-Forwarded-For = "203.0.113.1";
  set req.http.Fastly-Client-IP = "203.0.113.2";
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  assert.error(401);

  testing.inject_variable("client.ip", "2001:db8:abcd:12::2");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::3");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::4");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::5");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::6");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::7");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::8");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12::9");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");
  testing.inject_variable("client.ip", "2001:db8:abcd:12:dead:beef:cafe:10");
  set req.http.x-fastly-key = "wrong";
  testing.call_subroutine("vcl_recv");

  // 11th request from a new host in the SAME /64 WITH the valid key is blocked 429 FIRST!
  testing.inject_variable("client.ip", "2001:db8:abcd:12:ffff:ffff:ffff:ffff");
  set req.method = "GET";
  set req.url = "/raw/request/a.zst";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.error(429);

  // FASTLYPURGE from a penalty-boxed /64 MUST ALSO be blocked 429 FIRST (wrong key AND valid key)
  testing.inject_variable("client.ip", "2001:db8:abcd:12::beef");
  set req.method = "FASTLYPURGE";
  set req.url = "/raw/request/a.zst";
  set req.http.x-fastly-key = "wrong-purge-guess";
  testing.call_subroutine("vcl_recv");
  assert.error(429);

  set req.method = "FASTLYPURGE";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.error(429);

  // Different /64 succeeds
  testing.inject_variable("client.ip", "2001:db8:abcd:13::1");
  set req.method = "GET";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
}

// @scope: recv
sub test_disallowed_http_methods_rejected_405 {
  testing.inject_variable("client.ip", "198.51.100.99");
  testing.table_set(cdn_auth, "secret_key", "valid-cdn-secret-key");
  set req.method = "DELETE";
  set req.url = "/raw/request/a.zst";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.error(405);

  set req.method = "FASTLYPURGE";
  set req.url = "/raw/request/a.zst";
  set req.http.x-fastly-key = "valid-cdn-secret-key";
  testing.call_subroutine("vcl_recv");
  assert.error(405);
}

// @scope: error
sub test_vcl_error_sets_no_store_allow_and_retry_after {
  set obj.status = 429;
  set obj.response = "Too Many Requests";
  testing.call_subroutine("vcl_error");
  assert.equal(obj.http.Cache-Control, "no-store");
  assert.equal(obj.http.Retry-After, "900");
}
"""
    res = _run_raw_falco_test(preamble + "\n" + vcl_rl, test_vcl)
    assert res.returncode == 0, f"falco test failed:\n{res.stdout}\n{res.stderr}"


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec14_scorer_pass_snippet_strips_customer_cookies_and_preserves_allowlist() -> None:
    from backend.provision.session_scoring.wasm_deploy import SCORING_BACKEND_VCL_NAME, pass_snippet

    snip = pass_snippet("svc_scorer_1", "scorer-sec-1")
    assert "unset bereq.http.Cookie;" in snip
    assert 'subfield(req.http.Cookie, "X-Session-State", ";")' in snip
    assert 'subfield(req.http.Cookie, "_ess", ";")' in snip

    main_vcl = f"""
backend {SCORING_BACKEND_VCL_NAME} {{ .host = "scorer.edgecompute.app"; .port = "443"; }}

sub vcl_pass {{
{snip}
  return(fetch);
}}
"""
    test_vcl = f"""
// @scope: pass
sub test_strips_customer_cookies_and_keeps_x_session_state {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  set req.http.Cookie = "session_id=secret123; X-Session-State=tok_abc; csrftoken=xyz";
  set bereq.http.Cookie = req.http.Cookie;
  testing.call_subroutine("vcl_pass");
  assert.equal(bereq.http.Cookie, "X-Session-State=tok_abc");
}}

// @scope: pass
sub test_strips_customer_cookies_and_keeps_both_scorer_cookies {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  set req.http.Cookie = "session_id=secret123; _ess=ess_999; X-Session-State=tok_abc";
  set bereq.http.Cookie = req.http.Cookie;
  testing.call_subroutine("vcl_pass");
  assert.equal(bereq.http.Cookie, "X-Session-State=tok_abc; _ess=ess_999");
}}

// @scope: pass
sub test_strips_all_cookies_when_no_scorer_cookie_present {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  set req.http.Cookie = "session_id=secret123; auth_token=topsecret";
  set bereq.http.Cookie = req.http.Cookie;
  testing.call_subroutine("vcl_pass");
  assert.is_notset(bereq.http.Cookie);
}}
"""
    res = _run_raw_falco_test(main_vcl, test_vcl)
    assert res.returncode == 0, f"falco test failed:\n{res.stdout}\n{res.stderr}"

    with pytest.raises(ValueError):
        pass_snippet('svc"; set bereq.http.X-Pwn = "1', "scorer-sec-1")
    with pytest.raises(ValueError):
        pass_snippet("svc\npwn", "scorer-sec-1")


@pytest.mark.security_regression
@pytest.mark.no_postgres
@pytest.mark.parametrize(
    "bad_expr",
    [
        "true) || (true",
        "((true)",
        "(true))",
        'req.http.Host == "unterminated',
        'req.http.Host == "ok"\\',
        "req.http.Host == 'single'",
        'req.http.X-Foo; set req.http.X-Pwn = "1"',
        "req.http.X-Foo\nbar",
        "req.http.X-Foo\rbar",
        "req.http.X-Foo\x00bar",
        '{"long_string"}',
        "req.http.X-Foo // comment",
        "req.http.X-Foo /* comment */",
        "req.http.X-Foo */",
        "req.http.X-Foo # comment",
        'req.http.X-Foo "${pwn}"',
        'req.http.X-Foo "%{if true}x%{endif}"',
        "req.http.X-Foo }V",
        'table.lookup(fos_credentials, "secret_key", "")',
        'TABLE . LOOKUP(cdn_auth, "secret_key", "")',
        'table.lookup_integer(my_dict, "k", 0)',
        'table.lookup_bool(my_dict, "k", false)',
        'table.lookup_regex(my_dict, "k", "")',
        'table.contains(my_dict, "k")',
        "req.http.X-Foo\u2028bar",
        "req.http.X-Foo\u2029bar",
        "req.http.X-Foo\x85bar",
        "req.http.X-Edge-Scorer-Auth",
        "req.http.x-fastly-key",
        "req.http.Fastly-Key",
        "req.http.x-flp-secret",
        "req.http.Authorization",
        "req.http.Cookie",
        "bereq.http.Cookie",
        "resp.http.Set-Cookie",
        'subfield(req.http.Cookie, "session_id", ";")',
        '"session_scoring_secrets"',
        '"flp_fos_credentials"',
        '"flp_fos_buckets"',
        '"flp_front_auth"',
        r'req.http.X-A == "\") || true || (req.http.X-B == "x" && req.http.X-C == "y\"',
        r'(req.http.X-A == "\") || true || (req.http.X-B == "y\"',
        r'req.http.X-A == "\""',
        r"req.http.Host == \"ok\"",
    ],
)
def test_sec15_vcl_validators_block_all_breakout_and_exfiltration_vectors(bad_expr: str) -> None:
    from backend.core.catalog import validate_vcl_custom_condition, validate_vcl_log_expression
    from backend.core.fastly.client import validate_rum_custom_condition

    assert validate_vcl_custom_condition(bad_expr) is not None
    assert validate_rum_custom_condition(bad_expr) is not None
    assert len(validate_vcl_log_expression(bad_expr)) >= 1


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec15_valid_catalog_and_system_expressions_pass_validators_and_falco(run_falco_test) -> None:
    from backend.core.catalog import LOG_FIELD_CATALOG, validate_vcl_custom_condition, validate_vcl_log_expression
    from backend.core.fastly.client import validate_rum_custom_condition
    from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
    from backend.provision.rum_orchestrator import _RUM_CUSTOM_FIELDS
    from backend.provision.session_scoring.orchestrate import _SCORING_CUSTOM_FIELDS

    for cond in (
        None,
        "",
        'req.http.Host == "shop.example.com" && req.url.ext != "png"',
        'req.http.User-Agent !~ "bot\\b" && (resp.status >= 200 && resp.status < 500)',
    ):
        assert validate_vcl_custom_condition(cond) is None
        assert validate_rum_custom_condition(cond) is None

    assert validate_rum_custom_condition('req.url.path == "/checkout"') is not None
    assert validate_vcl_custom_condition('req.url.path == "/checkout"') is None

    for cf in [*_CMCD_CUSTOM_FIELDS, *_RUM_CUSTOM_FIELDS, *_SCORING_CUSTOM_FIELDS]:
        assert validate_vcl_log_expression(cf["vcl_log_expression"]) == [], cf
    assert validate_vcl_log_expression("req.http.x-edge-cookie-compliance") == []

    assert len(LOG_FIELD_CATALOG) >= 80
    cfg = {
        "groups": ["A"],
        "custom_fields": [
            {
                "name": "lt_run_id",
                "vcl_log_expression": "req.http.X-LT-Run-Id",
                "collection_stage": "origin",
                "origin_log_frequency": "all",
                "sql_type": "VARCHAR",
                "value_type": "string",
                "bytes_estimate": 48,
                "enabled": True,
            }
        ],
    }
    valid, lint_errs = lint_log_format(generate_log_format(cfg))
    assert valid, lint_errs
    falco_res = run_falco_test(
        cfg,
        """
// @scope: fetch
sub test_lt_run_id_captured {
    set req.http.X-LT-Run-Id = "run-123";
    testing.call_subroutine("vcl_fetch");
    assert.equal(beresp.http.x-fos-origin-data:lt_run_id, "run-123");
}
""",
    )
    assert falco_res.returncode == 0, f"{falco_res.stdout}\n{falco_res.stderr}"


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec11_sec14_scoring_scrub_without_logging_rum_add_set_cookie_and_rum_js_fetch_guard() -> None:
    from backend.core.fastly.client import generate_rum_vcl
    from backend.provision.declarative.models import FeatureState
    from backend.provision.declarative.vcl_builder import generate_consolidated_snippet

    # 1. Declarative vcl_recv with logging_enabled=False and scoring.enabled=True MUST scrub spoofed
    #    scoring/NGWAF headers on external first hop before scoring routing.
    scoring_only_state = FeatureState.from_config(
        {
            "service_id": "svc_score_only",
            "log_period": 60,
            "sample_rate": 100,
            "logging_enabled": False,
            "rum_enabled": False,
            "scoring": {"enabled": True, "domain": "scorer.edgecompute.app", "request_secret": "sec123"},
            "log_fields": {"groups": []},
        }
    )
    recv_snip = generate_consolidated_snippet(scoring_only_state, "vcl_recv")
    main_scoring = f"""
backend F_origin {{ .host = "origin.example.com"; .port = "443"; }}
backend F_session_scorer {{ .host = "scorer.edgecompute.app"; .port = "443"; }}
sub vcl_recv {{
  set req.backend = F_origin;
{recv_snip}
  return(lookup);
}}
"""
    test_scoring = r"""
// @scope: recv
sub test_spoofed_scoring_pass_scrubbed_and_routed_to_scorer_when_logging_disabled {
  set req.url = "/checkout";
  set req.http.X-Edge-Scoring-Pass = "1";
  set req.http.x-edge-score = "0.0";
  set req.http.X-Edge-Score = "0.0";
  set req.http.X-Edge-Score-Reason = "clean";
  set req.http.X-Edge-Score-Enforce = "allow";
  set req.http.X-Edge-Sid = "forged-sid";
  set req.http.X-Edge-Score-Set-Cookie = "X-Session-State=forged";
  set req.http.X-Edge-Prev-Anchor = "/home";
  set req.http.x-sigsci-skip-inspection-once = "1";
  testing.call_subroutine("vcl_recv");
  assert.state(pass);
  assert.equal(req.backend, F_session_scorer);
  assert.is_notset(req.http.x-edge-score);
  assert.is_notset(req.http.X-Edge-Score);
  assert.is_notset(req.http.X-Edge-Score-Reason);
  assert.is_notset(req.http.X-Edge-Score-Enforce);
  assert.is_notset(req.http.X-Edge-Sid);
  assert.is_notset(req.http.X-Edge-Score-Set-Cookie);
  assert.is_notset(req.http.X-Edge-Prev-Anchor);
}

// @scope: recv
sub test_excluded_path_scrubs_spoofed_ngwaf_skip_on_origin_when_logging_disabled {
  set req.url = "/favicon.ico";
  set req.http.x-sigsci-skip-inspection-once = "1";
  set req.http.X-Edge-Score = "0.0";
  testing.call_subroutine("vcl_recv");
  assert.state(lookup);
  assert.equal(req.backend, F_origin);
  assert.is_notset(req.http.x-sigsci-skip-inspection-once);
  assert.is_notset(req.http.X-Edge-Score);
}
"""
    res_scoring = _run_raw_falco_test(main_scoring, test_scoring)
    assert res_scoring.returncode == 0, f"falco test failed:\n{res_scoring.stdout}\n{res_scoring.stderr}"

    # 2. RUM - Set cookies must use `add resp.http.Set-Cookie` so customer/scorer Set-Cookie survives.
    rum_snips = generate_rum_vcl("svc_rum_1")
    deliver_cookie_vcl = rum_snips["RUM - Set cookies"]
    assert "add resp.http.Set-Cookie =" in deliver_cookie_vcl
    assert "set resp.http.Set-Cookie =" not in deliver_cookie_vcl

    combo_state = FeatureState.from_config(
        {
            "service_id": "svc_rum_1",
            "log_period": 60,
            "sample_rate": 100,
            "logging_enabled": False,
            "rum_enabled": True,
            "fos_bucket": "bkt",
            "fos_endpoint": "us-east-1.object.fastlystorage.app",
            "scoring": {"enabled": True, "domain": "scorer.edgecompute.app", "request_secret": "sec123"},
            "log_fields": {"groups": []},
        }
    )
    combo_deliver_vcl = generate_consolidated_snippet(combo_state, "vcl_deliver")
    assert 'if (req.restarts == 1 && fastly.ff.visits_this_service == 0 && req.url.path != "/rum-beacon")' in (
        combo_deliver_vcl
    )
    assert "set resp.http.Set-Cookie =" not in combo_deliver_vcl
    raw_deliver_main = f"""
backend F_origin {{ .host = "origin.example.com"; .port = "443"; }}
sub vcl_deliver {{
{combo_deliver_vcl}
  return(deliver);
}}
"""
    assert _run_raw_falco_lint(raw_deliver_main).returncode == 0
    # Note: falco test (variable/all.go:REQ_RESTARTS) keeps ctx.Restarts == 0 in unit-test mode
    # without lookupOverride; adapt only the post-restart guard in the test harness so pass-2
    # cookie emission executes end-to-end after pass-1 restart.
    sim_deliver_vcl = combo_deliver_vcl.replace("req.restarts == 1", "req.restarts == 0", 1)
    main_rum_cookie = f"""
backend F_origin {{ .host = "origin.example.com"; .port = "443"; }}
sub vcl_deliver {{
{sim_deliver_vcl}
  return(deliver);
}}
"""
    test_rum_cookie = r"""
// @scope: deliver
sub test_rum_cid_preserves_origin_and_scorer_set_cookie_across_restart {
  set req.url = "/home";
  set req.http.X-Edge-Scoring-Pass = "1";
  set resp.status = 200;
  set resp.http.x-edge-sid = "cid_abc_123";
  set resp.http.Set-Cookie = "X-Session-State=rot_tok; Path=/; Secure; HttpOnly";
  testing.call_subroutine("vcl_deliver");
  assert.state(restart);

  // Pass 2: origin responds with its own Set-Cookie; scorer Set-Cookie + rum_cid are appended without clobbering!
  set resp.status = 200;
  set resp.http.Set-Cookie = "session_id=cust_999; Path=/; Secure; HttpOnly";
  testing.call_subroutine("vcl_deliver");
  assert.equal(setcookie.get_value_by_name(resp, "session_id"), "cust_999");
  assert.equal(setcookie.get_value_by_name(resp, "X-Session-State"), "rot_tok");
  assert.equal(setcookie.get_value_by_name(resp, "rum_cid"), "cid_abc_123");
}
"""
    res_cookie = _run_raw_falco_test(main_rum_cookie, test_rum_cookie)
    assert res_cookie.returncode == 0, f"falco test failed:\n{res_cookie.stdout}\n{res_cookie.stderr}"

    # 3. Declarative vcl_fetch must gate /js/rum.js caching on req.http.X-FOS-Request == "1"
    rum_state = FeatureState.from_config(
        {
            "service_id": "svc_rum_fetch",
            "log_period": 60,
            "sample_rate": 100,
            "logging_enabled": False,
            "rum_enabled": True,
            "fos_bucket": "bkt",
            "fos_endpoint": "us-east-1.object.fastlystorage.app",
            "log_fields": {"groups": []},
        }
    )
    fetch_snip = generate_consolidated_snippet(rum_state, "vcl_fetch")
    main_fetch = f"""
backend F_origin {{ .host = "origin.example.com"; .port = "443"; }}
sub vcl_fetch {{
{fetch_snip}
  return(deliver);
}}
"""
    test_fetch = r"""
// @scope: fetch
sub test_non_fos_post_to_rum_js_not_marked_public_cacheable {
  set req.method = "POST";
  set req.url = "/js/rum.js";
  set beresp.status = 200;
  set beresp.http.Cache-Control = "private, no-store";
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Cache-Control, "private, no-store");
  assert.is_notset(beresp.http.Surrogate-Key);
}

// @scope: fetch
sub test_fos_rum_js_marked_cacheable_on_200 {
  set req.method = "GET";
  set req.url = "/js/rum.js";
  set req.http.X-FOS-Request = "1";
  set beresp.status = 200;
  testing.call_subroutine("vcl_fetch");
  assert.equal(beresp.http.Cache-Control, "public, max-age=300");
  assert.equal(beresp.http.Surrogate-Key, "rum-js");
}
"""
    res_fetch = _run_raw_falco_test(main_fetch, test_fetch)
    assert res_fetch.returncode == 0, f"falco test failed:\n{res_fetch.stdout}\n{res_fetch.stderr}"


@requires_falco
@pytest.mark.security_regression
@pytest.mark.no_postgres
def test_sec15_vcl_backslash_quote_lexer_differential_blocked_and_regex_escape_allowed() -> None:
    from backend.core.catalog import (
        _CANONICAL_SYSTEM_FIELD_SPECS,
        check_vcl_quotes_and_parens,
        validate_custom_field_entry,
        validate_vcl_custom_condition,
        validate_vcl_log_expression,
    )
    from backend.core.fastly.client import validate_rum_custom_condition
    from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS, _CMCD_FIELD_NAMES
    from backend.provision.declarative.models import FeatureState
    from backend.provision.declarative.vcl_builder import build_rum_log_condition
    from backend.provision.rum_orchestrator import _RUM_CUSTOM_FIELDS, _RUM_FIELD_NAMES
    from backend.provision.session_scoring.orchestrate import _SCORING_CUSTOM_FIELDS, _SCORING_FIELD_NAMES

    # 1. Fastly VCL never treats \" as an escaped quote inside "..." — verify paren-breakout payloads fail
    breakout_payload = r'req.http.X-A == "\") || true || (req.http.X-B == "x" && req.http.X-C == "y\"'
    assert check_vcl_quotes_and_parens(breakout_payload) == "contains unbalanced parentheses"
    assert check_vcl_quotes_and_parens('req.http.Host == "ok"\\') == "must not end with a trailing backslash"
    assert (
        check_vcl_quotes_and_parens(r"req.http.Host == \"ok\"") == "must not contain backslashes outside double quotes"
    )
    assert validate_vcl_custom_condition(breakout_payload) is not None
    assert validate_rum_custom_condition(breakout_payload) is not None
    assert len(validate_vcl_log_expression(breakout_payload)) >= 1
    with pytest.raises(ValueError, match="unbalanced parentheses"):
        FeatureState(
            service_id="svc_breakout",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition=breakout_payload,
            fos_prefix="raw",
            fos_endpoint="us-east-1.object.fastlystorage.app",
        )
    with pytest.raises(ValueError, match="unbalanced parentheses"):
        FeatureState(
            service_id="svc_breakout",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="raw",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            rum_enabled=True,
            rum_custom_condition=breakout_payload,
        )

    # 2. Valid regex escape inside double quotes continues to pass validators, FeatureState, and falco lint
    valid_regex_cond = r'req.http.User-Agent !~ "bot\b" && (client.geo.country_code == "US")'
    assert validate_vcl_custom_condition(valid_regex_cond) is None
    assert validate_rum_custom_condition(valid_regex_cond) is None
    fs_ok = FeatureState(
        service_id="svc_valid_regex",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition=valid_regex_cond,
        fos_prefix="raw",
        fos_endpoint="us-east-1.object.fastlystorage.app",
        rum_enabled=True,
        rum_custom_condition=valid_regex_cond,
    )
    rum_cond_vcl = build_rum_log_condition(fs_ok)
    lint_vcl = f"""
sub vcl_log {{
  if ({rum_cond_vcl}) {{
    log "syslog svc_valid_regex rum :: ok";
  }}
}}
"""
    lint_res = _run_raw_falco_lint(lint_vcl)
    assert lint_res.returncode == 0, f"falco lint failed:\n{lint_res.stdout}\n{lint_res.stderr}"

    # 3. Canonical system field specs in catalog match all 41 RUM/Scoring/CMCD definitions 1-to-1
    expected_specs = {
        cf["name"]: (cf["collection_stage"], cf["vcl_log_expression"])
        for cf in (*_RUM_CUSTOM_FIELDS, *_SCORING_CUSTOM_FIELDS, *_CMCD_CUSTOM_FIELDS)
    }
    assert dict(_CANONICAL_SYSTEM_FIELD_SPECS) == expected_specs

    system_names = set(_CMCD_FIELD_NAMES) | set(_RUM_FIELD_NAMES) | set(_SCORING_FIELD_NAMES)
    # Partial toggle entries without stage/expression remain valid
    assert (
        validate_custom_field_entry(
            {"name": "cmcd_sid", "sql_type": "VARCHAR", "enabled": True}, [], system_names=system_names
        )
        == []
    )
    # Exact canonical system field entries remain valid
    for cf in (*_RUM_CUSTOM_FIELDS, *_SCORING_CUSTOM_FIELDS, *_CMCD_CUSTOM_FIELDS):
        assert validate_custom_field_entry(dict(cf), [], system_names=system_names) == []

    # Tampered collection_stage or vcl_log_expression on system fields is rejected
    assert validate_custom_field_entry(
        {"name": "rum_cid", "collection_stage": "deliver"}, [], system_names=system_names
    )
    assert validate_custom_field_entry(
        {"name": "rum_cid", "collection_stage": "beacon", "vcl_log_expression": "resp.http.Server"},
        [],
        system_names=system_names,
    )
    assert validate_custom_field_entry(
        {"name": "edge_score", "collection_stage": "edge"}, [], system_names=system_names
    )
    assert validate_custom_field_entry(
        {"name": "cmcd_sid", "vcl_log_expression": "req.http.X-Other"}, [], system_names=system_names
    )
