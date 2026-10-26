from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from backend.provision import generate_capture_vcl

# Falco is required when FALCO_REQUIRED=1 (set in CI). Locally, the suite
# skips with a clear reason if the binary isn't on PATH.
FALCO_INSTALLED = shutil.which("falco") is not None
FALCO_REQUIRED = os.environ.get("FALCO_REQUIRED") == "1"

if FALCO_REQUIRED and not FALCO_INSTALLED:
    raise RuntimeError(
        "FALCO_REQUIRED=1 but the falco binary is not on PATH. Install falco or unset FALCO_REQUIRED to allow skipping."
    )

_FASTLY_STUBS = Path(__file__).resolve().parent.parent / "fixtures" / "fastly_stubs.vcl"


@pytest.fixture
def run_falco_test():
    """Generate a temporary workspace, wrap the generated snippets in the
    shared Fastly stubs template, and run falco against the result.

    The wrapper VCL (backend declaration + vcl_recv / vcl_fetch / vcl_deliver
    subroutines, plus any proprietary-variable injection stubs) lives in
    tests/fixtures/fastly_stubs.vcl so every falco test inherits it.
    """
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

            result = subprocess.run(["falco", "test", main_vcl_path], cwd=tmpdir, capture_output=True, text=True)
            return result

    return _run


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_origin_field_always_log(run_falco_test):
    """Test that origin fields set to log 'all' are promoted correctly."""
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

    // Promoted to req for vcl_log
    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    // Stripped from client response
    assert.is_notset(resp.http.x-fos-origin-data:x_drew);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\\n{result.stdout}\\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_shield_interaction(run_falco_test):
    """Test that shield nodes keep origin data for edge nodes, but edge nodes strip it."""
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
    // Simulate being a shield node (x-is-cluster-fetch was set by edge)
    set req.http.x-is-cluster-fetch = "1";

    testing.call_subroutine("vcl_deliver");

    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    // Must NOT be stripped, so edge receives it
    assert.equal(resp.http.x-fos-origin-data:x_drew, "hello");
}

// @scope: deliver
sub test_edge_strips_header {
    set resp.http.x-fos-origin-data:x_drew = "hello";
    // Simulate being an edge node (no cluster fetch flag)
    unset req.http.x-is-cluster-fetch;

    testing.call_subroutine("vcl_deliver");

    assert.equal(req.http.x-fos-origin-data:x_drew, "hello");
    // Must be stripped before sending to client
    assert.is_notset(resp.http.x-fos-origin-data:x_drew);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\\n{result.stdout}\\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_group_l_telemetry_cleanup(run_falco_test):
    """Test that all internal telemetry headers are stripped before client delivery."""
    cfg = {"groups": ["L"], "custom_fields": []}

    test_code = """
// @scope: deliver
sub test_telemetry_headers_stripped {
    // Populate all the internal headers Group L uses
    set resp.http.x-of-ttfb = "100";
    set resp.http.x-of-ttlb = "200";
    set resp.http.x-of-status = "200";
    set resp.http.x-of-oip = "1.2.3.4";
    set resp.http.x-of-oretries = "0";

    // Ensure we are simulating an edge node delivery to a client
    unset req.http.x-is-cluster-fetch;

    testing.call_subroutine("vcl_deliver");

    // Assert none of these leak to the client
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
    assert.is_notset(resp.http.x-of-status);
    assert.is_notset(resp.http.x-of-oip);
    assert.is_notset(resp.http.x-of-oretries);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\\n{result.stdout}\\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_origin_timing_ttfb_ttlb(run_falco_test):
    """Test that TTFB and TTLB are correctly calculated and promoted."""
    cfg = {"groups": ["L"], "custom_fields": []}

    test_code = """
// @scope: fetch
sub test_fetch_records_ttfb {
    declare local var.start INTEGER;
    set var.start = std.atoi(time.elapsed.usec);
    set var.start -= 500;
    set req.http.x-of-start = "" + var.start;

    set beresp.status = 200;
    // Workaround: beresp.backend.ip and req.restarts might not be available or settable in Falco FETCH scope
    // so we just verify the rest of the capture logic

    testing.call_subroutine("vcl_fetch");

    // TTFB should be at least 500 (plus minor execution jitter)
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

    // Initialize headers to empty string to avoid null comparison issues in Falco
    set req.http.x-of-ttlb = "";
    set req.http.x-is-cluster-fetch = "";

    testing.call_subroutine("vcl_deliver");

    // TTLB should be at least 800 (plus minor execution jitter)
    assert.true(std.atoi(req.http.x-of-ttlb) >= 800);

    // Internal headers should be stripped from resp
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\\n{result.stdout}\\n{result.stderr}"


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")
def test_falco_shield_to_edge_timing_promotion(run_falco_test):
    """Test that timing metrics are promoted from shield node response to edge node request."""
    cfg = {"groups": ["L"], "custom_fields": []}

    test_code = """
// @scope: deliver
sub test_edge_promotes_metrics_from_shield_resp {
    // Simulate edge node receiving metrics in shield response
    set resp.http.x-of-ttfb = "500";
    set resp.http.x-of-ttlb = "800";
    set resp.http.x-of-status = "200";

    // Ensure req.http headers are "" (not null) so the VCL check (== "") passes in Falco
    set req.http.x-of-start = "";
    set req.http.x-of-ttfb = "";
    set req.http.x-of-ttlb = "";
    set req.http.x-of-status = "";

    // Simulate being the edge PoP delivering to client
    set req.http.x-is-cluster-fetch = "";

    testing.call_subroutine("vcl_deliver");

    // Metrics should have been promoted to req.http so the edge can log them
    assert.equal(req.http.x-of-ttfb, "500");
    assert.equal(req.http.x-of-ttlb, "800");
    assert.equal(req.http.x-of-status, "200");

    // And stripped from the final client response
    assert.is_notset(resp.http.x-of-ttfb);
    assert.is_notset(resp.http.x-of-ttlb);
}
"""
    result = run_falco_test(cfg, test_code)
    assert result.returncode == 0, f"Falco test failed:\\n{result.stdout}\\n{result.stderr}"


# ── collection_stage="deliver" — pure-Python emission tests ────────────────────
# These don't need falco — they assert on the exact strings the generator
# emits. Falco-based runtime tests would be a future enhancement once the
# scoring-service VCL stabilizes.


def test_deliver_stage_emits_capture_block_in_vcl_deliver():
    """A custom_field with collection_stage='deliver' must produce a guarded
    capture line in the vcl_deliver snippet that copies the response header
    into the req.http.x-fos-edge-data:* namespace."""
    cfg = {
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
    snippets = generate_capture_vcl(cfg)
    deliver = snippets.get("deliver", "")
    assert "# --- Custom Deliver Fields Start ---" in deliver
    assert 'if (resp.http.X-Edge-Score != "") {' in deliver
    assert "set req.http.x-fos-edge-data:edge_score = resp.http.X-Edge-Score;" in deliver


def test_deliver_stage_field_appears_in_log_format():
    """Deliver-stage fields read their vcl_log_expression directly in the log
    format, bypassing the x-fos-edge-data subfield indirection used by
    edge-stage fields. (Subfield writes in vcl_deliver are readable via the
    subfield syntax within VCL but don't show up in the log-format evaluator's
    snapshot, so deliver-stage fields would land as NULL otherwise.)"""
    from backend.core.log_fields import generate_log_format

    cfg = {
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
    fmt = generate_log_format(cfg)
    # Numeric field: single-level if() with compound AND condition.
    # Nested if(if(...) != "", ...) would be rejected by Fastly's
    # parser ("if() condition must be a simple expression, not a
    # function call"), so we flatten with `gate && value-is-numeric`.
    # 014: the second predicate is a strict numeric regex (not just
    # ``!= ""``) so a custom field value like ``"]"`` cannot break out
    # of the JSON log line.
    assert (
        '"edge_score":%{if('
        "fastly.ff.visits_this_service == 0 && "
        # Finding 012: literal-dot in the regex is double-escaped (``\\\\.``)
        # in the generated VCL so the Fastly regex engine sees ``\.`` (literal
        # dot), not ``.`` (wildcard). Pre-fix the wildcard let payloads like
        # ``1"1`` pass the numeric guard and JSON-inject into the log line.
        # Numeric fields are substr-clamped to 20 (a u64 is ≤20 chars) so they
        # don't reserve the 2000-char string default and starve the line budget.
        'substr(req.http.x-edge-score, 0, 20) ~ "^-?[0-9]+(\\\\.[0-9]+)?$"'
        ', substr(req.http.x-edge-score, 0, 20), "null")}V'
    ) in fmt
    # String field: json.escape wraps a single if() that gates on the
    # shield-vs-edge check and substr-clamps the value (016/017) so an
    # oversized custom field cannot push the line past Fastly's 16 KB
    # log-line limit. The substr limit is divided by 6 (worst-case
    # json.escape expansion factor) to prevent post-escape overflow.
    # Empty string at shield → JSON empty string.
    import re

    assert re.search(
        r'"edge_cookie_compliance":"%\{json\.escape\('
        r"if\(fastly\.ff\.visits_this_service == 0, "
        r'substr\(req\.http\.x-edge-cookie-compliance, 0, \d+\), ""\)'
        r'\)}V"',
        fmt,
    ), fmt


def test_deliver_stage_does_not_fire_when_disabled():
    """disabled=False fields must NOT appear in the generated VCL."""
    cfg = {
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
    snippets = generate_capture_vcl(cfg)
    assert "edge_score" not in snippets.get("deliver", "")


def test_deliver_stage_coexists_with_edge_and_origin():
    """Edge + origin + deliver fields all in one config — each lands in
    the right snippet."""
    cfg = {
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
    snippets = generate_capture_vcl(cfg)
    assert "f_edge" in snippets["recv"]
    assert "f_origin" in snippets["fetch"]
    assert "f_deliver" in snippets["deliver"]
    # 020: every custom field appears in the recv scrub block (per-name
    # ``unset req.http.x-fos-edge-data:<name>;`` lines) so a client
    # cannot pre-set a value for any known field. That means f_origin
    # SHOULD appear in recv — but only inside the scrub block, never as
    # a ``set`` assignment. (Use space-prefix to disambiguate ``set``
    # from ``unset`` since the latter ends in ``...set`` too.)
    assert "unset req.http.x-fos-edge-data:f_origin;" in snippets["recv"]
    assert "unset req.http.x-fos-origin-data:f_origin;" in snippets["recv"]
    assert "  set req.http.x-fos-edge-data:f_origin" not in snippets["recv"]
    # Same property for the other cross-stage fields.
    assert "  set req.http.x-fos-edge-data:f_deliver" not in snippets["fetch"]
    assert "  set beresp.http.x-fos-origin-data:f_edge" not in snippets["fetch"]
    assert "f_edge" not in snippets["deliver"]


def test_ip_capture_checks_fastly_client_ip():
    """The 'ip' edge capture checks if req.http.Fastly-Client-IP is set and uses
    it if present, falling back to client.ip."""
    snippets = generate_capture_vcl({"groups": ["A"]})
    assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in snippets["recv"]
    assert "set req.http.x-fos-edge-data:ip = client.ip;" in snippets["recv"]


def test_recv_reset_unsets_client_supplied_fastly_client_ip_at_edge():
    """A separate snippet (installed at priority -100) drops any client-supplied
    Fastly-Client-IP at the true edge so a spoofed value can't poison the
    captured client IP. With no shield secret it is gated on the first edge
    pass via fastly.ff.visits_this_service == 0 (the operator's own override
    runs after it, the priority-1 capture reads the result)."""
    snippets = generate_capture_vcl({"groups": ["A"]})
    reset = snippets["recv_reset"]
    assert "unset req.http.Fastly-Client-IP;" in reset
    assert "req.restarts == 0 && fastly.ff.visits_this_service == 0" in reset


def test_recv_reset_edge_gate_is_always_unforgeable_fastly_ff():
    """X-Edge-Shield-Auth as an edge-detection mechanism is retired. The reset's
    edge gate is ALWAYS the unforgeable first-edge-pass check
    (``req.restarts == 0 && fastly.ff.visits_this_service == 0``) — there is no
    shield-auth secret variant. The reset still drops a client-supplied
    Fastly-Client-IP at the true edge, and the retired header appears nowhere."""
    reset = generate_capture_vcl({"groups": ["A"]})["recv_reset"]
    assert "req.restarts == 0 && fastly.ff.visits_this_service == 0" in reset
    assert "unset req.http.Fastly-Client-IP;" in reset
    assert "X-Edge-Shield-Auth" not in reset


def test_recv_reset_absent_when_ip_not_edge_captured():
    """If the ip field is disabled there is nothing to protect, so the reset
    snippet is not emitted."""
    snippets = generate_capture_vcl({"groups": [], "field_overrides": {"ip": False}})
    assert "recv_reset" not in snippets


def test_cookie_session_edge_hash_not_emitted_by_default():
    """``cookie_session`` is opt-in (``default_off``): enabling its group (H)
    must NOT emit the edge SHA-256 cookie-hash capture. The raw Cookie header
    is never read/hashed at the edge unless the operator explicitly opts in."""
    snippets = generate_capture_vcl({"groups": ["H"]})
    assert "x-fos-edge-data:cookie_session" not in snippets["recv"]
    assert "digest.hash_sha256" not in snippets["recv"]


def test_cookie_session_edge_hash_emitted_on_explicit_opt_in():
    """With an explicit ``field_overrides['cookie_session'] = True`` opt-in the
    edge capture emits the first-present-cookie SHA-256 hashing expression into
    the recv snippet (privacy: hashed AT the edge, raw cookie never forwarded)."""
    snippets = generate_capture_vcl({"groups": ["H"], "field_overrides": {"cookie_session": True}})
    assert "set req.http.x-fos-edge-data:cookie_session = " in snippets["recv"]
    assert "digest.hash_sha256" in snippets["recv"]


def _if_conditions(expr: str) -> list[str]:
    """Return the CONDITION substring (first arg) of every ``if(`` in expr,
    paren-aware. Used to assert no ``if(`` is nested inside a condition — the
    Fastly-validate failure mode (``if`` is reserved in condition position)."""
    conds = []
    i = 0
    while True:
        j = expr.find("if(", i)
        if j == -1:
            break
        # walk from just after "if(" to the top-level comma that ends the condition
        depth = 1
        k = j + 3
        start = k
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
    """Regression (2026-07-06): Fastly's compiler rejects an ``if()`` function
    inside another ``if()``'s CONDITION. The cookie_session capture expression
    must keep every condition a plain ``subfield(...) != ""`` — the hash goes in
    the value branch. (falco / make vcl-test do NOT catch this; only real
    Fastly /validate does, so this is a structural guard.)"""
    from backend.provision.fastly_api import SESSION_COOKIE_ALLOWLIST, _session_cookie_hash_vcl

    expr = _session_cookie_hash_vcl()
    for cond in _if_conditions(expr):
        assert "if(" not in cond, f"if() nested in an if()-condition (Fastly rejects): {cond!r}"
    # empty is never hashed (no constant-hash poisoning) and one hash per cookie
    assert 'digest.hash_sha256("")' not in expr
    assert expr.count("digest.hash_sha256") == len(SESSION_COOKIE_ALLOWLIST)
    # priority order preserved (first allowlist entry appears before the last)
    first, last = SESSION_COOKIE_ALLOWLIST[0], SESSION_COOKIE_ALLOWLIST[-1]
    assert expr.index(f'"{first}"') < expr.index(f'"{last}"')


# ── Merged from tests/utils/test_vcl_utils.py & tests/provision/test_vcl_builder.py ──

import inspect
import logging
import os
import pathlib
from datetime import UTC, datetime
from unittest.mock import patch

FALCO_PATH = shutil.which("falco")
requires_falco = pytest.mark.skipif(not FALCO_INSTALLED, reason="falco executable not found in PATH")

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, get_cmcd_fields, reconcile_cmcd_custom_fields
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
from backend.utils.vcl_safety import (
    LintResult,
    VclSafetyError,
    validate_log_format_macro,
    validate_quoted_literal,
)
from backend.utils.vcl_validator import (
    MAX_REGEX_BYTES,
    RegexValidationError,
    lint_vcl,
    validate_recv_exclusion_regex_with_lint,
    validate_url_exclusion_regex,
)

from backend.config import path_within_dir
from backend.core.rollups._common import quote_path_list
from backend.provision import cmcd_fields as orch
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend
from backend.utils.sql_validator import escape_sql_literal
from backend.utils.vcl_utils import lint_log_format, log_format_to_vcl_log


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
    assert ok is False
    if substr:
        assert substr in msg.lower()
    if FALCO_PATH is not None:
        ok_falco, _ = lint_log_format(fmt)
        assert ok_falco is False


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

    import backend.utils.vcl_safety as vs

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
        if needle in p.read_text(encoding="utf-8") and p.name != "sql_validator.py"
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
    lf = {"groups": ["A", "B", "C"], "custom_fields": get_cmcd_fields(True), "field_overrides": {}}
    recv = generate_capture_vcl(lf, cmcd_enabled=True, cmcd_mode="query_string", cmcd_version=1)["recv"]
    for cf in get_cmcd_fields(True):
        name = cf["name"]
        key = name[len("cmcd_") :]
        extract = recv.find(f"set req.http.x-cmcd:{key} ")
        capture = recv.find(f"set req.http.x-fos-edge-data:{name} ")
        assert -1 < extract < capture

    off = generate_capture_vcl(lf, cmcd_enabled=False)["recv"]
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

    import backend.provision.declarative.generators as gen

    import backend.provision.fastly_api as fa
    import backend.routers.services.core as sc

    for mod in (gen, fa, sc):
        assert "raw/%Y-%m-%d" not in inspect.getsource(mod)
    assert "log_paths" in inspect.getsource(gen)

    for faro_ver in (None, "2.9.0"):
        st = FeatureState(
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
        shipped = generate_consolidated_snippet(st, "vcl_recv")
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
            service_name="test-service",
            domain_name="test-domain.global.ssl.fastly.net",
            origin_host="1.2.3.4",
            origin_port=80,
            use_ssl=False,
            token="fake-token",
        )
    assert res == {
        "service_id": "service123",
        "version": 1,
        "domain_name": "test-domain.global.ssl.fastly.net",
        "origin_host": "1.2.3.4",
    }
    assert len(calls) == 5

    # Override host + SSL
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
            service_name="test-service",
            domain_name="test-domain.global.ssl.fastly.net",
            origin_host="1.2.3.4",
            origin_port=443,
            use_ssl=True,
            token="fake-token",
            override_host="my-override.example.com",
        )
    backend_payload = next(c[2] for c in calls_ssl if c[1] == "/service/service123/version/1/backend")
    assert (
        backend_payload["override_host"]
        == backend_payload["ssl_sni_hostname"]
        == backend_payload["ssl_cert_hostname"]
        == "my-override.example.com"
    )

    # Missing ID / version
    with patch("backend.provision.fastly_api.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError, match="Failed to retrieve service ID"):
            deploy_remote_frontend("s", "d", "1.2.3.4", 80, False, "fake-token")
    seq = [{"id": "service123"}, {"service_id": "service123"}]
    with patch("backend.provision.fastly_api.fastly", side_effect=lambda *a, **k: seq.pop(0)):
        with pytest.raises(RuntimeError, match="Failed to retrieve version number"):
            deploy_remote_frontend("s", "d", "1.2.3.4", 80, False, "fake-token")

    # Delete variants: active version, no active version, 404 tolerated, 500 raised
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

"""Pin the runtime log-line size budget against Fastly's 16 KiB Deliver cap.

Background
----------
Fastly silently truncates emitted log lines past 16 KiB on Deliver services
(64 KiB on Compute). No error surfaces to the customer — corrupt JSON shows
up in DuckDB hours later. The template-size gate at
``provision.fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX`` only protects the static
template string; it cannot see per-request value sizes.

``check_log_line_budget`` exists to give the customer a config-time signal
before that truncation starts costing data. It compares
``estimate_log_line_bytes`` (an average) against a safe-max set at ~60% of
the cap to leave headroom for URL- / UA-heavy requests that routinely
inflate real lines 2-3x past the average.

What this file pins
-------------------
1. A small config produces no warning.
2. A config that lands between safe_max and deliver_max returns
   ``LOG_LINE_APPROACHING_LIMIT`` (severity "warn").
3. A config that exceeds the deliver_max returns ``LOG_LINE_TOO_LARGE``
   (severity "error").
4. Warning payload shape (frontend reads these specific keys).
5. The two router endpoints that consume the estimate also surface the
   warning, so the dashboard can render it.

Why these specific cases: the warn band is the genuinely useful one (the
error band only triggers for pathological configs), so this file leans
on the warn-band assertions to lock in the actionable behavior.
"""


import pytest
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st

from backend import config
from backend.core import field_registry as fr
from backend.core import log_fields as lf
from backend.core.field_registry import (
    BY_CODE,
    BY_GROUP,
    REGISTRY,
    LogField,
)
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    LOG_FIELD_CATALOG,
    PRESETS,
    generate_log_format,
)
from backend.main import app
from backend.utils import sql_validator as sql_ident


# ── Merged from tests/core/test_log_fields.py ──
def test_generate_log_format():
    """Verify that VCL log format strings are generated correctly."""
    # Test minimal preset
    cfg_minimal = {"groups": PRESETS["minimal"]["groups"], "field_overrides": {}}
    fmt_minimal = generate_log_format(cfg_minimal)

    assert isinstance(fmt_minimal, str)
    assert fmt_minimal.startswith("{")
    assert fmt_minimal.endswith("}")
    assert '"timestamp":' in fmt_minimal
    assert '"ip":' in fmt_minimal
    assert '"url":' not in fmt_minimal  # URL is Group A

    # Test standard preset
    cfg_standard = {"groups": PRESETS["standard"]["groups"], "field_overrides": {}}
    fmt_standard = generate_log_format(cfg_standard)

    assert '"url":' in fmt_standard
    assert '"pop":' in fmt_standard  # Group C
    assert '"country":' in fmt_standard  # Group D


def test_field_overrides():
    """Verify that specific fields can be overridden regardless of group."""
    cfg = {
        "groups": [],  # No groups
        "field_overrides": {"url": True, "country": True},
    }
    fmt = generate_log_format(cfg)

    assert '"url":' in fmt
    assert '"country":' in fmt
    assert '"pop":' not in fmt  # Pop is Group C, shouldn't be included


def test_default_off_field_registry_non_empty():
    """At least one field carries the opt-in ``default_off`` flag
    (``cookie_session``). Guards the mechanism from being silently dropped:
    if this frozenset empties out, the opt-in gate has regressed to nothing.
    """
    from backend.core.log_fields import DEFAULT_OFF_FIELD_IDS

    assert "cookie_session" in DEFAULT_OFF_FIELD_IDS


def test_default_off_field_excluded_from_group_by_default():
    """INVARIANT: a ``default_off`` field is NOT included by
    ``resolve_enabled_fields`` merely because its group is enabled.

    ``cookie_session`` lives in group H; enabling H (and the whole full
    preset) must NOT pull it in — capturing a per-session client identifier
    requires a deliberate opt-in.
    """
    from backend.core.log_fields import PRESETS, resolve_enabled_fields

    # Group H directly enabled.
    enabled = resolve_enabled_fields({"groups": ["H"], "field_overrides": {}})
    assert "cookie_session" not in enabled
    # Sibling group-H fields are still auto-enabled (only the opt-in one is held back).
    assert "ja3" in enabled and "ja4" in enabled and "tls_ciphers_sha" in enabled

    # And via the security / full presets (their default field_overrides).
    for preset_name in ("security", "full"):
        preset = PRESETS[preset_name]
        cfg = {"groups": preset["groups"], "field_overrides": preset.get("field_overrides", {})}
        assert "cookie_session" not in resolve_enabled_fields(cfg), preset_name


def test_default_off_field_included_on_explicit_opt_in():
    """INVARIANT: an explicit ``field_overrides[id] = True`` opt-in DOES
    include the ``default_off`` field — and it then reaches both the emitted
    log format and the edge-capture requirement set (end-to-end opt-in).
    """
    from backend.core.log_fields import get_required_edge_headers, resolve_enabled_fields

    cfg = {"groups": ["H"], "field_overrides": {"cookie_session": True}}
    assert "cookie_session" in resolve_enabled_fields(cfg)
    assert '"cookie_session":' in generate_log_format(cfg)
    # The edge must be asked to capture (hash) the cookie only on opt-in.
    assert "cookie_session" in get_required_edge_headers(cfg)

    # Opt-in works even when the field's group is NOT enabled (override wins
    # over group membership, same as any other field_override).
    cfg_no_group = {"groups": [], "field_overrides": {"cookie_session": True}}
    assert "cookie_session" in resolve_enabled_fields(cfg_no_group)


def test_default_off_field_override_false_is_noop():
    """Explicitly disabling an already-off ``default_off`` field is a no-op
    (it was never in the set); the config stays valid and cookie_session
    remains absent."""
    from backend.core.log_fields import resolve_enabled_fields

    enabled = resolve_enabled_fields({"groups": ["H"], "field_overrides": {"cookie_session": False}})
    assert "cookie_session" not in enabled


def test_custom_origin_field_propagated_to_cluster():
    """Regression: origin custom fields must not be stripped when serving to cluster nodes.

    When a shield node serves to an edge node (x-is-cluster-fetch=1), it must keep
    resp.http.x-fos-origin-data:{name} in the response so the edge can cache and log it.
    Previously the deliver snippet always ran `unset resp.http.x-fos-origin-data:{name}`,
    causing the edge to log null for every origin custom field.
    """
    cfg = {
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
    snippets = generate_capture_vcl(cfg)
    deliver = snippets["deliver"]

    # The unset must live inside the cluster-fetch guard block (indented)
    expected_guard_block = (
        'if (req.http.x-is-cluster-fetch != "1") {\n  unset resp.http.x-fos-origin-data:bereq_drew;\n}'
    )
    assert expected_guard_block in deliver, "unset must be inside the x-is-cluster-fetch guard"

    # The unconditional bare unset (old bug) must NOT appear at column 0
    assert "\nunset resp.http.x-fos-origin-data:bereq_drew;" not in deliver, (
        "unset must not appear at the top level (unindented) — it must be inside the guard"
    )


def test_custom_edge_field_not_in_deliver():
    """Edge custom fields should not appear in the deliver snippet at all."""
    cfg = {
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
    snippets = generate_capture_vcl(cfg)
    assert "edge_randomint" in snippets["recv"]
    assert "deliver" not in snippets or "edge_randomint" not in snippets.get("deliver", "")


def test_numeric_custom_fields_do_not_starve_line_budget():
    """REGRESSION: numeric custom fields defaulted to a 2000-char substr
    reservation. Greedy budget allocation runs alphabetically, so several
    numeric fields each grabbing 2000 exhausted the 16 KiB line budget before
    the alphabetically-last fields, which then got substr(..., 0, 0) and
    always logged null (e.g. edge_score_rtt_us + edge_sid after the latency
    fields landed). Numeric fields must clamp to a short length so later
    fields keep a non-zero substr.
    """
    import re

    cfg = {
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
        # An alphabetically-last STRING field that previously got starved to 0.
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
    fmt = generate_log_format(cfg)

    # Every numeric field clamps to <= 20 (not 2000).
    num_lens = [int(m) for m in re.findall(r"x-edge-score:n\d+, 0, (\d+)\)", fmt)]
    assert num_lens and all(n <= 20 for n in num_lens), num_lens
    # The alphabetically-last string field is NOT starved to a zero-length substr.
    tail = re.search(r"x-edge-score:tail, 0, (\d+)\)", fmt)
    assert tail and int(tail.group(1)) > 0, "trailing field was starved to substr(...,0,0)"


def test_cmcd_string_fields_do_not_starve_scoring_budget():
    """REGRESSION: CMCD string fields (cmcd_cid, cmcd_ot, cmcd_sf, cmcd_sid,
    cmcd_st) defaulted to a 2000-byte substr reservation when no byte_limit
    was set. Because custom fields are processed alphabetically, these five
    cmcd_* fields consumed 10,000 bytes before the nine edge_* scoring fields,
    which then got substr(..., 0, 0) → null.

    With the bytes_estimate-aware default, a field declaring bytes_estimate=40
    gets min(max(40*4, 40), 2000) = 160 instead of 2000. This mirrors the
    numeric-field clamp but applies to string fields with a declared estimate.
    """
    import re

    from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS

    from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS

    cfg = {
        "groups": PRESETS["standard"]["groups"],
        "custom_fields": [dict(cf) for cf in _CMCD_CUSTOM_FIELDS] + [dict(cf) for cf in _SCORING_CUSTOM_FIELDS],
    }
    fmt = generate_log_format(cfg)

    edge_sid_match = re.search(r"x-edge-score:sid, 0, (\d+)\)", fmt)
    assert edge_sid_match and int(edge_sid_match.group(1)) > 0, "edge_sid was starved to substr(..., 0, 0)"

    rtt_match = re.search(r"x-edge-score:rtt, 0, (\d+)\)", fmt)
    assert rtt_match and int(rtt_match.group(1)) > 0, "edge_score_rtt_us was starved to substr(..., 0, 0)"

    cmcd_sid_match = re.search(r"x-fos-edge-data:cmcd_sid, 0, (\d+)\)", fmt)
    assert cmcd_sid_match, "cmcd_sid should appear in the format"
    cmcd_sid_limit = int(cmcd_sid_match.group(1))
    assert cmcd_sid_limit < 2000, f"cmcd_sid should use bytes_estimate-derived limit, not 2000 (got {cmcd_sid_limit})"


def _valid_field(**overrides) -> dict:
    """Minimal valid custom-field dict for validation tests."""
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


def test_validate_custom_field_returns_empty_for_minimum_valid_dict():
    """A minimal valid dict produces no errors. Pinned because the
    14-rule validator is order-sensitive and any new rule must not
    block this golden-path baseline."""
    from backend.core.log_fields import validate_custom_field

    assert validate_custom_field(_valid_field(), []) == []


def test_validate_custom_field_flags_missing_required_keys_and_short_circuits():
    """Missing keys yield "Missing required field: 'X'" errors AND
    short-circuit further validation (other errors would be
    confusing while keys are absent). Pinned because losing the
    short-circuit cascades into N follow-on errors per missing key."""
    from backend.core.log_fields import validate_custom_field

    field = {"name": "x"}  # only one required key present
    errors = validate_custom_field(field, [])

    # Errors include the missing required keys
    assert any("Missing required field" in e for e in errors)
    # But NOT subsequent rule errors (e.g. label-length, since label is absent → required-key error caught it)
    assert not any("label" in e and "characters" in e for e in errors)


def test_validate_custom_field_rejects_invalid_name_regex():
    """Names must match `^[a-z][a-z0-9_]{0,47}$`. Pinned because
    losing this would allow SQL identifiers with dashes/uppercase
    that need quoting everywhere downstream."""
    from backend.core.log_fields import validate_custom_field

    # Starts with a digit
    errors = validate_custom_field(_valid_field(name="1bad"), [])
    assert any("lowercase alphanumeric" in e for e in errors)

    # Contains a hyphen
    errors = validate_custom_field(_valid_field(name="bad-name"), [])
    assert any("lowercase alphanumeric" in e for e in errors)


def test_validate_custom_field_rejects_duckdb_reserved_word():
    """DuckDB reserved words can't be column names. Pinned because
    losing this would error at CREATE TABLE time with an opaque
    parser error."""
    from backend.core.log_fields import validate_custom_field

    # 'select' is a SQL reserved word
    errors = validate_custom_field(_valid_field(name="select"), [])
    assert any("reserved word" in e for e in errors)


def test_validate_custom_field_rejects_builtin_field_collision():
    """Custom field names can't shadow built-in field IDs (timestamp,
    ip, etc). Pinned because losing this would cause downstream
    code that reads the built-in column to get the custom value."""
    from backend.core.log_fields import validate_custom_field

    # 'timestamp' is a built-in
    errors = validate_custom_field(_valid_field(name="timestamp"), [])
    assert any("already a built-in" in e for e in errors)


def test_validate_custom_field_rejects_duplicate_custom_name():
    """Two custom fields can't share a name within the same service.
    Pinned because losing this would let the wizard overwrite
    existing fields silently."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(name="my_field"), ["my_field"])
    assert any("already exists" in e for e in errors)


def test_validate_custom_field_enforces_label_length_1_to_80():
    """`label` must be 1–80 chars. Pinned because the FE allocates
    a fixed-width slot in the column header and longer labels
    would break the layout."""
    from backend.core.log_fields import validate_custom_field

    # Empty label
    assert any("label" in e for e in validate_custom_field(_valid_field(label=""), []))
    # Too long label
    assert any("label" in e for e in validate_custom_field(_valid_field(label="x" * 81), []))


def test_validate_custom_field_enforces_description_max_500_chars():
    """`description` is optional but capped at 500 chars. Pinned
    because the field-list panel renders the description inline
    and longer would overflow."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(description="x" * 501), [])
    assert any("description" in e for e in errors)


def test_validate_custom_field_vcl_expression_must_be_non_empty():
    """`vcl_log_expression` blank → error. Pinned because deploying
    an empty VCL field would fail at Fastly upload."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(vcl_log_expression=""), [])
    assert any("vcl_log_expression" in e and "empty" in e for e in errors)


def test_validate_custom_field_vcl_expression_max_512_chars():
    """`vcl_log_expression` capped at 512 chars. Pinned because
    longer expressions blow past Fastly's per-snippet character
    budget."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(vcl_log_expression="x" * 513), [])
    assert any("vcl_log_expression" in e and "512" in e for e in errors)


def test_validate_custom_field_vcl_rejects_newlines_semicolons_comments():
    """VCL injection protection: no raw newlines, semicolons, or
    `//` / `/*` / `#` comments. Pinned because losing these would
    let user input alter the surrounding VCL snippet structure."""
    from backend.core.log_fields import validate_custom_field

    # Newline
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url\n"), [])
    assert any("newlines" in e for e in errors)

    # Semicolon
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url; set req.x = 1"), [])
    assert any("semicolons" in e for e in errors)

    # // comment
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url // hack"), [])
    assert any("comments" in e for e in errors)

    # # comment
    errors = validate_custom_field(_valid_field(vcl_log_expression="req.url # hack"), [])
    assert any("comments" in e for e in errors)


def test_validate_custom_field_rejects_unknown_duckdb_type():
    """`duckdb_type` must be a known type. Pinned because losing
    this would error at CREATE TABLE with an opaque parser error."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(duckdb_type="QUANTUM"), [])
    assert any("duckdb_type" in e for e in errors)


def test_validate_custom_field_rejects_unknown_value_type():
    """`value_type` enum: string/numeric/boolean/ip/url. Pinned
    because the FE renders different input widgets per type."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(value_type="unknown"), [])
    assert any("value_type" in e for e in errors)


def test_validate_custom_field_rejects_incompatible_duckdb_value_type_pair():
    """E.g. duckdb_type=INTEGER with value_type=ip → incompatible.
    Pinned because losing the pair check would let users create
    schemas that error at first INSERT."""
    from backend.core.log_fields import validate_custom_field

    # INTEGER + 'ip' is not compatible
    errors = validate_custom_field(
        _valid_field(duckdb_type="INTEGER", value_type="ip"),
        [],
    )
    assert any("not compatible" in e for e in errors)


def test_validate_custom_field_bytes_estimate_must_be_int_in_range_1_to_1024():
    """`bytes_estimate` must be int between 1 and 1024. Pinned
    because the wizard uses this to project log_format size — a
    bogus value would silently mislead the byte-budget UI."""
    from backend.core.log_fields import validate_custom_field

    # Zero
    errors = validate_custom_field(_valid_field(bytes_estimate=0), [])
    assert any("bytes_estimate" in e for e in errors)
    # Over the cap
    errors = validate_custom_field(_valid_field(bytes_estimate=2000), [])
    assert any("bytes_estimate" in e for e in errors)
    # Non-int
    errors = validate_custom_field(_valid_field(bytes_estimate="not-a-number"), [])
    assert any("bytes_estimate" in e for e in errors)


def test_validate_custom_field_collection_stage_must_be_edge_or_origin():
    """`collection_stage` enum: edge|origin. Pinned because the VCL
    generator routes the field into different subroutines based on
    this — an unknown value would silently drop the field."""
    from backend.core.log_fields import validate_custom_field

    errors = validate_custom_field(_valid_field(collection_stage="fetch"), [])
    assert any("collection_stage" in e for e in errors)


def test_validate_custom_field_warns_on_suspiciously_low_bytes_estimate():
    """`bytes_estimate` below the key+quotes+colon overhead emits
    a WARN: prefix (not a hard error). Pinned because the routes
    split WARN: lines into warnings (yellow) vs errors (red)."""
    from backend.core.log_fields import validate_custom_field

    # name is "my_field" = 8 chars; min_bytes = 8 + 5 = 13
    errors = validate_custom_field(_valid_field(bytes_estimate=5), [])
    # Should yield a WARN: line (not a hard error blocking save)
    warnings = [e for e in errors if e.startswith("WARN:")]
    assert len(warnings) >= 1


def test_client_ip_falls_back_to_socket_ip_when_capture_did_not_run():
    """Requests answered before the capture snippet runs (redirects) set neither the edge-data ip
    nor Fastly-Client-IP; the log line must still carry the socket IP instead of an empty string."""
    fmt = generate_log_format({"schema_version": 2, "groups": []})
    assert (
        'if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, '
        'if(req.http.Fastly-Client-IP != "", req.http.Fastly-Client-IP, client.ip))' in fmt
    )


# ── Merged from tests/core/test_log_line_budget.py ──
def _make_custom_fields(count: int, bytes_each: int) -> list[dict]:
    """Build saved-shape custom field records with a target bytes_estimate.

    Used to drive estimate_log_line_bytes into the warn or error band
    without depending on which built-in groups happen to be enabled.
    """
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


def test_constants_pin_the_fastly_caps():
    """If anyone moves the constants we want a loud failure here.

    16 KiB is the Fastly Deliver cap straight from
    https://docs.fastly.com/products/network-services-resource-limits and
    must not drift. The safe-max is a tunable headroom (~60%); the test
    pins the ratio band rather than the exact integer so a deliberate
    tweak (e.g., to 55% or 65%) doesn't require touching this assertion.
    """
    assert lf.FASTLY_LOG_LINE_DELIVER_MAX == 16 * 1024
    # Safe max must leave material headroom for value-size variance but
    # not so much that even fat configs never warn.
    assert 0.40 < lf.FASTLY_LOG_LINE_SAFE_MAX / lf.FASTLY_LOG_LINE_DELIVER_MAX < 0.80


def test_small_config_no_warning():
    """A minimal config has tons of slack — no warning, no false alarm."""
    cfg = {"groups": ["A"], "field_overrides": {}}
    estimate = lf.estimate_log_line_bytes(cfg)
    assert estimate < lf.FASTLY_LOG_LINE_SAFE_MAX
    assert lf.check_log_line_budget(cfg) is None


def test_warn_band_returns_approaching_limit():
    """When estimate crosses safe_max but stays under deliver_max, the warn
    band fires. ~20 fat custom fields land around ~11.7 KB — squarely in
    the warn zone (9830 < x < 16384).
    """
    cfg = {
        "groups": ["A", "B", "C", "D"],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=500),
    }
    estimate = lf.estimate_log_line_bytes(cfg)
    assert lf.FASTLY_LOG_LINE_SAFE_MAX <= estimate < lf.FASTLY_LOG_LINE_DELIVER_MAX, (
        f"test fixture drifted; estimate {estimate} is no longer in the warn band"
    )

    warning = lf.check_log_line_budget(cfg)
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"
    assert warning["severity"] == "warn"
    assert warning["estimate_bytes"] == estimate
    assert warning["deliver_max_bytes"] == lf.FASTLY_LOG_LINE_DELIVER_MAX
    assert warning["safe_max_bytes"] == lf.FASTLY_LOG_LINE_SAFE_MAX
    # The message must surface the actual size and the cap so the user
    # knows how much to trim. CustomFieldDrawer reads `message` verbatim.
    assert str(estimate) in warning["message"]
    assert "16" in warning["message"]


def test_error_band_returns_too_large():
    """Past the hard cap the warning escalates to error severity. ~20
    1-KiB custom fields land at ~20.7 KB — well past the 16 KiB cap.
    """
    cfg = {
        "groups": [],
        "field_overrides": {},
        "custom_fields": _make_custom_fields(count=20, bytes_each=1024),
    }
    estimate = lf.estimate_log_line_bytes(cfg)
    assert estimate >= lf.FASTLY_LOG_LINE_DELIVER_MAX

    warning = lf.check_log_line_budget(cfg)
    assert warning is not None
    assert warning["code"] == "LOG_LINE_TOO_LARGE"
    assert warning["severity"] == "error"
    assert warning["estimate_bytes"] == estimate
    # The error message must mention "truncate" / "16" so users see the
    # operational consequence (data loss), not just the threshold.
    assert "truncate" in warning["message"].lower()
    assert "16" in warning["message"]


def test_get_log_fields_endpoint_surfaces_warning(tmp_path, monkeypatch):
    """The GET /api/services/{id}/log-fields response must include
    ``line_budget_warning`` so the frontend can render it. Without this
    field on the wire, the helper would be dead code.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_get"
    cfg = {
        "service_id": svc_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "field_overrides": {},
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        },
    }
    config.save_config(svc_id, cfg)

    client = TestClient(app)
    resp = client.get(f"/api/services/{svc_id}/log-fields")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "line_budget_warning" in body
    warning = body["line_budget_warning"]
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"


def test_post_log_fields_endpoint_surfaces_warning(tmp_path, monkeypatch):
    """The POST endpoint must also surface the warning so the save flow
    can show the same inline banner as the load flow.

    Falco/state_sync are patched to keep the test offline.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_post"
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "log_fields": {"schema_version": 2, "groups": ["A"]},
        },
    )

    payload = {
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B", "C", "D"],
            "custom_fields": _make_custom_fields(count=20, bytes_each=500),
        }
    }
    client = TestClient(app)
    # export_admin_state hits state_sync internals; we don't need its
    # side effects for this assertion.
    with patch("backend.state_sync.export_admin_state", return_value=None):
        resp = client.post(f"/api/services/{svc_id}/log-fields", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "line_budget_warning" in body
    warning = body["line_budget_warning"]
    assert warning is not None
    assert warning["code"] == "LOG_LINE_APPROACHING_LIMIT"


def test_post_no_change_path_does_not_crash(tmp_path, monkeypatch):
    """When the POST handler short-circuits on a no-op change, it returns
    early WITHOUT the warning key. That branch must not crash and the
    response shape is allowed to omit ``line_budget_warning``.
    Locking this in prevents an over-eager refactor from changing the
    return shape and breaking the no-op contract.
    """
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_log_budget_noop"
    initial = {
        "service_id": svc_id,
        "log_fields": {"schema_version": 2, "groups": ["A"]},
    }
    config.save_config(svc_id, initial)
    # Pre-compute hash so the next save is recognized as a no-op.
    cfg = config.load_config(svc_id)
    cfg["log_fields"]["format_hash"] = lf.format_hash(cfg["log_fields"])
    config.save_config(svc_id, cfg)

    client = TestClient(app)
    resp = client.post(
        f"/api/services/{svc_id}/log-fields",
        json={"log_fields": cfg["log_fields"]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True


def test_generate_log_format_clamps_to_deliver_max_via_budget():
    """Regression for F013 (audit run 7ba15352).

    The prior generate_log_format() applied per-field substr() limits in
    isolation: url(2000) + ua(1000) + referer(1000) + 7 custom fields *
    2000 = ~18 KB substr cap budget, which let a single request's
    interpolated log line punch through Fastly's 16 KiB silent-truncation
    cap. Fastly drops the truncated mid-string line entirely, blinding
    the audit trail (repudiation).

    The aggregate-budget guard tracks a running ``budget`` and clamps
    each variable-length field's substr cap to ``min(configured, budget)``.
    We verify by enumerating all substr(*, 0, N) caps in the generated
    VCL and asserting their sum stays below FASTLY_LOG_LINE_DELIVER_MAX.
    """
    import re

    cfg = {
        # Force the worst-case shape from the audit: url + ua + referer
        # at max, plus 10 huge custom fields. Without the budget, sum
        # would be 2000 + 1000 + 1000 + 10*2000 = 24 KB.
        "field_limits": {"url": 4000, "ua": 4000, "referer": 4000},
        "custom_fields": _make_custom_fields(10, bytes_each=4000),
        # Per-field byte_limit overrides (mirror real config shape).
        "groups": {"core": True, "a": True},
    }
    # Attach byte_limit so each custom field requests 4000 bytes.
    for cf in cfg["custom_fields"]:
        cf["byte_limit"] = 4000

    fmt = lf.generate_log_format(cfg)

    caps = [int(m) for m in re.findall(r"substr\([^)]*, *0, *(\d+)\)", fmt)]
    assert caps, f"expected substr() caps in generated format, got: {fmt[:300]}…"
    total = sum(caps)
    assert total <= lf.FASTLY_LOG_LINE_DELIVER_MAX, (
        f"aggregate substr-cap budget {total} exceeds FASTLY_LOG_LINE_DELIVER_MAX "
        f"{lf.FASTLY_LOG_LINE_DELIVER_MAX} — F013 budget clamp regressed"
    )


# ── Merged from tests/core/test_field_registry.py ──
def test_registry_imports_and_is_non_empty() -> None:
    """The module loads at import time and exposes a populated REGISTRY."""
    assert isinstance(REGISTRY, tuple)
    assert len(REGISTRY) > 0
    assert all(isinstance(f, LogField) for f in REGISTRY)


def test_registry_is_frozen() -> None:
    """Instances are immutable — accidental mutation is a TypeError, not a silent change."""
    sample = REGISTRY[0]
    with pytest.raises((AttributeError, TypeError)):
        sample.code = "mutated"  # type: ignore[misc]


def test_by_code_is_readonly_view() -> None:
    """`BY_CODE` is a `MappingProxyType` — direct mutation raises."""
    with pytest.raises(TypeError):
        BY_CODE["bogus"] = REGISTRY[0]  # type: ignore[index]


def test_every_field_round_trips_through_lookup() -> None:
    """`get(code).code == code` for every field. KeyError on bogus codes."""
    for f in REGISTRY:
        assert fr.get(f.code) is f
        assert fr.try_get(f.code) is f

    assert fr.try_get("__not_a_real_code__") is None
    with pytest.raises(KeyError):
        fr.get("__not_a_real_code__")


def test_field_codes_are_unique() -> None:
    """No duplicate codes — the BY_CODE map invariant."""
    codes = [f.code for f in REGISTRY]
    assert len(codes) == len(set(codes))


def test_registry_codes_match_log_fields() -> None:
    """The registry covers every code in the legacy LOG_FIELD_CATALOG.

    This is the parity guarantee that lets callers migrate one-at-a-time:
    while both views are live, they must agree on the set of codes.
    """
    legacy = {entry["id"] for entry in LOG_FIELD_CATALOG}
    new = set(BY_CODE)
    missing_from_new = legacy - new
    missing_from_legacy = new - legacy
    assert not missing_from_new, f"registry is missing legacy codes: {missing_from_new}"
    assert not missing_from_legacy, f"registry has extra codes not in legacy: {missing_from_legacy}"


def test_registry_codes_match_builtin_set() -> None:
    """`_BUILTIN_FIELD_NAMES` (the custom-field validation gate) and the
    registry agree on what counts as a built-in name.

    A divergence here would let a user create a custom field whose name
    shadows a built-in one — exactly the failure that `validate_custom_field`
    is supposed to catch.
    """
    assert set(BY_CODE) == set(_BUILTIN_FIELD_NAMES)


@pytest.mark.security_regression
def test_wire_order_matches_legacy_emission_order() -> None:
    """Emission order in REGISTRY is byte-identical to the legacy catalog's VCL emission order.

    The Rust scorer reads positional JSON keys; reordering REGISTRY rows
    without coordinating with `compute/` silently breaks scorer parity.
    This test is the boot-time gate that fails loudly in CI.
    """
    legacy_emission_order = tuple(entry["id"] for entry in LOG_FIELD_CATALOG if entry["vcl"] is not None)
    assert tuple(f.code for f in REGISTRY if not f.is_derived) == legacy_emission_order


def test_default_off_codes_match_legacy_catalog() -> None:
    """The registry's ``default_off`` flags match ``log_fields.DEFAULT_OFF_FIELD_IDS``."""
    from backend.core.log_fields import DEFAULT_OFF_FIELD_IDS

    legacy = {entry["id"] for entry in LOG_FIELD_CATALOG if entry.get("default_off")}
    default_off_codes = {f.code for f in REGISTRY if f.default_off}
    assert default_off_codes == legacy
    assert default_off_codes == DEFAULT_OFF_FIELD_IDS
    assert "cookie_session" in default_off_codes


def test_by_group_partitions_registry() -> None:
    """Every field appears in exactly one group bucket. Together they cover REGISTRY."""
    seen: set[str] = set()
    for group, fields in BY_GROUP.items():
        for f in fields:
            assert f.group is group
            assert f.code not in seen, f"{f.code} appears in multiple group buckets"
            seen.add(f.code)
    assert seen == set(BY_CODE)


def test_derived_fields_have_no_vcl() -> None:
    """`is_derived` is exactly the `vcl is None` predicate."""
    for f in REGISTRY:
        assert f.is_derived is (f.vcl is None)


@given(code=st.sampled_from(sorted(f.code for f in REGISTRY)))
def test_hypothesis_every_code_lookup_succeeds(code: str) -> None:
    """For every known code, `get` returns a field whose `.code` matches input."""
    f = fr.get(code)
    assert f.code == code
    # Derived properties never raise for any known field.
    _ = f.is_derived


def test_required_by_is_tuple_of_strings() -> None:
    """Each field's `required_by` is a tuple of insight-id-shaped strings.

    Referential integrity between `required_by` and `INSIGHT_DEFINITIONS`
    is NOT asserted: the legacy catalog already contains references to
    insight ids that don't live in INSIGHT_DEFINITIONS (e.g.
    `image_optimization_opportunities`), and fixing that data drift is
    out of scope for the registry scaffolding. When the migration moves
    insight definitions into the registry too, this test should be
    replaced with a referential-integrity check.
    """
    for f in REGISTRY:
        assert isinstance(f.required_by, tuple)
        for insight_id in f.required_by:
            assert isinstance(insight_id, str) and insight_id  # non-empty


def test_insight_required_fields_exist_in_registry() -> None:
    """Each insight's `required_fields` names a real field code."""
    from backend.core.log_fields import INSIGHT_DEFINITIONS

    for insight in INSIGHT_DEFINITIONS:
        for code in insight["required_fields"]:
            assert fr.try_get(code) is not None, f"insight {insight['id']!r} requires unknown field {code!r}"


_RE_EXPORTED_HELPERS = (
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
)

_RE_EXPORTED_CONSTANTS = (
    "LOG_FIELD_CATALOG",
    "GROUP_INFO",
    "PRESETS",
    "INSIGHT_DEFINITIONS",
    "VALID_NAME_RE",
)


@pytest.mark.parametrize("name", _RE_EXPORTED_HELPERS)
def test_helper_is_same_object_as_log_fields(name: str) -> None:
    """`field_registry.HELPER is log_fields.HELPER` for every re-exported helper."""
    from backend.core import log_fields as lf

    assert hasattr(fr, name), f"field_registry is missing helper {name!r}"
    assert hasattr(lf, name), f"log_fields is missing helper {name!r}"
    assert getattr(fr, name) is getattr(lf, name), (
        f"{name!r} on field_registry is not the same object as on log_fields — "
        "re-export drift will break the @patch('backend.core.log_fields.X') pattern"
    )


@pytest.mark.parametrize("name", _RE_EXPORTED_CONSTANTS)
def test_constant_is_same_object_as_log_fields(name: str) -> None:
    """`field_registry.CONSTANT is log_fields.CONSTANT` for every re-exported constant."""
    from backend.core import log_fields as lf

    assert hasattr(fr, name), f"field_registry is missing constant {name!r}"
    assert hasattr(lf, name), f"log_fields is missing constant {name!r}"
    assert getattr(fr, name) is getattr(lf, name), (
        f"{name!r} on field_registry is not the same object as on log_fields — "
        "callers reading through the registry will miss mutations on log_fields"
    )


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


def test_rejects_non_strings() -> None:
    assert sql_ident.is_safe_ident(None) is False  # type: ignore[arg-type]
    assert sql_ident.is_safe_ident(5) is False  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        sql_ident.require_safe_ident(None, "bad ident")  # type: ignore[arg-type]


def test_error_message_embeds_label_and_repr() -> None:
    with pytest.raises(ValueError) as exc:
        sql_ident.require_safe_ident("a-b", "invalid ducklake alias")
    assert str(exc.value) == "invalid ducklake alias: 'a-b'"


def test_bare_identifier_regex_has_one_owner() -> None:
    """No module besides sql_ident may declare the bare-identifier regex literal."""
    needles = ("[A-Za-z_][A-Za-z0-9_]*",)
    offenders = []
    for path in BACKEND.rglob("*.py"):
        if path.name in {"sql_ident.py", "sql_validator.py"}:
            continue
        text = path.read_text()
        # pg_connection's unanchored token matcher and clickhouse param-name
        # matcher are lexers, not identifier validators.
        if path.name in {"pg_connection.py", "clickhouse_client.py"}:
            continue
        if any(n in text for n in needles):
            offenders.append(str(path.relative_to(BACKEND)))
    assert offenders == [], f"use backend.core.sql_ident instead of a private identifier regex: {offenders}"
