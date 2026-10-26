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
from backend.provision.declarative.generators import generate_consolidated_snippet
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
