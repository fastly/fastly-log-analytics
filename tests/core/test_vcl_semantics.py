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
def test_falco_origin_field_miss_pass_only(run_falco_test):
    """Test that origin fields set to log 'miss_pass' are ONLY promoted on cache miss.
    Note: Falco currently has a bug injecting 'fastly_info.state' for !~ regex checks,
    so we skip this test until Falco is updated.
    """
    pytest.skip("Falco testing.inject_variable('fastly_info.state') does not bind correctly for !~ operator.")


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
    assert "# --- Custom Deliver Fields ---" in deliver
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
        'substr(req.http.x-edge-score, 0, 2000) ~ "^-?[0-9]+(\\.[0-9]+)?$"'
        ', substr(req.http.x-edge-score, 0, 2000), "null")}V'
    ) in fmt
    # String field: json.escape wraps a single if() that gates on the
    # shield-vs-edge check and substr-clamps the value (016) so an
    # oversized custom field cannot push the line past Fastly's 16 KB
    # log-line limit. Empty string at shield → JSON empty string.
    assert (
        '"edge_cookie_compliance":"%{json.escape('
        "if(fastly.ff.visits_this_service == 0, "
        'substr(req.http.x-edge-cookie-compliance, 0, 2000), "")'
        ')}V"'
    ) in fmt


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
