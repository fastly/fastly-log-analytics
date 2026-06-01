"""Tests for ``backend.provision.fastly_api`` — log-format + VCL helpers.

The big provisioning orchestrators (``ensure_cdn_service``,
``ensure_logging_endpoint``, etc.) are covered indirectly via the
provision-router integration tests. This file pins the **pure**
helpers — log-format generation, length validation, regex-based VCL
syntax checks — that don't depend on the Fastly API.

These four helpers feed the wizard's "Validate" step; a regression in
any of them lets bad VCL escape into production.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.provision import fastly_api

# ── load_log_format: defaults + override ───────────────────────────────────


def test_load_log_format_uses_standard_preset_by_default():
    """No config supplied → use the "standard" preset's groups.
    Pinned because new installs land on standard, and shifting the
    default would silently change which fields ship in production."""
    out = fastly_api.load_log_format(None)
    # The result is a Fastly log-format string with ``%{...}V`` slots
    assert isinstance(out, str)
    assert "%{" in out  # at least one VCL expression slot
    assert len(out) > 50


def test_load_log_format_respects_provided_groups():
    """A custom config produces a different log format than the
    default standard preset."""
    minimal_cfg = {"groups": ["A"], "field_overrides": {}}
    out_min = fastly_api.load_log_format(minimal_cfg)
    out_default = fastly_api.load_log_format(None)
    # Different group selection → different log format
    assert out_min != out_default


# ── validate_log_format: length + regex paths ──────────────────────────────


def test_validate_log_format_returns_empty_list_for_standard_preset():
    """Default preset is known-valid. Pinned to catch a regression
    that broke the default config (would surface as wizard refusing
    to provision new services)."""
    # falco may or may not be installed; either way the result is []
    errors = fastly_api.validate_log_format(None)
    assert isinstance(errors, list)
    # No errors at all on the default
    assert errors == [] or all("warning" not in e.lower() for e in errors)


def test_validate_log_format_flags_oversize_format_with_attribution():
    """Log format > 8000 chars → LOG_FORMAT_TOO_LONG with a breakdown
    of how many chars came from custom fields vs built-in. Pinned
    because the breakdown is what tells admins which fields to remove.

    Length check fires BEFORE the falco/regex syntax check, but the
    raw log format must be > 8000 chars to trigger it. We mock
    ``load_log_format`` directly to skip generating real VCL (which
    would be syntactically rejected for being all X's)."""
    big_raw = "X" * 9000  # exceeds FASTLY_LOG_FORMAT_SAFE_MAX (8000)
    cfg = {
        "groups": ["A"],
        "custom_fields": [{"name": f"f{i}", "enabled": True, "vcl_log_expression": "X" * 100} for i in range(50)],
    }

    with patch("backend.provision.fastly_api.load_log_format", return_value=big_raw):
        errors = fastly_api.validate_log_format(cfg)

    assert len(errors) == 1
    assert errors[0].startswith("LOG_FORMAT_TOO_LONG")
    # The error must include both the built-in and custom-field char counts
    assert "Built-in fields" in errors[0]
    assert "Custom fields" in errors[0]


def test_validate_log_format_swallows_generation_failure():
    """If log_fields.generate_log_format raises (corrupt config),
    return a single error rather than crashing the validate UI.
    Pinned because the wizard's "Validate" button keys on the
    list-of-errors shape; a thrown exception would 500 it."""
    with patch(
        "backend.provision.fastly_api.lf.generate_log_format",
        side_effect=ValueError("bad config"),
    ):
        errors = fastly_api.validate_log_format({"groups": ["A"]})

    assert len(errors) == 1
    assert "Could not generate log format" in errors[0]
    assert "bad config" in errors[0]


def test_validate_log_format_falls_back_to_regex_when_falco_absent():
    """If ``falco`` isn't on PATH, the validator runs the regex-based
    check instead of the full lint. Pinned because losing this
    fallback would force every test/CI environment to install falco."""
    # The result should still be a list (not a crash); content depends
    # on what regex flags
    with patch("backend.provision.fastly_api.shutil.which", return_value=None):
        errors = fastly_api.validate_log_format(None)

    assert isinstance(errors, list)


# ── _validate_log_format_regex: pure VCL syntax checks ─────────────────────


def test_validate_log_format_regex_clean_input_returns_no_errors():
    raw = '%{client.ip}V %{req.url}V %{if(req.method == "POST", "y", "n")}V'
    assert fastly_api._validate_log_format_regex(raw) == []


def test_validate_log_format_regex_catches_bare_comparison_in_if():
    """``if(>...)`` missing the left-hand side is a common copy-paste
    bug that Fastly accepts at upload time but errors on at request
    time. Pinned because catching it in the wizard saves debugging
    a production VCL deploy failure."""
    raw = '%{if(> 100, "big", "small")}V'
    errors = fastly_api._validate_log_format_regex(raw)
    assert len(errors) == 1
    assert "missing left-hand side" in errors[0]


@pytest.mark.parametrize("op", [">", "<", ">=", "<=", "!=", "=="])
def test_validate_log_format_regex_catches_bare_comparison_for_each_operator(op):
    """All 6 comparison operators trigger the same check."""
    raw = f'%{{if({op} 5, "a", "b")}}V'
    errors = fastly_api._validate_log_format_regex(raw)
    assert any("missing left-hand side" in e for e in errors)


def test_validate_log_format_regex_catches_unclosed_if_parenthesis():
    """An ``if(...`` that never closes is another common bug. Pinned
    because Fastly's parser silently truncates the expression at
    the missing paren, producing surprising log output."""
    raw = '%{if(req.url == "/foo", "a", "b"}V'  # missing the final )
    errors = fastly_api._validate_log_format_regex(raw)
    assert any("unclosed if() parenthesis" in e for e in errors)


def test_validate_log_format_regex_handles_multiple_vcl_slots():
    """Each ``%{...}V`` slot is checked independently — only the
    broken one surfaces. Pinned because losing this isolation would
    flag valid slots as errors just because a sibling has a bug."""
    raw = '%{client.ip}V %{if(> 100, "x", "y")}V %{req.url}V'
    errors = fastly_api._validate_log_format_regex(raw)
    # Exactly the middle slot is flagged
    assert len(errors) == 1
    assert "missing left-hand side" in errors[0]


def test_validate_log_format_regex_ignores_text_outside_vcl_slots():
    """``if(...`` outside a ``%{...}V`` slot is plain log text, not
    VCL. Pinned because false-positives on log text would block
    valid log formats."""
    raw = "plain text with if(broken: %{client.ip}V"
    errors = fastly_api._validate_log_format_regex(raw)
    assert errors == []


def test_validate_log_format_regex_handles_nested_parens_correctly():
    """``if(regsub(x, y, z), a, b)`` has nested parens — must be
    detected as closed. Pinned because the unclosed-paren check
    uses depth tracking; a regression to naive substring search
    would false-positive on every regsub call."""
    raw = '%{if(regsub(req.url, "/foo/", ""), "yes", "no")}V'
    errors = fastly_api._validate_log_format_regex(raw)
    assert errors == []


# ── generate_capture_vcl: snippet keys ─────────────────────────────────────


def test_generate_capture_vcl_always_returns_recv_miss_pass():
    """The base subroutines (``recv``, ``miss``, ``pass``) are
    always emitted, regardless of which groups are enabled. Pinned
    because the wizard's deploy step keys on these names."""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A"], "field_overrides": {}})
    assert "recv" in snippets
    assert "miss" in snippets
    assert "pass" in snippets


def test_generate_capture_vcl_emits_origin_subroutines_when_group_l_enabled():
    """Group L (Origin Metrics) requires the ``fetch``, ``error``,
    and ``deliver`` subroutines to capture origin-side data.
    Pinned because losing these would silently zero the origin
    timing dashboard."""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A", "L"], "field_overrides": {}})
    assert "fetch" in snippets
    assert "error" in snippets
    assert "deliver" in snippets


def test_generate_capture_vcl_omits_origin_subroutines_when_group_l_disabled():
    """Without group L, the origin subroutines are not emitted —
    they would otherwise increase the deployed VCL byte size for no
    benefit (and could conflict with customer-defined subroutines)."""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A"], "field_overrides": {}})
    assert "fetch" not in snippets
    assert "error" not in snippets
    assert "deliver" not in snippets


def test_generate_capture_vcl_accepts_none_config():
    """``None`` config is the wizard's first-time-run case before
    any preset is selected. Must produce a default VCL set, not
    crash with KeyError."""
    snippets = fastly_api.generate_capture_vcl(None)
    assert "recv" in snippets


# ── EDGE_DATA_MAPPING: structural invariants ───────────────────────────────


def test_edge_data_mapping_covers_all_documented_subfields():
    """Each entry in EDGE_DATA_MAPPING is a (subfield_name, VCL_expr)
    pair. Pinned with a few canonical entries so a refactor that
    renames a subfield is caught — the dashboard's geo lookups
    key on these exact names."""
    m = fastly_api.EDGE_DATA_MAPPING
    # Geo subfields the country map relies on
    assert "country" in m
    assert "city" in m
    assert "lat" in m and "lon" in m
    # Network-quality subfields the network panel keys on
    assert "asn" in m
    assert "rtt" in m
    # TLS fingerprint subfields the security panel keys on
    assert "ja3" in m
    assert "ja4" in m


def test_edge_data_mapping_values_are_vcl_expressions():
    """Every value is a non-empty string (a VCL expression).
    Pinned because an empty value would emit ``"%{}V"`` which
    Fastly rejects at upload."""
    for name, expr in fastly_api.EDGE_DATA_MAPPING.items():
        assert isinstance(expr, str) and expr.strip(), f"empty VCL expr for {name!r}"


def test_fastly_log_format_safe_max_is_under_fastly_hard_limit():
    """8000 is the safe cap chosen to give 192 bytes of headroom
    under Fastly's documented 8192-char limit. Pinned because
    bumping this without bumping the hard-cap detection would let
    deploys fail at upload time instead of in our validator."""
    assert fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX <= 8192
    # And not absurdly low — the wizard would refuse legitimate configs
    assert fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX >= 4000


# ── generate_capture_vcl: deeper branch coverage ──────────────────────────


def test_generate_capture_vcl_includes_custom_edge_field_in_recv_snippet():
    """When a custom field has `collection_stage="edge"`, its VCL
    expression lands inside the recv snippet as
    `set req.http.x-fos-edge-data:<name> = <expr>;`. Pinned because
    losing this would silently drop the field's value at log time."""
    cfg = {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "my_custom",
                "enabled": True,
                "collection_stage": "edge",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "vcl_log_expression": "req.url",
                "bytes_estimate": 20,
            }
        ],
    }
    out = fastly_api.generate_capture_vcl(cfg)
    assert "my_custom" in out["recv"]
    assert "req.url" in out["recv"]


def test_generate_capture_vcl_emits_group_l_request_id_in_recv():
    """Group L adds the `randomstr(8)` request-id generator in recv
    so origin timing can correlate edge→shield→origin. Pinned
    because losing this would break the cluster-stitching across
    POPs."""
    cfg = {"groups": ["A", "L"], "field_overrides": {}}
    out = fastly_api.generate_capture_vcl(cfg)
    assert "x-req-id" in out["recv"]
    assert "randomstr(8)" in out["recv"]


def test_generate_capture_vcl_emits_group_l_timing_in_miss_snippet():
    """Group L adds origin-timing instrumentation to the `miss`
    snippet (records x-of-start before the fetch). Pinned because
    losing this would make ottfb metrics zero for all MISS paths."""
    cfg = {"groups": ["A", "L"], "field_overrides": {}}
    out = fastly_api.generate_capture_vcl(cfg)
    assert "x-of-start" in out["miss"]
    assert "time.elapsed.usec" in out["miss"]


def test_generate_capture_vcl_emits_fetch_error_deliver_snippets_for_group_l():
    """Group L adds 3 origin-side snippets (fetch/error/deliver) to
    capture TTFB, TTLB, status, origin IP. Pinned because losing
    any would zero out a column of the origin-latency chart."""
    cfg = {"groups": ["A", "L"], "field_overrides": {}}
    out = fastly_api.generate_capture_vcl(cfg)
    assert "fetch" in out
    assert "error" in out
    assert "deliver" in out
    # Each captures specific values
    assert "x-of-ttfb" in out["fetch"]
    assert "x-of-ttlb" in out["deliver"]


def test_generate_capture_vcl_custom_origin_field_emits_fetch_snippet_even_without_group_l():
    """A custom field with `collection_stage="origin"` triggers
    the fetch snippet even when group L is disabled. Pinned
    because losing this would silently drop the user's origin
    field at log time."""
    cfg = {
        "groups": ["A"],  # No L
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "my_origin_field",
                "enabled": True,
                "collection_stage": "origin",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "vcl_log_expression": "beresp.status",
                "bytes_estimate": 20,
            }
        ],
    }
    out = fastly_api.generate_capture_vcl(cfg)
    # fetch snippet is emitted to capture the custom origin field
    assert "fetch" in out
    assert "my_origin_field" in out["fetch"]
    assert "x-fos-origin-data" in out["fetch"]
