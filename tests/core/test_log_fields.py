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

from __future__ import annotations

import pathlib
from unittest.mock import patch

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
from backend.provision import generate_capture_vcl
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
