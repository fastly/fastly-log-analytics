"""Consolidated tests for test_fastly_api."""

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
    """Log format > 16000 chars → LOG_FORMAT_TOO_LONG with a breakdown
    of how many chars came from custom fields vs built-in. Pinned
    because the breakdown is what tells admins which fields to remove.

    Length check fires BEFORE the falco/regex syntax check, but the
    raw log format must be > 16000 chars to trigger it. We mock
    ``load_log_format`` directly to skip generating real VCL (which
    would be syntactically rejected for being all X's)."""
    big_raw = "X" * 17000  # exceeds FASTLY_LOG_FORMAT_SAFE_MAX (16000)
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


# ── 007: edge-hop detection via unforgeable fastly.ff (anti Fastly-FF spoof) ─
#
# X-Edge-Shield-Auth as an edge-detection mechanism is RETIRED. Edge detection
# is now ALWAYS ``fastly.ff.visits_this_service == 0`` — unforgeable (each
# Fastly-FF entry is a salted hash only genuine Fastly hops can produce), so a
# client cannot fake having already transited our edge. No shield-auth secret
# is generated, validated, or stamped anywhere in the capture VCL.


def test_capture_vcl_uses_fastly_ff_edge_detect_in_scrub_and_capture():
    """The edge scrub/capture guards key on the unforgeable
    fastly.ff.visits_this_service == 0 check. There is no shield-auth variant
    anymore: X-Edge-Shield-Auth must appear NOWHERE in the recv snippet
    (no scrub unset, no edge-detect comparison)."""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A"]})
    # scrub guard (first-pass-only): restarts == 0 && the unforgeable edge check.
    assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in snippets["recv"]
    # The retired shield-auth mechanism must not reappear in any form.
    assert "X-Edge-Shield-Auth" not in snippets["recv"]


def test_capture_vcl_never_references_shield_auth_anywhere():
    """The X-Edge-Shield-Auth header is retired from the capture VCL entirely:
    no inbound scrub unset, and no `set bereq.http.X-Edge-Shield-Auth` stamp on
    the miss/pass legs. (Previously a cluster_secret stamped it on every bereq;
    that mechanism is gone — fastly.ff is unforgeable so no auth header is
    needed to distinguish the edge hop.)"""
    snippets = fastly_api.generate_capture_vcl({"groups": ["A"]})
    for kind in ("recv", "miss", "pass"):
        assert "X-Edge-Shield-Auth" not in snippets[kind], f"{kind}: retired shield-auth header reappeared"


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
    """16000 is the safe cap chosen to give headroom
    under Fastly's documented 16384-char limit. Pinned because
    bumping this without bumping the hard-cap detection would let
    deploys fail at upload time instead of in our validator."""
    assert fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX <= 16384
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


# ── miss_pass deliver-gate (falco-free string coverage, py SEAM-01) ─────────
#
# An origin field with origin_log_frequency="miss_pass" must promote its value
# into the log line ONLY when the response did NOT come from cache — i.e. gated
# behind `if (fastly_info.state !~ "HIT")`. This is the one branch the falco
# semantic test can't exercise (falco can't bind fastly_info.state for the `!~`
# operator, see tests/core/test_vcl_semantics.py::test_falco_origin_field_miss_pass_only),
# so it was completely uncovered. These string-level tests pin the generated VCL
# directly: invert the operator (`!~`→`~`), drop the guard, or fall the branch
# through to the unconditional promote and one of these fails.


def _miss_pass_cfg(frequency: str) -> dict:
    return {
        "groups": ["A"],
        "field_overrides": {},
        "custom_fields": [
            {
                "name": "origin_cache_state",
                "enabled": True,
                "collection_stage": "origin",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "vcl_log_expression": "beresp.http.X-Cache",
                "origin_log_frequency": frequency,
                "bytes_estimate": 20,
            }
        ],
    }


def test_generate_capture_vcl_miss_pass_gates_promotion_behind_cache_miss():
    """miss_pass fields promote only on a non-HIT response. Pins the exact
    `fastly_info.state !~ "HIT"` deliver-gate AND that the promotion is nested
    INSIDE it — inverting the operator or hoisting the promote out fails here."""
    deliver = fastly_api.generate_capture_vcl(_miss_pass_cfg("miss_pass"))["deliver"]
    lines = deliver.splitlines()

    # 1. The deliver-gate exists with the exact `!~ "HIT"` operator. Inverting
    #    to `~` (promote ON hit — the opposite of "miss/pass only") or removing
    #    the guard changes this literal and trips the assert.
    guard = '  if (fastly_info.state !~ "HIT") {'
    assert guard in lines, f"miss_pass deliver-gate missing or changed.\nDELIVER:\n{deliver}"

    # 2. Promote-inside-if: the field's only promotion sits immediately inside
    #    the guard (deeper indent), and the guard closes right after it.
    gi = lines.index(guard)
    promote = "    set req.http.x-fos-origin-data:origin_cache_state = resp.http.x-fos-origin-data:origin_cache_state;"
    assert lines[gi + 1] == promote, f"promote not nested inside the miss_pass guard.\nDELIVER:\n{deliver}"
    assert lines[gi + 2].strip() == "}", f"miss_pass guard not closed right after the promote.\nDELIVER:\n{deliver}"

    # 3. The field is NEVER promoted at the unconditional outer indent (the
    #    shape the `all` branch uses) — proves the branch didn't fall through.
    unconditional = (
        "  set req.http.x-fos-origin-data:origin_cache_state = resp.http.x-fos-origin-data:origin_cache_state;"
    )
    assert unconditional not in lines, f"miss_pass field promoted unconditionally — gate bypassed.\nDELIVER:\n{deliver}"


def test_generate_capture_vcl_all_frequency_promotes_without_cache_state_guard():
    """Contrast case: the default `all` frequency promotes on every response,
    with no `fastly_info.state` gate. Pins the branch distinction so a future
    edit can't collapse miss_pass and all into one shape undetected."""
    deliver = fastly_api.generate_capture_vcl(_miss_pass_cfg("all"))["deliver"]
    assert "fastly_info.state" not in deliver, f"`all` frequency must not gate on cache state.\nDELIVER:\n{deliver}"
    unconditional = (
        "  set req.http.x-fos-origin-data:origin_cache_state = resp.http.x-fos-origin-data:origin_cache_state;"
    )
    assert unconditional in deliver.splitlines(), f"`all` frequency must promote unconditionally.\nDELIVER:\n{deliver}"


def test_pure_statement_helpers():
    from backend.provision.fastly_api import get_capture_vcl_statements, get_scrub_vcl_statements

    cfg = {"groups": ["A"]}
    scrubs = get_scrub_vcl_statements(cfg)
    captures = get_capture_vcl_statements(cfg)
    assert any("unset req.http.x-is-cluster-fetch" in s for s in scrubs)
    assert any("set req.http.x-fos-edge-data:ip" in s for s in captures)
    # Ensure no outer conditional wraps exist in statements
    assert not any("if (" in s for s in scrubs)
    assert not any("if (" in s for s in captures if "Fastly-Client-IP" not in s)


# ── Merged from test_fastly_api_orchestrators.py ──────────────────────────────────────────────

from unittest.mock import MagicMock

import pytest

from backend import config as svcconfig

# ── delete_cdn_service ───────────────────────────────────────────────────


def test_delete_cdn_service_deactivates_active_version_before_deleting():
    """Must deactivate the active version FIRST — Fastly refuses to
    delete a service with an active version. Pinned because losing the
    deactivate step would leave orphan services that accumulate
    minimum-charge billing."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}, {"number": 2, "active": True}]
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")

    # GET /service/svc-id/version
    assert ("GET", "/service/svc-id/version") in calls
    # PUT /service/svc-id/version/2/deactivate (the active one)
    assert ("PUT", "/service/svc-id/version/2/deactivate") in calls
    # DELETE /service/svc-id
    assert ("DELETE", "/service/svc-id") in calls


def test_delete_cdn_service_returns_silently_when_404_on_list():
    """A 404 on the version-list call (service already deleted) is
    a no-op success. Pinned because re-running teardown after a
    successful first run shouldn't fail."""
    call_count = [0]

    def fake_fastly(method, path, **kwargs):
        call_count[0] += 1
        if method == "GET" and "/version" in path:
            raise RuntimeError("HTTP 404: service not found")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        # Should not raise
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")

    # The version-list call happened, then we early-returned (no DELETE)
    assert call_count[0] == 1


def test_delete_cdn_service_swallows_404_on_delete_call():
    """A 404 on the DELETE call (service deleted between listing
    and deletion) → silent success. Pinned because the
    list+delete is non-atomic and another teardown process might
    have raced ahead."""

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        if method == "DELETE":
            raise RuntimeError("HTTP 404: not found")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        # Should not raise
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok")


def test_delete_cdn_service_reraises_non_404_delete_error():
    """A 5xx or other non-404 from DELETE → propagate. Pinned because
    a 502/503 is transient and the cron should retry — silently
    swallowing would leave the service orphaned."""

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        if method == "DELETE":
            raise RuntimeError("HTTP 502: bad gateway")
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError, match="502"):
            fastly_api.delete_cdn_service("svc-id", "MyService", "tok")


def test_delete_cdn_service_emits_status_callback_events():
    """Status callback receives a "Deleting..." message + completion
    message. Pinned because the SSE consumer in the wizard reads
    these status events to update the progress UI."""
    statuses = []

    def fake_fastly(method, path, **kwargs):
        if method == "GET" and "/version" in path:
            return [{"number": 1, "active": False}]
        return {}

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        fastly_api.delete_cdn_service("svc-id", "MyService", "tok", status_cb=statuses.append)

    assert any("Deleting" in s for s in statuses)
    assert any("deleted" in s.lower() for s in statuses)


# ── remove_logging_endpoint ───────────────────────────────────────────────


def test_remove_logging_endpoint_no_op_when_no_active_version():
    """No active version → warn and return (no API mutations). Pinned
    because customers sometimes delete the Fastly service before the
    wizard does its cleanup — losing the no-op would 500 the teardown."""

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
        patch("backend.provision.fastly_api.fastly") as mock_fastly,
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # No mutation calls
    mock_fastly.assert_not_called()


def test_remove_logging_endpoint_no_op_when_endpoint_already_absent():
    """If the named endpoint isn't on the active version, we still clone and
    clean up any leftover snippets, backends, dictionaries, or conditions,
    but we do NOT call DELETE on the absent endpoint itself."""
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["OtherEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # Cloned and validated, but no DELETE call for MyEndpoint
    paths = [p for _, p in calls]
    assert any("clone" in p for p in paths)
    assert not any("logging/s3/MyEndpoint" in p for _, p in calls if _ == "DELETE")
    assert any("validate" in p for p in paths)


def test_remove_logging_endpoint_clones_version_then_deletes_endpoint_and_snippets():
    """Happy path: clone active → delete endpoint → delete snippets
    → validate → activate. Pinned because the clone-then-mutate flow
    is what preserves the audit trail (every change is its own
    Fastly version).

    Snippet removal ENUMERATES the draft and deletes what it owns (it used to
    blind-DELETE a hardcoded name list), so the fake must serve a snippet list.
    """
    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path))
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": "Fastly Log Analytics - vcl_recv"}, {"name": "Fastly Log Analytics - vcl_deliver"}]
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # Clone, then delete endpoint, then delete snippets, validate, activate
    paths = [p for _, p in calls]
    assert any("clone" in p for p in paths)
    assert any("logging/s3/MyEndpoint" in p for _, p in calls if _ == "DELETE")
    assert any("snippet/" in p for _, p in calls if _ == "DELETE")
    assert any("validate" in p for p in paths)
    assert any("activate" in p for _, p in calls if _ == "PUT")


def test_remove_logging_endpoint_swallows_404_on_snippet_delete():
    """A 404 when deleting a snippet (already removed by a previous
    teardown attempt) → continue, don't fail. Pinned because a half-
    completed teardown that left some snippets behind shouldn't
    block a re-run from completing the rest."""

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "DELETE" and "snippet/" in path:
            raise RuntimeError("HTTP 404: snippet not found")
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        # Should NOT raise despite the 404s on snippet deletes
        fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")


def test_remove_logging_endpoint_rolls_back_on_validation_failure():
    """If validation of the draft fails, the old active version is
    re-activated. Pinned because losing the rollback would leave the
    customer with a deactivated logging endpoint (no logs flowing)."""
    activate_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        if "/clone" in path:
            return {"number": 6}
        if "/validate" in path:
            return {"status": "error", "errors": ["bad vcl"]}
        if method == "PUT" and path.endswith("/activate"):
            activate_calls.append(path)
        return {}

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=5),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.remove_logging_endpoint("svc-id", "MyEndpoint", "tok")

    # The OLD active version (5) was re-activated as rollback
    assert any("/version/5/activate" in p for p in activate_calls)


# ── ensure_cdn_service: input validation ──────────────────────────────────


def test_ensure_cdn_service_missing_logging_service_id():
    """Verify ValueError is raised if logging_service_id is missing from cfg."""
    with pytest.raises(ValueError, match="logging_service_id or service_id must be provided"):
        fastly_api.ensure_cdn_service({}, "AK", "SK", "tok")


# ── redeploy_cdn_vcl ────────────────────────────────────────────────────


def _reconciler_writing_cdn_id(cdn_id: str, *, then_raise: Exception | None = None, version: int = 1) -> MagicMock:
    """Stand-in for ``reconcile_cdn_service_state`` that persists the CDN id the way the real one does."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    def _reconcile(logging_service_id, token, status_cb=None, activate=True):
        svcconfig.mutate_config(logging_service_id, lambda c: c.setdefault("fos_proxy", {}).update(service_id=cdn_id))
        if then_raise is not None:
            raise then_raise
        return ReconciliationResult(service_id=cdn_id, activated_version=version)

    return MagicMock(side_effect=_reconcile)


def test_redeploy_cdn_vcl_success():
    """Verify redeploy_cdn_vcl maps cdn_service_id, writes rate_limiting to disk, and runs reconciler."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    mock_config = {"service_id": "service-id", "cdn_service_id": "cdn-id", "fos_proxy": {}}
    svcconfig.save_config("service-id", dict(mock_config))

    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile,
    ):
        mock_reconcile.return_value = ReconciliationResult(
            service_id="cdn-id", activated_version=12, changes_applied=None
        )

        result = fastly_api.redeploy_cdn_vcl("cdn-id", "tok", rate_limiting=False)

        assert result == 12
        assert svcconfig.load_config("service-id")["fos_proxy"]["rate_limiting_enabled"] is False
        mock_reconcile.assert_called_with(
            logging_service_id="service-id",
            token="tok",
            status_cb=None,
            activate=True,
        )


def test_redeploy_cdn_vcl_raises_when_no_mapping():
    """Verify redeploy_cdn_vcl raises ValueError when cdn_service_id cannot be mapped."""
    with patch("backend.config.list_configs", return_value=[]):
        with pytest.raises(ValueError, match="Could not map cdn_service_id"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


def test_redeploy_cdn_vcl_propagates_reconciler_error():
    """Verify redeploy_cdn_vcl propagates exceptions from the reconciler."""
    mock_config = {"service_id": "service-id", "cdn_service_id": "cdn-id", "fos_proxy": {}}
    svcconfig.save_config("service-id", dict(mock_config))
    with (
        patch("backend.config.list_configs", return_value=[mock_config]),
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            side_effect=RuntimeError("reconciler failure"),
        ),
    ):
        with pytest.raises(RuntimeError, match="reconciler failure"):
            fastly_api.redeploy_cdn_vcl("cdn-id", "tok")


# ── update_logging_endpoint (SSE generator) ──────────────────────────────


def _drain(gen):
    """Consume an SSE generator into (events, exception_or_None)."""
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def test_update_logging_endpoint_short_circuits_when_no_changes():
    """If period, path, sample_rate, edge_only, custom_condition, and
    format all match the current endpoint state, emit a "done"
    event with ``changed=False`` and NO clone. Pinned because
    cloning on every settings-page save would bump version numbers
    unnecessarily.

    Note: the orchestrator always rebuilds the response-condition
    statement with at minimum ``!segmented_caching.is_inner_req``,
    so for the no-change short-circuit to fire, the *current*
    condition statement must already equal that prefix."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc-id", activated_version=10, changes_applied=None),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc-id",
                    "endpoint_name": "MyEndpoint",
                    "sample_rate": 100,
                    "edge_only": False,
                    "log_period": 60,
                    "fos_path": "/raw/%Y-%m-%d/%H/",
                    "update_format": False,
                },
                "tok",
            )
        )

    assert exc is None
    # Final event is done + changed=False
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is False
    assert done["version"] == 10


def _run_update_logging_endpoint(cfg_in, stored_cfg):
    """Drive update_logging_endpoint and return the config it persisted."""
    import json

    from backend import config as svcconfig
    from backend.provision.declarative.reconciler import ReconciliationResult

    svcconfig.save_config("svc-id", dict(stored_cfg))

    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc-id", activated_version=11, changes_applied=["fmt"]),
        ),
    ):
        events, exc = _drain(fastly_api.update_logging_endpoint(cfg_in, "tok"))

    assert exc is None, f"generator raised: {exc}"
    return json.loads(svcconfig.config_path("svc-id").read_text()), events


def test_update_logging_endpoint_preserves_custom_fields_when_incoming_omits_them():
    """REGRESSION: 2026-08-12 production incident — the root cause. This
    orchestrator assigned ``cfg["log_fields"]`` WHOLESALE over the stored
    config with no merge guard (unlike its cli.py and log-fields-set
    siblings). Callers build ``log_fields`` from groups alone, so the assign
    dropped the user's custom_fields AND the system-managed scoring/CMCD
    entries. ``reconcile_vcl_state`` then regenerated the Fastly log format
    from the stripped config, so the CMCD extraction VCL kept running at the
    edge and nothing logged its output — every ``cmcd_*`` column ingested
    empty for a month while the UI still reported CMCD "enabled"."""
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

    stored = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {
            "logging_service_id": "svc-id",
            "endpoint_name": "MyEndpoint",
            "log_period": 60,
            # Built from groups alone — no custom_fields key at all.
            "log_fields": {"schema_version": 2, "preset": "standard", "groups": ["A", "B", "C"]},
        },
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    assert "my_custom" in saved_names, "user custom_field was stripped by the wholesale assign"
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} was stripped by the wholesale assign"
    # The incoming group change must still land.
    assert saved_cfg["log_fields"]["groups"] == ["A", "B", "C"]


def test_update_logging_endpoint_reasserts_cmcd_without_a_state_transition():
    """The reconciler must be keyed on CMCD STATE, not on a transition. A
    reconcile that says nothing about CMCD (no ``cmcd_enabled`` in the
    request) previously left the fields however it found them; with CMCD
    enabled it must re-assert the canonical 14 from code."""
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

    stored = {
        "logging_enabled": True,
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        # Already-stripped config — the broken state the affected service was in.
        "log_fields": {"schema_version": 2, "groups": ["A", "B"], "field_overrides": {}},
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} not re-asserted by a plain reconcile"


def test_update_logging_endpoint_strips_cmcd_when_disabled():
    """Disable converges too — a reconcile with CMCD off removes stale entries."""
    stored = {
        "logging_enabled": True,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }

    saved_cfg, _ = _run_update_logging_endpoint(
        {"logging_service_id": "svc-id", "endpoint_name": "MyEndpoint", "log_period": 60},
        stored,
    )

    saved_names = {cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]}
    assert saved_names == {"my_custom"}


def test_update_logging_endpoint_404_on_endpoint_lookup_raises_friendly_error():
    """If the endpoint name doesn't exist on the active version, raise
    a friendly error. Pinned because the FE renders error messages
    in the error toast — losing it would dump the opaque message."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc-id",
                error="Logging endpoint 'GhostEndpoint' not found on service",
            ),
        ),
    ):
        gen = fastly_api.update_logging_endpoint(
            {
                "logging_service_id": "svc-id",
                "endpoint_name": "GhostEndpoint",
                "log_period": 60,
            },
            "tok",
        )
        events, exc = _drain(gen)

    assert isinstance(exc, RuntimeError)
    assert "Declarative reconciliation failed" in str(exc)


def test_update_logging_endpoint_raises_when_no_active_version():
    """No active version → friendly RuntimeError. Pinned because this
    is the "Fastly service is brand-new / inactive" recovery signal
    that the FE keys on to render the "activate first" CTA."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc", error="No active version found for service 'svc'"),
        ),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        events, exc = _drain(gen)

    assert isinstance(exc, RuntimeError)


def test_update_logging_endpoint_refreshes_rate_limiting_flag():
    """A logging-settings update re-probes the account entitlement and persists
    the refreshed flag (so a later Update-CDN deploys correctly), emitting an
    informational status event. It does NOT redeploy the CDN itself."""
    import json

    from backend import config as svcconfig

    svcconfig.save_config("svc", {"provisioning": {"rate_limiting": True}, "log_fields": {"groups": ["A"]}})
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=False),
        # Short-circuit the rest of the generator right after the probe block.
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        events, exc = _drain(gen)

    # Flag refreshed + persisted under provisioning.rate_limiting.
    assert json.loads(svcconfig.config_path("svc").read_text())["provisioning"]["rate_limiting"] is False
    # An informational status event was surfaced...
    assert any(e.get("type") == "status" and "rate limiting" in e.get("message", "").lower() for e in events)
    # ...and the rest of the update still ran (and raised on no active version).
    assert isinstance(exc, RuntimeError)


def test_update_logging_endpoint_skips_flag_refresh_when_no_service_cfg():
    """No on-disk service config (load_config → None) → the probe block is
    skipped entirely (no save_config, no probe call)."""
    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.save_config", side_effect=AssertionError("must not save")),
        patch(
            "backend.provision.fastly_api.account_has_rate_limiting",
            side_effect=AssertionError("must not probe"),
        ),
        patch("backend.provision.fastly_api.get_active_version", return_value=None),
    ):
        gen = fastly_api.update_logging_endpoint(
            {"logging_service_id": "svc", "endpoint_name": "ep", "log_period": 60},
            "tok",
        )
        _events, exc = _drain(gen)
    assert isinstance(exc, RuntimeError)  # still raises on no active version


# ── ensure_cdn_service: happy-path orchestration ──────────────────────────


def test_ensure_cdn_service_success():
    """Verify ensure_cdn_service merges configs, writes them to disk, calls reconciler, and detects rate limiting."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
        "cdn_service_name": "MyCDN",
        "cdn_url": "https://mycdn.example.com",
    }
    statuses = []
    mock_reconcile = _reconciler_writing_cdn_id("cdn-new-123")

    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state", mock_reconcile),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True) as mock_detect,
    ):
        result = fastly_api.ensure_cdn_service(
            input_cfg,
            "AK",
            "SK",
            "tok",
            status_cb=statuses.append,
        )

        assert result == {
            "id": "cdn-new-123",
            "name": "MyCDN",
            "rate_limiting": True,
        }
        written_json = svcconfig.load_config("log-svc-123")
        assert written_json["fos_access_key_id"] == "AK"
        assert written_json["fos_secret_access_key"] == "SK"
        assert written_json["cdn_service_name"] == "MyCDN"

        mock_reconcile.assert_called_with(
            logging_service_id="log-svc-123",
            token="tok",
            status_cb=statuses.append,
            activate=True,
        )
        mock_detect.assert_called_with("tok", "log-svc-123")


def test_ensure_cdn_service_rate_limiting_scenarios():
    """Verify ensure_cdn_service returns rate_limiting True, False, or None depending on proactive detection."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
    }

    for detection_val in (True, False, None):
        with (
            patch(
                "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
                _reconciler_writing_cdn_id("cdn-new-123"),
            ),
            patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=detection_val),
        ):
            result = fastly_api.ensure_cdn_service(input_cfg, "AK", "SK", "tok")
            assert result["rate_limiting"] is detection_val


def test_ensure_cdn_service_on_created_callback():
    """Verify on_created is called with the newly created cdn service id."""
    input_cfg = {
        "logging_service_id": "log-svc-123",
    }
    created_ids = []

    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123"),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
    ):
        fastly_api.ensure_cdn_service(input_cfg, "AK", "SK", "tok", on_created=created_ids.append)
        assert created_ids == ["cdn-new-123"]


# ── account_has_rate_limiting: proactive account-entitlement probe ───────


def _vcl_with(pragma_line: str) -> str:
    return f"sub vcl_recv {{\n  {pragma_line}\n}}\n"


def test_account_has_rate_limiting_true_when_pragma_present():
    """The account-level ``ratelimit_opt_in true`` pragma in the generated VCL
    means edge rate limiting is entitled → True."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch(
            "backend.provision.fastly_api.get_generated_vcl",
            return_value=_vcl_with("pragma optional_param ratelimit_opt_in true;"),
        ),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is True


def test_account_has_rate_limiting_false_when_pragma_false():
    """``ratelimit_opt_in false`` must read as NOT entitled — the check matches
    the literal ``true`` value, not a loose ``ratelimit_opt_in`` substring."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch(
            "backend.provision.fastly_api.get_generated_vcl",
            return_value=_vcl_with("pragma optional_param ratelimit_opt_in false;"),
        ),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is False


def test_account_has_rate_limiting_false_when_pragma_absent():
    """Generated VCL read but no pragma at all → False (definitive)."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=7),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value="sub vcl_recv {}\n"),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is False


def test_account_has_rate_limiting_none_when_no_active_version():
    """No active version (Compute service mid-deploy / never activated) →
    inconclusive None, not False — so we don't wrongly strip rate limiting."""
    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is None


def test_account_has_rate_limiting_none_when_no_generated_vcl():
    """A wasm/Compute service has an active version but no generated VCL
    (get_generated_vcl → None) → inconclusive None."""
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=25),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=None),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "wasm-svc") is None


def test_account_has_rate_limiting_skips_falsy_ids_and_returns_none_with_no_ids():
    """Empty/None ids are skipped; with nothing conclusive → None."""
    with patch("backend.provision.fastly_api.get_active_version") as m_ver:
        assert fastly_api.account_has_rate_limiting("tok") is None
        assert fastly_api.account_has_rate_limiting("tok", "", None) is None
        m_ver.assert_not_called()


def test_account_has_rate_limiting_falls_through_to_next_vcl_sibling():
    """First id is a wasm service (no generated VCL); detection falls through to
    the second id, which has the pragma → True. This is why callers pass the CDN
    service AND the logging service."""
    gens = {"wasm-svc": None, "vcl-svc": _vcl_with("pragma optional_param ratelimit_opt_in true;")}
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=lambda sid, ver, tok: gens[sid]),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "wasm-svc", "vcl-svc") is True


def test_account_has_rate_limiting_first_conclusive_wins():
    """The first id that yields parseable generated VCL decides — a definitive
    False short-circuits before probing later ids."""
    seen = []

    def fake_gen(sid, ver, tok):
        seen.append(sid)
        return "sub vcl_recv {}\n"  # no pragma → False, conclusive

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=3),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=fake_gen),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "first", "second") is False
    assert seen == ["first"]


def test_account_has_rate_limiting_swallows_unexpected_probe_errors():
    """Detection is best-effort: an unexpected (non-RuntimeError) exception on a
    probe drops to the next id rather than breaking a deploy."""
    with (
        patch("backend.provision.fastly_api.get_active_version", side_effect=ValueError("boom")),
    ):
        assert fastly_api.account_has_rate_limiting("tok", "svc-1") is None


def test_update_logging_endpoint_clones_and_activates_on_changes_detected():
    """When period changes, the route clones the active version,
    updates the period on the new draft, validates, and activates.
    Pinned because the clone-then-mutate flow preserves the audit
    trail via per-change Fastly versions."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True, "log_fields": {}}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc",
                activated_version=11,
                changes_applied={"endpoint_update": {"log_period": 60}},
            ),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "log_period": 60,  # different from current 30
                },
                "tok",
            )
        )

    assert exc is None
    # Final done with changed=True
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is True
    assert done["version"] == 11


def test_update_logging_endpoint_removes_origin_snippets_when_group_l_disabled():
    """When the new log_fields config has NO group L (no `fetch`
    snippet in generate_capture_vcl), the route deletes any pre-
    existing Origin Fetch/Error/Deliver snippets. Pinned because
    losing this would leave orphan snippets that consume Fastly
    VCL budget and continue running stale capture logic."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(
                service_id="svc",
                activated_version=11,
                changes_applied={
                    "snippets_deleted": [
                        "Fastly Log Analytics Origin Fetch",
                        "Fastly Log Analytics Origin Error",
                        "Fastly Log Analytics Origin Deliver",
                    ]
                },
            ),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "update_format": True,
                },
                "tok",
            )
        )

    assert exc is None
    # Final event shows changes were applied
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["changed"] is True


def test_update_logging_endpoint_rolls_back_to_old_active_on_exception():
    """If any step after clone raises (validation fail, snippet error),
    re-activate the OLD active version (rollback) + yield error event +
    re-raise. Pinned because losing the rollback would leave the
    customer with no live logging endpoint after a partial update."""

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch("backend.config.load_config", return_value={"logging_enabled": True}),
        patch("backend.config.save_config"),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
        patch("backend.core.fastly.service.get_active_version", return_value=10),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id="svc", error="Validation exploded mid-update"),
        ),
    ):
        events, exc = _drain(
            fastly_api.update_logging_endpoint(
                {
                    "logging_service_id": "svc",
                    "endpoint_name": "MyEndpoint",
                    "log_period": 60,  # forces a change
                },
                "tok",
            )
        )

    # The exception was re-raised
    assert isinstance(exc, RuntimeError)


def test_ensure_cdn_service_reports_created_id_before_later_failure():
    """Verify on_created is called with the cdn_service_id if the reconciler failed AFTER writing it to config."""
    cfg = {
        "logging_service_id": "svc",
    }
    created_ids = []

    with (
        patch(
            "backend.provision.declarative.reconciler.reconcile_cdn_service_state",
            _reconciler_writing_cdn_id("cdn-new-123", then_raise=RuntimeError("Validation failed")),
        ),
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            fastly_api.ensure_cdn_service(cfg, "ak", "sk", "tok", on_created=created_ids.append)

    assert created_ids == ["cdn-new-123"]


def test_log_sampling_edge_clause_never_gates_on_restarts():
    """The Log Sampling edge gate must NEVER include ``req.restarts == 0``,
    regardless of scoring state. Customer VCL may use restarts for application
    logic (404 fallbacks, auth checks) — gating on restarts == 0 silently drops
    restarted requests from the telemetry stream (finding 007)."""
    from backend.provision.fastly_api import _log_sampling_edge_clause

    for scoring in (False, True):
        clause = _log_sampling_edge_clause(scoring)
        assert "req.restarts" not in clause
        assert clause == "fastly.ff.visits_this_service == 0"


def test_generate_capture_vcl_capture_and_scrubs_are_unified():
    """All edge captures, scrubs, custom fields, and request ID generation
    are consolidated in the main first-pass edge block:
    if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {
    to minimize VCL execution overhead and simplify structure."""
    from backend.provision.fastly_api import generate_capture_vcl

    cfg = {
        "groups": [],
        "custom_fields": [
            {"name": "cf1", "enabled": True, "collection_stage": "edge", "vcl_log_expression": "req.http.host"}
        ],
    }

    for scoring in (True, False):
        recv = generate_capture_vcl(cfg, scoring_enabled=scoring)["recv"]
        assert "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {" in recv
        assert "set req.http.x-fos-edge-data:cf1 = req.http.host;" in recv
        assert "set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;" in recv
        assert "set req.http.x-fos-edge-data:ip = client.ip;" in recv


def test_load_log_format_keeps_standard_fields_when_only_custom_fields_present():
    """A log_fields carrying custom_fields but no groups/preset must still emit
    the standard fields. Regression: enabling scoring on an empty-log_fields
    service added custom_fields, made the dict truthy, defeated the
    standard-preset fallback in load_log_format, and silently dropped
    url/method/edge (and all other group fields) from the log format."""
    from backend.provision.fastly_api import load_log_format

    fmt = load_log_format(
        {
            "custom_fields": [
                {
                    "name": "edge_score",
                    "enabled": True,
                    "collection_stage": "deliver",
                    "vcl_log_expression": "req.http.x-edge-score:score",
                }
            ]
        }
    )
    # Standard group fields survive alongside the custom field.
    assert "req.url" in fmt
    assert "req.method" in fmt
    assert "visits_this_service" in fmt  # the `edge` field
    assert "edge_score" in fmt

    # Empty / None configs still fall back to the standard fields (unchanged).
    assert "req.url" in load_log_format({})
    assert "req.url" in load_log_format(None)


def test_service_has_scoring_pragmas_requires_both_pragmas():
    """service_has_scoring_pragmas returns True only when both
    fix_unsent_body_drain and no_body_if_bereq_is_get_or_head are true in
    generated VCL."""
    both_vcl = (
        "pragma optional_param fix_unsent_body_drain true;\n"
        "pragma optional_param no_body_if_bereq_is_get_or_head true;\n"
    )
    only_drain_vcl = "pragma optional_param fix_unsent_body_drain true;\n"
    only_no_body_vcl = "pragma optional_param no_body_if_bereq_is_get_or_head true;\n"
    false_vcl = (
        "pragma optional_param fix_unsent_body_drain true;\n"
        "pragma optional_param no_body_if_bereq_is_get_or_head false;\n"
    )

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=both_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is True

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=only_drain_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=only_no_body_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", return_value=false_vcl),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is False

    assert fastly_api.service_has_scoring_pragmas("", "tok") is None
    assert fastly_api.service_has_scoring_pragmas("svc_1", "") is None

    with patch("backend.provision.fastly_api.get_active_version", return_value=None):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is None

    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.get_generated_vcl", side_effect=RuntimeError("boom")),
    ):
        assert fastly_api.service_has_scoring_pragmas("svc_1", "tok") is None


# ── Merged from test_teardown_dereference_order.py ──────────────────────────────────────────────


import pytest

from backend.provision.cmcd_fields import cmcd_snippet_names
from backend.provision.session_scoring_vcl import scoring_snippet_names

# What a real service looks like: ours, plus customer snippets that must survive.
CUSTOMER_SNIPPETS = ["Session Tracking - Recv", "Session Tracking - Deliver", "force_ssl", "Cache Overrides"]
OUR_SNIPPETS = [
    "Fastly Log Analytics - vcl_recv",
    "Fastly Log Analytics - vcl_miss",
    "Fastly Log Analytics - vcl_pass",
    "Fastly Log Analytics - vcl_fetch",
    "Fastly Log Analytics - vcl_deliver",
    "Fastly Log Analytics - vcl_error",
]


def _run_remove(snippet_names, deleted_sink):
    """Drive remove_logging_endpoint against a faked Fastly."""

    def fake_fastly(method, path, body=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": n} for n in snippet_names]
        if method == "DELETE" and "/snippet/" in path:
            import urllib.parse

            name = urllib.parse.unquote(path.rsplit("/snippet/", 1)[1])
            if name not in snippet_names:
                raise RuntimeError("HTTP 404 not found")
            deleted_sink.append(name)
            return {}
        if "/clone" in path:
            return {"number": 99}
        if "/validate" in path:
            return {"status": "ok"}
        if method == "GET" and path.endswith(("/backend", "/dictionary", "/condition")):
            return []
        return {}

    with (
        patch.object(fastly_api, "fastly", side_effect=fake_fastly),
        patch.object(fastly_api, "get_active_version", return_value=98),
        patch.object(fastly_api, "list_s3_endpoints", return_value=["Fastly Object Storage Logs"]),
    ):
        fastly_api.remove_logging_endpoint("svc-td", "Fastly Object Storage Logs", "tok")


def test_all_capture_snippets_removed_including_vcl_pass():
    """THE REGRESSION: every capture snippet goes, vcl_pass included."""
    deleted: list[str] = []
    _run_remove(OUR_SNIPPETS + CUSTOMER_SNIPPETS, deleted)
    for name in OUR_SNIPPETS:
        assert name in deleted, f"{name} was left on the service"


def test_customer_snippets_are_never_touched():
    """'Session Tracking - *' is the CUSTOMER's; ours is 'Session Scoring - *'.

    A prefix sweep on "Session " would delete their VCL. Pinned deliberately.
    """
    deleted: list[str] = []
    _run_remove(OUR_SNIPPETS + CUSTOMER_SNIPPETS, deleted)
    for name in CUSTOMER_SNIPPETS:
        assert name not in deleted, f"deleted customer-owned snippet {name!r}"


def test_legacy_capture_names_still_removed():
    """Older generations used 'Fastly Log Analytics Capture' etc."""
    legacy = ["Fastly Log Analytics Capture", "Fastly Log Analytics Origin Deliver"]
    deleted: list[str] = []
    _run_remove(legacy + CUSTOMER_SNIPPETS, deleted)
    assert set(legacy) <= set(deleted)


def test_feature_snippets_removed_from_canonical_lists():
    """Scoring / CMCD / RUM names come from the generators, not a copy."""
    feature = [*scoring_snippet_names(), *cmcd_snippet_names(), "RUM - Recv", "RUM - Set cookies"]
    deleted: list[str] = []
    _run_remove(feature + CUSTOMER_SNIPPETS, deleted)
    for name in feature:
        assert name in deleted, f"{name} was left on the service"


def test_unmatched_snippets_do_not_report_silent_success():
    """If nothing of ours matched, that must be surfaced, not logged as success.

    This is the exact 2026-08-13 shape: names on the service didn't match, so
    zero were removed and teardown reported success.
    """
    deleted: list[str] = []
    with patch.object(fastly_api, "warn") as mock_warn:
        _run_remove(CUSTOMER_SNIPPETS, deleted)
    assert deleted == []
    assert mock_warn.called, "removing zero analytics snippets must warn"
    assert any("teardown left VCL behind" in str(c) or "still on draft" in str(c) for c in mock_warn.call_args_list)


# ── scoring: never delete the Compute service while it may still be referenced ──


def test_scoring_teardown_aborts_when_vcl_strip_fails():
    """A strip failure must NOT proceed to delete the Compute service."""
    from backend.provision import session_scoring_orchestrator as sso

    with (
        patch.object(sso, "get_active_version", side_effect=RuntimeError("HTTP 500 clone failed")),
        patch.object(sso, "delete_scoring_service") as mock_delete,
    ):
        with pytest.raises(RuntimeError, match="Aborting scoring teardown"):
            sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")

    mock_delete.assert_not_called()


def test_scoring_teardown_proceeds_when_service_already_gone():
    """No active version means nothing left to dereference — not a failure."""
    from backend.provision import session_scoring_orchestrator as sso

    with (
        patch.object(sso, "get_active_version", return_value=None),
        patch.object(sso, "delete_scoring_service", return_value=[]) as mock_delete,
    ):
        sso.teardown_scoring_resources("svc-td", {"scoring_service_id": "compute-1"}, "tok")

    mock_delete.assert_called_once()
