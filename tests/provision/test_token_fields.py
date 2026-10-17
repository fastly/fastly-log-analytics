"""Token (subscriber id / content id) system fields + VCL extraction.

Each id is extracted into its header (``req.http.x-cs-sub-id`` /
``req.http.x-cs-content-id``) by an admin-supplied VCL expression and promoted
into the log line by its system-managed field (``cs_sub_id`` /
``cs_content_id``). Same two failure modes as CMCD: a field silently dropping
out of ``log_fields`` (Trap #27) and extraction being emitted after capture
(Trap #29). Plus one of its own: a field must exist only while its expression
is configured, or it reserves log-line budget for nothing (Trap #30).
"""

from __future__ import annotations

import pytest

from backend.core.field_registry import _TOKEN_SYSTEM_FIELDS, _is_system_field
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState, LogFieldsConfig, TokenConfig
from backend.provision.fastly_api import generate_capture_vcl, get_scrub_vcl_statements
from backend.provision.system_fields import reconcile_cfg_system_custom_fields, system_feature_flags
from backend.provision.token_fields import (
    _TOKEN_FIELD_NAMES,
    CONTENT_ID,
    CONTENT_ID_FIELD,
    ENFORCEMENT_FIELDS,
    SUBSCRIBER_ID,
    SUBSCRIBER_ID_FIELD,
    configured_token_fields,
    generate_token_vcl_lines,
    get_token_fields,
    reconcile_token_custom_fields,
    resolve_token_exprs,
    validate_token_exprs,
)

_SUB_EXPR = 'regsub(req.http.Authorization, "^Bearer ", "")'
_CID_EXPR = 'subfield(req.http.CAT, "cid", ",")'
_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}
_BOTH = frozenset({SUBSCRIBER_ID_FIELD, CONTENT_ID_FIELD})


def _vcl(t, expr):
    return {
        "unset": f"unset {t.header};",
        "set": f"set {t.header} = {expr};",
        "capture": f"set req.http.x-fos-edge-data:{t.field} = {t.header};",
    }


_SUB = _vcl(SUBSCRIBER_ID, _SUB_EXPR)
_CID = _vcl(CONTENT_ID, _CID_EXPR)


# ── validation ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sub,cid",
    [
        (_SUB_EXPR, ""),
        ("", _CID_EXPR),
        (_SUB_EXPR, _CID_EXPR),
        ("req.http.X-Subscriber", None),
        (None, "req.http.X-Content"),
    ],
)
def test_valid_combinations_pass(sub, cid):
    assert validate_token_exprs(sub, cid) == []


def test_at_least_one_id_required():
    msg = ["Configure at least one of Subscriber ID, Content ID or the enforcement log"]
    assert validate_token_exprs("", "  ") == msg
    assert validate_token_exprs(None, None)
    # The enforcement log needs no expression, so it counts as a selection on its own.
    assert validate_token_exprs("", "", enforcement=True) == []
    assert validate_token_exprs("a; b", "", enforcement=True)  # but a set expression must still be safe


@pytest.mark.parametrize(
    "bad",
    [
        'req.http.a; set req.http.b = "x"',  # statement injection
        "req.http.a # hidden",
        "req.http.a // hidden",
        "req.http.a\nset req.http.b = 1",
        "x" * 513,
    ],
)
@pytest.mark.parametrize("which", ["sub", "cid"])
def test_unsafe_expression_rejected_and_labelled(bad, which):
    errors = validate_token_exprs(bad, _CID_EXPR) if which == "sub" else validate_token_exprs(_SUB_EXPR, bad)
    assert errors
    label = "Subscriber ID" if which == "sub" else "Content ID"
    assert all(e.startswith(f"{label}: ") for e in errors)


def test_resolve_keeps_stored_on_none_and_clears_on_empty():
    stored = {"subscriber_id_expr": "a", "content_id_expr": "b"}
    assert resolve_token_exprs(stored, None, None) == ("a", "b")
    assert resolve_token_exprs(stored, " c ", "") == ("c", "")
    assert resolve_token_exprs(None, None, "d") == ("", "d")


def test_generate_lines_set_only_configured_ids():
    """The unsets live with the other scrubs, not beside the sets."""
    both = generate_token_vcl_lines(f"  {_SUB_EXPR}  ", _CID_EXPR)
    assert _SUB["set"] in both and _CID["set"] in both
    assert _SUB["unset"] not in both and _CID["unset"] not in both
    only_cid = generate_token_vcl_lines("", _CID_EXPR)
    assert _CID["set"] in only_cid
    assert not any(SUBSCRIBER_ID.header in ln for ln in only_cid)


def test_generate_lines_refuses_invalid_expression():
    with pytest.raises(ValueError):
        generate_token_vcl_lines(_SUB_EXPR, "req.http.a; esi")


@pytest.mark.parametrize("t", [SUBSCRIBER_ID, CONTENT_ID])
def test_scrub_unsets_header_only_when_its_field_enabled(t):
    unset = f"unset {t.header};"
    assert unset in get_scrub_vcl_statements({"custom_fields": get_token_fields({t.field})})
    assert unset not in get_scrub_vcl_statements({"custom_fields": []})
    disabled = [{**get_token_fields({t.field})[0], "enabled": False}]
    assert unset not in get_scrub_vcl_statements({"custom_fields": disabled})


# ── system field reconciliation ──────────────────────────────────────────────


def test_configured_fields_follow_expressions():
    assert configured_token_fields(None) == frozenset()
    assert configured_token_fields({"enabled": False, "subscriber_id_expr": _SUB_EXPR}) == frozenset()
    assert configured_token_fields({"enabled": True, "subscriber_id_expr": _SUB_EXPR}) == {SUBSCRIBER_ID_FIELD}
    assert configured_token_fields({"enabled": True, "content_id_expr": _CID_EXPR, "subscriber_id_expr": " "}) == {
        CONTENT_ID_FIELD
    }
    assert (
        configured_token_fields({"enabled": True, "subscriber_id_expr": _SUB_EXPR, "content_id_expr": _CID_EXPR})
        == _BOTH
    )


def test_reconcile_applies_exact_set_in_canonical_order():
    on = reconcile_token_custom_fields([_USER_FIELD], fields={CONTENT_ID_FIELD, SUBSCRIBER_ID_FIELD})
    assert [cf["name"] for cf in on] == ["my_custom", SUBSCRIBER_ID_FIELD, CONTENT_ID_FIELD]
    only_sub = reconcile_token_custom_fields(on, fields={SUBSCRIBER_ID_FIELD})
    assert [cf["name"] for cf in only_sub] == ["my_custom", SUBSCRIBER_ID_FIELD]
    assert reconcile_token_custom_fields(on, fields=frozenset()) == [_USER_FIELD]


def test_flags_carry_configured_token_fields():
    cfg = {"token": {"enabled": True, "subscriber_id_expr": _SUB_EXPR, "content_id_expr": _CID_EXPR}}
    assert system_feature_flags(cfg) == (False, False, _BOTH)
    assert system_feature_flags({"token": None}) == (False, False, frozenset())


def test_cfg_reconcile_reasserts_token_without_a_transition():
    """Trap #27: keyed on current state, so an already-stripped config heals."""
    cfg = {
        "token": {"enabled": True, "subscriber_id_expr": _SUB_EXPR, "content_id_expr": _CID_EXPR},
        "log_fields": {"custom_fields": [_USER_FIELD]},
    }
    names = [cf["name"] for cf in reconcile_cfg_system_custom_fields(cfg)]
    assert names == ["my_custom", SUBSCRIBER_ID_FIELD, CONTENT_ID_FIELD]


def test_cfg_reconcile_drops_field_when_its_expression_is_cleared():
    """Trap #30: a dead field would still reserve log-line budget."""
    cfg = {
        "token": {"enabled": True, "subscriber_id_expr": _SUB_EXPR, "content_id_expr": ""},
        "log_fields": {"custom_fields": [_USER_FIELD, *get_token_fields(_BOTH)]},
    }
    assert [cf["name"] for cf in reconcile_cfg_system_custom_fields(cfg)] == ["my_custom", SUBSCRIBER_ID_FIELD]


def test_cfg_reconcile_strips_token_when_disabled():
    cfg = {"log_fields": {"custom_fields": [_USER_FIELD, *get_token_fields(_BOTH)]}}
    assert reconcile_cfg_system_custom_fields(cfg) == [_USER_FIELD]


def test_field_registry_mirror_matches_provision_names():
    """core can't import provision; the duplicated name set must not drift."""
    assert set(_TOKEN_SYSTEM_FIELDS) == _TOKEN_FIELD_NAMES
    assert _is_system_field(SUBSCRIBER_ID_FIELD)
    assert _is_system_field(CONTENT_ID_FIELD)
    assert not _is_system_field("token_my_own_field")
    assert not _is_system_field("cs_sub_id_extra")
    assert not _is_system_field("cs_content_id_extra")


# ── generated VCL ordering (Trap #29) ────────────────────────────────────────


def _assert_ordered(recv: str, vcl: dict) -> None:
    unset, set_, capture = recv.find(vcl["unset"]), recv.find(vcl["set"]), recv.find(vcl["capture"])
    assert -1 not in (unset, set_, capture), recv
    assert unset < set_ < capture, "scrub, then token extraction, then field capture"
    # The unset sits inside the contiguous block of scrub unsets.
    lines = [ln.strip() for ln in recv.splitlines()]
    i = lines.index(vcl["unset"])
    assert lines[i - 1].startswith("unset "), f"{vcl['unset']} is not grouped with the scrubs: {lines[i - 1]!r}"
    assert recv.count(vcl["unset"]) == 1


def test_capture_vcl_orders_extraction_before_capture_for_both_ids():
    lf = {"groups": ["A"], "custom_fields": get_token_fields(_BOTH)}
    recv = generate_capture_vcl(lf, token_subscriber_id_expr=_SUB_EXPR, token_content_id_expr=_CID_EXPR)["recv"]
    _assert_ordered(recv, _SUB)
    _assert_ordered(recv, _CID)


def test_capture_vcl_omits_extraction_when_disabled():
    recv = generate_capture_vcl({"groups": ["A"], "custom_fields": []})["recv"]
    assert "x-cs-sub-id" not in recv and "x-cs-content-id" not in recv


def _state(**token_kw) -> FeatureState:
    return FeatureState.from_config(
        {
            "service_id": "svcTOKEN",
            "log_period": 60,
            "token": token_kw,
            "log_fields": {"groups": ["A"], "custom_fields": []},
        }
    )


def test_declarative_state_injects_both_fields_and_orders_extraction():
    state = _state(enabled=True, subscriber_id_expr=_SUB_EXPR, content_id_expr=_CID_EXPR)
    assert _BOTH <= {cf["name"] for cf in state.log_fields.custom_fields}
    recv = generate_consolidated_snippet(state, "vcl_recv")
    _assert_ordered(recv, _SUB)
    _assert_ordered(recv, _CID)


def test_declarative_state_content_id_only():
    state = _state(enabled=True, content_id_expr=_CID_EXPR)
    names = {cf["name"] for cf in state.log_fields.custom_fields}
    assert CONTENT_ID_FIELD in names and SUBSCRIBER_ID_FIELD not in names
    recv = generate_consolidated_snippet(state, "vcl_recv")
    _assert_ordered(recv, _CID)
    assert "x-cs-sub-id" not in recv


def test_declarative_state_strips_stale_field_for_cleared_id():
    state = FeatureState.from_config(
        {
            "service_id": "svcTOKEN",
            "log_period": 60,
            "token": {"enabled": True, "subscriber_id_expr": _SUB_EXPR, "content_id_expr": ""},
            "log_fields": {"groups": ["A"], "custom_fields": get_token_fields(_BOTH)},
        }
    )
    assert CONTENT_ID_FIELD not in {cf["name"] for cf in state.log_fields.custom_fields}


def test_declarative_state_disabled_emits_nothing():
    state = _state()
    assert not _BOTH & {cf["name"] for cf in state.log_fields.custom_fields}
    recv = generate_consolidated_snippet(state, "vcl_recv")
    assert "x-cs-sub-id" not in recv and "x-cs-content-id" not in recv


@pytest.mark.parametrize(
    "token",
    [
        TokenConfig(enabled=True, subscriber_id_expr="a; b"),
        TokenConfig(enabled=True, content_id_expr="a // b"),
        TokenConfig(enabled=True),  # neither id configured
    ],
)
def test_declarative_state_rejects_invalid_token(token):
    with pytest.raises(ValueError):
        FeatureState(
            service_id="svcTOKEN",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
            token=token,
            log_fields=LogFieldsConfig(),
        )


# ── enforcement fields (Content Security outcome, set by the operator's VCL) ──


_ENFORCEMENT = frozenset({e.field for e in ENFORCEMENT_FIELDS})


def test_enforcement_fields_need_token_enabled_and_the_flag():
    assert configured_token_fields({"enabled": True, "subscriber_id_expr": _SUB_EXPR}) == {SUBSCRIBER_ID_FIELD}
    assert configured_token_fields({"enabled": False, "enforcement": True}) == frozenset()
    with_flag = configured_token_fields({"enabled": True, "subscriber_id_expr": _SUB_EXPR, "enforcement": True})
    assert with_flag == {SUBSCRIBER_ID_FIELD} | _ENFORCEMENT


def test_enforcement_fields_are_deliver_stage_strings_reading_the_operators_headers():
    defs = {cf["name"]: cf for cf in get_token_fields(_ENFORCEMENT)}
    assert set(defs) == {"cs_status", "cs_mode", "cs_rl_mode", "cs_pace_kbps", "cs_policy_id", "cs_policy_reason"}
    assert defs["cs_status"]["vcl_log_expression"] == "req.http.cs-status"
    assert defs["cs_rl_mode"]["vcl_log_expression"] == "req.http.cs-rl-mode"
    assert all(cf["collection_stage"] == "deliver" and cf["duckdb_type"] == "VARCHAR" for cf in defs.values())
    # "check_failed" / "rate_limited" are the longest values; the cap must hold them.
    assert defs["cs_status"]["byte_limit"] >= len("check_failed")
    # A UUID policy id and the longest reason ("exempt:proxy_description") must fit their caps.
    assert defs["cs_policy_id"]["vcl_log_expression"] == "req.http.cs-policy"
    assert defs["cs_policy_id"]["byte_limit"] >= 6 * 36
    assert defs["cs_policy_reason"]["vcl_log_expression"] == "req.http.cs-reason"
    assert defs["cs_policy_reason"]["byte_limit"] >= 6 * len("exempt:proxy_description")


def test_enforcement_fields_follow_token_fields_in_canonical_order():
    out = reconcile_token_custom_fields([_USER_FIELD], fields=_ENFORCEMENT | _BOTH)
    assert [cf["name"] for cf in out] == [
        "my_custom",
        SUBSCRIBER_ID_FIELD,
        CONTENT_ID_FIELD,
        "cs_status",
        "cs_mode",
        "cs_rl_mode",
        "cs_pace_kbps",
        "cs_policy_id",
        "cs_policy_reason",
    ]


def test_cfg_reconcile_reasserts_and_drops_enforcement_fields():
    cfg = {"token": {"enabled": True, "subscriber_id_expr": _SUB_EXPR, "enforcement": True}}
    names = {cf["name"] for cf in reconcile_cfg_system_custom_fields(cfg)}
    assert _ENFORCEMENT <= names
    cfg["token"]["enforcement"] = False
    names = {cf["name"] for cf in reconcile_cfg_system_custom_fields(cfg)}
    assert not _ENFORCEMENT & names


def test_enforcement_headers_are_scrubbed_only_while_their_field_is_enabled():
    on = get_scrub_vcl_statements({"custom_fields": get_token_fields(_ENFORCEMENT)})
    assert {"unset req.http.cs-status;", "unset req.http.cs-rl-mode;"} <= set(on)
    assert "unset req.http.cs-status;" not in get_scrub_vcl_statements({"custom_fields": []})


def test_enforcement_fields_are_promoted_in_vcl_deliver():
    from backend.provision.fastly_api import generate_capture_vcl

    snippets = generate_capture_vcl({"groups": ["A"], "custom_fields": get_token_fields(_ENFORCEMENT)})
    deliver = snippets["deliver"]
    assert 'req.http.cs-status != ""' in deliver
    assert "set req.http.x-fos-edge-data:cs_status = req.http.cs-status;" in deliver
