"""Token (subscriber id) system field + VCL extraction.

The subscriber id is extracted into ``req.http.x-subscriber-id`` by an
admin-supplied VCL expression and promoted into the log line by the
system-managed ``token_subscriber_id`` field. Same two failure modes as CMCD:
the field silently dropping out of ``log_fields`` (Trap #27) and extraction
being emitted after capture (Trap #29).
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
    SUBSCRIBER_ID_FIELD,
    generate_token_vcl_lines,
    get_token_fields,
    reconcile_token_custom_fields,
    validate_subscriber_id_expr,
)

_EXPR = 'regsub(req.http.Authorization, "^Bearer ", "")'
_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}

_UNSET = "unset req.http.x-subscriber-id;"
_SET = f"set req.http.x-subscriber-id = {_EXPR};"
_CAPTURE = f"set req.http.x-fos-edge-data:{SUBSCRIBER_ID_FIELD} = req.http.x-subscriber-id;"


# ── validation ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("expr", [_EXPR, "req.http.X-Subscriber", 'subfield(req.http.CAT, "sub", ",")'])
def test_valid_expressions_pass(expr):
    assert validate_subscriber_id_expr(expr) == []


@pytest.mark.parametrize(
    "expr",
    [
        "",
        "   ",
        'req.http.a; set req.http.b = "x"',  # statement injection
        "req.http.a # hidden",
        "req.http.a // hidden",
        "req.http.a\nset req.http.b = 1",
        "x" * 513,
    ],
)
def test_unsafe_or_empty_expressions_rejected(expr):
    assert validate_subscriber_id_expr(expr)


def test_generate_lines_only_set():
    """The unset lives with the other scrubs, not beside the set."""
    lines = generate_token_vcl_lines(f"  {_EXPR}  ")
    assert _SET in lines
    assert _UNSET not in lines


def test_scrub_unsets_header_only_when_token_field_enabled():
    assert _UNSET in get_scrub_vcl_statements({"custom_fields": get_token_fields(True)})
    assert _UNSET not in get_scrub_vcl_statements({"custom_fields": []})
    disabled = [{**get_token_fields(True)[0], "enabled": False}]
    assert _UNSET not in get_scrub_vcl_statements({"custom_fields": disabled})


def test_generate_lines_refuses_invalid_expression():
    with pytest.raises(ValueError):
        generate_token_vcl_lines("req.http.a; esi")


# ── system field reconciliation ──────────────────────────────────────────────


def test_reconcile_adds_and_strips():
    on = reconcile_token_custom_fields([_USER_FIELD], enabled=True)
    assert [cf["name"] for cf in on] == ["my_custom", SUBSCRIBER_ID_FIELD]
    assert reconcile_token_custom_fields(on, enabled=False) == [_USER_FIELD]


def test_flags_include_token():
    assert system_feature_flags({"token": {"enabled": True}}) == (False, False, True)
    assert system_feature_flags({"token": None}) == (False, False, False)


def test_cfg_reconcile_reasserts_token_without_a_transition():
    """Trap #27: keyed on current state, so an already-stripped config heals."""
    cfg = {"token": {"enabled": True, "subscriber_id_expr": _EXPR}, "log_fields": {"custom_fields": [_USER_FIELD]}}
    names = [cf["name"] for cf in reconcile_cfg_system_custom_fields(cfg)]
    assert names == ["my_custom", SUBSCRIBER_ID_FIELD]


def test_cfg_reconcile_strips_token_when_disabled():
    cfg = {"log_fields": {"custom_fields": [_USER_FIELD, *get_token_fields(True)]}}
    assert reconcile_cfg_system_custom_fields(cfg) == [_USER_FIELD]


def test_field_registry_mirror_matches_provision_names():
    """core can't import provision; the duplicated name set must not drift."""
    assert set(_TOKEN_SYSTEM_FIELDS) == _TOKEN_FIELD_NAMES
    assert _is_system_field(SUBSCRIBER_ID_FIELD)
    assert not _is_system_field("token_my_own_field")


# ── generated VCL ordering (Trap #29) ────────────────────────────────────────


def _assert_ordered(recv: str) -> None:
    unset, set_, capture = recv.find(_UNSET), recv.find(_SET), recv.find(_CAPTURE)
    assert -1 not in (unset, set_, capture), recv
    assert unset < set_ < capture, "scrub, then token extraction, then field capture"
    # The unset sits inside the contiguous block of scrub unsets.
    lines = [ln.strip() for ln in recv.splitlines()]
    i = lines.index(_UNSET)
    assert lines[i - 1].startswith("unset "), f"x-subscriber-id unset is not grouped with the scrubs: {lines[i - 1]!r}"
    assert recv.count(_UNSET) == 1


def test_capture_vcl_orders_extraction_before_capture():
    lf = {"groups": ["A"], "custom_fields": get_token_fields(True)}
    _assert_ordered(generate_capture_vcl(lf, token_subscriber_id_expr=_EXPR)["recv"])


def test_capture_vcl_omits_extraction_when_disabled():
    lf = {"groups": ["A"], "custom_fields": []}
    assert "x-subscriber-id" not in generate_capture_vcl(lf)["recv"]


def _state(**token_kw) -> FeatureState:
    return FeatureState.from_config(
        {
            "service_id": "svcTOKEN",
            "log_period": 60,
            "token": token_kw,
            "log_fields": {"groups": ["A"], "custom_fields": []},
        }
    )


def test_declarative_state_injects_field_and_orders_extraction():
    state = _state(enabled=True, subscriber_id_expr=_EXPR)
    assert SUBSCRIBER_ID_FIELD in {cf["name"] for cf in state.log_fields.custom_fields}
    _assert_ordered(generate_consolidated_snippet(state, "vcl_recv"))


def test_declarative_state_disabled_emits_nothing():
    state = _state()
    assert SUBSCRIBER_ID_FIELD not in {cf["name"] for cf in state.log_fields.custom_fields}
    assert "x-subscriber-id" not in generate_consolidated_snippet(state, "vcl_recv")


def test_declarative_state_rejects_invalid_expression():
    with pytest.raises(ValueError):
        FeatureState(
            service_id="svcTOKEN",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
            token=TokenConfig(enabled=True, subscriber_id_expr="a; b"),
            log_fields=LogFieldsConfig(),
        )
