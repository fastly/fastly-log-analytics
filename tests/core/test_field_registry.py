"""Unit tests for the typed log field registry."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

import backend.core.log_fields as fr
from backend.core import log_fields as lf
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    BY_CODE,
    BY_GROUP,
    DEFAULT_OFF_FIELD_IDS,
    INSIGHT_DEFINITIONS,
    LOG_FIELD_CATALOG,
    REGISTRY,
    LogField,
)


def test_registry_invariants_and_parity() -> None:
    assert isinstance(REGISTRY, tuple)
    assert len(REGISTRY) > 0
    assert all(isinstance(f, LogField) for f in REGISTRY)
    with pytest.raises((AttributeError, TypeError)):
        REGISTRY[0].code = "mutated"  # type: ignore[misc]
    with pytest.raises(TypeError):
        BY_CODE["bogus"] = REGISTRY[0]  # type: ignore[index]

    for f in REGISTRY:
        assert fr.get(f.code) is f
        assert fr.try_get(f.code) is f
        assert f.is_derived is (f.vcl is None)
        assert isinstance(f.required_by, tuple)
        assert all(isinstance(i, str) and i for i in f.required_by)
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
            assert f.group is group
            assert f.code not in seen
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
