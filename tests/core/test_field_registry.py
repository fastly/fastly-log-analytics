"""Tests for the Phase 7 FieldRegistry scaffolding (backend/core/field_registry.py).

The registry is scaffolding: no callers have migrated yet, so these tests
exercise the registry itself, NOT any caller's use of it. The single most
important assertion is parity with the legacy `LOG_FIELD_CATALOG` — if those
two views ever diverge, every downstream Rust scorer byte-offset and every
emitted VCL log line is at risk.

Adding a `@pytest.mark.security_regression` marker to two of the parity
tests captures the security-relevant invariants the registry is supposed
to preserve:

  - `test_wire_order_matches_legacy_emission_order` guards Rust scorer
    byte-pinning, which the legacy comments call out explicitly.
  - `test_security_hook_codes_match_legacy_hooks` guards against a new
    field landing without a `json.escape(...)` / digit-regex guard.
"""

from __future__ import annotations

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from backend.core import field_registry as fr
from backend.core.field_registry import (
    BY_CODE,
    BY_GROUP,
    REGISTRY,
    SECURITY_HOOK_CODES,
    WIRE_ORDER,
    Agg,
    DuckType,
    FilterOp,
    Group,
    LogField,
)
from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    GROUP_DEPENDENCIES,
    LOG_FIELD_CATALOG,
)

# A regex matching the legacy security-hook convention. Kept here so the
# tests below don't import the private regex from the production module
# (the production module's regex is the canonical source; this duplicate
# is the audit copy).
_LEGACY_SEC_HOOK = re.compile(r"json\.escape\(|~\s*\"\^")


# ---------------------------------------------------------------------------
# 1. Smoke
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 2. Per-field round-trip: every declared field's code resolves to itself
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 3. Coverage: the registry knows every code in the legacy constants
# ---------------------------------------------------------------------------


def test_registry_codes_match_log_fields() -> None:
    """The registry covers every code in the legacy LOG_FIELD_CATALOG.

    This is the parity guarantee that lets callers migrate one-at-a-time:
    while both views are live, they must agree on the set of codes.
    """
    legacy = {entry["id"] for entry in LOG_FIELD_CATALOG}
    new = fr.all_codes()
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
    assert fr.all_codes() == set(_BUILTIN_FIELD_NAMES)


@pytest.mark.security_regression
def test_wire_order_matches_legacy_emission_order() -> None:
    """WIRE_ORDER is byte-identical to the legacy catalog's VCL emission order.

    The Rust scorer reads positional JSON keys; reordering REGISTRY rows
    without coordinating with `compute/` silently breaks scorer parity.
    This test is the boot-time gate that fails loudly in CI.
    """
    legacy_emission_order = tuple(entry["id"] for entry in LOG_FIELD_CATALOG if entry["vcl"] is not None)
    assert WIRE_ORDER == legacy_emission_order


def test_default_off_codes_match_legacy_catalog() -> None:
    """The registry's ``DEFAULT_OFF_CODES`` covers exactly the catalog entries
    flagged ``default_off`` — and agrees with ``log_fields.DEFAULT_OFF_FIELD_IDS``.

    ``default_off`` fields are opt-in (excluded from ``resolve_enabled_fields``
    unless explicitly enabled); a divergence between the two views would let a
    field be opt-in in one resolution path and auto-on in another.
    """
    from backend.core.log_fields import DEFAULT_OFF_FIELD_IDS

    legacy = {entry["id"] for entry in LOG_FIELD_CATALOG if entry.get("default_off")}
    assert fr.DEFAULT_OFF_CODES == legacy
    assert fr.DEFAULT_OFF_CODES == DEFAULT_OFF_FIELD_IDS
    assert "cookie_session" in fr.DEFAULT_OFF_CODES


@pytest.mark.security_regression
def test_security_hook_codes_match_legacy_hooks() -> None:
    """Every field the registry tags as security-hooked has a `json.escape`
    or digit-regex guard in its legacy VCL string.

    This guards against a new field landing without a hook: if anyone adds
    a VCL expression that interpolates an attacker-influenced value and
    forgets json.escape, the SECURITY_HOOK_CODES set will be smaller than
    expected and the security regression sweep will catch the omission.
    """
    legacy_hooks = {
        entry["id"]
        for entry in LOG_FIELD_CATALOG
        if entry.get("vcl") is not None and _LEGACY_SEC_HOOK.search(entry["vcl"]) is not None
    }
    assert SECURITY_HOOK_CODES == legacy_hooks


# ---------------------------------------------------------------------------
# 4. Group invariants
# ---------------------------------------------------------------------------


def test_by_group_partitions_registry() -> None:
    """Every field appears in exactly one group bucket. Together they cover REGISTRY."""
    seen: set[str] = set()
    for group, fields in BY_GROUP.items():
        for f in fields:
            assert f.group is group
            assert f.code not in seen, f"{f.code} appears in multiple group buckets"
            seen.add(f.code)
    assert seen == fr.all_codes()


def test_group_dependencies_match_legacy() -> None:
    """The dataclass-side `_GROUP_REQS` agrees with legacy GROUP_DEPENDENCIES."""
    # Translate legacy (string-keyed) to the enum-keyed registry view.
    legacy_translated = {Group.from_legacy(g): Group.from_legacy(req) for g, req in GROUP_DEPENDENCIES.items()}
    assert legacy_translated == dict(fr._GROUP_REQS)


def test_core_group_fields_are_always_on() -> None:
    """`is_always_on` is True for CORE group, False for everything else."""
    for f in REGISTRY:
        assert f.is_always_on is (f.group is Group.CORE)


# ---------------------------------------------------------------------------
# 5. Derivation invariants (vcl=None → derived; derived → no aggs missing)
# ---------------------------------------------------------------------------


def test_derived_fields_have_no_vcl() -> None:
    """`is_derived` is exactly the `vcl is None` predicate."""
    for f in REGISTRY:
        assert f.is_derived is (f.vcl is None)


def test_loggable_fields_emit_vcl() -> None:
    """`loggable()` returns fields whose `render_vcl()` is non-None."""
    for f in fr.loggable():
        assert f.render_vcl() is not None
    for f in fr.derived():
        assert f.render_vcl() is None


def test_render_vcl_no_limits_returns_baseline() -> None:
    """`render_vcl()` without overrides matches the raw catalog VCL byte-for-byte."""
    for f in REGISTRY:
        baseline = f.render_vcl()
        assert baseline == f.vcl


def test_render_vcl_substr_cap_override() -> None:
    """`render_vcl({code: N})` substitutes the cap inside the substr literal."""
    url = BY_CODE["url"]
    assert url.substr_cap == 2000
    rendered = url.render_vcl({"url": 500})
    assert rendered is not None
    assert "substr(req.url, 0, 500)" in rendered
    assert "substr(req.url, 0, 2000)" not in rendered

    # Override matching the default is a no-op (no string change).
    same = url.render_vcl({"url": 2000})
    assert same == url.vcl


# ---------------------------------------------------------------------------
# 6. Type-driven aggregation/operator derivation
# ---------------------------------------------------------------------------


def test_numeric_fields_support_sum_avg() -> None:
    """Every numeric column supports SUM and AVG."""
    for f in REGISTRY:
        if f.duck_type in fr._NUMERIC:
            assert Agg.SUM in f.valid_aggs
            assert Agg.AVG in f.valid_aggs
            assert FilterOp.GT in f.valid_ops


def test_boolean_fields_reject_string_ops() -> None:
    """Boolean columns get only eq/neq filter operators."""
    for f in REGISTRY:
        if f.duck_type is DuckType.BOOLEAN:
            assert f.valid_ops == frozenset({FilterOp.EQ, FilterOp.NEQ})
            assert Agg.SUM not in f.valid_aggs


def test_varchar_fields_support_contains() -> None:
    """VARCHAR columns support CONTAINS but not arithmetic ops."""
    for f in REGISTRY:
        if f.duck_type is DuckType.VARCHAR:
            assert FilterOp.CONTAINS in f.valid_ops
            assert FilterOp.STARTS_WITH in f.valid_ops
            assert Agg.SUM not in f.valid_aggs


def test_timestamp_fields_support_only_range_ops() -> None:
    """TIMESTAMP columns support range comparisons but not CONTAINS / IN."""
    for f in REGISTRY:
        if f.duck_type is DuckType.TIMESTAMP:
            assert FilterOp.GT in f.valid_ops
            assert FilterOp.CONTAINS not in f.valid_ops
            assert FilterOp.IN not in f.valid_ops


# ---------------------------------------------------------------------------
# 7. Hypothesis: every field round-trips through validation without raising
# ---------------------------------------------------------------------------


@given(code=st.sampled_from(sorted(f.code for f in REGISTRY)))
def test_hypothesis_every_code_lookup_succeeds(code: str) -> None:
    """For every known code, `get` returns a field whose `.code` matches input."""
    f = fr.get(code)
    assert f.code == code
    # Derived properties never raise for any known field.
    _ = f.valid_aggs
    _ = f.valid_ops
    _ = f.has_security_hook
    _ = f.is_derived
    _ = f.is_always_on
    _ = f.render_vcl()


@given(
    code=st.sampled_from(sorted(f.code for f in REGISTRY)),
    cap=st.integers(min_value=1, max_value=10_000),
)
def test_hypothesis_render_vcl_never_raises(code: str, cap: int) -> None:
    """`render_vcl({code: cap})` returns either None or a non-empty string,
    never raises, for any combination of known code and reasonable cap.
    """
    f = fr.get(code)
    out = f.render_vcl({code: cap})
    if f.vcl is None:
        assert out is None
    else:
        assert isinstance(out, str)
        assert len(out) > 0


@given(op_value=st.sampled_from([op.value for op in FilterOp]))
def test_hypothesis_filter_op_enum_roundtrips(op_value: str) -> None:
    """Every FilterOp value round-trips through the enum constructor."""
    assert FilterOp(op_value).value == op_value


@given(agg_value=st.sampled_from([a.value for a in Agg]))
def test_hypothesis_agg_enum_roundtrips(agg_value: str) -> None:
    """Every Agg value round-trips through the enum constructor."""
    assert Agg(agg_value).value == agg_value


# ---------------------------------------------------------------------------
# 8. Required-by integrity (insight references must point at real fields)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 9. Re-export parity: every helper + constant on the registry is the same
#    object as the legacy module's name. Lets callers flip imports without
#    behavior drift; guards against an accidental shadow / re-implementation.
# ---------------------------------------------------------------------------

# Helpers re-exported from `backend.core.log_fields`. Identity equality is
# the strong invariant: same function object, not just same return value.
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

# Constants/objects re-exported from `backend.core.log_fields`. Same object
# identity ensures a mutation through `log_fields` is observed by registry
# callers — a hard constraint of the migration plan.
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
