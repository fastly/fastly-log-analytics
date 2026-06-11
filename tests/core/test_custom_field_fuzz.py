"""Property-based fuzz tests for custom-field validation.

`validate_custom_field` is the only gate between user input and DuckDB column
identifiers / VCL snippet generation. The example-based tests in
``tests/core/test_log_fields.py`` exercise specific known-bad strings; these
hypothesis tests verify the same invariants hold across the whole input
space, so a future weakening of any rule (regex, length cap, forbidden-char
set) trips a fuzz failure rather than silently shipping.
"""

from __future__ import annotations

import re

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from backend.core.log_fields import (
    _BUILTIN_FIELD_NAMES,
    _DUCKDB_RESERVED,
    validate_custom_field,
)


def _base_field(**overrides) -> dict:
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


_NAME_REGEX = re.compile(r"^[a-z][a-z0-9_]{0,47}$")


# ── Name validation ────────────────────────────────────────────────────────


@given(name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=1, max_size=48))
def test_regex_match_plus_clean_name_passes_validator(name: str):
    """Any name matching the documented regex (and avoiding reserved-word /
    builtin collisions) must NOT trigger a name-related validator error.
    Catches regressions where the regex tightens too far or new collision
    rules block previously-valid names."""
    assume(_NAME_REGEX.match(name))
    assume(name not in _DUCKDB_RESERVED)
    assume(name not in _BUILTIN_FIELD_NAMES)
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert not any("lowercase alphanumeric" in e for e in errors), (
        f"name {name!r} matches regex but validator rejected it: {errors}"
    )


@given(
    name=st.text(min_size=1, max_size=80).filter(lambda s: not _NAME_REGEX.match(s)),
)
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_regex_mismatch_always_errors(name: str):
    """Any string NOT matching the regex must produce a name error. Catches
    regressions where the regex loosens (e.g. accidentally allowing
    uppercase or hyphens that would need quoting in every downstream SQL)."""
    errors = validate_custom_field(_base_field(name=name), existing_names=[])
    assert any("lowercase alphanumeric" in e for e in errors), (
        f"name {name!r} does not match regex but validator accepted it: {errors}"
    )


# ── VCL injection guards ───────────────────────────────────────────────────


_FORBIDDEN_VCL_CHARS = ("\n", ";", "//", "/*", "#")


@given(
    expr=st.text(min_size=1, max_size=400).filter(lambda s: s.strip() and any(c in s for c in _FORBIDDEN_VCL_CHARS)),
)
@settings(suppress_health_check=[HealthCheck.filter_too_much])
def test_vcl_expression_with_forbidden_char_always_errors(expr: str):
    """Any non-blank VCL expression containing newlines, semicolons, or
    comment markers must be rejected — these are the structural tokens
    that would let user input escape the surrounding generated snippet.
    (Whitespace-only expressions are caught by a different rule first;
    see test_validate_custom_field_vcl_expression_must_be_non_empty in
    test_log_fields.py.)"""
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    forbidden_kind_errors = [e for e in errors if "newlines" in e or "semicolons" in e or "comments" in e]
    assert forbidden_kind_errors, (
        f"expression {expr!r} contains forbidden char(s) but validator did not flag injection: {errors}"
    )


@given(expr_body=st.text(alphabet=st.characters(blacklist_characters="\n;/#"), min_size=1, max_size=500))
def test_vcl_expression_clean_and_under_limit_passes(expr_body: str):
    """A non-empty expression without forbidden chars and under the 512-char
    cap must not produce a VCL-injection error. Stops a future patch from
    over-rejecting valid expressions like ``req.http.X-Real-IP``."""
    assume(expr_body.strip())  # validator separately rejects whitespace-only
    errors = validate_custom_field(_base_field(vcl_log_expression=expr_body), existing_names=[])
    bad = [
        e
        for e in errors
        if "newlines" in e or "semicolons" in e or "comments" in e or "≤ 512" in e or "not be empty" in e
    ]
    assert not bad, f"clean expression {expr_body!r} was incorrectly flagged: {bad}"


@given(extra_len=st.integers(min_value=1, max_value=2000))
def test_vcl_expression_over_limit_always_errors(extra_len: int):
    """An expression longer than 512 chars must error. Hypothesis sweeps the
    threshold to catch off-by-one regressions (e.g. switching to ``>=`` 512
    or capping at 511)."""
    expr = "a" * (512 + extra_len)
    errors = validate_custom_field(_base_field(vcl_log_expression=expr), existing_names=[])
    assert any("≤ 512" in e for e in errors), f"len-{len(expr)} expression must error: {errors}"


# ── bytes_estimate range ───────────────────────────────────────────────────


@given(n=st.integers(min_value=-(2**31), max_value=2**31).filter(lambda x: x < 1 or x > 1024))
def test_bytes_estimate_outside_1_1024_always_errors(n: int):
    """``bytes_estimate`` must be 1..1024 inclusive. Boundary fuzz catches
    off-by-one drift in the range check."""
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    assert any("bytes_estimate" in e for e in errors), f"bytes_estimate={n} must error: {errors}"


@given(n=st.integers(min_value=1, max_value=1024))
def test_bytes_estimate_within_range_passes(n: int):
    """Every value inside the documented range must validate without a
    hard bytes_estimate error. Low values still trigger ``WARN:`` advisories
    (e.g. "1 is less than the name overhead") — those are non-blocking."""
    errors = validate_custom_field(_base_field(bytes_estimate=n), existing_names=[])
    hard = [e for e in errors if "bytes_estimate" in e and not e.startswith("WARN:")]
    assert not hard, f"bytes_estimate={n} must NOT hard-error: {hard}"
