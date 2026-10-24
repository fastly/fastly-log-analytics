"""Regression: custom-field byte budgets vs the emitted character cap.

``generate_log_format`` emits ``substr(expr, 0, cf_limit // 6)`` for string
custom fields. ``cf_limit`` is a BYTE budget and the ``// 6`` is worst-case
JSON-escape expansion (a char can encode as ``\\uXXXX``). So the CHARACTER cap
an operator actually gets is ``byte_limit // 6`` — six times smaller than the
number in the config.

That divisor silently truncated CMCD session ids: ``cmcd_sid`` had
``bytes_estimate: 40`` → ``cf_limit`` 160 → **26 characters**, cutting 36-char
UUIDs and breaking session-level joins. The column held both 26- and 40-char
values (the longer ones written before the current format), which reads like a
client quirk rather than a config effect.

The cap itself is a deliberate guard, not an accident: the value is
client-supplied (CMCD arrives in ``?CMCD=``), so an unbounded field would let a
caller push the log line past Fastly's ~16 KB limit and silently drop the whole
entry. These tests pin "big enough for a real id" rather than "unbounded".
"""

from __future__ import annotations

import re

from backend.core.log_fields import (
    check_log_line_budget,
    estimate_log_line_bytes,
    generate_log_format,
    get_catalog_for_api,
)
from backend.provision.cmcd_fields import get_cmcd_fields
from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS

# A realistic heavy config: every field group plus both system feature sets.
_CFG = {
    "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "L", "M"],
    "custom_fields": get_cmcd_fields(True) + [dict(c) for c in _SCORING_CUSTOM_FIELDS],
    "field_overrides": {},
}

_UUID_LEN = 36  # canonical CMCD v1 sid


def _caps(fmt: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for m in re.finditer(r'"(\w+)":"?%\{[^}]*?substr\([^,]+,\s*0,\s*(\d+)\)', fmt):
        out.setdefault(m.group(1), int(m.group(2)))
    return out


def test_cmcd_sid_fits_a_full_uuid():
    """THE REGRESSION: 26 chars truncated every UUID session id."""
    caps = _caps(generate_log_format(_CFG))
    assert "cmcd_sid" in caps, "cmcd_sid emitted no substr cap"
    assert caps["cmcd_sid"] >= _UUID_LEN, (
        f"cmcd_sid caps at {caps['cmcd_sid']} chars, truncating {_UUID_LEN}-char UUIDs. "
        "byte_limit is a BYTE budget divided by 6 for JSON-escape expansion — "
        "it must be >= 6 * the character length you want."
    )


def test_cmcd_cid_fits_a_realistic_content_id():
    caps = _caps(generate_log_format(_CFG))
    assert caps.get("cmcd_cid", 0) >= _UUID_LEN


def test_no_custom_field_is_clamped_to_zero():
    """A zero cap logs null forever, silently — the aggregate-budget failure mode."""
    fmt = generate_log_format(_CFG)
    zeros = [m.group(1) for m in re.finditer(r'"(\w+)":[^,]*?substr\([^,]+,\s*0,\s*0\)', fmt)]
    assert zeros == [], f"fields clamped to zero chars (will always log null): {zeros}"


def test_raising_a_byte_limit_raises_the_char_cap():
    """Guards the direction of the relationship.

    Setting byte_limit BELOW 6x the desired chars silently shrinks the cap —
    the trap that made an earlier attempt at this fix produce a 6-char sid.
    """
    import copy

    low = copy.deepcopy(_CFG)
    for cf in low["custom_fields"]:
        if cf["name"] == "cmcd_sid":
            cf["byte_limit"] = 60  # 60 // 6 == 10 chars
    assert _caps(generate_log_format(low))["cmcd_sid"] == 10

    high = copy.deepcopy(_CFG)
    for cf in high["custom_fields"]:
        if cf["name"] == "cmcd_sid":
            cf["byte_limit"] = 600  # 600 // 6 == 100 chars
    assert _caps(generate_log_format(high))["cmcd_sid"] == 100


def test_widened_budgets_stay_inside_the_line_limit():
    """The cap exists so a client-supplied value can't drop the log line."""
    assert check_log_line_budget(_CFG) is None, "widening sid/cid pushed the line over budget"
    assert estimate_log_line_bytes(_CFG) < 16384


def test_catalog_enrichment():
    """Verify that the catalog API output includes UI metadata like formatter and unit."""
    catalog = get_catalog_for_api()

    resp_bytes = next(f for f in catalog if f["id"] == "resp_bytes")
    assert resp_bytes.get("formatter") == "bytes"

    country = next(f for f in catalog if f["id"] == "country")
    assert country.get("formatter") == "country"

    elapsed = next(f for f in catalog if f["id"] == "elapsed")
    assert elapsed.get("unit") == "µs"

    tls = next(f for f in catalog if f["id"] == "tls")
    assert tls.get("formatter") == "number"
    assert tls.get("precision") == 1


# ── Merged from test_custom_field_fuzz.py ──


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
