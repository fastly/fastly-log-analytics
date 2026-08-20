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

from backend.core.log_fields import check_log_line_budget, estimate_log_line_bytes, generate_log_format
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
