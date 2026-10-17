"""Pure validation / resolution for Content Security policies."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.utils import cs_policy as p

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("category", "raw", "expected"),
    [
        ("country", " br ", "BR"),
        ("asn", "AS7922", "7922"),
        ("asn", "007922", "7922"),
        ("ip", "2001:DB8::0001", "2001:db8::1"),
        ("ip", "[203.0.113.9]", "203.0.113.9"),
        ("referer", "https://User@Pirate.Example:8080/watch?x=1", "pirate.example"),
        ("referer", "Pirate.Example", "pirate.example"),
        ("host", "Cdn.Example.COM", "cdn.example.com"),
        ("proxy_type", "VPN", "vpn"),
        ("subscriber_id", "Sub-ABC", "Sub-ABC"),
    ],
)
def test_normalize_value(category, raw, expected):
    assert p.normalize_value(category, raw) == expected


@pytest.mark.parametrize(
    ("category", "raw"),
    [
        ("country", "USA"),
        ("asn", "AS0"),
        ("asn", "x"),
        ("asn", "4294967296"),
        ("ip", "999.1.1.1"),
        ("host", "  "),
        ("nope", "x"),
    ],
)
def test_normalize_value_rejects(category, raw):
    with pytest.raises(p.PolicyValidationError):
        p.normalize_value(category, raw)


def test_individual_categories_only_block():
    assert p.normalize_rule("ip", "203.0.113.9", "block")["action"] == "block"
    for action in ("pace", "exempt"):
        with pytest.raises(p.PolicyValidationError):
            p.normalize_rule("subscriber_id", "s", action, 256)


def test_pace_needs_positive_kbps():
    assert p.normalize_rule("country", "br", "pace", 256) == {
        "category": "country",
        "value": "BR",
        "action": "pace",
        "kbps": 256,
    }
    for bad in (None, 0, -1):
        with pytest.raises(p.PolicyValidationError):
            p.normalize_rule("country", "br", "pace", bad)


def test_normalize_rules_dedupes_and_rejects_conflicts():
    rules = p.normalize_rules(
        [
            {"category": "asn", "value": "AS7922", "action": "block"},
            {"category": "asn", "value": "7922", "action": "block"},
            {"category": "country", "value": "us", "action": "exempt"},
        ]
    )
    assert [(r["category"], r["value"]) for r in rules] == [("country", "US"), ("asn", "7922")]
    with pytest.raises(p.PolicyValidationError) as exc:
        p.normalize_rules(
            [
                {"category": "country", "value": "BR", "action": "block"},
                {"category": "country", "value": "br", "action": "exempt"},
            ]
        )
    assert exc.value.code == "conflicting_rules"


@pytest.mark.parametrize("pattern", [r"^(CN|RU)$", "^[A-M].$", "", None])
def test_country_regex_accepts(pattern):
    assert p.validate_country_regex(pattern) in (pattern, None)


@pytest.mark.parametrize(
    "pattern",
    [r"(a)\1", r"(?=x)", r"(?<!x)y", r"(a+)+$", r"(?P<n>x)", "(", "x" * 101],
)
def test_country_regex_rejects(pattern):
    with pytest.raises(p.PolicyValidationError) as exc:
        p.validate_country_regex(pattern)
    assert exc.value.code == "invalid_regex"


def test_regex_preview_matches_iso_codes():
    assert p.regex_matches("^(CN|RU|KP)$", list(p.ISO_COUNTRY_CODES)) == ["CN", "KP", "RU"]
    assert len(p.ISO_COUNTRY_CODES) == 249


def test_window_bounds_start_inclusive_end_exclusive():
    start, end = "2026-10-20T12:00:00Z", "2026-10-20T13:00:00Z"
    assert p.window_contains(start, end, NOW)
    assert not p.window_contains(start, end, datetime(2026, 10, 20, 11, 59, 59, tzinfo=UTC))
    assert not p.window_contains(start, end, datetime(2026, 10, 20, 13, 0, tzinfo=UTC))
    assert p.window_contains(None, None, NOW)


def test_window_rejects_end_before_start():
    with pytest.raises(p.PolicyValidationError):
        p.normalize_window("2026-10-20T13:00:00Z", "2026-10-20T12:00:00Z")
    assert p.normalize_window("2026-10-20T08:00:00-04:00", None) == ("2026-10-20T12:00:00Z", None)


def test_windows_overlap():
    a = ("2026-10-20T10:00:00Z", "2026-10-20T12:00:00Z")
    assert p.windows_overlap(a, ("2026-10-20T11:00:00Z", None))
    assert not p.windows_overlap(a, ("2026-10-20T12:00:00Z", "2026-10-20T14:00:00Z"))  # touching is fine
    assert p.windows_overlap((None, None), a)
    assert not p.windows_overlap(a, (None, "2026-10-20T10:00:00Z"))


def _a(scope, content, start=None, end=None):
    return {"scope_type": scope, "content_id": content, "starts_at": start, "ends_at": end}


def test_pick_assignment_prefers_content_over_default():
    default, ev = _a("default", None), _a("content_id", "ev-1")
    assert p.pick_assignment([default, ev], "ev-1", NOW) is ev
    assert p.pick_assignment([default, ev], "ev-2", NOW) is default
    assert p.pick_assignment([default, ev], None, NOW) is default


def test_pick_assignment_falls_back_when_content_window_inactive():
    default = _a("default", None)
    ended = _a("content_id", "ev-1", end="2026-10-19T00:00:00Z")
    future = _a("content_id", "ev-1", start="2026-10-21T00:00:00Z")
    assert p.pick_assignment([default, ended, future], "ev-1", NOW) is default
    assert p.pick_assignment([ended, future], "ev-1", NOW) is None


def test_content_id_match_is_case_sensitive():
    ev = _a("content_id", "Ev-1")
    assert p.pick_assignment([ev], "ev-1", NOW) is None


def test_assignment_status():
    assert p.assignment_status(None, None, NOW) == "active"
    assert p.assignment_status("2026-10-21T00:00:00Z", None, NOW) == "scheduled"
    assert p.assignment_status(None, "2026-10-20T12:00:00Z", NOW) == "ended"
