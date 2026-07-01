"""Unit tests for the (range_token, anchor) -> window resolver.

Covers the deterministic resolution + anchor quantization + adaptive "auto"
bands + the invite-clamp fingerprint that the network relative-range keyed path
(the 30d analyst-cliff fix) depends on. The cache-key partitioning + clamp-
ceiling invariants live in tests/repositories/test_network.py (security_regression).
"""

from datetime import UTC, datetime, timedelta

import pytest

from backend.utils.time_window import (
    ANCHOR_QUANTUM_SECONDS,
    VALID_RANGE_TOKENS,
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
    resolve_window,
)

_NOW = datetime(2026, 6, 29, 12, 30, 45, tzinfo=UTC)


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ── token vocabulary ──────────────────────────────────────────────────────────


def test_valid_tokens_are_recognized():
    assert is_valid_range_token("24h")
    assert is_valid_range_token("7d")
    assert is_valid_range_token("30d")
    assert is_valid_range_token("auto")
    assert VALID_RANGE_TOKENS == {"24h", "7d", "30d", "auto"}


def test_unknown_token_falls_through_not_raises():
    # An additive wire field must degrade to the legacy path, never reject.
    assert not is_valid_range_token(None)
    assert not is_valid_range_token("")
    assert not is_valid_range_token("90d")
    assert not is_valid_range_token("nonsense")


# ── anchor quantization ───────────────────────────────────────────────────────


def test_quantize_floors_to_quantum():
    q = quantize_anchor("2026-06-29T12:30:45Z")
    assert q == "2026-06-29T12:30:00Z"  # seconds floored to the 60s grid


def test_quantize_is_stable_within_quantum():
    a = quantize_anchor("2026-06-29T12:30:01Z")
    b = quantize_anchor("2026-06-29T12:30:59Z")
    assert a == b  # same minute → same key fragment (the cliff fix)
    c = quantize_anchor("2026-06-29T12:31:00Z")
    assert a != c  # next minute → distinct


def test_quantize_missing_anchor_uses_now():
    q = quantize_anchor(None, now=_NOW)
    assert q == "2026-06-29T12:30:00Z"


def test_quantize_invalid_anchor_uses_now():
    q = quantize_anchor("not-a-date", now=_NOW)
    assert q == "2026-06-29T12:30:00Z"


def test_anchor_quantum_aligns_with_memo_ttl_intent():
    # 60s quantum keeps a 30s-TTL memo entry reachable across rolling reloads.
    assert ANCHOR_QUANTUM_SECONDS == 60


# ── fixed-token resolution ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "token,delta",
    [("24h", timedelta(hours=24)), ("7d", timedelta(days=7)), ("30d", timedelta(days=30))],
)
def test_fixed_tokens_resolve_to_anchor_minus_delta(token, delta):
    start, end = resolve_window(token, _NOW.isoformat(), now=_NOW)
    # end == quantized anchor; start == quantized anchor - delta.
    assert end == "2026-06-29T12:30:00Z"
    assert _dt(end) - _dt(start) == delta


def test_resolution_uses_quantized_anchor():
    # Two sub-minute anchors resolve to the IDENTICAL window (stable key).
    w1 = resolve_window("7d", "2026-06-29T12:30:05Z", now=_NOW)
    w2 = resolve_window("7d", "2026-06-29T12:30:55Z", now=_NOW)
    assert w1 == w2


def test_resolve_unknown_token_raises():
    # Programming-error guard: the router gates with is_valid_range_token first.
    with pytest.raises(ValueError):
        resolve_window("90d", _NOW.isoformat(), now=_NOW)


# ── "auto" adaptive default (ports pickInsightsDefault bands) ──────────────────


@pytest.mark.parametrize(
    "history_hours,expected",
    [
        (1, "24h"),  # brand-new service → small fast window
        (6 * 24, "24h"),  # <7d → 24h
        (10 * 24, "7d"),  # 7d..30d → 7d
        (29 * 24, "7d"),
        (45 * 24, "30d"),  # >=30d → 30d
    ],
)
def test_auto_picks_band_from_history(history_hours, expected):
    earliest = (_NOW - timedelta(hours=history_hours)).isoformat()
    start, end = resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW)
    fixed_start, fixed_end = resolve_window(expected, _NOW.isoformat(), now=_NOW)
    assert (start, end) == (fixed_start, fixed_end)


def test_auto_with_no_extents_falls_back_to_7d():
    # Unknown history → safe middle default, NOT the 30d cost.
    auto = resolve_window("auto", _NOW.isoformat(), earliest_log_at=None, now=_NOW)
    seven = resolve_window("7d", _NOW.isoformat(), now=_NOW)
    assert auto == seven


def test_auto_band_boundary_is_higher_bucket():
    # Exactly 7d of history → "7d" (half-open, boundary selects higher bucket).
    earliest = (_NOW - timedelta(days=7)).isoformat()
    auto = resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW)
    assert auto == resolve_window("7d", _NOW.isoformat(), now=_NOW)


def test_auto_date_only_extent_widened_to_utc_start_of_day():
    # A date-only extent must parse (mirrors historyHoursFromExtents widening).
    auto = resolve_window("auto", _NOW.isoformat(), earliest_log_at="2026-06-01", now=_NOW)
    # 2026-06-01 → ~28d of history → still in the <30d band → "7d".
    assert auto == resolve_window("7d", _NOW.isoformat(), now=_NOW)


# ── invite-clamp fingerprint ──────────────────────────────────────────────────


class _Session:
    def __init__(self, qs=None, qe=None, qw=None):
        self.query_start_time = qs
        self.query_end_time = qe
        self.query_window_hours = qw


def test_fingerprint_none_for_admin():
    assert invite_clamp_fingerprint(None) is None


def test_fingerprint_distinct_per_invite_shape():
    open_fp = invite_clamp_fingerprint(_Session())  # all-None invite (open)
    restricted = invite_clamp_fingerprint(_Session(qs="2026-01-01T00:00:00Z", qe="2026-02-01T00:00:00Z"))
    windowed = invite_clamp_fingerprint(_Session(qw=24))
    # Open invite folds to the same "||" digest regardless of being a session
    # vs admin-None? No — admin is None; an all-None SESSION is a real (open)
    # fingerprint. Each distinct shape is distinct from the others.
    assert restricted != windowed
    assert open_fp != restricted
    assert open_fp != windowed
    # Fixed-width 16-hex digest, no raw invite timestamps leaked into the key.
    assert len(restricted) == 16


def test_fingerprint_is_stable_for_same_shape():
    a = invite_clamp_fingerprint(_Session(qw=168))
    b = invite_clamp_fingerprint(_Session(qw=168))
    assert a == b
