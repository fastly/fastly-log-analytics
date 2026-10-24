"""Consolidated tests for test_date_utils."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from backend.core.rollups._common import parse_hour_token
from backend.utils.date_utils import (
    _parse_dt,
    ensure_utc,
    iso_z,
    iso_z_now,
    parse_date_window,
    parse_day_key_strict,
    parse_hour_key,
    parse_hour_key_strict,
    parse_iso_utc,
    parse_relative_time_window,
    parse_window,
    parse_window_str_to_dt,
    safe_iso,
    window_to_epoch,
)


def test_parse_iso_utc():
    # 1. Empty/None inputs
    assert parse_iso_utc(None) is None
    assert parse_iso_utc("") is None

    # 2. ISO format with timezone or Z
    dt1 = parse_iso_utc("2026-08-19T12:00:00Z")
    assert dt1 is not None
    assert dt1.tzinfo == UTC
    assert dt1.hour == 12

    # 3. Naive date format
    dt2 = parse_iso_utc("2026-08-19")
    assert dt2 is not None
    assert dt2.tzinfo == UTC
    assert dt2.hour == 0
    assert dt2.day == 19

    # 4. Invalid inputs
    assert parse_iso_utc("invalid-date-string") is None


def test_iso_z():
    dt = datetime(2026, 8, 19, 12, 34, 56, tzinfo=UTC)
    assert iso_z(dt) == "2026-08-19T12:34:56Z"


def test_iso_z_now():
    now_str = iso_z_now()
    assert now_str.endswith("Z")
    assert "T" in now_str


def test_parse_dt_helper():
    default_val = datetime(2020, 1, 1, tzinfo=UTC)
    # Success path
    assert _parse_dt("2026-08-19T12:00:00Z", default_val).year == 2026
    # Fallback path
    assert _parse_dt("invalid", default_val) == default_val


def test_parse_date_window():
    # Standard 10-char date triggers hour/minute/second replace on end_dt
    start, end = parse_date_window("2026-08-10", "2026-08-19")
    assert start == "2026-08-10T00:00:00Z"
    assert end == "2026-08-19T23:59:59Z"

    # Longer string doesn't replace hours/minutes/seconds
    start2, end2 = parse_date_window("2026-08-10T12:00:00Z", "2026-08-19T12:00:00Z")
    assert start2 == "2026-08-10T12:00:00Z"
    assert end2 == "2026-08-19T12:00:00Z"


def test_safe_iso():
    assert safe_iso(None) is None
    assert safe_iso("") is None

    # Datetime-like object
    dt = datetime(2026, 8, 19, 12, 0, 0)
    assert safe_iso(dt) == "2026-08-19T12:00:00Z"

    # Naive date string fallback
    assert safe_iso("2026-08-19") == "2026-08-19"

    # Space-separated timestamp string (e.g. from ClickHouse JSON)
    assert safe_iso("2026-08-19 12:00:00") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19 12:00:00.123") == "2026-08-19T12:00:00.123Z"

    # Already ISO-8601 with Z or offset
    assert safe_iso("2026-08-19T12:00:00Z") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19T12:00:00+00:00") == "2026-08-19T12:00:00+00:00"


def test_parse_window_str_to_dt():
    dt = parse_window_str_to_dt("2026-08-19T12:00:00Z")
    assert dt.year == 2026
    assert dt.month == 8

    with pytest.raises(ValueError, match="Invalid date window string"):
        parse_window_str_to_dt("invalid")


def test_window_to_epoch():
    s, e, from_ts, to_ts = window_to_epoch("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z")
    assert s == "2026-08-10T00:00:00Z"
    assert e == "2026-08-19T12:00:00Z"
    assert from_ts > 0
    assert to_ts > from_ts


def test_parse_relative_time_window():
    # 1. Empty since defaults to 1h
    dt_empty = parse_relative_time_window("")
    assert (datetime.now(UTC) - dt_empty).total_seconds() == pytest.approx(3600, abs=10)

    # 2. Hours unit "24h"
    dt_hours = parse_relative_time_window("12h")
    assert (datetime.now(UTC) - dt_hours).total_seconds() == pytest.approx(12 * 3600, abs=10)

    # 3. Days unit "7d"
    dt_days = parse_relative_time_window("5d")
    assert (datetime.now(UTC) - dt_days).total_seconds() == pytest.approx(5 * 24 * 3600, abs=10)

    # 4. Minutes unit "30m"
    dt_mins = parse_relative_time_window("30m")
    assert (datetime.now(UTC) - dt_mins).total_seconds() == pytest.approx(30 * 60, abs=10)

    # 5. Invalid / parse fail defaults to 1h
    dt_invalid = parse_relative_time_window("invalid")
    assert (datetime.now(UTC) - dt_invalid).total_seconds() == pytest.approx(3600, abs=10)


def test_parse_window_returns_utc_pair_for_a_valid_window():
    st, et = parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00")
    assert (st, et) == (datetime(2026, 8, 5, 10, tzinfo=UTC), datetime(2026, 8, 5, 12, 30, tzinfo=UTC))
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", None),
        ("", ""),
        ("garbage", "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", "garbage"),
        ("2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z"),
        ("2026-08-05T12:00:00Z", "2026-08-05T10:00:00Z"),
    ],
)
def test_parse_window_is_none_for_missing_unparseable_or_non_positive_windows(start, end):
    assert parse_window(start, end) is None


# --- B5: partition-key / UTC-normalisation helpers --------------------------

_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]


@pytest.mark.parametrize("key", _HOUR_KEYS)
def test_parse_hour_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    assert parse_hour_key(key) == raw
    assert parse_hour_key_strict(key) == raw
    assert parse_hour_key(key).tzinfo is UTC


@pytest.mark.parametrize("key", _BAD_KEYS)
def test_parse_hour_key_malformed(key):
    assert parse_hour_key(key) is None
    with pytest.raises(ValueError):
        parse_hour_key_strict(key)
    assert parse_hour_token(key) is None


@pytest.mark.parametrize("key", ["2026-10-19", "2024-02-29", "2026-1-5"])
def test_parse_day_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)
    assert parse_day_key_strict(key) == raw


@pytest.mark.parametrize("key", ["", "2026-02-30", "2026-10-19-05", "x"])
def test_parse_day_key_malformed(key):
    with pytest.raises(ValueError):
        parse_day_key_strict(key)


def test_ensure_utc_naive_is_labelled_utc():
    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)


def test_ensure_utc_aware_preserves_instant():
    plus5 = timezone(timedelta(hours=5))
    src = datetime(2026, 10, 19, 5, tzinfo=plus5)
    out = ensure_utc(src)
    assert out == src
    assert out.utcoffset() == timedelta(0)
    assert out.hour == 0


# ── Merged from test_time_window.py ──────────────────────────────────────────────

import re
from pathlib import Path

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
REPO_ROOT = Path(__file__).resolve().parents[2]


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ── token vocabulary ──────────────────────────────────────────────────────────


def test_valid_tokens_are_recognized():
    assert is_valid_range_token("24h")
    assert is_valid_range_token("7d")
    assert is_valid_range_token("30d")
    assert is_valid_range_token("auto")
    assert VALID_RANGE_TOKENS == {"1h", "3h", "6h", "12h", "24h", "3d", "7d", "30d", "auto"}


def test_unknown_token_falls_through_not_raises():
    # An additive wire field must degrade to the legacy path, never reject.
    assert not is_valid_range_token(None)
    assert not is_valid_range_token("")
    assert not is_valid_range_token("90d")
    assert not is_valid_range_token("nonsense")


def test_filterbar_presets_lockstep_with_token_vocabulary():
    """Every FilterBar quick-preset must be a recognized range token.

    A preset the backend doesn't recognize degrades off the memo-stable
    keyed path — and before resolveRangeWire also sent the display bounds
    in token mode, it degraded all the way to clamp(None, None): an
    all-time scan for admins (1h/3h/6h/12h/3d shipped showing all-time
    top-N counts). This test pins the FE preset vocabulary to
    VALID_RANGE_TOKENS so it can't recur. The FE-mirror leg lives in
    frontend/__tests__/lib/time-window.parity.test.ts (same scrape).
    """
    filterbar = REPO_ROOT / "frontend" / "components" / "FilterBar" / "FilterBar.tsx"
    src = filterbar.read_text()
    presets = re.findall(r"pickRelative\(\s*'([^']+)'\s*,\s*([\d.]+)\s*\)", src)
    assert presets, f"no pickRelative presets found in {filterbar} — regex stale?"
    # Partial-drop guard: every pickRelative CALL SITE must have matched the
    # extraction regex. Without this, a preset written with double quotes, a
    # template literal, or an hours arg the pattern doesn't cover would be
    # silently skipped while the other presets keep the test green — exactly
    # the drift this gate exists to catch. (The definition
    # ``const pickRelative = React.useCallback(`` contains no ``pickRelative(``
    # substring, so the count is call sites only.)
    call_sites = src.count("pickRelative(")
    assert len(presets) == call_sites, (
        f"{call_sites - len(presets)} pickRelative call(s) in {filterbar} did not "
        "match the preset-extraction regex — fix the regex (or the preset shape) "
        "so EVERY quick-preset is vocabulary-checked"
    )
    for label, hours in presets:
        assert is_valid_range_token(label), f"FilterBar preset {label!r} not in VALID_RANGE_TOKENS"
        # The wire label must also MEAN the same thing on both sides: the
        # backend's delta for the token must equal the FE's hour count.
        start, end = resolve_window(label, _NOW.isoformat(), now=_NOW)
        assert _dt(end) - _dt(start) == timedelta(hours=float(hours)), (
            f"FilterBar preset {label!r} is {hours}h on the FE but {_dt(end) - _dt(start)} on the backend"
        )


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
    [
        ("1h", timedelta(hours=1)),
        ("3h", timedelta(hours=3)),
        ("6h", timedelta(hours=6)),
        ("12h", timedelta(hours=12)),
        ("24h", timedelta(hours=24)),
        ("3d", timedelta(days=3)),
        ("7d", timedelta(days=7)),
        ("30d", timedelta(days=30)),
    ],
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


# ── Merged from test_insights_defaults.py ──────────────────────────────────────────────

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from backend.utils.time_window import (
    _BANDS,
    STATIC_DEFAULT,
    history_hours_from_earliest,
    pick_insights_default,
)

_TS_SOURCE = Path(__file__).resolve().parents[2] / "frontend" / "lib" / "insights-defaults.ts"

_INSIGHTS_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)


# ── pick_insights_default: band table ────────────────────────────────────────


@pytest.mark.parametrize(
    ("history_hours", "expected"),
    [
        (0.0, (0.25, 1.0)),
        (0.99, (0.25, 1.0)),
        (1.0, (1.0, 1.0)),  # boundary → higher bucket (half-open bands)
        (3.9, (1.0, 1.0)),
        (4.0, (4.0, 1.0)),
        (23.9, (4.0, 1.0)),
        (24.0, (4.0, 24.0)),
        (47.9, (4.0, 24.0)),
        (48.0, (24.0, 24.0)),
        (167.9, (24.0, 24.0)),
        (168.0, (24.0, 168.0)),
        (719.9, (24.0, 168.0)),
        (720.0, (24.0, 168.0)),  # ≥30 d defaults to prior 7-day period (24h vs 168h)
        (1270.0, (24.0, 168.0)),
    ],
)
def test_band_table(history_hours: float, expected: tuple[float, float]) -> None:
    assert pick_insights_default(history_hours) == expected


def test_none_and_nonfinite_fall_back_to_static_default() -> None:
    assert pick_insights_default(None) == STATIC_DEFAULT
    assert pick_insights_default(math.nan) == STATIC_DEFAULT
    assert pick_insights_default(math.inf) == STATIC_DEFAULT
    assert STATIC_DEFAULT == (24.0, 168.0)


# ── history_hours_from_earliest ──────────────────────────────────────────────


def test_history_hours_parses_full_iso() -> None:
    earliest = (_INSIGHTS_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_INSIGHTS_NOW) == pytest.approx(53 * 24)


def test_history_hours_widens_date_only_to_utc_midnight() -> None:
    # Mirrors the TS side: "2026-07-01" → 2026-07-01T00:00:00Z.
    assert history_hours_from_earliest("2026-07-01", now=_INSIGHTS_NOW) == pytest.approx(6 * 24 + 12)


def test_history_hours_future_earliest_clamps_to_zero() -> None:
    future = (_INSIGHTS_NOW + timedelta(hours=5)).isoformat()
    assert history_hours_from_earliest(future, now=_INSIGHTS_NOW) == 0.0


@pytest.mark.parametrize("bad", [None, "", "not-a-date"])
def test_history_hours_absent_or_unparseable_is_none(bad: str | None) -> None:
    assert history_hours_from_earliest(bad, now=_INSIGHTS_NOW) is None


# ── TS parity: parse the frontend band table out of the source ───────────────


def _ts_pick_function_body() -> str:
    src = _TS_SOURCE.read_text()
    start = src.index("export function pickInsightsDefault")
    return src[start:]


def test_ts_band_table_matches_python_mirror() -> None:
    """Extract every `if (h < N) return { window: 'W', baseline: 'B' }` band
    plus the trailing `return { window: 'W', baseline: 'B' }` from the TS
    picker and compare against the Python implementation band-for-band."""
    body = _ts_pick_function_body()

    band_re = re.compile(
        r"if\s*\(\s*h\s*<\s*([\d.]+)\s*\)\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}"
    )
    ts_bands = [(float(u), float(w), float(b)) for u, w, b in band_re.findall(body)]
    assert len(ts_bands) >= 5, (
        f"parsed only {len(ts_bands)} bands from {_TS_SOURCE} — the picker was "
        "reformatted or restructured; update this parser AND verify "
        "backend/utils/insights_defaults.py still mirrors it"
    )

    tail_re = re.compile(r"^\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", re.MULTILINE)
    tails = tail_re.findall(body)
    assert len(tails) == 1, f"expected exactly one unconditional tail return in the TS picker, found {len(tails)}"
    ts_bands.append((math.inf, float(tails[0][0]), float(tails[0][1])))

    assert tuple(ts_bands) == _BANDS


def test_ts_static_default_matches() -> None:
    src = _TS_SOURCE.read_text()
    m = re.search(r"STATIC_DEFAULT\s*=\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", src)
    assert m is not None, f"could not find STATIC_DEFAULT in {_TS_SOURCE}"
    assert (float(m.group(1)), float(m.group(2))) == STATIC_DEFAULT
