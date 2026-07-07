"""Tests for :mod:`backend.utils.insights_defaults` — the Python mirror of
``frontend/lib/insights-defaults.ts``.

Two layers of protection:
1. A hand-pinned band table (the primary spec — readable, reviewable).
2. A parity test that parses the TS source's band table so a frontend-only
   band change fails the backend suite instead of silently stranding the
   prewarmer on a shape the page no longer requests.
"""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from backend.utils.insights_defaults import (
    STATIC_DEFAULT,
    history_hours_from_earliest,
    pick_insights_default,
)

_TS_SOURCE = Path(__file__).resolve().parents[2] / "frontend" / "lib" / "insights-defaults.ts"

_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)


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
        (168.0, (1.0, 168.0)),
        (719.9, (1.0, 168.0)),
        (720.0, (1.0, 720.0)),  # ≥30 d: the shape the prewarm regression missed
        (1270.0, (1.0, 720.0)),
    ],
)
def test_band_table(history_hours: float, expected: tuple[float, float]) -> None:
    assert pick_insights_default(history_hours) == expected


def test_none_and_nonfinite_fall_back_to_static_default() -> None:
    assert pick_insights_default(None) == STATIC_DEFAULT
    assert pick_insights_default(math.nan) == STATIC_DEFAULT
    assert pick_insights_default(math.inf) == STATIC_DEFAULT
    assert STATIC_DEFAULT == (1.0, 168.0)


# ── history_hours_from_earliest ──────────────────────────────────────────────


def test_history_hours_parses_full_iso() -> None:
    earliest = (_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_NOW) == pytest.approx(53 * 24)


def test_history_hours_widens_date_only_to_utc_midnight() -> None:
    # Mirrors the TS side: "2026-07-01" → 2026-07-01T00:00:00Z.
    assert history_hours_from_earliest("2026-07-01", now=_NOW) == pytest.approx(6 * 24 + 12)


def test_history_hours_future_earliest_clamps_to_zero() -> None:
    future = (_NOW + timedelta(hours=5)).isoformat()
    assert history_hours_from_earliest(future, now=_NOW) == 0.0


@pytest.mark.parametrize("bad", [None, "", "not-a-date"])
def test_history_hours_absent_or_unparseable_is_none(bad: str | None) -> None:
    assert history_hours_from_earliest(bad, now=_NOW) is None


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
    assert len(ts_bands) >= 6, (
        f"parsed only {len(ts_bands)} bands from {_TS_SOURCE} — the picker was "
        "reformatted or restructured; update this parser AND verify "
        "backend/utils/insights_defaults.py still mirrors it"
    )

    tail_re = re.compile(r"^\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", re.MULTILINE)
    tails = tail_re.findall(body)
    assert len(tails) == 1, f"expected exactly one unconditional tail return in the TS picker, found {len(tails)}"
    ts_bands.append((math.inf, float(tails[0][0]), float(tails[0][1])))

    from backend.utils.insights_defaults import _BANDS

    assert tuple(ts_bands) == _BANDS


def test_ts_static_default_matches() -> None:
    src = _TS_SOURCE.read_text()
    m = re.search(r"STATIC_DEFAULT\s*=\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", src)
    assert m is not None, f"could not find STATIC_DEFAULT in {_TS_SOURCE}"
    assert (float(m.group(1)), float(m.group(2))) == STATIC_DEFAULT
