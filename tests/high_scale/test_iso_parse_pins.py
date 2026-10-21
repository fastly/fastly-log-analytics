"""Pin the per-module ISO parsing semantics shared with ``_projection_utils.range_value``.

Date-only input stays naive in the ``range_value`` family (ClickHouse bound
params), while the ``astimezone(UTC)`` family localises it; these must not drift.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.high_scale import aggregates, publication, raw_query, sessions


@pytest.mark.parametrize("module", [sessions])
def test_parse_ts_range_value_family(module) -> None:
    assert module._parse_ts(None) is None
    assert module._parse_ts("") is None
    assert module._parse_ts("2026-01-02").tzinfo is None
    assert module._parse_ts("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert module._parse_ts("2026-01-02T03:04:05+02:00").utcoffset().total_seconds() == 7200


def test_sessions_parse_ts_raises_on_garbage() -> None:
    with pytest.raises(ValueError):
        sessions._parse_ts("not-a-date")


def test_publication_timestamp_assumes_utc_for_naive_and_wraps_errors() -> None:
    assert publication._timestamp({"t": "2026-01-02"}, "t") == datetime(2026, 1, 2, tzinfo=UTC)
    assert publication._timestamp({"t": "2026-01-02T03:04:05Z"}, "t") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="t must be an ISO timestamp"):
        publication._timestamp({"t": "garbage"}, "t")


@pytest.mark.parametrize(
    "fn,key",
    [(aggregates._event_timestamp, "timestamp"), (raw_query._row_timestamp, "timestamp")],
)
def test_astimezone_family_returns_utc_aware(fn, key) -> None:
    assert fn({key: "2026-01-02T03:04:05Z"}) == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02T03:04:05+02:00"}) == datetime(2026, 1, 2, 1, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02"}).tzinfo is not None
    assert fn({key: 5}) is None
    with pytest.raises(ValueError):
        fn({key: "garbage"})
