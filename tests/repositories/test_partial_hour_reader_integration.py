"""Unit-level pin for the partial-hour seam's return contract.

The fixture-backed, real-writer end-to-end proof that this seam is wired
correctly into try_time_series_from_rollup / try_count_from_rollup lives in
tests/repositories/test_time_series_rollup.py's
TestPartialHourMergeIntoRollupReaders class (M3, final whole-branch review:
this file used to also carry a vacuous ``assert callable(...)`` test for
that wiring — removed as redundant with the real fixture-backed coverage
rather than kept as dead weight).
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch


def test_partial_hour_adjusted_live_start_narrows_and_returns_total():
    from backend.repositories._base import QueryRunner

    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_hour = active_hour_dt.strftime("%Y-%m-%d-%H")
    watermark_dt = active_hour_dt + timedelta(minutes=20)
    window_end = datetime(2027, 1, 1, tzinfo=UTC)

    with (
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_watermark",
            return_value=watermark_dt.timestamp(),
        ),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_all_fields",
            return_value=[("country", "US", 42)],
        ),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_total",
            return_value=42,
        ),
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        runner.con = None
        live_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            active_hour_dt, active_hour, window_end
        )
        # The watermark (hour_start + 20min) is later than the naive window
        # start passed in (hour_start), so it should win via max(). No skew
        # margin is applied (see I2 investigation in the method's docstring
        # — subtracting one is unsafe for this writer).
        assert live_start == watermark_dt
        assert partial_rows == [("country", "US", 42)]
        # C1: the total comes from the writer's __total__ row, NOT from
        # summing partial_rows across every field.
        assert partial_total == 42


def test_partial_hour_adjusted_live_start_skips_when_window_starts_mid_hour():
    """I1: a caller whose window starts AFTER the active hour's own start
    can't safely use the partial rollup (it always covers the WHOLE
    [hour_start, watermark) span) — the seam must fall back to an
    unmodified live scan instead of over-counting."""
    from backend.repositories._base import QueryRunner

    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_hour = active_hour_dt.strftime("%Y-%m-%d-%H")
    mid_hour_start = active_hour_dt + timedelta(minutes=15)
    window_end = datetime(2027, 1, 1, tzinfo=UTC)

    with patch(
        "backend.core.rollups.partial_hour.read_partial_hour_watermark",
        return_value=(active_hour_dt + timedelta(minutes=20)).timestamp(),
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        runner.con = None
        live_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            mid_hour_start, active_hour, window_end
        )

    assert live_start == mid_hour_start
    assert partial_rows == []
    assert partial_total == 0
