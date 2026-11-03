"""Virtual-field top-N must reuse the partial-hour accumulator instead of exploding the whole live hour."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from backend.repositories.rollup_executor.top_n import _TopNRollupMixin


class _Runner(_TopNRollupMixin):
    def __init__(self, *, carries: bool, partial_rows: list[tuple[str, str, int]], tail_rows: list[tuple[str, int]]):
        self.carries = carries
        self.partial_rows = partial_rows
        self.tail_rows = tail_rows
        self.temp_requests: list[tuple[datetime, datetime]] = []
        self.src = {"name": "svc"}

    def _collect_rollup_paths(self, *_a: Any, **_k: Any) -> list[str]:
        return ["stored.parquet"]

    def _partial_hour_carries_virtual_fields(self, _token: str) -> bool:
        return self.carries

    def _partial_hour_adjusted_live_start(self, naive_live_start: datetime, _token: str, window_end: datetime):
        return window_end, self.partial_rows, 0

    def _create_active_hour_temp_direct(self, _fields, _cols, live_start, live_end, **_k):
        self.temp_requests.append((live_start, live_end))
        return "t_tail"

    def release_active_direct_temp(self, _tmp: str) -> None:
        return None

    def _exec_rollup(self, sql: str, *_a: Any, **_k: Any):
        return self.tail_rows if "t_tail" in sql else [("A", 2), ("B", 1)]


def _window() -> tuple[str, str]:
    now = datetime.now(UTC)
    return (now - timedelta(hours=2)).isoformat(), now.isoformat()


@pytest.mark.usefixtures("frozen_clock")
def test_accumulator_rows_are_merged_and_the_tail_is_not_exploded_when_empty():
    runner = _Runner(
        carries=True,
        partial_rows=[("waf_sig_ind", "A", 5), ("country", "US", 99)],
        tail_rows=[],
    )
    start, end = _window()
    got = runner.try_virtual_field_top_n_from_rollup(
        "waf_sig_ind", "waf_sig", start, end, has_filters=False, actual_cols=["waf_sig"]
    )
    assert got == {"top": [{"value": "A", "count": 7}, {"value": "B", "count": 1}], "total": 8}
    assert runner.temp_requests == []


@pytest.mark.usefixtures("frozen_clock")
def test_without_the_marker_the_whole_live_hour_is_still_read_raw():
    runner = _Runner(carries=False, partial_rows=[("waf_sig_ind", "A", 5)], tail_rows=[("Z", 4)])
    start, end = _window()
    got = runner.try_virtual_field_top_n_from_rollup(
        "waf_sig_ind", "waf_sig", start, end, has_filters=False, actual_cols=["waf_sig"]
    )
    assert got == {"top": [{"value": "Z", "count": 4}], "total": 4}
    assert len(runner.temp_requests) == 1
