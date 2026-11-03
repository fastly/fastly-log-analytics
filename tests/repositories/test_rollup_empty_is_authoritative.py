"""A fully covered rollup window with zero rows means "no data", not "rollup unavailable"."""

from typing import Any

import pytest

from backend.repositories.rollup_executor.scalars import _ScalarsRollupMixin
from backend.repositories.rollup_executor.top_n import _TopNRollupMixin


class _Runner(_ScalarsRollupMixin, _TopNRollupMixin):
    def __init__(self, rows: Any) -> None:
        self._rows = rows

    def _rollup_paths_sql(self, *_a: Any, **_k: Any) -> str:
        return "'covered.parquet'"

    def _exec_rollup(self, *_a: Any, **_k: Any) -> Any:
        return self._rows


_WINDOW: dict[str, Any] = {
    "start_time": "2026-11-02T00:00:00Z",
    "end_time": "2026-11-03T00:00:00Z",
    "has_filters": False,
}

_CASES = [
    ("try_origin_status_from_rollup", {}, {"has_data": False, "rows": []}),
    (
        "try_origin_path_breakdown_from_rollup",
        {},
        {"has_data": False, "shielding_detected": False, "rows": [], "_approx": True},
    ),
    ("try_origin_pop_latency_from_rollup", {"limit": 5}, {"has_data": False, "requires_group_c": False, "rows": []}),
    ("try_origin_ip_health_from_rollup", {"limit": 5}, {"has_data": False, "rows": [], "_approx": True}),
    ("try_security_req_size_from_rollup", {}, []),
    ("try_security_conn_reuse_from_rollup", {}, []),
    ("try_security_top_ips_from_rollup", {}, []),
]


@pytest.mark.parametrize(("method", "extra", "expected"), _CASES)
def test_empty_covered_rollup_is_served_not_a_fallback(method: str, extra: dict, expected: Any) -> None:
    assert getattr(_Runner([]), method)(**_WINDOW, **extra) == expected


@pytest.mark.parametrize(("method", "extra", "_expected"), _CASES)
def test_failed_rollup_read_still_falls_back(method: str, extra: dict, _expected: Any) -> None:
    assert getattr(_Runner(None), method)(**_WINDOW, **extra) is None
