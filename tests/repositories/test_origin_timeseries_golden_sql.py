"""Golden SQL for ``origin.get_timeseries`` latency expressions (B3).

Pins the exact rendered query for every (metric, column-availability,
percentile) shape so the ``ottfb``/``ttfb`` COALESCE can be sourced from
``metric_expressions`` without any byte of the SQL changing.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest

from backend.repositories import origin

_TS = """
        SELECT
          time_bucket(INTERVAL '5' minutes, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg} {unit}                                            AS value

        FROM "t_svc"
        WHERE 1=1 AND ({lat} IS NOT NULL)
        GROUP BY ts
        ORDER BY ts
        """

_COALESCE = 'COALESCE("ottfb", "ttfb" * 1000000.0)'

_CASES = [
    # (cols, metric, percentile, lat_expr, unit_conv)
    (["ottfb", "ttfb"], "ttfb", "p95", _COALESCE, "/ 1000.0"),
    (["ottfb", "ttfb"], "ttfb", "p50", _COALESCE, "/ 1000.0"),
    (["ottfb", "ttfb"], "ttfb", "p99", _COALESCE, "/ 1000.0"),
    (["ottfb"], "ttfb", "p95", '"ottfb"', "/ 1000.0"),
    (["ttfb"], "ttfb", "p95", '"ttfb"', "* 1000.0"),
    (["ottfb", "ttfb", "ottlb"], "ttlb", "p95", '"ottlb"', "/ 1000.0"),
]


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _run(cols: list[str], metric: str, percentile: str) -> str | None:
    runner = MagicMock()
    runner.get_schema_cols.return_value = cols
    runner.execute.return_value.fetchall.return_value = []
    with (
        patch.object(origin, "QueryRunner", return_value=runner),
        patch.object(origin, "_check_response_cache", return_value=("k", None)),
        patch.object(origin, "_store_response_cache", side_effect=lambda _k, payload, _r: payload),
        patch.object(origin, "_safe_table", return_value='"t_svc"'),
        patch.object(origin, "build_where_clause", return_value=([], "1=1")),
    ):
        origin.get_timeseries(MagicMock(), {"name": "svc"}, None, None, {}, metric=metric, percentile=percentile)
    if not runner.execute.called:
        return None
    return runner.execute.call_args.args[0]


@pytest.mark.parametrize("cols,metric,percentile,lat,unit", _CASES)
def test_timeseries_sql_is_byte_stable(cols, metric, percentile, lat, unit):
    if percentile == "p50":
        agg = f"MEDIAN({lat})"
    else:
        agg = f"APPROX_QUANTILE({lat}, {0.95 if percentile == 'p95' else 0.99})"
    expected = _TS.format(agg=agg, unit=unit, lat=lat)
    assert _norm(_run(cols, metric, percentile)) == _norm(expected)


def test_timeseries_without_any_latency_column_runs_no_query():
    assert _run(["url"], "ttfb", "p95") is None
