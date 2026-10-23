"""Bounded ClickHouse reads for the high-scale Origin page."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from backend.core.high_scale_sections import HighScaleSections
from backend.high_scale import origin_facts
from backend.high_scale._filters import (
    build_clickhouse_filters,
)
from backend.high_scale._filters import (
    build_published_projection_where as _where,
)
from backend.high_scale._sketch import RELATIVE_ERROR
from backend.high_scale.registry import HighScaleService
from backend.utils.date_utils import safe_iso

_EMPTY = {
    "summary": {},
    "timeseries": {"has_data": False, "series": []},
    "slow_urls": {"has_data": False, "rows": []},
    "status_codes": {"has_data": False, "rows": []},
    "path_breakdown": {"has_data": False, "shielding_detected": False, "rows": []},
    "pop_latency": {"has_data": False, "requires_group_c": False, "rows": []},
    "ip_health": {"has_data": False, "rows": []},
}


_APPROX_NOTE = (
    "Latency percentiles are merged from per-minute log-histogram sketches "
    f"(relative error at most {RELATIVE_ERROR:.0%}). Request counts and error rates are exact."
)
_APPROX = {"_approx": True, "_approx_note": _APPROX_NOTE}


def _sketch_select(alias: str, column: str, quantiles: tuple[float, ...]) -> str:
    """Merge the per-minute ``{column}_sk_v/_sk_w`` histograms and read ``quantiles`` from the union."""
    levels = ", ".join(str(level) for level in quantiles)
    return (
        f"quantilesExactWeightedArray({levels})({column}_sk_v, {column}_sk_w) AS {alias}_q, "
        f"sumArray({column}_sk_w) AS {alias}_n"
    )


def _quantile(row: dict[str, Any], alias: str, index: int) -> float | None:
    """The ``index``-th requested quantile in milliseconds-ready microseconds; None with no observations."""
    if int(row.get(f"{alias}_n") or 0) == 0:
        return None
    values = row.get(f"{alias}_q") or []
    return _float_or_none(values[index]) if index < len(values) else None


def _summary(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    rows = service.client.execute(
        "/* origin:summary */ SELECT "
        "sum(requests) AS requests, sum(misses) AS misses, sum(passes) AS passes, "
        "sum(origin_5xx) AS origin_5xx, sum(status_count) AS status_count, "
        f"{_sketch_select('latency', 'latency', (0.5, 0.75, 0.95, 0.99))}, "
        f"{_sketch_select('ttlb', 'ttlb', (0.5, 0.95))}, "
        f"{_sketch_select('overhead', 'overhead', (0.5,))}, "
        f"{_sketch_select('bytes', 'origin_bytes', (0.5,))} "
        f"FROM origin_minute_summary WHERE {where}",
        params,
    )
    row = rows[0] if rows else {}
    requests = int(row.get("requests") or 0)
    latency_p50 = _quantile(row, "latency", 0)
    if requests == 0 or latency_p50 is None:
        return {
            "has_data": False,
            "total_misses": None,
            "total_passes": None,
            "ottfb_p50_ms": None,
            "ottfb_p75_ms": None,
            "ottfb_p95_ms": None,
            "ottfb_p99_ms": None,
            "ottlb_p50_ms": None,
            "ottlb_p95_ms": None,
            "cdn_overhead_p50_ms": None,
            "origin_error_rate": None,
            "obytes_p50": None,
        }
    return {
        "has_data": True,
        "total_misses": int(row.get("misses") or 0),
        "total_passes": int(row.get("passes") or 0),
        "ottfb_p50_ms": _milliseconds(latency_p50),
        "ottfb_p75_ms": _milliseconds(_quantile(row, "latency", 1)),
        "ottfb_p95_ms": _milliseconds(_quantile(row, "latency", 2)),
        "ottfb_p99_ms": _milliseconds(_quantile(row, "latency", 3)),
        "ottlb_p50_ms": _milliseconds(_quantile(row, "ttlb", 0)),
        "ottlb_p95_ms": _milliseconds(_quantile(row, "ttlb", 1)),
        "cdn_overhead_p50_ms": _milliseconds(_quantile(row, "overhead", 0)),
        "origin_error_rate": (
            int(row.get("origin_5xx") or 0) / int(row["status_count"])
            if int(row.get("status_count") or 0) > 0
            else None
        ),
        "obytes_p50": _quantile(row, "bytes", 0),
        **_APPROX,
    }


def _timeseries(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    if req.split_by_leg:
        raise ValueError("high-scale Origin timeseries does not support split_by_leg")
    if req.bucket_minutes < 1:
        raise ValueError("high-scale Origin timeseries requires bucket_minutes of at least 1")
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    params["bucket_minutes"] = int(req.bucket_minutes)
    metric = {
        ("ttfb", "p50"): ("latency", 0.5),
        ("ttfb", "p95"): ("latency", 0.95),
        ("ttfb", "p99"): ("latency", 0.99),
        ("ttlb", "p50"): ("ttlb", 0.5),
        ("ttlb", "p95"): ("ttlb", 0.95),
        ("ttlb", "p99"): ("ttlb", 0.99),
    }.get((req.timeseries_metric, req.timeseries_percentile))
    if metric is None:
        raise ValueError("high-scale Origin timeseries does not support the requested metric and percentile")
    metric_column, level = metric
    rows = service.client.execute(
        "/* origin:timeseries */ SELECT "
        "toStartOfInterval(bucket_start, toIntervalMinute({bucket_minutes:UInt32})) AS time, "
        "sum(misses) AS miss_count, "
        f"{_sketch_select('value', metric_column, (level,))} "
        f"FROM origin_minute_summary WHERE {where} "
        "GROUP BY time ORDER BY time",
        params,
    )
    return {
        "has_data": bool(rows),
        "series": [
            {
                "time": safe_iso(row["time"]),
                "miss_count": int(row.get("miss_count") or 0),
                "value": _milliseconds(_quantile(row, "value", 0)),
            }
            for row in rows
        ],
        **_APPROX,
    }


def _dimension_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    select: str,
    order_by: str,
    limit: int | None = None,
    having: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    where, params = _where(
        service.service_id,
        "origin_dimensions",
        start_time,
        end_time,
        dimension=dimension,
    )
    suffix = f" HAVING {having}" if having else ""
    params.update(extra_params or {})
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " ORDER BY " + order_by + " LIMIT {limit:UInt32}"
    else:
        suffix += " ORDER BY " + order_by
    return service.client.execute(
        f"/* origin:{marker} */ SELECT value, {select} "
        f"FROM origin_minute_dimensions WHERE {where} GROUP BY value{suffix}",
        params,
    )


def _slow_urls(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    rows = _dimension_rows(
        service,
        marker="slow_urls",
        dimension="url",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            f"{_sketch_select('latency', 'latency', (0.5, 0.95, 0.99))}"
        ),
        having="sum(origin_minute_dimensions.requests) >= {min_requests:UInt64}",
        order_by="latency_q[2] DESC",
        limit=req.slow_urls_limit,
        extra_params={"min_requests": int(req.slow_urls_min_requests)},
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "url": row["value"],
                "requests": int(row["requests"]),
                "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                "p99_ms": _milliseconds(_quantile(row, "latency", 2)),
            }
            for row in rows
        ],
        **_APPROX,
    }


def _status_codes(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    rows = _dimension_rows(
        service,
        marker="status_codes",
        dimension="status",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            "sum(sum(origin_minute_dimensions.requests)) OVER () AS total_requests"
        ),
        order_by="requests DESC",
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "status": int(row["value"]),
                "count": int(row["requests"]),
                "pct": (int(row["requests"]) * 100.0 / int(row["total_requests"])),
            }
            for row in rows
            if int(row.get("total_requests") or 0) > 0
        ],
    }


def _latency_dimension(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    limit: int | None = None,
    having: str | None = None,
) -> list[dict[str, Any]]:
    return _dimension_rows(
        service,
        marker=marker,
        dimension=dimension,
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(origin_minute_dimensions.requests) AS requests, "
            "sum(origin_5xx) AS origin_5xx, "
            f"{_sketch_select('latency', 'latency', (0.5, 0.95))}"
        ),
        order_by="latency_q[2] DESC",
        limit=limit,
        having=having,
    )


def _milliseconds(value: object) -> float | None:
    parsed = _float_or_none(value)
    return parsed / 1000.0 if parsed is not None else None


def _float_or_none(value: object) -> float | None:
    return float(str(value)) if value is not None else None


def aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None,
) -> dict[str, Any]:
    filter_sql, filter_params = build_clickhouse_filters(req.filters)
    facts = (
        origin_facts.scope(service, start_time, end_time, filter_sql, filter_params) if filter_sql != "1=1" else None
    )
    approx = _APPROX if facts is None else {"_approx": False}

    def latency_rows(marker: str, dimension: str, limit: int | None = None, having: str | None = None):
        if facts is not None:
            return origin_facts.latency_dimension(service, facts, marker=marker, dimension=dimension, limit=limit)
        return _latency_dimension(
            service,
            marker=marker,
            dimension=dimension,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            having=having,
        )

    sections = sections or set(_EMPTY)

    def _build_path_breakdown() -> dict[str, Any]:
        rows = latency_rows("path_breakdown", "edge")
        return {
            "has_data": bool(rows),
            "shielding_detected": any(row["value"] == "false" for row in rows),
            "rows": [
                {
                    "edge": row["value"] == "true",
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                }
                for row in rows
            ],
            **approx,
        }

    def _build_pop_latency() -> dict[str, Any]:
        rows = latency_rows("pop_latency", "pop", limit=req.pop_latency_limit)
        p95s = [value for row in rows if (value := _milliseconds(_quantile(row, "latency", 1))) is not None]
        p95s.sort()
        median_p95 = p95s[len(p95s) // 2] if p95s else 0
        return {
            "has_data": bool(rows),
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": [
                {
                    "pop": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                    "elevated": _is_elevated(_quantile(row, "latency", 1), median_p95),
                }
                for row in rows
            ],
            **approx,
        }

    def _build_ip_health() -> dict[str, Any]:
        rows = latency_rows(
            "ip_health",
            "oip",
            limit=req.ip_health_limit,
            having="sum(origin_minute_dimensions.requests) >= 10",
        )
        return {
            "has_data": bool(rows),
            "rows": [
                {
                    "oip": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(_quantile(row, "latency", 0)),
                    "p95_ms": _milliseconds(_quantile(row, "latency", 1)),
                    "error_pct": int(row.get("origin_5xx") or 0) * 100.0 / int(row["requests"]),
                }
                for row in rows
                if int(row["requests"]) > 0
            ],
            **approx,
        }

    tasks: list[tuple[str, Any]] = []
    if "summary" in sections:
        tasks.append(
            (
                "summary",
                lambda: (
                    _summary(service, start_time, end_time) if facts is None else origin_facts.summary(service, facts)
                ),
            )
        )
    if "timeseries" in sections:
        tasks.append(
            (
                "timeseries",
                lambda: (
                    _timeseries(service, req, start_time, end_time)
                    if facts is None
                    else origin_facts.timeseries(service, facts, req)
                ),
            )
        )
    if "slow_urls" in sections:
        tasks.append(
            (
                "slow_urls",
                lambda: (
                    _slow_urls(service, req, start_time, end_time)
                    if facts is None
                    else origin_facts.slow_urls(service, facts, req)
                ),
            )
        )
    if "status_codes" in sections:
        tasks.append(
            (
                "status_codes",
                lambda: (
                    _status_codes(service, start_time, end_time)
                    if facts is None
                    else origin_facts.status_codes(service, facts)
                ),
            )
        )
    if "path_breakdown" in sections:
        tasks.append(("path_breakdown", _build_path_breakdown))
    if "pop_latency" in sections:
        tasks.append(("pop_latency", _build_pop_latency))
    if "ip_health" in sections:
        tasks.append(("ip_health", _build_ip_health))

    trace = HighScaleSections(service.service_id, "origin")
    result: dict[str, Any] = {}
    if len(tasks) <= 1:
        for key, fn in tasks:
            result[key] = trace.run(key, fn, source="projection" if facts is None else "fact")
    else:
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            futures = [
                (
                    key,
                    trace.submit(pool, key, fn, source="projection" if facts is None else "fact"),
                )
                for key, fn in tasks
            ]
            for key, fut in futures:
                result[key] = fut.result()

    summary = result.get("summary", _EMPTY["summary"])
    return {"has_data": bool(summary.get("has_data")), **result, "section_timings": trace.timings()}


def _is_elevated(value_us: object, median_p95_ms: float) -> bool:
    value_ms = _milliseconds(value_us)
    return value_ms is not None and value_ms > median_p95_ms * 2
