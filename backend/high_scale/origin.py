"""Bounded ClickHouse reads for the high-scale Origin page."""

from __future__ import annotations

from datetime import datetime
from typing import Any

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


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _where(
    service_id: str,
    domain: str,
    start_time: str | None,
    end_time: str | None,
    *,
    dimension: str | None = None,
) -> tuple[str, dict[str, Any]]:
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={publication_domain:String} "
        "AND publication_state='visible')",
    ]
    params: dict[str, Any] = {
        "service_id": service_id,
        "publication_domain": domain,
    }
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start < {end:DateTime64(3)}")
        params["end"] = end
    if dimension is not None:
        clauses.append("dimension={dimension:String}")
        params["dimension"] = dimension
    return " AND ".join(clauses), params


def _weighted(column: str, count_column: str = "latency_count") -> str:
    return f"sum({column} * {count_column}) / nullIf(sum({count_column}), 0)"


def _summary(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    rows = service.client.execute(
        "/* origin:summary */ SELECT "
        "sum(requests) AS requests, sum(misses) AS misses, sum(passes) AS passes, "
        "sum(origin_5xx) AS origin_5xx, sum(status_count) AS status_count, "
        f"{_weighted('latency_p50_us')} AS latency_p50_us, "
        f"{_weighted('latency_p75_us')} AS latency_p75_us, "
        f"{_weighted('latency_p95_us')} AS latency_p95_us, "
        f"{_weighted('latency_p99_us')} AS latency_p99_us, "
        f"{_weighted('ttlb_p50_us', 'ttlb_count')} AS ttlb_p50_us, "
        f"{_weighted('ttlb_p95_us', 'ttlb_count')} AS ttlb_p95_us, "
        f"{_weighted('cdn_overhead_p50_us', 'overhead_count')} AS cdn_overhead_p50_us, "
        f"{_weighted('origin_bytes_p50', 'origin_bytes_count')} AS origin_bytes_p50 "
        f"FROM origin_minute_summary WHERE {where}",
        params,
    )
    row = rows[0] if rows else {}
    requests = int(row.get("requests") or 0)
    latency_p50 = row.get("latency_p50_us")
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
        "ottfb_p75_ms": _milliseconds(row.get("latency_p75_us")),
        "ottfb_p95_ms": _milliseconds(row.get("latency_p95_us")),
        "ottfb_p99_ms": _milliseconds(row.get("latency_p99_us")),
        "ottlb_p50_ms": _milliseconds(row.get("ttlb_p50_us")),
        "ottlb_p95_ms": _milliseconds(row.get("ttlb_p95_us")),
        "cdn_overhead_p50_ms": _milliseconds(row.get("cdn_overhead_p50_us")),
        "origin_error_rate": (
            int(row.get("origin_5xx") or 0) / int(row["status_count"])
            if int(row.get("status_count") or 0) > 0
            else None
        ),
        "obytes_p50": _float_or_none(row.get("origin_bytes_p50")),
        "_approx": True,
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
    metric_column = {
        ("ttfb", "p50"): "latency_p50_us",
        ("ttfb", "p95"): "latency_p95_us",
        ("ttfb", "p99"): "latency_p99_us",
        ("ttlb", "p50"): "ttlb_p50_us",
        ("ttlb", "p95"): "ttlb_p95_us",
    }.get((req.timeseries_metric, req.timeseries_percentile))
    if metric_column is None:
        raise ValueError("high-scale Origin timeseries does not support the requested metric and percentile")
    rows = service.client.execute(
        "/* origin:timeseries */ SELECT "
        "toStartOfInterval(bucket_start, toIntervalMinute({bucket_minutes:UInt32})) AS time, "
        "sum(misses) AS miss_count, "
        f"{_weighted(metric_column)} AS value_us "
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
                "value": _milliseconds(row.get("value_us")),
            }
            for row in rows
        ],
        "_approx": True,
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
            f"{_weighted('latency_p50_us')} AS p50_us, "
            f"{_weighted('latency_p95_us')} AS p95_us, "
            f"{_weighted('latency_p99_us')} AS p99_us"
        ),
        having="sum(origin_minute_dimensions.requests) >= {min_requests:UInt64}",
        order_by="p95_us DESC",
        limit=req.slow_urls_limit,
        extra_params={"min_requests": int(req.slow_urls_min_requests)},
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "url": row["value"],
                "requests": int(row["requests"]),
                "p50_ms": _milliseconds(row.get("p50_us")),
                "p95_ms": _milliseconds(row.get("p95_us")),
                "p99_ms": _milliseconds(row.get("p99_us")),
            }
            for row in rows
        ],
        "_approx": True,
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
            f"{_weighted('latency_p50_us')} AS p50_us, "
            f"{_weighted('latency_p95_us')} AS p95_us"
        ),
        order_by="p95_us DESC",
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
    if req.filters:
        raise ValueError("high-scale Origin projections do not support filters")

    sections = sections or set(_EMPTY)
    result: dict[str, Any] = {}
    if "summary" in sections:
        result["summary"] = _summary(service, start_time, end_time)
    if "timeseries" in sections:
        result["timeseries"] = _timeseries(service, req, start_time, end_time)
    if "slow_urls" in sections:
        result["slow_urls"] = _slow_urls(service, req, start_time, end_time)
    if "status_codes" in sections:
        result["status_codes"] = _status_codes(service, start_time, end_time)
    if "path_breakdown" in sections:
        rows = _latency_dimension(
            service,
            marker="path_breakdown",
            dimension="edge",
            start_time=start_time,
            end_time=end_time,
        )
        result["path_breakdown"] = {
            "has_data": bool(rows),
            "shielding_detected": any(row["value"] == "false" for row in rows),
            "rows": [
                {
                    "edge": row["value"] == "true",
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(row.get("p50_us")),
                    "p95_ms": _milliseconds(row.get("p95_us")),
                }
                for row in rows
            ],
            "_approx": True,
        }
    if "pop_latency" in sections:
        rows = _latency_dimension(
            service,
            marker="pop_latency",
            dimension="pop",
            start_time=start_time,
            end_time=end_time,
            limit=req.pop_latency_limit,
        )
        p95s = [value for row in rows if (value := _milliseconds(row.get("p95_us"))) is not None]
        p95s.sort()
        median_p95 = p95s[len(p95s) // 2] if p95s else 0
        result["pop_latency"] = {
            "has_data": bool(rows),
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": [
                {
                    "pop": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(row.get("p50_us")),
                    "p95_ms": _milliseconds(row.get("p95_us")),
                    "elevated": _is_elevated(row.get("p95_us"), median_p95),
                }
                for row in rows
            ],
            "_approx": True,
        }
    if "ip_health" in sections:
        rows = _latency_dimension(
            service,
            marker="ip_health",
            dimension="oip",
            start_time=start_time,
            end_time=end_time,
            limit=req.ip_health_limit,
            having="sum(origin_minute_dimensions.requests) >= 10",
        )
        result["ip_health"] = {
            "has_data": bool(rows),
            "rows": [
                {
                    "oip": row["value"],
                    "requests": int(row["requests"]),
                    "p50_ms": _milliseconds(row.get("p50_us")),
                    "p95_ms": _milliseconds(row.get("p95_us")),
                    "error_pct": int(row.get("origin_5xx") or 0) * 100.0 / int(row["requests"]),
                }
                for row in rows
                if int(row["requests"]) > 0
            ],
            "_approx": True,
        }
    summary = result.get("summary", _EMPTY["summary"])
    return {"has_data": bool(summary.get("has_data")), **result}


def _is_elevated(value_us: object, median_p95_ms: float) -> bool:
    value_ms = _milliseconds(value_us)
    return value_ms is not None and value_ms > median_p95_ms * 2
