"""High-scale origin latency, summary, slow URLs, status codes, and minute projections."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from backend.high_scale._common import (
    bucket as _bucket,
)
from backend.high_scale._common import (
    finite_float as _finite_float,
)
from backend.high_scale._common import (
    percentiles as _percentiles,
)
from backend.high_scale._common import (
    text as _text,
)

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

# ── Folded from backend/high_scale/origin.py ─────────────────────────────────

import math
import sys
from dataclasses import dataclass
from dataclasses import field as _dc_field

from backend.core.high_scale_sections import HighScaleSections
from backend.high_scale._common import (
    _TIMESERIES_METRICS,
    RELATIVE_ERROR,
    build_clickhouse_filters,
    build_published_facts_where,
)
from backend.high_scale._common import build_published_projection_where as _where
from backend.high_scale._common import optional_int as _integer
from backend.high_scale._common import sketch as _sketch
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
_NOT_APPROX = {"_approx": False}

_BASE_COLUMNS = (
    "event_timestamp, url, "
    "coalesce(toFloat64OrNull(custom_fields['ottfb']), toFloat64OrNull(custom_fields['ttfb']) * 1000000.0) "
    "AS latency_us, "
    "toInt32OrNull(custom_fields['ost']) AS status, "
    "upper(custom_fields['cache']) AS cache_u, "
    "toFloat64OrNull(custom_fields['ottlb']) AS ottlb_us, "
    "toFloat64OrNull(custom_fields['elapsed']) AS elapsed_us, "
    "toInt64OrNull(custom_fields['obytes']) AS obytes, "
    "custom_fields['pop'] AS pop, "
    "multiIf(lower(custom_fields['edge']) IN ('true', '1'), 'true', "
    "lower(custom_fields['edge']) IN ('false', '0'), 'false', '') AS edge_s, "
    "custom_fields['oip'] AS oip"
)
_ORDER_BY_P95 = "ifNotFinite(latency_q[2], -1.0) DESC"
_LATENCY_DIMENSIONS = {
    "edge": ("edge_s", "latency_us IS NOT NULL AND edge_s != ''", 0),
    "pop": ("pop", "latency_us IS NOT NULL AND pop != ''", 0),
    "oip": ("oip", "status IS NOT NULL AND oip != ''", 10),
}


@dataclass(frozen=True)
class FactsScope:
    base: str
    params: dict[str, Any]


def _facts_scope(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any] | None
) -> FactsScope:
    where_sql, params = build_published_facts_where(service.service_id, start_time, end_time, filters)
    return FactsScope(f"(SELECT {_BASE_COLUMNS} FROM request_facts WHERE {where_sql})", params)


def _sketch_select(alias: str, column: str, quantiles: tuple[float, ...]) -> str:
    """Merge the per-minute ``{column}_sk_v/_sk_w`` histograms and read ``quantiles`` from the union."""
    levels = ", ".join(str(level) for level in quantiles)
    return (
        f"quantilesExactWeightedArray({levels})({column}_sk_v, {column}_sk_w) AS {alias}_q, "
        f"sumArray({column}_sk_w) AS {alias}_n"
    )


def _quantile(row: Mapping[str, Any], alias: str, index: int, count_key: str | None = None) -> float | None:
    """The ``index``-th requested quantile in milliseconds-ready microseconds; None with no observations."""
    if int(row.get(count_key or f"{alias}_n") or 0) == 0:
        return None
    values = row.get(alias if count_key else f"{alias}_q") or []
    return _float_or_none(values[index]) if index < len(values) else None


def _resolve_timeseries_metric(req: Any) -> tuple[str, str, float]:
    if req.split_by_leg:
        raise ValueError("high-scale Origin timeseries does not support split_by_leg")
    if req.bucket_minutes < 1:
        raise ValueError("high-scale Origin timeseries requires bucket_minutes of at least 1")
    metric = _TIMESERIES_METRICS.get((req.timeseries_metric, req.timeseries_percentile))
    if metric is None:
        raise ValueError("high-scale Origin timeseries does not support the requested metric and percentile")
    fact_col, level = metric
    proj_col = "latency" if fact_col == "latency_us" else "ttlb"
    return fact_col, proj_col, level


def _origin_summary(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, Any]:
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


def _timeseries(service: HighScaleService, req: Any, start_time: str | None, end_time: str | None) -> dict[str, Any]:
    _, metric_column, level = _resolve_timeseries_metric(req)
    where, params = _where(service.service_id, "origin_summary", start_time, end_time)
    params["bucket_minutes"] = int(req.bucket_minutes)
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


def _facts_summary(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_summary */ SELECT "
        "countIf(latency_us IS NOT NULL) AS requests, "
        "countIf(latency_us IS NOT NULL AND cache_u LIKE 'MISS%') AS misses, "
        "countIf(latency_us IS NOT NULL AND cache_u LIKE 'PASS%') AS passes, "
        "countIf(latency_us IS NOT NULL AND status BETWEEN 500 AND 599) AS origin_5xx, "
        "countIf(latency_us IS NOT NULL AND status IS NOT NULL) AS status_count, "
        "quantilesExact(0.5, 0.75, 0.95, 0.99)(latency_us) AS latency_q, "
        "countIf(latency_us IS NOT NULL AND ottlb_us IS NOT NULL) AS ttlb_n, "
        "quantilesExactIf(0.5, 0.95)(ottlb_us, latency_us IS NOT NULL AND ottlb_us IS NOT NULL) AS ttlb_q, "
        "countIf(latency_us IS NOT NULL AND ottlb_us IS NOT NULL AND elapsed_us IS NOT NULL) AS overhead_n, "
        "quantileExactIf(0.5)(elapsed_us - ottlb_us, "
        "latency_us IS NOT NULL AND ottlb_us IS NOT NULL AND elapsed_us IS NOT NULL) AS overhead_q, "
        "countIf(latency_us IS NOT NULL AND obytes >= 0) AS bytes_n, "
        "quantileExactIf(0.5)(toFloat64(obytes), latency_us IS NOT NULL AND obytes >= 0) AS bytes_q "
        f"FROM {facts.base}",
        facts.params,
    )
    row = rows[0] if rows else {}
    requests = int(row.get("requests") or 0)
    if requests == 0:
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
            **_NOT_APPROX,
        }
    status_count = int(row.get("status_count") or 0)
    overhead_n = int(row.get("overhead_n") or 0)
    bytes_n = int(row.get("bytes_n") or 0)
    return {
        "has_data": True,
        "total_misses": int(row.get("misses") or 0),
        "total_passes": int(row.get("passes") or 0),
        "ottfb_p50_ms": _milliseconds(_quantile(row, "latency_q", 0, "requests")),
        "ottfb_p75_ms": _milliseconds(_quantile(row, "latency_q", 1, "requests")),
        "ottfb_p95_ms": _milliseconds(_quantile(row, "latency_q", 2, "requests")),
        "ottfb_p99_ms": _milliseconds(_quantile(row, "latency_q", 3, "requests")),
        "ottlb_p50_ms": _milliseconds(_quantile(row, "ttlb_q", 0, "ttlb_n")),
        "ottlb_p95_ms": _milliseconds(_quantile(row, "ttlb_q", 1, "ttlb_n")),
        "cdn_overhead_p50_ms": _milliseconds(float(row["overhead_q"])) if overhead_n else None,
        "origin_error_rate": int(row.get("origin_5xx") or 0) / status_count if status_count else None,
        "obytes_p50": float(row["bytes_q"]) if bytes_n else None,
        **_NOT_APPROX,
    }


def _facts_timeseries(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
    column, _, level = _resolve_timeseries_metric(req)
    rows = service.client.execute(
        "/* origin:facts_timeseries */ SELECT "
        "toStartOfInterval(event_timestamp, toIntervalMinute({bucket_minutes:UInt32})) AS time, "
        "countIf(cache_u LIKE 'MISS%') AS miss_count, "
        f"countIf({column} IS NOT NULL) AS value_n, "
        f"quantileExact({level})({column}) AS value "
        f"FROM {facts.base} WHERE latency_us IS NOT NULL GROUP BY time ORDER BY time",
        {**facts.params, "bucket_minutes": int(req.bucket_minutes)},
    )
    return {
        "has_data": bool(rows),
        "series": [
            {
                "time": safe_iso(row["time"]),
                "miss_count": int(row.get("miss_count") or 0),
                "value": _milliseconds(row["value"]) if int(row.get("value_n") or 0) else None,
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def _facts_slow_urls(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_slow_urls */ SELECT url AS value, count() AS requests, "
        "quantilesExact(0.5, 0.95, 0.99)(latency_us) AS latency_q "
        f"FROM {facts.base} WHERE latency_us IS NOT NULL AND url != '' GROUP BY url "
        f"HAVING requests >= {{min_requests:UInt64}} ORDER BY {_ORDER_BY_P95} LIMIT {{limit:UInt32}}",
        {**facts.params, "min_requests": int(req.slow_urls_min_requests), "limit": int(req.slow_urls_limit)},
    )
    return {
        "has_data": bool(rows),
        "rows": [
            {
                "url": row["value"],
                "requests": int(row["requests"]),
                "p50_ms": _milliseconds(_quantile(row, "latency_q", 0, "requests")),
                "p95_ms": _milliseconds(_quantile(row, "latency_q", 1, "requests")),
                "p99_ms": _milliseconds(_quantile(row, "latency_q", 2, "requests")),
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def _facts_status_codes(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
    rows = service.client.execute(
        "/* origin:facts_status_codes */ SELECT "
        "toInt32(multiIf(status BETWEEN 100 AND 599, status, -1)) AS value, count() AS requests "
        f"FROM {facts.base} WHERE status IS NOT NULL GROUP BY value ORDER BY requests DESC",
        facts.params,
    )
    total = sum(int(row["requests"]) for row in rows)
    return {
        "has_data": bool(rows),
        "rows": [
            {"status": int(row["value"]), "count": int(row["requests"]), "pct": int(row["requests"]) * 100.0 / total}
            for row in rows
            if total > 0
        ],
        **_NOT_APPROX,
    }


def _facts_latency_dimension(
    service: HighScaleService, facts: FactsScope, *, marker: str, dimension: str, limit: int | None = None
) -> list[dict[str, Any]]:
    column, predicate, minimum = _LATENCY_DIMENSIONS[dimension]
    params = dict(facts.params)
    suffix = f" HAVING requests >= {minimum}" if minimum else ""
    suffix += f" ORDER BY {_ORDER_BY_P95}"
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " LIMIT {limit:UInt32}"
    return service.client.execute(
        f"/* origin:facts_{marker} */ SELECT {column} AS value, count() AS requests, "
        "countIf(status BETWEEN 500 AND 599) AS origin_5xx, "
        "countIf(latency_us IS NOT NULL) AS latency_n, "
        "quantilesExact(0.5, 0.95)(latency_us) AS latency_q "
        f"FROM {facts.base} WHERE {predicate} GROUP BY {column}{suffix}",
        params,
    )


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
    where, params = _where(service.service_id, "origin_dimensions", start_time, end_time, dimension=dimension)
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
    return parsed / 1000.0 if parsed is not None and math.isfinite(parsed) else None


def _float_or_none(value: object) -> float | None:
    return float(str(value)) if value is not None else None


def origin_aggregates(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None, *, sections: set[str] | None
) -> dict[str, Any]:
    filter_sql, _ = build_clickhouse_filters(req.filters)
    facts = _facts_scope(service, start_time, end_time, req.filters) if filter_sql != "1=1" else None
    approx = _APPROX if facts is None else _NOT_APPROX

    def latency_rows(marker: str, dimension: str, limit: int | None = None, having: str | None = None):
        if facts is not None:
            return _facts_latency_dimension(service, facts, marker=marker, dimension=dimension, limit=limit)
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
            "ip_health", "oip", limit=req.ip_health_limit, having="sum(origin_minute_dimensions.requests) >= 10"
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
                    _origin_summary(service, start_time, end_time) if facts is None else _facts_summary(service, facts)
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
                    else _facts_timeseries(service, facts, req)
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
                    else _facts_slow_urls(service, facts, req)
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
                    else _facts_status_codes(service, facts)
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
    result = trace.run_parallel(tasks, source="projection" if facts is None else "fact")

    summary = result.get("summary", _EMPTY["summary"])
    return {"has_data": bool(summary.get("has_data")), **result, "section_timings": trace.timings()}


def _is_elevated(value_us: object, median_p95_ms: float) -> bool:
    value_ms = _milliseconds(value_us)
    return value_ms is not None and value_ms > median_p95_ms * 2


# ── Pure request-event projection (folded from backend.high_scale.origin_projection) ──


@dataclass(frozen=True)
class OriginProjection:
    summary_rows: tuple[dict[str, Any], ...]
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _OriginAccumulator:
    requests: int = 0
    misses: int = 0
    passes: int = 0
    origin_5xx: int = 0
    status_count: int = 0
    origin_bytes: int = 0
    latencies: list[float] = _dc_field(default_factory=list)
    ttlb_values: list[float] = _dc_field(default_factory=list)
    overhead_values: list[float] = _dc_field(default_factory=list)
    byte_values: list[float] = _dc_field(default_factory=list)


def build_origin_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> OriginProjection:
    summaries: dict[datetime, _OriginAccumulator] = defaultdict(_OriginAccumulator)
    dimensions: dict[tuple[datetime, str, str], _OriginAccumulator] = defaultdict(_OriginAccumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        latency = _origin_latency_us(row)
        status = _integer(row.get("ost"))
        origin_bytes = _non_negative_integer(row.get("obytes"))
        if latency is not None:
            summary = summaries[bucket]
            _add_latency_metrics(summary, row, latency, status, origin_bytes)

        if status is not None:
            status_value = str(status if 100 <= status <= 599 else -1)
            status_acc = dimensions[(bucket, "status", status_value)]
            status_acc.requests += 1
            status_acc.origin_5xx += int(500 <= status < 600)

        for dimension, value in _latency_dimensions(row):
            if latency is None:
                continue
            acc = dimensions[(bucket, dimension, value)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

        origin_ip = _text(row.get("oip"))
        if origin_ip and status is not None:
            acc = dimensions[(bucket, "oip", origin_ip)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

    return OriginProjection(
        summary_rows=tuple(_origin_summary_row(bucket, acc) for bucket, acc in sorted(summaries.items())),
        dimension_rows=tuple(
            _origin_dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
    )


def _add_latency_metrics(
    acc: _OriginAccumulator, row: Mapping[str, Any], latency: float, status: int | None, origin_bytes: int | None
) -> None:
    acc.requests += 1
    acc.latencies.append(latency)
    cache = _text(row.get("cache")).upper()
    acc.misses += int(cache.startswith("MISS"))
    acc.passes += int(cache.startswith("PASS"))
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    acc.status_count += int(status is not None)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes
        acc.byte_values.append(float(origin_bytes))
    ttlb = _finite_float(row.get("ottlb"))
    if ttlb is not None:
        acc.ttlb_values.append(ttlb)
        elapsed = _finite_float(row.get("elapsed"))
        if elapsed is not None:
            acc.overhead_values.append(elapsed - ttlb)


def _add_dimension_metrics(
    acc: _OriginAccumulator, latency: float | None, status: int | None, origin_bytes: int | None
) -> None:
    if latency is not None:
        acc.latencies.append(latency)
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes


def _origin_summary_row(bucket: datetime, acc: _OriginAccumulator) -> dict[str, Any]:
    lat_p50, lat_p75, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.75, 0.95, 0.99))
    ttlb_p50, ttlb_p95 = _percentiles(acc.ttlb_values, (0.50, 0.95))
    (overhead_p50,) = _percentiles(acc.overhead_values, (0.50,))
    (bytes_p50,) = _percentiles(acc.byte_values, (0.50,))
    latency_sk = _sketch(acc.latencies)
    ttlb_sk = _sketch(acc.ttlb_values)
    overhead_sk = _sketch(acc.overhead_values)
    bytes_sk = _sketch(acc.byte_values)
    return {
        "bucket_start": bucket.isoformat(),
        "requests": acc.requests,
        "misses": acc.misses,
        "passes": acc.passes,
        "origin_5xx": acc.origin_5xx,
        "status_count": acc.status_count,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "ttlb_count": len(acc.ttlb_values),
        "overhead_count": len(acc.overhead_values),
        "origin_bytes_count": len(acc.byte_values),
        "latency_p50_us": lat_p50,
        "latency_p75_us": lat_p75,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
        "ttlb_p50_us": ttlb_p50,
        "ttlb_p95_us": ttlb_p95,
        "cdn_overhead_p50_us": overhead_p50,
        "origin_bytes_p50": bytes_p50,
        "latency_sk_v": latency_sk[0],
        "latency_sk_w": latency_sk[1],
        "ttlb_sk_v": ttlb_sk[0],
        "ttlb_sk_w": ttlb_sk[1],
        "overhead_sk_v": overhead_sk[0],
        "overhead_sk_w": overhead_sk[1],
        "origin_bytes_sk_v": bytes_sk[0],
        "origin_bytes_sk_w": bytes_sk[1],
    }


def _origin_dimension_row(bucket: datetime, dimension: str, value: str, acc: _OriginAccumulator) -> dict[str, Any]:
    lat_p50, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.95, 0.99))
    latency_sk = _sketch(acc.latencies)
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "origin_5xx": acc.origin_5xx,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "latency_p50_us": lat_p50,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
        "latency_sk_v": latency_sk[0],
        "latency_sk_w": latency_sk[1],
    }


def _latency_dimensions(row: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    for dimension, source_field in (("url", "url"), ("pop", "pop")):
        value = _text(row.get(source_field))
        if value:
            values.append((dimension, value))
    edge = row.get("edge")
    if isinstance(edge, bool):
        values.append(("edge", "true" if edge else "false"))
    return tuple(values)


def _origin_latency_us(row: Mapping[str, Any]) -> float | None:
    ottfb = _finite_float(row.get("ottfb"))
    if ottfb is not None:
        return ottfb
    ttfb = _finite_float(row.get("ttfb"))
    return ttfb * 1_000_000.0 if ttfb is not None else None


def _non_negative_integer(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None


from backend.high_scale.network import origin_shielding_analysis  # noqa: E402

aggregates = origin_aggregates
shielding_analysis = origin_shielding_analysis
_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.origin_projection"] = _self_mod
if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "origin", _self_mod)
    setattr(_hs_pkg, "origin_projection", _self_mod)
