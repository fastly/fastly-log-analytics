"""Exact high-scale Origin reads straight from ``request_facts`` for filtered requests.

The Origin projections are pre-aggregated per minute without the filterable dimensions, so a request
carrying filters cannot be served from them. These readers apply the filters to the fact rows and
compute every card with exact quantiles, using the same per-row semantics as
``origin_projection.build_origin_projection_rows`` (latency is ``ottfb`` else ``ttfb``, status is ``ost``,
overhead is ``elapsed - ottlb``, ...), so filtered and unfiltered pages agree up to sketch error.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from backend.high_scale._projection_utils import range_value as _range_value
from backend.high_scale._scope import visible_fact_clauses
from backend.high_scale.registry import HighScaleService
from backend.utils.date_utils import safe_iso

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


@dataclass(frozen=True)
class FactsScope:
    base: str
    params: dict[str, Any]


def scope(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filter_sql: str,
    filter_params: Mapping[str, Any],
) -> FactsScope:
    clauses = [*visible_fact_clauses("request"), filter_sql]
    params: dict[str, Any] = {"service_id": service.service_id, **filter_params}
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("event_timestamp <= {end:DateTime64(3)}")
        params["end"] = end
    return FactsScope(f"(SELECT {_BASE_COLUMNS} FROM request_facts WHERE {' AND '.join(clauses)})", params)


def _ms(value: object) -> float | None:
    if value is None:
        return None
    parsed = float(str(value))
    return parsed / 1000.0 if parsed == parsed else None


def _quantile(row: Mapping[str, Any], key: str, index: int, count_key: str) -> float | None:
    """The ``index``-th quantile in microseconds; None when no row contributed an observation."""
    if int(row.get(count_key) or 0) == 0:
        return None
    values = row.get(key) or []
    return float(values[index]) if index < len(values) else None


def summary(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
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
        "ottfb_p50_ms": _ms(_quantile(row, "latency_q", 0, "requests")),
        "ottfb_p75_ms": _ms(_quantile(row, "latency_q", 1, "requests")),
        "ottfb_p95_ms": _ms(_quantile(row, "latency_q", 2, "requests")),
        "ottfb_p99_ms": _ms(_quantile(row, "latency_q", 3, "requests")),
        "ottlb_p50_ms": _ms(_quantile(row, "ttlb_q", 0, "ttlb_n")),
        "ottlb_p95_ms": _ms(_quantile(row, "ttlb_q", 1, "ttlb_n")),
        "cdn_overhead_p50_ms": _ms(float(row["overhead_q"])) if overhead_n else None,
        "origin_error_rate": int(row.get("origin_5xx") or 0) / status_count if status_count else None,
        "obytes_p50": float(row["bytes_q"]) if bytes_n else None,
        **_NOT_APPROX,
    }


_TIMESERIES_METRICS = {
    ("ttfb", "p50"): ("latency_us", 0.5),
    ("ttfb", "p95"): ("latency_us", 0.95),
    ("ttfb", "p99"): ("latency_us", 0.99),
    ("ttlb", "p50"): ("ottlb_us", 0.5),
    ("ttlb", "p95"): ("ottlb_us", 0.95),
    ("ttlb", "p99"): ("ottlb_us", 0.99),
}


def timeseries(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
    if req.split_by_leg:
        raise ValueError("high-scale Origin timeseries does not support split_by_leg")
    if req.bucket_minutes < 1:
        raise ValueError("high-scale Origin timeseries requires bucket_minutes of at least 1")
    metric = _TIMESERIES_METRICS.get((req.timeseries_metric, req.timeseries_percentile))
    if metric is None:
        raise ValueError("high-scale Origin timeseries does not support the requested metric and percentile")
    column, level = metric
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
                "value": _ms(row["value"]) if int(row.get("value_n") or 0) else None,
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def slow_urls(service: HighScaleService, facts: FactsScope, req: Any) -> dict[str, Any]:
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
                "p50_ms": _ms(_quantile(row, "latency_q", 0, "requests")),
                "p95_ms": _ms(_quantile(row, "latency_q", 1, "requests")),
                "p99_ms": _ms(_quantile(row, "latency_q", 2, "requests")),
            }
            for row in rows
        ],
        **_NOT_APPROX,
    }


def status_codes(service: HighScaleService, facts: FactsScope) -> dict[str, Any]:
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


_LATENCY_DIMENSIONS = {
    # dimension -> (column, extra predicate, minimum requests)
    "edge": ("edge_s", "latency_us IS NOT NULL AND edge_s != ''", 0),
    "pop": ("pop", "latency_us IS NOT NULL AND pop != ''", 0),
    "oip": ("oip", "status IS NOT NULL AND oip != ''", 10),
}


def latency_dimension(
    service: HighScaleService,
    facts: FactsScope,
    *,
    marker: str,
    dimension: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Rows shaped like the projection reader's: ``value``, ``requests``, ``origin_5xx``, ``latency_q``/``latency_n``."""
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
