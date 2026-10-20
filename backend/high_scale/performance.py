from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.high_scale._projection_utils import (
    range_value as _range_value,
)
from backend.high_scale.dashboard import _FIELD_DIMENSIONS, _build_clickhouse_filters
from backend.high_scale.performance_projection import SCATTER_SAMPLE_SIZE
from backend.high_scale.registry import HighScaleService
from backend.models.performance import PerformanceAggregatesResponse

_ALL_SECTIONS = frozenset({"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"})
_SORT_COLUMNS = {"avg": "avg_ms", "p50": "p50_ms", "p95": "p95_ms", "p99": "p99_ms"}
# Same minimum request counts as the standard path's live queries.
_MIN_URL_REQUESTS = 5
_MIN_ASN_REQUESTS = 10
_TOP_N = 20

# Upper bound (seconds, inclusive) -> label; identical to the standard ttl_dist CASE.
_TTL_EDGES = (
    (0, "0s"),
    (10, "<10s"),
    (30, "<30s"),
    (60, "<1m"),
    (300, "<5m"),
    (600, "<10m"),
    (1800, "<30m"),
    (3600, "<1h"),
    (10800, "<3h"),
    (21600, "<6h"),
    (43200, "<12h"),
    (86400, "<1d"),
    (259200, "<3d"),
    (604800, "<1w"),
    (1209600, "<2w"),
    (2592000, "<30d"),
    (7776000, "<90d"),
    (31536000, "<1y"),
)


def _ttl_bucket_expr(column: str) -> str:
    arms = ", ".join(f"{column} <= {edge}, '{label}'" for edge, label in _TTL_EDGES)
    return f"multiIf({arms}, '>1y')"


# Raw request_facts expressions. Event fields live as strings in custom_fields, in
# the units the log format emits: elapsed/ottfb/ottlb in microseconds, ttfb in seconds.
_ELAPSED_MS = "toFloat64OrNull(custom_fields['elapsed']) / 1000.0"
_TTFB_MS = "toFloat64OrNull(custom_fields['ttfb']) * 1000.0"
_OTTFB_MS = "coalesce(toFloat64OrNull(custom_fields['ottfb']), 0) / 1000.0"
_OTTLB_MS = "coalesce(toFloat64OrNull(custom_fields['ottlb']), 0) / 1000.0"


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
        clauses.append("bucket_start <= {end:DateTime64(3)}")
        params["end"] = end
    if dimension is not None:
        clauses.append("dimension={dimension:String}")
        params["dimension"] = dimension
    return " AND ".join(clauses), params


def _facts_where(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    filter_sql, filter_params = _build_clickhouse_filters(dict(filters))
    clauses = ["service_id={service_id:String}", "publication_state='visible'", filter_sql]
    params: dict[str, Any] = {"service_id": service_id, **filter_params}
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("event_timestamp <= {end:DateTime64(3)}")
        params["end"] = end
    return " AND ".join(clauses), params


def _active_filters(req: Any) -> dict[str, Any]:
    """Filters that actually constrain rows; a field with no facts mapping is rejected, never ignored."""
    active: dict[str, Any] = {}
    for field, config in (getattr(req, "filters", None) or {}).items():
        values = config.get("values") if isinstance(config, Mapping) else getattr(config, "values", None)
        if not values:
            continue
        if field not in _FIELD_DIMENSIONS:
            raise ValueError(f"unsupported Performance filter field for high-scale: {field}")
        active[field] = config
    return active


def _weighted(column: str, count_column: str = "latency_count") -> str:
    return f"sum({column} * {count_column}) / nullIf(sum(if({column} IS NOT NULL, {count_column}, 0)), 0)"


def _optional_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _projection_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    where, params = _where(
        service.service_id,
        "performance_dimensions",
        start_time,
        end_time,
        dimension=dimension,
    )
    params["limit"] = _TOP_N
    # The stored latency_*_ms columns hold the raw elapsed value, which is microseconds.
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, "
        "sum(requests) AS req_count, "
        "sum(latency_sum_ms) / nullIf(sum(latency_count), 0) / 1000.0 AS avg_ms, "
        f"{_weighted('latency_p50_ms')} / 1000.0 AS p50_ms, "
        f"{_weighted('latency_p95_ms')} / 1000.0 AS p95_ms, "
        f"{_weighted('latency_p99_ms')} / 1000.0 AS p99_ms "
        f"FROM performance_minute_dimensions WHERE {where} GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        params,
    )


def _facts_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    key_expr: str,
    where: str,
    params: dict[str, Any],
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, count() AS req_count, "
        "avg(elapsed_ms) AS avg_ms, "
        "quantile(0.5)(elapsed_ms) AS p50_ms, "
        "quantile(0.95)(elapsed_ms) AS p95_ms, "
        "quantile(0.99)(elapsed_ms) AS p99_ms "
        f"FROM (SELECT {key_expr} AS value, {_ELAPSED_MS} AS elapsed_ms FROM request_facts WHERE {where}) "
        f"WHERE value != '' AND elapsed_ms IS NOT NULL GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        {**params, "limit": _TOP_N},
    )


def _latency_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requests": int(row["req_count"]),
        "avg": _optional_float(row["avg_ms"]),
        "p50": _optional_float(row["p50_ms"]),
        "p95": _optional_float(row["p95_ms"]),
        "p99": _optional_float(row["p99_ms"]),
    }


def _asn_rows(service: HighScaleService, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach the same ``label`` the standard path's ``_resolve_asns`` adds."""
    from backend.core import duckdb as _db

    asn_list = [int(row["value"]) for row in rows if str(row["value"]).isdigit()]
    asn_names = _db.get_asn_names(service.service_id, asn_list)
    out: list[dict[str, Any]] = []
    for row in rows:
        asn: int | str = int(row["value"]) if str(row["value"]).isdigit() else row["value"]
        label = _db.format_asn_label(asn, asn_names.get(asn, "")) if isinstance(asn, int) else str(asn)
        out.append({"asn": asn, "label": label, **_latency_row(row)})
    return out


def _ttl_dist(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        source = f"SELECT toFloat64OrNull(custom_fields['ttl']) AS ttl, 1 AS weight FROM request_facts WHERE {where}"
    else:
        where, params = _where(service.service_id, "request_aggregate", start_time, end_time, dimension="ttl")
        source = f"SELECT toFloat64OrNull(value) AS ttl, request_count AS weight FROM request_aggregates WHERE {where}"
    rows = service.client.execute(
        f"/* perf:ttl_dist */ SELECT {_ttl_bucket_expr('ttl')} AS bucket, sum(weight) AS count, min(ttl) AS min_ttl "
        f"FROM ({source}) WHERE ttl IS NOT NULL GROUP BY bucket ORDER BY min_ttl",
        params,
    )
    return [{"bucket": row["bucket"], "count": int(row["count"])} for row in rows]


def _waterfall(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any],
) -> dict[str, Any]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        sql = (
            "/* perf:waterfall */ SELECT count() AS component_count, "
            "sum(greatest(0, ttfb_ms - ottfb_ms)) / nullIf(count(), 0) AS edge_processing, "
            "sum(ottfb_ms) / nullIf(count(), 0) AS origin_wait, "
            "sum(greatest(0, ottlb_ms - ottfb_ms)) / nullIf(count(), 0) AS origin_download, "
            "sum(greatest(0, elapsed_ms - greatest(ottlb_ms, ttfb_ms))) / nullIf(count(), 0) AS client_download "
            f"FROM (SELECT {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"{_OTTFB_MS} AS ottfb_ms, {_OTTLB_MS} AS ottlb_ms FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL"
        )
    else:
        where, params = _where(service.service_id, "performance_summary", start_time, end_time)
        sql = (
            "/* perf:waterfall */ SELECT sum(component_count) AS component_count, "
            "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0) AS edge_processing, "
            "sum(origin_wait_sum_ms) / nullIf(sum(component_count), 0) AS origin_wait, "
            "sum(origin_download_sum_ms) / nullIf(sum(component_count), 0) AS origin_download, "
            "sum(client_download_sum_ms) / nullIf(sum(component_count), 0) AS client_download "
            f"FROM performance_minute_summary WHERE {where}"
        )
    rows = service.client.execute(sql, params)
    row = rows[0] if rows else {}
    return {
        "avg": {
            "edge_processing": _optional_float(row.get("edge_processing")),
            "origin_wait": _optional_float(row.get("origin_wait")),
            "origin_download": _optional_float(row.get("origin_download")),
            "client_download": _optional_float(row.get("client_download")),
        }
    }


def _scatter(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        sql = (
            "/* perf:scatter */ SELECT ttfb_ms AS origin_ms, elapsed_ms - ttfb_ms AS edge_ms, cache "
            f"FROM (SELECT event_id, {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"custom_fields['cache'] AS cache FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL AND elapsed_ms - ttfb_ms >= 0 "
            "ORDER BY cityHash64(event_id) LIMIT {scatter_limit:UInt32}"
        )
    else:
        where, params = _where(service.service_id, "performance_scatter", start_time, end_time)
        sql = (
            "/* perf:scatter */ SELECT origin_ms, edge_ms, cache "
            f"FROM performance_scatter_samples WHERE {where} "
            "ORDER BY sample_key LIMIT {scatter_limit:UInt32}"
        )
    rows = service.client.execute(sql, {**params, "scatter_limit": SCATTER_SAMPLE_SIZE})
    return [
        {
            "origin": _optional_float(row["origin_ms"]),
            "edge": _optional_float(row["edge_ms"]),
            "cache": row["cache"] or None,
        }
        for row in rows
    ]


def performance_aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
) -> PerformanceAggregatesResponse:
    wanted = _ALL_SECTIONS if sections is None else sections
    filters = _active_filters(req)
    sort_column = _SORT_COLUMNS.get(getattr(req, "sort_by", "p99"), "p99_ms")
    result: dict[str, Any] = {}

    if "top_urls" in wanted:
        if filters:
            where, params = _facts_where(service.service_id, start_time, end_time, filters)
            rows = _facts_percentile_rows(
                service,
                marker="top_urls",
                key_expr="url",
                where=where,
                params=params,
                sort_column=sort_column,
                min_requests=_MIN_URL_REQUESTS,
            )
        else:
            rows = _projection_percentile_rows(
                service,
                marker="top_urls",
                dimension="url",
                start_time=start_time,
                end_time=end_time,
                sort_column=sort_column,
                min_requests=_MIN_URL_REQUESTS,
            )
        result["top_urls"] = [{"url": row["value"], **_latency_row(row)} for row in rows]

    if "top_asns" in wanted:
        if filters:
            where, params = _facts_where(service.service_id, start_time, end_time, filters)
            rows = _facts_percentile_rows(
                service,
                marker="top_asns",
                key_expr="custom_fields['asn']",
                where=where,
                params=params,
                sort_column=sort_column,
                min_requests=_MIN_ASN_REQUESTS,
            )
        else:
            rows = _projection_percentile_rows(
                service,
                marker="top_asns",
                dimension="asn",
                start_time=start_time,
                end_time=end_time,
                sort_column=sort_column,
                min_requests=_MIN_ASN_REQUESTS,
            )
        result["top_asns"] = _asn_rows(service, rows)

    if "ttl_dist" in wanted:
        result["ttl_dist"] = _ttl_dist(service, start_time, end_time, filters)
    if "waterfall" in wanted:
        result["waterfall"] = _waterfall(service, start_time, end_time, filters)
    if "scatter" in wanted:
        result["scatter"] = _scatter(service, start_time, end_time, filters)

    # Unfiltered top tables request-weight-average per-minute percentiles; filtered ones
    # are exact ClickHouse quantiles over the matching facts.
    return PerformanceAggregatesResponse.with_telemetry(**result, _approx=not filters)
