"""Translate request-filter payloads into ClickHouse predicates over ``request_facts``.

Shared by the high-scale raw readers (logs, sessions, filtered dashboard aggregates) so a
filter means the same thing everywhere: keys are normalised exactly as the standard path and
the analyst PII filter-lock normalise them, every key and value is a typed parameter, and a
filter the fact table cannot express raises instead of silently returning unfiltered rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.high_scale._common import range_value as _range_value
from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key

# Real columns on request_facts; every other key lives in the ``custom_fields`` map.
_COLUMNS = {"client_ip": "client_ip", "ip": "client_ip", "country": "country", "url": "url"}
# Stored as floats in the map; the standard path filters on the rounded integer.
_ROUNDED = frozenset({"age", "ttl"})
# Stored as 'True'/'False' or '1'/'0' strings in custom_fields; normalise both sides to 'true'/'false'.
_BOOLEAN = frozenset({"edge", "is_ipv6", "waf"})
# Resolved by the standard path through lookups the fact table has no column for.
_UNSUPPORTED = frozenset({"_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind", "edge_score_reason_ind"})


def _column(key: str, params: dict[str, Any], param: str) -> str:
    if key in _COLUMNS:
        return _COLUMNS[key]
    params[param] = key
    lookup = f"custom_fields[{{{param}:String}}]"
    if key in _ROUNDED:
        return f"toString(toInt32(round(toFloat64OrZero({lookup}))))"
    if key in _BOOLEAN:
        return f"if(lower({lookup}) IN ('true', '1'), 'true', if(lower({lookup}) IN ('false', '0'), 'false', {lookup}))"
    return lookup


def request_filter_clauses(
    filters: Mapping[str, Any] | None, *, prefix: str = "flt"
) -> tuple[list[str], dict[str, Any]]:
    """Return ``(clauses, params)`` AND-ing every non-empty filter in ``filters``."""
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        key = normalize_filter_key(raw_key)
        mode = str(filter_spec_attr(spec, "mode") or "include")
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if not (isinstance(value, str) and value.strip() == "")
        ]
        if not values:
            continue
        if key in _UNSUPPORTED or not key:
            raise ValueError(f"high-scale request readers do not support filtering on {raw_key!r}")
        column = _column(key, params, f"{prefix}_k{i}")
        concrete = [value for value in values if value is not None]
        names: list[str] = []
        for j, value in enumerate(concrete):
            name = f"{prefix}_{i}_{j}"
            str_val = str(value)
            if key in _BOOLEAN:
                lowered = str_val.strip().lower()
                if lowered in ("true", "1"):
                    str_val = "true"
                elif lowered in ("false", "0"):
                    str_val = "false"
            params[name] = str_val
            names.append(f"{{{name}:String}}")
        include = mode != "exclude"
        parts: list[str] = []
        if names:
            parts.append(f"{column} {'IN' if include else 'NOT IN'} ({', '.join(names)})")
        if len(concrete) != len(values):
            # A null filter value means "the field is absent", which a map lookup reports as ''.
            parts.append(f"{column} {'=' if include else '!='} ''")
        clauses.append(f"({(' OR ' if include else ' AND ').join(parts)})")
    return clauses, params


def build_clickhouse_filters(
    filters: Mapping[str, Any] | None, *, prefix: str = "filter"
) -> tuple[str, dict[str, Any]]:
    """Return ``(sql, params)`` where ``sql`` is ``1=1`` when no filter is active."""
    clauses, params = request_filter_clauses(filters, prefix=prefix)
    return (" AND ".join(clauses) if clauses else "1=1"), params


def build_published_projection_where(
    service_id: str,
    domain: str,
    start_time: str | None,
    end_time: str | None,
    *,
    dimension: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build ClickHouse WHERE predicate and params for published minute projections."""
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


def build_published_facts_where(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any] | None = None,
    *,
    end_inclusive: bool = True,
    domain: str = "request",
) -> tuple[str, dict[str, Any]]:
    """Build ClickHouse WHERE predicate and params for published fact rows."""
    from backend.high_scale._common import visible_publication_fence_sql

    filter_sql, filter_params = build_clickhouse_filters(dict(filters or {}))
    clauses = ["service_id={service_id:String}", visible_publication_fence_sql(domain), filter_sql]
    params: dict[str, Any] = {"service_id": service_id, **filter_params}
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        op = "<=" if end_inclusive else "<"
        clauses.append(f"event_timestamp {op} {{end:DateTime64(3)}}")
        params["end"] = end
    return " AND ".join(clauses), params
