"""High-scale raw log query implementation backed by ClickHouse request_facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.share_db.validation import SESSION_ID_KEYS, mask_ip
from backend.high_scale._projection_utils import range_value
from backend.high_scale.registry import HighScaleService
from backend.models.logs import LogsQueryRequest


def _parse_ts(val: str | None) -> datetime | None:
    if not val:
        return None
    try:
        return range_value(val)
    except Exception:
        return None


def logs_query_endpoint(
    service: HighScaleService,
    req: LogsQueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
) -> dict[str, Any]:
    """Execute high-scale logs query against ClickHouse request_facts."""
    now = datetime.now(UTC)
    end_dt = _parse_ts(end_time) or now
    start_dt = _parse_ts(start_time) or (end_dt - timedelta(hours=24))

    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
    }

    where_clauses = [
        "service_id = {service_id:String}",
        "publication_state = 'visible'",
        "event_timestamp >= {start_time:DateTime64(3)}",
        "event_timestamp <= {end_time:DateTime64(3)}",
    ]

    # Filters
    if req.filters:
        for idx, (k, v) in enumerate(req.filters.items()):
            if not v:
                continue
            param_name = f"flt_{idx}"
            if k == "client_ip":
                where_clauses.append(f"client_ip = {{{param_name}:String}}")
                params[param_name] = str(v)
            elif k == "url":
                where_clauses.append(f"url = {{{param_name}:String}}")
                params[param_name] = str(v)
            elif k == "country":
                where_clauses.append(f"country = {{{param_name}:String}}")
                params[param_name] = str(v)
            else:
                where_clauses.append(f"custom_fields['{k}'] = {{{param_name}:String}}")
                params[param_name] = str(v)

    # Search term
    if req.search and req.search.strip():
        search_term = f"%{req.search.strip()}%"
        params["search_term"] = search_term
        where_clauses.append(
            "(url ILIKE {search_term:String} OR client_ip ILIKE {search_term:String} OR "
            "custom_fields['status'] ILIKE {search_term:String} OR custom_fields['pop'] ILIKE {search_term:String})"
        )

    where_sql = " AND ".join(where_clauses)

    # Count query
    count_sql = f"SELECT count() as cnt FROM request_facts WHERE {where_sql}"
    try:
        count_res = service.client.execute(count_sql, params)
        total_count = int(count_res[0]["cnt"]) if count_res else 0
    except Exception:
        total_count = 0

    sort_dir = "ASC" if str(req.sort_dir).upper() == "ASC" else "DESC"
    limit = req.limit or 100
    offset = req.offset or 0

    data_sql = f"""
    SELECT
        event_timestamp as timestamp,
        client_ip,
        url,
        country,
        custom_fields
    FROM request_facts
    WHERE {where_sql}
    ORDER BY event_timestamp {sort_dir}
    LIMIT {limit} OFFSET {offset}
    """

    res = service.client.execute(data_sql, params)
    rows: list[dict[str, Any]] = []
    seen_cols: set[str] = {"timestamp", "client_ip", "url", "country"}

    for r in res:
        ts_val = r.get("timestamp")
        iso_ts = ""
        if ts_val is not None:
            if hasattr(ts_val, "isoformat"):
                iso_ts = str(ts_val.isoformat()).replace("+00:00", "Z")
            else:
                iso_ts = str(ts_val)

        row: dict[str, Any] = {
            "timestamp": iso_ts,
            "client_ip": r.get("client_ip", ""),
            "url": r.get("url", ""),
            "country": r.get("country", ""),
        }
        custom = r.get("custom_fields") or {}
        if isinstance(custom, dict):
            for ck, cv in custom.items():
                seen_cols.add(ck)
                row[ck] = cv

        if mask_ips:
            for ip_key in ("client_ip", "ip", "remote_addr"):
                if ip_key in row and row[ip_key]:
                    row[ip_key] = mask_ip(str(row[ip_key]))
            for sess_key in SESSION_ID_KEYS:
                if sess_key in row and row[sess_key]:
                    row[sess_key] = "[redacted]"

        rows.append(row)

    # Filter columns if requested
    if req.columns:
        selected_cols = [c for c in req.columns if c in seen_cols]
        if "timestamp" not in selected_cols:
            selected_cols.insert(0, "timestamp")
    else:
        selected_cols = sorted(list(seen_cols))

    schema_columns = [{"name": c, "type": "VARCHAR"} for c in selected_cols]
    has_more = (offset + len(rows)) < total_count

    return {
        "rows": rows,
        "total_count": total_count,
        "has_more": has_more,
        "limit": limit,
        "offset": offset,
        "columns": selected_cols,
        "schema_columns": schema_columns,
        "missing_fields": [],
    }
