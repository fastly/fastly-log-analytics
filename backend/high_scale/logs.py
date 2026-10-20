"""High-scale raw log query implementation backed by ClickHouse request_facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.share_db.validation import SESSION_ID_KEYS, mask_ip
from backend.high_scale._filters import request_filter_clauses
from backend.high_scale._projection_utils import range_value
from backend.high_scale._scope import visible_fact_clauses
from backend.high_scale.registry import HighScaleService
from backend.models.logs import LogsQueryRequest
from backend.utils.date_utils import safe_iso

_BASE_COLUMNS = ("timestamp", "client_ip", "url", "country")
# Newest rows sampled to discover which custom-field columns exist; keeps the column set
# stable from page to page without scanning the whole window for map keys.
_COLUMN_SAMPLE_ROWS = 1000
_SEARCH_FIELDS = (
    "url ILIKE {search_term:String}",
    "client_ip ILIKE {search_term:String}",
    "custom_fields['status'] ILIKE {search_term:String}",
    "custom_fields['pop'] ILIKE {search_term:String}",
    "custom_fields['method'] ILIKE {search_term:String}",
)


def _parse_ts(val: str | None) -> datetime | None:
    if not val:
        return None
    try:
        return range_value(val)
    except ValueError:
        return None


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        stamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.isoformat().replace("+00:00", "Z")
    return safe_iso(value) or ""


def logs_query_endpoint(
    service: HighScaleService,
    req: LogsQueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
) -> dict[str, Any]:
    """Execute high-scale logs query against ClickHouse request_facts."""
    end_dt = _parse_ts(end_time) or datetime.now(UTC)
    start_dt = _parse_ts(start_time) or (end_dt - timedelta(hours=24))

    filter_clauses, filter_params = request_filter_clauses(req.filters)
    clauses = [
        *visible_fact_clauses("request"),
        "event_timestamp >= {start_time:DateTime64(3)}",
        "event_timestamp <= {end_time:DateTime64(3)}",
        *filter_clauses,
    ]
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        **filter_params,
    }
    if req.search and req.search.strip():
        params["search_term"] = f"%{req.search.strip()}%"
        clauses.append(f"({' OR '.join(_SEARCH_FIELDS)})")
    where_sql = " AND ".join(clauses)

    count_rows = service.client.execute(
        f"/* logs:count */ SELECT count() AS cnt FROM request_facts WHERE {where_sql}", params
    )
    total_count = int(count_rows[0]["cnt"]) if count_rows else 0

    sort_dir = "ASC" if str(req.sort_dir).upper() == "ASC" else "DESC"
    limit = req.limit or 100
    offset = req.offset or 0
    page = service.client.execute(
        "/* logs:page */ SELECT event_timestamp AS timestamp, client_ip, url, country, custom_fields "
        f"FROM request_facts WHERE {where_sql} ORDER BY event_timestamp {sort_dir} "
        "LIMIT {limit:UInt32} OFFSET {offset:UInt32}",
        {**params, "limit": limit, "offset": offset},
    )
    sampled = service.client.execute(
        "/* logs:columns */ SELECT groupUniqArrayArray(mapKeys(custom_fields)) AS keys FROM ("
        f"SELECT custom_fields FROM request_facts WHERE {where_sql} "
        f"ORDER BY event_timestamp DESC LIMIT {_COLUMN_SAMPLE_ROWS})",
        params,
    )

    known: set[str] = set(_BASE_COLUMNS)
    known.update(sampled[0]["keys"] if sampled and sampled[0].get("keys") else [])
    rows: list[dict[str, Any]] = []
    for record in page:
        row: dict[str, Any] = {
            "timestamp": _iso(record.get("timestamp")),
            "client_ip": record.get("client_ip", ""),
            "url": record.get("url", ""),
            "country": record.get("country", ""),
        }
        custom = record.get("custom_fields") or {}
        if isinstance(custom, dict):
            known.update(custom)
            row.update(custom)
        rows.append(row)

    if req.columns:
        selected_cols = [column for column in req.columns if column in known]
        if "timestamp" not in selected_cols:
            selected_cols.insert(0, "timestamp")
    else:
        selected_cols = sorted(known)

    projected: list[dict[str, Any]] = []
    for row in rows:
        out = {column: row.get(column) for column in selected_cols}
        if mask_ips:
            for ip_key in ("client_ip", "ip", "remote_addr"):
                if out.get(ip_key):
                    out[ip_key] = mask_ip(str(out[ip_key]))
            for session_key in SESSION_ID_KEYS:
                if out.get(session_key):
                    out[session_key] = "[redacted]"
        projected.append(out)

    return {
        "rows": projected,
        "total_count": total_count,
        "has_more": (offset + len(projected)) < total_count,
        "limit": limit,
        "offset": offset,
        "columns": selected_cols,
        "schema_columns": [
            {"name": column, "type": "TIMESTAMP" if column == "timestamp" else "VARCHAR"} for column in selected_cols
        ],
        "missing_fields": [],
    }
