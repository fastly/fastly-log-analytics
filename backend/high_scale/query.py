from __future__ import annotations

import re
import time
from typing import Any

import duckdb

from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import QueryRequest
from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _safe_table
from backend.repositories.query import MAX_QUERY_ROWS
from backend.utils.sql_validator import SQLValidationError, validate_user_sql
from backend.utils.telemetry import get_tracked_calls


def query_endpoint(
    service: HighScaleService,
    req: QueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
    *,
    session_id: str | None = None,
) -> dict[str, Any]:
    timer = SectionTimer()
    section_timings = timer.entries

    raw_sql = req.sql.strip()
    max_rows = max(1, min(int(req.max_rows), MAX_QUERY_ROWS))
    svc_table = _safe_table(service.service_id)
    allowed_tables = frozenset({"logs", "client_vitals", "client_errors", svc_table.lower()})

    _t_val = time.perf_counter()
    with duckdb.connect(":memory:") as parser_con:
        try:
            validate_user_sql(
                raw_sql,
                parser_con=parser_con,
                session_id=session_id,
                service_id=service.service_id,
                allowed_tables=allowed_tables,
            )
        except SQLValidationError as exc:
            raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t_val)

    sql = raw_sql.rstrip().rstrip(";")

    # Translate DuckDB table names to ClickHouse fact tables
    if svc_table != "logs":
        sql = re.sub(rf"\b{re.escape(svc_table)}\b", "request_facts", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\blogs\b", "request_facts", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_vitals\b", "rum_vitals_facts", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_errors\b", "rum_error_facts", sql, flags=re.IGNORECASE)

    # Translate canonical column names for ClickHouse fact tables
    sql = re.sub(r"(?<!AS\s)(?<!')\btimestamp\b(?!')", "event_timestamp", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(?<!AS\s)(?<!')\bip\b(?!')", "client_ip", sql, flags=re.IGNORECASE)

    from backend.high_scale.dashboard import _FIELD_DIMENSIONS

    for field in _FIELD_DIMENSIONS.values():
        if field in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            continue
        sql = re.sub(rf"(?<!')\b{field}\b(?!')", f"custom_fields['{field}']", sql)

    has_fact_table = bool(
        re.search(r"\b(?:request_facts|rum_vitals_facts|rum_error_facts)\b", sql, flags=re.IGNORECASE)
    )
    if has_fact_table:
        time_clauses = [f"service_id = '{service.service_id}'"]
        if start_time:
            time_clauses.append(f"event_timestamp >= parseDateTime64BestEffort('{start_time}')")
        if end_time:
            time_clauses.append(f"event_timestamp <= parseDateTime64BestEffort('{end_time}')")
        time_cond = " AND ".join(time_clauses)

        if re.search(r"\bWHERE\b", sql, flags=re.IGNORECASE):
            sql = re.sub(r"\bWHERE\b", f"WHERE {time_cond} AND ", sql, count=1, flags=re.IGNORECASE)
        elif re.search(r"\bGROUP\s+BY\b", sql, flags=re.IGNORECASE):
            sql = re.sub(r"\bGROUP\s+BY\b", f"WHERE {time_cond} GROUP BY ", sql, count=1, flags=re.IGNORECASE)
        elif re.search(r"\bORDER\s+BY\b", sql, flags=re.IGNORECASE):
            sql = re.sub(r"\bORDER\s+BY\b", f"WHERE {time_cond} ORDER BY ", sql, count=1, flags=re.IGNORECASE)
        elif re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
            sql = re.sub(r"\bLIMIT\b", f"WHERE {time_cond} LIMIT ", sql, count=1, flags=re.IGNORECASE)
        else:
            sql += f" WHERE {time_cond}"

    has_explicit_limit = bool(re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE))
    if not has_explicit_limit:
        sql += f" LIMIT {max_rows + 1}"

    t0 = time.perf_counter()
    try:
        res = service.client.execute(sql)
    except Exception as e:
        raise RuntimeError(str(e)) from e
    timer.mark("execute", t0)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

    _debug_queries = [
        {
            "sql": _compact_sql_for_debug(sql.strip()),
            "time_ms": elapsed_ms,
            "engine": "ClickHouse",
        }
    ]

    fetched_rows = len(res) if res else 0
    truncated = fetched_rows > max_rows
    if truncated:
        res = res[:max_rows]
    total_rows = -1 if (truncated and not has_explicit_limit) else fetched_rows

    _t_serialize = time.perf_counter()
    data: list[dict[str, Any]] = []
    cols = list(res[0].keys()) if res else []
    redact_keys = IP_FAMILY_KEYS | SESSION_ID_KEYS | {"client_ip"}

    for d in res:
        row_copy = dict(d)
        for k, v in row_copy.items():
            if hasattr(v, "isoformat"):
                val = v.isoformat()
                row_copy[k] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
            elif mask_ips and k.lower() in redact_keys and v:
                row_copy[k] = "[redacted]"
        data.append(row_copy)

    if mask_ips:
        data = mask_ip_values(data)
    timer.mark("serialize_json", _t_serialize)

    return {
        "columns": cols,
        "data": data,
        "row_count": len(data),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": _debug_queries,
        "_debug_queries": _debug_queries,
        "debug_calls": get_tracked_calls(),
        "section_timings": section_timings,
    }
