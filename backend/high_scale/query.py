from __future__ import annotations

import re
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import QueryRequest


def query_endpoint(
    service: HighScaleService, req: QueryRequest, start_time: str | None, end_time: str | None, mask_ips: bool
) -> dict[str, Any]:

    sql = req.sql.strip()

    # Very basic translation from DuckDB syntax to ClickHouse syntax for the Query Builder
    sql = re.sub(r"\blogs\b", "fastly_log_analytics.request_facts", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_vitals\b", "fastly_log_analytics.rum_vitals_facts", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_errors\b", "fastly_log_analytics.rum_error_facts", sql, flags=re.IGNORECASE)

    from backend.high_scale.dashboard import _FIELD_DIMENSIONS

    for field in _FIELD_DIMENSIONS.values():
        if field in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            continue
        elif field in {"age", "ttl"}:
            # Instead of a complex cast which might break generic SQL, we just extract it.
            # In a WHERE clause `age = 2367`, it's easier to just use the raw string or let ClickHouse cast it.
            # But the UI sends `age = '2367'` or `age IN ('2367')`
            sql = re.sub(rf"\b{field}\b", f"custom_fields['{field}']", sql)
        else:
            sql = re.sub(rf"\b{field}\b", f"custom_fields['{field}']", sql)

    # Inject time bounds
    time_clauses = [f"service_id = '{service.service_id}'"]
    if start_time:
        time_clauses.append(f"event_timestamp >= parseDateTime64BestEffort('{start_time}')")
    if end_time:
        time_clauses.append(f"event_timestamp <= parseDateTime64BestEffort('{end_time}')")
    time_cond = " AND ".join(time_clauses)

    if "WHERE" in sql.upper():
        sql = re.sub(r"\bWHERE\b", f"WHERE {time_cond} AND ", sql, flags=re.IGNORECASE)
    elif "GROUP BY" in sql.upper():
        sql = re.sub(r"\bGROUP BY\b", f"WHERE {time_cond} GROUP BY ", sql, flags=re.IGNORECASE)
    elif "ORDER BY" in sql.upper():
        sql = re.sub(r"\bORDER BY\b", f"WHERE {time_cond} ORDER BY ", sql, flags=re.IGNORECASE)
    else:
        sql += f" WHERE {time_cond}"

    if "LIMIT" not in sql.upper():
        sql += f" LIMIT {req.max_rows}"

    try:
        res = service.client.execute(sql)
    except Exception as e:
        # Give a generic error response expected by the UI
        return {"error": str(e), "data": [], "columns": [], "time_ms": 0}

    data = []
    cols = list(res[0].keys()) if res else []

    for d in res:
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                val = v.isoformat()
                d[k] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
        data.append(d)

    return {
        "data": data,
        "columns": cols,
        "time_ms": 10,  # Mock execution time
        "telemetry": {},
    }
