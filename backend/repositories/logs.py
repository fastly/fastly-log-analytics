"""Repository for Raw Logs Explorer."""

from __future__ import annotations

import time as _time
from typing import Any

import duckdb

from backend.core.share_db.validation import SESSION_ID_KEYS, mask_ip
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
)
from backend.repositories.utils.filters import build_where_clause


def query_logs(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 100,
    offset: int = 0,
    sort_dir: str = "DESC",
    search: str | None = None,
    columns: list[str] | None = None,
    mask_ips: bool = False,
) -> dict[str, Any]:
    """Execute a paginated, partition-pruned raw logs query with optional IP masking."""
    timer = SectionTimer()
    section_timings = timer.entries

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)

    if not actual_cols:
        return empty_schema_response(
            rows=[],
            total_count=0,
            has_more=False,
            limit=limit,
            offset=offset,
            columns=[],
            schema_columns=[],
            missing_fields=[],
            section_timings=section_timings,
            **runner.telemetry(),
        )

    # Fetch column types for schema metadata
    _t = _time.perf_counter()
    schema_columns: list[dict[str, str]] = []
    try:
        describe_rows = con.execute(f"DESCRIBE {table_name}").fetchall()
        schema_columns = [{"name": r[0], "type": str(r[1])} for r in describe_rows]
    except Exception:
        schema_columns = [{"name": c, "type": "VARCHAR"} for c in actual_cols]
    timer.mark("describe_schema", _t)

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        actual_cols,
        inline_params=False,
        partition_pruning=True,
    )

    # Search filter across common text fields
    conditions = [where_clause]
    query_params = list(params)
    if search and search.strip():
        term = f"%{search.strip()}%"
        search_parts = []
        for search_col in ("url", "client_ip", "status", "pop", "request_method", "fastly_service_id"):
            if search_col in actual_cols:
                search_parts.append(f'CAST("{search_col}" AS VARCHAR) ILIKE ?')
                query_params.append(term)
        if search_parts:
            conditions.append(f"({' OR '.join(search_parts)})")

    eff_where_clause = " AND ".join(conditions)
    timer.mark("build_where_clause", _t)

    # Total count query
    _t = _time.perf_counter()
    count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {eff_where_clause}"
    try:
        count_res = con.execute(count_sql, query_params).fetchone()
        total_count = int(count_res[0]) if count_res else 0
    except Exception:
        total_count = 0
    timer.mark("count_query", _t)

    # Columns selection
    if columns:
        valid_selected = [c for c in columns if c in actual_cols]
        if "timestamp" in actual_cols and "timestamp" not in valid_selected:
            valid_selected.insert(0, "timestamp")
        selected_cols = valid_selected or actual_cols
    else:
        selected_cols = actual_cols

    col_select_sql = ", ".join(f'"{c}"' for c in selected_cols)
    safe_sort_dir = "ASC" if str(sort_dir).upper() == "ASC" else "DESC"

    order_clause = ""
    if "timestamp" in actual_cols:
        order_clause = f'ORDER BY "timestamp" {safe_sort_dir}'

    data_sql = f"SELECT {col_select_sql} FROM {table_name} WHERE {eff_where_clause} {order_clause} LIMIT ? OFFSET ?"
    data_params = query_params + [limit, offset]

    _t = _time.perf_counter()
    try:
        arrow_table = con.execute(data_sql, data_params).arrow()
        records: list[dict[str, Any]] = arrow_table.to_pylist()
    except Exception:
        cursor = con.execute(data_sql, data_params)
        desc = cursor.description or []
        colnames = [d[0] for d in desc]
        records = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    timer.mark("data_query", _t)

    # Convert datetimes / timestamps to ISO strings if needed
    _t = _time.perf_counter()
    for row in records:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

        if mask_ips:
            for ip_key in ("client_ip", "ip", "remote_addr"):
                if ip_key in row and row[ip_key]:
                    row[ip_key] = mask_ip(str(row[ip_key]))
            for sess_key in SESSION_ID_KEYS:
                if sess_key in row and row[sess_key]:
                    row[sess_key] = "[redacted]"
    timer.mark("ip_masking", _t)

    has_more = (offset + len(records)) < total_count

    return {
        "rows": records,
        "total_count": total_count,
        "has_more": has_more,
        "limit": limit,
        "offset": offset,
        "columns": selected_cols,
        "schema_columns": schema_columns,
        "missing_fields": [],
        "section_timings": section_timings,
        **runner.telemetry(),
    }
