"""DuckDB emulation of the high-scale ``request_facts`` keyset query for standard-mode services."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.models.high_scale import HighScaleRequestFactRequest
from backend.repositories._sql import request_facts as sql
from backend.utils.telemetry import track_query


def fetch_request_facts_emulation(
    con,
    candidate_tables: list[str],
    req: HighScaleRequestFactRequest,
    *,
    start: datetime,
    end: datetime,
    cursor_ts: Any,
) -> tuple[list[tuple], Any, Any]:
    """``(page rows + 1 lookahead, coverage_start, coverage_end)`` from the first candidate table with a schema.

    ``candidate_tables`` are ``_safe_table_name`` identifiers. A requested filter whose column is
    absent from the table yields ``([], None, None)`` rather than an unfiltered page. ``cursor_ts`` is the
    decoded keyset cursor timestamp (rows strictly after it), or ``None`` for the first page.
    """
    table = candidate_tables[0]
    col_names: set[str] = set()
    for cand in candidate_tables:
        try:
            with track_query(
                con, sql.DESCRIBE_TABLE.format(table=cand), [], "high_scale.describe_emulation"
            ) as desc_cur:
                col_names = {str(r[0]) for r in desc_cur.fetchall()}
            if col_names:
                table = cand
                break
        except Exception:
            continue

    if "timestamp" not in col_names:
        with track_query(con, sql.EMULATION_PROBE, [], "high_scale.emulation_probe") as probe_cur:
            probe_cur.fetchone()
        return [], None, None

    country_col = (
        "country" if "country" in col_names else ("geo_country_code" if "geo_country_code" in col_names else None)
    )

    select_exprs = [
        "timestamp",
        f"{country_col} AS country" if country_col else "'' AS country",
        "client_ip" if "client_ip" in col_names else "'' AS client_ip",
        "url" if "url" in col_names else "'' AS url",
        "CAST(status AS VARCHAR) AS status" if "status" in col_names else "'' AS status",
        "pop" if "pop" in col_names else "'' AS pop",
        "host" if "host" in col_names else "'' AS host",
    ]
    where_parts = ["timestamp >= ?", "timestamp <= ?"]
    params: list[object] = [start, end]

    if cursor_ts is not None:
        where_parts.append("timestamp > ?")
        params.append(cursor_ts)

    if req.country is not None and str(req.country).strip():
        if country_col:
            where_parts.append(f"{country_col} = ?")
            params.append(str(req.country).strip())
        else:
            return [], None, None

    if req.status is not None and str(req.status).strip():
        if "status" in col_names:
            where_parts.append("CAST(status AS VARCHAR) = ?")
            params.append(str(req.status).strip())
        else:
            return [], None, None

    if req.pop is not None and str(req.pop).strip():
        if "pop" in col_names:
            where_parts.append("pop = ?")
            params.append(str(req.pop).strip())
        else:
            return [], None, None

    if req.domain is not None and str(req.domain).strip():
        dom_val = str(req.domain).strip()
        if "host" in col_names and "url" in col_names:
            where_parts.append("(host = ? OR url ILIKE ?)")
            params.extend([dom_val, f"%{dom_val}%"])
        elif "host" in col_names:
            where_parts.append("host = ?")
            params.append(dom_val)
        elif "url" in col_names:
            where_parts.append("url ILIKE ?")
            params.append(f"%{dom_val}%")
        else:
            return [], None, None

    if req.batch_id is not None and str(req.batch_id).strip():
        b_val = str(req.batch_id).strip()
        if b_val != "duckdb-emulation":
            return [], None, None

    limit_plus_one = int(req.limit) + 1
    request_sql = (
        f"SELECT {', '.join(select_exprs)} "
        f"FROM {table} "
        f"WHERE {' AND '.join(where_parts)} "
        f"ORDER BY timestamp ASC "
        f"LIMIT {limit_plus_one}"
    )
    with track_query(con, request_sql, params, "high_scale.request_facts_emulation") as cur:
        fetched = cur.fetchall()

    ext_sql = sql.EXTENTS.format(table=table)
    with track_query(con, ext_sql, [], "high_scale.request_facts_extents") as ext_cur:
        ext_row = ext_cur.fetchone()
    cov_start = ext_row[0] if ext_row else None
    cov_end = ext_row[1] if ext_row else None
    return fetched, cov_start, cov_end
