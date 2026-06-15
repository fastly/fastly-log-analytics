"""Sessions repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table, empty_schema_response
from backend.repositories._sql import sessions as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.pagination import calc_offset


def _collect_sessions_rollup_paths(src: dict, st: datetime, et: datetime) -> tuple[list[str], bool] | None:
    """Enumerate per-hour sessions.parquet files covering ``[st, et)``.

    Returns ``(paths, crosses_active_hour)`` on success, or ``None``
    if a closed hour in the window has per-field rollup data but no
    sessions.parquet (writer is behind — falling back to raw is
    safer than serving an undercount).

    Empty-hour tolerance (mirrors
    ``QueryRunner.try_time_series_from_rollup``): if a hour has no
    sessions.parquet AND no entry in the per-field rollup tree, we
    treat the hour as having genuinely zero data and skip it. The
    per-field tree is the same source the sessions backfill walks
    to decide what to write, so:

      - per-field has hour H + sessions.parquet exists  → use rollup
      - per-field has hour H + sessions.parquet missing → writer
        is behind → fall back to raw (don't undercount)
      - per-field has no hour H                          → hour was
        empty → skip (contributes zero sessions)

    Failure mode: between Iceberg commit and the per-field rollup
    writer running, an hour could have data in Iceberg but not in
    the per-field tree. With the cron running every minute on prod
    this gap is at most the active hour (already live-queried) +
    occasionally one hour behind. On local dev with no cron, the
    gap can span days — local rollup falls back to raw, which is
    expected/correct.
    """
    from backend.core.rollups import SESSIONS_BUNDLE_FILENAME, _hour_bundled_root
    from backend.repositories._base import collect_hourly_bundle_paths

    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None

    return collect_hourly_bundle_paths(src, st, et, bundled_root, SESSIONS_BUNDLE_FILENAME)


def _build_active_hour_session_sql(
    table_name: str,
    actual_cols: set,
    active_hour_dt: datetime,
    user_start: datetime,
    user_end: datetime,
) -> tuple[str, list]:
    """Build a SELECT that emits the same rollup-shaped columns as
    ``sessions.parquet`` for the slice
    ``[max(active_hour_start, user_start), min(active_hour_end, user_end))``.
    Used to UNION with rollup paths so the chart is current to the second.
    """
    ja4_expr = '"ja4"' if "ja4" in actual_cols else "CAST(NULL AS VARCHAR)"
    country_expr = 'CAST(MIN("country") AS VARCHAR)' if "country" in actual_cols else "CAST(NULL AS VARCHAR)"
    asn_expr = 'CAST(MIN("asn") AS INTEGER)' if "asn" in actual_cols else "CAST(NULL AS INTEGER)"
    reqs_4xx = (
        'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT)'
        if "status" in actual_cols
        else "CAST(0 AS BIGINT)"
    )
    reqs_5xx = (
        'CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT)'
        if "status" in actual_cols
        else "CAST(0 AS BIGINT)"
    )
    total_bytes = (
        'CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT)' if "resp_bytes" in actual_cols else "CAST(0 AS BIGINT)"
    )
    rtt_sum = 'CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE)' if "tcp_rtt" in actual_cols else "CAST(0.0 AS DOUBLE)"
    rtt_count = (
        'CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT)'
        if "tcp_rtt" in actual_cols
        else "CAST(0 AS BIGINT)"
    )
    edge_cnt = (
        'CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT)' if "edge" in actual_cols else "CAST(0 AS BIGINT)"
    )
    shield_cnt = (
        'CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT)' if "edge" in actual_cols else "CAST(0 AS BIGINT)"
    )
    ua_min_expr = 'CAST(MIN("ua") AS VARCHAR)' if "ua" in actual_cols else "CAST(NULL AS VARCHAR)"
    edge_sid_expr = 'CAST(MAX("edge_sid") AS VARCHAR)' if "edge_sid" in actual_cols else "CAST(NULL AS VARCHAR)"

    live_start = max(active_hour_dt, user_start)
    live_end = min(active_hour_dt + timedelta(hours=1), user_end)
    sql = f"""
        SELECT
            time_bucket(INTERVAL '1 hour', timestamp) AS bucket,
            CAST("ip" AS VARCHAR) AS ip,
            CAST({ja4_expr} AS VARCHAR) AS ja4,
            MIN(timestamp) AS first_ts,
            MAX(timestamp) AS last_ts,
            CAST(COUNT(*) AS BIGINT) AS req_count,
            {country_expr} AS country,
            {asn_expr} AS asn,
            {reqs_4xx} AS reqs_4xx,
            {reqs_5xx} AS reqs_5xx,
            {total_bytes} AS total_bytes,
            {rtt_sum} AS rtt_sum,
            {rtt_count} AS rtt_count,
            {edge_cnt} AS edge_count,
            {shield_cnt} AS shield_count,
            {ua_min_expr} AS ua_min,
            {edge_sid_expr} AS edge_sid_max
        FROM {table_name}
        WHERE timestamp >= TIMESTAMPTZ '{live_start.isoformat()}'
          AND timestamp <  TIMESTAMPTZ '{live_end.isoformat()}'
          AND "ip" IS NOT NULL
        GROUP BY 1, 2, 3
    """
    return sql, []


def _build_rollup_filter_sql(rollup_filters: FiltersDict | None) -> str:
    """Build a SQL WHERE clause fragment from the subset of filter pills
    that the sessions rollup can serve (country, asn).

    Values are inlined as SQL literals (with quote-escaping) rather than
    parameterised because the surrounding rollup query uses inlined
    file paths too — keeping the inline pattern uniform avoids a separate
    params list threading through the UNION ALL.
    """
    if not rollup_filters:
        return ""
    parts: list[str] = []
    for col, spec_raw in rollup_filters.items():
        if col not in ("country", "asn"):
            # Caller's eligibility gate is supposed to enforce this;
            # the check here is defense-in-depth.
            return ""
        # ``spec`` is either a FilterSpec pydantic model OR a plain dict —
        # the function accepts both shapes historically. Cast away here so
        # the hasattr-or-dict-get pattern doesn't trip the type checker.
        spec: Any = spec_raw
        values = spec.values if hasattr(spec, "values") else spec.get("values", [])
        mode = spec.mode if hasattr(spec, "mode") else spec.get("mode", "include")
        if not values:
            continue
        if col == "asn":
            # asn is INTEGER in the rollup; cast user-supplied values.
            int_literals: list[str] = []
            for v in values:
                try:
                    int_literals.append(str(int(v)))
                except (TypeError, ValueError):
                    continue
            if not int_literals:
                continue
            in_list = ", ".join(int_literals)
            op = "NOT IN" if mode == "exclude" else "IN"
            parts.append(f'"asn" {op} ({in_list})')
        else:  # country: VARCHAR
            country_literals = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
            op = "NOT IN" if mode == "exclude" else "IN"
            parts.append(f'"country" {op} ({country_literals})')
    if not parts:
        return ""
    return " AND " + " AND ".join(parts)


def _enrich_sessions_with_asn_labels(sessions: list[dict], src: dict) -> None:
    """Mutate session dicts in place to add an "asn_label" key.

    Batches the lookup so a 100-row page is one cache+resolve cycle. Cold
    asn_names cache entries trigger WHOIS resolution and amortise on later
    requests. Same path used by network/performance/dashboard responses.
    """
    asn_ints = sorted({int(sess["asn"]) for sess in sessions if sess.get("asn") is not None})
    if not asn_ints:
        return
    from backend.core import duckdb as _db

    asn_names = _db.get_asn_names(src["name"], asn_ints)
    for sess in sessions:
        asn_val = sess.get("asn")
        if asn_val is not None:
            sess["asn_label"] = _db.format_asn_label(int(asn_val), asn_names.get(int(asn_val), ""))


def _get_sessions_from_rollup(
    runner: QueryRunner,
    con: duckdb.DuckDBPyConnection,
    src: dict,
    table_name: str,
    actual_cols: set,
    start_dt: datetime,
    end_dt: datetime,
    page: int,
    limit: int,
    sort_by: str,
    sort_dir: str,
    flagged_only: bool,
    min_reqs_flag: int,
    min_4xx_pct_flag: float,
    has_ja4: bool,
    has_rtt: bool,
    has_edge: bool,
    has_edge_sid: bool,
    section_timings: list,
    rollup_filters: FiltersDict | None = None,
) -> dict | None:
    """Rollup-served version of get_sessions for the unfiltered case.

    Returns the same response shape as get_sessions, or ``None`` if
    the rollup can't serve this query (writer behind, no bundled
    root, etc.) — caller falls back to the raw path.

    Single-hour-or-less queries (``end - start <= 1h``) bypass the
    rollup because the raw scan is fast at that range and the rollup
    can't deliver ``unique_urls`` for the existing UI. Larger windows
    drop ``unique_urls`` (set to NULL) and report ``median_rtt_ms`` as
    the per-row mean (rtt_sum / rtt_count) — labelled the same field
    name for back-compat. Both caveats are baked into the contract;
    callers wanting exact median or unique_urls counts should use the
    raw path explicitly.
    """
    # Bail for windows ≤ 1h — raw is fast there and the rollup grain
    # is hourly so there's no win to chase.
    if (end_dt - start_dt) <= timedelta(hours=1):
        return None

    _t = time.perf_counter()
    paths_result = _collect_sessions_rollup_paths(src, start_dt, end_dt)
    section_timings.append({"section": "rollup_paths_collect", "time_ms": round((time.perf_counter() - _t) * 1000, 2)})
    if paths_result is None:
        # Writer behind for at least one in-window hour with data.
        return None
    rollup_paths, crosses_active = paths_result
    if not rollup_paths and not crosses_active:
        # No rollup files at all AND not in the active hour — nothing
        # to serve.
        return None

    # Build the UNION ALL of rollup + active-hour rows.
    union_parts: list[str] = []
    if rollup_paths:
        # The rollup writer stores `bucket` as TIMESTAMPTZ but DuckDB
        # may infer naive on re-read depending on the parquet metadata.
        # The downstream sessions logic only cares about first_ts/last_ts
        # ordering, so neither timezone interpretation breaks correctness.
        paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in rollup_paths)
        union_parts.append(
            f"SELECT bucket, ip, ja4, first_ts, last_ts, req_count, country, asn, "
            f"reqs_4xx, reqs_5xx, total_bytes, rtt_sum, rtt_count, edge_count, shield_count, "
            f"ua_min, edge_sid_max "
            f"FROM read_parquet([{paths_sql}])"
        )
    if crosses_active:
        active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_sql, _ = _build_active_hour_session_sql(table_name, actual_cols, active_hour_dt, start_dt, end_dt)
        union_parts.append(live_sql)
    union_sql = " UNION ALL ".join(union_parts)

    # Sort/filter compatible columns for the final SELECT. The rollup
    # has session_start/session_end via MIN(first_ts)/MAX(last_ts)
    # after the stitching aggregation below.
    sort_col_sql = {
        "session_start": "session_start",
        "session_end": "session_end",
        "req_count": "req_count",
        "edge_count": "edge_count",
        "shield_count": "shield_count",
        "unique_urls": "session_start",  # not tracked at rollup grain; sort by start as a safe fallback
        "median_rtt_ms": "median_rtt_ms",
        "total_bytes": "total_bytes",
    }.get(sort_by, "session_start")
    sort_dir_sql = "DESC" if sort_dir.upper() == "DESC" else "ASC"
    flagged_filter_sql = "WHERE flagged = true" if flagged_only else ""
    offset = calc_offset(page, limit)

    # Window-function stitching: walk the per-(ip, ja4) rollup rows in
    # bucket order, start a new session whenever the gap between this
    # row's first_ts and the previous row's last_ts exceeds 30 minutes.
    # Then GROUP BY the stitched session id.
    #
    # median_rtt_ms is APPROXIMATED as the row-weighted mean
    # (SUM(rtt_sum) / SUM(rtt_count)) since true median can't compose
    # from per-hour aggregates. The frontend column header keeps its
    # name for back-compat; the rollup path's value is within ~10% of
    # the raw-path value for typical distributions and dramatically
    # cheaper.
    #
    # unique_urls is NULL on the rollup path — the rollup grain is
    # hourly and we don't pre-aggregate URL sets. Frontend renders
    # NULL as a dash.
    # Push filter pills into the rollup CTE so we skip stitching rows
    # the user filtered out. Country / asn are MIN-aggregated in the
    # rollup row, so the filter semantics match raw-path for any IP
    # whose country/asn is stable across the hour (the common case).
    rollup_filter_sql = _build_rollup_filter_sql(rollup_filters)

    stitch_sql = f"""
        WITH src AS ({union_sql}),
        filtered AS (
            SELECT * FROM src WHERE 1=1 {rollup_filter_sql}
        ),
        ordered AS (
            SELECT *,
                   LAG(last_ts) OVER (PARTITION BY ip, ja4 ORDER BY first_ts) AS prev_last_ts
            FROM filtered
        ),
        marks AS (
            SELECT *,
                   CASE WHEN prev_last_ts IS NULL
                          OR (first_ts - prev_last_ts) > INTERVAL 30 MINUTES
                        THEN 1 ELSE 0 END AS is_new
            FROM ordered
        ),
        sids AS (
            SELECT *,
                   SUM(is_new) OVER (PARTITION BY ip, ja4 ORDER BY first_ts
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid
            FROM marks
        ),
        agg AS (
            SELECT ip,
                   ja4,
                   MIN(first_ts) AS session_start,
                   MAX(last_ts)  AS session_end,
                   SUM(req_count) AS req_count,
                   MIN(country)  AS country,
                   MIN(asn)      AS asn,
                   SUM(reqs_4xx) AS reqs_4xx,
                   SUM(reqs_5xx) AS reqs_5xx,
                   SUM(total_bytes) AS total_bytes,
                   SUM(rtt_sum)  AS rtt_sum,
                   SUM(rtt_count) AS rtt_count,
                   SUM(edge_count) AS edge_count,
                   SUM(shield_count) AS shield_count,
                   MIN(ua_min)   AS ua,
                   MAX(edge_sid_max) AS edge_sid
            FROM sids
            GROUP BY ip, ja4, sid
        ),
        flagged AS (
            SELECT *,
                   CASE WHEN rtt_count > 0 THEN rtt_sum / rtt_count / 1000.0 ELSE NULL END AS median_rtt_ms,
                   CAST(NULL AS BIGINT) AS unique_urls,
                   (req_count >= {min_reqs_flag}
                    OR (reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= {min_4xx_pct_flag}) AS flagged
            FROM agg
        )
        SELECT * FROM flagged
        {flagged_filter_sql}
        ORDER BY {sort_col_sql} {sort_dir_sql}
        LIMIT {limit} OFFSET {offset}
    """

    _t = time.perf_counter()
    try:
        result = runner.execute(stitch_sql, [])
    except duckdb.Error as e:
        # If the rollup query throws (schema drift, file corruption,
        # whatever), fall back to raw rather than 500-ing the user.
        import logging as _logging

        _logging.getLogger(__name__).warning("[sessions] rollup query failed, falling back: %s", e)
        section_timings.append(
            {"section": "sessions_rollup_failed", "time_ms": round((time.perf_counter() - _t) * 1000, 2)}
        )
        return None
    rows = result.fetchall()
    col_names = [desc[0] for desc in con.description]
    section_timings.append({"section": "sessions_rollup_query", "time_ms": round((time.perf_counter() - _t) * 1000, 2)})

    sessions: list[dict] = []
    for row in rows:
        d = dict(zip(col_names, row))
        for k in ("session_start", "session_end"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        # Drop fields the front-end doesn't use from the rollup output.
        d.pop("rtt_sum", None)
        d.pop("rtt_count", None)
        sessions.append(d)
    total = len(sessions)
    _enrich_sessions_with_asn_labels(sessions, src)

    return {
        "sessions": sessions,
        "total": total,
        "page": page,
        "limit": limit,
        "has_rtt": has_rtt,
        "has_ja4": has_ja4,
        "has_edge": has_edge,
        "has_edge_sid": has_edge_sid,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "section_timings": section_timings,
        # Hint for the frontend that median/unique_urls are reduced on
        # the rollup path. The current frontend ignores unknown keys.
        "_rollup_served": True,
        **runner.telemetry(),
    }


def get_sessions(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    page: int,
    limit: int,
    sort_by: str,
    sort_dir: str,
    flagged_only: bool,
    min_reqs_flag: int | None,
    min_4xx_pct_flag: float | None,
) -> dict:
    if min_reqs_flag is None:
        min_reqs_flag = 1000
    if min_4xx_pct_flag is None:
        min_4xx_pct_flag = 20.0

    # Per-phase timings surface in the response under _section_timings
    # so the perf harness can attribute wall time inside /api/sessions
    # without re-running ad-hoc instrumentation. Mirrors the pattern in
    # dashboard.py / bootstrap.py.
    timer = SectionTimer()
    section_timings = timer.entries

    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    _t = time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)
    if not actual_cols:
        return empty_schema_response(
            sessions=[],
            total=0,
            page=page,
            limit=limit,
            has_rtt=False,
            has_ja4=False,
            has_edge=False,
            has_edge_sid=False,
            **runner.telemetry(),
        )

    # Max 7-day range guard
    if start_time and end_time:
        try:
            from backend.utils.date_utils import parse_iso_utc

            s = parse_iso_utc(str(start_time))
            e = parse_iso_utc(str(end_time))
            if s and e and (e - s).days > 7:
                raise ValueError("Sessions view is limited to 7 days. Please narrow your date range.")
        except ValueError:
            raise

    _t = time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols))
    timer.mark("build_where_clause", _t)

    has_ja4 = "ja4" in actual_cols
    has_asn = "asn" in actual_cols
    has_country = "country" in actual_cols
    has_rtt = "tcp_rtt" in actual_cols
    has_status = "status" in actual_cols
    has_resp_bytes = "resp_bytes" in actual_cols
    has_ua = "ua" in actual_cols
    has_url = "url" in actual_cols
    has_edge = "edge" in actual_cols
    has_edge_sid = "edge_sid" in actual_cols

    # Sessions-rollup fast path: serve from per-hour sessions.parquet
    # rollups (built by backend.core.rollups.build_session_bundles)
    # instead of the multi-second raw window-function scan.
    #
    # Eligibility:
    #   - Window > 1 h (rollup grain is hourly; raw is fast at <= 1 h).
    #   - All filter pills are rollup-compatible. The rollup schema has
    #     ``country`` and ``asn`` as MIN-aggregated columns per
    #     (ip, ja4, hour). For a given IP the MIN is deterministic and
    #     matches the raw-path filter value in practice (an IP rarely
    #     changes country mid-hour). Other filter columns (url, ua,
    #     custom fields, status) aren't in the rollup → fall back to raw.
    #
    # Returns None if the rollup can't serve (writer behind, no bundled
    # root, active-hour only, etc.); we fall back to the raw path below.
    _ROLLUP_FILTERABLE = {"country", "asn"}
    if start_time and end_time and all(k in _ROLLUP_FILTERABLE for k in filters):
        try:
            from backend.utils.date_utils import parse_iso_utc

            _st = parse_iso_utc(str(start_time))
            _et = parse_iso_utc(str(end_time))
        except (ValueError, TypeError):
            _st = _et = None
        if _st and _et and _et > _st:
            _t = time.perf_counter()
            rollup_result = _get_sessions_from_rollup(
                runner=runner,
                con=con,
                src=src,
                table_name=table_name,
                actual_cols=actual_cols,
                start_dt=_st,
                end_dt=_et,
                page=page,
                limit=limit,
                sort_by=sort_by,
                sort_dir=sort_dir,
                flagged_only=flagged_only,
                min_reqs_flag=min_reqs_flag,
                min_4xx_pct_flag=min_4xx_pct_flag,
                has_ja4=has_ja4,
                has_rtt=has_rtt,
                has_edge=has_edge,
                has_edge_sid=has_edge_sid,
                section_timings=section_timings,
                rollup_filters=filters,
            )
            timer.mark("sessions_rollup_attempt", _t)
            if rollup_result is not None:
                return rollup_result

    group_cols = ["ip"]
    if has_ja4:
        group_cols.append("ja4")

    group_key = ", ".join(f'"{c}"' for c in group_cols)
    part_key = group_key

    extra_aggs = ""
    if has_edge_sid:
        # Representative cookie session id per (ip[, ja4]) session.
        # MAX() across rows in the same session ensures a stable value;
        # rows where the inbound request had no valid cookie store ''
        # (see backend/provision/session_scoring_orchestrator.py).
        extra_aggs += ', MAX("edge_sid") AS edge_sid'
    if has_edge:
        extra_aggs += ', SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS edge_count'
        extra_aggs += ', SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS shield_count'
    if has_asn:
        extra_aggs += ', MIN("asn") AS asn'
    if has_country:
        extra_aggs += ', MIN("country") AS country'
    if has_status:
        extra_aggs += (
            ', SUM(CASE WHEN "status" >= 400 AND "status" < 500 THEN 1 ELSE 0 END) AS reqs_4xx'
            ', SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS reqs_5xx'
        )
    if has_resp_bytes:
        extra_aggs += ', SUM("resp_bytes") AS total_bytes'
    if has_rtt:
        extra_aggs += ', MEDIAN("tcp_rtt") / 1000.0 AS median_rtt_ms'
    if has_ua:
        extra_aggs += ', MIN("ua") AS ua'
    if has_url:
        extra_aggs += ', COUNT(DISTINCT "url") AS unique_urls'

    flag_parts = [f"req_count >= {min_reqs_flag}"]
    if has_status:
        flag_parts.append(f"(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= {min_4xx_pct_flag}")
    flag_expr = " OR ".join(f"({p})" for p in flag_parts)

    flagged_filter = "WHERE flagged = true" if flagged_only else ""

    valid_sorts = {
        "session_start",
        "session_end",
        "req_count",
        "edge_count",
        "shield_count",
        "unique_urls",
        "median_rtt_ms",
        "total_bytes",
    }
    if sort_by not in valid_sorts:
        sort_by = "session_start"

    # Single CTE pipeline: filter → window functions → aggregation.
    # Replaces the item-19 three-stage TEMP TABLE approach now that
    # profiling identified sessions_raw materialization as the bottleneck
    # (~3000ms of ~3700ms total). DuckDB pipelines single-consumer CTEs
    # without intermediate materialization, saving the I/O overhead.
    cte_prefix = SQL.SESSIONS_CTE_PIPELINE.format(
        group_key=group_key,
        ua_proj=', "ua"' if has_ua else "",
        status_proj=', "status"' if has_status else "",
        resp_bytes_proj=', "resp_bytes"' if has_resp_bytes else "",
        rtt_proj=', "tcp_rtt"' if has_rtt else "",
        asn_proj=', "asn"' if has_asn else "",
        country_proj=', "country"' if has_country else "",
        url_proj=', "url"' if has_url else "",
        edge_proj=', "edge"' if has_edge else "",
        edge_sid_proj=', "edge_sid"' if has_edge_sid else "",
        table_name=table_name,
        where_clause=where_clause,
        part_key=part_key,
        extra_aggs=extra_aggs,
    )

    data_sql = SQL.SESSIONS_PAGE_SELECT.format(
        cte_prefix=cte_prefix,
        flag_expr=flag_expr,
        flagged_filter=flagged_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )
    _t = time.perf_counter()
    result = runner.execute_with_retry(data_sql, params)
    timer.mark("sessions_query", _t)
    if result is None:
        return empty_schema_response(
            sessions=[],
            total=0,
            page=page,
            limit=limit,
            has_rtt=has_rtt,
            has_ja4=has_ja4,
            has_edge=has_edge,
            has_edge_sid=has_edge_sid,
            **runner.telemetry(),
        )

    _t = time.perf_counter()
    rows = result.fetchall()
    timer.mark("fetchall", _t)
    col_names = [desc[0] for desc in con.description]

    sessions: list[dict] = []
    for row in rows:
        d = dict(zip(col_names, row))
        for k in ("session_start", "session_end"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        sessions.append(d)
    total = len(sessions)

    _enrich_sessions_with_asn_labels(sessions, src)

    if not rows and offset > 0:
        _t = time.perf_counter()
        count_sql = SQL.SESSIONS_COUNT_WRAPPER.format(
            cte_prefix=cte_prefix,
            flag_expr=flag_expr,
            flagged_filter=flagged_filter,
        )
        total = runner.execute(count_sql, params).fetchone()[0]
        timer.mark("count_query", _t)

    return {
        "sessions": sessions,
        "total": total,
        "page": page,
        "limit": limit,
        "has_rtt": has_rtt,
        "has_ja4": has_ja4,
        "has_edge": has_edge,
        "has_edge_sid": has_edge_sid,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "section_timings": section_timings,
        **runner.telemetry(),
    }


def get_session_detail(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    ip: str,
    session_start: str,
    session_end: str,
    ua: str | None = None,
    ja4: str | None = None,
) -> dict:
    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])

    conditions = [
        '"ip" = ?',
        "timestamp >= CAST(? AS TIMESTAMPTZ)",
        "timestamp <= CAST(? AS TIMESTAMPTZ)",
    ]
    p: list[Any] = [ip, session_start, session_end]

    if ua is not None:
        conditions.append('"ua" IS NOT DISTINCT FROM ?')
        p.append(ua)
    if ja4 is not None:
        conditions.append('"ja4" IS NOT DISTINCT FROM ?')
        p.append(ja4)

    where = " AND ".join(conditions)
    q = f"SELECT * FROM {table_name} WHERE {where} ORDER BY timestamp ASC LIMIT 500"
    rows = runner.execute(q, p).fetchall()
    col_names = [desc[0] for desc in con.description]
    records = [dict(zip(col_names, r)) for r in rows]
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    return {"data": records, "columns": col_names, **runner.telemetry()}
