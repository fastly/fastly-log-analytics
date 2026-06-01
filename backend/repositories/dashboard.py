"""Dashboard repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    CANONICAL_METRICS,
    QueryRunner,
    _get_schema,
    _safe_table,
    get_source_extent,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    time_bucket_select,
)
from backend.repositories.utils.filters import build_where_clause, resolve_col
from backend.repositories.utils.pagination import calc_offset

# ── In-memory caches ──────────────────────────────────────────────────────────

_dashboard_cache: dict[str, tuple[float, Any]] = {}
DASHBOARD_CACHE_TTL = 30  # seconds


# ── aggregates ────────────────────────────────────────────────────────────────

from backend.core.log_fields import LOG_FIELD_CATALOG

FIELDS = [f["id"] for f in LOG_FIELD_CATALOG if f["id"] != "_source_file"] + ["waf_sig_ind"]


def _add_bot_columns(actual_cols: set[str], columns: list[str], select_cols: list[str]) -> tuple[bool, bool]:
    """Ensure UA + IP (Arcjet) or waf_req_id (NGWAF) columns are in select_cols
    when the caller requested the virtual `_bot_name` / `_ngwaf_bot_name` fields.

    Mutates `select_cols` in place. Returns (wants_bot, wants_ngwaf_bot).
    """
    wants_bot = "_bot_name" in columns
    wants_ngwaf_bot = "_ngwaf_bot_name" in columns
    if wants_bot:
        if "ua" in actual_cols and '"ua"' not in select_cols:
            select_cols.append('"ua"')
        if "ip" in actual_cols and '"ip"' not in select_cols:
            select_cols.append('"ip"')
    if wants_ngwaf_bot and "waf_req_id" in actual_cols and '"waf_req_id"' not in select_cols:
        select_cols.append('"waf_req_id"')
    return wants_bot, wants_ngwaf_bot


def get_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str,
    chart_metric: str,
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)

    lf_config = src.get("log_fields") or {}
    _custom_field_names = [
        cf["name"]
        for cf in lf_config.get("custom_fields", [])
        if cf.get("enabled", True) and cf.get("show_in_dashboard", True)
    ]
    fields = FIELDS + _custom_field_names

    _key_payload = json.dumps(
        {
            "s": start_time,
            "e": end_time,
            "f": {k: (v.mode, sorted(str(x) for x in v.values)) for k, v in sorted(filters.items())},
            "ci": chart_interval,
            "cm": chart_metric,
        },
        separators=(",", ":"),
    )
    cache_key = hashlib.sha256(f"{_key_payload}:{source_name}".encode()).hexdigest()
    now = time.time()
    if DASHBOARD_CACHE_TTL > 0 and cache_key in _dashboard_cache:
        cached_at, cached_res = _dashboard_cache[cache_key]
        if now - cached_at < DASHBOARD_CACHE_TTL:
            cached_res = cached_res.copy()
            cached_res["_is_cached"] = True
            return cached_res

    runner = QueryRunner(con, src)
    interval = "1 minute"

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        empty = {f: {"top": [], "total": 0} for f in fields}
        return {
            "data": empty,
            "time_series": [],
            "map_data": [],
            "where_clause": "1=1",
            "interval": interval,
            "metric": "requests",
            "total_rows": 0,
            "total_rows_total": 0,
            **runner.telemetry(),
        }

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    # Iceberg handles partition pruning natively via hidden partitioning — no manual file enumeration needed.

    # Build temp table with only needed columns
    needed_cols: set[str] = set()
    if "timestamp" in actual_cols:
        needed_cols.add('"timestamp"')
    for field in fields:
        if field == "waf_sig_ind":
            continue
        if field in actual_cols:
            needed_cols.add(f'"{field}"')

    for mc in [
        "resp_bytes",
        "elapsed",
        "status",
        "cache",
        "status",
        "resp_state",
        "req_header_bytes",
        "req_bytes",
        "ttfb",
        "server_region",
        "tls_ciphers_sha",
        "is_ipv6",
        "conn_requests",
    ]:
        if mc in actual_cols:
            needed_cols.add(f'"{mc}"')

    cols_str = ", ".join(needed_cols) if needed_cols else "*"
    # Use TEMP TABLE instead of TEMP VIEW to materialize the filtered results in memory.
    # This prevents DuckDB from re-scanning the underlying files for every branch of the UNION ALL.
    temp_table = f"t_{uuid.uuid4().hex}"
    sql = f"CREATE TEMP TABLE {temp_table} AS SELECT {cols_str} FROM {table_name} WHERE {where_clause}"
    if not runner.create_temp_table(sql, params):
        empty = {f: {"top": [], "total": 0} for f in fields}
        return {
            "data": empty,
            "time_series": [],
            "map_data": [],
            "where_clause": "1=1",
            "interval": interval,
            "metric": "requests",
            "total_rows": 0,
            "total_rows_total": 0,
            **runner.telemetry(),
        }

    # All subsequent queries use the temp table
    table_name = temp_table
    where_clause = "1=1"
    params = []

    results: dict[str, Any] = {f: {"top": [], "total": 0} for f in fields}

    try:
        # Optimization: Combine count(*) and field counts into a single scan
        count_cols: list[str] = [CANONICAL_METRICS["requests"]]
        valid_fields: list[str] = []
        for field in fields:
            if field == "waf_sig_ind":
                continue
            if field in actual_cols:
                count_cols.append(f"count({resolve_col(field, actual_cols)})")
                valid_fields.append(field)
        field_totals: dict[str, int] = {}
        total_rows = 0
        earliest_log_at = None
        latest_log_at = None
        if count_cols:
            count_res = runner.execute(f"SELECT {', '.join(count_cols)} FROM {table_name}").fetchone()
            total_rows = count_res[0]
            for i, field in enumerate(valid_fields):
                field_totals[field] = count_res[i + 1]

        orig_table_name = _safe_table(source_name)
        total_rows_total, earliest_log_at, latest_log_at = get_source_extent(runner, src, orig_table_name)

        schema_types = {col["name"]: col["type"] for col in _get_schema(con, src)}

        batch_fields = [f for f in fields if f != "waf_sig_ind" and f in field_totals]
        all_top_res, field_order = runner.execute_top_n_batch(
            batch_fields, table_name, actual_cols, schema_types, limit=10
        )

        if all_top_res:
            # Group results back by field
            for field in field_order:
                results[field] = {"top": [], "total": field_totals.get(field, 0)}

            # Prepare to resolve ASN names if 'asn' is present
            asn_list = []
            for f_name, f_val, f_count in all_top_res:
                if f_name == "asn" and f_val is not None and str(f_val).isdigit():
                    asn_list.append(int(f_val))

            asn_names = {}
            if asn_list:
                from backend.core import duckdb as _db

                asn_names = _db.get_asn_names(src["name"], asn_list)

            for f_name, f_val, f_count in all_top_res:
                entry = {"value": f_val, "count": f_count}
                if f_name == "asn" and f_val is not None and str(f_val).isdigit():
                    from backend.core import duckdb as _db

                    asn_int = int(f_val)
                    entry["label"] = _db.format_asn_label(asn_int, asn_names.get(asn_int, ""))

                results[f_name]["top"].append(entry)

        # Special handling for individual WAF signals (remains separate due to unnest overhead)
        if "waf_sig_ind" in FIELDS:
            if "waf_sig" in actual_cols:
                q = f"""
                    WITH split_data AS (
                        SELECT trim(signal) AS signal
                        FROM (
                            SELECT unnest(string_split("waf_sig", ',')) AS signal
                            FROM {table_name}
                            WHERE "waf_sig" IS NOT NULL AND "waf_sig" != ''
                        )
                        WHERE trim(signal) != ''
                    ),
                    total_count AS (SELECT {CANONICAL_METRICS["requests"]} AS tc FROM split_data),
                    top_values AS (
                        SELECT signal AS value, {CANONICAL_METRICS["requests"]} AS c
                        FROM split_data GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                    )
                    SELECT tv.value, tv.c, tc.tc FROM top_values tv CROSS JOIN total_count tc
                """
                res = runner.execute(q).fetchall()
                if res:
                    results["waf_sig_ind"] = {"top": [{"value": r[0], "count": r[1]} for r in res], "total": res[0][2]}
                else:
                    results["waf_sig_ind"] = {"top": [], "total": 0}
            else:
                results["waf_sig_ind"] = {"top": [], "total": 0}

        # Special handling for conn_requests (bucketed histogram)
        if "conn_requests" in actual_cols:
            q = f"""
                SELECT
                    CASE
                        WHEN "conn_requests" = 1 THEN '1'
                        WHEN "conn_requests" BETWEEN 2 AND 5 THEN '2–5'
                        WHEN "conn_requests" BETWEEN 6 AND 20 THEN '6–20'
                        ELSE '21+'
                    END AS bucket,
                    {CANONICAL_METRICS["requests"]} AS c
                FROM {table_name}
                WHERE "conn_requests" IS NOT NULL AND "conn_requests" > 0
                GROUP BY 1
                ORDER BY MIN("conn_requests")
            """
            res = runner.execute(q).fetchall()
            total_conn = sum(r[1] for r in res)
            results["conn_requests"] = {
                "top": [{"value": r[0], "count": r[1]} for r in res],
                "total": total_conn,
            }
        else:
            results["conn_requests"] = {"top": [], "total": 0}

        # Time series
        time_series: list[dict] = []
        chart_metric_out = "requests"
        if "timestamp" in actual_cols:
            interval = safe_interval(chart_interval, default=interval)

            sql_cache = resolve_col("cache", actual_cols)
            sql_elapsed = resolve_col("elapsed", actual_cols)

            if chart_metric == "5xx" and "status" in actual_cols:
                chart_metric_out = "5xx"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["5xx_rate"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "4xx" and "status" in actual_cols:
                chart_metric_out = "4xx"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["4xx_rate"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "hit_rate" and ("cache" in actual_cols or "resp_state" in actual_cols):
                chart_metric_out = "hit_rate"
                # Fallback to resp_state if cache is missing
                cache_col = '"cache"' if "cache" in actual_cols else '"resp_state"'
                hit_rate_expr = CANONICAL_METRICS["hit_rate"].format(cache_col=cache_col)
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {hit_rate_expr} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric.endswith("_latency") and ("elapsed" in actual_cols or "elapsed_us" in actual_cols):
                chart_metric_out = chart_metric
                percentile = 0.95
                if chart_metric.startswith("p50"):
                    percentile = 0.50
                elif chart_metric.startswith("p99"):
                    percentile = 0.99
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {percentile_ms_expr(sql_elapsed, percentile)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL AND {sql_elapsed} IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "throughput" and "resp_bytes" in actual_cols and "elapsed" in actual_cols:
                chart_metric_out = "throughput"
                sql_resp_bytes = resolve_col("resp_bytes", actual_cols)
                # Note: elapsed and elapsed_us both map to the same field in DuckDB (µs)
                sql_elapsed_val = resolve_col("elapsed", actual_cols)
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["throughput"].format(cache_col=sql_cache, elapsed_col=sql_elapsed_val, resp_bytes_col=sql_resp_bytes)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "req_size" and any(c in actual_cols for c in ["req_header_bytes", "req_bytes"]):
                chart_metric_out = "req_size"
                header_col = '"req_header_bytes"' if "req_header_bytes" in actual_cols else "0"
                body_col = resolve_col("req_bytes", actual_cols) if "req_bytes" in actual_cols else "0"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["req_size"].format(header_bytes_col=header_col, req_bytes_col=body_col)} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            elif chart_metric == "ttfb" and "ttfb" in actual_cols:
                chart_metric_out = "ttfb"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["ttfb_ms"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """
            else:
                chart_metric_out = "requests"
                ts_q = f"""
                    SELECT {time_bucket_select(interval)},
                           {CANONICAL_METRICS["requests"]} AS value
                    FROM {table_name}
                    WHERE timestamp IS NOT NULL
                    GROUP BY 1 ORDER BY 1
                """

            ts_res = runner.execute(ts_q, []).fetchall()
            for r in ts_res:
                if r[0] is None:
                    continue
                pt: dict[str, Any] = {"time": safe_iso(r[0]), "value": float(r[1]) if r[1] is not None else 0.0}
                if len(r) >= 3 and r[2] is not None:
                    pt["category"] = str(r[2])
                time_series.append(pt)

        # Map data
        map_data: list[dict] = []
        if "country" in actual_cols:
            map_q = f"""
                SELECT "country" AS country, {CANONICAL_METRICS["requests"]} AS count
                FROM {table_name}
                WHERE "country" IS NOT NULL
                GROUP BY 1
            """
            map_data = [{"country": r[0], "count": r[1]} for r in runner.execute(map_q, []).fetchall()]

        payload: dict[str, Any] = {
            "data": results,
            "time_series": time_series,
            "map_data": map_data,
            "where_clause": where_clause,
            "interval": interval,
            "metric": chart_metric_out,
            "total_rows": total_rows,
            "total_rows_total": total_rows_total,
            "earliest_log_at": earliest_log_at,
            "latest_log_at": latest_log_at,
            **runner.telemetry(),
        }
        if DASHBOARD_CACHE_TTL > 0:
            _dashboard_cache[cache_key] = (now, payload)
        return payload

    finally:
        try:
            con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except Exception:
            pass


# ── raw ───────────────────────────────────────────────────────────────────────


def get_raw(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    page: int,
    limit: int,
    sort_col: str | None,
    sort_dir: str,
    columns: list[str],
) -> dict:
    runner = QueryRunner(con, src)

    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return {
            "columns": [],
            "data": [],
            "total_rows": 0,
            "total_rows_total": 0,
            "page": page,
            "limit": limit,
            **runner.telemetry(),
        }

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols)

    order_clause = ""
    if sort_col and sort_col in actual_cols:
        order_clause = f'ORDER BY "{sort_col}" {sort_dir}'
    elif "timestamp" in actual_cols:
        order_clause = f"ORDER BY timestamp {sort_dir}"

    if columns:
        select_cols = [f'"{c}"' for c in columns if c in actual_cols]
        if sort_col and sort_col in actual_cols and f'"{sort_col}"' not in select_cols:
            select_cols.append(f'"{sort_col}"')
        if "timestamp" in actual_cols and '"timestamp"' not in select_cols:
            select_cols.append('"timestamp"')

        wants_bot, wants_ngwaf_bot = _add_bot_columns(actual_cols, columns, select_cols)

        select_clause = ", ".join(select_cols)
    else:
        wants_bot = True  # By default, calculate bot data if possible
        wants_ngwaf_bot = True
        select_clause = "*"

    q = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause} {order_clause} LIMIT {limit} OFFSET {offset}"

    result = runner.execute_with_retry(q, params)
    if result is None:
        return {
            "columns": columns or [],
            "data": [],
            "total_rows": 0,
            "total_rows_total": 0,
            "page": page,
            "limit": limit,
            **runner.telemetry(),
        }
    df = result.fetchdf()

    if wants_bot or wants_ngwaf_bot:
        from backend.utils.bot_sources import enrich_bot_metadata

        enrich_bot_metadata(df)

    total_rows = len(df)
    total_rows_total = 0
    earliest_log_at = None
    latest_log_at = None

    col_names = list(df.columns)

    if wants_bot and "_bot_name" not in col_names and "_bot_name" in columns:
        df["_bot_name"] = "null"
        col_names.append("_bot_name")

    if wants_ngwaf_bot and "_ngwaf_bot_name" not in col_names and "_ngwaf_bot_name" in (columns or []):
        df["_ngwaf_bot_name"] = None
        col_names.append("_ngwaf_bot_name")

    records = json.loads(df.to_json(orient="records", date_format="iso"))

    if columns:
        filtered_records = []
        for r in records:
            filtered_records.append({c: r.get(c, "null") for c in columns})
        records = filtered_records
        col_names = columns

    try:
        from backend import config as svcconfig

        cached_status = svcconfig.get_status(src["name"])
        if cached_status:
            total_rows_total = cached_status.get("local_rows", 0)
            earliest_log_at = cached_status.get("earliest_log_at")
            latest_log_at = cached_status.get("latest_log_at")
        else:
            agg_res = runner.execute(
                f"SELECT {CANONICAL_METRICS['requests']}, min(timestamp), max(timestamp) FROM {table_name}"
            ).fetchone()
            if agg_res:
                total_rows_total = agg_res[0]
                earliest_log_at = safe_iso(agg_res[1])
                latest_log_at = safe_iso(agg_res[2])
    except Exception:
        try:
            total_rows_total = runner.execute(f"SELECT {CANONICAL_METRICS['requests']} FROM {table_name}").fetchone()[0]
        except Exception:
            pass

    return {
        "columns": col_names,
        "data": records,
        "total_rows": total_rows,
        "total_rows_total": total_rows_total,
        "page": page,
        "limit": limit,
        "earliest_log_at": earliest_log_at,
        "latest_log_at": latest_log_at,
        **runner.telemetry(),
    }


def get_raw_df(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int,
    columns: list[str],
):
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        import pandas as pd

        return pd.DataFrame()

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols)

    if columns:
        select_cols = [f'"{c}"' for c in columns if c in actual_cols]
        wants_bot, wants_ngwaf_bot = _add_bot_columns(actual_cols, columns, select_cols)
        select_clause = ", ".join(select_cols)
    else:
        wants_bot = True  # By default, calculate bot data if possible
        wants_ngwaf_bot = True
        select_clause = "*"

    q = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause} ORDER BY timestamp DESC LIMIT {limit}"
    df = runner.execute(q, params).fetchdf()

    if wants_bot or wants_ngwaf_bot:
        from backend.utils.bot_sources import enrich_bot_metadata

        enrich_bot_metadata(df)

    if columns:
        # Keep only requested columns in order
        existing_cols = [c for c in columns if c in df.columns]
        if "_bot_name" in columns and "_bot_name" not in existing_cols:
            df["_bot_name"] = "null"
            existing_cols.append("_bot_name")
        if "_ngwaf_bot_name" in columns and "_ngwaf_bot_name" not in existing_cols:
            df["_ngwaf_bot_name"] = None
            existing_cols.append("_ngwaf_bot_name")
        df = df[existing_cols]

    return df


# ── field_values ──────────────────────────────────────────────────────────────


def get_field_values(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    field: str,
    search: str,
    limit: int,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)

    clean_field = "".join(ch for ch in field if ch.isalnum() or ch == "_")
    if not clean_field:
        raise ValueError("Invalid field name")

    table_name = _safe_table(src["name"])

    # Try top-values cache first (no-search path only)
    if not search:
        try:
            from backend.core.duckdb import _cache_dir

            cache_path = os.path.join(_cache_dir(src), "top_values.json")
            if os.path.exists(cache_path):
                with open(cache_path) as f:
                    top_values = json.load(f)
                if clean_field in top_values:
                    vals = top_values[clean_field][:limit]
                    if clean_field == "asn":
                        from backend.core.duckdb import enrich_asn_labels

                        enrich_asn_labels(vals, src["name"])
                    return {"values": vals, "field": field, **runner.telemetry()}
        except Exception:
            pass

    # Verify table exists
    try:
        exists = (
            runner.execute(
                f"SELECT {CANONICAL_METRICS['requests']} FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()[0]
            > 0
        )
        if not exists:
            return {"values": [], "field": field, **runner.telemetry()}
    except Exception:
        return {"values": [], "field": field, **runner.telemetry()}

    actual_cols = runner.get_schema_cols()

    # Exclude the field's own filter so picker shows all available values
    filters_excl = {k: v for k, v in filters.items() if k != field}
    params, where_clause = build_where_clause(start_time, end_time, filters_excl, actual_cols)

    if field == "_bot_name":
        # SPECIAL HANDLING FOR VIRTUAL BOT NAME FIELD
        if "ua" not in actual_cols:
            return {"values": [], "field": field, **runner.telemetry()}

        try:
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern
        except ImportError:
            return {"values": [], "field": field, **runner.telemetry()}

        # Optimization: use regex pre-filter from known bot literals
        pattern = get_bot_regex_pattern(200)
        ua_filter = ""
        if pattern:
            pattern_sql = pattern.replace("'", "''")
            ua_filter = f"AND regexp_matches(ua, '{pattern_sql}')"

        # We query unique UAs to keep local bot-matching overhead manageable
        q = f"""
            SELECT ua, {CANONICAL_METRICS["requests"]} AS cnt
            FROM {table_name}
            WHERE {where_clause} AND ua IS NOT NULL {ua_filter}
            GROUP BY ua
            ORDER BY cnt DESC
            LIMIT 5000
        """
        rows = runner.execute(q, params).fetchall()

        match_ua = build_matcher()
        bot_counts: dict[str, dict] = {}
        search_lower = search.lower() if search else ""

        for ua_val, cnt in rows:
            for entry in match_ua(str(ua_val) if ua_val else ""):
                bot_id = entry.get("id", "unknown")
                bot_name = entry.get("name", bot_id.replace("-", " ").title())

                # Apply search filter if provided (matching on ID or Name)
                if search_lower and search_lower not in bot_id.lower() and search_lower not in bot_name.lower():
                    continue

                if bot_id not in bot_counts:
                    bot_counts[bot_id] = {
                        "value": bot_id,
                        "label": bot_name,
                        "count": 0,
                    }
                bot_counts[bot_id]["count"] += cnt

        sorted_vals = sorted(bot_counts.values(), key=lambda x: x["count"], reverse=True)
        return {"values": sorted_vals[:limit], "field": field, **runner.telemetry()}

    is_signals_individual = field == "waf_sig_ind"
    backing_col = "waf_sig" if is_signals_individual else clean_field
    if backing_col not in actual_cols:
        raise LookupError(f"Field '{field}' not found")

    search_params = list(params)

    if is_signals_individual or clean_field == "waf_sig":
        search_cond = ""
        if search:
            search_cond = "AND trim(signal) ILIKE ?"
            search_params.append(f"%{search}%")
        q = f"""
            SELECT trim(signal) AS value, {CANONICAL_METRICS["requests"]} AS count
            FROM (
                SELECT unnest(string_split("{backing_col}", ',')) AS signal
                FROM {table_name}
                WHERE {where_clause} AND "{backing_col}" IS NOT NULL AND "{backing_col}" != ''
            )
            WHERE trim(signal) != '' {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """
    else:
        search_cond = ""
        if search:
            if clean_field == "country":
                from backend.utils.countries import COUNTRY_MAP

                codes = [c for c, name in COUNTRY_MAP.items() if search.lower() in name.lower()]
                if codes:
                    placeholders = ",".join(["?"] * len(codes))
                    search_cond = (
                        f'AND (CAST("{clean_field}" AS VARCHAR) ILIKE ? '
                        f'OR CAST("{clean_field}" AS VARCHAR) IN ({placeholders}))'
                    )
                    search_params.append(f"%{search}%")
                    search_params.extend(codes)
                else:
                    search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                    search_params.append(f"%{search}%")
            elif clean_field == "asn":
                # ASN-name search: pre-fetch matching ASN ints from per-service
                # SQLite metadata, then inline them as a parameterised IN list.
                # Avoids ATTACH overhead / SQLite-extension dependency in DuckDB.
                from backend.core import metadata_db

                try:
                    matching_asns = metadata_db.asn_ints_for_search(src["name"], f"%{search}%")
                except Exception:
                    matching_asns = []
                if matching_asns:
                    in_placeholders = ",".join(["?"] * len(matching_asns))
                    search_cond = (
                        f'AND (CAST("{clean_field}" AS VARCHAR) ILIKE ? '
                        f'OR CAST("{clean_field}" AS VARCHAR) IN ({in_placeholders}))'
                    )
                    search_params.append(f"%{search}%")
                    search_params.extend([str(a) for a in matching_asns])
                else:
                    search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                    search_params.append(f"%{search}%")
            else:
                search_cond = f'AND CAST("{clean_field}" AS VARCHAR) ILIKE ?'
                search_params.append(f"%{search}%")

        q = f"""
            SELECT "{clean_field}" AS value, {CANONICAL_METRICS["requests"]} AS count
            FROM {table_name}
            WHERE {where_clause} {search_cond}
            GROUP BY 1 ORDER BY 2 DESC LIMIT {limit}
        """

    result = runner.execute_with_retry(q, search_params)
    if result is None:
        return {"values": [], "field": field, **runner.telemetry()}
    res = result.fetchall()

    vals = [{"value": r[0], "count": r[1]} for r in res]
    if clean_field == "asn" and vals:
        from backend.core.duckdb import enrich_asn_labels

        enrich_asn_labels(vals, src["name"])

    return {"values": vals, "field": field, **runner.telemetry()}
