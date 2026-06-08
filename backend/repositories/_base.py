"""Shared helpers used by every repository module.

Import from here instead of redefining in each file:

    from backend.repositories._base import _safe_table, _get_schema, safe_iso, QueryRunner
"""

from __future__ import annotations

import contextlib
import re
import time
from typing import Any

import duckdb

# Pre-compile once; called per ``runner.execute`` invocation.
_PARQUET_LIST_RE = re.compile(r"read_parquet\(\[\s*('[^']+'\s*(?:,\s*'[^']+'\s*)*)\]")


def _compact_sql_for_debug(sql: str) -> str:
    """Replace explicit ``read_parquet([...long file list...])`` literals
    with ``read_parquet([N files])`` for transport in the debug-panel
    payload.

    The dashboard's per-request SQL embeds hundreds of buffer/rollup
    parquet paths in a single ``read_parquet`` call. Shipping those
    verbatim made ``_debug_queries`` ~220 KB of the response (60% of
    total) — pure network + JSON-parse cost on every dashboard refresh
    when the operator has ``DEBUG_RESPONSES=true`` set. The path list
    isn't useful to a human reading the debug panel; the count is.

    Compacting cuts the field to ~tens of bytes per query without
    losing the SQL shape an operator cares about for tuning.
    """

    def _replace(m: re.Match) -> str:
        # Count items by quote pairs — cheap and exact.
        count = m.group(1).count("'") // 2
        return f"read_parquet([{count} files]"

    return _PARQUET_LIST_RE.sub(_replace, sql)


@contextlib.contextmanager
def _attach_sqlite(con: duckdb.DuckDBPyConnection, sqlite_path: str, alias: str):
    """Context manager that ATTACHes a SQLite file to a DuckDB connection read-only.

    Yields True if the ATTACH succeeded, False otherwise (file missing, SQLite
    extension unavailable, etc.). DETACHes on exit. Callers should treat a False
    yield as "the bridge isn't available — fall back to a Python-side query".
    """
    import os

    attached = False
    if sqlite_path and os.path.exists(sqlite_path):
        try:
            escaped = sqlite_path.replace("'", "''")
            con.execute(f"ATTACH '{escaped}' AS {alias} (TYPE SQLITE, READ_ONLY)")
            attached = True
        except Exception:
            pass
    try:
        yield attached
    finally:
        if attached:
            try:
                con.execute(f"DETACH {alias}")
            except Exception:
                pass


@contextlib.contextmanager
def attach_ngwaf_cache(con: duckdb.DuckDBPyConnection, schema_cols: list[str], alias: str = "ngwaf_cache"):
    """Attach the NGWAF bot cache SQLite database when the schema warrants it."""
    if "waf_req_id" not in schema_cols:
        yield False
        return
    from backend import config as svcconfig

    with _attach_sqlite(con, svcconfig.ngwaf_db_path(), alias) as attached:
        yield attached


@contextlib.contextmanager
def attach_metadata_db(con: duckdb.DuckDBPyConnection, service_id: str, alias: str = "meta"):
    """Attach the per-service metadata SQLite file (alerts, views, asn_names, …).

    Use ``meta.<table_name>`` to reference rows from inside a DuckDB query
    (e.g. JOINing the log table against `meta.asn_names`). Yields False if the
    file doesn't exist yet — callers should fall back gracefully.
    """
    from backend.core import metadata_db

    with _attach_sqlite(con, metadata_db.db_path(service_id), alias) as attached:
        yield attached


def _safe_table(name: str) -> str:
    """Convert a source name to a valid SQL table identifier."""
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(name)


def _get_schema(con: duckdb.DuckDBPyConnection, src: dict) -> list[dict]:
    """Return the schema for the given source's log table."""
    from backend.core.duckdb import get_schema

    return get_schema(con, src)


from backend.utils.date_utils import safe_iso  # noqa: E402, F401 — re-export


def _is_stale_view_error(e: Exception) -> bool:
    """Return True when the error indicates an Iceberg view that references a deleted buffer file."""
    msg = str(e)
    return (
        "No files found" in msg
        or "Catalog Error: Table with name" in msg
        or "does not exist" in msg
        or "No such file or directory" in msg
    )


def optional_col(col: str, actual_cols, default: str = "NULL") -> str:
    """Return a quoted column reference if the column exists, else a SQL default expression."""
    return f'"{col}"' if col in actual_cols else default


VALID_CHART_INTERVALS: frozenset[str] = frozenset({"1 second", "1 minute", "1 hour", "1 day"})


def safe_interval(requested: str, default: str = "1 minute") -> str:
    """Return requested if it is a known-safe interval string, else default."""
    return requested if requested in VALID_CHART_INTERVALS else default


CANONICAL_METRICS = {
    "hit_rate": "ROUND(COUNT(*) FILTER (WHERE {cache_col} IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "requests": "COUNT(*)",
    "avg_ttfb": "ROUND(AVG(ttfb) * 1000.0, 2)",
    "p95_ttfb": "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0, 2)",
    "5xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "4xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 400 AND status < 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "avg_resp_bytes": "ROUND(AVG(resp_bytes), 2)",
    "total_resp_bytes": "SUM(resp_bytes)",
    "throughput": "ROUND(COALESCE(MEDIAN(CASE WHEN ({cache_col} ILIKE 'HIT%%') AND {elapsed_col} > 0 THEN {resp_bytes_col} * 1e6 / NULLIF(CAST({elapsed_col} AS DOUBLE), 0) ELSE NULL END), 0), 2)",
    "req_size": "ROUND(COALESCE(MEDIAN(CAST({header_bytes_col} AS DOUBLE) + CAST({req_bytes_col} AS DOUBLE)), 0), 2)",
    "ttfb_ms": "ROUND(COALESCE(MEDIAN(CASE WHEN ttfb IS NOT NULL AND ttfb > 0 THEN ttfb * 1000.0 ELSE NULL END), 0), 2)",
}


# ── Canonical SQL expression builders ─────────────────────────────────────────


def time_bucket_select(interval: str, ts_col: str = "timestamp") -> str:
    """Return 'time_bucket(INTERVAL <interval>, <ts_col>) AS bucket'.

    Validates the interval via safe_interval to prevent injection.
    """
    safe_iv = safe_interval(interval)
    return f"time_bucket(INTERVAL '{safe_iv}', {ts_col}) AS bucket"


def percentile_ms_expr(col: str, p: float = 0.95, filter_expr: str = "") -> str:
    """Return PERCENTILE_CONT expression that converts microseconds → milliseconds."""
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    return f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}){filter_clause} / 1000.0"


def error_rate_expr(status_col: str = "status", threshold: int = 500, filter_expr: str = "") -> str:
    """Return percentage of rows where status_col >= threshold."""
    filter_clause = f" FILTER ({filter_expr})" if filter_expr else ""
    return f"ROUND(SUM(CASE WHEN {status_col} >= {threshold} THEN 1 ELSE 0 END){filter_clause} * 100.0 / NULLIF(count(*){filter_clause}, 0), 2)"


def origin_latency_us_expr(actual_cols: set[str] | list[str]) -> str:
    """Unified expression resolving ottfb (us) vs ttfb (s) vs nothing."""
    if "ottfb" in actual_cols and "ttfb" in actual_cols:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    elif "ottfb" in actual_cols:
        return '"ottfb"'
    elif "ttfb" in actual_cols:
        return '"ttfb" * 1000000.0'
    else:
        return "NULL"


def empty_schema_response(**extra_fields) -> dict:
    """Return a safe empty response dict when the log table has no schema yet.

    Callers merge in response-specific empty collections via extra_fields.
    Example: empty_schema_response(rows=[], series=[])
    """
    return {"has_data": False, "total": 0, **extra_fields}


def get_source_extent(
    runner: QueryRunner,
    src: dict,
    orig_table_name: str,
) -> tuple[int, str | None, str | None]:
    """Return (total_rows, earliest_log_at, latest_log_at) for a source.

    Checks the svcconfig status cache first; falls back to a live COUNT query.
    """
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
        if cached:
            return (
                cached.get("local_rows", 0),
                cached.get("earliest_log_at"),
                cached.get("latest_log_at"),
            )
        row = runner.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {orig_table_name}").fetchone()
        if row:
            return row[0], safe_iso(row[1]), safe_iso(row[2])
    except Exception:
        try:
            count = runner.execute(f"SELECT count(*) FROM {orig_table_name}").fetchone()[0]
            return count, None, None
        except Exception:
            pass
    return 0, None, None


class QueryRunner:
    """
    Centralises query execution, debug tracking, schema fallback, and stale-view
    recovery logic. Reduces boilerplate in repository functions.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, src: dict):
        self.con = con
        self.src = src
        self.debug_queries: list[dict] = []
        self.actual_cols: set[str] = set()

        from backend.core.iceberg import inject_view_debug

        inject_view_debug(self.debug_queries, src)

    @property
    def debug_calls(self) -> list[dict]:
        """Return the list of calls tracked in the current context."""
        try:
            from backend.utils.telemetry import get_tracked_calls

            return get_tracked_calls()
        except ImportError:
            return []

    def execute(self, q: str, p: list | None = None):
        """Execute a query and track its execution time.

        Self-heals on stale-view errors: if the connection's bound view
        references a buffer parquet file that no longer exists (the sync
        cron deleted it between the view bind and this query), refresh
        the view once and retry. Belt-and-suspenders alongside the pool's
        checkout fingerprint — that catches the common case, this catches
        the race where a commit lands while a query is in flight.

        ``execute_with_retry`` below also does this, but most callers use
        plain ``execute()``, so the retry needs to live here too. The
        cost when nothing's stale is a single Python try/except — no SQL,
        no extra round-trip.
        """
        t0 = time.time()
        try:
            res = self.con.execute(q, p if p is not None else [])
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
            try:
                from backend.core import iceberg as db_iceberg

                # force=True skips the fast path. We're already in an
                # error state because the view's cached SQL referenced a
                # file that no longer exists on disk; the fast path
                # would re-execute that same cached SQL (binding it,
                # which succeeds — but the next query against the view
                # would re-raise the same IOException). Force-rebuild
                # reads disk under the lock and regenerates the SQL.
                db_iceberg.update_iceberg_view(self.con, self.src, force=True)
            except Exception:
                # Refresh itself failed — surface the ORIGINAL error so
                # callers see the real symptom, not the rebind side-effect.
                raise e
            res = self.con.execute(q, p if p is not None else [])
        self.debug_queries.append(
            {"sql": _compact_sql_for_debug(q.strip()), "time_ms": round((time.time() - t0) * 1000, 2)}
        )
        return res

    def get_schema_cols(self) -> list[str]:
        """Get schema columns, retrying and refreshing the view if needed."""
        actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
        if not actual_cols:
            # Buffer file may have been deleted by a commit job. Refresh the view.
            try:
                from backend.core import iceberg as db_iceberg

                db_iceberg.update_iceberg_view(self.con, self.src)
                actual_cols = [col["name"] for col in _get_schema(self.con, self.src)]
            except Exception:
                pass
        self.actual_cols = set(actual_cols)
        return actual_cols

    def execute_with_retry(self, sql: str, params: list | None = None):
        """Execute a query with one stale-view retry.

        Returns the cursor on success, or None on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        try:
            return self.execute(sql, params)
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
        try:
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(self.con, self.src)
            return self.execute(sql, params)
        except Exception:
            return None

    def create_temp_table(self, sql: str, params: list | None = None) -> bool:
        """Execute a CREATE TEMP TABLE statement with one stale-view retry.

        A background commit job may delete a buffer file between the time our
        connection was opened and when we run the query. When that happens,
        re-initialising the Iceberg view and retrying once is sufficient to
        pick up the new snapshot.

        Returns True on success, False on permanent failure after retry.
        Non-stale errors are re-raised immediately.
        """
        try:
            self.execute(sql, params)
            return True
        except Exception as e:
            if not _is_stale_view_error(e):
                raise
        # Stale view — refresh and retry once.
        try:
            from backend.core import iceberg as db_iceberg

            db_iceberg.update_iceberg_view(self.con, self.src)
            self.execute(sql, params)
            return True
        except Exception:
            return False

    def telemetry(self) -> dict:
        """Return the standard debug telemetry dict for response construction."""
        return {"debug_queries": self.debug_queries, "debug_calls": self.debug_calls}

    def create_filtered_temp_table(
        self,
        cols: list[str],
        actual_cols: list[str],
        source_table: str,
        where_clause: str,
        params: list | None = None,
    ) -> str | None:
        """Build and execute a CREATE TEMP TABLE from a filtered column list.

        Returns the temp table name on success, None on failure.
        """
        import uuid as _uuid

        select_cols = [f'"{c}"' for c in cols if c in actual_cols]
        if not select_cols:
            return None
        temp_name = f"t_{_uuid.uuid4().hex}"
        sql = (
            f"CREATE TEMP TABLE {temp_name} AS SELECT {', '.join(select_cols)} FROM {source_table} WHERE {where_clause}"
        )
        if not self.create_temp_table(sql, params):
            return None
        return temp_name

    @contextlib.contextmanager
    def temp_table(
        self,
        cols: list[str],
        actual_cols: list[str] | set[str],
        source_table: str,
        where_clause: str,
        params: list | None = None,
    ):
        """Yield a filtered TEMP TABLE name (or None on creation failure), dropping it on exit.

        Compared to the bare ``create_filtered_temp_table`` + manual DROP pattern, the
        context manager guarantees the DROP runs even if an intermediate query raises.
        """
        name = self.create_filtered_temp_table(cols, actual_cols, source_table, where_clause, params)
        try:
            yield name
        finally:
            if name is not None:
                try:
                    self.execute(f"DROP TABLE IF EXISTS {name}")
                except Exception:
                    pass

    def execute_top_n_rollups(
        self,
        fields: list[str],
        start_time: str | None,
        end_time: str | None,
        limit: int = 10,
    ) -> tuple[list[tuple[str, Any, int]], list[str]]:
        import os
        from datetime import UTC, datetime, timedelta

        from backend.core.duckdb import _cache_dir
        from backend.core.rollups import _is_safe_ident, _safe_table_for
        from backend.utils.date_utils import parse_iso_utc

        cache_dir = _cache_dir(self.src)
        rollup_dir = os.path.join(cache_dir, "rollups", "hour")
        if not os.path.exists(rollup_dir):
            return [], fields

        # Defense-in-depth: field names land in a SQL IN-list as quoted
        # literals AND the service name lands in the base-table identifier.
        # Both should already be safe (FIELDS + validate_custom_field
        # constrain custom names; service IDs are Fastly-format slugs), but
        # we re-validate here so a future caller can't pierce the boundary.
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            return [], fields
        base_table = _safe_table_for(self.src)
        if not base_table:
            # Service name failed the identifier safelist; refuse to query.
            return [], fields

        # Parse bounds
        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None

        hour_cond = ""
        if st_dt:
            st_str = st_dt.strftime("%Y-%m-%d-%H")
            hour_cond += f" AND hour >= '{st_str}'"
        if et_dt:
            # Half-open semantics: a request ending exactly on an hour
            # boundary (e.g. ``end_time=2026-06-04T15:00:00``) should
            # EXCLUDE the 15:00 hour rollup (which covers [15:00, 16:00)).
            # Subtracting 1 microsecond before strftime keeps mid-hour
            # ends inclusive of the surrounding hour while making exact
            # boundaries exclusive — matching how the live-hour query
            # below uses ``timestamp < et_dt``.
            et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")
            hour_cond += f" AND hour <= '{et_inclusive}'"

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_dt_end = active_dt + timedelta(hours=1)
        active_str = active_dt.strftime("%Y-%m-%d-%H")

        # Glob `rollups/hour/**/*.parquet` was the obvious shape but it has
        # DuckDB enumerate every file under the tree before the WHERE clause
        # can prune ANYTHING. On a service with N fields × H hours of rollups
        # that's N*H file stats up front, dominating wall time (witnessed
        # 2026-06-04: ~2.8s on 18,648 files for a 24h query that should be
        # reading ~1,700). Hive-partition pruning kicks in AFTER the glob
        # expands, not before.
        #
        # Instead: enumerate the exact (field, hour) combinations we want in
        # Python (cheap directory listdir per field, bounded by safe_fields ×
        # hours-in-window), then pass DuckDB an explicit file list. Skips
        # the glob, hands DuckDB only the files it needs.
        st_str_floor = st_dt.strftime("%Y-%m-%d-%H") if st_dt else None
        # End cutoff for the directory-list filter — `et_inclusive` was
        # already computed above for the SQL fallback path. Use the same
        # bounds here so the half-open semantics match.
        if et_dt:
            et_str_floor = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")
        else:
            et_str_floor = None

        target_paths: list[str] = []
        for field in safe_fields:
            field_dir = os.path.join(rollup_dir, f"field={field}")
            if not os.path.isdir(field_dir):
                continue
            try:
                hour_entries = os.listdir(field_dir)
            except OSError:
                continue
            for hour_entry in hour_entries:
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # Lexicographic string compare is correct here because the
                # YYYY-MM-DD-HH format is fixed-width.
                if st_str_floor and hour < st_str_floor:
                    continue
                if et_str_floor and hour > et_str_floor:
                    continue
                if hour >= active_str:
                    # Active hour is served live, not from rollups.
                    continue
                hour_dir = os.path.join(field_dir, hour_entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet"):
                            target_paths.append(os.path.join(hour_dir, fname))
                except OSError:
                    continue

        if not target_paths:
            rolled_res: list = []
        else:
            # Inline the explicit path list as a SQL array literal. DuckDB
            # handles thousands of paths fine in a single statement; the
            # SQL string size is ~80 bytes/path × few-thousand = a few MB
            # at worst, well within parser limits. hive_partitioning=1
            # still lets DuckDB read `field` from the path so the SELECT's
            # `field` column resolves; `value`/`count` come from parquet
            # content.
            paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in target_paths)
            q = f"""
                SELECT field, value, SUM(count) AS c
                FROM read_parquet([{paths_sql}], hive_partitioning=1)
                GROUP BY field, value
            """
            try:
                rolled_res = self.execute(q).fetchall()
            except Exception:
                rolled_res = []

        # We also need to get the live active hour stats from the base table
        live_res = []

        live_where = f"timestamp >= '{active_dt.isoformat()}' AND timestamp < '{active_dt_end.isoformat()}'"
        # We only query the active hour if it overlaps with the requested time window
        should_query_live = True
        if et_dt and et_dt <= active_dt:
            should_query_live = False
        if st_dt and st_dt >= active_dt_end:
            should_query_live = False

        if should_query_live:
            # We run a standard execute_top_n_batch query on the base table for just the active hour
            try:
                actual_cols = self.get_schema_cols()
                from backend.core.duckdb import _get_schema

                schema_types = {col["name"]: col["type"] for col in _get_schema(self.con, self.src)}

                # To prevent creating a massive UNION, we'll create a temp table for just the live hour
                tmp_name = self.create_filtered_temp_table(fields, actual_cols, base_table, live_where)
                if tmp_name:
                    try:
                        live_res, _ = self.execute_top_n_batch(fields, tmp_name, actual_cols, schema_types, limit=limit)
                    finally:
                        try:
                            self.execute(f"DROP TABLE IF EXISTS {tmp_name}")
                        except Exception:
                            pass
            except Exception:
                pass

        # Combine rolled and live
        combined = {}
        for field, value, count in rolled_res:
            key = (field, value)
            combined[key] = combined.get(key, 0) + count

        for field, value, count in live_res:
            key = (field, value)
            combined[key] = combined.get(key, 0) + count

        # Sort and limit
        top_results = []
        for field in fields:
            field_items = [(k[1], v) for k, v in combined.items() if k[0] == field]
            field_items.sort(key=lambda x: x[1], reverse=True)
            for val, count in field_items[:limit]:
                top_results.append((field, val, count))

        return top_results, fields

    def execute_top_n_batch(
        self, fields: list[str], table_name: str, actual_cols: list[str], schema_types: dict[str, str], limit: int = 10
    ) -> tuple[list[tuple], list[str]]:
        """
        Generates and executes a single optimized UNION ALL query for multiple Top-N fields.
        Returns (fetchall_results, field_order).
        """
        from backend.repositories.utils.filters import resolve_col

        top_queries = []
        field_order = []

        # Fields whose underlying values are stored as FLOAT but represent
        # integer seconds (Fastly's obj.ttl / obj.age). Floating-point jitter
        # at ingest produces near-duplicate keys (3600.027, 3600.028, …) that
        # split GROUP BY into many tiny buckets. Round to integer to collapse
        # them — also drops the "3600.027" display in TopTenTable.
        INT_AGGREGATE_FIELDS = {"ttl", "age"}

        for field in fields:
            sql_col = resolve_col(field, actual_cols)
            col_type = schema_types.get(sql_col, "VARCHAR")

            if col_type == "VARCHAR":
                where_filter = f"{sql_col} IS NOT NULL AND {sql_col} != ''"
                select_val = sql_col
            elif field in INT_AGGREGATE_FIELDS:
                where_filter = f"{sql_col} IS NOT NULL"
                select_val = f"CAST(CAST(ROUND({sql_col}) AS INTEGER) AS VARCHAR)"
            else:
                where_filter = f"{sql_col} IS NOT NULL"
                select_val = f"CAST({sql_col} AS VARCHAR)"

            field_order.append(field)
            top_queries.append(f"""
                (SELECT '{field}' as field, {select_val} as value, count(*) as c
                FROM {table_name}
                WHERE {where_filter}
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT {limit})
            """)

        if not top_queries:
            return [], []

        union_q = " UNION ALL ".join(top_queries)
        return self.execute(union_q).fetchall(), field_order
