"""Shared helpers used by every repository module.

Import from here instead of redefining in each file:

    from backend.repositories._base import _safe_table, _get_schema, safe_iso, QueryRunner
"""

from __future__ import annotations

import contextlib
import time

import duckdb


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


def safe_iso(dt) -> str | None:
    """Normalise a datetime or string to an ISO-8601 string ending in Z."""
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        s = dt.isoformat()
        # Append Z for naive UTC datetimes that lack a tz suffix
        if not s.endswith("Z") and "+" not in s and s.count("-") <= 2:
            s += "Z"
        return s
    return str(dt)


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
        """Execute a query and track its execution time."""
        t0 = time.time()
        res = self.con.execute(q, p if p is not None else [])
        self.debug_queries.append({"sql": q.strip(), "time_ms": round((time.time() - t0) * 1000, 2)})
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
