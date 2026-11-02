from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import tenacity

from backend.core.duckdb import escape_sql_literal
from backend.utils.telemetry import parse_hour_key_strict

if TYPE_CHECKING:
    import duckdb

from ._common import (
    TOP_K,
    _build_copy_query,
    _is_safe_ident,
    parquet_zstd_target,
    parse_hour_token,
    quote_path_list,
    unlink_quiet,
)

logger = logging.getLogger(__name__)


def partial_hour_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "partial_hour")


def partial_hour_dir(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_root(source), f"hour={hour}")


def _watermark_path(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_dir(source, hour), ".watermark")


def _all_fields_path(source: dict, hour: str) -> str:
    return os.path.join(partial_hour_dir(source, hour), "all_fields.parquet")


def read_partial_hour_watermark(source: dict, hour: str) -> float:
    path = _watermark_path(source, hour)
    try:
        with open(path) as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return 0.0


def _write_partial_hour_watermark(source: dict, hour: str, value: float) -> None:
    d = partial_hour_dir(source, hour)
    os.makedirs(d, exist_ok=True)
    path = _watermark_path(source, hour)
    tmp = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
    with open(tmp, "w") as f:
        f.write(repr(value))
    os.replace(tmp, path)


TOTAL_FIELD = "__total__"


def _read_partial_hour_parquet[T](
    path: str,
    con: duckdb.DuckDBPyConnection | None,
    query_fn: Callable[[duckdb.DuckDBPyConnection], T],
    default: T,
    *,
    err_label: str = "read",
) -> T:
    if not os.path.isfile(path):
        return default
    from backend.core.storage import is_stale_view_error

    def _run_once() -> T:
        if con is not None:
            return query_fn(con)
        from backend.core.duckdb import get_memory_connection

        tmp_con = get_memory_connection()
        try:
            return query_fn(tmp_con)
        finally:
            tmp_con.close()

    def _is_transient_read_error(exc: BaseException) -> bool:
        return is_stale_view_error(exc) or "Cannot open file" in str(exc) or "No such file" in str(exc)

    attempt_num = 0
    try:
        for attempt in tenacity.Retrying(
            stop=tenacity.stop_after_attempt(2),
            wait=tenacity.wait_fixed(0.01),
            retry=tenacity.retry_if_exception(_is_transient_read_error),
            sleep=lambda s: time.sleep(s),
            reraise=True,
        ):
            with attempt:
                attempt_num = attempt.retry_state.attempt_number
                return _run_once()
    except Exception as e:
        if attempt_num > 1:
            logger.warning("[partial_hour] Retry %s failed on %s: %s", err_label, path, e)
            return default
        logger.warning("[partial_hour] Failed %s on %s: %s", err_label, path, e)
        if any(err in str(e).lower() for err in ("corrupt", "not a parquet file", "bad magic number", "footer")):
            unlink_quiet(path)
        return default
    return default


def read_partial_hour_all_fields(
    source: dict, hour: str, con: duckdb.DuckDBPyConnection | None = None
) -> list[tuple[str, str, int]]:
    path = _all_fields_path(source, hour)
    safe_path = escape_sql_literal(str(path))
    sql = f"SELECT field, value, count FROM read_parquet('{safe_path}') WHERE field != '{TOTAL_FIELD}'"

    def _query(c: duckdb.DuckDBPyConnection) -> list[tuple[str, str, int]]:
        rows = c.execute(sql).fetchall()
        return [(r[0], r[1], int(r[2])) for r in rows]

    return _read_partial_hour_parquet(path, con, _query, [], err_label="read")


def read_partial_hour_total(source: dict, hour: str, con: duckdb.DuckDBPyConnection | None = None) -> int:
    path = _all_fields_path(source, hour)
    safe_path = escape_sql_literal(str(path))
    sql = f"SELECT count FROM read_parquet('{safe_path}') WHERE field = '{TOTAL_FIELD}' AND value = '' LIMIT 1"

    def _query(c: duckdb.DuckDBPyConnection) -> int:
        row = c.execute(sql).fetchone()
        return int(row[0]) if row else 0

    return _read_partial_hour_parquet(path, con, _query, 0, err_label="total read")


def _list_new_source_files(source: dict, hour: str, watermark: float) -> list[str]:
    from backend.core.duckdb import _cache_dir

    cache_dir = _cache_dir(source)
    dirs = [os.path.join(cache_dir, "buffer"), os.path.join(cache_dir, "data", f"timestamp_hour={hour}")]
    try:
        from backend.core.storage._core import _tombstoned_parquet_paths

        tombstoned = _tombstoned_parquet_paths(os.path.join(cache_dir, "buffer"))
    except Exception:
        tombstoned = set()
    out: list[str] = []
    for d in dirs:
        try:
            with os.scandir(d) as it:
                for e in it:
                    if not e.name.endswith(".parquet") or e.name.startswith(".tmp_"):
                        continue
                    if e.path in tombstoned:
                        continue
                    try:
                        if e.stat().st_mtime <= watermark:
                            continue
                    except OSError:
                        continue
                    out.append(e.path)
        except OSError:
            continue
    return out


def merge_partial_hour(service_id: str, source: dict, fields: list[str]) -> dict:
    from backend.core.storage._core import _get_service_lock

    lock_key = source.get("name", service_id)
    with _get_service_lock(lock_key):
        return _merge_partial_hour_locked(service_id, source, fields)


def _merge_partial_hour_locked(service_id: str, source: dict, fields: list[str]) -> dict:
    t0 = time.perf_counter()
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    existing_path = _all_fields_path(source, active_hour)
    existing_corrupt = False
    from backend.core.duckdb import get_memory_connection

    if os.path.isfile(existing_path):
        probe_con = get_memory_connection()
        try:
            safe_existing = escape_sql_literal(str(existing_path))
            probe_con.execute(f"SELECT 1 FROM read_parquet('{safe_existing}') LIMIT 1")
        except Exception as e:
            logger.warning(
                "[partial_hour] %s: existing partial-hour file %s is corrupt or unreadable (%s); "
                "deleting and resetting watermark to re-accumulate from active-hour raw buffer files",
                service_id,
                existing_path,
                e,
            )
            unlink_quiet(existing_path)
            existing_corrupt = True
        finally:
            probe_con.close()
    watermark = 0.0 if existing_corrupt else read_partial_hour_watermark(source, active_hour)
    new_files = _list_new_source_files(source, active_hour, watermark)
    if not new_files:
        return {"hour": active_hour, "new_files": 0, "duration_ms": (time.perf_counter() - t0) * 1000}
    safe_fields = [f for f in fields if _is_safe_ident(f) and f != TOTAL_FIELD]
    con = get_memory_connection()
    try:
        con.execute("SET TimeZone='UTC';")
        paths_sql = quote_path_list(new_files)
        table_ident = f"read_parquet([{paths_sql}], union_by_name=true)"
        desc_rows = con.execute(f"DESCRIBE SELECT * FROM {table_ident} LIMIT 0").fetchall()
        actual_cols = {row[0] for row in desc_rows}
        available_fields = [f for f in safe_fields if f in actual_cols]
        hour_start = parse_hour_key_strict(active_hour)
        hour_end = hour_start + timedelta(hours=1)
        where_sql = (
            f"timestamp >= TIMESTAMP '{hour_start.isoformat()}' AND timestamp < TIMESTAMP '{hour_end.isoformat()}'"
        )
        existing_sql = ""
        if not existing_corrupt and os.path.isfile(existing_path):
            existing_sql = (
                f" UNION ALL SELECT field, value, count FROM read_parquet({quote_path_list([existing_path])})"
            )
        total_select = (
            f"SELECT '{TOTAL_FIELD}' AS field, '' AS value, COUNT(*) AS count FROM {table_ident} WHERE {where_sql}"
        )
        if available_fields:
            per_field_selects = [_build_copy_query(table_ident, f, where_sql) for f in available_fields]
            increment_sql = " UNION ALL ".join(f"({s})" for s in per_field_selects)
            increment_source = f"SELECT field, value, count FROM ({increment_sql})"
            select_sources = f"{increment_source}{existing_sql} UNION ALL ({total_select})"
        else:
            if existing_sql:
                existing_clean = existing_sql.removeprefix(" UNION ALL ")
                select_sources = f"{existing_clean} UNION ALL ({total_select})"
            else:
                select_sources = f"{total_select}"
        merged_sql = f"""
        SELECT field, value, CAST(count AS BIGINT) AS count FROM ( SELECT field, value, SUM(count) AS count,
        ROW_NUMBER() OVER (PARTITION BY field ORDER BY SUM(count) DESC) AS rn FROM ( {select_sources} ) GROUP BY field, value ) WHERE rn <= {TOP_K}
    """
        d = partial_hour_dir(source, active_hour)
        os.makedirs(d, exist_ok=True)
        tmp_path = os.path.join(d, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
        try:
            try:
                con.execute(f"COPY ({merged_sql}) {parquet_zstd_target(tmp_path)}")
            except Exception as e:
                from backend.core.storage import is_stale_view_error

                if is_stale_view_error(e) or "Cannot open file" in str(e) or "No such file" in str(e):
                    logger.info(
                        "[partial_hour] %s: buffer file vanished during merge (%s); re-listing",
                        service_id,
                        e,
                    )
                    new_files = [f for f in new_files if os.path.isfile(f)]
                    if not new_files:
                        return {"hour": active_hour, "new_files": 0, "duration_ms": (time.perf_counter() - t0) * 1000}
                    paths_sql = quote_path_list(new_files)
                    table_ident = f"read_parquet([{paths_sql}], union_by_name=true)"
                    desc_rows = con.execute(f"DESCRIBE SELECT * FROM {table_ident} LIMIT 0").fetchall()
                    actual_cols = {row[0] for row in desc_rows}
                    available_fields = [f for f in safe_fields if f in actual_cols]
                    total_select = f"SELECT '{TOTAL_FIELD}' AS field, '' AS value, COUNT(*) AS count FROM {table_ident} WHERE {where_sql}"
                    if available_fields:
                        per_field_selects = [_build_copy_query(table_ident, f, where_sql) for f in available_fields]
                        increment_sql = " UNION ALL ".join(f"({s})" for s in per_field_selects)
                        increment_source = f"SELECT field, value, count FROM ({increment_sql})"
                        select_sources = f"{increment_source}{existing_sql} UNION ALL ({total_select})"
                    else:
                        if existing_sql:
                            existing_clean = existing_sql.removeprefix(" UNION ALL ")
                            select_sources = f"{existing_clean} UNION ALL ({total_select})"
                        else:
                            select_sources = f"{total_select}"
                    merged_sql = f"""
        SELECT field, value, CAST(count AS BIGINT) AS count FROM ( SELECT field, value, SUM(count) AS count,
        ROW_NUMBER() OVER (PARTITION BY field ORDER BY SUM(count) DESC) AS rn FROM ( {select_sources} ) GROUP BY field, value ) WHERE rn <= {TOP_K}
    """
                    con.execute(f"COPY ({merged_sql}) {parquet_zstd_target(tmp_path)}")
                else:
                    raise
            os.replace(tmp_path, _all_fields_path(source, active_hour))
        finally:
            if os.path.exists(tmp_path):
                unlink_quiet(tmp_path)
    finally:
        con.close()
    max_mtime = max((os.path.getmtime(p) for p in new_files if os.path.exists(p)), default=watermark)
    _write_partial_hour_watermark(source, active_hour, max_mtime)
    duration_ms = (time.perf_counter() - t0) * 1000
    logger.debug(
        "[partial_hour] %s: merged %d new file(s) into hour=%s (%.1fms)",
        service_id,
        len(new_files),
        active_hour,
        duration_ms,
    )
    return {"hour": active_hour, "new_files": len(new_files), "duration_ms": duration_ms}


def gc_stale_partial_hours(source: dict, max_age_hours: int = 2) -> int:
    from backend.core.storage._core import _get_service_lock

    lock_key = source.get("name", source.get("service_id", "default"))
    with _get_service_lock(lock_key):
        root = partial_hour_root(source)
        if not os.path.isdir(root):
            return 0
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
        removed = 0
        try:
            entries = os.listdir(root)
        except OSError:
            return 0
        for entry in entries:
            if not entry.startswith("hour="):
                continue
            hour = entry[len("hour=") :]
            if hour == active_hour:
                continue
            parsed = parse_hour_token(hour)
            if parsed is None or parsed >= cutoff:
                continue
            try:
                shutil.rmtree(os.path.join(root, entry))
                removed += 1
            except OSError as e:
                logger.warning("[partial_hour] failed to GC stale hour=%s: %s", hour, e)
        return removed
