"""DuckDB sync status, schema discovery, top-values caching, ASN enrichment, and usage/cron status helpers."""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.core.sql_validator import escape_sql_literal
from backend.utils.bounded_cache import BoundedLRUCache, BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.telemetry import log_once_per, parse_iso_utc
from backend.utils.telemetry import safe_iso as _safe_iso

logger = logging.getLogger(__name__)

_STATUS_BUSY_WINDOW_MINS = 5
_TASK_TO_CRON_KEY = {"sync": "cron_sync", "local_compact": "cron_compact"}


def _db():
    from backend.core import duckdb as _db_mod

    return _db_mod


def _cache_dir(source: dict | None = None) -> str:
    return _db()._cache_dir(source)


def _is_stale_view_error(err: BaseException) -> bool:
    return _db()._is_stale_view_error(err)


def _source_service_id(source: dict | None) -> str:
    return _db()._source_service_id(source)


def _safe_table_name(name: str) -> str:
    return _db()._safe_table_name(name)


def is_configured(source: dict | None = None) -> bool:
    return _db().is_configured(source)


def get_connection(
    source: dict | None = None,
    skip_view_update: bool = False,
    read_only: bool = False,
    memory_limit: str | None = None,
) -> duckdb.DuckDBPyConnection:
    if memory_limit is not None:
        return _db().get_connection(
            source,
            skip_view_update=skip_view_update,
            read_only=read_only,
            memory_limit=memory_limit,
        )
    return _db().get_connection(
        source,
        skip_view_update=skip_view_update,
        read_only=read_only,
    )


def start_cron_run(source: dict, task: str) -> int | None:
    from backend.core import metadata as metadata_db

    return metadata_db.start_cron_run(source["name"], task)


def log_cron_run(
    source: dict,
    task: str,
    duration_s: float,
    status: str,
    error_message: str | None = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list | None = None,
    summary: str | None = None,
    log_output: str | None = None,
    run_id: int | None = None,
    outcome_counters: dict[str, int] | None = None,
):
    from backend.core import metadata as metadata_db

    service_id = source["name"]
    prov = (svcconfig.load_config(service_id) or {}).get("provisioning", {})
    cron_key = _TASK_TO_CRON_KEY.get(task)
    log_enabled = prov.get(cron_key, {}).get("log_enabled", True) if cron_key else True
    if status == "success" and corrupt_rows and corrupt_rows > 0:
        status = "partial_success"
    if not log_enabled and status == "success":
        if run_id is not None:
            try:
                metadata_db.delete_cron_run(service_id, run_id)
            except Exception:
                logger.warning("log_cron_run: metadata_db.delete_cron_run failed (ignored)", exc_info=True)
        return
    try:
        metadata_db.log_cron_run(
            service_id,
            task,
            duration_s,
            status,
            error_message=error_message,
            files_downloaded=files_downloaded or 0,
            files_deleted_fos=files_deleted_fos or 0,
            rows_ingested=rows_ingested or 0,
            corrupt_rows=corrupt_rows or 0,
            parquet_files_created=parquet_files_created or 0,
            parquet_files_optimized=parquet_files_optimized or 0,
            parquet_keys=parquet_keys or [],
            summary=summary,
            log_output=log_output,
            run_id=run_id,
            outcome_counters=outcome_counters,
        )
    except Exception as e:
        logger.warning("[cron_log] failed to persist cron run for %s/%s: %s", service_id, task, e)


def finalize_cron_run_if_running(
    source: dict,
    task: str,
    run_id: int | None,
    *,
    duration_s: float = 0.0,
    summary: str | None = None,
    error_message: str | None = None,
) -> bool:
    if run_id is None:
        return False
    from backend.core import metadata as metadata_db

    try:
        return metadata_db.finalize_cron_run_if_running(
            source["name"], task, run_id, duration_s=duration_s, summary=summary, error_message=error_message
        )
    except Exception as e:
        logger.warning("[cron_log] finalize-backstop failed for %s/%s: %s", source.get("name"), task, e)
        return False


_data_stats_cache: BoundedLRUCache[str, tuple[tuple, int, Any, Any]] = BoundedLRUCache(maxsize=256)
_data_stats_cache_lock = threading.Lock()
_top_values_cache: BoundedLRUCache[str, tuple] = BoundedLRUCache(maxsize=256)
_top_values_cache_lock = threading.Lock()
_ducklake_stats_cache: BoundedLRUCache[tuple[str, str], tuple[int, int, Any, Any]] = BoundedLRUCache(maxsize=256)
_ducklake_stats_cache_lock = threading.Lock()


def _data_stats_fingerprint(source: dict) -> tuple | None:
    data_dir = os.path.join(_cache_dir(source), "data")
    if not os.path.isdir(data_dir):
        return None
    data_sum = data_count = 0
    try:
        for entry in os.scandir(data_dir):
            try:
                data_sum += entry.stat().st_mtime_ns
                data_count += 1
            except OSError:
                pass
    except OSError:
        return None
    return (data_sum, data_count)


def _top_values_fingerprint(con: duckdb.DuckDBPyConnection, source: dict) -> tuple | None:
    if (fp := _data_stats_fingerprint(source)) is not None:
        return fp
    try:
        from backend.core.storage._core import ducklake_current_snapshot_id

        snap_id = ducklake_current_snapshot_id(con)
        return ("ducklake", snap_id) if snap_id is not None else None
    except Exception:
        logger.debug("update_top_values: step failed (ignored)", exc_info=True)
        return None


def clear_ducklake_stats_cache(source_key: str | None = None) -> None:
    with _ducklake_stats_cache_lock:
        if source_key is None:
            _ducklake_stats_cache.clear()
        else:
            for k in [k for k in _ducklake_stats_cache if k[0] == source_key]:
                _ducklake_stats_cache.pop(k, None)
    with _data_stats_cache_lock:
        if source_key is None:
            _data_stats_cache.clear()
        else:
            _data_stats_cache.pop(source_key, None)


def _query_buffer_stats(con, src) -> tuple[int, Any, Any]:
    from backend.core import storage as _storage

    buf_paths = [p for p in _storage.buffer_files(src) if os.path.isfile(p)]
    if not buf_paths:
        return (0, None, None)
    paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in buf_paths)
    row = con.execute(
        f"SELECT count(*), min(timestamp), max(timestamp) FROM read_parquet([{paths_sql}], union_by_name=true, hive_partitioning=false)"
    ).fetchone()
    return ((row[0] or 0), row[1], row[2]) if row else (0, None, None)


def _query_or_cache_lake_stats(con, src_name: str, lake_table: str, snap_id: Any) -> tuple[int, Any, Any]:
    cached = None
    if snap_id is not None:
        with _ducklake_stats_cache_lock:
            cached = _ducklake_stats_cache.get((src_name, lake_table))
        if (cached is None or cached[0] != snap_id) and lake_table == "logs":
            with _data_stats_cache_lock:
                data_cached = _data_stats_cache.get(src_name)
            if data_cached is not None and data_cached[0] == ("ducklake", snap_id):
                cached = (snap_id, data_cached[1], data_cached[2], data_cached[3])
                with _ducklake_stats_cache_lock:
                    _ducklake_stats_cache[(src_name, lake_table)] = cached
    if cached is not None and cached[0] == snap_id:
        return cached[1], cached[2], cached[3]
    row = con.execute(f'SELECT count(*), min(timestamp), max(timestamp) FROM lake."{lake_table}"').fetchone()
    c, mn, mx = ((row[0] or 0), row[1], row[2]) if row else (0, None, None)
    if snap_id is not None:
        with _ducklake_stats_cache_lock:
            _ducklake_stats_cache[(src_name, lake_table)] = (snap_id, c, mn, mx)
        if lake_table == "logs":
            with _data_stats_cache_lock:
                _data_stats_cache[src_name] = (("ducklake", snap_id), c, mn, mx)
    return c, mn, mx


def _authoritative_direct_stats(
    con, src, *, buffer_stats: tuple[int, Any, Any] | None = None
) -> tuple[int, Any, Any] | None:
    counts, mins, maxs = [], [], []
    src_name = src.get("name", "default")
    try:
        from backend.core.storage._core import ducklake_current_snapshot_id, ducklake_table_name

        lake_table = ducklake_table_name(src)
        if con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [lake_table]
        ).fetchone():
            lc, lmn, lmx = _query_or_cache_lake_stats(con, src_name, lake_table, ducklake_current_snapshot_id(con))
            if lc > 0:
                counts.append(lc)
                mins.extend([lmn] if lmn is not None else [])
                maxs.extend([lmx] if lmx is not None else [])
    except Exception as e:
        logger.debug("[sync-status] direct lake stats unavailable: %s", e)
    try:
        b_cnt, b_min, b_max = buffer_stats if buffer_stats is not None else _query_buffer_stats(con, src)
        if b_cnt > 0:
            counts.append(b_cnt)
        mins.extend([b_min] if b_min is not None else [])
        maxs.extend([b_max] if b_max is not None else [])
    except Exception as e:
        logger.debug("[sync-status] direct buffer stats unavailable: %s", e)
    total = sum(counts)
    if total <= 0:
        return None
    earliest = min(mins) if mins else None
    latest = max(maxs) if maxs else None
    return (total, earliest, latest)


def get_sync_status(
    con: duckdb.DuckDBPyConnection, source: dict | None = None, skip_fos: bool = False, force: bool = False
) -> dict:
    src = source or _db()._DEFAULT_SOURCE
    if not is_configured(src):
        return {
            "configured": False,
            "local_rows": 0,
            "ingested": 0,
            "fos_total": 0,
            "storage_mode": "cloud",
        }
    cached_status = svcconfig.get_status(src["name"])
    if cached_status and not force and skip_fos:
        cached_status.update(storage_mode=src.get("storage_mode", "cloud"), configured=True)
        return cached_status
    table_name = _safe_table_name(src["name"])
    try:
        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(src["name"])
    except Exception:
        summary = {
            "file_count": 0,
            "total_rows": 0,
            "total_bytes": 0,
            "count_with_bytes": 0,
            "last_ingested": None,
            "latest_file_name": None,
        }
    file_count, local_rows_ingested = summary["file_count"], summary["total_rows"]
    last_ingested, latest_file_name = summary["last_ingested"], summary["latest_file_name"]
    total_bytes, count_with_bytes = summary["total_bytes"], summary["count_with_bytes"]
    avg_log_size_kb = (total_bytes / count_with_bytes / 1024.0) if count_with_bytes > 0 else None
    latest_ingested_file_at = (
        f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"
        if latest_file_name
        and (m := re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", latest_file_name.split("/")[-1]))
        else None
    )
    latest_log_at = earliest_log_at = None
    local_rows = local_rows_ingested
    try:
        stats = None
        data_fp = _data_stats_fingerprint(src)
        lake_table = None
        if data_fp is None:
            try:
                from backend.core.storage._core import ducklake_current_snapshot_id, ducklake_table_name

                lt = ducklake_table_name(src)
                exists = con.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [lt]
                ).fetchone()
                if exists and exists[0] and (snap_id := ducklake_current_snapshot_id(con)) is not None:
                    data_fp = ("ducklake", snap_id)
                    lake_table = lt
            except Exception:
                logger.debug("get_sync_status: step failed (ignored)", exc_info=True)
        cache_key = src["name"]
        direct_stats = None
        queried_buffer_stats = None
        if data_fp is not None:
            try:
                if (
                    lake_table is not None
                    and isinstance(data_fp, tuple)
                    and len(data_fp) == 2
                    and data_fp[0] == "ducklake"
                ):
                    d_count, d_min, d_max = _query_or_cache_lake_stats(con, cache_key, lake_table, data_fp[1])
                else:
                    with _data_stats_cache_lock:
                        cached = _data_stats_cache.get(cache_key)
                    if cached is not None and cached[0] == data_fp:
                        d_count, d_min, d_max = cached[1], cached[2], cached[3]
                    else:
                        safe_glob = escape_sql_literal(os.path.join(_cache_dir(src), "data", "**", "*.parquet"))
                        d_row = con.execute(
                            f"SELECT count(*), min(timestamp), max(timestamp) FROM read_parquet('{safe_glob}', union_by_name=true, hive_partitioning=false)"
                        ).fetchone()
                        d_count, d_min, d_max = ((d_row[0] or 0), d_row[1], d_row[2]) if d_row else (0, None, None)
                        with _data_stats_cache_lock:
                            _data_stats_cache[cache_key] = (data_fp, d_count, d_min, d_max)
                b_count, b_min, b_max = queried_buffer_stats = _query_buffer_stats(con, src)
                mins, maxs = [m for m in (d_min, b_min) if m is not None], [m for m in (d_max, b_max) if m is not None]
                stats = (d_count + b_count, min(mins) if mins else None, max(maxs) if maxs else None)
                if lake_table is not None and stats[0] > 0:
                    direct_stats = stats
            except Exception as split_err:
                with _data_stats_cache_lock:
                    _data_stats_cache.pop(cache_key, None)
                if _is_stale_view_error(split_err):
                    raise
                logger.debug("[sync-status] split-stats query failed, falling back to view: %s", split_err)
        if stats is None:
            stats = con.execute(f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}").fetchone()
        if stats:
            view_rows = stats[0] if stats[0] is not None else 0
            direct = (
                direct_stats
                if direct_stats is not None
                else _authoritative_direct_stats(con, src, buffer_stats=queried_buffer_stats)
            )
            if view_rows > 0:
                local_rows, earliest_log_at, latest_log_at = view_rows, stats[1], stats[2]
                if direct is not None:
                    if direct[2] is not None and (latest_log_at is None or direct[2] > latest_log_at):
                        latest_log_at = direct[2]
                    if direct[1] is not None and (earliest_log_at is None or direct[1] < earliest_log_at):
                        earliest_log_at = direct[1]
            elif direct is not None:
                local_rows, earliest_log_at, latest_log_at = direct[0], direct[1], direct[2]
            else:
                local_rows = (
                    prior_rows
                    if (prior_rows := (cached_status or {}).get("local_rows") or 0) > 0
                    else local_rows_ingested
                )
    except Exception as e:
        if _is_stale_view_error(e):
            try:
                from backend.core import storage

                storage.clear_source_caches(src.get("name", "default"), keep_snapshot_cache=True)
                storage.update_storage_view(con, src)
                if stats := con.execute(
                    f"SELECT count(*), min(timestamp), max(timestamp) FROM {table_name}"
                ).fetchone():
                    local_rows, earliest_log_at, latest_log_at = stats[0] or 0, stats[1], stats[2]
            except Exception as retry_e:
                logger.debug("[sync-status] log stats unavailable mid-rebuild: %s", retry_e)
                local_rows = local_rows_ingested
        else:
            logger.warning("[sync-status] Failed to get log stats from view: %s", e)
            local_rows = local_rows_ingested
    try:
        cron_stats, busy = {}, False
        time_cutoff = (
            (datetime.now(UTC) - timedelta(minutes=_STATUS_BUSY_WINDOW_MINS))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        from backend.core.metadata.usage_log import latest_cron_per_task

        for t, data in latest_cron_per_task(_source_service_id(src)).items():
            st = data.get("started_at")
            if data.get("status") == "running" and st and str(st) > time_cutoff:
                busy = True
            cron_stats[t] = {
                "last_run": _safe_iso(st),
                "duration_s": data.get("duration_s"),
                "status": data.get("status"),
                "error_message": data.get("error_message"),
                "summary": data.get("summary"),
            }
    except Exception:
        busy, cron_stats = False, {}
    return {
        "busy": busy,
        "fos_total": file_count,
        "ingested": file_count,
        "local_rows": local_rows,
        "ingested_bytes": total_bytes,
        "avg_log_size_kb": avg_log_size_kb,
        "table_name": table_name,
        "last_ingested_at": _safe_iso(last_ingested),
        "latest_log_at": _safe_iso(latest_log_at),
        "earliest_log_at": _safe_iso(earliest_log_at),
        "latest_ingested_file_at": latest_ingested_file_at,
        "latest_available_file_at": latest_ingested_file_at,
        "configured": is_configured(src),
        "storage_mode": src.get("storage_mode", "cloud"),
        "logging_service_id": src.get("logging_service_id", ""),
        "cdn_service_id": src.get("cdn_service_id", ""),
        "cron_stats": cron_stats,
    }


def refresh_config_status(service_id: str, include_top_values: bool = True):
    if not (src := svcconfig.load_config(service_id)):
        return
    source = svcconfig.config_to_source(src)
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    if get_high_scale_service_registry().resolve(service_id) is not None:
        logger.info("[refresh_status] %s: high-scale service uses ClickHouse status", service_id)
        return
    durable_serving, cached_status = svcconfig.is_durable_serving_mode(source), src.get("status") or {}
    buf_bytes = (
        cached_status.get("buffer_size_bytes")
        if (not include_top_values and "buffer_size_bytes" in cached_status)
        else None
    )
    if include_top_values or "buffer_size_bytes" not in cached_status:
        try:
            from backend.utils.dir_size import _get_dir_size

            if os.path.isdir(buf_dir := _cache_dir(source)):
                buf_bytes = _get_dir_size(buf_dir)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)
    storage_bytes = storage_files = None
    if not include_top_values and "storage_bytes" in cached_status and "storage_files" in cached_status:
        storage_bytes, storage_files = cached_status.get("storage_bytes"), cached_status.get("storage_files")
    else:
        try:
            from backend.core import storage as _db_storage

            info = _db_storage.get_table_info(source)
            if not info.get("error"):
                storage_bytes, storage_files = int(info.get("size_bytes", 0) or 0), int(info.get("data_files", 0) or 0)
        except Exception:
            logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)
    con = None
    try:
        con = get_connection(source, skip_view_update=not durable_serving, read_only=True)
        status = {} if durable_serving else get_sync_status(con, source, skip_fos=False, force=True)
        for k, v in (
            ("buffer_size_bytes", buf_bytes),
            ("storage_bytes", storage_bytes),
            ("storage_files", storage_files),
        ):
            if v is not None:
                status[k] = v
        if not include_top_values:
            if "edge_ratio" in cached_status:
                status["edge_ratio"] = cached_status.get("edge_ratio")
        else:
            try:
                from backend.repositories import assets as _usage_repo

                if (ratio := _usage_repo.get_edge_ratio(con, source)[0]) is not None:
                    status["edge_ratio"] = ratio
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)
            status["schema"] = get_schema(con, source)
        rum_latest = rum_last_sync = request_last_sync = None
        rum_total, request_total, request_latest = 0, status.get("local_rows", 0), status.get("latest_log_at")
        try:
            from backend.core.metadata.usage_log import latest_cron_per_task

            cron_data = latest_cron_per_task(service_id)
            rum_last_sync = cron_data.get("rum_sync", {}).get("started_at")
            request_last_sync = cron_data.get("log_discovery", {}).get("started_at") or cron_data.get("sync", {}).get(
                "started_at"
            )
        except Exception:
            logger.debug("refresh_config_status: cron_data.get failed (ignored)", exc_info=True)
        if svcconfig.is_rum_enabled(source):
            try:
                persisted_rum = (src.get("status") or {}).get("rum") or {}
                rum_total, rum_latest = persisted_rum.get("total_rows", 0) or 0, persisted_rum.get("latest_log_at")
                rum_last_sync = persisted_rum.get("last_sync_at") or rum_last_sync
            except Exception:
                logger.debug("refresh_config_status: step failed (ignored)", exc_info=True)
        status["rum"] = {"latest_log_at": rum_latest, "total_rows": rum_total, "last_sync_at": rum_last_sync}
        if not durable_serving:
            status["request"] = {
                "latest_log_at": request_latest,
                "total_rows": request_total,
                "last_sync_at": request_last_sync,
            }
            svcconfig.update_status(service_id, status)
        else:
            from backend.core.request_telemetry import refresh_durable_request_metrics

            refresh_durable_request_metrics(source, extra_status=status)
        if include_top_values:
            logger.info("[refresh_status] %s: Updating top-values cache for filter suggestions...", service_id)
            update_top_values(con, source)
    except Exception:
        logger.warning("Failed to refresh config status for %s", service_id, exc_info=True)
    finally:
        if con:
            con.close()


_TOP_VALUE_FIELDS = (
    "ip",
    "country",
    "city",
    "host",
    "url",
    "method",
    "ua",
    "status",
    "cache",
    "waf",
    "waf_resp",
    "waf_ms",
    "waf_sig",
    "waf_sig_ind",
    "ja3",
    "ja4",
    "asn",
    "edge",
    "proto",
    "tls",
    "referer",
    "p_type",
    "p_desc",
    "backend",
    "pop",
)


def update_top_values(con: duckdb.DuckDBPyConnection, source: dict):
    service_id, table_name = source["name"], _safe_table_name(source["name"])
    cached_top_values_path = os.path.join(_cache_dir(source), "top_values.json")
    data_fp = _top_values_fingerprint(con, source)
    if data_fp is not None and os.path.exists(cached_top_values_path):
        with _top_values_cache_lock:
            if _top_values_cache.get(service_id) == data_fp:
                return
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        return
    schema_cols = {f["name"] for f in get_schema(con, source, stats=False)}
    if not (
        fields := [
            f for f in _TOP_VALUE_FIELDS if f in schema_cols or (f == "waf_sig_ind" and "waf_sig" in schema_cols)
        ]
    ):
        return
    sel = ", ".join(
        dict.fromkeys(
            f'"{"waf_sig" if f == "waf_sig_ind" else f}"'
            for f in fields
            if ("waf_sig" if f == "waf_sig_ind" else f) in schema_cols
        )
    )
    sample_table = f"_top_sample_{service_id.replace('-', '_')}"
    top_values: dict = {}
    sample_sql = (
        f"CREATE TEMP TABLE {sample_table} AS SELECT {sel} FROM {table_name} USING SAMPLE reservoir(100000 ROWS)"
    )
    try:
        con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        try:
            con.execute(sample_sql)
        except Exception as _e:
            if not _is_stale_view_error(_e):
                raise
            from backend.core import storage

            storage.update_storage_view(con, source)
            con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
            con.execute(sample_sql)
        queries, field_order = [], []
        for f in fields:
            col = "waf_sig" if f == "waf_sig_ind" else f
            if col not in schema_cols:
                continue
            src_sql = (
                f"(SELECT unnest(string_split(\"{col}\", ',')) AS signal FROM {sample_table} "
                f"WHERE \"{col}\" IS NOT NULL AND \"{col}\" != '') WHERE trim(signal) != ''"
                if f == "waf_sig_ind"
                else f'{sample_table} WHERE "{col}" IS NOT NULL'
            )
            val_expr = "trim(signal)" if f == "waf_sig_ind" else f'CAST("{col}" AS VARCHAR)'
            queries.append(
                f"(SELECT '{f}' AS _field, {val_expr} AS _value, count(*) AS _cnt FROM {src_sql} GROUP BY 1,2 ORDER BY 3 DESC LIMIT 200)"
            )
            field_order.append(f)
        if queries:
            top_values = {fname: [] for fname in field_order}
            for fname, fval, fcnt in con.execute(" UNION ALL ".join(queries)).fetchall():
                if fname in top_values and len(top_values[fname]) < 200:
                    top_values[fname].append({"value": fval, "count": fcnt})
    except Exception:
        logger.warning("Failed to build top-values index", exc_info=True)
    finally:
        try:
            con.execute(f'DROP TABLE IF EXISTS "{sample_table}"')
        except Exception:
            if log_once_per("update_top_values: DROP TABLE failed (ignored)", 60):
                logger.debug("update_top_values: DROP TABLE failed (ignored)", exc_info=True)
    if top_values:
        os.makedirs(cache_dir := _cache_dir(source), exist_ok=True)
        with open(os.path.join(cache_dir, "top_values.json"), "w") as fp:
            json.dump(top_values, fp)
        if (post_fp := _top_values_fingerprint(con, source)) is not None:
            with _top_values_cache_lock:
                _top_values_cache[service_id] = post_fp


def get_ingested_files(con: duckdb.DuckDBPyConnection | None, source: dict | None = None) -> list[dict]:
    from backend.core import metadata as metadata_db

    return metadata_db.list_ingested_files((source or _db()._DEFAULT_SOURCE)["name"])


_SCHEMA_CACHE_TTL = 300
_schema_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_SCHEMA_CACHE_TTL)


def _clear_schema_cache(source_name: str | None = None):
    if source_name:
        for k in [k for k in _schema_cache.keys() if k[0] == source_name]:
            _schema_cache.pop(k, None)
    else:
        _schema_cache.clear()


def get_schema(con: duckdb.DuckDBPyConnection, source: dict | None = None, stats: bool = True) -> list[dict]:
    src = source or _db()._DEFAULT_SOURCE
    source_name, table_name = src["name"], _safe_table_name(src["name"])
    cache_key = (source_name, table_name, stats)
    if (cached := _schema_cache.get(cache_key)) is not None:
        return cached
    try:
        row = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table_name]
        ).fetchone()
        if row is None:
            raise RuntimeError("row is unexpectedly None")
        if row[0] <= 0:
            return []
        if not stats:
            schema = [{"name": r[0], "type": r[1]} for r in con.execute(f"DESCRIBE {table_name}").fetchall()]
        else:
            schema = []
            for r in con.execute(f"SUMMARIZE SELECT * FROM {table_name} LIMIT 10000").fetchall():
                if r[11] is not None:
                    null_pct = float(r[11])
                elif r[10] == 0:
                    null_pct = 100.0
                else:
                    null_pct = 0.0
                schema.append(
                    {
                        "name": r[0],
                        "type": r[1],
                        "min": str(r[2]) if r[2] is not None else None,
                        "max": str(r[3]) if r[3] is not None else None,
                        "approx_unique": r[4],
                        "null_percentage": null_pct,
                        "count": r[10],
                    }
                )
        if len(schema) == 1 and schema[0]["name"] == "timestamp":
            schema = []
        _schema_cache[cache_key] = schema
        return schema
    except Exception:
        try:
            schema = [{"name": r[0], "type": r[1]} for r in con.execute(f"DESCRIBE {table_name}").fetchall()]
            _schema_cache[cache_key] = schema
            return schema
        except Exception:
            return []


ASN_CACHE_TTL_DAYS = 30


def get_asn_names(service_id: str, asns: list) -> dict:
    asns_clean = [int(a) for a in (asns or ()) if a is not None]
    if not asns_clean or not service_id:
        return {}
    from backend.core import metadata as metadata_db

    try:
        cached = metadata_db.lookup_asn_names(service_id, asns_clean, max_age_days=ASN_CACHE_TTL_DAYS)
    except Exception:
        cached = {}
    need = [a for a in asns_clean if a not in cached]
    resolved: dict[int, str] = {}
    if need:
        try:
            import cymruwhois  # type: ignore

            c = cymruwhois.Client()
            if callable(orig_connect := getattr(c, "_connect", None)):

                def _safe_connect() -> None:
                    host, port = getattr(c, "host", None), getattr(c, "port", None)
                    if isinstance(host, str) and isinstance(port, int):
                        c.socket = sock = socket.create_connection((host, port), timeout=2.0)
                        c.file = sock.makefile("rw", encoding="utf-8", errors="replace")
                    else:
                        orig_connect()
                        sock_obj = getattr(c, "socket", None)
                        if sock_obj is not None and hasattr(sock_obj, "settimeout"):
                            sock_obj.settimeout(2.0)
                        if callable(reconf := getattr(getattr(c, "file", None), "reconfigure", None)):
                            reconf(encoding="utf-8", errors="replace")

                c._connect = _safe_connect
            for res in c.lookupmany([f"AS{asn}" for asn in need]):
                if res and res.asn:
                    asn_int = int(res.asn)
                    raw_owner = res.owner or f"AS{asn_int}"
                    resolved[asn_int] = raw_owner.split(" - ", 1)[1] if " - " in raw_owner else raw_owner
        except Exception as exc:
            logger.warning("ASN resolution failed: %s", type(exc).__name__)
        for asn in need:
            resolved.setdefault(asn, f"AS{asn}")
        if resolved:
            try:
                metadata_db.upsert_asn_names(service_id, resolved)
            except Exception:
                logger.warning("get_asn_names: metadata_db.upsert_asn_names failed (ignored)", exc_info=True)
    result = {**cached, **resolved}
    for asn in need:
        result.setdefault(asn, f"AS{asn}")
    return result


def format_asn_label(asn: int, name: str) -> str:
    return f"AS{asn}" if not name or (name.startswith("AS") and name[2:].isdigit()) else f"{name} ({asn})"


def enrich_asn_labels(values: list[dict], service_id: str) -> list[dict]:
    asn_list = [int(v["value"]) for v in values if str(v["value"]).isdigit()]
    if not asn_list:
        return values
    names_map = _db().get_asn_names(service_id, asn_list)
    for v in values:
        if str(v["value"]).isdigit():
            v["label"] = format_asn_label(int(v["value"]), names_map.get(int(v["value"]), ""))
    return values


def update_cron_duration(source: dict, run_id: int, duration_s: float, log_output: str | None = None):
    from backend.core import metadata as metadata_db

    if service_id := _source_service_id(source):
        metadata_db.update_cron_duration(service_id, run_id, duration_s, log_output=log_output)


def log_usage_calls(source: dict, calls: list[dict], process_context: str | None = None):
    if svcconfig.is_usage_logging_enabled() and (service_id := _source_service_id(source)):
        from backend.core import metadata as metadata_db

        metadata_db.log_usage_calls(service_id, calls, process_context=process_context)


def backfill_fastly_edge_writes(source: dict) -> int:
    if not svcconfig.is_usage_logging_enabled() or not (service_id := _source_service_id(source)):
        return 0
    try:
        from backend.core import metadata as metadata_db

        since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        files = metadata_db.list_unbackfilled_fastly_edge_files(service_id, since=since)
        if not files:
            return 0
        calls = [
            {
                "method": "PUT_OBJECT",
                "path": f_name,
                "service": "FOS",
                "details": "Class A · synthesized from ingest",
                "bytes": f_size,
                "status": "OK",
                "caller": "fastly.edge",
                "time_ms": 0,
                "_timestamp_override": (m.group(1) + "Z")
                if (m := re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", f_name))
                else f_ingested,
            }
            for f_name, f_ingested, _row_count, f_size in files
            if f_name != "__seeding_attempted__"
        ]
        return metadata_db.log_synthetic_usage(service_id, calls)
    except Exception as e:
        logger.debug("[usage_log] Fastly-edge write backfill failed: %s", e)
        return 0


def reconcile_fastly_stats(source: dict, hours_back: int = 12) -> int:
    if not svcconfig.is_usage_logging_enabled():
        return 0
    service_id, logging_svc_id = _source_service_id(source), source.get("logging_service_id", "")
    if not service_id or not logging_svc_id or not (api_key := svcconfig.get_fastly_api_key(logging_svc_id)):
        return 0
    try:
        from backend.core import metadata as metadata_db
        from backend.core.fastly.client import StatsResponse, fastly

        now_dt = datetime.now(UTC)
        if latest_recon := metadata_db.get_latest_reconciliation_ts(service_id):
            latest_dt = parse_iso_utc(latest_recon) if isinstance(latest_recon, str) else None
            if latest_dt is not None and (now_dt - latest_dt) < timedelta(hours=1):
                return 0
        now = now_dt.replace(minute=0, second=0, microsecond=0)
        from_ts, to_ts = (
            int((now - timedelta(hours=hours_back)).timestamp()),
            int((now + timedelta(hours=1)).timestamp()),
        )
        payload = fastly(
            "GET", f"/stats/aggregate?by=hour&from={from_ts}&to={to_ts}", token=api_key, max_retries=0, timeout=30
        )
        hourly = [
            {
                "hour_iso": datetime.fromtimestamp(rec.start_time, tz=UTC).strftime("%Y-%m-%dT%H:00:00Z"),
                "class_a": rec.fos_ops()[0],
                "class_b": rec.fos_ops()[1],
            }
            for rec in StatsResponse.model_validate(payload).data
            if rec.start_time is not None
        ]
        return metadata_db.reconcile_fastly_stats(service_id, hourly)
    except Exception as e:
        logger.debug("[usage_log] Fastly stats reconciliation failed: %s", e)
        return 0


def purge_usage_log(source: dict):
    retention_days = int(svcconfig.load_usage_logging_config().get("retention_days", 30))
    if service_id := _source_service_id(source):
        from backend.core import metadata as metadata_db

        metadata_db.purge_usage_log(service_id, retention_days)


_CacheRegistry.register("core._duckdb_status._schema_cache", _schema_cache)
_CacheRegistry.register("core._duckdb_status._data_stats_cache", _data_stats_cache)
_CacheRegistry.register("core._duckdb_status._top_values_cache", _top_values_cache)


class _DucklakeStatsCacheAdapter:
    def clear(self) -> None:
        clear_ducklake_stats_cache()


_CacheRegistry.register("duckdb._ducklake_stats_cache", _DucklakeStatsCacheAdapter())
