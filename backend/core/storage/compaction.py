"""Local hourly/daily/weekly Parquet compaction and DuckLake maintenance (optimize, retention, snapshot expiry)."""

from __future__ import annotations

import contextlib
import logging
import os
import re
import shutil
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from backend.core.duckdb import escape_sql_literal, get_memory_connection
from backend.core.metadata.usage_log import record_cloud_copy_deletion
from backend.core.request_telemetry import InstrumentedDuckDBConnection
from backend.core.storage.catalog import (
    _CLOUD_WRITE_LOCK_TIMEOUT_S,
    _ICE,
    _RUM_BEACON_TABLES,
    RUM_NATURAL_KEYS,
    _ducklake_write_connection,
    _get_service_lock,
    _lake_columns,
    _lake_ident,
    _prune_empty_dirs,
    _resolve_catalog_dsn,
    _resolve_data_path,
    clear_source_caches,
    ducklake_table_name,
    ducklake_write_admission,
)
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict

logger = logging.getLogger("backend.core.storage._core")


def optimize_table(
    source: dict,
    target_file_size_mb: int = 128,
    min_files_per_partition: int | None = None,
    table_name: str | None = None,
) -> dict:
    """Compact small DuckLake data files into larger ones (flush inlined data, merge, rewrite)."""
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s optimize_table: service lock held (reset or another writer in progress) — skipping this cycle",
            _ICE,
        )
        return {"error": "service busy (reset or another writer in progress)", "files_rewritten": 0, "files_added": 0}
    try:
        return _optimize_table_impl(source, target_file_size_mb, min_files_per_partition, table_name=table_name)
    finally:
        lock.release()


def _optimize_table_impl(
    source: dict,
    target_file_size_mb: int = 128,
    min_files_per_partition: int | None = None,
    table_name: str | None = None,
) -> dict:
    try:
        with _ducklake_write_connection(source) as con:
            # Flush inlined rows first so merge/rewrite see materialized parquet files.
            con.execute("CALL ducklake_flush_inlined_data('lake')").fetchall()
            sid = source.get("service_id") or source.get("name", "default")
            logger.info("[ducklake] %s: ducklake_flush_inlined_data executed successfully", sid)

            candidate_tables = (
                [table_name]
                if table_name and table_name not in ("logs", "all", "*")
                else ["logs", "client_vitals", "client_errors"]
            )
            op_rows, partition_errors, tables_processed = [], [], []

            for t_name in candidate_tables:
                tbl = ducklake_table_name(source, table_name=t_name)
                if not _lake_columns(con, tbl):
                    continue
                tables_processed.append(t_name)
                for fn_name, label in (
                    ("ducklake_merge_adjacent_files", "merge"),
                    ("ducklake_rewrite_data_files", "rewrite"),
                ):
                    try:
                        op_rows.extend(con.execute(f"CALL {fn_name}('lake', '{tbl}')").fetchall())
                        logger.info("[ducklake] %s: %s executed successfully for table %s", sid, fn_name, tbl)
                    except Exception as exc:
                        logger.warning("%s optimize_table: %s failed on %s: %s", sid, label, tbl, exc)
                        partition_errors.append(f"{t_name} {label}: {exc}")

            logger.info("[ducklake] %s: ducklake_rewrite_data_files executed successfully", sid)
            return {
                "files_rewritten": sum(int(r[2]) for r in op_rows if len(r) >= 4),
                "files_added": sum(int(r[3]) for r in op_rows if len(r) >= 4),
                "eligible_partitions": len(tables_processed) or 1,
                "partition_errors": partition_errors,
            }
    except Exception as e:
        return {"error": str(e), "files_rewritten": 0, "files_added": 0}


def run_cloud_maintenance(source: dict) -> dict:
    """Run weekly maintenance: expire old metadata, delete old data, and purge old local cache."""
    sid = source.get("service_id") or source.get("name", "default")
    lock = _get_service_lock(source.get("name", "default"))
    if not lock.acquire(timeout=_CLOUD_WRITE_LOCK_TIMEOUT_S):
        logger.warning(
            "%s run_cloud_maintenance: service lock held (reset or another writer in progress) — skipping this cycle",
            _ICE,
        )
        return {"error": "service busy (reset or another writer in progress)"}
    try:
        with ducklake_write_admission(sid):
            return _run_cloud_maintenance_impl(source)
    finally:
        lock.release()


def _lake_delete_before(con, table: str, cutoff: datetime, extra_predicate: str = "") -> int:
    where = f"timestamp < ? AND {extra_predicate}" if extra_predicate else "timestamp < ?"
    row = con.execute(f'DELETE FROM lake."{_lake_ident(table)}" WHERE {where}', [cutoff]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ducklake_retention_delete(con, source: dict, data_retention_days: int, rum_retention_days: int) -> dict:
    """Step 1: enforce ``data_retention_days`` / ``rum_retention_days`` (0 means keep forever)."""
    out: dict[str, Any] = {}
    now = datetime.now(UTC)
    logs_table = ducklake_table_name(source, table_name="logs")
    deleted_anything = False

    if data_retention_days > 0:
        cutoff = now - timedelta(days=data_retention_days)
        predicate = (
            "rum_cid IS NULL"
            if rum_retention_days > data_retention_days and "rum_cid" in _lake_columns(con, logs_table)
            else ""
        )
        out["data_deleted_before_days"] = data_retention_days
        out["data_rows_deleted"] = _lake_delete_before(con, logs_table, cutoff, predicate)
        deleted_anything = True
        if rum_retention_days > 0:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_log_rows_deleted"] = _lake_delete_before(con, logs_table, now - timedelta(days=rum_retention_days))

    if rum_retention_days > 0:
        rum_cutoff = now - timedelta(days=rum_retention_days)
        beacon_rows, beacon_tables = 0, 0
        for t_name in _RUM_BEACON_TABLES:
            tbl = ducklake_table_name(source, table_name=t_name)
            if not _lake_columns(con, tbl):
                continue
            beacon_rows += _lake_delete_before(con, tbl, rum_cutoff)
            beacon_tables += 1
        if beacon_tables:
            out["rum_deleted_before_days"] = rum_retention_days
            out["rum_beacon_rows_deleted"] = beacon_rows
            deleted_anything = True

    if deleted_anything:
        clear_source_caches(source.get("name", "default"))
    return out


def _remote_data_path(source: dict) -> bool:
    return _resolve_data_path(source, source.get("service_id") or source.get("name", "default")).startswith("s3://")


def _fos_object_size(source: dict, bucket: str, key: str) -> int:
    from backend.core.duckdb import _get_fos_client

    try:
        return int(_get_fos_client(source).head_object(Bucket=bucket, Key=key)["ContentLength"])
    except Exception:
        return 0


def _scheduled_file_sizes(source: dict, cutoff: datetime) -> dict[str, int]:
    """Read sizes before cleanup unlinks this service's scheduled FOS files."""
    dsn = _resolve_catalog_dsn(source.get("service_id") or source.get("name"))
    if not dsn:
        return {}
    root = _resolve_data_path(source, source.get("service_id") or source.get("name", "default"))
    bucket, _, key = root[len("s3://") :].partition("/")
    base = f"{key.rstrip('/')}/" if key.strip("/") else ""
    patterns = [f"main/{ducklake_table_name(source, t)}/%" for t in ("logs", "client_vitals", "client_errors")]
    with psycopg.connect(dsn.removeprefix("postgres:")) as pg:
        rows = pg.execute(
            "SELECT path FROM ducklake_files_scheduled_for_deletion WHERE path_is_relative AND schedule_start < %s "
            "AND path LIKE ANY(%s)",
            (cutoff, patterns),
        ).fetchall()
    return {os.path.basename(str(r[0])): _fos_object_size(source, bucket, base + str(r[0])) for r in rows}


def _ducklake_expire_snapshots(con, source: dict, keep_snapshot_days: int) -> dict:
    """Step 2: prune DuckLake catalog snapshots older than ``keep_snapshot_days``."""
    out: dict[str, Any] = {}
    cutoff = datetime.now(UTC) - timedelta(days=keep_snapshot_days)
    snapshot_errors: list[str] = []
    snapshots_before: int | None = None
    snapshots_after: int | None = None

    def _count_snapshots(phase: str) -> int | None:
        try:
            row = con.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()
            return int(row[0]) if row else 0
        except Exception as e:
            snapshot_errors.append(str(e))
            logger.warning("[ducklake] %s: could not count snapshots %s expiry: %s", source.get("name"), phase, e)
            return None

    snapshots_before = _count_snapshots("before")
    if snapshots_before is not None:
        out["snapshots_before"] = snapshots_before
    try:
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff]).fetchall()
    except Exception as e:
        snapshot_errors.append(str(e))
        logger.warning("[ducklake] %s: snapshot expiry failed: %s", source.get("name"), e)
    snapshots_after = _count_snapshots("after")
    if snapshots_after is not None:
        out["snapshots_after"] = snapshots_after

    if snapshots_before is not None and snapshots_after is not None:
        out["snapshots_expired_before_days"] = keep_snapshot_days
        out["snapshots_expired_count"] = max(0, snapshots_before - snapshots_after)
        if snapshots_before == 0:
            out["snapshot_expiry_note"] = "no snapshots present in the DuckLake catalog"
            logger.info("[ducklake] %s: no snapshots present to expire", source.get("name"))
    if snapshot_errors:
        out["snapshot_expiry_error"] = "; ".join(snapshot_errors)

    billing_id = str(source.get("service_id") or source.get("name"))
    sizes: dict[str, int] = {}
    if _remote_data_path(source):
        # FOS's minimum billing period continues after deletion.
        try:
            sizes = _scheduled_file_sizes(source, cutoff)
        except Exception:
            logger.debug("[ducklake] %s: could not size files for billing", source.get("name"), exc_info=True)
    try:
        cleaned = con.execute("CALL ducklake_cleanup_old_files('lake', older_than => ?)", [cutoff]).fetchall()
        out["data_files_cleaned"] = len(cleaned)
        if sizes and cleaned:
            try:
                names = [os.path.basename(str(r[0])) for r in cleaned]
                billed = sum(sizes.get(n, 0) for n in names)
                record_cloud_copy_deletion(billing_id, billed, len(names))
                out["fos_deleted_bytes_billed"] = billed
            except Exception:
                logger.debug("[ducklake] %s: could not bill deleted files", source.get("name"), exc_info=True)
    except Exception as e:
        logger.warning("[ducklake] %s: old-file cleanup after expiry failed: %s", source.get("name"), e)
        out["data_file_cleanup_error"] = str(e)

    snapshots_expired = out.get("snapshots_expired_count", 0)
    if snapshots_expired > 0 or out.get("data_files_cleaned", 0):
        out["snapshot_expiry_note"] = (
            "catalog metadata entries only; parquet is not deleted by the expiry itself — "
            f"ducklake_cleanup_old_files (older_than={keep_snapshot_days}d) unlinked "
            f"{out.get('data_files_cleaned', 'unknown')} file(s), and rows deleted "
            "by retention persist physically until the daily ducklake_rewrite_data_files rewrite"
        )
        logger.info(
            "[ducklake] %s: expired %d snapshots (%s -> %s), unlinked %s file(s)",
            source.get("name"),
            snapshots_expired,
            snapshots_before,
            snapshots_after,
            out.get("data_files_cleaned", "unknown"),
        )
    return out


def _run_ducklake_maintenance(
    source: dict, *, data_retention_days: int, rum_retention_days: int, keep_snapshot_days: int
) -> dict:
    """Run retention delete and snapshot expiry on one read-write ``lake`` attach."""
    out: dict[str, Any] = {}
    try:
        with _ducklake_write_connection(source) as raw_con:
            con = InstrumentedDuckDBConnection(
                raw_con, service_id=source.get("service_id") or source.get("name", "default")
            )
            if data_retention_days > 0 or rum_retention_days > 0:
                try:
                    out.update(_ducklake_retention_delete(con, source, data_retention_days, rum_retention_days))
                except Exception as e:
                    logger.warning("[ducklake] Data deletion skipped: %s", e)
                    out["data_deletion_error"] = str(e)
            try:
                out.update(_ducklake_expire_snapshots(con, source, keep_snapshot_days))
            except Exception as e:
                logger.warning("[ducklake] Snapshot expiry skipped: %s", e)
                out["snapshot_expiry_error"] = str(e)
    except Exception as e:
        logger.warning("[ducklake] %s: maintenance could not open the lake: %s", source.get("name"), e)
        return {"data_deletion_error": str(e), "snapshot_expiry_error": str(e)}
    return out


def _run_cloud_maintenance_impl(source: dict) -> dict:
    """Run retention deletion, snapshot expiry, and independent local purges."""
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(source.get("service_id") or source.get("name")) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        data_retention_days = int(cron_sync.get("data_retention_days", 30))
        rum_retention_days = int(cron_sync.get("rum_retention_days", data_retention_days))
        cache_retention_days = int(cron_sync.get("cache_retention_days", 90))
        keep_snapshot_days = max(1, int(cron_sync.get("keep_snapshot_days", 7)))
    except Exception as e:
        return {"error": str(e)}

    results: dict[str, Any] = {
        "local_cache_files_deleted": 0,
        "local_temp_files_deleted": 0,
        "local_rollup_files_deleted": 0,
    }
    results.update(
        _run_ducklake_maintenance(
            source,
            data_retention_days=data_retention_days,
            rum_retention_days=rum_retention_days,
            keep_snapshot_days=keep_snapshot_days,
        )
    )

    def _purge_old_files(
        dir_path: str,
        suffixes: str | tuple[str, ...],
        cutoff: datetime,
        errors: list[str],
        label: str,
        prune: bool = False,
    ) -> int:
        if not os.path.exists(dir_path):
            return 0
        deleted = 0

        def _on_walk_err(error: OSError) -> None:
            errors.append(str(error))
            logger.warning("[storage] Local %s scan failed: %s", label, error)

        for root, _, files in os.walk(dir_path, onerror=_on_walk_err):
            for file in files:
                if not file.endswith(suffixes):
                    continue
                filepath = os.path.join(root, file)
                try:
                    if datetime.fromtimestamp(os.path.getmtime(filepath), tz=UTC) < cutoff:
                        os.remove(filepath)
                        deleted += 1
                except Exception as e:
                    errors.append(f"{filepath}: {e}")
                    logger.warning("[storage] Could not remove expired %s %s: %s", label, filepath, e)
        if prune:
            _prune_empty_dirs(dir_path)
        return deleted

    cache_errors: list[str] = []
    try:
        from backend.core.duckdb import _cache_dir

        cache_root = _cache_dir(source)
        candidate_dirs = [os.path.join(cache_root, "data"), os.path.join(cache_root, "buffer")] + [
            os.path.join(cache_root, f"data_{t_name}") for t_name in _RUM_BEACON_TABLES
        ]
        now_utc = datetime.now(UTC)
        if cache_retention_days > 0:
            cache_cutoff = now_utc - timedelta(days=cache_retention_days)
            for c_dir in candidate_dirs:
                results["local_cache_files_deleted"] += _purge_old_files(
                    c_dir, ".parquet", cache_cutoff, cache_errors, "cache file", prune=True
                )
        results["local_temp_files_deleted"] += _purge_old_files(
            cache_root, (".tmp", ".part", ".bad.jsonl.tmp"), now_utc - timedelta(hours=2), cache_errors, "temp file"
        )
    except Exception as e:
        cache_errors.append(str(e))
        logger.warning("[storage] Local cache cleanup failed: %s", e)
    if cache_errors:
        results["local_cache_error"] = "; ".join(cache_errors)

    rollup_retention_months = int(cron_sync.get("rollup_retention_months") or 12)
    if rollup_retention_months > 0:
        try:
            from backend.core.duckdb import _cache_dir

            rollup_errors: list[str] = []
            results["local_rollup_files_deleted"] += _purge_old_files(
                os.path.join(_cache_dir(source), "rollups"),
                ".parquet",
                datetime.now(UTC) - timedelta(days=rollup_retention_months * 30),
                rollup_errors,
                "rollup",
                prune=True,
            )
            if rollup_errors:
                results["local_rollup_error"] = "; ".join(rollup_errors)
        except Exception as e:
            logger.warning("[storage] Local rollup cleanup skipped: %s", e)
            results["local_rollup_error"] = str(e)

    results["retention_deleted_rows"] = sum(
        int(results.get(key, 0) or 0)
        for key in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
    )
    results["snapshots_expired"] = int(results.get("snapshots_expired_count", 0) or 0)
    results["files_unlinked"] = int(results.get("data_files_cleaned", 0) or 0)
    return results


# ── Local tiered Parquet compaction (folded from local_compaction.py) ──

_MAX_PARTITION_BYTES = int(os.environ.get("LOCAL_COMPACT_MAX_PARTITION_MB", "256")) * 1024 * 1024
_DAILY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_DAILY_TIER_DAYS", "1"))
_WEEKLY_TIER_AGE_DAYS = int(os.environ.get("LOCAL_COMPACT_WEEKLY_TIER_DAYS", "30"))

_DAILY_DIR = "daily"
_WEEKLY_DIR = "weekly"

_HOUR_PART_RE = re.compile(r"^timestamp_hour=(\d{4}-\d{2}-\d{2})-(\d{2})$")
_DAILY_FILE_RE = re.compile(r"^daily_(\d{4}-\d{2}-\d{2})_[0-9a-f]+\.parquet$")


def _detect_natural_keys(col_names: set[str]) -> tuple[str, ...] | None:
    """Detect if the given parquet columns match known RUM composite natural keys."""
    for natural_keys in RUM_NATURAL_KEYS.values():
        if set(natural_keys).issubset(col_names):
            return natural_keys
    return None


def _build_merge_select_sql(
    paths_sql: str,
    cols_to_strip: list[str],
    has_rid: bool = False,
    natural_keys: tuple[str, ...] | list[str] | None = None,
) -> str:
    """SELECT clause for the COPY that produces a merged parquet."""
    exclude_clause = f" EXCLUDE ({', '.join(cols_to_strip)})" if cols_to_strip else ""
    if has_rid:
        inner_exclude_clause = f" EXCLUDE ({', '.join([*cols_to_strip, '_dup_rn'])})"
        return (
            f"SELECT *{inner_exclude_clause} FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY timestamp) AS _dup_rn"
            f"  FROM read_parquet([{paths_sql}], union_by_name=true)"
            f"  WHERE rid IS NOT NULL"
            f") WHERE _dup_rn = 1"
            f" UNION ALL BY NAME "
            f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"
            f" WHERE rid IS NULL"
        )
    if natural_keys:
        inner_exclude_clause = f" EXCLUDE ({', '.join([*cols_to_strip, '_dup_rn'])})"
        partition_expr = ", ".join(f'"{k}"' for k in natural_keys)
        return (
            f"SELECT *{inner_exclude_clause} FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_expr} ORDER BY timestamp) AS _dup_rn"
            f"  FROM read_parquet([{paths_sql}], union_by_name=true)"
            f") WHERE _dup_rn = 1"
        )
    return f"SELECT *{exclude_clause} FROM read_parquet([{paths_sql}], union_by_name=true)"


def _bin_pack_files(file_paths: list[str], max_bin_size_bytes: int) -> list[list[str]]:
    """Group file_paths into bins such that the sum of file sizes in each bin
    does not exceed max_bin_size_bytes. Preserves the original file order.
    If any single file exceeds max_bin_size_bytes, it goes in its own bin.
    """
    bins: list[list[str]] = []
    current_bin: list[str] = []
    current_size = 0

    for path in file_paths:
        try:
            file_size = os.path.getsize(path)
        except OSError:
            continue

        if current_size + file_size > max_bin_size_bytes:
            if current_bin:
                bins.append(current_bin)
                current_bin = []
                current_size = 0

            if file_size >= max_bin_size_bytes:
                bins.append([path])
            else:
                current_bin.append(path)
                current_size = file_size
        else:
            current_bin.append(path)
            current_size += file_size

    if current_bin:
        bins.append(current_bin)

    return bins


def compact_local_partitions(
    source: dict,
    min_files_per_partition: int = 1,
    dry_run: bool = False,
    table_name: str = "logs",
    compact_active_hour: bool = False,
) -> dict[str, Any]:
    """Merge small parquet files within each hour-partition directory into
    a single larger file. Additionally rolls partitions older than
    ``_DAILY_TIER_AGE_DAYS`` into per-day merged files.
    """
    from backend.core.duckdb import _cache_dir

    t0 = time.time()
    cache_root = _cache_dir(source)
    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    result: dict[str, Any] = {
        "partitions_scanned": 0,
        "partitions_compacted": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "daily_rollups": 0,
        "weekly_rollups": 0,
        "active_hour_skipped": False,
        "stale_tmp_removed": 0,
        "errors": [],
        "duration_ms": 0,
        "dry_run": dry_run,
    }

    if not os.path.isdir(data_dir):
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    service_key = source.get("name", "default")
    publish_lock = _get_service_lock(service_key)

    removed_basenames: list[str] = []

    if not dry_run:
        with publish_lock:
            result["stale_tmp_removed"] = _cleanup_stale_tmp(data_dir)
            result["watermark_evicted"] = _enforce_disk_watermark(data_dir, removed_basenames=removed_basenames)

    active_hour = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    result["active_hour_skipped"] = (
        os.path.isdir(os.path.join(data_dir, active_hour)) if not compact_active_hour else False
    )

    for entry in sorted(os.listdir(data_dir)):
        if entry == active_hour and not compact_active_hour:
            continue
        if entry in (_DAILY_DIR, _WEEKLY_DIR):
            continue
        part_dir = os.path.join(data_dir, entry)
        if not os.path.isdir(part_dir):
            continue
        parquets = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
        if len(parquets) <= min_files_per_partition:
            continue

        parquets_sorted = sorted(parquets)
        full_paths = [os.path.join(part_dir, f) for f in parquets_sorted]
        bins = _bin_pack_files(full_paths, _MAX_PARTITION_BYTES)

        if min_files_per_partition == 0:
            eligible_bins = bins
        else:
            eligible_bins = [b for b in bins if len(b) > 1]
        if not eligible_bins:
            continue

        result["partitions_scanned"] += 1
        partition_compacted = False

        for bin_paths in eligible_bins:
            bin_basenames = [os.path.basename(p) for p in bin_paths]
            try:
                r = _compact_single_partition(part_dir, bin_basenames, dry_run=dry_run, publish_lock=publish_lock)
                if r.get("skipped_enospc"):
                    result["errors"].append(f"{part_dir}: ENOSPC pre-check failed (< 2x needed free disk)")
                    continue
                partition_compacted = True
                result["files_merged"] += r["files_merged"]
                result["files_removed"] += r["files_removed"]
                result["bytes_before"] += r["bytes_before"]
                result["bytes_after"] += r["bytes_after"]
                removed_basenames.extend(r.get("removed_basenames", []))
            except Exception as e:
                msg = f"{part_dir} (bin): {type(e).__name__}: {e}"
                logger.warning("[local-compact] %s", msg)
                result["errors"].append(msg)

        if partition_compacted:
            result["partitions_compacted"] += 1

    try:
        r = _compact_daily_tier(data_dir, dry_run=dry_run, publish_lock=publish_lock)
        result["daily_rollups"] = r["daily_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"daily-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    try:
        r = _compact_weekly_tier(data_dir, dry_run=dry_run, publish_lock=publish_lock)
        result["weekly_rollups"] = r["weekly_rollups"]
        result["files_merged"] += r["files_merged"]
        result["files_removed"] += r["files_removed"]
        result["bytes_before"] += r["bytes_before"]
        result["bytes_after"] += r["bytes_after"]
        removed_basenames.extend(r.get("removed_basenames", []))
    except Exception as e:
        msg = f"weekly-tier: {type(e).__name__}: {e}"
        logger.warning("[local-compact] %s", msg)
        result["errors"].append(msg)

    result["duration_ms"] = int((time.time() - t0) * 1000)
    logger.info(
        "🧹 [local-compact] %s: hourly=%d/%d merged=%d removed=%d daily=%d weekly=%d tmp_cleaned=%d in %dms",
        source.get("name"),
        result["partitions_compacted"],
        result["partitions_scanned"],
        result["files_merged"],
        result["files_removed"],
        result["daily_rollups"],
        result["weekly_rollups"],
        result["stale_tmp_removed"],
        result["duration_ms"],
    )
    return result


def _enforce_disk_watermark(
    data_dir: str,
    threshold_pct: float = 0.85,
    target_pct: float = 0.70,
    *,
    removed_basenames: list[str] | None = None,
) -> int:
    """Check disk pressure and aggressively evict oldest local parquet if exceeded."""
    try:
        usage = shutil.disk_usage(data_dir)
    except OSError:
        return 0

    current_pct = usage.used / max(usage.total, 1)
    if current_pct <= threshold_pct:
        return 0

    target_used_bytes = usage.total * target_pct
    bytes_to_free = usage.used - target_used_bytes

    if bytes_to_free <= 0:
        return 0

    logger.warning(
        "Disk pressure hit %.1f%% (threshold %.1f%%). Evicting oldest cache to reach %.1f%%...",
        current_pct * 100,
        threshold_pct * 100,
        target_pct * 100,
    )

    files_with_mtime = []
    for root, _, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet"):
                path = os.path.join(root, f)
                try:
                    stat = os.stat(path)
                    files_with_mtime.append((stat.st_mtime, stat.st_size, path))
                except OSError:
                    pass

    files_with_mtime.sort(key=lambda x: x[0])

    freed = 0
    removed_count = 0
    for _mtime, size, path in files_with_mtime:
        if freed >= bytes_to_free:
            break
        try:
            os.remove(path)
            freed += size
            removed_count += 1
            if removed_basenames is not None:
                removed_basenames.append(os.path.basename(path))
        except OSError:
            pass

    if removed_count > 0:
        logger.warning(
            "Evicted %d old cache files (%.2f MB) to relieve disk pressure.", removed_count, freed / (1024 * 1024)
        )

    return removed_count


def _cleanup_stale_tmp(data_dir: str) -> int:
    """Walk data_dir and rm any *.parquet.tmp left over from crashed runs."""
    n = 0
    for root, _, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet.tmp") or (f.startswith(".tmp_") and f.endswith(".parquet")):
                try:
                    os.remove(os.path.join(root, f))
                    n += 1
                except OSError:
                    pass
    return n


def _rollup_bins(
    bins: list[list[str]],
    out_root: str,
    name_prefix: str,
    token: str,
    rollup_key: str,
    result: dict[str, Any],
    *,
    dry_run: bool,
    publish_lock: Any = None,
    log_migrate,
    log_migrate_fail,
    log_merge,
    log_merge_fail,
) -> None:
    """Roll up each pre-packed bin into one output parquet under ``out_root``."""
    lock_ctx = publish_lock if publish_lock is not None else contextlib.nullcontext()
    for bin_paths in bins:
        bytes_before = sum(os.path.getsize(p) for p in bin_paths)
        if dry_run:
            result[rollup_key] += 1
            result["files_merged"] += len(bin_paths)
            result["bytes_before"] += bytes_before
            continue

        try:
            free_bytes = shutil.disk_usage(out_root).free
            if free_bytes < 2 * bytes_before:
                logger.warning(
                    "[local-compact] Insufficient disk space in %s for %s: %d bytes free < 2x needed (%d bytes). Skipping.",
                    out_root,
                    rollup_key,
                    free_bytes,
                    2 * bytes_before,
                )
                continue
        except OSError:
            pass

        if len(bin_paths) == 1:
            old_path = bin_paths[0]
            old_name = os.path.basename(old_path)
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            out_path = os.path.join(out_root, out_name)
            try:
                with lock_ctx:
                    os.rename(old_path, out_path)
                result["files_removed"] += 1
                result.setdefault("removed_basenames", []).append(old_name)
                result[rollup_key] += 1
                result["files_merged"] += 1
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_before
                log_migrate(old_name, out_name)
            except Exception as e:
                log_migrate_fail(old_path, e)
        else:
            out_name = f"{name_prefix}{token}_{uuid.uuid4().hex[:8]}.parquet"
            tmp_path = os.path.join(out_root, f"{out_name}.tmp")
            out_path = os.path.join(out_root, out_name)
            try:
                con = get_memory_connection()
                try:
                    paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in bin_paths)
                    probe = (
                        con.execute(
                            f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0"
                        ).description
                        or []
                    )
                    col_names = {d[0] for d in probe}
                    cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
                    has_rid = "rid" in col_names
                    natural_keys = None if has_rid else _detect_natural_keys(col_names)
                    select_sql = _build_merge_select_sql(
                        paths_sql, cols_to_strip, has_rid=has_rid, natural_keys=natural_keys
                    )
                    order_by = "ORDER BY timestamp"
                    if "ip" in col_names:
                        order_by += ", ip"
                    con.execute(
                        f"COPY ({select_sql} {order_by}) "
                        f"TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                finally:
                    con.close()

                with lock_ctx:
                    os.rename(tmp_path, out_path)
                    bytes_after = os.path.getsize(out_path)

                    for p in bin_paths:
                        try:
                            os.remove(p)
                            result["files_removed"] += 1
                            result.setdefault("removed_basenames", []).append(os.path.basename(p))
                        except OSError as e:
                            logger.warning("[local-compact] failed to remove %s: %s", p, e)

                result[rollup_key] += 1
                result["files_merged"] += len(bin_paths)
                result["bytes_before"] += bytes_before
                result["bytes_after"] += bytes_after
                log_merge(token, len(bin_paths))
            except Exception as e:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    pass
                log_merge_fail(token, e)


def _compact_daily_tier(data_dir: str, dry_run: bool = False, *, publish_lock: Any = None) -> dict[str, Any]:
    """Group hour-partitions older than _DAILY_TIER_AGE_DAYS by day, merge
    each day's parquets into size-capped daily files under data/daily/, and
    remove the now-empty hour partition dirs.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=_DAILY_TIER_AGE_DAYS)).date()
    daily_root = os.path.join(data_dir, _DAILY_DIR)

    by_day: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    for entry in os.listdir(data_dir):
        m = _HOUR_PART_RE.fullmatch(entry)
        if not m:
            continue
        day_str = m.group(1)
        try:
            day = parse_day_key_strict(day_str).date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        part_dir = os.path.join(data_dir, entry)
        parquets = [
            os.path.join(part_dir, f)
            for f in os.listdir(part_dir)
            if f.endswith(".parquet") and not f.startswith(".tmp_")
        ]
        if not parquets:
            continue
        by_day[day_str].append((part_dir, parquets))

    result: dict[str, Any] = {
        "daily_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not by_day:
        return result

    if not dry_run:
        os.makedirs(daily_root, exist_ok=True)

    for day_str, parts in by_day.items():
        all_paths: list[str] = []
        for _, paths in parts:
            all_paths.extend(paths)

        all_paths = sorted(all_paths)
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            daily_root,
            "daily_",
            day_str,
            "daily_rollups",
            result,
            dry_run=dry_run,
            publish_lock=publish_lock,
            log_migrate=lambda old, out: logger.info("🚚 [local-compact] migrated single-file bin %s to %s", old, out),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info("📦 [local-compact] daily bin rollup %s: %d files → 1", tok, n),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] daily bin rollup %s failed: %s", tok, e),
        )

        if not dry_run:
            for part_dir, _ in parts:
                try:
                    os.rmdir(part_dir)
                except OSError:
                    pass

    return result


def _compact_weekly_tier(data_dir: str, dry_run: bool = False, *, publish_lock: Any = None) -> dict[str, Any]:
    """Group daily files older than _WEEKLY_TIER_AGE_DAYS by ISO week, merge
    each week's parquets into size-capped weekly files under data/weekly/,
    and delete originals.
    """
    daily_root = os.path.join(data_dir, _DAILY_DIR)
    weekly_root = os.path.join(data_dir, _WEEKLY_DIR)
    result: dict[str, Any] = {
        "weekly_rollups": 0,
        "files_merged": 0,
        "files_removed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "removed_basenames": [],
    }
    if not os.path.isdir(daily_root):
        return result

    from datetime import date as _date

    cutoff = (datetime.now(UTC) - timedelta(days=_WEEKLY_TIER_AGE_DAYS)).date()
    by_week: dict[str, list[tuple[str, _date]]] = defaultdict(list)
    for fname in os.listdir(daily_root):
        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
            continue
        m = _DAILY_FILE_RE.fullmatch(fname)
        if not m:
            continue
        try:
            day = parse_day_key_strict(m.group(1)).date()
        except ValueError:
            continue
        if day >= cutoff:
            continue
        iso = day.isocalendar()
        week_key = f"{iso[0]:04d}-W{iso[1]:02d}"
        by_week[week_key].append((os.path.join(daily_root, fname), day))

    if not by_week:
        return result

    if not dry_run:
        os.makedirs(weekly_root, exist_ok=True)

    for week_key, items in by_week.items():
        if len(items) < 2:
            continue

        items_sorted = sorted(items, key=lambda x: x[0])
        all_paths = [p for p, _ in items_sorted]
        bins = _bin_pack_files(all_paths, _MAX_PARTITION_BYTES)

        _rollup_bins(
            bins,
            weekly_root,
            "weekly_",
            week_key,
            "weekly_rollups",
            result,
            dry_run=dry_run,
            publish_lock=publish_lock,
            log_migrate=lambda old, out: logger.info(
                "🚚 [local-compact] migrated single-file weekly bin %s to %s", old, out
            ),
            log_migrate_fail=lambda old, e: logger.warning(
                "[local-compact] failed to migrate single-file weekly bin %s: %s", old, e
            ),
            log_merge=lambda tok, n: logger.info(
                "🗓️  [local-compact] weekly bin rollup %s: %d daily file(s) → 1", tok, n
            ),
            log_merge_fail=lambda tok, e: logger.warning("[local-compact] weekly bin rollup %s failed: %s", tok, e),
        )

    return result


def _compact_single_partition(
    part_dir: str, parquets: list[str], dry_run: bool = False, *, publish_lock: Any = None
) -> dict[str, Any]:
    """Merge `parquets` (relative names) in `part_dir` into one new parquet."""
    paths = [os.path.join(part_dir, p) for p in parquets]
    bytes_before = sum(os.path.getsize(p) for p in paths)

    if dry_run:
        return {
            "files_merged": len(parquets),
            "files_removed": 0,
            "bytes_before": bytes_before,
            "bytes_after": 0,
        }

    try:
        free_bytes = shutil.disk_usage(part_dir).free
        if free_bytes < 2 * bytes_before:
            logger.warning(
                "[local-compact] Insufficient disk space in %s: %d bytes free < 2x needed (%d bytes). Skipping.",
                part_dir,
                free_bytes,
                2 * bytes_before,
            )
            return {
                "files_merged": 0,
                "files_removed": 0,
                "removed_basenames": [],
                "bytes_before": bytes_before,
                "bytes_after": 0,
                "skipped_enospc": True,
            }
    except OSError:
        pass

    out_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
    tmp_name = f"{out_name}.tmp"
    tmp_path = os.path.join(part_dir, tmp_name)
    out_path = os.path.join(part_dir, out_name)

    con = get_memory_connection()
    try:
        paths_sql = ", ".join(f"'{escape_sql_literal(p)}'" for p in paths)
        probe = con.execute(f"SELECT * FROM read_parquet([{paths_sql}], union_by_name=true) LIMIT 0").description or []
        col_names = {d[0] for d in probe}
        cols_to_strip = sorted(c for c in ("timestamp_hour", "dt") if c in col_names)
        has_rid = "rid" in col_names
        natural_keys = None if has_rid else _detect_natural_keys(col_names)
        select_sql = _build_merge_select_sql(paths_sql, cols_to_strip, has_rid=has_rid, natural_keys=natural_keys)
        order_by = "ORDER BY timestamp"
        if "ip" in col_names:
            order_by += ", ip"
        con.execute(
            f"COPY ({select_sql} {order_by}) TO '{escape_sql_literal(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()

    lock_ctx = publish_lock if publish_lock is not None else contextlib.nullcontext()
    with lock_ctx:
        try:
            os.rename(tmp_path, out_path)
            bytes_after = os.path.getsize(out_path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            raise

        files_removed = 0
        removed_basenames: list[str] = []
        for p in paths:
            try:
                os.remove(p)
                files_removed += 1
                removed_basenames.append(os.path.basename(p))
            except OSError as e:
                logger.warning("[local-compact] failed to remove %s: %s", p, e)

    return {
        "files_merged": len(parquets),
        "files_removed": files_removed,
        "removed_basenames": removed_basenames,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
    }


_COMPACTION_STATS_TTL = 5.0
_COMPACTION_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_COMPACTION_STATS_TTL)


def compaction_stats(source: dict, table_name: str = "logs") -> dict[str, Any]:
    """Snapshot of file-count distribution across local cache partitions."""
    from backend.core.duckdb import _cache_dir

    cache_root = _cache_dir(source)
    cache_key = f"{cache_root}:{table_name}"
    cached = _COMPACTION_STATS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    data_dir = os.path.join(cache_root, "data" if table_name == "logs" else f"data_{table_name}")
    total_files = 0
    partitions = 0
    above_3 = 0
    above_10 = 0
    daily_files = 0
    weekly_files = 0
    if not os.path.isdir(data_dir):
        result: dict[str, Any] = {
            "total_files": 0,
            "partitions": 0,
            "partitions_above_3": 0,
            "partitions_above_10": 0,
            "daily_files": 0,
            "weekly_files": 0,
            "avg_files_per_partition": 0.0,
        }
        _COMPACTION_STATS_CACHE[cache_key] = result
        return result
    for entry in os.listdir(data_dir):
        full = os.path.join(data_dir, entry)
        if not os.path.isdir(full):
            continue
        n = sum(1 for f in os.listdir(full) if f.endswith(".parquet") and not f.startswith(".tmp_"))
        if entry == _DAILY_DIR:
            daily_files += n
        elif entry == _WEEKLY_DIR:
            weekly_files += n
        else:
            partitions += 1
            total_files += n
            if n > 3:
                above_3 += 1
            if n > 10:
                above_10 += 1
    result = {
        "total_files": total_files + daily_files + weekly_files,
        "partitions": partitions,
        "partitions_above_3": above_3,
        "partitions_above_10": above_10,
        "daily_files": daily_files,
        "weekly_files": weekly_files,
        "avg_files_per_partition": (total_files / partitions) if partitions else 0.0,
    }
    _COMPACTION_STATS_CACHE[cache_key] = result
    return result


_CacheRegistry.register("core.local_compaction._COMPACTION_STATS_CACHE", _COMPACTION_STATS_CACHE)
