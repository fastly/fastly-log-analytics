"""Recompute / backfill / cleanup and coverage index for per-field per-hour rollups."""

from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from cachetools import TTLCache

from backend import config as svcconfig
from backend.models.dashboard import HourlyRollupCoverage, ReaderCoverage, RollupPathCoverage, ScalarRollupCoverage
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_hour_key_strict, parse_window
from backend.utils.telemetry import ensure_utc

from .. import metric_expressions
from ._common import (
    _VIRTUAL_FIELD_BACKING,
    DAY_BUNDLE_FILENAME,
    IP_SAMPLE_CAP,
    IP_SPREAD_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    SESSIONS_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _build_copy_query,
    _build_ip_spread_select_query,
    _build_virtual_field_copy_query,
    _day_bundled_root,
    _day_rollups_root,
    _get_fields,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    _publish_field_partitions,
    _rollups_root,
    _safe_table_for,
    acquire_rollup_connection_and_cols,
    backfill_missing_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
    parse_hour_token,
    quote_path_list,
    rollup_coverage_ready,
    shared_rollup_session,
)
from .day_bundles import bundle_hours, bundle_hours_ip_spread
from .origin_dims import (  # noqa: F401
    build_network_geo_bundles,
    build_network_heatmap_bundles,
    build_network_quality_bundles,
    build_network_rtt_bundles,
    build_network_speed_bundles,
    build_network_summary_bundles,
    build_origin_dims_bundles,
    build_origin_latency_ts_bundles,
    build_origin_summary_bundles,
    build_perf_dims_bundles,
    build_perf_latency_bundles,
    build_pop_health_bundles,
    build_slow_urls_bundles,
)
from .security_dims import (  # noqa: F401
    build_ngwaf_bots_bundles,
    build_ngwaf_bots_ts_bundles,
    build_security_dims_bundles,
    build_security_proxies_bundles,
    build_verified_bots_ts_bundles,
)
from .wellknown_bots import recompute_wellknown_bots_rollup  # noqa: F401

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RollupSpec:
    """A best-effort per-hour rollup writer."""

    label: str
    builder: str
    fallback: str = "raw scan"


HOUR_WRITERS: tuple[RollupSpec, ...] = (
    RollupSpec("time_series", "build_time_series_bundles"),
    RollupSpec("sessions", "build_session_bundles"),
    RollupSpec("slow_urls", "build_slow_urls_bundles"),
    RollupSpec("origin_summary", "build_origin_summary_bundles"),
    RollupSpec("network_rtt", "build_network_rtt_bundles"),
    RollupSpec("network_speed", "build_network_speed_bundles"),
    RollupSpec("network_quality", "build_network_quality_bundles"),
    RollupSpec("network_heatmap", "build_network_heatmap_bundles"),
    RollupSpec("network_geo", "build_network_geo_bundles"),
    RollupSpec("verified_bots_ts", "build_verified_bots_ts_bundles"),
    RollupSpec("perf_latency", "build_perf_latency_bundles"),
    RollupSpec("pop_health", "build_pop_health_bundles"),
    RollupSpec("origin_dims", "build_origin_dims_bundles"),
    RollupSpec("origin_latency_ts", "build_origin_latency_ts_bundles"),
    RollupSpec("security_dims", "build_security_dims_bundles"),
    RollupSpec("security_proxies", "build_security_proxies_bundles"),
    RollupSpec("perf_dims", "build_perf_dims_bundles"),
    RollupSpec("ngwaf_bots", "build_ngwaf_bots_bundles", fallback="direct join"),
    RollupSpec("ngwaf_bots_ts", "build_ngwaf_bots_ts_bundles", fallback="direct join"),
    RollupSpec("overview", "build_overview_bundles"),
    RollupSpec("network_summary", "build_network_summary_bundles"),
    RollupSpec("wellknown_bots", "recompute_wellknown_bots_rollup"),
)


@dataclass(frozen=True)
class DayCompactorSpec:
    """A closed-day compactor run by the daily ``rollup_compact`` cron and the admin backfill."""

    label: str
    compactor: str
    admin_key: str | None = None


DAY_COMPACTORS: tuple[DayCompactorSpec, ...] = (
    DayCompactorSpec("origin_summary", "compact_origin_summary_closed_days_to_daily", "origin_summary_days"),
    DayCompactorSpec("network_rtt", "compact_network_rtt_closed_days_to_daily", "network_rtt_days"),
    DayCompactorSpec("network_speed", "compact_network_speed_closed_days_to_daily", "network_speed_days"),
    DayCompactorSpec("network_quality", "compact_network_quality_closed_days_to_daily", "network_quality_days"),
    DayCompactorSpec("verified_bots_ts", "compact_verified_bots_ts_closed_days_to_daily", "verified_bots_ts_days"),
    DayCompactorSpec("perf_latency", "compact_perf_latency_closed_days_to_daily", "perf_latency_days"),
    DayCompactorSpec("origin_dims", "compact_origin_dims_closed_days_to_daily", "origin_dims_days"),
    DayCompactorSpec("origin_latency_ts", "compact_origin_latency_ts_closed_days_to_daily", "origin_latency_ts_days"),
    DayCompactorSpec("security_dims", "compact_security_dims_closed_days_to_daily", "security_dims_days"),
    DayCompactorSpec("security_proxies", "compact_security_proxies_closed_days_to_daily", "security_proxies_days"),
    DayCompactorSpec("perf_dims", "compact_perf_dims_closed_days_to_daily", "perf_ttl_dist_days"),
    DayCompactorSpec("ngwaf_bots", "compact_ngwaf_bots_closed_days_to_daily", "ngwaf_bots_days"),
    DayCompactorSpec("ngwaf_bots_ts", "compact_ngwaf_bots_ts_closed_days_to_daily", "ngwaf_bots_ts_days"),
    DayCompactorSpec("overview", "compact_overview_closed_days_to_daily", "overview_days"),
    DayCompactorSpec("time_series", "compact_time_series_closed_days_to_daily"),
    DayCompactorSpec("pop_health", "compact_pop_health_closed_days_to_daily"),
)

ADMIN_BACKFILLS: tuple[tuple[str, str], ...] = (
    ("slow_urls", "backfill_slow_urls_bundles"),
    ("origin_summary", "backfill_origin_summary_bundles"),
    ("origin_dims", "backfill_origin_dims_bundles"),
    ("origin_latency_ts", "backfill_origin_latency_ts_bundles"),
    ("network_rtt", "backfill_network_rtt_bundles"),
    ("network_speed", "backfill_network_speed_bundles"),
    ("network_quality", "backfill_network_quality_bundles"),
    ("network_heatmap", "backfill_network_heatmap_bundles"),
    ("network_geo", "backfill_network_geo_bundles"),
    ("verified_bots_ts", "backfill_verified_bots_ts_bundles"),
    ("perf_latency", "backfill_perf_latency_bundles"),
    ("pop_health", "backfill_pop_health_bundles"),
    ("security_dims", "backfill_security_dims_bundles"),
    ("security_proxies", "backfill_security_proxies_bundles"),
    ("perf_ttl_dist", "backfill_perf_dims_bundles"),
    ("wellknown_bots", "backfill_wellknown_bots_rollup"),
    ("ngwaf_bots", "backfill_ngwaf_bots_bundles"),
    ("ngwaf_bots_ts", "backfill_ngwaf_bots_ts_bundles"),
    ("overview", "backfill_overview_bundles"),
    ("network_summary", "backfill_network_summary_bundles"),
)


def recompute_touched_hours(service_id: str, source: dict, hours: set[str]) -> None:
    """Recompute rollups and per-hour bundles for all dashboard fields across *hours*."""
    if not hours:
        return

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    parsed: list[tuple[str, datetime]] = []
    for h in hours:
        if h == active_hour:
            continue
        dt = parse_hour_token(h)
        if dt is None:
            logger.warning("[rollups] skipping malformed hour token: %r", h)
            continue
        parsed.append((h, dt))
    if not parsed:
        return

    table_ident = _safe_table_for(source)
    if not table_ident:
        return

    min_start = min(dt for _, dt in parsed)
    max_end = max(dt for _, dt in parsed) + timedelta(hours=1)
    hour_list_sql = ", ".join(f"'{h}'" for h, _ in parsed)
    where_sql = (
        f"timestamp >= '{min_start.isoformat()}' "
        f"AND timestamp < '{max_end.isoformat()}' "
        f"AND strftime(timestamp, '%Y-%m-%d-%H') IN ({hour_list_sql})"
    )
    fields_to_rollup = _get_fields(source)
    touched_hours = [h for h, _ in parsed]
    with shared_rollup_session(source):
        _run_per_field_copy(service_id, source, table_ident, where_sql, fields_to_rollup)

        try:
            _run_ip_spread_per_field(service_id, source, table_ident, where_sql, fields_to_rollup)
        except Exception as e:
            logger.warning(
                "[rollups] %s: ip_spread per-field write failed (count rollup unaffected): %s",
                service_id,
                e,
            )

        try:
            bundle_hours(service_id, source, touched_hours)
        except Exception as e:
            logger.warning("[rollups] %s: hour bundling failed (per-field still serves): %s", service_id, e)

        try:
            bundle_hours_ip_spread(service_id, source, touched_hours)
        except Exception as e:
            logger.warning(
                "[rollups] %s: ip_spread hour bundling failed (per-field ip_spread still serves): %s",
                service_id,
                e,
            )

        for spec in HOUR_WRITERS:
            try:
                getattr(sys.modules[__name__], spec.builder)(service_id, source, touched_hours)
            except Exception as e:
                logger.warning(
                    "[rollups] %s: %s bundle failed (%s will serve): %s",
                    service_id,
                    spec.label,
                    spec.fallback,
                    e,
                )


def _bundle_metadata(path: str) -> pq.FileMetaData | None:
    """Return readable metadata for a valid all-fields bundle."""
    if not os.path.isfile(path):
        return None
    try:
        metadata = pq.read_metadata(path)
    except (OSError, ValueError, pa.ArrowException):
        return None
    if not {"field", "value", "count"}.issubset(metadata.schema.names):
        return None
    return metadata


def _bundle_has_rows(path: str) -> bool:
    """Return whether an all-fields bundle is a non-empty data bundle."""
    metadata = _bundle_metadata(path)
    return metadata is not None and metadata.num_rows > 0


def backfill_missing_hour_bundles(
    service_id: str,
    source: dict,
    lookback_days: int = 30,
    max_missing_hours: int | None = None,
) -> dict[str, int | bool]:
    """Self-heal pass: rebuild closed hours where the lake view has rows but no bundle exists."""
    import duckdb as _ddb

    from backend.core.storage import update_storage_view

    bundled_root = _hour_bundled_root(source)
    existing: set[str] = set()
    if os.path.isdir(bundled_root):
        try:
            for e in os.listdir(bundled_root):
                if e.startswith("hour="):
                    hour = e[len("hour=") :]
                    if _bundle_has_rows(os.path.join(bundled_root, e, "all_fields.parquet")):
                        existing.add(hour)
        except OSError:
            pass

    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    end_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=lookback_days)

    def _coverage_complete() -> bool:
        cur = start_dt
        while cur < end_dt:
            hour = cur.strftime("%Y-%m-%d-%H")
            cur += timedelta(hours=1)
            if hour >= active:
                break
            if _bundle_metadata(os.path.join(bundled_root, f"hour={hour}", "all_fields.parquet")) is None:
                return False
        return True

    con = _ddb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC'")
        from backend.core.duckdb import _configure_fos, _safe_table_name

        _configure_fos(con, source)
        update_storage_view(con, source)
        from backend import config as svcconfig

        if svcconfig.is_durable_serving_mode(source):
            lake_row = con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake' LIMIT 1").fetchone()
            if lake_row is None:
                logger.warning(
                    "[rollups] %s: backfill_missing_hour_bundles could not attach DuckLake catalog",
                    service_id,
                )
                return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}

        view_name = _safe_table_name(source.get("name") or service_id)
        view_row = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [view_name],
        ).fetchone()
        if view_row is None or not _is_safe_ident(view_row[0]):
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}
        view_name = view_row[0]
        try:
            rows = con.execute(
                f"SELECT strftime(timestamp, '%Y-%m-%d-%H') AS h, COUNT(*) AS n "
                f"FROM {view_name} WHERE timestamp >= ? AND timestamp < ? "
                f"GROUP BY 1 HAVING n > 0",
                [start_dt.isoformat(), end_dt.isoformat()],
            ).fetchall()
        except _ddb.Error as e:
            logger.warning("[rollups] %s: backfill_missing_hour_bundles view query failed: %s", service_id, e)
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}

        in_window_with_data = {h for h, _ in rows if h < active}
        candidate_existing = sorted(existing & in_window_with_data)
        if candidate_existing:
            try:
                view_cols = {
                    r[0]
                    for r in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                        [view_name],
                    ).fetchall()
                }
            except _ddb.Error:
                view_cols = set()
            required_bundle_fields = sorted(view_cols & {"ip", "status", "method", "pop"})
            if required_bundle_fields:
                bundle_paths: list[str] = []
                candidate_hours: list[str] = []
                for h in candidate_existing:
                    bp = os.path.join(bundled_root, f"hour={h}", "all_fields.parquet")
                    if os.path.isfile(bp):
                        bundle_paths.append(bp)
                        candidate_hours.append(h)
                if bundle_paths:
                    try:
                        paths_sql = quote_path_list(bundle_paths)
                        req_in = ", ".join(f"'{f}'" for f in required_bundle_fields)
                        complete_rows = con.execute(
                            f"SELECT regexp_extract(filename, 'hour=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}-[0-9]{{2}})', 1) AS h "
                            f"FROM read_parquet([{paths_sql}], filename=true, hive_partitioning=0) "
                            f"WHERE field IN ({req_in}) "
                            f"GROUP BY 1 "
                            f"HAVING COUNT(DISTINCT field) >= {len(required_bundle_fields)}"
                        ).fetchall()
                        complete_hours = {r[0] for r in complete_rows if r and r[0]}
                        incomplete_hours = set(candidate_hours) - complete_hours
                        if incomplete_hours:
                            logger.info(
                                "[rollups] %s: backfill_missing_hour_bundles found %d incomplete hour bundle(s) missing core fields %s: %s",
                                service_id,
                                len(incomplete_hours),
                                required_bundle_fields,
                                sorted(incomplete_hours)[:10],
                            )
                            for inc_h in incomplete_hours:
                                inc_path = os.path.join(bundled_root, f"hour={inc_h}", "all_fields.parquet")
                                try:
                                    os.utime(inc_path, (0, 0))
                                except OSError:
                                    pass
                            existing -= incomplete_hours
                    except _ddb.Error as e:
                        logger.debug(
                            "[rollups] %s: backfill_missing_hour_bundles completeness check skipped: %s",
                            service_id,
                            e,
                        )

        missing = sorted(h for h, _ in rows if h < active and h not in existing)
        if max_missing_hours is not None:
            if max_missing_hours < 1:
                raise ValueError("max_missing_hours must be positive")
            missing = missing[:max_missing_hours]

        with_data = {h for h, _ in rows}
        empty_hours: list[str] = []
        cur = start_dt
        while cur < end_dt:
            h = cur.strftime("%Y-%m-%d-%H")
            cur += timedelta(hours=1)
            if h >= active or h in existing or h in with_data:
                continue
            empty_hours.append(h)

        if not missing and not empty_hours:
            return {
                "missing": 0,
                "rebuilt_fields": 0,
                "bundled": 0,
                "stamped_empty": 0,
                "coverage_verified": _coverage_complete(),
            }
        if missing:
            logger.info(
                "[rollups] %s: backfill_missing_hour_bundles found %d hour(s) missing bundles: %s",
                service_id,
                len(missing),
                missing[:10] + (["…"] if len(missing) > 10 else []),
            )
    finally:
        con.close()

    from backend.core.rollups.hour_bundles import stamp_empty_hour_sentinels

    stamped = stamp_empty_hour_sentinels(service_id, source, empty_hours)
    if not missing:
        return {
            "missing": 0,
            "rebuilt_fields": 0,
            "bundled": 0,
            "stamped_empty": stamped,
            "coverage_verified": _coverage_complete(),
        }

    recompute_touched_hours(service_id, source, set(missing))

    bundled_now = 0
    missing_set = set(missing)
    if os.path.isdir(bundled_root):
        try:
            for entry in os.listdir(bundled_root):
                if entry.startswith("hour=") and entry[len("hour=") :] in missing_set:
                    if _bundle_metadata(os.path.join(bundled_root, entry, "all_fields.parquet")) is not None:
                        bundled_now += 1
        except OSError:
            pass

    return {
        "missing": len(missing),
        "rebuilt_fields": len(_get_fields(source)),
        "bundled": bundled_now,
        "stamped_empty": stamped,
        "coverage_verified": _coverage_complete(),
    }


def backfill_missing_hour_ip_spread(
    service_id: str,
    source: dict,
    lookback_days: int = 30,
) -> dict[str, int]:
    """Self-heal pass for the IP-spread bundle tree."""
    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root):
        return {"missing": 0, "rebuilt": 0}

    end_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=lookback_days)

    missing: list[str] = []
    try:
        for entry in os.listdir(bundled_root):
            if not entry.startswith("hour="):
                continue
            hour = entry[len("hour=") :]
            parsed = parse_hour_token(hour)
            if parsed is None or parsed < start_dt or parsed >= end_dt:
                continue
            if not os.path.isfile(os.path.join(bundled_root, entry, IP_SPREAD_BUNDLE_FILENAME)):
                missing.append(hour)
    except OSError:
        return {"missing": 0, "rebuilt": 0}

    if not missing:
        return {"missing": 0, "rebuilt": 0}

    logger.info(
        "[rollups] %s: ip_spread self-heal found %d hour(s) missing ip_spread bundle: %s",
        service_id,
        len(missing),
        missing[:10] + (["…"] if len(missing) > 10 else []),
    )
    recompute_touched_hours(service_id, source, set(missing))
    return {"missing": len(missing), "rebuilt": len(missing)}


def _prune_partition_dirs(
    parent_dir: str,
    cutoff_str: str,
    prefix: str,
    *,
    should_delete: Callable[[str], bool] | None = None,
) -> int:
    """Remove ``<prefix><token>`` child directories under *parent_dir* older than *cutoff_str*."""
    deleted = 0
    for entry in os.listdir(parent_dir):
        if not entry.startswith(prefix):
            continue
        token = entry[len(prefix) :]
        if should_delete(token) if should_delete is not None else token < cutoff_str:
            target_dir = os.path.join(parent_dir, entry)
            try:
                shutil.rmtree(target_dir)
                deleted += 1
            except OSError as e:
                logger.warning("[rollups] could not delete %s: %s", target_dir, e)
    return deleted


def cleanup_old_rollups(
    service_id: str,
    source: dict,
    max_age_days: int,
    *,
    hour_bundle_max_age_days: int | None = None,
) -> int:
    """Delete per-hour and per-day rollup directories older than retention limits."""
    if max_age_days <= 0:
        return 0

    cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).strftime("%Y-%m-%d-%H")
    day_cutoff = cutoff[:10]
    hour_cutoff = (
        (datetime.now(UTC) - timedelta(days=hour_bundle_max_age_days)).strftime("%Y-%m-%d-%H")
        if hour_bundle_max_age_days and hour_bundle_max_age_days > 0
        else cutoff
    )

    deleted = 0
    for root_dir, walk_label in ((_rollups_root(source), "hour"), (_ip_spread_root(source), "ip_spread")):
        if not os.path.isdir(root_dir):
            continue
        try:
            for field_entry in os.listdir(root_dir):
                if field_entry.startswith("field="):
                    try:
                        deleted += _prune_partition_dirs(os.path.join(root_dir, field_entry), hour_cutoff, "hour=")
                    except OSError:
                        continue
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (%s): %s", service_id, walk_label, e)

    day_bundled_root = _day_bundled_root(source)
    if os.path.isdir(day_bundled_root):
        try:
            deleted += _prune_partition_dirs(day_bundled_root, day_cutoff, "day=")
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (day_bundled): %s", service_id, e)

    day_root = _day_rollups_root(source)
    if os.path.isdir(day_root):
        try:
            for field_entry in os.listdir(day_root):
                if field_entry.startswith("field="):
                    try:
                        deleted += _prune_partition_dirs(
                            os.path.join(day_root, field_entry),
                            day_cutoff,
                            "day=",
                            should_delete=lambda day: (
                                os.path.isfile(os.path.join(day_bundled_root, f"day={day}", "all_fields.parquet"))
                                or day < day_cutoff
                            ),
                        )
                    except OSError:
                        continue
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (day): %s", service_id, e)

    hour_bundled_root = _hour_bundled_root(source)
    if os.path.isdir(hour_bundled_root):
        try:
            deleted += _prune_partition_dirs(
                hour_bundled_root,
                cutoff,
                "hour=",
                should_delete=lambda hour: (
                    (
                        os.path.isfile(os.path.join(day_bundled_root, f"day={hour[:10]}", "all_fields.parquet"))
                        and hour < hour_cutoff
                    )
                    or hour < cutoff
                ),
            )
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (hour_bundled): %s", service_id, e)

    return deleted


def _run_per_field_copy(
    service_id: str,
    source: dict,
    table_ident: str,
    where_sql: str,
    fields: list[str],
) -> None:
    """Shared core of recompute_touched_hours and backfill_rollups."""
    from backend.core.duckdb import _cache_dir
    from backend.core.storage import execute_with_stale_view_retry
    from backend.core.storage.view import _get_service_lock

    cache_root = _cache_dir(source)
    rollups_dir = _rollups_root(source)
    os.makedirs(rollups_dir, exist_ok=True)
    lock_key = source.get("name", "default")

    con, cols, owns_con = acquire_rollup_connection_and_cols(
        source,
        table_ident,
        logger=logger,
        log_label="could not describe",
    )
    try:
        if cols is None:
            return

        for field in fields:
            if not _is_safe_ident(field):
                logger.warning("[rollups] skipping unsafe field name: %r", field)
                continue
            backing_col = _VIRTUAL_FIELD_BACKING.get(field)
            if backing_col is not None:
                if backing_col not in cols or not _is_safe_ident(backing_col):
                    continue
            elif field not in cols:
                continue

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp", f"{field}_{uuid.uuid4().hex[:8]}")
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
            os.makedirs(tmp_field_dir, exist_ok=True)

            inner = (
                _build_virtual_field_copy_query(table_ident, field, backing_col, where_sql)
                if backing_col is not None
                else _build_copy_query(table_ident, field, where_sql)
            )
            query = (
                f"COPY ({inner}) TO '{tmp_field_dir}' "
                "(FORMAT PARQUET, PARTITION_BY (field, hour), OVERWRITE_OR_IGNORE, COMPRESSION ZSTD)"
            )
            try:
                execute_with_stale_view_retry(con, source, lambda c: c.execute(query))
            except Exception as e:  # noqa: BLE001
                logger.warning("[rollups] %s: COPY failed for field=%s: %s", service_id, field, e)
                shutil.rmtree(tmp_field_dir, ignore_errors=True)
                continue

            with _get_service_lock(lock_key):
                _publish_field_partitions(tmp_field_dir, rollups_dir, field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
    finally:
        if owns_con:
            con.close()


def _run_ip_spread_per_field(
    service_id: str,
    source: dict,
    table_ident: str,
    where_sql: str,
    fields: list[str],
) -> None:
    """Build the per-(field, hour, value) HLL IP-spread parquets."""
    from backend.core.duckdb import _cache_dir
    from backend.core.storage import execute_with_stale_view_retry
    from backend.core.storage.view import _get_service_lock
    from backend.utils.hll import HyperLogLog

    cache_root = _cache_dir(source)
    ip_spread_dir = _ip_spread_root(source)
    os.makedirs(ip_spread_dir, exist_ok=True)
    lock_key = source.get("name", "default")

    con, cols, owns_con = acquire_rollup_connection_and_cols(
        source,
        table_ident,
        logger=logger,
        log_label="ip_spread describe",
    )
    try:
        if cols is None or "ip" not in cols:
            return

        for field in fields:
            if not _is_safe_ident(field):
                logger.warning("[rollups] ip_spread: skipping unsafe field name: %r", field)
                continue
            if field in _VIRTUAL_FIELD_BACKING or field not in cols:
                continue

            select_sql = _build_ip_spread_select_query(table_ident, field, where_sql)
            try:
                arrow_table = execute_with_stale_view_retry(
                    con, source, lambda c: c.execute(select_sql).to_arrow_table()
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("[rollups] %s: ip_spread SELECT failed for field=%s: %s", service_id, field, e)
                continue

            if arrow_table.num_rows == 0:
                continue

            ip_samples = arrow_table.column("ip_sample").to_pylist()
            observed_counts = arrow_table.column("ip_count_observed").to_pylist()

            sketch_blobs: list[bytes] = []
            capped_flags: list[bool] = []
            for ip_list, observed in zip(ip_samples, observed_counts, strict=False):
                hll = HyperLogLog()
                if ip_list:
                    for ip in ip_list:
                        if ip:
                            hll.add(ip)
                sketch_blobs.append(hll.to_bytes())
                capped_flags.append(observed is not None and observed >= IP_SAMPLE_CAP)

            output = pa.table(
                {
                    "field": arrow_table.column("field"),
                    "hour": arrow_table.column("hour"),
                    "value": arrow_table.column("value"),
                    "ip_sketch": pa.array(sketch_blobs, type=pa.binary()),
                    "ip_count_observed": arrow_table.column("ip_count_observed"),
                    "sample_capped": pa.array(capped_flags, type=pa.bool_()),
                }
            )

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp_ip", f"{field}_{uuid.uuid4().hex[:8]}")
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
            os.makedirs(tmp_field_dir, exist_ok=True)
            try:
                pq.write_to_dataset(
                    output,
                    root_path=tmp_field_dir,
                    partition_cols=["field", "hour"],
                    compression="zstd",
                    existing_data_behavior="overwrite_or_ignore",
                )
            except Exception as e:
                logger.warning("[rollups] %s: ip_spread parquet write failed for field=%s: %s", service_id, field, e)
                shutil.rmtree(tmp_field_dir, ignore_errors=True)
                continue

            with _get_service_lock(lock_key):
                _publish_field_partitions(tmp_field_dir, ip_spread_dir, field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
    finally:
        if owns_con:
            con.close()


# ── Per-hour time-series, overview, and session bundle writers ──────────────


def build_time_series_bundles(
    service_id: str, source: dict, hours: list[str], *, output_root: str | None = None
) -> int:
    """Write a 1-minute time_series rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping time_series bundle", service_id, table_ident
            )
            return None

        select_parts = [
            "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
            "CAST(COUNT(*) AS BIGINT) AS requests",
            *(
                (
                    "CAST(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS BIGINT) AS status_4xx",
                    "CAST(COUNT(*) FILTER (WHERE status >= 500) AS BIGINT) AS status_5xx",
                )
                if "status" in cols
                else ("CAST(0 AS BIGINT) AS status_4xx", "CAST(0 AS BIGINT) AS status_5xx")
            ),
            *(
                (
                    "CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS hits",
                    "CAST(COUNT(*) FILTER (WHERE cache IS NOT NULL) AS BIGINT) AS cache_total",
                )
                if "cache" in cols
                else ("CAST(0 AS BIGINT) AS hits", "CAST(0 AS BIGINT) AS cache_total")
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes')} AS BIGINT) AS resp_bytes_sum"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS resp_bytes_sum"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('ttfb', zero='0.0')} AS DOUBLE) AS ttfb_sum",
                    "CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL) AS BIGINT) AS ttfb_count",
                )
                if "ttfb" in cols
                else ("CAST(0.0 AS DOUBLE) AS ttfb_sum", "CAST(0 AS BIGINT) AS ttfb_count")
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' GROUP BY 1) "
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=TIME_SERIES_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ts_",
        label="time_series",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
        output_root=output_root,
    )


_HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def build_overview_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write an overview rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping overview bundle", service_id, table_ident
            )
            return None

        has_cache = "cache" in cols
        select_parts: list[str] = [
            "CAST(COUNT(*) AS BIGINT) AS requests",
            *(
                (
                    f"CAST(COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'PASS') AS BIGINT) AS pass_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache = 'SYNTH') AS BIGINT) AS synth_requests",
                    "CAST(COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS BIGINT) AS origin_requests",
                )
                if has_cache
                else (
                    "CAST(0 AS BIGINT) AS hit_requests",
                    "CAST(0 AS BIGINT) AS miss_requests",
                    "CAST(0 AS BIGINT) AS pass_requests",
                    "CAST(0 AS BIGINT) AS synth_requests",
                    "CAST(0 AS BIGINT) AS origin_requests",
                )
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes', where=f'cache IN {_HIT_STATES}')} AS BIGINT) AS bandwidth_saved_bytes"
                if "resp_bytes" in cols and has_cache
                else "CAST(0 AS BIGINT) AS bandwidth_saved_bytes"
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('resp_bytes')} AS BIGINT) AS total_bandwidth_bytes"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS total_bandwidth_bytes"
            ),
            *(
                (
                    f"CAST(COUNT(*) FILTER (WHERE is_shield = true AND cache IN {_HIT_STATES}) AS BIGINT) AS shield_hit_requests",
                    "CAST(COUNT(*) FILTER (WHERE is_shield = true) AS BIGINT) AS shield_total_requests",
                )
                if "is_shield" in cols and has_cache
                else ("CAST(0 AS BIGINT) AS shield_hit_requests", "CAST(0 AS BIGINT) AS shield_total_requests")
            ),
            (
                "CAST(COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS BIGINT) AS threats_blocked"
                if "waf" in cols and "waf_resp" in cols
                else "CAST(0 AS BIGINT) AS threats_blocked"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('elapsed', zero='0.0', where=f'cache IN {_HIT_STATES}')} AS DOUBLE) AS hit_elapsed_sum",
                    f"CAST(COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_elapsed_count",
                    "CAST("
                    + metric_expressions.sum_or_zero("elapsed", zero="0.0", where="cache = 'MISS'")
                    + " AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(COUNT(elapsed) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_elapsed_count",
                )
                if "elapsed" in cols and has_cache
                else (
                    "CAST(0.0 AS DOUBLE) AS hit_elapsed_sum",
                    "CAST(0 AS BIGINT) AS hit_elapsed_count",
                    "CAST(0.0 AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(0 AS BIGINT) AS miss_elapsed_count",
                )
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT TIMESTAMPTZ '{start_iso}' AS hour_start, {select_sql} "
            f"FROM {table_ident} WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}') "
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        tmp_prefix=".tmp_ov_",
        label="overview",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


def backfill_overview_bundles(service_id: str, source: dict) -> int:
    """Build overview.parquet for every closed hour that has all_fields.parquet but no overview.parquet."""
    return backfill_missing_bundles(
        service_id,
        source,
        bundle_filename=OVERVIEW_BUNDLE_FILENAME,
        label="overview",
        builder=build_overview_bundles,
        logger=logger,
    )


def build_session_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ``sessions.parquet`` rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols or "ip" not in cols:
            logger.info(
                "[rollups] %s: skipping sessions bundle (timestamp=%s, ip=%s)",
                service_id,
                "timestamp" in cols,
                "ip" in cols,
            )
            return None

        ja4_expr = '"ja4"' if "ja4" in cols else "CAST(NULL AS VARCHAR)"
        select_parts = [
            "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
            'CAST("ip" AS VARCHAR) AS ip',
            f"CAST({ja4_expr} AS VARCHAR) AS ja4",
            "MIN(timestamp) AS first_ts",
            "MAX(timestamp) AS last_ts",
            "CAST(COUNT(*) AS BIGINT) AS req_count",
            'CAST(MIN("country") AS VARCHAR) AS country' if "country" in cols else "CAST(NULL AS VARCHAR) AS country",
            'CAST(MIN("asn") AS INTEGER) AS asn' if "asn" in cols else "CAST(NULL AS INTEGER) AS asn",
            *(
                (
                    'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_4xx',
                    'CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_5xx',
                )
                if "status" in cols
                else ("CAST(0 AS BIGINT) AS reqs_4xx", "CAST(0 AS BIGINT) AS reqs_5xx")
            ),
            (
                f"CAST({metric_expressions.sum_or_zero('"resp_bytes"')} AS BIGINT) AS total_bytes"
                if "resp_bytes" in cols
                else "CAST(0 AS BIGINT) AS total_bytes"
            ),
            *(
                (
                    f"CAST({metric_expressions.sum_or_zero('"tcp_rtt"', zero='0.0')} AS DOUBLE) AS rtt_sum",
                    'CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT) AS rtt_count',
                )
                if "tcp_rtt" in cols
                else ("CAST(0.0 AS DOUBLE) AS rtt_sum", "CAST(0 AS BIGINT) AS rtt_count")
            ),
            *(
                (
                    'CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT) AS edge_count',
                    'CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT) AS shield_count',
                )
                if "edge" in cols
                else ("CAST(0 AS BIGINT) AS edge_count", "CAST(0 AS BIGINT) AS shield_count")
            ),
            'CAST(MIN("ua") AS VARCHAR) AS ua_min' if "ua" in cols else "CAST(NULL AS VARCHAR) AS ua_min",
            'CAST(MAX("edge_sid") AS VARCHAR) AS edge_sid_max'
            if "edge_sid" in cols
            else "CAST(NULL AS VARCHAR) AS edge_sid_max",
            (
                'CAST(COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != \'\') AS BIGINT) AS cmcd_count'
                if "cmcd_sid" in cols
                else "CAST(0 AS BIGINT) AS cmcd_count"
            ),
        ]
        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f'AND "ip" IS NOT NULL GROUP BY 1, 2, 3) '
            f"{parquet_zstd_target(tmp_path)}"
        )

    return build_per_hour_bundles(
        service_id,
        source,
        hours,
        bundle_filename=SESSIONS_BUNDLE_FILENAME,
        tmp_prefix=".tmp_sess_",
        label="sessions",
        eligibility=eligibility,
        build_copy_sql=build_copy_sql,
        logger=logger,
    )


# ── RUM rollup writers (folded from backend.core.rollups.rum) ────────────────

_RECOMPUTE_LOCKS: dict[str, threading.Lock] = {}
_RECOMPUTE_LOCKS_GUARD = threading.Lock()


def _recompute_lock(service_id: str) -> threading.Lock:
    with _RECOMPUTE_LOCKS_GUARD:
        return _RECOMPUTE_LOCKS.setdefault(service_id, threading.Lock())


def _normalize_hour_dt(dt_val: datetime) -> datetime:
    """Normalize a datetime to an hour boundary, converting aware timestamps to UTC."""
    if getattr(dt_val, "tzinfo", None) is not None:
        dt_val = dt_val.astimezone(UTC)
    return dt_val.replace(minute=0, second=0, microsecond=0)


def table_exists(con: Any, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


def _rum_raw_latest(con: Any) -> Any:
    """MAX(timestamp) across the raw RUM beacon tables, or ``None``."""
    ts_list = []
    for tbl in ("client_vitals", "client_errors"):
        try:
            row = con.execute(f"SELECT MAX(timestamp) FROM {tbl}").fetchone()
        except Exception:
            continue
        if row and row[0]:
            ts_list.append(row[0])
    return max(ts_list) if ts_list else None


def rum_count_and_latest(con: Any) -> tuple[int, Any]:
    """Return ``(beacon_count, latest_timestamp)`` for the RUM store."""
    if table_exists(con, "rum_vitals_aggregates") and table_exists(con, "rum_error_aggregates"):
        try:
            v_res = con.execute(
                "SELECT SUM(event_count) FROM rum_vitals_aggregates "
                "WHERE dimension = 'total' AND value IN ('pageviews', 'interactions')"
            ).fetchone()
            v_cnt = v_res[0] if v_res and v_res[0] is not None else 0
            e_res = con.execute("SELECT SUM(error_count) FROM rum_error_aggregates").fetchone()
            e_cnt = e_res[0] if e_res and e_res[0] is not None else 0
            cnt = int(v_cnt) + int(e_cnt)
            if cnt > 0:
                return cnt, _rum_raw_latest(con)
        except Exception:
            logger.debug("rum_count_and_latest: step failed (ignored)", exc_info=True)

    distinct_id = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"
    cnt = 0
    for tbl in ("client_vitals", "client_errors"):
        try:
            row = con.execute(f"SELECT COUNT(DISTINCT {distinct_id}) FROM {tbl}").fetchone()
        except Exception:
            continue
        cnt += int(row[0]) if row and row[0] else 0
    return cnt, _rum_raw_latest(con)


def recompute_rum_aggregates(con: Any, service_id: str, hours: list[datetime] | list[str] | None = None) -> None:
    """Ensure RUM aggregate tables exist, find active hours, and update them idempotently."""
    with _recompute_lock(service_id):
        _recompute_rum_aggregates_locked(con, service_id, hours)


def _recompute_rum_aggregates_locked(con: Any, service_id: str, hours: list[datetime] | list[str] | None) -> None:
    logger.info("[rum_rollups] Ensuring RUM aggregate schemas exist...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_vitals_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR, value VARCHAR,
            event_count BIGINT, value_sum DOUBLE, good_count BIGINT, ni_count BIGINT,
            poor_count BIGINT, p50_value DOUBLE, p75_value DOUBLE, p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_error_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR, value VARCHAR,
            error_count BIGINT
        )
    """)

    has_vitals = table_exists(con, "client_vitals")
    has_errors = table_exists(con, "client_errors")
    if not has_vitals and not has_errors:
        logger.info("[rum_rollups] %s: No client_vitals or client_errors table found. Skipping.", service_id)
        return

    hours_set: set[datetime] = set()
    if hours is not None:
        for h in hours:
            if isinstance(h, str):
                try:
                    hours_set.add(_normalize_hour_dt(datetime.fromisoformat(h.replace("Z", "+00:00"))))
                except ValueError:
                    pass
            elif isinstance(h, datetime):
                hours_set.add(_normalize_hour_dt(h))
    else:
        for tbl, present in (("client_vitals", has_vitals), ("client_errors", has_errors)):
            if present:
                try:
                    res = con.execute(
                        f"SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM {tbl} WHERE timestamp IS NOT NULL"
                    ).fetchall()
                    for r in res:
                        if r[0]:
                            hours_set.add(_normalize_hour_dt(r[0]) if isinstance(r[0], datetime) else r[0])
                except Exception as e:
                    logger.warning("[rum_rollups] Failed to query active hours from %s: %s", tbl, e)

    if not hours_set:
        logger.info("[rum_rollups] %s: No active RUM data hours found. Skipping.", service_id)
        return

    if any(getattr(h, "tzinfo", None) is not None for h in hours_set):
        hours_set = {ensure_utc(h) for h in hours_set}

    hours_list = sorted(hours_set)
    hours_str = ", ".join(f"TIMESTAMPTZ '{h.isoformat()}'" for h in hours_list)
    logger.info(
        "[rum_rollups] %s: Recomputing aggregates for %d hours: %s",
        service_id,
        len(hours_list),
        [h.isoformat() for h in hours_list],
    )

    con.execute(
        f"DELETE FROM rum_vitals_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )
    con.execute(
        f"DELETE FROM rum_error_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )

    distinct_id = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

    if has_vitals:

        def _v_dim(
            dim: str,
            val_expr: str,
            evt_expr: str,
            *,
            group_col: str,
            val_sum: str = "0.0",
            good: str = "0",
            ni: str = "0",
            poor: str = "0",
            p50: str = "0.0",
            p75: str = "0.0",
            p99: str = "0.0",
            extra_where: str = "",
        ) -> str:
            return (
                f"SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, "
                f"'{dim}' AS dimension, {val_expr} AS value, {evt_expr} AS event_count, "
                f"{val_sum} AS value_sum, {good} AS good_count, {ni} AS ni_count, {poor} AS poor_count, "
                f"{p50} AS p50_value, {p75} AS p75_value, {p99} AS p99_value "
                f"FROM client_vitals WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}){extra_where} "
                f"GROUP BY bucket_start, {group_col}"
            )

        p75_expr = "CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)"
        vitals_selects = [
            _v_dim(
                "total",
                "CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END",
                f"COUNT(DISTINCT {distinct_id})",
                group_col="value",
            ),
            _v_dim(
                "metric_name",
                "metric_name",
                "COUNT(*)",
                group_col="metric_name",
                val_sum="CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE)",
                good="COUNT(*) FILTER (WHERE metric_rating = 'good')",
                ni="COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement')",
                poor="COUNT(*) FILTER (WHERE metric_rating = 'poor')",
                p50="CAST(MEDIAN(metric_value) AS DOUBLE)",
                p75=p75_expr,
                p99="CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            ),
            _v_dim("browser", "COALESCE(browser, 'Unknown')", f"COUNT(DISTINCT {distinct_id})", group_col="browser"),
            _v_dim("os", "COALESCE(os, 'Unknown')", f"COUNT(DISTINCT {distinct_id})", group_col="os"),
            _v_dim("device", "COALESCE(device, 'Unknown')", f"COUNT(DISTINCT {distinct_id})", group_col="device"),
            _v_dim(
                "pathname",
                "pathname",
                f"COUNT(DISTINCT {distinct_id})",
                group_col="pathname",
                extra_where=" AND pathname IS NOT NULL",
            ),
            _v_dim(
                "path_metric_load",
                "pathname",
                "COUNT(*)",
                group_col="pathname",
                val_sum="CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE)",
                extra_where=" AND pathname IS NOT NULL AND metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')",
            ),
            _v_dim(
                "path_metric_lcp",
                "pathname",
                "COUNT(*)",
                group_col="pathname",
                p75=p75_expr,
                extra_where=" AND pathname IS NOT NULL AND metric_name IN ('LCP', 'lcp')",
            ),
            _v_dim(
                "path_metric_cls",
                "pathname",
                "COUNT(*)",
                group_col="pathname",
                p75=p75_expr,
                extra_where=" AND pathname IS NOT NULL AND metric_name IN ('CLS', 'cls')",
            ),
        ]
        con.execute(
            "INSERT INTO rum_vitals_aggregates\n" + "\nUNION ALL\n".join(vitals_selects),
            [service_id] * 9,
        )

    if has_errors:
        con.execute(
            f"""
            INSERT INTO rum_error_aggregates
            SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'total' AS dimension,
                   'errors' AS value, COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) GROUP BY bucket_start
            UNION ALL
            SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'pathname' AS dimension,
                   pathname AS value, COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) AND pathname IS NOT NULL
            GROUP BY bucket_start, pathname
            UNION ALL
            SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'exception' AS dimension,
                   concat(COALESCE(error_message, ''), '|', COALESCE(error_file, ''), '|', CAST(COALESCE(error_line, 0) AS VARCHAR), '|', CAST(COALESCE(error_col, 0) AS VARCHAR)) AS value,
                   COUNT(*) AS error_count
            FROM client_errors WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) GROUP BY bucket_start, value
        """,
            [service_id] * 3,
        )

    union_parts = [
        f"SELECT timestamp, {distinct_id} AS distinct_id FROM {tbl} WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})"
        for tbl, present in (("client_vitals", has_vitals), ("client_errors", has_errors))
        if present
    ]
    con.execute(
        f"""
        INSERT INTO rum_vitals_aggregates
        SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'total' AS dimension,
               'total_beacons' AS value, COUNT(DISTINCT distinct_id) AS event_count,
               0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
               0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
        FROM ({" UNION ALL ".join(union_parts)}) GROUP BY bucket_start
        """,
        [service_id],
    )
    logger.info("[rum_rollups] %s: RUM aggregates successfully updated for %d hours", service_id, len(hours_list))


def backfill_missing_rum_aggregates(
    con: Any,
    service_id: str,
    lookback_days: int = 30,
) -> list[datetime]:
    """Detect and heal hours in client_vitals / client_errors lacking a single total_beacons row."""
    has_vitals = table_exists(con, "client_vitals")
    has_errors = table_exists(con, "client_errors")
    if not has_vitals and not has_errors:
        return []

    cutoff = datetime.now(UTC) - timedelta(days=max(1, int(lookback_days)))
    raw_parts: list[str] = []
    params: list[object] = []
    for tbl, present in (("client_vitals", has_vitals), ("client_errors", has_errors)):
        if present:
            raw_parts.append(
                f"SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM {tbl} WHERE timestamp IS NOT NULL AND timestamp >= ?"
            )
            params.append(cutoff)

    raw_union = " UNION ".join(raw_parts)
    if table_exists(con, "rum_vitals_aggregates"):
        sql = f"""
            SELECT DISTINCT r.bucket_start FROM ({raw_union}) r
            WHERE r.bucket_start IS NOT NULL
              AND (SELECT COUNT(*) FROM rum_vitals_aggregates a
                   WHERE a.service_id = ? AND a.bucket_start = r.bucket_start AND a.value = 'total_beacons') <> 1
            ORDER BY r.bucket_start ASC
        """
        rows = con.execute(sql, [*params, service_id]).fetchall()
    else:
        sql = f"SELECT DISTINCT r.bucket_start FROM ({raw_union}) r WHERE r.bucket_start IS NOT NULL ORDER BY r.bucket_start ASC"
        rows = con.execute(sql, params).fetchall()

    missing_hours: list[datetime] = []
    for r in rows:
        if r and r[0] is not None:
            dt_val = r[0]
            missing_hours.append(_normalize_hour_dt(dt_val) if isinstance(dt_val, datetime) else dt_val)

    if missing_hours:
        logger.info(
            "[rum_rollups] %s: Healing %d RUM aggregate hour(s) with a missing or duplicated total_beacons row (lookback=%dd)",
            service_id,
            len(missing_hours),
            lookback_days,
        )
        recompute_rum_aggregates(con, service_id, hours=missing_hours)

    return missing_hours


# ── Rollup partition discovery & coverage index (folded from backend.repositories.rollup_coverage) ──

ALL_FIELDS_FILENAME = "all_fields.parquet"
MIN_CLOSED_HOUR_COVERAGE = 0.5
MISSING_HOUR_HEAL_CAP = 48

LISTDIR_CACHE_TTL_S = 60.0
LISTDIR_CACHE_MAX_ENTRIES = 32768
_listdir_cache_lock = threading.Lock()
_listdir_cache: TTLCache[str, list[str]] = TTLCache(
    maxsize=LISTDIR_CACHE_MAX_ENTRIES, ttl=LISTDIR_CACHE_TTL_S, timer=time.monotonic
)
_CacheRegistry.register("repositories.rollup_coverage._listdir_cache", _listdir_cache)


def cached_listdir(path: str) -> list[str]:
    """Return ``os.listdir(path)`` cached for ``LISTDIR_CACHE_TTL_S``."""
    with _listdir_cache_lock:
        cached = _listdir_cache.get(path)
    if cached is not None:
        return list(cached)
    try:
        entries = os.listdir(path)
    except OSError:
        entries = []
    with _listdir_cache_lock:
        if len(_listdir_cache) >= LISTDIR_CACHE_MAX_ENTRIES:
            _listdir_cache.clear()
        _listdir_cache[path] = entries
    return entries


def rc_service_id(src: dict) -> str:
    """Service key used by the durable-serving readiness flag."""
    return src.get("service_id") or src.get("name") or ""


def durable_serving_blocked(src: dict) -> bool:
    """True when a durable serving replica must not trust its local rollup tree."""
    return svcconfig.is_durable_serving_mode(src) and not rollup_coverage_ready(rc_service_id(src))


def eligible_window(
    start_time: str | None,
    end_time: str | None,
    *,
    has_filters: bool,
    require_top_asns: list[int] | None = None,
    min_hours: int,
    observation: ReaderCoverage | None = None,
) -> tuple[datetime, datetime] | None:
    """Parse and gate a request window shared by the rollup readers."""
    if observation is not None:
        observation.minimum_window_hours = min_hours
        observation.eligible = False
    if has_filters:
        if observation is not None:
            observation.eligibility_reason = "filtered"
        return None
    if not start_time or not end_time:
        if observation is not None:
            observation.eligibility_reason = "bounds_missing"
        return None
    if require_top_asns is not None and not require_top_asns:
        if observation is not None:
            observation.eligibility_reason = "top_asns_empty"
        return None
    window = parse_window(start_time, end_time)
    if window is None:
        if observation is not None:
            observation.eligibility_reason = "invalid_window"
        return None
    st, et = window
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
    if (et - st) < timedelta(hours=min_hours):
        if observation is not None:
            observation.eligibility_reason = "window_too_short"
        return None
    if (et - st) > timedelta(days=366):
        if observation is not None:
            observation.eligibility_reason = "window_too_long"
        return None
    if observation is not None:
        observation.eligible = True
        observation.eligibility_reason = "eligible"
    return st, et


def meets_coverage_floor(covered_hours: int, total_closed_hours: int) -> bool:
    """Whether ``covered_hours`` of ``total_closed_hours`` clears the tolerant floor."""
    return not (total_closed_hours > 0 and (covered_hours / total_closed_hours) < MIN_CLOSED_HOUR_COVERAGE)


@dataclass(frozen=True)
class CoverageIndex:
    """Closed-hour grid for one ``[st, et)`` window."""

    st: datetime
    et: datetime
    active_dt: datetime
    active_hour_str: str
    closed_hours: tuple[str, ...]
    crosses_active: bool

    @classmethod
    def build(
        cls,
        st: datetime,
        et: datetime,
        *,
        skip_partial_start: bool = False,
        now: datetime | None = None,
    ) -> CoverageIndex:
        now = now if now is not None else datetime.now(UTC)
        active_hour_str = now.strftime("%Y-%m-%d-%H")
        active_dt = parse_hour_key_strict(active_hour_str)
        cursor = st.replace(minute=0, second=0, microsecond=0)
        if skip_partial_start and cursor < st:
            cursor += timedelta(hours=1)
        hours: list[str] = []
        crosses_active = False
        while cursor < et:
            key = cursor.strftime("%Y-%m-%d-%H")
            if key >= active_hour_str:
                crosses_active = True
                break
            hours.append(key)
            cursor += timedelta(hours=1)
        return cls(st, et, active_dt, active_hour_str, tuple(hours), crosses_active)

    def by_day(self) -> dict[str, list[str]]:
        """Closed hours bucketed by their ``YYYY-MM-DD`` day, oldest day first."""
        days: dict[str, list[str]] = {}
        for hour in self.closed_hours:
            days.setdefault(hour[:10], []).append(hour)
        return days


def _empty_parquet(path: str) -> bool:
    try:
        return bool(pq.read_metadata(path).num_rows == 0)
    except Exception:
        return False


class BundleProbe:
    """Memoized probes against one hour-bundled root, scoped to a single read."""

    __slots__ = ("_empty", "_has", "root")

    def __init__(self, bundled_root: str) -> None:
        self.root = bundled_root
        self._has: dict[tuple[str, str], bool] = {}
        self._empty: dict[tuple[str, str], bool] = {}

    def path(self, hour: str, filename: str) -> str:
        return os.path.join(self.root, f"hour={hour}", filename)

    def has(self, hour: str, filename: str) -> bool:
        key = (hour, filename)
        found = self._has.get(key)
        if found is None:
            found = self._has[key] = os.path.isfile(self.path(hour, filename))
        return found

    def is_empty(self, hour: str, filename: str) -> bool:
        key = (hour, filename)
        empty = self._empty.get(key)
        if empty is None:
            empty = self._empty[key] = self.has(hour, filename) and _empty_parquet(self.path(hour, filename))
        return empty


def _per_field_hours(src: dict) -> set[str]:
    """Union of every ``hour=…`` entry across all per-field rollup dirs."""
    root = _rollups_root(src)
    hours: set[str] = set()
    for field_dir in cached_listdir(root):
        if not field_dir.startswith("field="):
            continue
        for entry in cached_listdir(os.path.join(root, field_dir)):
            if entry.startswith("hour="):
                hours.add(entry[len("hour=") :])
    return hours


def _walk_hourly_bundles(
    src: dict,
    index: CoverageIndex,
    probe: BundleProbe,
    bundle_filename: str,
    allow_all_fields_coverage: bool,
) -> list[str] | None:
    paths: list[str] = []
    per_field_hours: set[str] | None = None
    for hour in index.closed_hours:
        if probe.has(hour, bundle_filename):
            paths.append(probe.path(hour, bundle_filename))
            continue
        if allow_all_fields_coverage and probe.has(hour, ALL_FIELDS_FILENAME):
            continue
        if per_field_hours is None:
            per_field_hours = _per_field_hours(src)
        if hour in per_field_hours:
            return None
    return paths


def collect_hourly_bundle_paths(
    src: dict,
    st: datetime,
    et: datetime,
    bundled_root: str,
    bundle_filename: str,
    *,
    allow_all_fields_coverage: bool = False,
) -> tuple[list[str], bool] | None:
    """Walk ``[st, et)`` by UTC hour, return ``(paths, crosses_active)``."""
    if durable_serving_blocked(src):
        return None
    index = CoverageIndex.build(st, et)
    paths = _walk_hourly_bundles(src, index, BundleProbe(bundled_root), bundle_filename, allow_all_fields_coverage)
    return (paths, index.crosses_active) if paths is not None else None


def _missing_hours(
    index: CoverageIndex,
    probe: BundleProbe,
    bundle_filename: str,
    coverage_bundle_filename: str | None,
) -> list[str]:
    return [
        hour
        for hour in index.closed_hours
        if not probe.has(hour, bundle_filename)
        and (coverage_bundle_filename is None or not probe.is_empty(hour, coverage_bundle_filename))
    ]


def heal_where_clause(missing_hours: list[str], st: datetime | None, et: datetime | None) -> str:
    """WHERE clause that live-queries exactly ``missing_hours`` from the raw base table."""
    start = parse_hour_key_strict(missing_hours[0])
    if st and st > start:
        start = st
    end = parse_hour_key_strict(missing_hours[-1]) + timedelta(hours=1)
    if et and et < end:
        end = et
    hours_in_sql = ", ".join(f"'{h}'" for h in missing_hours)
    return (
        f"timestamp >= '{start.isoformat()}' "
        f"AND timestamp < '{end.isoformat()}' "
        f"AND strftime(timestamp, '%Y-%m-%d-%H') IN ({hours_in_sql})"
    )


@dataclass(frozen=True)
class HourlyReadPlan:
    """Fail-closed plan for the minute-grain time-series readers."""

    index: CoverageIndex
    paths: list[str]
    missing_hours: list[str]

    @property
    def crosses_active(self) -> bool:
        return self.index.crosses_active

    @property
    def active_hour_str(self) -> str:
        return self.index.active_hour_str

    @property
    def active_dt(self) -> datetime:
        return self.index.active_dt

    def heal_where(self) -> str:
        return heal_where_clause(self.missing_hours, self.index.st, self.index.et)


def plan_hourly_read(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
    *,
    allow_all_fields_coverage: bool = False,
    heal: bool = False,
    observation: HourlyRollupCoverage | None = None,
) -> HourlyReadPlan | None:
    """Plan a rollup read of ``bundle_filename`` over ``[st, et)``, or ``None``."""
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
        observation.heal_horizon_hours = MISSING_HOUR_HEAL_CAP if heal else None
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        if observation is not None:
            observation.reason = "no_bundled_root"
        return None
    if durable_serving_blocked(src):
        if observation is not None:
            observation.reason = "durable_unready"
        return None

    index = CoverageIndex.build(st, et)
    if observation is not None:
        observation.closed_hours = len(index.closed_hours)
        observation.active_hour_start = index.active_dt.isoformat()
    probe = BundleProbe(bundled_root)
    paths = _walk_hourly_bundles(src, index, probe, bundle_filename, allow_all_fields_coverage)
    if paths is None:
        if observation is not None:
            observation.reason = "writer_behind"
        return None
    if observation is not None:
        observation.bundle_hours = len(paths)
    if not paths and not index.crosses_active:
        if observation is not None:
            observation.reason = "no_paths"
        return None
    if not heal:
        if observation is not None:
            observation.reason = "planned"
        return HourlyReadPlan(index, paths, [])

    gaps = _missing_hours(index, probe, bundle_filename, ALL_FIELDS_FILENAME)
    if observation is not None:
        observation.missing_hours = len(gaps)
        observation.verified_empty_hours = len(index.closed_hours) - len(paths) - len(gaps)
        observation.covered_hours = len(index.closed_hours) - len(gaps)
        observation.coverage_fraction = (
            observation.covered_hours / len(index.closed_hours) if index.closed_hours else None
        )
    heal_floor = max(
        index.st.replace(minute=0, second=0, microsecond=0), index.active_dt - timedelta(hours=MISSING_HOUR_HEAL_CAP)
    )
    heal_floor_key = heal_floor.strftime("%Y-%m-%d-%H")
    missing = [hour for hour in gaps if hour >= heal_floor_key]
    if len(missing) < len(gaps):
        if observation is not None:
            observation.reason = "old_gaps"
        return None
    if observation is not None:
        observation.reason = "planned"
    return HourlyReadPlan(index, paths, missing)


def _day_bundle_file(day_root: str, day: str, bundle_filename: str) -> str | None:
    day_file = os.path.join(day_root, f"day={day}", bundle_filename)
    if os.path.isfile(day_file):
        return day_file
    if bundle_filename == DAY_BUNDLE_FILENAME:
        alt_day_file = os.path.join(day_root, f"day={day}", f"day_bundle_{day}.parquet")
        flat_day_file = os.path.join(day_root, f"day_bundle_{day}.parquet")
        if os.path.isfile(alt_day_file):
            return alt_day_file
        if os.path.isfile(flat_day_file):
            return flat_day_file
    return None


def collect_day_prefer_paths(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
    *,
    skip_partial_start: bool = True,
    allow_empty_with_done: bool = False,
    observation: RollupPathCoverage | None = None,
) -> list[str] | None:
    """Day-prefer / hour-fallback rollup paths for ``[st, et)``, or ``None``."""
    if observation is not None:
        observation.minimum_coverage_fraction = MIN_CLOSED_HOUR_COVERAGE
    if durable_serving_blocked(src):
        if observation is not None:
            observation.reason = "durable_unready"
        return None

    hour_root = _hour_bundled_root(src)
    day_root = _day_bundled_root(src)
    if not os.path.isdir(hour_root) and not os.path.isdir(day_root):
        if observation is not None:
            observation.reason = "no_bundled_root"
        return None

    index = CoverageIndex.build(st, et, skip_partial_start=skip_partial_start)
    paths: list[str] = []
    covered_hours = 0
    day_files = hour_files = done_markers = 0
    for day, hours_in_day in sorted(index.by_day().items()):
        chosen_day = _day_bundle_file(day_root, day, bundle_filename)
        if len(hours_in_day) == 24 and chosen_day:
            paths.append(chosen_day)
            covered_hours += 24
            day_files += 1
        else:
            for hour in hours_in_day:
                path = os.path.join(hour_root, f"hour={hour}", bundle_filename)
                if os.path.isfile(path):
                    paths.append(path)
                    covered_hours += 1
                    hour_files += 1
                elif allow_empty_with_done and os.path.isfile(f"{path}.done"):
                    covered_hours += 1
                    done_markers += 1

    if observation is not None:
        observation.active_hour_start = index.active_dt.isoformat()
        observation.closed_hours = len(index.closed_hours)
        observation.covered_hours = covered_hours
        observation.coverage_fraction = covered_hours / len(index.closed_hours) if index.closed_hours else None
        observation.partial_start_skipped = skip_partial_start and st > st.replace(minute=0, second=0, microsecond=0)
        observation.day_files, observation.hour_files, observation.done_markers = day_files, hour_files, done_markers
    if not paths and not (allow_empty_with_done and covered_hours > 0):
        if observation is not None:
            observation.reason = "no_paths"
        return None
    if not meets_coverage_floor(covered_hours, len(index.closed_hours)):
        if observation is not None:
            observation.reason = "below_coverage_floor"
        return None
    if observation is not None:
        observation.reason = "planned"
    return paths


def collect_hour_paths_with_floor(
    src: dict,
    st: datetime,
    et: datetime,
    bundle_filename: str,
) -> list[str] | None:
    """Per-hour-only paths for ``[st, et)`` with the tolerant coverage floor."""
    if durable_serving_blocked(src):
        return None
    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None
    index = CoverageIndex.build(st, et, skip_partial_start=True)
    paths = [
        path
        for path in (os.path.join(bundled_root, f"hour={hour}", bundle_filename) for hour in index.closed_hours)
        if os.path.exists(path)
    ]
    if not paths or not meets_coverage_floor(len(paths), len(index.closed_hours)):
        return None
    return paths


@dataclass(frozen=True)
class TimeSeriesRollupWindowPlan:
    rollup_paths: list[str]
    day_ov_paths: list[str]
    crosses_active: bool
    active_hour_str: str
    active_hour_dt: datetime
    missing_hours: list[str]
    heal_where_sql: str


@dataclass(frozen=True)
class ClosedHourFieldRollupWindow:
    bundled_paths: list[str]
    per_field_paths: list[str]
    st_dt: datetime
    et_dt: datetime
    active_dt: datetime
    active_end_dt: datetime


def _finalize_scalar_coverage(observation: ScalarRollupCoverage) -> None:
    trailing = observation.trailing
    if (
        trailing is not None
        and trailing.active_hour_start is not None
        and trailing.active_hour_start != observation.active_hour_start
    ):
        observation.reason = "boundary_changed"
        return
    trailing_covered = trailing.covered_hours if trailing is not None else 0
    if trailing_covered is not None:
        observation.covered_hours = observation.prefix_hours + trailing_covered
        observation.coverage_fraction = (
            observation.covered_hours / observation.closed_hours if observation.closed_hours else None
        )


def plan_time_series_rollup_window(
    src: dict[str, Any],
    st: datetime,
    et: datetime,
    *,
    can_use_overview: bool = True,
    coverage_mod: Any = None,
    plan_hourly_read_fn: Any = None,
    observation: ScalarRollupCoverage | None = None,
) -> TimeSeriesRollupWindowPlan | None:
    """Plan closed-day prefix + trailing hourly bundles for time_series/count rollup reads."""
    rollup_coverage = coverage_mod if coverage_mod is not None else sys.modules[__name__]
    plan_hourly_read_impl = plan_hourly_read_fn if plan_hourly_read_fn is not None else plan_hourly_read

    day_ts_paths: list[str] = []
    day_ov_paths: list[str] = []
    trailing_st = st
    cov = rollup_coverage.CoverageIndex.build(st, et)
    if observation is not None:
        observation.window_start, observation.window_end = st.isoformat(), et.isoformat()
        observation.closed_hours = len(cov.closed_hours)
        observation.active_hour_start = cov.active_dt.isoformat()
    if timedelta(days=6) < (et - st) <= timedelta(days=90) and not rollup_coverage.durable_serving_blocked(src):
        day_root = _day_bundled_root(src)
        covered_until_hour: str | None = None
        for day, day_hours in sorted(cov.by_day().items()):
            if day >= cov.active_hour_str[:10]:
                break
            day_ts_file = rollup_coverage._day_bundle_file(day_root, day, TIME_SERIES_BUNDLE_FILENAME)
            day_ov_file = (
                rollup_coverage._day_bundle_file(day_root, day, OVERVIEW_BUNDLE_FILENAME)
                if can_use_overview
                and (len(day_hours) == 24 or (st.minute == 0 and st.second == 0 and st.microsecond == 0))
                else None
            )
            if day_ts_file:
                day_ts_paths.append(day_ts_file)
                covered_until_hour = day_hours[-1]
            elif day_ov_file:
                day_ov_paths.append(day_ov_file)
                covered_until_hour = day_hours[-1]
            else:
                break
            if observation is not None:
                observation.prefix_hours += len(day_hours)
        if covered_until_hour is not None:
            trailing_st = parse_hour_key_strict(covered_until_hour) + timedelta(hours=1)

    if observation is not None:
        observation.trailing_start = trailing_st.isoformat()
        observation.day_time_series_files = len(day_ts_paths)
        observation.day_overview_files = len(day_ov_paths)
    if trailing_st < et:
        if observation is not None:
            observation.trailing = HourlyRollupCoverage()
        plan = plan_hourly_read_impl(
            src,
            trailing_st,
            et,
            TIME_SERIES_BUNDLE_FILENAME,
            allow_all_fields_coverage=True,
            heal=True,
            **({"observation": observation.trailing} if observation is not None else {}),
        )
        if plan is None:
            if observation is not None:
                observation.reason = "trailing_unavailable"
                _finalize_scalar_coverage(observation)
            return None
        rollup_paths = day_ts_paths + list(plan.paths)
        crosses_active = plan.crosses_active
        active_hour_str, active_hour_dt = plan.active_hour_str, plan.active_dt
        missing_hours = list(plan.missing_hours)
        heal_where_sql = plan.heal_where() if plan.missing_hours else ""
    else:
        rollup_paths = day_ts_paths
        crosses_active = False
        active_hour_str, active_hour_dt = cov.active_hour_str, cov.active_dt
        missing_hours = []
        heal_where_sql = ""

    if observation is not None:
        observation.reason = "planned"
        _finalize_scalar_coverage(observation)
    return TimeSeriesRollupWindowPlan(
        rollup_paths=rollup_paths,
        day_ov_paths=day_ov_paths,
        crosses_active=crosses_active,
        active_hour_str=active_hour_str,
        active_hour_dt=active_hour_dt,
        missing_hours=missing_hours,
        heal_where_sql=heal_where_sql,
    )


def enumerate_closed_hour_field_rollups(
    src: dict[str, Any],
    field: str,
    start_time: str | None,
    end_time: str | None,
    *,
    stop_on_first: bool = False,
) -> ClosedHourFieldRollupWindow | None:
    """Enumerate in-window closed-hour bundled (`all_fields.parquet`) and per-field rollup files."""
    from backend.core.duckdb import _cache_dir

    if svcconfig.is_durable_serving_mode(src) or not _is_safe_ident(field) or not start_time or not end_time:
        return None
    try:
        window = parse_window(start_time, end_time)
    except Exception:
        return None
    if window is None:
        return None
    st_dt, et_dt = window

    cache_dir = _cache_dir(src)
    bundled_root = os.path.join(cache_dir, "rollups", "hour_bundled")
    per_field_root = os.path.join(cache_dir, "rollups", "hour", f"field={field}")

    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_end_dt = active_dt + timedelta(hours=1)
    active_str = active_dt.strftime("%Y-%m-%d-%H")
    st_str = st_dt.strftime("%Y-%m-%d-%H")
    et_inclusive = (et_dt - timedelta(microseconds=1)).strftime("%Y-%m-%d-%H")

    def _in_window(hour: str) -> bool:
        return st_str <= hour <= et_inclusive and hour < active_str

    bundled_paths: list[str] = []
    bundled_hours: set[str] = set()
    if os.path.isdir(bundled_root):
        try:
            for entry in os.listdir(bundled_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour):
                    continue
                p = os.path.join(bundled_root, entry, "all_fields.parquet")
                if os.path.isfile(p):
                    bundled_paths.append(p)
                    bundled_hours.add(hour)
                    if stop_on_first:
                        return ClosedHourFieldRollupWindow(
                            bundled_paths=bundled_paths,
                            per_field_paths=[],
                            st_dt=st_dt,
                            et_dt=et_dt,
                            active_dt=active_dt,
                            active_end_dt=active_end_dt,
                        )
        except OSError:
            pass

    per_field_paths: list[str] = []
    if os.path.isdir(per_field_root):
        try:
            for entry in os.listdir(per_field_root):
                if not entry.startswith("hour="):
                    continue
                hour = entry[5:]
                if not _in_window(hour) or hour in bundled_hours:
                    continue
                hour_dir = os.path.join(per_field_root, entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet") and not fname.startswith(".tmp_"):
                            per_field_paths.append(os.path.join(hour_dir, fname))
                            if stop_on_first:
                                return ClosedHourFieldRollupWindow(
                                    bundled_paths=bundled_paths,
                                    per_field_paths=per_field_paths,
                                    st_dt=st_dt,
                                    et_dt=et_dt,
                                    active_dt=active_dt,
                                    active_end_dt=active_end_dt,
                                )
                except OSError:
                    continue
        except OSError:
            pass

    return ClosedHourFieldRollupWindow(
        bundled_paths=bundled_paths,
        per_field_paths=per_field_paths,
        st_dt=st_dt,
        et_dt=et_dt,
        active_dt=active_dt,
        active_end_dt=active_end_dt,
    )


for _legacy_mod in (
    "backend.core.rollups.time_series",
    "backend.core.rollups.overview",
    "backend.core.rollups.sessions",
    "backend.core.rollups.rum",
    "backend.repositories.rollup_coverage",
    "backend.repositories._rollup_planner",
):
    sys.modules[_legacy_mod] = sys.modules[__name__]
