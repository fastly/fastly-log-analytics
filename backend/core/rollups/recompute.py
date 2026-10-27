"""Recompute / backfill / cleanup orchestrator for per-field per-hour and RUM rollups."""

from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import types
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.utils.telemetry import ensure_utc

from . import overview_rollups as _overview_mod
from . import rollup_coverage as _coverage_mod
from . import session_rollups as _session_mod
from . import time_series_rollups as _ts_mod
from ._common import (
    _VIRTUAL_FIELD_BACKING,
    IP_SAMPLE_CAP,
    IP_SPREAD_BUNDLE_FILENAME,
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
    parse_hour_token,
    quote_path_list,
    shared_rollup_session,
)
from .day_bundles import bundle_hours, bundle_hours_ip_spread, stamp_empty_hour_sentinels
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
from .overview_rollups import (  # noqa: F401
    _HIT_STATES,
    backfill_overview_bundles,
    build_overview_bundles,
)
from .rollup_coverage import (  # noqa: F401
    ALL_FIELDS_FILENAME,
    LISTDIR_CACHE_MAX_ENTRIES,
    LISTDIR_CACHE_TTL_S,
    MIN_CLOSED_HOUR_COVERAGE,
    MISSING_HOUR_HEAL_CAP,
    BundleProbe,
    ClosedHourFieldRollupWindow,
    CoverageIndex,
    HourlyReadPlan,
    TimeSeriesRollupWindowPlan,
    _day_bundle_file,
    _empty_parquet,
    _finalize_scalar_coverage,
    _listdir_cache,
    _listdir_cache_lock,
    _missing_hours,
    _per_field_hours,
    _walk_hourly_bundles,
    cached_listdir,
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    collect_hourly_bundle_paths,
    durable_serving_blocked,
    eligible_window,
    enumerate_closed_hour_field_rollups,
    heal_where_clause,
    meets_coverage_floor,
    plan_hourly_read,
    plan_time_series_rollup_window,
    rc_service_id,
)
from .security_dims import (  # noqa: F401
    build_ngwaf_bots_bundles,
    build_ngwaf_bots_ts_bundles,
    build_security_dims_bundles,
    build_security_proxies_bundles,
    build_verified_bots_ts_bundles,
    recompute_wellknown_bots_rollup,
)
from .session_rollups import build_session_bundles  # noqa: F401
from .time_series_rollups import build_time_series_bundles  # noqa: F401

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
    from backend.core.storage._core import _get_service_lock

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
    from backend.core.storage._core import _get_service_lock
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


# ── RUM rollup writers ───────────────────────────────────────────────────────

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


_SUBMODULES = (_ts_mod, _overview_mod, _session_mod, _coverage_mod)


class _RollupsRecomputeShimModule(types.ModuleType):
    """Forwards attribute assignments on ``backend.core.rollups.recompute`` to extracted submodules."""

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        for mod in _SUBMODULES:
            if name in mod.__dict__:
                setattr(mod, name, value)


sys.modules[__name__].__class__ = _RollupsRecomputeShimModule
