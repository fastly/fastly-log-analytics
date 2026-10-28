from __future__ import annotations

import logging
import os
import shutil
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from backend.core.sql_validator import escape_sql_literal
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import parse_day_key_strict

from . import day_compaction_sql as _day_compaction_sql
from . import partial_hour as _partial_hour
from ._common import (
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    IP_SPREAD_BUNDLE_FILENAME,
    _closed_day_window,
    _day_bundled_root,
    _day_rollups_root,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    _rollups_root,
    cleanup_per_field_after_bundle,
    collect_per_field_partition_parquets,
    parquet_zstd_target,
    parse_hour_token,
    publish_temp_parquet,
    quote_path_list,
    unlink_quiet,
)
from .day_compaction_sql import (  # noqa: F401
    _make_day_compactor,
    _network_quality_copy_sql,
    _network_rtt_copy_sql,
    _origin_ip_copy_sql,
    _origin_latency_ts_copy_sql,
    _origin_path_copy_sql,
    _origin_pop_copy_sql,
    _origin_summary_copy_sql,
    _overview_copy_sql,
    _perf_latency_copy_sql,
    _pop_health_copy_sql,
    _security_bot_eff_copy_sql,
    _security_cov_copy_sql,
    _security_flagged_copy_sql,
    _security_topips_copy_sql,
    _time_series_copy_sql,
    compact_network_quality_closed_days_to_daily,
    compact_network_rtt_closed_days_to_daily,
    compact_network_speed_closed_days_to_daily,
    compact_ngwaf_bots_closed_days_to_daily,
    compact_ngwaf_bots_ts_closed_days_to_daily,
    compact_origin_dims_closed_days_to_daily,
    compact_origin_latency_ts_closed_days_to_daily,
    compact_origin_summary_closed_days_to_daily,
    compact_overview_closed_days_to_daily,
    compact_perf_dims_closed_days_to_daily,
    compact_perf_latency_closed_days_to_daily,
    compact_pop_health_closed_days_to_daily,
    compact_security_dims_closed_days_to_daily,
    compact_security_proxies_closed_days_to_daily,
    compact_time_series_closed_days_to_daily,
    compact_verified_bots_ts_closed_days_to_daily,
    rebuild_time_series_days,
)
from .partial_hour import (  # noqa: F401
    TOTAL_FIELD,
    _all_fields_path,
    _list_new_source_files,
    _merge_partial_hour_locked,
    _read_partial_hour_parquet,
    _watermark_path,
    _write_partial_hour_watermark,
    gc_stale_partial_hours,
    merge_partial_hour,
    partial_hour_dir,
    partial_hour_root,
    read_partial_hour_all_fields,
    read_partial_hour_total,
    read_partial_hour_watermark,
)

logger = logging.getLogger(__name__)


def _link_day_bundle_alias(src_path: str, dst_path: str) -> None:
    tmp_path = dst_path + ".tmp"
    Path(tmp_path).unlink(missing_ok=True)
    try:
        try:
            os.link(src_path, tmp_path)
        except OSError:
            shutil.copyfile(src_path, tmp_path)
        os.replace(tmp_path, dst_path)
    except OSError as e:
        logger.warning("[rollups] Failed to create alias %s -> %s: %s", src_path, dst_path, e)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass


def bundle_days(service_id: str, source: dict, days: list[str]) -> int:
    if not days:
        return 0
    import duckdb

    day_per_field_root = _day_rollups_root(source)
    bundled_root = _day_bundled_root(source)
    if not os.path.isdir(day_per_field_root):
        return 0
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    rebuilt = 0
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        for day in days:
            if day == active_day:
                continue
            try:
                parse_day_key_strict(day)
            except ValueError:
                continue
            collected = collect_per_field_partition_parquets(day_per_field_root, f"day={day}")
            if collected is None:
                continue
            per_field_paths, updated_fields, max_src_mtime = collected
            if not per_field_paths:
                continue
            bundle_dir = os.path.join(bundled_root, f"day={day}")
            bundle_path = os.path.join(bundle_dir, DAY_BUNDLE_FILENAME)
            alias_dir_path = os.path.join(bundle_dir, f"day_bundle_{day}.parquet")
            alias_root_path = os.path.join(bundled_root, f"day_bundle_{day}.parquet")
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        if not os.path.exists(alias_dir_path):
                            _link_day_bundle_alias(bundle_path, alias_dir_path)
                        if not os.path.exists(alias_root_path):
                            _link_day_bundle_alias(bundle_path, alias_root_path)
                        _cleanup_per_field_day_after_bundle(day_per_field_root, day, bundle_path, service_id)
                        continue
                except OSError:
                    pass
            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)

            def _build_day_bundle_query(src_union_sql: str = "") -> str:
                return (
                    f"COPY ("
                    f"  WITH src AS (SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"               FROM read_parquet([{paths_sql}]){src_union_sql}), "
                    f"       ranked AS (SELECT field, value, count, "
                    f"                  ROW_NUMBER() OVER (PARTITION BY field ORDER BY count DESC) AS rn "
                    f"                  FROM src) "
                    f"  SELECT field, value, count FROM ranked WHERE rn <= {DAY_BUNDLE_TOP_K} "
                    f"  UNION ALL "
                    f"  SELECT field, '__other__' AS value, SUM(count) AS count "
                    f"  FROM ranked WHERE rn > {DAY_BUNDLE_TOP_K} "
                    f"  GROUP BY field "
                    f"  HAVING SUM(count) > 0"
                    f") "
                    f"{parquet_zstd_target(tmp_path)}"
                )

            plain_query = _build_day_bundle_query()
            if os.path.isfile(bundle_path) and updated_fields:
                esc_bundle = escape_sql_literal(bundle_path)
                safe_in = ", ".join(f"'{escape_sql_literal(f)}'" for f in sorted(updated_fields))
                preserve_sql = (
                    f" UNION ALL SELECT field, value, CAST(count AS BIGINT) AS count "
                    f"FROM read_parquet('{esc_bundle}', hive_partitioning=0) "
                    f"WHERE field NOT IN ({safe_in})"
                )
                query = _build_day_bundle_query(preserve_sql)
            else:
                query = plain_query
            try:
                con.execute(query)
            except duckdb.Error as e:
                if query != plain_query:
                    unlink_quiet(tmp_path)
                    try:
                        con.execute(plain_query)
                    except duckdb.Error as e2:
                        logger.warning("[rollups] %s: day-bundle COPY failed for day=%s: %s", service_id, day, e2)
                        unlink_quiet(tmp_path)
                        continue
                else:
                    logger.warning("[rollups] %s: day-bundle COPY failed for day=%s: %s", service_id, day, e)
                    unlink_quiet(tmp_path)
                    continue

            def _link_aliases(
                _bp: str = bundle_path,
                _ad: str = alias_dir_path,
                _ar: str = alias_root_path,
            ) -> None:
                _link_day_bundle_alias(_bp, _ad)
                _link_day_bundle_alias(_bp, _ar)

            publish_temp_parquet(
                tmp_path,
                bundle_path,
                lock_key,
                after_replace_under_lock=_link_aliases,
            )
            _cleanup_per_field_day_after_bundle(day_per_field_root, day, bundle_path, service_id)
            rebuilt += 1
    finally:
        con.close()
    return rebuilt


def backfill_day_bundles(
    service_id: str,
    source: dict,
    max_days: int | None = None,
    lookback_days: int | None = None,
) -> int:
    day_per_field_root = _day_rollups_root(source)
    bundled_root = _day_bundled_root(source)
    if not os.path.isdir(day_per_field_root):
        return 0
    active_day, cutoff_day = _closed_day_window(lookback_days)
    all_days: set[str] = set()
    try:
        for field_entry in os.listdir(day_per_field_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(day_per_field_root, field_entry)
            try:
                for day_entry in os.listdir(field_dir):
                    if not day_entry.startswith("day="):
                        continue
                    day = day_entry[len("day=") :]
                    if day >= active_day:
                        continue
                    if cutoff_day is not None and day < cutoff_day:
                        continue
                    all_days.add(day)
            except OSError:
                continue
    except OSError:
        return 0
    to_bundle: list[str] = []
    for day in sorted(all_days):
        if cutoff_day is not None and day < cutoff_day:
            continue
        bundle_path = os.path.join(bundled_root, f"day={day}", DAY_BUNDLE_FILENAME)
        if not os.path.exists(bundle_path):
            to_bundle.append(day)
        if max_days and len(to_bundle) >= max_days:
            break
    if not to_bundle:
        return 0
    return bundle_days(service_id, source, to_bundle)


def _cleanup_per_field_day_after_bundle(
    day_per_field_root: str,
    day: str,
    bundle_path: str,
    service_id: str,
) -> None:
    cleanup_per_field_after_bundle(
        day_per_field_root,
        f"day={day}",
        bundle_path,
        service_id,
        file_kind_label="per-field day",
        dir_kind_label="per-field day",
        cleanup_label="cleanup",
        logger=logger,
    )


def compact_closed_days_to_daily(
    service_id: str,
    source: dict,
    lookback_days: int | None = None,
) -> int:
    import duckdb

    hour_root = _rollups_root(source)
    day_root = _day_rollups_root(source)
    if not os.path.isdir(hour_root) and not os.path.isdir(_hour_bundled_root(source)):
        return 0
    active_day, cutoff_day = _closed_day_window(lookback_days)
    lock_key = source.get("name", "default")
    rebuilt = 0
    bundled_hour_root_path = _hour_bundled_root(source)
    hour_bundled_by_day: dict[str, list[str]] = {}
    if os.path.isdir(bundled_hour_root_path):
        try:
            for hour_entry in os.listdir(bundled_hour_root_path):
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                if len(hour) < 13:
                    continue
                day = hour[:10]
                if day == active_day:
                    continue
                if cutoff_day is not None and day < cutoff_day:
                    continue
                bundle_path = os.path.join(bundled_hour_root_path, hour_entry, "all_fields.parquet")
                if os.path.isfile(bundle_path):
                    hour_bundled_by_day.setdefault(day, []).append(bundle_path)
        except OSError:
            pass
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        field_set: set[str] = set()
        bundle_fields: dict[str, set[str]] = {}
        try:
            for entry in os.listdir(hour_root):
                if entry.startswith("field="):
                    f = entry[len("field=") :]
                    if _is_safe_ident(f):
                        field_set.add(f)
        except OSError:
            pass
        if hour_bundled_by_day:
            all_bundle_paths = sorted({p for paths in hour_bundled_by_day.values() for p in paths})
            try:
                paths_sql = quote_path_list(all_bundle_paths)
                for f, path in con.execute(
                    f"SELECT DISTINCT field, filename FROM read_parquet([{paths_sql}], filename=1)"
                ).fetchall():
                    if isinstance(f, str) and _is_safe_ident(f):
                        field_set.add(f)
                        bundle_fields.setdefault(path, set()).add(f)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: could not inspect hourly bundles for day compaction: %s", service_id, e)
                return 0
        for field in sorted(field_set):
            field_entry = f"field={field}"
            field_hour_dir = os.path.join(hour_root, field_entry)
            by_day: dict[str, list[str]] = {}
            try:
                hour_entries = os.listdir(field_hour_dir) if os.path.isdir(field_hour_dir) else []
            except OSError:
                hour_entries = []
            for hour_entry in hour_entries:
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                if len(hour) < 13:
                    continue
                day = hour[:10]
                if day == active_day:
                    continue
                if cutoff_day is not None and day < cutoff_day:
                    continue
                hour_dir = os.path.join(field_hour_dir, hour_entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet"):
                            by_day.setdefault(day, []).append(os.path.join(hour_dir, fname))
                except OSError:
                    continue
            all_days = set(by_day.keys()) | set(hour_bundled_by_day.keys())
            for day in all_days:
                if cutoff_day is not None and day < cutoff_day:
                    continue
                hour_paths = by_day.get(day, [])
                bundled_paths = hour_bundled_by_day.get(day, [])
                if not hour_paths and not bundled_paths:
                    continue
                day_dir = os.path.join(day_root, field_entry, f"day={day}")
                day_file = os.path.join(day_dir, "compacted.parquet")
                all_source_paths = hour_paths + bundled_paths
                try:
                    source_mtimes = {p: os.path.getmtime(p) for p in all_source_paths}
                except OSError as e:
                    logger.warning(
                        "[rollups] %s: day-compact source unavailable for %s/%s: %s", service_id, field, day, e
                    )
                    continue
                try:
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(source_mtimes.values())
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild
                per_field_hours: dict[str, list[str]] = {}
                for path in hour_paths:
                    per_field_hours.setdefault(os.path.basename(os.path.dirname(path)), []).append(path)
                hour_paths = []
                superseded_bundles: set[str] = set()
                for hour_entry, paths in per_field_hours.items():
                    bundle_path = os.path.join(bundled_hour_root_path, hour_entry, "all_fields.parquet")
                    if bundle_path in source_mtimes and field in bundle_fields.get(bundle_path, set()):
                        if source_mtimes[bundle_path] >= max(source_mtimes[p] for p in paths):
                            continue
                        superseded_bundles.add(bundle_path)
                    hour_paths.extend(paths)
                bundled_paths = [p for p in bundled_paths if p not in superseded_bundles]
                tmp_file = os.path.join(day_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
                os.makedirs(day_dir, exist_ok=True)
                branches = []
                if hour_paths:
                    paths_sql = quote_path_list(hour_paths)
                    branches.append(
                        f"SELECT field, value, CAST(count AS BIGINT) AS count "
                        f"FROM read_parquet([{paths_sql}], hive_partitioning=1)"
                    )
                if bundled_paths:
                    paths_sql = quote_path_list(bundled_paths)
                    safe_field_sql = escape_sql_literal(field)
                    branches.append(
                        f"SELECT field, value, CAST(count AS BIGINT) AS count "
                        f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                        f"WHERE field = '{safe_field_sql}'"
                    )
                copy_sql = (
                    f"COPY ("
                    f"SELECT field, value, CAST(SUM(count) AS BIGINT) AS count "
                    f"FROM ({' UNION ALL '.join(branches)}) "
                    f"GROUP BY field, value"
                    f") {parquet_zstd_target(tmp_file)}"
                )
                try:
                    con.execute(copy_sql)
                except duckdb.Error as e:
                    logger.warning(
                        "[rollups] %s: day-compact COPY failed for %s/%s: %s",
                        service_id,
                        field,
                        day,
                        e,
                    )
                    unlink_quiet(tmp_file)
                    continue
                try:
                    publish_temp_parquet(tmp_file, day_file, lock_key)
                    rebuilt += 1
                except OSError as e:
                    logger.warning("[rollups] %s: rename to %s failed: %s", service_id, day_file, e)
    finally:
        con.close()
    return rebuilt


def retire_compacted_hour_bundles(
    service_id: str,
    source: dict,
    lookback_days: int | None = 30,
) -> tuple[int, int]:
    from backend.core.duckdb import get_memory_connection
    from backend.core.storage._core import _get_service_lock

    bundled_root = _day_bundled_root(source)
    hour_bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root) or not os.path.isdir(hour_bundled_root):
        return 0, 0
    active_day, cutoff_day = _closed_day_window(lookback_days)
    retired_files = 0
    retired_bytes = 0
    lock_key = source.get("name", "default")
    dry_run = os.environ.get("ROLLUP_CLEANUP_DRY_RUN") == "1"
    candidate_days: set[str] = set()
    try:
        for entry in os.listdir(bundled_root):
            if entry.startswith("day="):
                d = entry[len("day=") :]
                if d < active_day and (cutoff_day is None or d >= cutoff_day):
                    candidate_days.add(d)
            elif entry.startswith("day_bundle_") and entry.endswith(".parquet"):
                d = entry[len("day_bundle_") : -len(".parquet")]
                if len(d) == 10 and d < active_day and (cutoff_day is None or d >= cutoff_day):
                    candidate_days.add(d)
    except OSError:
        return 0, 0
    con = get_memory_connection()
    try:
        for day in sorted(candidate_days):
            bundle_path = os.path.join(bundled_root, f"day={day}", DAY_BUNDLE_FILENAME)
            alt_bundle_path = os.path.join(bundled_root, f"day={day}", f"day_bundle_{day}.parquet")
            flat_bundle_path = os.path.join(bundled_root, f"day_bundle_{day}.parquet")
            valid_path = (
                bundle_path
                if os.path.isfile(bundle_path)
                else (
                    alt_bundle_path
                    if os.path.isfile(alt_bundle_path)
                    else (flat_bundle_path if os.path.isfile(flat_bundle_path) else None)
                )
            )
            if not valid_path:
                continue
            try:
                res = con.execute(f"SELECT count(*) FROM read_parquet('{valid_path}')").fetchone()
                row_cnt = res[0] if res else 0
                if not row_cnt or row_cnt <= 0:
                    continue
            except Exception as e:
                logger.warning(
                    "[rollups] %s: day bundle read validation failed for %s: %s",
                    service_id,
                    valid_path,
                    e,
                )
                continue
            day_ip_spread_exists = os.path.isfile(os.path.join(bundled_root, f"day={day}", IP_SPREAD_BUNDLE_FILENAME))
            target_filenames = (
                (DAY_BUNDLE_FILENAME, IP_SPREAD_BUNDLE_FILENAME) if day_ip_spread_exists else (DAY_BUNDLE_FILENAME,)
            )
            for hour_idx in range(24):
                hour_str = f"{day}-{hour_idx:02d}"
                hour_dir = os.path.join(hour_bundled_root, f"hour={hour_str}")
                if not os.path.isdir(hour_dir):
                    continue
                for target_filename in target_filenames:
                    target_file = os.path.join(hour_dir, target_filename)
                    if os.path.isfile(target_file):
                        try:
                            fsize = os.path.getsize(target_file)
                            if not dry_run:
                                with _get_service_lock(lock_key):
                                    os.remove(target_file)
                            retired_files += 1
                            retired_bytes += fsize
                        except OSError as e:
                            logger.warning(
                                "[rollups] %s: failed to unlink %s: %s",
                                service_id,
                                target_file,
                                e,
                            )
                if not dry_run and os.path.isdir(hour_dir):
                    try:
                        os.rmdir(hour_dir)
                    except OSError:
                        pass
    finally:
        con.close()
    return retired_files, retired_bytes


def _bundle_per_field_hours_impl(
    service_id: str,
    source: dict,
    hours: list[str],
    *,
    per_field_root: str,
    bundle_filename: str,
    select_columns_sql: str,
    cleanup_fn: Callable[[str, str, str, str], None],
    err_label: str,
    require_root_exists: bool = False,
) -> int:
    if not hours:
        return 0
    import duckdb

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    if require_root_exists and not os.path.isdir(per_field_root):
        return 0
    rebuilt = 0
    from backend.core.duckdb import get_memory_connection

    con = get_memory_connection()
    try:
        for hour in hours:
            if hour == active_hour:
                continue
            if parse_hour_token(hour) is None:
                continue
            collected = collect_per_field_partition_parquets(per_field_root, f"hour={hour}")
            if collected is None:
                continue
            per_field_paths, updated_fields, max_src_mtime = collected
            if not per_field_paths:
                continue
            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            bundle_path = os.path.join(bundle_dir, bundle_filename)
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        cleanup_fn(per_field_root, hour, bundle_path, service_id)
                        continue
                except OSError:
                    pass
            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = quote_path_list(per_field_paths)
            plain_query = (
                f"COPY ({select_columns_sql} FROM read_parquet([{paths_sql}])) {parquet_zstd_target(tmp_path)}"
            )
            if os.path.isfile(bundle_path) and updated_fields:
                esc_bundle = escape_sql_literal(bundle_path)
                safe_in = ", ".join(f"'{escape_sql_literal(f)}'" for f in sorted(updated_fields))
                query = (
                    f"COPY ("
                    f"{select_columns_sql} FROM read_parquet([{paths_sql}]) "
                    f"UNION ALL "
                    f"{select_columns_sql} FROM read_parquet('{esc_bundle}', hive_partitioning=0) "
                    f"WHERE field NOT IN ({safe_in})"
                    f") {parquet_zstd_target(tmp_path)}"
                )
            else:
                query = plain_query
            try:
                con.execute(query)
            except duckdb.Error as e:
                if query != plain_query:
                    unlink_quiet(tmp_path)
                    try:
                        con.execute(plain_query)
                    except duckdb.Error as e2:
                        logger.warning("[rollups] %s: %s COPY failed for hour=%s: %s", service_id, err_label, hour, e2)
                        unlink_quiet(tmp_path)
                        continue
                else:
                    logger.warning("[rollups] %s: %s COPY failed for hour=%s: %s", service_id, err_label, hour, e)
                    unlink_quiet(tmp_path)
                    continue
            publish_temp_parquet(
                tmp_path,
                bundle_path,
                lock_key,
                after_replace_under_lock=lambda: cleanup_fn(per_field_root, hour, bundle_path, service_id),
            )
            rebuilt += 1
    finally:
        con.close()
    return rebuilt


def bundle_hours(service_id: str, source: dict, hours: list[str]) -> int:
    return _bundle_per_field_hours_impl(
        service_id,
        source,
        hours,
        per_field_root=_rollups_root(source),
        bundle_filename="all_fields.parquet",
        select_columns_sql="SELECT field, value, CAST(count AS BIGINT) AS count",
        cleanup_fn=lambda root, hr, bpath, sid: _cleanup_per_field_after_bundle(root, hr, bpath, sid),
        err_label="bundle",
        require_root_exists=False,
    )


def stamp_empty_hour_sentinels(service_id: str, source: dict, hours: list[str]) -> int:
    if not hours:
        return 0
    import pyarrow as pa
    import pyarrow.parquet as pq

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
    empty_table = pa.table({"field": [], "value": [], "count": []}, schema=schema)
    stamped = 0
    for hour in hours:
        if hour >= active_hour:
            continue
        if parse_hour_token(hour) is None:
            continue
        bundle_dir = os.path.join(bundled_root, f"hour={hour}")
        bundle_path = os.path.join(bundle_dir, "all_fields.parquet")
        if os.path.exists(bundle_path):
            continue
        os.makedirs(bundle_dir, exist_ok=True)
        tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
        try:
            pq.write_table(empty_table, tmp_path)
            publish_temp_parquet(tmp_path, bundle_path, lock_key)
            stamped += 1
        except OSError as e:
            logger.warning("[rollups] %s: empty-hour sentinel write failed for hour=%s: %s", service_id, hour, e)
            unlink_quiet(tmp_path)
    return stamped


def bundle_hours_ip_spread(service_id: str, source: dict, hours: list[str]) -> int:
    return _bundle_per_field_hours_impl(
        service_id,
        source,
        hours,
        per_field_root=_ip_spread_root(source),
        bundle_filename=IP_SPREAD_BUNDLE_FILENAME,
        select_columns_sql=(
            "SELECT field, value, ip_sketch, CAST(ip_count_observed AS BIGINT) AS ip_count_observed, sample_capped"
        ),
        cleanup_fn=lambda root, hr, bpath, sid: _cleanup_ip_spread_per_field_after_bundle(root, hr, bpath, sid),
        err_label="ip_spread bundle",
        require_root_exists=True,
    )


def _cleanup_per_field_after_bundle(
    hour_per_field_root: str,
    hour: str,
    bundle_path: str,
    service_id: str,
) -> None:
    cleanup_per_field_after_bundle(
        hour_per_field_root,
        f"hour={hour}",
        bundle_path,
        service_id,
        file_kind_label="per-field",
        dir_kind_label="per-field",
        cleanup_label="cleanup",
        logger=logger,
    )


def _cleanup_ip_spread_per_field_after_bundle(
    ip_spread_per_field_root: str,
    hour: str,
    bundle_path: str,
    service_id: str,
) -> None:
    cleanup_per_field_after_bundle(
        ip_spread_per_field_root,
        f"hour={hour}",
        bundle_path,
        service_id,
        file_kind_label="per-field ip_spread",
        dir_kind_label="per-field ip_spread",
        cleanup_label="ip_spread cleanup",
        logger=logger,
    )


install_shim_module(__name__, (_day_compaction_sql, _partial_hour), bidirectional_seed=True)
