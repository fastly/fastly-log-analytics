from __future__ import annotations

import logging
import os
import shutil
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from backend.utils.sql_validator import escape_sql_literal
from backend.utils.telemetry import parse_day_key_strict, parse_hour_key_strict

if TYPE_CHECKING:
    import duckdb
from ._common import (
    DAY_BUNDLE_FILENAME,
    DAY_BUNDLE_TOP_K,
    IP_SPREAD_BUNDLE_FILENAME,
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_DIMS_BUNDLE_TOP_K,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    PERF_LATENCY_BUNDLE_TOP_K,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_TOP_K,
    TIME_SERIES_BUNDLE_FILENAME,
    TOP_K,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _bucket_min_copy_sql,
    _build_copy_query,
    _closed_day_window,
    _day_bundled_root,
    _day_rollups_root,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    _rollups_root,
    _sum_count_copy_sql,
    cleanup_per_field_after_bundle,
    collect_per_field_partition_parquets,
    compact_closed_days,
    parquet_zstd_target,
    parse_hour_token,
    publish_temp_parquet,
    quote_path_list,
    unlink_quiet,
    wavg,
)
from .security_dims import _purge_stale_ngwaf_bots_parquets

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
                for (f,) in con.execute(f"SELECT DISTINCT field FROM read_parquet([{paths_sql}])").fetchall():
                    if isinstance(f, str) and _is_safe_ident(f):
                        field_set.add(f)
            except duckdb.Error:
                pass
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
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(os.path.getmtime(p) for p in all_source_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild
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


def _make_day_compactor(
    jobs: list[tuple[str, str, Callable[[str, str], str]]],
    *,
    pre_hook: Callable[[dict], object] | None = None,
) -> Callable[..., int]:
    def _compact(
        service_id: str,
        source: dict,
        lookback_days: int | None = None,
    ) -> int:
        if pre_hook is not None:
            pre_hook(source)
        return compact_closed_days(
            service_id,
            source,
            jobs=jobs,
            lookback_days=lookback_days,
            logger=logger,
        )

    return _compact


def _origin_summary_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST(SUM(total_misses) AS BIGINT) AS total_misses, "
        f"    CAST(SUM(total_passes) AS BIGINT) AS total_passes, "
        f"    CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
        f"    CAST({wavg('ottfb_p50_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p50_us, "
        f"    CAST({wavg('ottfb_p75_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p75_us, "
        f"    CAST({wavg('ottfb_p95_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p95_us, "
        f"    CAST({wavg('ottfb_p99_us', 'lat_us_count')} AS DOUBLE) AS ottfb_p99_us, "
        f"    CAST(SUM(ottlb_count) AS BIGINT) AS ottlb_count, "
        f"    CAST({wavg('ottlb_p50_us', 'ottlb_count')} AS DOUBLE) AS ottlb_p50_us, "
        f"    CAST({wavg('ottlb_p95_us', 'ottlb_count')} AS DOUBLE) AS ottlb_p95_us, "
        f"    CAST(SUM(cdn_ovh_count) AS BIGINT) AS cdn_ovh_count, "
        f"    CAST({wavg('cdn_ovh_p50_us', 'cdn_ovh_count')} AS DOUBLE) AS cdn_ovh_p50_us, "
        f"    CAST(SUM(ost_5xx_count) AS BIGINT) AS ost_5xx_count, "
        f"    CAST(SUM(ost_total_count) AS BIGINT) AS ost_total_count, "
        f"    CAST(SUM(obytes_count) AS BIGINT) AS obytes_count, "
        f"    CAST({wavg('obytes_p50', 'obytes_count')} AS DOUBLE) AS obytes_p50 "
        f"  FROM read_parquet([{paths_sql}])"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_origin_summary_closed_days_to_daily = _make_day_compactor(
    [(ORIGIN_SUMMARY_BUNDLE_FILENAME, ".tmp_os_", _origin_summary_copy_sql)]
)


def _network_rtt_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT asn, "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST(SUM(rtt_count) AS BIGINT) AS rtt_count, "
        f"    CAST({wavg('p95_us', 'rtt_count')} AS DOUBLE) AS p95_us, "
        f"    CAST({wavg('p99_us', 'rtt_count')} AS DOUBLE) AS p99_us "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY asn"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_network_rtt_closed_days_to_daily = _make_day_compactor(
    [(NETWORK_RTT_BUNDLE_FILENAME, ".tmp_nr_", _network_rtt_copy_sql)]
)
compact_network_speed_closed_days_to_daily = _make_day_compactor(
    [(NETWORK_SPEED_BUNDLE_FILENAME, ".tmp_ns_", _sum_count_copy_sql("asn, c_speed"))]
)
compact_ngwaf_bots_closed_days_to_daily = _make_day_compactor(
    [(NGWAF_BOTS_BUNDLE_FILENAME, ".tmp_nb_", _sum_count_copy_sql("bot_name, wellknown_bot_name, category"))],
    pre_hook=_purge_stale_ngwaf_bots_parquets,
)
compact_ngwaf_bots_ts_closed_days_to_daily = _make_day_compactor(
    [(NGWAF_BOTS_TS_BUNDLE_FILENAME, ".tmp_nbts_", _sum_count_copy_sql("bucket_ts, bot_name"))]
)
compact_verified_bots_ts_closed_days_to_daily = _make_day_compactor(
    [(VERIFIED_BOTS_TS_BUNDLE_FILENAME, ".tmp_vbts_", _sum_count_copy_sql("bucket_ts, bot_type"))]
)


def _origin_latency_ts_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT bucket_ts, "
        f"    CAST(SUM(ttfb_count) AS BIGINT) AS ttfb_count, "
        f"    {wavg('ttfb_p50_us', 'ttfb_count')} AS ttfb_p50_us, "
        f"    {wavg('ttfb_p95_us', 'ttfb_count')} AS ttfb_p95_us, "
        f"    {wavg('ttfb_p99_us', 'ttfb_count')} AS ttfb_p99_us, "
        f"    CAST(SUM(ttlb_count) AS BIGINT) AS ttlb_count, "
        f"    {wavg('ttlb_p50_us', 'ttlb_count')} AS ttlb_p50_us, "
        f"    {wavg('ttlb_p95_us', 'ttlb_count')} AS ttlb_p95_us, "
        f"    {wavg('ttlb_p99_us', 'ttlb_count')} AS ttlb_p99_us "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY bucket_ts"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_origin_latency_ts_closed_days_to_daily = _make_day_compactor(
    [(ORIGIN_LATENCY_TS_BUNDLE_FILENAME, ".tmp_olts_", _origin_latency_ts_copy_sql)]
)


def _perf_latency_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT value, requests, elapsed_count, elapsed_sum, p50_us, p95_us, p99_us FROM ("
        f"    SELECT value, "
        f"      CAST(SUM(requests) AS BIGINT) AS requests, "
        f"      CAST(SUM(elapsed_count) AS BIGINT) AS elapsed_count, "
        f"      CAST(SUM(elapsed_sum) AS DOUBLE) AS elapsed_sum, "
        f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
        f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
        f"      CAST({wavg('p99_us', 'requests')} AS DOUBLE) AS p99_us, "
        f"      ROW_NUMBER() OVER ("
        f"        ORDER BY {wavg('p99_us', 'requests')} DESC"
        f") AS rn "
        f"    FROM read_parquet([{paths_sql}]) "
        f"    GROUP BY value"
        f"  ) WHERE rn <= {PERF_LATENCY_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_perf_latency_closed_days_to_daily = _make_day_compactor(
    [
        (PERF_TOP_URLS_BUNDLE_FILENAME, ".tmp_pl_", _perf_latency_copy_sql),
        (PERF_TOP_ASNS_BUNDLE_FILENAME, ".tmp_pl_", _perf_latency_copy_sql),
    ]
)


def _origin_pop_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT pop, requests, lat_us_count, lat_us_sum, p50_us, p95_us FROM ("
        f"    SELECT pop, "
        f"      CAST(SUM(requests) AS BIGINT) AS requests, "
        f"      CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
        f"      CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
        f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
        f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
        f"      ROW_NUMBER() OVER (ORDER BY SUM(requests) DESC) AS rn "
        f"    FROM read_parquet([{paths_sql}]) "
        f"    GROUP BY pop"
        f"  ) WHERE rn <= {ORIGIN_DIMS_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_file)}"
    )


def _origin_ip_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT oip, requests, lat_us_count, lat_us_sum, p50_us, p95_us, "
        f"         ost_5xx_count, ost_total_count FROM ("
        f"    SELECT oip, "
        f"      CAST(SUM(requests) AS BIGINT) AS requests, "
        f"      CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
        f"      CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
        f"      CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
        f"      CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us, "
        f"      CAST(SUM(ost_5xx_count) AS BIGINT) AS ost_5xx_count, "
        f"      CAST(SUM(ost_total_count) AS BIGINT) AS ost_total_count, "
        f"      ROW_NUMBER() OVER (ORDER BY SUM(requests) DESC) AS rn "
        f"    FROM read_parquet([{paths_sql}]) "
        f"    GROUP BY oip"
        f"  ) WHERE rn <= {ORIGIN_DIMS_BUNDLE_TOP_K}"
        f") {parquet_zstd_target(tmp_file)}"
    )


def _origin_path_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT edge, "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST(SUM(lat_us_count) AS BIGINT) AS lat_us_count, "
        f"    CAST(SUM(lat_us_sum) AS DOUBLE) AS lat_us_sum, "
        f"    CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us, "
        f"    CAST({wavg('p95_us', 'requests')} AS DOUBLE) AS p95_us "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY edge"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_origin_dims_closed_days_to_daily = _make_day_compactor(
    [
        (ORIGIN_POP_BUNDLE_FILENAME, ".tmp_od_", _origin_pop_copy_sql),
        (ORIGIN_IP_BUNDLE_FILENAME, ".tmp_od_", _origin_ip_copy_sql),
        (ORIGIN_PATH_BUNDLE_FILENAME, ".tmp_od_", _origin_path_copy_sql),
    ]
)


def _security_topips_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT ip, max_header FROM ("
        f"    SELECT ip, CAST(MAX(max_header) AS BIGINT) AS max_header "
        f"    FROM read_parquet([{paths_sql}]) "
        f"    GROUP BY ip "
        f"    ORDER BY max_header DESC "
        f"    LIMIT {SECURITY_TOPIPS_BUNDLE_TOP_K}"
        f"  )"
        f") {parquet_zstd_target(tmp_file)}"
    )


def _security_cov_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    CAST(SUM(total_rows) AS BIGINT) AS total_rows, "
        f"    CAST(SUM(tls_populated) AS BIGINT) AS tls_populated "
        f"  FROM read_parquet([{paths_sql}])"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_security_dims_closed_days_to_daily = _make_day_compactor(
    [
        (SECURITY_REQ_SIZE_BUNDLE_FILENAME, ".tmp_sd_", _bucket_min_copy_sql("min_val")),
        (SECURITY_CONN_REUSE_BUNDLE_FILENAME, ".tmp_sd_", _bucket_min_copy_sql("min_val")),
        (SECURITY_TOPIPS_BUNDLE_FILENAME, ".tmp_sd_", _security_topips_copy_sql),
        (SECURITY_COV_BUNDLE_FILENAME, ".tmp_sd_", _security_cov_copy_sql),
    ]
)


def _security_flagged_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, CAST(SUM(cnt) AS BIGINT) AS cnt "
        f"  FROM read_parquet([{paths_sql}]) GROUP BY ALL"
        f") {parquet_zstd_target(tmp_file)}"
    )


def _security_bot_eff_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  WITH merged AS ("
        f"    SELECT kind, key, CAST(SUM(cnt) AS BIGINT) AS cnt, CAST(SUM(resp_bytes) AS BIGINT) AS resp_bytes "
        f"    FROM read_parquet([{paths_sql}]) GROUP BY kind, key"
        f"  ), "
        f"  ranked AS ("
        f"    SELECT *, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY resp_bytes DESC, cnt DESC) AS rn FROM merged"
        f") "
        f"  SELECT kind, key, cnt, resp_bytes FROM ranked "
        f"  WHERE kind != 'ua' OR rn <= {SECURITY_BOT_EFF_UA_TOP_K}"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_security_proxies_closed_days_to_daily = _make_day_compactor(
    [
        (SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, ".tmp_sp_", _sum_count_copy_sql("type")),
        (SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME, ".tmp_sp_", _security_flagged_copy_sql),
        (SECURITY_BOT_EFF_BUNDLE_FILENAME, ".tmp_sp_", _security_bot_eff_copy_sql),
    ]
)
compact_perf_dims_closed_days_to_daily = _make_day_compactor(
    [(PERF_TTL_DIST_BUNDLE_FILENAME, ".tmp_pd_", _bucket_min_copy_sql("min_ttl"))]
)


def _overview_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT hour_start, "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST(SUM(hit_requests) AS BIGINT) AS hit_requests, "
        f"    CAST(SUM(miss_requests) AS BIGINT) AS miss_requests, "
        f"    CAST(SUM(pass_requests) AS BIGINT) AS pass_requests, "
        f"    CAST(SUM(synth_requests) AS BIGINT) AS synth_requests, "
        f"    CAST(SUM(origin_requests) AS BIGINT) AS origin_requests, "
        f"    CAST(SUM(bandwidth_saved_bytes) AS BIGINT) AS bandwidth_saved_bytes, "
        f"    CAST(SUM(total_bandwidth_bytes) AS BIGINT) AS total_bandwidth_bytes, "
        f"    CAST(SUM(shield_hit_requests) AS BIGINT) AS shield_hit_requests, "
        f"    CAST(SUM(shield_total_requests) AS BIGINT) AS shield_total_requests, "
        f"    CAST(SUM(threats_blocked) AS BIGINT) AS threats_blocked, "
        f"    CAST(SUM(hit_elapsed_sum) AS DOUBLE) AS hit_elapsed_sum, "
        f"    CAST(SUM(hit_elapsed_count) AS BIGINT) AS hit_elapsed_count, "
        f"    CAST(SUM(miss_elapsed_sum) AS DOUBLE) AS miss_elapsed_sum, "
        f"    CAST(SUM(miss_elapsed_count) AS BIGINT) AS miss_elapsed_count "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY hour_start"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_overview_closed_days_to_daily = _make_day_compactor(
    [(OVERVIEW_BUNDLE_FILENAME, ".tmp_ov_", _overview_copy_sql)]
)


def _time_series_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"    bucket, "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST(SUM(status_4xx) AS BIGINT) AS status_4xx, "
        f"    CAST(SUM(status_5xx) AS BIGINT) AS status_5xx, "
        f"    CAST(SUM(hits) AS BIGINT) AS hits, "
        f"    CAST(SUM(cache_total) AS BIGINT) AS cache_total, "
        f"    CAST(SUM(resp_bytes_sum) AS BIGINT) AS resp_bytes_sum, "
        f"    CAST(SUM(ttfb_sum) AS DOUBLE) AS ttfb_sum, "
        f"    CAST(SUM(ttfb_count) AS BIGINT) AS ttfb_count "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY bucket "
        f"  ORDER BY bucket"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_time_series_closed_days_to_daily = _make_day_compactor(
    [(TIME_SERIES_BUNDLE_FILENAME, ".tmp_ts_day_", _time_series_copy_sql)]
)


def rebuild_time_series_days(service_id: str, source: dict, days: list[str]) -> int:
    from contextlib import closing
    from tempfile import TemporaryDirectory

    import duckdb

    from backend.core.duckdb import get_memory_connection

    from .recompute import build_time_series_bundles

    starts = [parse_day_key_strict(day) for day in days]
    if any(start.strftime("%Y-%m-%d") != day for day, start in zip(days, starts, strict=True)):
        raise ValueError("invalid rollup day")
    if len(set(days)) != len(days):
        raise ValueError("duplicate rollup days")
    active_day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    if any(start >= active_day for start in starts):
        raise ValueError("rollup day is not closed")
    rebuilt = 0
    for day, start in zip(days, starts, strict=True):
        hours = [(start + timedelta(hours=h)).strftime("%Y-%m-%d-%H") for h in range(24)]
        day_dir = os.path.join(_day_bundled_root(source), f"day={day}")
        os.makedirs(day_dir, exist_ok=True)
        day_file = os.path.join(day_dir, TIME_SERIES_BUNDLE_FILENAME)
        try:
            with TemporaryDirectory(prefix=".tmp_ts_repair_", dir=day_dir) as staging:
                if build_time_series_bundles(service_id, source, hours, output_root=staging) != 24:
                    raise RuntimeError("incomplete hourly recompute; existing day preserved")
                paths = [os.path.join(staging, f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME) for hour in hours]
                tmp_file = os.path.join(staging, TIME_SERIES_BUNDLE_FILENAME)
                with closing(get_memory_connection()) as con:
                    con.execute(_time_series_copy_sql(quote_path_list(paths), tmp_file))
                publish_temp_parquet(tmp_file, day_file, source.get("name", "default"))
        except (duckdb.Error, OSError) as exc:
            raise RuntimeError("time-series day rebuild failed; existing day preserved") from exc
        rebuilt += 1
    logger.info("[rollups] %s: repaired %d selected time_series day(s)", service_id, rebuilt)
    return rebuilt


def _pop_health_copy_sql(paths_sql: str, tmp_file: str) -> str:
    return (
        f"COPY ("
        f"  SELECT "
        f"      CAST(pop AS VARCHAR) AS pop, "
        f"      CAST(SUM(requests) AS BIGINT) AS requests, "
        f"      CAST(SUM(errors) AS BIGINT) AS errors, "
        f"      CAST(SUM(cache_hits) AS BIGINT) AS cache_hits, "
        f"      CAST(SUM(bandwidth_bytes) AS BIGINT) AS bandwidth_bytes, "
        f"      CAST({wavg('p50_rtt_us', 'requests')} AS DOUBLE) AS p50_rtt_us, "
        f"      CAST({wavg('p95_ttfb_ms', 'requests')} AS DOUBLE) AS p95_ttfb_ms "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY pop"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_pop_health_closed_days_to_daily = _make_day_compactor(
    [(POP_HEALTH_BUNDLE_FILENAME, ".tmp_ph_", _pop_health_copy_sql)]
)


def _network_quality_copy_sql(include_country: bool) -> Callable[[str, str], str]:
    country_select = ", country" if include_country else ""
    return lambda paths_sql, tmp_file: (
        f"COPY ("
        f"  SELECT dim_val {country_select}, "
        f"    CAST(SUM(requests) AS BIGINT) AS requests, "
        f"    CAST({wavg('p50_us', 'requests')} AS DOUBLE) AS p50_us "
        f"  FROM read_parquet([{paths_sql}]) "
        f"  GROUP BY dim_val {country_select} "
        f"  ORDER BY requests DESC LIMIT 100"
        f") {parquet_zstd_target(tmp_file)}"
    )


compact_network_quality_closed_days_to_daily = _make_day_compactor(
    [
        (NETWORK_QUALITY_COUNTRY_FILENAME, ".tmp_nq_", _network_quality_copy_sql(False)),
        (NETWORK_QUALITY_ASN_FILENAME, ".tmp_nq_", _network_quality_copy_sql(False)),
        (NETWORK_QUALITY_REGION_FILENAME, ".tmp_nq_", _network_quality_copy_sql(True)),
        (NETWORK_QUALITY_POP_FILENAME, ".tmp_nq_", _network_quality_copy_sql(False)),
    ]
)


def retire_compacted_hour_bundles(
    service_id: str,
    source: dict,
    lookback_days: int | None = 30,
) -> tuple[int, int]:
    from backend.core.duckdb import get_memory_connection
    from backend.core.iceberg.view import _get_service_lock

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
    from backend.core.iceberg import is_stale_view_error

    def _run_once() -> T:
        if con is not None:
            return query_fn(con)
        from backend.core.duckdb import get_memory_connection

        tmp_con = get_memory_connection()
        try:
            return query_fn(tmp_con)
        finally:
            tmp_con.close()

    try:
        return _run_once()
    except Exception as e:
        if is_stale_view_error(e) or "Cannot open file" in str(e) or "No such file" in str(e):
            time.sleep(0.01)
            try:
                return _run_once()
            except Exception as e2:
                logger.warning("[partial_hour] Retry %s failed on %s: %s", err_label, path, e2)
                return default
        logger.warning("[partial_hour] Failed %s on %s: %s", err_label, path, e)
        if any(err in str(e).lower() for err in ("corrupt", "not a parquet file", "bad magic number", "footer")):
            unlink_quiet(path)
        return default


def read_partial_hour_all_fields(
    source: dict, hour: str, con: duckdb.DuckDBPyConnection | None = None
) -> list[tuple[str, str, int]]:
    path = _all_fields_path(source, hour)
    sql = f"SELECT field, value, count FROM read_parquet('{path}') WHERE field != '{TOTAL_FIELD}'"

    def _query(c: duckdb.DuckDBPyConnection) -> list[tuple[str, str, int]]:
        rows = c.execute(sql).fetchall()
        return [(r[0], r[1], int(r[2])) for r in rows]

    return _read_partial_hour_parquet(path, con, _query, [], err_label="read")


def read_partial_hour_total(source: dict, hour: str, con: duckdb.DuckDBPyConnection | None = None) -> int:
    path = _all_fields_path(source, hour)
    sql = f"SELECT count FROM read_parquet('{path}') WHERE field = '{TOTAL_FIELD}' AND value = '' LIMIT 1"

    def _query(c: duckdb.DuckDBPyConnection) -> int:
        row = c.execute(sql).fetchone()
        return int(row[0]) if row else 0

    return _read_partial_hour_parquet(path, con, _query, 0, err_label="total read")


def _list_new_source_files(source: dict, hour: str, watermark: float) -> list[str]:
    from backend.core.duckdb import _cache_dir

    cache_dir = _cache_dir(source)
    dirs = [os.path.join(cache_dir, "buffer"), os.path.join(cache_dir, "data", f"timestamp_hour={hour}")]
    try:
        from backend.core.iceberg.buffer import _tombstoned_parquet_paths

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
    from backend.core.iceberg.view import _get_service_lock

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
            probe_con.execute(f"SELECT 1 FROM read_parquet('{existing_path}') LIMIT 1")
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
                from backend.core.iceberg import is_stale_view_error

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
    from backend.core.iceberg.view import _get_service_lock

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


sys.modules["backend.core.rollups.hour_bundles"] = sys.modules[__name__]
sys.modules["backend.core.rollups.partial_hour"] = sys.modules[__name__]
