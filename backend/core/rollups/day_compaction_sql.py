"""Per-feature day compaction SQL builders and compactors."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.utils.telemetry import parse_day_key_strict

from ._common import (
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
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    _bucket_min_copy_sql,
    _day_bundled_root,
    _sum_count_copy_sql,
    compact_closed_days,
    parquet_zstd_target,
    publish_temp_parquet,
    quote_path_list,
    wavg,
)
from .security_dims import _purge_stale_ngwaf_bots_parquets

logger = logging.getLogger(__name__)


def _make_day_compactor(
    jobs: list[tuple[str, str, Callable[[str, str], str]]],
    *,
    pre_hook: Callable[[dict[str, Any]], object] | None = None,
) -> Callable[..., int]:
    def _compact(
        service_id: str,
        source: dict[str, Any],
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


def rebuild_time_series_days(service_id: str, source: dict[str, Any], days: list[str]) -> int:
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
                publish_temp_parquet(tmp_file, day_file, str(source.get("name", "default")))
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
