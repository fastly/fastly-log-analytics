"""Recompute / backfill / cleanup for per-field per-hour rollups.

Holds the cron-triggered ``recompute_touched_hours`` (called after every
ingest tick), the one-shot ``backfill_rollups``
(boot + new-custom-field path), and ``cleanup_old_rollups`` (daily retention
trim).

The shared write core (``_run_per_field_copy``) lives here too — it's the
single COPY-PARTITION_BY path both recompute and backfill funnel through.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.utils.telemetry import ensure_utc

from .. import metric_expressions
from ._common import (
    _VIRTUAL_FIELD_BACKING,
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
    shared_rollup_session,
)
from .day_bundles import bundle_hours, bundle_hours_ip_spread

# Writers dispatched by name from spec.HOUR_WRITERS (resolved against this
# module at call time, so each name here is also the test patch point).
from .origin_dims import (  # noqa: F401  # noqa: F401
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
    """Recompute rollups for all dashboard fields across the given hours.

    Excludes the active (current UTC) hour — the dashboard serves the
    in-progress hour live off the base table. One COPY query per field
    handles all touched hours via PARTITION_BY, so the work is O(fields)
    not O(fields × hours).

    After the per-field rebuild completes, bundles each touched hour's
    per-field parquets into a single bundled file under
    ``rollups/hour_bundled/hour=H/all_fields.parquet`` so the dashboard
    reader can open one file per hour instead of ~40 per-field files.
    """
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

        # IP-spread rollup runs alongside the count rollup so the security
        # fingerprint cards can serve cross-hour distinct-IP estimates from
        # rollups without an ip column in the per-request temp scan.
        # Best-effort: a failure here doesn't block the count rollup or
        # the bundles below; the reader falls back to the live temp scan
        # when ip_spread.parquet is missing.
        try:
            _run_ip_spread_per_field(service_id, source, table_ident, where_sql, fields_to_rollup)
        except Exception as e:
            logger.warning(
                "[rollups] %s: ip_spread per-field write failed (count rollup unaffected): %s",
                service_id,
                e,
            )

        # Bundle the touched hours so the dashboard reader can open one
        # file per hour instead of N per-field files. Best-effort: if
        # bundling fails, the per-field files still serve correctly via
        # the reader's fallback path.
        try:
            bundle_hours(service_id, source, touched_hours)
        except Exception as e:
            logger.warning("[rollups] %s: hour bundling failed (per-field still serves): %s", service_id, e)

        # IP-spread bundle runs alongside the count bundle. Best-effort:
        # a failure here leaves per-field ip_spread parquets intact and
        # the reader's per-field path serves until the next bundling tick.
        try:
            bundle_hours_ip_spread(service_id, source, touched_hours)
        except Exception as e:
            logger.warning(
                "[rollups] %s: ip_spread hour bundling failed (per-field ip_spread still serves): %s",
                service_id,
                e,
            )

        # Per-hour feature rollups (see spec.HOUR_WRITERS). Same best-effort
        # contract for every writer: a failure is logged and leaves the affected
        # hours on the spec's fallback path (raw scan / direct join) without
        # blocking the writers after it.
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
    """Self-heal pass: find closed hours where the iceberg view has rows
    but no ``hour_bundled/`` file exists, then rebuild per-field rollups
    + bundle for those hours.

    The ``recompute_touched_hours`` hot path is driven by the ingest
    pipeline's "touched hours" set — if ingest under-reports (sync delay,
    a retried ingest commit that landed AFTER the recompute fired for
    that hour, the hour was never seen as touched at all), the per-field
    rollup never gets written and the hour_bundler then silently skips
    the hour ("no per-field files exist (nothing to bundle)"). The
    dashboard's reader has no fallback path — those rows just disappear
    from top-N panels.

    This driver checks for that gap directly against the iceberg view
    and runs the rebuild via the same code path the cron tick uses. Safe
    to call on every daily compaction tick — idempotent (no-op when the
    bundle tree is complete).

    ``max_missing_hours`` bounds the number of non-empty hours rebuilt in one
    pass. This lets scheduled durable-serving catch-up make incremental
    progress without allowing one large DuckDB operation to evict the serving
    pod. Coverage remains unverified until a later pass confirms the complete
    lookback.

    Returns a summary dict: ``{"missing": N, "rebuilt_fields": F,
    "bundled": B, "stamped_empty": E}`` so callers can log a concise
    progress line. ``stamped_empty`` counts zero-row closed hours that
    received an empty sentinel bundle (see
    :func:`~backend.core.rollups.hour_bundles.stamp_empty_hour_sentinels`)
    so the reader can tell verified-empty apart from writer-gap.
    """
    import duckdb as _ddb

    from backend.core.iceberg import update_iceberg_view

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

    # Discover hours with data via the iceberg view. We use the view (not
    # the per-service .duckdb file directly) so this runs concurrently
    # with uvicorn's RW connection on that file — backed by a fresh
    # in-memory DuckDB connection that holds no persistent state.
    con = _ddb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC'")
        from backend.core.duckdb import _configure_fos

        _configure_fos(con, source)
        update_iceberg_view(con, source)
        from backend import config as svcconfig

        if svcconfig.is_durable_serving_mode(source):
            lake_row = con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake' LIMIT 1").fetchone()
            if lake_row is None:
                logger.warning(
                    "[rollups] %s: backfill_missing_hour_bundles could not attach DuckLake catalog",
                    service_id,
                )
                return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}
        # The view's actual SQL identifier is set by update_iceberg_view
        # — query through ``information_schema`` to find it rather than
        # guess (the name is derived from source["name"]/svc_name, not
        # source["service_id"]).
        from backend.core.duckdb import _safe_table_name

        view_name = _safe_table_name(source.get("name") or service_id)
        view_row = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [view_name],
        ).fetchone()
        if view_row is None:
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}
        view_name = view_row[0]
        if not _is_safe_ident(view_name):
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": False}
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

        # Detect partial/incomplete bundles: if an existing hour bundle is
        # missing core fields that are present in the view (e.g. ``ip``,
        # ``status``, ``method``, ``pop`` — which happened when a single-field
        # per-hour write + bundle pass wiped previously bundled fields), treat
        # that hour as missing so ``recompute_touched_hours`` rebuilds all
        # configured fields for it.
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

        # Closed hours with genuinely ZERO rows can never acquire coverage
        # through the data-driven writers (the HAVING n > 0 above never
        # surfaces them) — stamp those with an empty sentinel bundle so the
        # reader's missing-hour live heal doesn't classify them as writer
        # gaps and re-scan them on every request forever.
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

        # Reuse recompute_touched_hours so the full per-field write +
        # post-write bundle pipeline runs. recompute_touched_hours
        # opens its own RO connection on the per-service .duckdb file,
        # which contends with uvicorn — close ours first.
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

    # Verify how many bundles materialised. (recompute_touched_hours
    # bundles internally, but a per-hour COPY can yield 0 rows for a
    # field that genuinely has no values in that hour — leaving the
    # bundle un-written for that hour. The count below is what's on
    # disk after the call.)
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
    """Self-heal pass for the IP-spread bundle tree.

    The IP-spread rollup shipped after the count rollup was already
    backfilled for most services, so a daily-compaction tick on a
    pre-existing service will discover closed hours where
    ``rollups/hour_bundled/hour=H/all_fields.parquet`` exists but
    ``all_fields_ip.parquet`` does NOT. Walks the bundled-hour tree,
    finds those hours, and re-runs :func:`recompute_touched_hours`
    against them — which writes per-field ip_spread parquets AND
    bundles them in one pass.

    Bounded by ``lookback_days`` (default 30) so a one-time backfill
    can't run away on a long-lived service. Idempotent — already-
    populated hours are skipped via the ``all_fields_ip.parquet``
    presence check.

    Returns ``{"missing": N, "rebuilt": M}``. ``rebuilt`` is the
    count we asked recompute to rebuild; the actual per-hour parquet
    count depends on which fields had data, which the count rollup
    already tracks (and which doesn't affect the bundle's existence).
    """
    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(bundled_root):
        # No bundle tree yet — backfill_missing_hour_bundles will create
        # the initial bundles + ip_spread on its next pass; nothing to
        # heal independently here.
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
            if parsed is None:
                continue
            if parsed < start_dt or parsed >= end_dt:
                continue
            ip_bundle = os.path.join(bundled_root, entry, IP_SPREAD_BUNDLE_FILENAME)
            if not os.path.isfile(ip_bundle):
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

    # Re-trigger the full recompute for these hours. That writes per-
    # field count + ip_spread, then bundles both — covers both the
    # case where ip_spread was never built and the case where the
    # bundle is missing but per-field ip_spread exists (idempotent
    # re-bundle is cheap).
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
    """Delete per-hour and per-day rollup directories older than retention limits.

    ``max_age_days <= 0`` disables cleanup (keep everything). Returns the
    number of directories deleted. Safe to call concurrently with the
    writers because we only ever delete hours/days STRICTLY older than the
    cutoff — current and just-written hours/days are never candidates.
    """
    if max_age_days <= 0:
        return 0

    cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).strftime("%Y-%m-%d-%H")
    day_cutoff = cutoff[:10]
    if hour_bundle_max_age_days and hour_bundle_max_age_days > 0:
        hour_cutoff = (datetime.now(UTC) - timedelta(days=hour_bundle_max_age_days)).strftime("%Y-%m-%d-%H")
    else:
        hour_cutoff = cutoff

    deleted = 0

    # 1 & 2. rollups/hour/field=*/hour=* and rollups/hour_ip_spread/field=*/hour=*
    for root_dir, walk_label in (
        (_rollups_root(source), "hour"),
        (_ip_spread_root(source), "ip_spread"),
    ):
        if not os.path.isdir(root_dir):
            continue
        try:
            for field_entry in os.listdir(root_dir):
                if not field_entry.startswith("field="):
                    continue
                field_dir = os.path.join(root_dir, field_entry)
                try:
                    deleted += _prune_partition_dirs(field_dir, hour_cutoff, "hour=")
                except OSError:
                    continue
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (%s): %s", service_id, walk_label, e)

    # 3. rollups/day_bundled/day=*
    day_bundled_root = _day_bundled_root(source)
    if os.path.isdir(day_bundled_root):
        try:
            deleted += _prune_partition_dirs(day_bundled_root, day_cutoff, "day=")
        except OSError as e:
            logger.warning("[rollups] cleanup walk failed for %s (day_bundled): %s", service_id, e)

    # 4. rollups/day/field=*/day=* (per-field day files redundant when day_bundled exists)
    day_root = _day_rollups_root(source)
    if os.path.isdir(day_root):
        try:
            for field_entry in os.listdir(day_root):
                if not field_entry.startswith("field="):
                    continue
                field_dir = os.path.join(day_root, field_entry)
                try:
                    deleted += _prune_partition_dirs(
                        field_dir,
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

    # 5. rollups/hour_bundled/hour=*
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
    """Shared core of recompute_touched_hours and backfill_rollups.

    One COPY query per field, writing to a per-field temp directory via
    PARTITION_BY (field, hour), then publishing each hour-dir under the
    per-service iceberg lock.
    """
    from backend.core.duckdb import _cache_dir
    from backend.core.iceberg import execute_with_stale_view_retry
    from backend.core.iceberg.view import _get_service_lock

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
                # Belt-and-suspenders — _get_fields already filters, but
                # defend against direct callers passing raw names.
                logger.warning("[rollups] skipping unsafe field name: %r", field)
                continue
            # Virtual fields rollup the unnested CSV column instead of the
            # column itself — skip-test on the BACKING column, not the
            # virtual name (which never exists in the table schema).
            backing_col = _VIRTUAL_FIELD_BACKING.get(field)
            if backing_col is not None:
                if backing_col not in cols or not _is_safe_ident(backing_col):
                    continue
            elif field not in cols:
                continue

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp", f"{field}_{uuid.uuid4().hex[:8]}")
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
            os.makedirs(tmp_field_dir, exist_ok=True)

            if backing_col is not None:
                inner = _build_virtual_field_copy_query(table_ident, field, backing_col, where_sql)
            else:
                inner = _build_copy_query(table_ident, field, where_sql)
            query = (
                f"COPY ({inner}) TO '{tmp_field_dir}' "
                "(FORMAT PARQUET, PARTITION_BY (field, hour), OVERWRITE_OR_IGNORE, COMPRESSION ZSTD)"
            )
            try:
                execute_with_stale_view_retry(con, source, lambda c: c.execute(query))
            except Exception as e:  # noqa: BLE001 — DuckDB raises typed errors but iceberg may wrap them
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
    """Build the per-(field, hour, value) HLL IP-spread parquets.

    Parallel writer to :func:`_run_per_field_copy` for the count rollup.
    Differs in that it can't run as a pure SQL ``COPY`` — DuckDB 1.5.3
    doesn't expose HyperLogLog sketches as values, so the writer pulls
    each ``(field, hour, value, ip_sample, ip_count_observed)`` row
    into Python via Arrow, builds the sketch with
    :class:`backend.utils.hll.HyperLogLog`, then writes the sketch
    bytes + observed count + cap flag back out as parquet via
    pyarrow's ``write_to_dataset``. The output tree mirrors the count
    rollup's hive layout (``field=X/hour=Y/<file>.parquet``) so the
    same ``_publish_field_partitions`` helper publishes both atomically
    under the per-service iceberg lock.

    Skips entirely when the table has no ``ip`` column or the
    field-list doesn't intersect the schema; virtual fields are
    skipped here too (the IP-spread question only makes sense for raw
    indexed columns, not the unnested CSV virtual fields). A failure
    on one field logs + continues so the rest of the fields still
    publish on the same cron tick.
    """
    from backend.core.duckdb import _cache_dir
    from backend.core.iceberg import execute_with_stale_view_retry
    from backend.core.iceberg.view import _get_service_lock
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
        if cols is None:
            return
        if "ip" not in cols:
            # Schema doesn't carry ip — no IP-spread to build. The
            # security FE handles this case today (the fingerprint
            # cards return empty when "ip" isn't present); the
            # rollup writer's silence is the right signal.
            return

        for field in fields:
            if not _is_safe_ident(field):
                logger.warning("[rollups] ip_spread: skipping unsafe field name: %r", field)
                continue
            # Virtual fields wouldn't add value here — the unnested
            # signals (waf_sig elements, etc.) don't have a 1:1 IP
            # relationship the way raw fingerprint columns do, so
            # skip them and let the count rollup cover them alone.
            if field in _VIRTUAL_FIELD_BACKING:
                continue
            if field not in cols:
                continue

            select_sql = _build_ip_spread_select_query(table_ident, field, where_sql)
            try:
                # ``.to_arrow_table()`` materializes the whole result
                # set as a pyarrow.Table; ``.arrow()`` returns a
                # streaming RecordBatchReader (no ``num_rows`` attr).
                # We need num_rows + column slicing below, so the
                # eager path is the right pick. ``fetch_arrow_table``
                # is the legacy alias of the same call and emits a
                # DeprecationWarning under DuckDB 1.5+.
                #
                # Routed through the stale-view self-heal (Trap #35) so a
                # cold-start DuckLake detach race (``schema "lake" does
                # not exist``) re-attaches + retries instead of dropping
                # the field — same hop the describe path already takes.
                arrow_table = execute_with_stale_view_retry(
                    con, source, lambda c: c.execute(select_sql).to_arrow_table()
                )
            except Exception as e:  # noqa: BLE001 — DuckDB raises typed errors but iceberg may wrap them
                logger.warning(
                    "[rollups] %s: ip_spread SELECT failed for field=%s: %s",
                    service_id,
                    field,
                    e,
                )
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
                        # array_agg(DISTINCT) skips NULLs in DuckDB but
                        # be defensive — a future ip-normalisation pass
                        # might leave empty strings or accidental Nones.
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
                # partition_cols drops the (field, hour) values from the
                # parquet data files — they live in the hive path only,
                # which matches how the reader will read them via
                # read_parquet(..., hive_partitioning=1).
                pq.write_to_dataset(
                    output,
                    root_path=tmp_field_dir,
                    partition_cols=["field", "hour"],
                    compression="zstd",
                    existing_data_behavior="overwrite_or_ignore",
                )
            except Exception as e:
                logger.warning(
                    "[rollups] %s: ip_spread parquet write failed for field=%s: %s",
                    service_id,
                    field,
                    e,
                )
                shutil.rmtree(tmp_field_dir, ignore_errors=True)
                continue

            with _get_service_lock(lock_key):
                _publish_field_partitions(tmp_field_dir, ip_spread_dir, field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
    finally:
        if owns_con:
            con.close()


# ---------------------------------------------------------------------------
# Per-hour time-series, overview, and session bundle writers
# (formerly backend.core.rollups.time_series)
# ---------------------------------------------------------------------------


def build_time_series_bundles(
    service_id: str, source: dict, hours: list[str], *, output_root: str | None = None
) -> int:
    """Write a 1-minute time_series rollup for each closed hour in ``hours``."""

    def eligibility(cols: set[str], table_ident: str) -> str | None:
        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping time_series bundle",
                service_id,
                table_ident,
            )
            return None

        select_parts = [
            "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]
        if "status" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS BIGINT) AS status_4xx")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status >= 500) AS BIGINT) AS status_5xx")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS status_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS status_5xx")

        if "cache" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS hits")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IS NOT NULL) AS BIGINT) AS cache_total")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS hits")
            select_parts.append("CAST(0 AS BIGINT) AS cache_total")

        if "resp_bytes" in cols:
            resp_bytes_expr = metric_expressions.sum_or_zero("resp_bytes")
            select_parts.append(f"CAST({resp_bytes_expr} AS BIGINT) AS resp_bytes_sum")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS resp_bytes_sum")

        if "ttfb" in cols:
            ttfb_expr = metric_expressions.sum_or_zero("ttfb", zero="0.0")
            select_parts.append(f"CAST({ttfb_expr} AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL) AS BIGINT) AS ttfb_count")
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(0 AS BIGINT) AS ttfb_count")

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f"GROUP BY 1) "
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
                "[rollups] %s: no `timestamp` column on %s; skipping overview bundle",
                service_id,
                table_ident,
            )
            return None

        select_parts: list[str] = [
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]

        has_cache = "cache" in cols
        if has_cache:
            select_parts.append(f"CAST(COUNT(*) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'PASS') AS BIGINT) AS pass_requests")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache = 'SYNTH') AS BIGINT) AS synth_requests")
            select_parts.append(
                "CAST(COUNT(*) FILTER (WHERE cache IN ('MISS', 'PASS', 'SYNTH', 'ERROR')) AS BIGINT) AS origin_requests"
            )
        else:
            select_parts.extend(
                [
                    "CAST(0 AS BIGINT) AS hit_requests",
                    "CAST(0 AS BIGINT) AS miss_requests",
                    "CAST(0 AS BIGINT) AS pass_requests",
                    "CAST(0 AS BIGINT) AS synth_requests",
                    "CAST(0 AS BIGINT) AS origin_requests",
                ]
            )

        if "resp_bytes" in cols and has_cache:
            bw_saved = metric_expressions.sum_or_zero("resp_bytes", where=f"cache IN {_HIT_STATES}")
            select_parts.append(f"CAST({bw_saved} AS BIGINT) AS bandwidth_saved_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS bandwidth_saved_bytes")

        if "resp_bytes" in cols:
            total_bw = metric_expressions.sum_or_zero("resp_bytes")
            select_parts.append(f"CAST({total_bw} AS BIGINT) AS total_bandwidth_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bandwidth_bytes")

        if "is_shield" in cols and has_cache:
            select_parts.append(
                f"CAST(COUNT(*) FILTER (WHERE is_shield = true AND cache IN {_HIT_STATES}) AS BIGINT)"
                " AS shield_hit_requests"
            )
            select_parts.append("CAST(COUNT(*) FILTER (WHERE is_shield = true) AS BIGINT) AS shield_total_requests")
        else:
            select_parts.extend(
                [
                    "CAST(0 AS BIGINT) AS shield_hit_requests",
                    "CAST(0 AS BIGINT) AS shield_total_requests",
                ]
            )

        if "waf" in cols and "waf_resp" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE waf = 1 AND waf_resp = 406) AS BIGINT) AS threats_blocked")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS threats_blocked")

        if "elapsed" in cols and has_cache:
            hit_el = metric_expressions.sum_or_zero("elapsed", zero="0.0", where=f"cache IN {_HIT_STATES}")
            miss_el = metric_expressions.sum_or_zero("elapsed", zero="0.0", where="cache = 'MISS'")
            select_parts.extend(
                [
                    f"CAST({hit_el} AS DOUBLE) AS hit_elapsed_sum",
                    f"CAST(COUNT(elapsed) FILTER (WHERE cache IN {_HIT_STATES}) AS BIGINT) AS hit_elapsed_count",
                    f"CAST({miss_el} AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(COUNT(elapsed) FILTER (WHERE cache = 'MISS') AS BIGINT) AS miss_elapsed_count",
                ]
            )
        else:
            select_parts.extend(
                [
                    "CAST(0.0 AS DOUBLE) AS hit_elapsed_sum",
                    "CAST(0 AS BIGINT) AS hit_elapsed_count",
                    "CAST(0.0 AS DOUBLE) AS miss_elapsed_sum",
                    "CAST(0 AS BIGINT) AS miss_elapsed_count",
                ]
            )

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT TIMESTAMPTZ '{start_iso}' AS hour_start, {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}') "
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
    """Build overview.parquet for every closed hour that already has
    ``all_fields.parquet`` but no ``overview.parquet``."""
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
        ]
        if "country" in cols:
            select_parts.append('CAST(MIN("country") AS VARCHAR) AS country')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS country")
        if "asn" in cols:
            select_parts.append('CAST(MIN("asn") AS INTEGER) AS asn')
        else:
            select_parts.append("CAST(NULL AS INTEGER) AS asn")
        if "status" in cols:
            select_parts.append(
                'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_4xx'
            )
            select_parts.append('CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_5xx')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS reqs_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS reqs_5xx")
        if "resp_bytes" in cols:
            tot_bytes = metric_expressions.sum_or_zero('"resp_bytes"')
            select_parts.append(f"CAST({tot_bytes} AS BIGINT) AS total_bytes")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bytes")
        if "tcp_rtt" in cols:
            rtt_s = metric_expressions.sum_or_zero('"tcp_rtt"', zero="0.0")
            select_parts.append(f"CAST({rtt_s} AS DOUBLE) AS rtt_sum")
            select_parts.append('CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT) AS rtt_count')
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS rtt_sum")
            select_parts.append("CAST(0 AS BIGINT) AS rtt_count")
        if "edge" in cols:
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT) AS edge_count')
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT) AS shield_count')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS edge_count")
            select_parts.append("CAST(0 AS BIGINT) AS shield_count")
        if "ua" in cols:
            select_parts.append('CAST(MIN("ua") AS VARCHAR) AS ua_min')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS ua_min")
        if "edge_sid" in cols:
            select_parts.append('CAST(MAX("edge_sid") AS VARCHAR) AS edge_sid_max')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS edge_sid_max")
        if "cmcd_sid" in cols:
            select_parts.append(
                'CAST(COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != \'\') AS BIGINT) AS cmcd_count'
            )
        else:
            select_parts.append("CAST(0 AS BIGINT) AS cmcd_count")

        return ",\n               ".join(select_parts)

    def build_copy_sql(select_sql: object, table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
        return (
            f"COPY (SELECT {select_sql} "
            f"FROM {table_ident} "
            f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
            f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
            f'AND "ip" IS NOT NULL '
            f"GROUP BY 1, 2, 3) "
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


for _legacy_mod in (
    "backend.core.rollups.time_series",
    "backend.core.rollups.overview",
    "backend.core.rollups.sessions",
):
    sys.modules[_legacy_mod] = sys.modules[__name__]


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
    """Return ``(beacon_count, latest_timestamp)`` for the RUM store.

    Freshness (``latest_timestamp``) is ALWAYS the raw beacon event-time
    MAX(timestamp), mirroring the request side (AGENTS.md Trap #39). The
    precomputed aggregate tables are only a fast *count* path, and only
    when they actually hold rows — an existing-but-empty aggregate (rollup
    lag, a failed recompute tick, or a service whose first rollup hasn't
    run yet) must fall through to a raw distinct-beacon count so the header
    never reports "Never" over live, queryable beacons.
    """
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

    # Raw fallback: aggregates missing, empty, or errored. Count distinct
    # beacons and read freshness straight from the raw tables.
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
    """Ensure RUM aggregate tables exist, find active hours, and update them idempotently.

    Serialized per service: the delete-then-insert below is not atomic, so two concurrent runs (commit cron, compaction
    heal, admin recompute) over the same hours each delete nothing and both insert, doubling every beacon count.
    """
    with _recompute_lock(service_id):
        _recompute_rum_aggregates_locked(con, service_id, hours)


def _recompute_rum_aggregates_locked(con: Any, service_id: str, hours: list[datetime] | list[str] | None) -> None:
    logger.info("[rum_rollups] Ensuring RUM aggregate schemas exist...")
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_vitals_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            event_count BIGINT,
            value_sum DOUBLE,
            good_count BIGINT,
            ni_count BIGINT,
            poor_count BIGINT,
            p50_value DOUBLE,
            p75_value DOUBLE,
            p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rum_error_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            error_count BIGINT
        )
    """)

    has_vitals = table_exists(con, "client_vitals")
    has_errors = table_exists(con, "client_errors")

    if not has_vitals and not has_errors:
        logger.info("[rum_rollups] %s: No client_vitals or client_errors table found. Skipping.", service_id)
        return

    # 2. Identify the hours that have raw data
    hours_set: set[datetime] = set()
    if hours is not None:
        for h in hours:
            if isinstance(h, str):
                try:
                    parsed_dt = _normalize_hour_dt(datetime.fromisoformat(h.replace("Z", "+00:00")))
                    hours_set.add(parsed_dt)
                except ValueError:
                    pass
            elif isinstance(h, datetime):
                hours_set.add(_normalize_hour_dt(h))
    else:
        if has_vitals:
            try:
                res = con.execute(
                    "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_vitals WHERE timestamp IS NOT NULL"
                ).fetchall()
                for r in res:
                    if r[0]:
                        dt_val = r[0]
                        if isinstance(dt_val, datetime):
                            dt_val = _normalize_hour_dt(dt_val)
                        hours_set.add(dt_val)
            except Exception as e:
                logger.warning("[rum_rollups] Failed to query active hours from client_vitals: %s", e)

        if has_errors:
            try:
                res = con.execute(
                    "SELECT DISTINCT DATE_TRUNC('hour', timestamp) FROM client_errors WHERE timestamp IS NOT NULL"
                ).fetchall()
                for r in res:
                    if r[0]:
                        dt_val = r[0]
                        if isinstance(dt_val, datetime):
                            dt_val = _normalize_hour_dt(dt_val)
                        hours_set.add(dt_val)
            except Exception as e:
                logger.warning("[rum_rollups] Failed to query active hours from client_errors: %s", e)

    if not hours_set:
        logger.info("[rum_rollups] %s: No active RUM data hours found. Skipping.", service_id)
        return

    if any(getattr(h, "tzinfo", None) is not None for h in hours_set):
        hours_set = {ensure_utc(h) for h in hours_set}

    # 3. Format hours as SQL TIMESTAMPTZ list
    hours_list = sorted(list(hours_set))
    hours_str = ", ".join(f"TIMESTAMPTZ '{h.isoformat()}'" for h in hours_list)

    logger.info(
        "[rum_rollups] %s: Recomputing aggregates for %d hours: %s",
        service_id,
        len(hours_list),
        [h.isoformat() for h in hours_list],
    )

    # 4. Delete existing aggregates for these hours
    con.execute(
        f"DELETE FROM rum_vitals_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )
    con.execute(
        f"DELETE FROM rum_error_aggregates WHERE service_id = ? AND bucket_start IN ({hours_str})", [service_id]
    )

    distinct_id = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

    # 5. Populate vitals aggregates
    if has_vitals:
        con.execute(
            f"""
            INSERT INTO rum_vitals_aggregates
            -- Dimension: total
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'total' AS dimension,
                CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, value

            UNION ALL

            -- Dimension: metric_name
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'metric_name' AS dimension,
                metric_name AS value,
                COUNT(*) AS event_count,
                CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
                COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
                COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
                COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count,
                CAST(MEDIAN(metric_value) AS DOUBLE) AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, metric_name

            UNION ALL

            -- Dimension: browser
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'browser' AS dimension,
                COALESCE(browser, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, browser

            UNION ALL

            -- Dimension: os
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'os' AS dimension,
                COALESCE(os, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, os

            UNION ALL

            -- Dimension: device
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'device' AS dimension,
                COALESCE(device, 'Unknown') AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, device

            UNION ALL

            -- Dimension: pathname
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'pathname' AS dimension,
                pathname AS value,
                COUNT(DISTINCT {distinct_id}) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) AND pathname IS NOT NULL
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_load
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_load' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                CAST(COALESCE(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                0.0 AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_lcp
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_lcp' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('LCP', 'lcp')
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: path_metric_cls
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'path_metric_cls' AS dimension,
                pathname AS value,
                COUNT(*) AS event_count,
                0.0 AS value_sum,
                0 AS good_count,
                0 AS ni_count,
                0 AS poor_count,
                0.0 AS p50_value,
                CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
                0.0 AS p99_value
            FROM client_vitals
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
              AND pathname IS NOT NULL
              AND metric_name IN ('CLS', 'cls')
            GROUP BY bucket_start, pathname
        """,
            [service_id] * 9,
        )

    # 6. Populate error aggregates
    if has_errors:
        con.execute(
            f"""
            INSERT INTO rum_error_aggregates
            -- Dimension: total
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'total' AS dimension,
                'errors' AS value,
                COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start

            UNION ALL

            -- Dimension: pathname
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'pathname' AS dimension,
                pathname AS value,
                COUNT(DISTINCT {distinct_id}) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str}) AND pathname IS NOT NULL
            GROUP BY bucket_start, pathname

            UNION ALL

            -- Dimension: exception
            SELECT
                ? AS service_id,
                DATE_TRUNC('hour', timestamp) AS bucket_start,
                'exception' AS dimension,
                concat(COALESCE(error_message, ''), '|', COALESCE(error_file, ''), '|', CAST(COALESCE(error_line, 0) AS VARCHAR), '|', CAST(COALESCE(error_col, 0) AS VARCHAR)) AS value,
                COUNT(*) AS error_count
            FROM client_errors
            WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})
            GROUP BY bucket_start, value
        """,
            [service_id] * 3,
        )

    # 7. Populate a single unified beacon count per hour. This MUST match the
    # header badge's cross-table COUNT(DISTINCT distinct_id) semantics
    # (backend/routers/bootstrap.py) — summing the vitals 'pageviews' +
    # 'interactions' dimension counts (step 5) plus the error 'errors'
    # dimension count (step 6) double-counts any physical beacon event that
    # emits more than one row sharing a distinct_id (e.g. a vitals metric AND
    # an interaction event from the same page load). Computed directly from
    # a UNION of the raw tables rather than from the per-dimension aggregates
    # above, since those are already partitioned in a way that loses the
    # cross-partition distinct_id overlap.
    union_parts = []
    if has_vitals:
        union_parts.append(
            f"SELECT timestamp, {distinct_id} AS distinct_id FROM client_vitals "
            f"WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})"
        )
    if has_errors:
        union_parts.append(
            f"SELECT timestamp, {distinct_id} AS distinct_id FROM client_errors "
            f"WHERE DATE_TRUNC('hour', timestamp) IN ({hours_str})"
        )
    con.execute(
        f"""
        INSERT INTO rum_vitals_aggregates
        SELECT
            ? AS service_id,
            DATE_TRUNC('hour', timestamp) AS bucket_start,
            'total' AS dimension,
            'total_beacons' AS value,
            COUNT(DISTINCT distinct_id) AS event_count,
            0.0 AS value_sum,
            0 AS good_count,
            0 AS ni_count,
            0 AS poor_count,
            0.0 AS p50_value,
            0.0 AS p75_value,
            0.0 AS p99_value
        FROM ({" UNION ALL ".join(union_parts)})
        GROUP BY bucket_start
        """,
        [service_id],
    )

    logger.info("[rum_rollups] %s: RUM aggregates successfully updated for %d hours", service_id, len(hours_list))


def backfill_missing_rum_aggregates(
    con: Any,
    service_id: str,
    lookback_days: int = 30,
) -> list[datetime]:
    """Detect and heal hours in ``client_vitals`` / ``client_errors`` within
    ``lookback_days`` that are either absent from ``rum_vitals_aggregates`` or
    lack a ``total_beacons`` aggregate row or carry more than one (a past concurrent recompute).

    Historical hours aggregated before commit ``3fcbcd27`` already have rows in
    ``rum_vitals_aggregates`` (for ``LCP``/``CLS``/``pageviews``/etc.) and would
    be skipped by a naive ``NOT EXISTS`` check on ``bucket_start`` alone.
    Checking specifically for ``value = 'total_beacons'`` ensures those legacy
    hours are detected and recomputed.
    """
    has_vitals = table_exists(con, "client_vitals")
    has_errors = table_exists(con, "client_errors")
    if not has_vitals and not has_errors:
        return []

    cutoff = datetime.now(UTC) - timedelta(days=max(1, int(lookback_days)))
    raw_parts: list[str] = []
    params: list[object] = []

    if has_vitals:
        raw_parts.append(
            "SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start "
            "FROM client_vitals WHERE timestamp IS NOT NULL AND timestamp >= ?"
        )
        params.append(cutoff)
    if has_errors:
        raw_parts.append(
            "SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start "
            "FROM client_errors WHERE timestamp IS NOT NULL AND timestamp >= ?"
        )
        params.append(cutoff)

    raw_union = " UNION ".join(raw_parts)
    if table_exists(con, "rum_vitals_aggregates"):
        sql = f"""
            SELECT DISTINCT r.bucket_start
            FROM ({raw_union}) r
            WHERE r.bucket_start IS NOT NULL
              AND (
                  SELECT COUNT(*)
                  FROM rum_vitals_aggregates a
                  WHERE a.service_id = ?
                    AND a.bucket_start = r.bucket_start
                    AND a.value = 'total_beacons'
              ) <> 1
            ORDER BY r.bucket_start ASC
        """
        rows = con.execute(sql, [*params, service_id]).fetchall()
    else:
        sql = f"""
            SELECT DISTINCT r.bucket_start
            FROM ({raw_union}) r
            WHERE r.bucket_start IS NOT NULL
            ORDER BY r.bucket_start ASC
        """
        rows = con.execute(sql, params).fetchall()

    missing_hours: list[datetime] = []
    for r in rows:
        if r and r[0] is not None:
            dt_val = r[0]
            if isinstance(dt_val, datetime):
                dt_val = _normalize_hour_dt(dt_val)
            missing_hours.append(dt_val)

    if missing_hours:
        logger.info(
            "[rum_rollups] %s: Healing %d RUM aggregate hour(s) with a missing or duplicated total_beacons row (lookback=%dd)",
            service_id,
            len(missing_hours),
            lookback_days,
        )
        recompute_rum_aggregates(con, service_id, hours=missing_hours)

    return missing_hours


sys.modules["backend.core.rollups.rum"] = sys.modules[__name__]
