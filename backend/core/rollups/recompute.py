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
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

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
from .hour_bundles import bundle_hours, bundle_hours_ip_spread

# Writers dispatched by name from spec.HOUR_WRITERS (resolved against this
# module at call time, so each name here is also the test patch point).
from .network import (  # noqa: F401
    build_network_geo_bundles,
    build_network_heatmap_bundles,
    build_network_quality_bundles,
    build_network_rtt_bundles,
    build_network_speed_bundles,
    build_network_summary_bundles,
    build_pop_health_bundles,
)
from .ngwaf_bots import build_ngwaf_bots_bundles, build_ngwaf_bots_ts_bundles  # noqa: F401
from .origin_dims import (  # noqa: F401
    build_origin_dims_bundles,
    build_origin_latency_ts_bundles,
    build_origin_summary_bundles,
    build_perf_dims_bundles,
    build_perf_latency_bundles,
    build_slow_urls_bundles,
)
from .security_dims import (  # noqa: F401
    build_security_dims_bundles,
    build_security_proxies_bundles,
    build_verified_bots_ts_bundles,
)
from .time_series import (  # noqa: F401
    build_overview_bundles,
    build_session_bundles,
    build_time_series_bundles,
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
