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
from datetime import UTC, datetime, timedelta

from ._common import (
    _VIRTUAL_FIELD_BACKING,
    IP_SAMPLE_CAP,
    _build_copy_query,
    _build_ip_spread_select_query,
    _build_virtual_field_copy_query,
    _get_fields,
    _ip_spread_root,
    _is_safe_ident,
    _load_markers,
    _publish_field_partitions,
    _rollups_root,
    _safe_table_for,
    _save_markers,
    describe_columns,
    parse_hour_token,
)
from .hour_bundles import bundle_hours, bundle_hours_ip_spread
from .network_health_geo import build_network_geo_bundles
from .network_health_heatmap import build_network_heatmap_bundles
from .network_rtt import build_network_rtt_bundles
from .network_speed import build_network_speed_bundles
from .network_summary import build_network_summary_bundles
from .ngwaf_bots import build_ngwaf_bots_bundles
from .origin_dims import build_origin_dims_bundles
from .origin_latency_ts import build_origin_latency_ts_bundles
from .origin_summary import build_origin_summary_bundles
from .overview import build_overview_bundles
from .perf_dims import build_perf_dims_bundles
from .perf_latency import build_perf_latency_bundles
from .security_dims import build_security_dims_bundles
from .sessions import build_session_bundles
from .slow_urls import build_slow_urls_bundles
from .time_series import build_time_series_bundles
from .verified_bots_ts import build_verified_bots_ts_bundles

logger = logging.getLogger(__name__)


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
    touched_hours = [h for h, _ in parsed]
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

    # Time-series rollups for the dashboard chart. Same best-effort
    # contract: if the build fails, the dashboard falls back to a raw
    # scan for the affected hours.
    try:
        build_time_series_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: time_series bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Sessions rollups for /api/sessions. Best-effort: if the build
    # fails, the sessions endpoint falls back to a raw window-function
    # scan for any hours that lack a sessions.parquet.
    try:
        build_session_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: sessions bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Slow-URLs rollups for the /origin slow_urls panel. Same best-effort
    # contract: a failure here leaves the panel on its raw TEMP-table
    # path for the affected hours.
    try:
        build_slow_urls_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: slow_urls bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Origin-summary rollup for the /origin summary card. Same posture.
    try:
        build_origin_summary_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: origin_summary bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Network-RTT per-ASN rollup for the /api/network-health
    # rtt_percentiles_query panel (5.2 s on prod 30 d before this
    # rollup). Same best-effort contract — a failure leaves the panel
    # on its raw TEMP-table path for the affected hours.
    try:
        build_network_rtt_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: network_rtt bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Network-speed per-ASN per-c_speed distribution rollup for the
    # /api/network-health speed_distribution_query panel (2.9 s on
    # prod 30 d before this rollup). Exact counts SUM across hours —
    # no _approx flag. Same best-effort contract.
    try:
        build_network_speed_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: network_speed bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-ASN heatmap metrics rollup for /api/network-health's heatmap +
    # leaderboard + summary + buckets sections (all blocked by the 2–4 s
    # create_filtered_temp_table on 30 d windows). Enables the skip-temp
    # guard in get_health() when the geo rollup also hits. Same best-effort
    # contract — a failure leaves those sections on the raw TEMP-table path.
    try:
        build_network_heatmap_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: network_heatmap bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-geocell metrics rollup for /api/network-health's map_buckets +
    # cities + metro_leaderboard sections. Together with the heatmap rollup
    # above, enables the skip-temp guard in get_health(). Same best-effort
    # contract — a failure leaves those sections on the raw TEMP-table path.
    try:
        build_network_geo_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: network_geo bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Verified-bot minute-granular time-series rollup for the
    # /api/security/aggregates verified_bots_ts panel (~1.2 s on prod
    # 30 d before this rollup). Exact counts SUM across hours; the
    # reader re-buckets from minute granularity and fills the active
    # hour live. Same best-effort contract.
    try:
        build_verified_bots_ts_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: verified_bots_ts bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour top-N url/asn latency rollups for /api/performance/aggregates'
    # top_urls (~1.45 s) + top_asns (~0.77 s) panels. Same per-dimension
    # percentile-over-window shape as slow_urls; reader request-weight-
    # averages + re-ranks by sort_by. Same best-effort contract.
    try:
        build_perf_latency_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: perf_latency bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour origin-dimension percentile rollups (pop / oip / edge) for
    # /api/origin/aggregates' pop_latency + ip_health + path_breakdown panels.
    # Same per-dimension percentile-over-window shape as slow_urls; reader
    # request-weight-averages (error_pct stays exact via carried counts).
    # Same best-effort contract — a failure leaves those panels on the raw
    # TEMP-table path for the affected hours.
    try:
        build_origin_dims_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: origin_dims bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour MINUTE-granular origin-latency-percentile time-series rollup for
    # /api/origin/aggregates' timeseries panel (the temp_table_create driver —
    # this is the last section to roll up, enabling the skip-temp guard). Hybrid
    # shape: time-series re-bucket (like verified_bots_ts) + request-weighted
    # percentile merge (like slow_urls); counts exact, percentiles _approx.
    # Same best-effort contract — a failure leaves the panel on the raw
    # TEMP-table path for the affected hours.
    try:
        build_origin_latency_ts_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: origin_latency_ts bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour security-dimension rollups (req_size / conn_reuse / topips / cov)
    # for /api/security/aggregates' equivalent panels (each an all-rows live
    # scan today). EXACT counts/MAX SUM/merge across hours — no _approx flag.
    # Same best-effort contract — a failure leaves those panels on the raw
    # TEMP-table path for the affected hours.
    try:
        build_security_dims_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: security_dims bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour performance-dimension rollup (ttl_dist) for
    # /api/performance/aggregates' ttl_dist histogram panel (an all-rows live
    # scan today). EXACT counts SUM + MIN-of-MIN across hours — no _approx flag.
    # Same best-effort contract — a failure leaves that panel on the raw
    # TEMP-table path for the affected hours.
    try:
        build_perf_dims_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: perf_dims bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Per-hour NGWAF-bots rollup (waf_req_id ⨝ ngwaf_bot_cache aggregated at
    # write time) for get_top_bots' ngwaf_bots panel. EXACT SUM across hours.
    # Same best-effort contract — a failure leaves the panel on the direct
    # base-table join for the affected hours.
    try:
        build_ngwaf_bots_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: ngwaf_bots bundle failed (direct join will serve): %s",
            service_id,
            e,
        )

    # Per-hour overview rollup for /api/value/summary's combined
    # overview + caching sections. All SUM-aggregatable — no _approx.
    # Same best-effort contract — a failure leaves the summary page
    # on its raw GROUP BY scan for the affected hours.
    try:
        build_overview_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: overview bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    try:
        build_network_summary_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: network_summary bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )


def backfill_rollups(service_id: str, source: dict, fields: list[str] | None = None) -> None:
    """One-shot bulk build for all historical hours up to (but not including)
    the current hour.

    ``fields``: if provided, only backfills the given subset (used when a
    new custom field is added).
    Defaults to all eligible fields.
    """
    table_ident = _safe_table_for(source)
    if not table_ident:
        return

    target_fields = fields if fields is not None else _get_fields(source)
    if not target_fields:
        return

    dt_end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    where_sql = f"timestamp < '{dt_end.isoformat()}'"
    _run_per_field_copy(service_id, source, table_ident, where_sql, target_fields)

    # IP-spread companion to the count rollup — see _run_ip_spread_per_field
    # for why this can't run as a pure SQL COPY. Best-effort; the count
    # rollup completing successfully is the primary contract of this
    # call, and the IP-spread reader has its own fallback when files are
    # missing.
    try:
        _run_ip_spread_per_field(service_id, source, table_ident, where_sql, target_fields)
    except Exception as e:
        logger.warning(
            "[rollups] %s: backfill ip_spread per-field write failed (count rollup unaffected): %s",
            service_id,
            e,
        )

    # Stamp completion in the markers file so _ensure_rollups can detect
    # which fields still need a backfill on next startup / cfg change.
    markers = _load_markers(source)
    stamp = datetime.now(UTC).isoformat()
    for f in target_fields:
        markers[f] = stamp
    _save_markers(source, markers)


def backfill_missing_hour_bundles(
    service_id: str,
    source: dict,
    lookback_days: int = 30,
) -> dict[str, int]:
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

    Returns a summary dict: ``{"missing": N, "rebuilt_fields": F,
    "bundled": B, "stamped_empty": E}`` so callers can log a concise
    progress line. ``stamped_empty`` counts zero-row closed hours that
    received an empty sentinel bundle (see
    :func:`~backend.core.rollups.hour_bundles.stamp_empty_hour_sentinels`)
    so the reader can tell verified-empty apart from writer-gap.
    """
    import duckdb as _ddb

    from backend.core.iceberg import update_iceberg_view
    from backend.core.rollups._common import _hour_bundled_root

    bundled_root = _hour_bundled_root(source)
    existing: set[str] = set()
    if os.path.isdir(bundled_root):
        try:
            for e in os.listdir(bundled_root):
                if e.startswith("hour="):
                    existing.add(e[len("hour=") :])
        except OSError:
            pass

    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    end_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=lookback_days)

    # Discover hours with data via the iceberg view. We use the view (not
    # the per-service .duckdb file directly) so this runs concurrently
    # with uvicorn's RW connection on that file — backed by a fresh
    # in-memory DuckDB connection that holds no persistent state.
    con = _ddb.connect(":memory:")
    from backend.core.duckdb import _configure_fos

    _configure_fos(con, source)
    try:
        update_iceberg_view(con, source)
        # The view's actual SQL identifier is set by update_iceberg_view
        # — query through ``information_schema`` to find it rather than
        # guess (the name is derived from source["name"]/svc_name, not
        # source["service_id"]).
        view_row = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'logs_%' LIMIT 1"
        ).fetchone()
        if view_row is None:
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0}
        view_name = view_row[0]
        if not _is_safe_ident(view_name):
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0}
        try:
            rows = con.execute(
                f"SELECT strftime(timestamp, '%Y-%m-%d-%H') AS h, COUNT(*) AS n "
                f"FROM {view_name} WHERE timestamp >= ? AND timestamp < ? "
                f"GROUP BY 1 HAVING n > 0",
                [start_dt.isoformat(), end_dt.isoformat()],
            ).fetchall()
        except _ddb.Error as e:
            logger.warning("[rollups] %s: backfill_missing_hour_bundles view query failed: %s", service_id, e)
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0}

        missing = sorted(h for h, _ in rows if h < active and h not in existing)

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
            return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0}

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
        return {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": stamped}

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
                    if os.path.isfile(os.path.join(bundled_root, entry, "all_fields.parquet")):
                        bundled_now += 1
        except OSError:
            pass

    return {
        "missing": len(missing),
        "rebuilt_fields": len(_get_fields(source)),
        "bundled": bundled_now,
        "stamped_empty": stamped,
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
    from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME, _hour_bundled_root

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


def cleanup_old_rollups(service_id: str, source: dict, max_age_days: int) -> int:
    """Delete per-hour rollup directories older than ``max_age_days``.

    ``max_age_days <= 0`` disables cleanup (keep everything). Returns the
    number of hour-dirs deleted. Safe to call concurrently with the
    writers because we only ever delete hours STRICTLY older than the
    cutoff — current and just-written hours are never candidates.
    """
    if max_age_days <= 0:
        return 0
    rollup_root = _rollups_root(source)
    if not os.path.isdir(rollup_root):
        return 0

    cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).strftime("%Y-%m-%d-%H")
    deleted = 0
    try:
        for field_entry in os.listdir(rollup_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(rollup_root, field_entry)
            for hour_entry in os.listdir(field_dir):
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # String compare works because the format is fixed-width
                # YYYY-MM-DD-HH which sorts lexicographically by time.
                if hour < cutoff:
                    hour_dir = os.path.join(field_dir, hour_entry)
                    try:
                        shutil.rmtree(hour_dir)
                        deleted += 1
                    except OSError as e:
                        logger.warning("[rollups] could not delete %s: %s", hour_dir, e)
    except OSError as e:
        logger.warning("[rollups] cleanup walk failed for %s: %s", service_id, e)
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
    import duckdb

    from backend.core.duckdb import _cache_dir, get_connection
    from backend.core.iceberg.view import _get_service_lock

    cache_root = _cache_dir(source)
    rollups_dir = _rollups_root(source)
    os.makedirs(rollups_dir, exist_ok=True)
    lock_key = source.get("name", "default")

    con = get_connection(source=source, read_only=True)
    try:
        cols = describe_columns(con, source, table_ident, logger=logger, log_label="could not describe")
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

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp", field)
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
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: COPY failed for field=%s: %s", service_id, field, e)
                shutil.rmtree(tmp_field_dir, ignore_errors=True)
                continue

            with _get_service_lock(lock_key):
                _publish_field_partitions(tmp_field_dir, rollups_dir, field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
    finally:
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
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.core.duckdb import _cache_dir, get_connection
    from backend.core.iceberg.view import _get_service_lock
    from backend.utils.hll import HyperLogLog

    cache_root = _cache_dir(source)
    ip_spread_dir = _ip_spread_root(source)
    os.makedirs(ip_spread_dir, exist_ok=True)
    lock_key = source.get("name", "default")

    con = get_connection(source=source, read_only=True)
    try:
        cols = describe_columns(con, source, table_ident, logger=logger, log_label="ip_spread describe")
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
                arrow_table = con.execute(select_sql).to_arrow_table()
            except duckdb.Error as e:
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

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp_ip", field)
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
        con.close()
