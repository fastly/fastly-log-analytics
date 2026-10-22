"""Registry of the per-hour rollup writers fired after every recompute tick.

Adding a per-hour rollup writer means adding one :class:`RollupSpec` here (plus
its writer module); ``recompute_touched_hours`` iterates this registry instead
of carrying one hand-written ``try/except`` block per writer.

``builder`` is the name of the writer as imported into
``backend.core.rollups.recompute`` and is resolved at dispatch time, so the
module attribute stays the single patch point for tests.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RollupSpec:
    """A best-effort per-hour rollup writer.

    ``label`` names the rollup in failure logs; ``fallback`` says what serves
    the affected hours when the writer fails.
    """

    label: str
    builder: str
    fallback: str = "raw scan"


# Order is the order the writers run in; later entries never depend on earlier
# ones, but the origin-latency timeseries is deliberately late (it is the last
# section whose rollup enables the origin skip-temp guard).
HOUR_WRITERS: tuple[RollupSpec, ...] = (
    RollupSpec("time_series", "build_time_series_bundles"),
    RollupSpec("sessions", "build_session_bundles"),
    RollupSpec("slow_urls", "build_slow_urls_bundles"),
    RollupSpec("origin_summary", "build_origin_summary_bundles"),
    RollupSpec("network_rtt", "build_network_rtt_bundles"),
    RollupSpec("network_speed", "build_network_speed_bundles"),
    RollupSpec("network_heatmap", "build_network_heatmap_bundles"),
    RollupSpec("network_geo", "build_network_geo_bundles"),
    RollupSpec("verified_bots_ts", "build_verified_bots_ts_bundles"),
    RollupSpec("perf_latency", "build_perf_latency_bundles"),
    RollupSpec("origin_dims", "build_origin_dims_bundles"),
    RollupSpec("origin_latency_ts", "build_origin_latency_ts_bundles"),
    RollupSpec("security_dims", "build_security_dims_bundles"),
    RollupSpec("security_proxies", "build_security_proxies_bundles"),
    RollupSpec("perf_dims", "build_perf_dims_bundles"),
    RollupSpec("ngwaf_bots", "build_ngwaf_bots_bundles", fallback="direct join"),
    RollupSpec("overview", "build_overview_bundles"),
    RollupSpec("network_summary", "build_network_summary_bundles"),
    RollupSpec("wellknown_bots", "recompute_wellknown_bots_rollup"),
)


@dataclass(frozen=True)
class DayCompactorSpec:
    """A closed-day compactor run by the daily ``rollup_compact`` cron and the admin backfill.

    ``compactor`` names a function exported by ``backend.core.rollups`` and is
    resolved at call time so tests keep patching the package attribute.
    ``label`` names the rollup in cron error attribution. ``admin_key`` is the
    ``/admin/backfill-bundle-rollups`` response key for the compacted-day
    count, or ``None`` when the response does not report it.
    """

    label: str
    compactor: str
    admin_key: str | None = None


# Order is the cron's execution order; the compactors are independent of each
# other (each reads only its own hour bundles).
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
    DayCompactorSpec("overview", "compact_overview_closed_days_to_daily", "overview_days"),
    DayCompactorSpec("time_series", "compact_time_series_closed_days_to_daily"),
    DayCompactorSpec("pop_health", "compact_pop_health_closed_days_to_daily"),
)

# ``/admin/backfill-bundle-rollups``: (response key, ``backend.core.rollups``
# backfill function), in run order. time_series and sessions are backfilled
# separately (they go through ``backfill_missing_bundles`` directly).
ADMIN_BACKFILLS: tuple[tuple[str, str], ...] = (
    ("slow_urls", "backfill_slow_urls_bundles"),
    ("origin_summary", "backfill_origin_summary_bundles"),
    ("origin_dims", "backfill_origin_dims_bundles"),
    ("origin_latency_ts", "backfill_origin_latency_ts_bundles"),
    ("network_rtt", "backfill_network_rtt_bundles"),
    ("network_speed", "backfill_network_speed_bundles"),
    ("network_quality", "backfill_network_quality_bundles"),
    ("verified_bots_ts", "backfill_verified_bots_ts_bundles"),
    ("perf_latency", "backfill_perf_latency_bundles"),
    ("pop_health", "backfill_pop_health_bundles"),
    ("security_dims", "backfill_security_dims_bundles"),
    ("security_proxies", "backfill_security_proxies_bundles"),
    ("perf_ttl_dist", "backfill_perf_dims_bundles"),
    ("wellknown_bots", "backfill_wellknown_bots_rollup"),
    ("ngwaf_bots", "backfill_ngwaf_bots_bundles"),
    ("overview", "backfill_overview_bundles"),
    ("network_summary", "backfill_network_summary_bundles"),
)
