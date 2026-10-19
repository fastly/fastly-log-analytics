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
    RollupSpec("perf_dims", "build_perf_dims_bundles"),
    RollupSpec("ngwaf_bots", "build_ngwaf_bots_bundles", fallback="direct join"),
    RollupSpec("overview", "build_overview_bundles"),
    RollupSpec("network_summary", "build_network_summary_bundles"),
)
