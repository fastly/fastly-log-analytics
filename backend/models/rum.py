"""Response models for the RUM JSON routes (``routers/rum.py``).

Producers hand-build dicts, so every field is optional and ``WireSafeModel``
omits whatever a given branch did not set. Telemetry keys injected by
``_inject_telemetry`` (``_debug_queries`` etc.) ride along as extras.
"""

from __future__ import annotations

from typing import Any

from backend.models._base import WireSafeModel


class RumStatusResponse(WireSafeModel):
    """``GET /{service_id}/rum/status``. Analyst sessions only get the first two keys."""

    enabled: bool | None = None
    enabled_at: str | None = None
    deployed_vcl_sha: str | None = None
    current_vcl_sha: str | None = None
    vcl_drift: bool | None = None
    capture_vitals: bool | None = None
    capture_performance: bool | None = None
    capture_errors: bool | None = None
    capture_events: bool | None = None
    custom_condition: str | None = None


class RumSettingsUpdateResponse(WireSafeModel):
    ok: bool
    message: str | None = None
    skipped: bool | None = None


class RumBeaconHealthResponse(WireSafeModel):
    enabled: bool | None = None
    beacon_fire_rate: int | None = None
    recent_beacons: int | None = None
    last_beacon_time: str | None = None
    setup_complete: bool | None = None
    message: str | None = None


# Numeric fields below are ``int | float`` (not ``float``) so an integer the
# producer emitted (an INP p75, a whole-number average) stays ``5`` on the wire
# instead of being coerced to ``5.0``.
RumNumber = int | float


class RumCwvDistribution(WireSafeModel):
    good: int
    needs_improvement: int
    poor: int


class RumCwvVital(WireSafeModel):
    """One Core Web Vital. ``distribution`` is ``None`` only on the no-data branch."""

    p75: RumNumber | None
    distribution: RumCwvDistribution | None


class RumFidVital(WireSafeModel):
    p75: RumNumber | None
    fcp: RumNumber | None
    ttfb: RumNumber | None


class RumVitals(WireSafeModel):
    lcp: RumCwvVital
    cls: RumCwvVital
    inp: RumCwvVital
    fid: RumFidVital


class RumWorstPage(WireSafeModel):
    path: str
    views: int
    avg_load_time: RumNumber
    lcp_p75: RumNumber | None
    cls_p75: RumNumber | None
    error_rate: RumNumber


class RumJsError(WireSafeModel):
    message: str | None
    file: str
    line: int
    col: int
    count: int


class RumTrends(WireSafeModel):
    """Parallel per-bucket series. ``pageviews`` / ``interactions`` / ``errors``
    are absent on the no-data branch."""

    timestamps: list[str]
    lcp: list[RumNumber | None]
    cls: list[RumNumber | None]
    error_rate: list[RumNumber | None]
    pageviews: list[int] | None = None
    interactions: list[int] | None = None
    errors: list[int] | None = None


class RumEnvironments(WireSafeModel):
    browsers: dict[str, int]
    os: dict[str, int]
    devices: dict[str, int]


class RumAnalyticsResponse(WireSafeModel):
    """``GET /{service_id}/rum/analytics`` (data, no-data and failure branches)."""

    is_mock: bool | None = None
    no_data: bool | None = None
    beacon_count: int | None = None
    pageview_count: int | None = None
    interaction_count: int | None = None
    error_count: int | None = None
    message: str | None = None
    error: str | None = None
    vitals: RumVitals | None = None
    worst_pages: list[RumWorstPage] | None = None
    errors: list[RumJsError] | None = None
    trends: RumTrends | None = None
    environments: RumEnvironments | None = None


class RumLiveEventItem(WireSafeModel):
    time: str
    type: str
    path: str
    desc: str
    browser: str
    os: str
    raw_log: dict[str, Any] = {}


RumLiveResponse = list[RumLiveEventItem]
