from __future__ import annotations

from typing import Any

from backend.models.analytics._common import _OkWireSafeBase
from backend.models.common import WireSafeModel


class RumStatusResponse(WireSafeModel):
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


class RumSettingsUpdateResponse(_OkWireSafeBase):
    message: str | None = None
    skipped: bool | None = None


class RumBeaconHealthResponse(WireSafeModel):
    enabled: bool | None = None
    beacon_fire_rate: int | None = None
    recent_beacons: int | None = None
    last_beacon_time: str | None = None
    setup_complete: bool | None = None
    message: str | None = None


RumNumber = int | float


class RumCwvDistribution(WireSafeModel):
    good: int
    needs_improvement: int
    poor: int


class RumCwvVital(WireSafeModel):
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
