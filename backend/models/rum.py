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
    vitals: dict[str, Any] | None = None
    worst_pages: list[dict[str, Any]] | None = None
    errors: list[dict[str, Any]] | None = None
    trends: dict[str, Any] | None = None
    environments: dict[str, Any] | None = None
