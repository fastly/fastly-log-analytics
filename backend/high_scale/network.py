from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.network import (
    NetworkHealthResponse,
    NetworkQualityResponse,
)
from backend.routers.network import PopHealthListResponse


def network_health(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkHealthResponse:
    return NetworkHealthResponse.with_telemetry(
        edge_shield_heatmap=[],
        tls_version_dist=[],
        tls_handshake_times=[],
        conn_reuse_rates=[],
        ipv6_adoption_ts=[],
        _approx=True,
    )


def network_quality(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkQualityResponse:
    return NetworkQualityResponse.with_telemetry(
        client_ttfb_p95=[],
        client_rtt_p95=[],
        edge_rtt_p95=[],
        rtt_by_country=[],
        ttfb_by_country=[],
        _approx=True,
    )


def get_pop_health(
    service: HighScaleService, start_time: datetime | None, end_time: datetime | None
) -> PopHealthListResponse:
    return PopHealthListResponse.with_telemetry(
        data=[],
    )
