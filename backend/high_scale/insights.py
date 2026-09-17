from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import CacheCollapseDetailResponse, InsightsResponse


def insights_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> InsightsResponse:
    # High-scale mode currently returns empty insights
    return InsightsResponse.with_telemetry(insights=[])


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> CacheCollapseDetailResponse:
    return CacheCollapseDetailResponse.with_telemetry(
        has_data=False,
        clusters=[],
    )
