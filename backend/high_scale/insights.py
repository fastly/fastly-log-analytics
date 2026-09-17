from datetime import UTC
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import CacheCollapseDetailResponse, InsightsResponse


def insights_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> InsightsResponse:
    from datetime import datetime

    now = datetime.now(UTC).isoformat()
    # High-scale mode currently returns empty insights
    return InsightsResponse.with_telemetry(
        insights=[],
        window_start=now,
        window_end=now,
        baseline_start=now,
        baseline_end=now,
        computed_at=now,
        window_hours=0.0,
        baseline_hours=0.0,
    )


def cache_collapse_detail_endpoint(
    service: HighScaleService, req: Any, clamp_start: str | None, clamp_end: str | None
) -> CacheCollapseDetailResponse:
    return CacheCollapseDetailResponse.with_telemetry(
        has_data=False,
        clusters=[],
    )
