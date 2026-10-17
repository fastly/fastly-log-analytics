"""Admin-only high-scale advisory endpoints.

``/api/admin/`` is blocked for analyst and remote sessions by
``RemoteAccessMiddleware``; nothing here needs an analyst allowance.
"""

from __future__ import annotations

from typing import Literal

import structlog
from fastapi import HTTPException, Query

from backend import config
from backend.high_scale.disk_sampler import latest_disk_snapshot
from backend.high_scale.retention import ServiceProfile, StorageProfile, recommend_retention
from backend.models.admin_high_scale import HighScaleRetentionRecommendationResponse
from backend.routers.admin._router import router
from backend.utils.router_utils import make_error

logger = structlog.get_logger(__name__)


@router.get(
    "/admin/high-scale/retention-recommendation",
    response_model=HighScaleRetentionRecommendationResponse,
)
def api_high_scale_retention_recommendation(
    service_id: str = Query(min_length=1, max_length=128),
    events_per_second: float = Query(gt=0, description="Observed or estimated request events per second"),
    average_row_bytes: float = Query(default=500.0, gt=0),
    total_bytes: int | None = Query(
        default=None, gt=0, description="Serving volume size; defaults to the sampled volume"
    ),
) -> HighScaleRetentionRecommendationResponse:
    """Recommend hot/warm retention for sizing; an explicit per-service setting always wins."""
    if not config.is_high_scale_mode():
        raise HTTPException(404, detail=make_error("high_scale_not_enabled", "High-scale mode is not enabled"))
    try:
        cfg = config.load_config(service_id)
    except Exception:
        raise HTTPException(
            503, detail=make_error("service_unavailable", "Service configuration unavailable")
        ) from None
    if not cfg:
        raise HTTPException(404, detail=make_error("service_not_found", "Service not found"))

    volume_source: Literal["request", "sampled"] = "request"
    volume_bytes = total_bytes
    if volume_bytes is None:
        snapshot = latest_disk_snapshot()
        if snapshot is None:
            raise HTTPException(
                409,
                detail=make_error(
                    "high_scale_volume_unknown",
                    "Serving volume size is not sampled yet; pass total_bytes",
                ),
            )
        volume_bytes = snapshot.total_bytes
        volume_source = "sampled"

    recommendation = recommend_retention(
        ServiceProfile(events_per_second=events_per_second, average_row_bytes=average_row_bytes),
        StorageProfile(total_bytes=volume_bytes),
    )
    # ``provisioning.cron_sync.data_retention_days`` is the explicit per-service
    # setting (0 means keep forever). Absent means "never set", not a default.
    explicit = ((cfg.get("provisioning") or {}).get("cron_sync") or {}).get("data_retention_days")
    explicit_days = explicit if isinstance(explicit, int) and not isinstance(explicit, bool) and explicit >= 0 else None
    source: Literal["override", "recommendation"] = "override" if explicit_days is not None else "recommendation"
    effective: float | None
    if explicit_days is None:
        effective = recommendation.warm_days
    else:
        effective = float(explicit_days) if explicit_days > 0 else None

    logger.info(
        "high_scale.retention_recommended",
        service_id=service_id,
        events_per_second=events_per_second,
        average_row_bytes=average_row_bytes,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        recommended_hot_days=round(recommendation.hot_days, 3),
        recommended_warm_days=round(recommendation.warm_days, 3),
        explicit_retention_days=explicit_days,
        source=source,
    )
    return HighScaleRetentionRecommendationResponse(
        service_id=service_id,
        recommended_hot_days=recommendation.hot_days,
        recommended_warm_days=recommendation.warm_days,
        reasoning=recommendation.reasoning,
        volume_total_bytes=volume_bytes,
        volume_total_source=volume_source,
        explicit_retention_days=explicit_days,
        effective_retention_days=effective,
        source=source,
    )
