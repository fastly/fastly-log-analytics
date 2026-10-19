"""Admin-only high-scale advisory surfaces."""

from __future__ import annotations

from typing import Literal

from backend.models.common import BaseResponse


class HighScaleRetentionRecommendationResponse(BaseResponse):
    """Advisory hot/warm retention for one service (never applied automatically).

    ``effective_retention_days`` is the explicit per-service
    ``provisioning.cron_sync.data_retention_days`` whenever one is set
    (``source="override"``; ``null`` when it is ``0``, meaning keep forever),
    otherwise the recommended warm window (``source="recommendation"``).
    """

    service_id: str
    recommended_hot_days: float
    recommended_warm_days: float
    reasoning: str
    volume_total_bytes: int
    volume_total_source: Literal["request", "sampled"]
    explicit_retention_days: int | None = None
    effective_retention_days: float | None = None
    source: Literal["override", "recommendation"]
