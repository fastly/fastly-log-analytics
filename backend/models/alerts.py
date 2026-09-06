"""Pydantic models for alerts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from backend.models.alert_channels import AlertChannel
from backend.models.common import BaseResponse


class Alert(BaseModel):
    id: str | None = None
    service_id: str
    name: str
    category: Literal["reliability", "performance", "traffic", "caching"] = "reliability"
    metric: Literal[
        "requests",
        "5xx",
        "5xx_rate",
        "4xx",
        "4xx_rate",
        "specific_status",
        "specific_status_rate",
        "p95_latency",
        "hit_rate",
        "bandwidth",
        "ttfb",
    ]
    evaluation_type: Literal["absolute", "relative_increase", "relative_decrease", "anomaly_zscore"] = "absolute"
    evaluation_scope: Literal["all", "edge", "origin"] = "all"
    operator: Literal[">", "<", ">=", "<="]
    threshold: float
    window_min: float
    comparison_period_min: float | None = None
    status_codes: list[int] | None = None
    webhook_url: str | None = None
    channels: list[AlertChannel] | None = None
    zscore_threshold: float | None = None
    baseline_period_days: int | None = None
    enabled: bool = True
    last_triggered_at: str | None = None
    created_at: str | None = None


class AlertListResponse(BaseResponse):
    data: list[Alert]
    evaluated_at: str


class AlertResponse(BaseResponse):
    data: dict


class AlertPreviewResponse(BaseResponse):
    data: dict | None = None
    error: str | None = None


class _ToggleBody(BaseModel):
    enabled: bool
