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


class AlertMutationData(BaseModel):
    id: str
    status: str = "success"
    service_id: str | None = None


class AlertResponse(BaseResponse):
    data: AlertMutationData | dict


class AlertPreviewData(BaseModel):
    times: list[str]
    values: list[float]
    type: Literal["absolute", "relative", "anomaly_zscore"] = "absolute"
    hist_values: list[float] | None = None
    threshold_values: list[float] | None = None


class AlertPreviewResponse(BaseResponse):
    data: AlertPreviewData | None = None
    error: str | None = None


class AlertEvaluateRequest(BaseModel):
    service_id: str | None = None
    alert_id: str | None = None


class AlertEvaluateResultItem(BaseModel):
    id: str
    name: str
    metric: str
    triggered: bool
    last_triggered_at: str | None = None


class AlertEvaluateData(BaseModel):
    service_id: str
    evaluated: int
    triggered: int
    results: list[AlertEvaluateResultItem]
    evaluated_at: str


class AlertEvaluateResponse(BaseResponse):
    data: AlertEvaluateData


class AlertTestWebhookRequest(BaseModel):
    service_id: str | None = None
    webhook_url: str | None = None
    channel_type: Literal["webhook", "slack", "pagerduty"] = "webhook"
    dry_run: bool = False


class AlertTestWebhookData(BaseModel):
    alert_id: str
    service_id: str
    status: Literal["sent", "validated", "failed"]
    targets_tested: int
    channel_type: str = "webhook"
    detail: str | None = None


class AlertTestWebhookResponse(BaseResponse):
    data: AlertTestWebhookData


class _ToggleBody(BaseModel):
    enabled: bool
