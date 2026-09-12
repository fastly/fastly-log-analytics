"""Wire models for the isolated high-scale query API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.high_scale.pagination import MAX_PAGE_SIZE
from backend.models.common import BaseResponse


class HighScaleRequestFactRequest(BaseModel):
    start_time: str | None = None
    end_time: str | None = None
    limit: int = Field(default=MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    cursor: str | None = None


class ServingWatermarkResponse(BaseModel):
    service_id: str
    domain: str
    owner_epoch: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    last_accepted_cursor: str | None
    last_archived_event_id: str | None
    last_visible_event_id: str | None
    exact: bool


class QueryResponseMetadataResponse(BaseModel):
    status: Literal["complete", "partial", "queued", "running", "failed"]
    exact: bool
    coverage: float
    freshness_lag_seconds: float
    watermark: ServingWatermarkResponse
    approximation_error: float | None
    error: str | None


class HighScaleRequestFactResponse(BaseResponse):
    rows: list[dict[str, Any]]
    metadata: QueryResponseMetadataResponse
    next_cursor: str | None


class HighScaleRumFactResponse(BaseResponse):
    rows: list[dict[str, Any]]
    metadata: QueryResponseMetadataResponse
    next_cursor: str | None


ExportDomain = Literal["request", "rum_vitals", "rum_errors", "cmcd"]


class HighScaleExportRequest(HighScaleRequestFactRequest):
    domain: ExportDomain = "request"


class HighScaleExportResponse(BaseResponse):
    export_id: str
    service_id: str
    state: Literal["queued", "running", "completed", "cancelled", "failed", "expired"]
    rows_written: int
    bytes_written: int
    error: str | None


class HighScaleExportCancelResponse(HighScaleExportResponse):
    cancelled: bool
