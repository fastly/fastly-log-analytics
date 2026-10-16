"""Pydantic request and response models for Raw Logs Explorer."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from backend.models.common import BaseResponse, FilterSpec, Limit500, RangeKeyedRequest, SortDir


class ColumnMeta(BaseModel):
    name: str
    type: str


class LogsQueryRequest(RangeKeyedRequest):
    """Request schema for querying paginated raw access logs."""

    limit: Limit500 = 100
    offset: int = Field(default=0, ge=0)
    sort_dir: SortDir = "desc"
    search: str | None = None
    columns: list[str] | None = None

    @field_validator("filters", mode="before")
    @classmethod
    def _normalize_filters(cls, v: Any) -> Any:
        if v is None:
            return {}
        if not isinstance(v, dict):
            return v
        normalized = {}
        for k, spec in v.items():
            if spec is None:
                continue
            if isinstance(spec, FilterSpec):
                normalized[k] = spec
            elif isinstance(spec, dict) and "values" in spec:
                normalized[k] = FilterSpec(**spec)
            elif isinstance(spec, (list, tuple, set)):
                normalized[k] = FilterSpec(mode="include", values=list(spec))
            else:
                normalized[k] = FilterSpec(mode="include", values=[spec])
        return normalized


class LogsQueryResponse(BaseResponse):
    """Response envelope for paginated raw access logs."""

    rows: list[dict[str, Any]] = Field(default_factory=list)
    total_count: int = 0
    has_more: bool = False
    limit: int = 100
    offset: int = 0
    columns: list[str] = Field(default_factory=list)
    schema_columns: list[ColumnMeta] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
