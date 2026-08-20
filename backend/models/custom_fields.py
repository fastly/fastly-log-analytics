"""Pydantic models for the custom log fields API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, field_validator

from backend.core.field_registry import VALID_NAME_RE
from backend.models.common import BaseResponse


class CustomField(BaseModel):
    name: str
    label: str
    description: str = ""
    vcl_log_expression: str
    collection_stage: Literal["edge", "origin", "deliver"] = "edge"
    origin_log_frequency: Literal["all", "miss_pass"] = "all"
    duckdb_type: Literal["VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN"] = "VARCHAR"
    value_type: Literal["string", "numeric", "boolean", "ip", "url"] = "string"
    bytes_estimate: int = 20
    nullable: bool = True
    enabled: bool = True
    show_in_dashboard: bool = False
    show_in_logs: bool = True
    filterable: bool = True

    @field_validator("name")
    @classmethod
    def _name_valid(cls, v: str) -> str:
        if not VALID_NAME_RE.match(v):
            raise ValueError("name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars")
        return v

    @field_validator("bytes_estimate")
    @classmethod
    def _bytes_in_range(cls, v: int) -> int:
        if not (1 <= v <= 1024):
            raise ValueError("bytes_estimate must be 1–1024")
        return v


class CustomFieldCreate(CustomField):
    pass


class CustomFieldUpdate(BaseModel):
    """All fields optional for PATCH semantics."""

    label: str | None = None
    description: str | None = None
    vcl_log_expression: str | None = None
    collection_stage: Literal["edge", "origin", "deliver"] | None = None
    origin_log_frequency: Literal["all", "miss_pass"] | None = None
    duckdb_type: Literal["VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN"] | None = None
    value_type: Literal["string", "numeric", "boolean", "ip", "url"] | None = None
    bytes_estimate: int | None = None
    nullable: bool | None = None
    enabled: bool | None = None
    show_in_dashboard: bool | None = None
    show_in_logs: bool | None = None
    filterable: bool | None = None


class CustomFieldResponse(BaseResponse):
    field: CustomField
    warnings: list[str] = []


class CustomFieldsListResponse(BaseResponse):
    fields: list[CustomField]


class VclLintRequest(BaseModel):
    vcl_log_expression: str
    collection_stage: Literal["edge", "origin", "deliver"] = "edge"
    log_fields_config: dict | None = None


class VclLintResponse(BaseModel):
    valid: bool
    errors: list[str]
    warnings: list[str]
    format_length: int | None = None
    format_length_limit: int = 12000
