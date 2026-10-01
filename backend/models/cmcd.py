from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, StringConstraints

from backend.models.common import BaseResponse, FilteredRequest, Limit100, Seconds14400

CmcdSectionName = Literal[
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "startup_ts",
    "session_duration_dist",
]


class CmcdRequest(FilteredRequest):
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    sections: list[CmcdSectionName] | None = None
    range_token: str | None = None
    anchor: str | None = None


class CmcdAggregatesResponse(BaseResponse):
    available: bool
    has_data: bool = True
    overview: dict[str, Any] | None = None
    buffer_health_ts: list[dict[str, Any]] = []
    bitrate_ts: list[dict[str, Any]] = []
    throughput_ts: list[dict[str, Any]] = []
    top_content: list[dict[str, Any]] = []
    rebuffer_by_country: list[dict[str, Any]] = []
    rebuffer_by_asn: list[dict[str, Any]] = []
    object_type_dist: list[dict[str, Any]] = []
    streaming_format_dist: list[dict[str, Any]] = []
    sessions_ts: list[dict[str, Any]] = []
    startup_ts: list[dict[str, Any]] = []
    session_duration_dist: list[dict[str, Any]] = []


class ContentSecurityRequest(FilteredRequest):
    content_id: Annotated[str, StringConstraints(max_length=512)] | None = None
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 10
    range_token: str | None = None
    anchor: str | None = None


class ContentSecurityTopRow(BaseModel):
    value: str
    requests: int
    bytes: int | None = None


class ContentSecurityContentId(BaseModel):
    content_id: str
    requests: int


class ContentSecurityBandwidthPoint(BaseModel):
    bucket: str
    edge_bytes: int
    shield_bytes: int | None = None


class ContentSecurityResponse(BaseResponse):
    available: bool
    reason: str | None = None
    fields: dict[str, bool] = {}
    content_id: str | None = None
    has_shield_split: bool = False
    content_ids: list[ContentSecurityContentId] = []
    top_countries: list[ContentSecurityTopRow] = []
    top_referers: list[ContentSecurityTopRow] = []
    top_hosts: list[ContentSecurityTopRow] = []
    bandwidth_ts: list[ContentSecurityBandwidthPoint] = []
