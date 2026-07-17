from __future__ import annotations

from typing import Any, Literal

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
