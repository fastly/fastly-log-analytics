from __future__ import annotations

from typing import Any, Literal

from backend.models.analytics._common import _move_fields
from backend.models.common import (
    BaseResponse,
    Limit100,
    RangeKeyedRequest,
    Seconds14400,
    WireSafeModel,
)

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


class CmcdRequest(RangeKeyedRequest):
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    sections: list[CmcdSectionName] | None = None


class CmcdOverview(WireSafeModel):
    active_sessions: int
    rebuffer_rate: float | None = None
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None
    median_throughput: float | None = None
    peak_viewers: int
    avg_session_duration: int | None = None


class _CmcdBucketPointBase(WireSafeModel):
    bucket: str


class CmcdSessionsPoint(_CmcdBucketPointBase):
    concurrent_sessions: int
    rebuffer_session_pct: float | None = None
    new_sessions: int


class CmcdBufferHealthPoint(_CmcdBucketPointBase):
    p50_buffer: float | None = None
    p95_buffer: float | None = None
    starvation_rate: float | None = None


class CmcdBitratePoint(_CmcdBucketPointBase):
    avg_bitrate: float | None = None
    utilization_ratio: float | None = None


class CmcdThroughputPoint(_CmcdBucketPointBase):
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class CmcdStartupPoint(_CmcdBucketPointBase):
    startup_ratio: float | None = None


class _CmcdBitrateBufferMixin(WireSafeModel):
    rebuffer_rate: float | None = None
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        extra = ("rebuffer_rate", "session_count") if "label" in cls.model_fields else ("rebuffer_rate",)
        _move_fields(cls, *extra, "avg_bitrate", "avg_buffer_length")


class CmcdTopContentRow(_CmcdBitrateBufferMixin):
    content_id: str | None = None
    session_count: int


class CmcdRebufferByCountryRow(WireSafeModel):
    country: str | None = None
    rebuffer_rate: float | None = None
    session_count: int
    median_throughput: float | None = None


class CmcdRebufferByAsnRow(_CmcdBitrateBufferMixin):
    asn: int | str
    label: str
    session_count: int


class CmcdObjectTypeRow(WireSafeModel):
    object_type: str | None = None
    request_count: int


class CmcdStreamingFormatRow(WireSafeModel):
    streaming_format: str | None = None
    session_count: int


class CmcdSessionDurationRow(WireSafeModel):
    duration_bucket: str
    session_count: int


class CmcdAggregatesResponse(BaseResponse):
    available: bool
    has_data: bool = True
    overview: CmcdOverview | None = None
    buffer_health_ts: list[CmcdBufferHealthPoint] = []
    bitrate_ts: list[CmcdBitratePoint] = []
    throughput_ts: list[CmcdThroughputPoint] = []
    top_content: list[CmcdTopContentRow] = []
    rebuffer_by_country: list[CmcdRebufferByCountryRow] = []
    rebuffer_by_asn: list[CmcdRebufferByAsnRow] = []
    object_type_dist: list[CmcdObjectTypeRow] = []
    streaming_format_dist: list[CmcdStreamingFormatRow] = []
    sessions_ts: list[CmcdSessionsPoint] = []
    startup_ts: list[CmcdStartupPoint] = []
    session_duration_dist: list[CmcdSessionDurationRow] = []
