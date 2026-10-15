from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, StringConstraints

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
    # Display name for the value when it differs from the raw value (ASN → "Comcast Cable (7922)").
    label: str | None = None
    # Distinct (subscriber id, IP) pairs; IP alone for requests without one.
    users: int | None = None
    requests: int
    bytes: int | None = None


class ContentSecuritySubscriberRow(BaseModel):
    subscriber_id: str
    distinct_ips: int
    distinct_countries: int | None = None
    requests: int
    bytes: int | None = None
    last_seen: str | None = None


class ContentSecurityContentId(BaseModel):
    content_id: str
    requests: int


class ContentSecurityBandwidthPoint(BaseModel):
    bucket: str
    edge_bytes: int
    shield_bytes: int | None = None


class ContentSecurityEnforcementRow(BaseModel):
    # One row per cs_status value (allowed / blocked / throttled / check_failed /
    # counted / rate_limited). ``enforced`` is the subset of ``requests`` whose
    # mode column said ENFORCE; the rest were only recorded (WARN).
    status: str
    requests: int
    enforced: int
    users: int | None = None
    bytes: int | None = None


class ContentSecurityEnforcementPoint(BaseModel):
    bucket: str
    status: str
    requests: int


class ContentSecurityResponse(BaseResponse):
    available: bool
    reason: str | None = None
    fields: dict[str, bool] = {}
    content_id: str | None = None
    has_shield_split: bool = False
    content_ids: list[ContentSecurityContentId] = []
    top_countries: list[ContentSecurityTopRow] = []
    top_asns: list[ContentSecurityTopRow] = []
    top_referers: list[ContentSecurityTopRow] = []
    top_hosts: list[ContentSecurityTopRow] = []
    top_subscribers: list[ContentSecuritySubscriberRow] = []
    # Every p_type / p_desc value (not capped by top_n).
    proxy_types: list[ContentSecurityTopRow] = []
    proxy_descriptions: list[ContentSecurityTopRow] = []
    subscribers_masked: bool = False
    bandwidth_ts: list[ContentSecurityBandwidthPoint] = []
    enforcement: list[ContentSecurityEnforcementRow] = []
    enforcement_ts: list[ContentSecurityEnforcementPoint] = []


class SubscriberIpsRequest(FilteredRequest):
    subscriber_id: Annotated[str, StringConstraints(min_length=1, max_length=512)]
    content_id: Annotated[str, StringConstraints(max_length=512)] | None = None
    range_token: str | None = None
    anchor: str | None = None


class SubscriberIpRow(BaseModel):
    # One row per distinct (ip, country, host) combination.
    ip: str
    country: str | None = None
    host: str | None = None
    requests: int
    bytes: int | None = None
    last_seen: str | None = None


class SubscriberIpsResponse(BaseResponse):
    available: bool
    subscriber_id: str
    fields: dict[str, bool] = {}
    ips: list[SubscriberIpRow] = []
    # More IPs exist than the list carries (capped, most recent first).
    truncated: bool = False


# ── Content Security blocklist (admin-only) ──────────────────────────────────

# Categories the blocklist HTTP function receives as ``category``.
BlockCategory = Literal["country", "asn", "referer", "host", "proxy_type", "proxy_description", "subscriber_id", "ip"]


class ContentSecuritySettings(BaseModel):
    """Global blocklist config (Admin → Overall Settings → Content Security).

    The auth header's value is a credential and is never returned — only its name.
    """

    blocklist_url: str = ""
    # e.g. "Authorization"; null when no header is configured.
    auth_header_name: str | None = None


class ContentSecuritySettingsUpdate(BaseModel):
    blocklist_url: Annotated[str, StringConstraints(max_length=2048)]
    # Omitted/null keeps the saved header, "" removes it, "Name: value" replaces it.
    # Length/format are checked by parse_auth_header, whose errors never echo the
    # value — a model constraint would echo the credential back in the 422 body.
    auth_header: str | None = None


class ContentSecurityBlockCondition(BaseModel):
    category: BlockCategory
    value: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2048)]


class ContentSecurityBlockRequest(BaseModel):
    # All conditions must match (AND). One per category — two values for the
    # same category could never both match.
    conditions: Annotated[list[ContentSecurityBlockCondition], Field(min_length=1, max_length=8)]
    # 0 blocks matching traffic outright; > 0 throttles it to that many kbps.
    kbps: Annotated[int, Field(ge=0, le=10_000_000)] = 0


class ContentSecurityBlock(BaseModel):
    id: str
    conditions: list[ContentSecurityBlockCondition]
    kbps: int = 0
    created_at: str
    created_by: str
    unblocked_at: str | None = None
    unblocked_by: str | None = None


class ContentSecurityBlocksResponse(BaseModel):
    # False until an admin sets the blocklist URL — the tab then points them at Admin settings.
    configured: bool
    blocks: list[ContentSecurityBlock]
