"""Bootstrap, cron/audit/profiler status, UX telemetry, and alerts models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from backend.models.admin import (
    LogAccountingResponse,
    LogExtentsResponse,
    ShareBannerResponse,
    ShareHeaderBadgeResponse,
    SyncStatusResponse,
)
from backend.models.analytics import (
    ScoringLabelsListResponse,
    SlowQueriesCountResponse,
    SummaryResponse,
)
from backend.models.common.base import BaseResponse, PassthroughModel, SchemaColumn, WireSafeModel
from backend.models.common.services import LogFieldsCatalogResponse, SavedView


class BootstrapService(WireSafeModel):
    service_id: str
    name: str | None = None
    cmcd_enabled: bool | None = None
    rum_enabled: bool | None = None
    rum_log_period: int | None = None
    is_high_scale: bool | None = None


class BootstrapSettings(WireSafeModel):
    """Typed settings envelope returned by ``GET /api/bootstrap``."""

    initializing: bool | None = None
    storage_mode: str | None = None
    is_remote_analyst: bool | None = None
    needs_login: bool | None = None
    analyst_email: str | None = None
    analyst_name: str | None = None
    mask_ips: bool | None = None
    web_vitals_enabled: bool | None = None


class BootstrapPopGeo(WireSafeModel):
    """One PoP's label parts, parsed from the cached ``/datacenters`` list
    (``get_pop_location_map``); any part may be an empty string."""

    city: str
    region: str
    country: str


class BootstrapDebugState(WireSafeModel):
    """Diagnostics capability and audience settings for authenticated clients."""

    debug_responses_enabled: bool
    query_debug_visibility: str
    api_call_debug_visibility: str


class CustomCard(WireSafeModel):
    """Custom dashboard card entry in BootstrapResponse."""

    id: str
    label: str


class LastSyncInfo(WireSafeModel):
    """Latest non-running sync cron run summary in BootstrapResponse."""

    started_at: str | None = None
    status: str | None = None
    duration_s: float | None = None


class CronRunEntry(WireSafeModel):
    """One row of GET /api/cron-runs.

    ``extra="allow"`` passes any future cron_runs column through verbatim, and
    the field order mirrors ``metadata.cron_log.get_cron_runs`` so the
    serialized key order is byte-identical. Types match the producer exactly to
    avoid any coercion of the wire bytes: ``duration_s`` is a float (writers
    store ``0.0``), the count columns are ints, ``parquet_keys`` is the
    json.loads'd list."""

    # ``id`` / ``task`` / ``started_at`` are NOT NULL columns; ``status`` is nullable text.
    id: int
    task: str
    started_at: str
    duration_s: float | None = None
    status: str | None
    error_message: str | None = None
    files_downloaded: int | None = None
    files_deleted_fos: int | None = None
    rows_ingested: int | None = None
    corrupt_rows: int | None = None
    parquet_files_created: int | None = None
    parquet_files_optimized: int | None = None
    parquet_keys: list[str] = []
    summary: str | None = None


class CronRunsResponse(WireSafeModel):
    total: int | None = None
    page: int | None = None
    per_page: int | None = None
    entries: list[CronRunEntry] = []


class CronScheduleEntry(PassthroughModel):
    """One row of GET /api/cron-schedule (``build_cron_schedule_payload``).
    ``disabled_reason`` appears only on synthesized disabled rows; the
    last_run_* trio only when a run has been recorded."""

    task: str
    next_run_time: str | None
    last_run_time: str | None = None
    last_run_status: str | None = None
    last_run_duration_s: float | None = None
    last_run_summary: str | None = None
    disabled_reason: str | None = None


class CronScheduleResponse(PassthroughModel):
    schedules: list[CronScheduleEntry] = []


class DebugStateResponse(WireSafeModel):
    """``GET /api/debug/state``."""

    debug_responses_enabled: bool


class SchemaResponse(WireSafeModel):
    """``GET /api/schema``: cached or live column list plus the safe table name."""

    schema_: list[SchemaColumn] = Field(alias="schema")
    table_name: str


class AuditLogEntry(WireSafeModel):
    """One ``metadata.get_audit_logs`` row (``details`` is the json-decoded payload)."""

    id: int
    timestamp: str
    event_type: str
    details: dict[str, Any]
    actor: str | None
    source: str


class AuditLogsResponse(WireSafeModel):
    """``GET /api/audit-logs``. ``_debug_calls`` rides along as an extra."""

    total: int
    page: int
    per_page: int
    entries: list[AuditLogEntry]


class SqliteProfilerEntry(BaseModel):
    seq: int
    ts: str
    sql: str
    params_kind: str
    time_ms: float
    rows: int
    op: Literal["execute", "executemany", "executescript"]


class RecentSqliteResponse(BaseModel):
    queries: list[SqliteProfilerEntry]
    buffer_size: int
    buffer_cap: int
    dropped: int
    last_seq: int


class ClearSqliteResponse(BaseModel):
    ok: bool = True
    buffer_size: int
    buffer_cap: int
    dropped: int


class OpsOverviewResponse(WireSafeModel):
    """Seed for OperationsOverview admin cards in BootstrapResponse."""

    queries_summary: SummaryResponse | None = None
    slow_queries_count: SlowQueriesCountResponse | None = None
    log_accounting: LogAccountingResponse | None = None


class BootstrapResponse(BaseResponse):
    active_service_id: str | None
    services: list[BootstrapService]
    schema_: list[SchemaColumn] | None = Field(default=None, alias="schema")
    # _safe_table_name(active_source.name) — the same value /api/schema
    # returns alongside its schema list. Folded in so the frontend can
    # seed its ['admin', 'schema', service_id] React Query cache with the
    # full {schema, table_name} payload and skip the dedicated round-trip
    # on /query nav. None when no active service / status not populated.
    table_name: str | None = None
    countries: dict[str, str] | None = None
    pops: dict[str, tuple[float | None, float | None]] | None = None
    # Per-PoP {city, region, country} for the shared PoP label
    # ("DEN (Denver, CO - USA)"). Parsed from the cached /datacenters list;
    # the frontend formats + renders it (see frontend/lib/format.ts). Separate
    # from `pops` (lat/lon) so the world-map consumer is untouched.
    pop_geo: dict[str, BootstrapPopGeo] | None = None
    settings: BootstrapSettings | None = None
    custom_dashboard_cards: list[CustomCard] = Field(default_factory=list)
    active_log_field_ids: list[str] = Field(default_factory=list)
    ngwaf_configured: bool = False
    # Saved views for the active service, folded in so the frontend can
    # render ViewSelector and rehydrate from URL view params without a
    # second /api/views/{service_id} round-trip on every page nav.
    views: list[SavedView] = Field(default_factory=list)
    # Full log-fields catalog (same payload as /api/log-fields/catalog)
    # for the active service. Folded in so the frontend can seed its
    # ['log-fields-catalog', service_id] React Query cache and skip the
    # 35-KB round-trip on every cold page load (perf audit Phase D).
    # None when no active service.
    log_fields_catalog: LogFieldsCatalogResponse | None = None
    # Cached sync-status (same fast-path payload /api/sync-status?skip_fos=true
    # returns). Folded in so SyncStatusBadge / logs page hit cache on
    # first mount. ADMIN ONLY — None for analyst sessions (matches the
    # dedicated endpoint's 403 for analysts).
    sync_status: SyncStatusResponse | None = None
    # Lean share-status banner ({sharing_active, public_url}). Folded
    # in so the global share banner has its initial state on first
    # render and skips the first /api/admin/share/banner poll.
    # ADMIN ONLY — analysts don't manage sharing.
    share_banner: ShareBannerResponse | None = None
    # Analyst-safe sibling of sync_status, projected down to the two
    # fields the global SyncStatusBadge renders (latest_log_at,
    # local_rows). Available to BOTH admin AND analyst sessions so the
    # badge shows on prod for analyst-shared instances too.
    header_badge: ShareHeaderBadgeResponse | None = None
    # Analyst-safe log extents (same shape as /api/log-extents): the
    # earliest + latest log timestamps the FilterBar uses for its
    # auto-range snap-to-extents. Folded in so the FilterBar's first
    # render skips the dedicated round-trip; the existing 3-s
    # not-yet-populated poll continues from useFilterBar for new
    # services where extents land later.
    log_extents: LogExtentsResponse | None = None
    # Admin DiagnosticsPanel reads backend capability; both roles use the
    # audience settings to honor operator-enabled diagnostics. The response
    # middleware independently enforces analyst visibility.
    debug_state: BootstrapDebugState | None = None
    # Seed for the OperationsOverview admin cards:
    # ``{queries_summary, log_accounting, slow_queries_count}``.
    # The card-level useQueries (10-s poll, same queryKeys) hit cache
    # on first paint so the cards render with real values instead of
    # "—" placeholders. ADMIN ONLY.
    ops_overview: OpsOverviewResponse | None = None
    # Seed for the /logs cron tab: mirrors the lean delta-poll shape of
    # /api/cron-runs?per_page=10 with ``with_total=False`` so the
    # count(*) precount stays off the cold-path WAL writer. The heavy
    # 500-row cron-history pull is intentionally NOT seeded — it's
    # tab-gated and a session-3 lesson (commit bbbd381) showed seeding
    # expensive payloads dominates the bootstrap hot path. The cron
    # schedule itself is a lazy-load on the cron tab (P1#5).
    # ADMIN ONLY — analysts don't reach /logs.
    cron_runs_first_page: CronRunsResponse | None = None
    # Seed for useLastSync (the "Last Sync: Xs ago" header badge).
    # Shape: ``{started_at, status, duration_s}`` of the latest non-running
    # sync cron run, derived from ``latest_cron_per_task("sync")``. Without
    # this seed every admin page-load fired one mandatory
    # /api/cron-runs?task=sync request + 1-2 SSE-invalidation-driven
    # refetches before the 5-min poll TTL kicked in. ADMIN ONLY.
    last_sync: LastSyncInfo | None = None
    # Seed for useScoringLabels — TopFlaggedTable + admin Labels tab +
    # dashboard Flag column all read from ``['scoring-labels', sid]``.
    # Without this seed every cold load fires
    # GET /api/services/{sid}/scoring/labels (p95 311 ms admin / 702 ms
    # analyst). Shape mirrors the endpoint: ``{labels: [...], counts: {...}}``.
    # ADMIN ONLY.
    scoring_labels: ScoringLabelsListResponse | None = None
    # section_timings is inherited from BaseResponse.


class UxEventPayload(BaseModel):
    """Generic UX-event envelope. Specific event types live as
    ``event`` discriminator strings; the ``details`` bag carries
    event-specific fields without forcing a per-event schema."""

    event: str = Field(..., max_length=80)
    pathname: str | None = Field(default=None, max_length=300)
    component_id: str | None = Field(default=None, max_length=120)
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("details")
    @classmethod
    def validate_details(cls, v: dict[str, Any]) -> dict[str, Any]:
        if len(str(v)) > 4096:
            raise ValueError("Details payload too large")

        def _check(obj: Any, depth: int = 0) -> None:
            if depth > 5:
                raise ValueError("Details too deep")
            if isinstance(obj, dict):
                if len(obj) > 50:
                    raise ValueError("Too many items")
                for val in obj.values():
                    _check(val, depth + 1)
            elif isinstance(obj, list):
                if len(obj) > 50:
                    raise ValueError("Too many items")
                for item in obj:
                    _check(item, depth + 1)

        _check(v)
        return v

    model_config = {
        "extra": "forbid",
    }


class WebVitalsPayload(BaseModel):
    """Mirrors the web-vitals SDK metric shape (v5 API)."""

    id: str = Field(..., max_length=120)
    name: Literal["CLS", "FCP", "FID", "INP", "LCP", "TTFB"]
    value: float
    rating: Literal["good", "needs-improvement", "poor"]
    pathname: str | None = Field(default=None, max_length=300)
    navigation_type: str | None = Field(default=None, max_length=40)
    delta: float | None = None


# ── Alerts Models (folded from alerts.py) ─────────────────────────────────────


class AlertChannel(BaseModel):
    type: Literal["slack", "pagerduty", "webhook"]
    url: str
    config: dict | None = None


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


UxTelemetryPayload = UxEventPayload
WebVitalPayload = WebVitalsPayload

StatusResponse = ClearSqliteResponse
