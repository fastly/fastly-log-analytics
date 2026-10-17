"""Response models for admin action/status routes that previously returned bare dicts.

Each model mirrors the producer's dict (handler return value in ``routers/admin/*``,
``sharing_domain``, ``main`` health probe, ``web_vitals`` / ``ux_events``). They derive from
``WireSafeModel`` (extra="allow", unset fields omitted) so the wire body stays byte-identical;
inner dicts that may hold non-JSON natives keep the ``jsonable_encoder`` encoding via
``JsonDict`` / ``JsonRows``.
"""

from __future__ import annotations

from backend.models._base import JsonAny, JsonDict, JsonRows, WireSafeModel


class BotSourceRefreshResponse(WireSafeModel):
    """``POST /admin/bot-sources/{source_id}/refresh``."""

    ok: bool
    source: JsonDict


class BotSourcesRefreshResponse(WireSafeModel):
    """``POST /admin/bot-sources/refresh`` and ``/admin/bots/refresh``."""

    ok: bool
    sources: JsonRows
    updated_count: int
    failed_count: int


class RdnsEnrichSummary(WireSafeModel):
    resolved: int
    errors: int
    discovered: int
    batch_limit: int


class RdnsEnrichResponse(WireSafeModel):
    """``POST /admin/rdns/enrich``."""

    ok: bool
    summary: RdnsEnrichSummary


class DuckdbRecycleResponse(WireSafeModel):
    """``POST /admin/duckdb/recycle``: ``skipped`` carries reason/rss_mb/threshold_mb, ``recycled`` the rest."""

    ok: bool
    status: str
    reason: str | None = None
    rss_mb: float | None = None
    threshold_mb: float | None = None
    detail: str | None = None
    rss_before_mb: float | None = None
    rss_after_mb: float | None = None
    freed_mb: float | None = None


class DucklakeMigrateResponse(WireSafeModel):
    """``POST /admin/ducklake/migrate`` (202)."""

    ok: bool
    message: str
    service_id: str


class NgwafSyncResponse(WireSafeModel):
    """``POST /admin/ngwaf/sync/{service_id}``."""

    ok: bool
    service_id: str
    workspace_id: str
    stats: JsonDict


class SharePurgeResponse(WireSafeModel):
    """``POST /admin/share/purge``: counts from ``share_db.purge_all_share_records``."""

    ok: bool
    deleted_audit_logs: int | None = None
    deleted_expired_invites: int | None = None
    deleted_stale_sessions: int | None = None
    deleted_claim_tokens: int | None = None
    audit_retention_days: int | None = None
    max_idle_session_days: int | None = None


class IcebergCalendarDay(WireSafeModel):
    data_files: int
    size_bytes: int


class IcebergCalendarResponse(WireSafeModel):
    """``GET /admin/iceberg-calendar``: ISO date -> per-day entry (open-keyed), plus debug envelope keys."""

    __pydantic_extra__: dict[str, IcebergCalendarDay | list]  # type: ignore[assignment]


class VclHealthResponse(WireSafeModel):
    """``GET /admin/vcl-health``: ``error`` replaces the snippet fields when the check cannot run."""

    service_id: str
    is_clean: bool
    error: str | None = None
    active_version: int | str | None = None
    legacy_snippets_found: int | None = None
    legacy_names: list[str] | None = None
    consolidated_snippets_found: int | None = None
    consolidated_names: list[str] | None = None
    recommendation: str | None = None


class PageTelemetryQuery(WireSafeModel):
    engine: str | None = None
    sql_query: str | None = None
    duration_ms: float | None = None


class PageTelemetrySection(WireSafeModel):
    section_name: str | None = None
    duration_ms: float | None = None


class PageTelemetryCall(WireSafeModel):
    service: str
    method: str | None = None
    path: str | None = None
    status: str | int | None = None
    time_ms: float | None = None
    caller: str | None = None
    bytes: int | None = None


class PageTelemetryResponse(WireSafeModel):
    """``GET /debug/page-telemetry``."""

    queries: list[PageTelemetryQuery]
    sections: list[PageTelemetrySection]
    calls: list[PageTelemetryCall]


class BackfillWindowResponse(WireSafeModel):
    """``POST /admin/backfill-window``: ``iceberg.sync_data`` counters, or ``error`` + ``files_downloaded=0``."""

    error: str | None = None
    files_downloaded: int | None = None
    rows_downloaded: int | None = None
    bytes_downloaded: int | None = None
    files_removed: int | None = None
    files_skipped: int | None = None


class HealthServiceState(WireSafeModel):
    service_id: str
    status: str
    last_ingest: JsonAny = None
    last_sync_status: str | None = None
    last_sync_at: JsonAny = None
    reason: str | None = None
    stale_minutes_used: int | None = None


class HealthResponse(WireSafeModel):
    """``GET /api/health``: liveness payload; ``services`` only for ``?deep=1`` local callers."""

    status: str
    version: str
    services: list[HealthServiceState] | None = None
