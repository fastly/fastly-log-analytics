"""Bootstrap, sources, and schema endpoints."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request

from backend.deps import get_con, get_service_id, get_source
from backend.models.common import BootstrapResponse
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["bootstrap"])


@router.get("/bootstrap", response_model=BootstrapResponse)
def bootstrap(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    import time as _time

    from backend.core import duckdb as _db
    from backend.core.duckdb import STORAGE_MODE
    from backend.services.service_manager import get_enriched_services
    from backend.utils.countries import COUNTRY_MAP
    from backend.utils.pop_utils import get_pop_lat_lon_map

    # Cold-path attribution: time each major phase so the harness can pin
    # which section owns the bootstrap wall time. Each entry is
    # {"section": str, "time_ms": float} and surfaces via
    # BootstrapResponse._section_timings.
    section_timings: list[dict] = []

    def _timed(name: str, fn):
        t0 = _time.monotonic()
        try:
            return fn()
        finally:
            section_timings.append({"section": name, "time_ms": round((_time.monotonic() - t0) * 1000, 2)})

    # /api/bootstrap is in _UNAUTH_ANALYST_PATHS so anonymous remote visitors
    # can get a stub response telling the frontend to redirect them to
    # /share-login. The middleware therefore SKIPS session validation for
    # this path — we validate the cookie manually here so authenticated
    # analysts still get the full response with their scoped services.
    analyst_session = getattr(request.state, "analyst_session", None)
    is_remote = getattr(request.state, "is_remote", False)
    if is_remote and analyst_session is None:
        sid = request.cookies.get("analyst_session_id")
        if sid:
            from backend.utils.tunnel import get_tunnel_manager

            def _validate():
                return get_tunnel_manager().validate_session(sid)

            analyst_session = _timed("validate_analyst_session", _validate)
            if analyst_session is not None:
                request.state.analyst_session = analyst_session

    # Anonymous remote visitor: return a minimal stub so the frontend hides
    # admin nav and redirects to /share-login.
    if is_remote and analyst_session is None:
        return BootstrapResponse.with_telemetry(
            active_service_id=None,
            services=[],
            settings={
                "is_remote_analyst": True,
                "needs_login": True,
            },
            section_timings=section_timings,
        )

    src: dict | None = None
    if service_id:
        src = _timed("get_source_for_service", lambda: _db.get_source_for_service(service_id))

    services = _timed("get_enriched_services", lambda: get_enriched_services(service_id))

    # Analyst path: filter services to those scoped on the invite and force
    # access_level=read_only regardless of what get_source_for_service returned.
    if analyst_session is not None:
        allowed = set(analyst_session.service_ids or [])
        services = [s for s in services if s.get("service_id") in allowed]
        if service_id not in allowed:
            # Reset active id to the first allowed service (if any).
            service_id = next((s.get("service_id") for s in services), None)
            src = _db.get_source_for_service(service_id) if service_id else None

    # Validate the provided service_id exists; if not, fallback to the first one
    valid_active_id = service_id
    if service_id and not any(s.get("service_id") == service_id for s in services):
        valid_active_id = services[0].get("service_id") if services else None

    schema: list = []

    # Use cached schema from config to avoid acquiring a DB lock
    def _resolve_schema() -> list:
        if not valid_active_id:
            return []
        active_svc = next((s for s in services if s.get("service_id") == valid_active_id), None)
        if active_svc and active_svc.get("status"):
            return active_svc["status"].get("schema", []) or []
        return []

    schema = _timed("schema_lookup", _resolve_schema)

    # NOTE: the previous fallback opened a read-only DuckDB connection here
    # and ran get_schema() against the source on cold-cache loads. That call
    # acquired the per-service lock + did a parquet glob, costing 1-3s on
    # the very first /api/bootstrap after a backend restart and blocking
    # the whole admin UI from rendering. With the status-refresh cron
    # populating active_svc["status"]["schema"], the cache is the source
    # of truth — drop the fallback. If schema is empty here, the dashboard
    # renders without a hint banner; the user can refresh once the cron
    # has run (typically <60s after startup).

    pops = _timed("get_pop_lat_lon_map", get_pop_lat_lon_map)

    # Per the perf audit (F6): bootstrap's custom_fields_catalog was a
    # ~10-15 KB duplicate of what every chart page already fetches
    # separately from /api/log-fields/catalog. No frontend code reads
    # bootstrap.custom_fields_catalog (the dashboard card hook only
    # uses custom_dashboard_cards and active_log_field_ids — both
    # derived from the catalog and shipped here). Keep the field on the
    # response model for API back-compat but emit it empty.
    custom_dashboard_cards: list[dict] = []
    active_log_field_ids: list[str] = []

    def _resolve_custom_fields():
        nonlocal custom_dashboard_cards, active_log_field_ids
        if not valid_active_id:
            return
        from backend import config as svcconfig
        from backend.core import log_fields as _lf

        active_cfg = svcconfig.load_config(valid_active_id)
        if not active_cfg:
            return
        lf_config = _lf.get_lf_config(active_cfg)
        catalog_entries = _lf.get_custom_fields_catalog_entries(lf_config)
        custom_dashboard_cards = [
            {"id": f["id"], "label": f["label"]} for f in catalog_entries if f.get("show_in_dashboard")
        ]
        active_log_field_ids = sorted(_lf.resolve_enabled_fields(lf_config)) + [
            cf["name"] for cf in lf_config.get("custom_fields", []) if cf.get("enabled", True)
        ]

    _timed("custom_fields_catalog", _resolve_custom_fields)

    # Perf audit Phase D: fold the log-fields catalog into the
    # bootstrap response so the frontend can seed its
    # ['log-fields-catalog', service_id] React Query cache from the
    # same payload (mirrors how `views` is already seeded). Saves one
    # HTTP round-trip + ~35 KB transfer on every cold page load
    # without changing the dedicated /api/log-fields/catalog endpoint
    # (other consumers / direct callers still work). Analyst-scope is
    # already enforced for valid_active_id above.
    log_fields_catalog_payload: dict | None = None

    def _resolve_log_fields_catalog():
        nonlocal log_fields_catalog_payload
        if not valid_active_id:
            return
        log_fields_catalog_payload = _compute_log_fields_catalog(valid_active_id)

    _timed("log_fields_catalog", _resolve_log_fields_catalog)

    # Phase D-2: fold the cached sync-status into bootstrap for admin
    # callers. The dedicated /api/sync-status endpoint is admin-only
    # (RemoteAccessMiddleware blocks analysts) — same restriction
    # applies here. Frontend seeds its ['sync-status', service_id]
    # React Query cache so SyncStatusBadge / useLogsPageState hit
    # cache on first call instead of paying a round-trip.
    #
    # Only emit when the analyst gate would let the dedicated endpoint
    # return data: admin caller AND a valid_active_id with cached
    # status persisted. Analyst sessions get None, matching their 403
    # on the dedicated endpoint.
    sync_status_payload: dict | None = None

    def _resolve_sync_status():
        nonlocal sync_status_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        from backend.routers.admin import compute_sync_status_cached

        sync_status_payload = compute_sync_status_cached(valid_active_id)

    _timed("sync_status", _resolve_sync_status)

    # Header badge: analyst-safe sibling of sync_status, projected
    # down to the two fields the global SyncStatusBadge renders
    # (Latest Log: timestamp, Total Logs: row count). Emitted for
    # BOTH admin and analyst so analysts see the badge on prod the
    # same way admins do — previously they got null because
    # /api/sync-status is admin-only and the badge fell through.
    header_badge_payload: dict | None = None

    def _resolve_header_badge():
        nonlocal header_badge_payload
        if not valid_active_id:
            return
        # svcconfig.get_status is keyed on the service NAME, not the
        # service_id. They're often identical, but resolving via the
        # source dict matches the dedicated /api/sync-status handler
        # exactly so analyst/admin both look in the same place.
        active_src = _db.get_source_for_service(valid_active_id)
        if not active_src:
            return
        from backend import config as svcconfig

        cached_status = svcconfig.get_status(active_src["name"]) or {}
        latest = (
            cached_status.get("latest_log_at")
            or cached_status.get("latest_available_file_at")
            or cached_status.get("latest_ingested_file_at")
        )
        local_rows = cached_status.get("local_rows")
        if latest is None and local_rows is None:
            return
        header_badge_payload = {
            "latest_log_at": latest,
            "local_rows": local_rows,
        }

    _timed("header_badge", _resolve_header_badge)

    views: list[dict] = []

    def _resolve_views() -> list[dict]:
        if not valid_active_id:
            return []
        from backend.repositories import views as _views_repo

        try:
            return _views_repo.get_views(valid_active_id)
        except Exception:
            # Views are a UX nicety, not a correctness gate. A repo error
            # must not break /api/bootstrap.
            return []

    views = _timed("views", _resolve_views)

    # Force read_only for analyst sessions regardless of underlying source.
    if analyst_session is not None:
        access_level = "read_only"
    elif src and valid_active_id == service_id:
        access_level = src.get("access_level", "read_write")
    else:
        access_level = "read_write"

    return BootstrapResponse.with_telemetry(
        active_service_id=valid_active_id,
        services=services,
        schema=schema,
        countries=COUNTRY_MAP,
        pops=pops,
        settings={
            "access_level": access_level,
            "storage_mode": STORAGE_MODE,
            "is_remote_analyst": analyst_session is not None,
            "analyst_email": analyst_session.email if analyst_session else None,
            "analyst_name": analyst_session.name if analyst_session else None,
        },
        custom_dashboard_cards=custom_dashboard_cards,
        active_log_field_ids=active_log_field_ids,
        views=views,
        log_fields_catalog=log_fields_catalog_payload,
        sync_status=sync_status_payload,
        header_badge=header_badge_payload,
        section_timings=section_timings,
    )


@router.get("/sources")
@query_errors(status_code=500)
def sources_endpoint(request: Request):
    """Return storage metadata (endpoint / bucket / prefix / region) for the
    configured sources the caller is authorized to see.

    Security: filter by analyst session scope. Without this, an
    authenticated analyst can enumerate every service's S3 bucket / endpoint
    / prefix configuration, including ones not in their invite. Admin
    requests (no analyst_session on request.state) see the full list.
    """
    from backend import config as svcconfig
    from backend.core.duckdb import _safe_table_name

    analyst_session = getattr(request.state, "analyst_session", None)
    allowed: set[str] | None = set(analyst_session.service_ids or []) if analyst_session else None

    configs = svcconfig.list_configs()
    sources = []
    for cfg in configs:
        if allowed is not None and cfg.get("service_id") not in allowed:
            continue
        src = svcconfig.config_to_source(cfg)
        sources.append(
            {
                "name": src["name"],
                "table_name": _safe_table_name(src["name"]),
                "endpoint": src["endpoint"],
                "bucket": src["bucket"],
                "prefix": src["prefix"],
                "region": src["region"],
            }
        )
    return sources


@router.get("/schema")
@query_errors(status_code=500)
def schema_endpoint(
    request: Request,
    source: dict = Depends(get_source),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    from backend import config as svcconfig
    from backend.core.duckdb import _safe_table_name, get_schema

    # Cross-tenant guard: an analyst session scoped to ``svc-A`` must not
    # be able to read ``svc-B``'s schema (custom-field names, types, and
    # PII flags). Mirrors the check in ``log_fields_catalog``.
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        allowed = set(analyst_session.service_ids or [])
        if source.get("name") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": source.get("name")},
            )

    # Try cache first
    cached_status = svcconfig.get_status(source["name"])
    if cached_status and "schema" in cached_status:
        return {"schema": cached_status["schema"], "table_name": _safe_table_name(source["name"])}

    return {"schema": get_schema(con, source), "table_name": _safe_table_name(source["name"])}


def _compute_log_fields_catalog(service_id: str | None) -> dict:
    """Build the log-fields catalog payload for ``service_id``.

    Extracted so /api/bootstrap can fold the catalog into its response
    (page-shell composite, perf audit Phase D) without paying a second
    HTTP round-trip on every cold page load.

    Caller is responsible for analyst-scope enforcement on ``service_id``
    before invoking — this helper trusts the caller.
    """
    from backend.core import field_registry as fr
    from backend.core import log_fields as lf

    field_limits: dict = {}
    custom_entries: list = []
    if service_id:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        if cfg:
            lf_config = lf.get_lf_config(cfg)
            field_limits = lf_config.get("field_limits", {})
            custom_entries = lf.get_custom_fields_catalog_entries(lf_config)

    fields = fr.get_catalog_for_api(field_limits) + custom_entries

    return {
        "groups": fr.get_groups_for_api(),
        "fields": fields,
        "insights": fr.INSIGHT_DEFINITIONS,
        "presets": {
            name: {"label": p["label"], "description": p["description"], "groups": p["groups"]}
            for name, p in fr.PRESETS.items()
        },
    }


@router.get("/log-fields/catalog")
@query_errors(status_code=500)
def log_fields_catalog(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    """Return the log-fields catalog for the requested service.

    Security: enforce analyst session scope on the requested
    ``service_id``. Without this, an analyst scoped to ``svc-A`` can pass
    ``?service_id=svc-B`` and read svc-B's custom field configuration
    (including PII-related field configs).
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None and service_id is not None:
        allowed = set(analyst_session.service_ids or [])
        if service_id not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": service_id},
            )

    return _compute_log_fields_catalog(service_id)


from backend.models.dashboard import InsightsAvailabilityResponse


@router.get("/insight-availability", response_model=InsightsAvailabilityResponse)
@query_errors(status_code=500)
def insight_availability(
    request: Request,
    source: dict = Depends(get_source),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    from backend.core.duckdb import get_schema

    # Cross-tenant guard: insight availability discloses which fields are
    # populated (presence/absence of optional columns), so it needs the
    # same scope check as the schema endpoint.
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        allowed = set(analyst_session.service_ids or [])
        if source.get("name") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": source.get("name")},
            )

    # Prefer the cached schema snapshot maintained by the status-refresh
    # cron — same source of truth the /schema endpoint and /bootstrap
    # already use. Saves ~300 ms per /insight-availability call because
    # we skip the per-service lock + parquet glob that get_schema would
    # otherwise pay on cold cache, especially when /insights is in
    # flight concurrently.
    from backend import config as svcconfig

    actual_cols: set[str] = set()
    cached_status = svcconfig.get_status(source["name"])
    if cached_status and "schema" in cached_status:
        actual_cols = {col["name"] for col in cached_status["schema"]}
    if not actual_cols:
        # Fallback: cron hasn't populated status yet (cold-start
        # within the first ~60s after backend boot). Do the live
        # lookup so first-load isn't a 503 — subsequent calls hit
        # the cron-populated cache.
        actual_cols = {col["name"] for col in get_schema(con, source)}
    from backend.core.field_registry import INSIGHT_DEFINITIONS

    result = []
    for d in INSIGHT_DEFINITIONS:
        req_cols = d.get("required_fields", [])
        available = all(c in actual_cols for c in req_cols)
        result.append({**d, "available": available})
    return {"insights": result, "available": True}


@router.get("/dma.json")
@query_errors(status_code=500)
def dma_json():
    import os

    from fastapi.responses import FileResponse

    for fname in ("data/system/dma_geojson.json", "data/system/dma.json"):
        if os.path.exists(fname):
            return FileResponse(fname)
    from backend.core import duckdb as _db

    return _db._get_dma_map()
