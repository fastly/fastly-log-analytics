"""Bootstrap, sources, and schema endpoints."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request

from backend.deps import get_meta_con, get_service_id, get_source
from backend.models.common import BootstrapResponse
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["bootstrap"])


@router.get("/bootstrap", response_model=BootstrapResponse)
def bootstrap(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    from backend.core import duckdb as _db
    from backend.core.duckdb import STORAGE_MODE
    from backend.services.service_manager import get_enriched_services
    from backend.utils.countries import COUNTRY_MAP
    from backend.utils.pop_utils import get_pop_lat_lon_map

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

            analyst_session = get_tunnel_manager().validate_session(sid)
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
        )

    src: dict | None = None
    if service_id:
        src = _db.get_source_for_service(service_id)

    services = get_enriched_services(service_id)

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
    if valid_active_id:
        active_svc = next((s for s in services if s.get("service_id") == valid_active_id), None)
        if active_svc and active_svc.get("status"):
            schema = active_svc["status"].get("schema", [])

    # NOTE: the previous fallback opened a read-only DuckDB connection here
    # and ran get_schema() against the source on cold-cache loads. That call
    # acquired the per-service lock + did a parquet glob, costing 1-3s on
    # the very first /api/bootstrap after a backend restart and blocking
    # the whole admin UI from rendering. With the status-refresh cron
    # populating active_svc["status"]["schema"], the cache is the source
    # of truth — drop the fallback. If schema is empty here, the dashboard
    # renders without a hint banner; the user can refresh once the cron
    # has run (typically <60s after startup).

    pops = get_pop_lat_lon_map()

    # Include custom field info so the dashboard can render custom distribution cards
    # without a separate fetch. We load the raw config here because the enriched
    # services list above strips log_fields out.
    custom_dashboard_cards: list[dict] = []
    custom_fields_catalog: list[dict] = []
    active_log_field_ids: list[str] = []
    if valid_active_id:
        from backend import config as svcconfig
        from backend.core import log_fields as _lf

        active_cfg = svcconfig.load_config(valid_active_id)
        if active_cfg:
            lf_config = _lf.get_lf_config(active_cfg)
            custom_fields_catalog = _lf.get_custom_fields_catalog_entries(lf_config)
            custom_dashboard_cards = [
                {"id": f["id"], "label": f["label"]} for f in custom_fields_catalog if f.get("show_in_dashboard")
            ]
            active_log_field_ids = sorted(_lf.resolve_enabled_fields(lf_config)) + [
                cf["name"] for cf in lf_config.get("custom_fields", []) if cf.get("enabled", True)
            ]

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
        custom_fields_catalog=custom_fields_catalog,
        active_log_field_ids=active_log_field_ids,
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
    con: duckdb.DuckDBPyConnection = Depends(get_meta_con),
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
    from backend.core import log_fields as lf
    from backend.core.log_fields import INSIGHT_DEFINITIONS

    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None and service_id is not None:
        allowed = set(analyst_session.service_ids or [])
        if service_id not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": service_id},
            )

    # Try to load existing limits
    field_limits = {}
    if service_id:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        if cfg:
            lf_config = lf.get_lf_config(cfg)
            field_limits = lf_config.get("field_limits", {})
            custom_entries = lf.get_custom_fields_catalog_entries(lf_config)
        else:
            custom_entries = []
    else:
        custom_entries = []

    fields = lf.get_catalog_for_api(field_limits) + custom_entries

    return {
        "groups": lf.get_groups_for_api(),
        "fields": fields,
        "insights": INSIGHT_DEFINITIONS,
        "presets": {
            name: {"label": p["label"], "description": p["description"], "groups": p["groups"]}
            for name, p in lf.PRESETS.items()
        },
    }


from backend.models.dashboard import InsightsAvailabilityResponse


@router.get("/insight-availability", response_model=InsightsAvailabilityResponse)
@query_errors(status_code=500)
def insight_availability(
    request: Request,
    source: dict = Depends(get_source),
    con: duckdb.DuckDBPyConnection = Depends(get_meta_con),
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

    actual_cols = {col["name"] for col in get_schema(con, source)}
    from backend.core.log_fields import INSIGHT_DEFINITIONS

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
