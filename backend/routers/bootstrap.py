"""Bootstrap, sources, and schema endpoints."""

from __future__ import annotations

import asyncio
import time

import duckdb
from fastapi import APIRouter, Depends, Request
from fastapi.concurrency import run_in_threadpool

from backend.deps import get_con, get_service_id, get_source
from backend.models.common import BootstrapResponse
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories._base import SectionTimer
from backend.utils.auth import mask_ips_for, require_service_in_scope
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["bootstrap"], responses=DEFAULT_ERROR_RESPONSES)


# Single-flight + short-TTL cache for the loopback ADMIN bootstrap path.
# The admin branch runs ~6 SQLite reads against the per-service metadata.db
# that the sync cron concurrently writes; under a reload storm those reads
# stack behind SQLite's 30s busy_timeout and bootstrap wall time balloons to
# 100s+ (prod outage 2026-06-23). Coalescing concurrent identical admin
# bootstraps into ONE computation, plus a short TTL, caps the blast radius.
#
# ONLY the loopback admin path is cached. Analyst / anonymous (remote)
# callers are never cached: their payload is per-session (scoped services,
# email/name, and never the admin_token) AND they skip every contended
# admin-only section, so they're already cheap and must not share an
# admin-shaped body. Keyed by active service_id — the loopback admin is a
# single identity.
_BOOTSTRAP_CACHE_TTL_S = 3.0
_bootstrap_cache: dict[str, tuple[float, BootstrapResponse]] = {}
_bootstrap_inflight: dict[str, asyncio.Future] = {}

from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.bootstrap._bootstrap_cache", _bootstrap_cache)
_CacheRegistry.register("routers.bootstrap._bootstrap_inflight", _bootstrap_inflight)
# NB: the short-TTL admin bootstrap response is derived from the on-disk
# config set, so ANY mutation of that set (provision writes / teardown removes
# a config) must drop this cache or the change lingers for up to
# _BOOTSTRAP_CACHE_TTL_S (a torn-down service keeps appearing; a new one stays
# missing). The provision router does that via
# CacheRegistry.clear("routers.bootstrap._bootstrap_cache") — going through the
# registry rather than importing this router keeps them import-independent.


@router.get("/bootstrap", response_model=BootstrapResponse)
async def bootstrap(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    import sys

    main_module = sys.modules.get("backend.main")
    startup_complete = getattr(main_module, "_startup_complete", False) if main_module else False

    if "pytest" in sys.modules:
        startup_complete = True

    if not startup_complete:
        from backend.core.web_vitals_store import collection_enabled as _web_vitals_enabled

        return BootstrapResponse.with_telemetry(
            active_service_id=None,
            services=[],
            settings={
                "initializing": True,
                "is_remote_analyst": False,
                "needs_login": False,
                "web_vitals_enabled": _web_vitals_enabled(),
            },
        )
    # NB: kept docstring-free on purpose — FastAPI publishes a route
    # docstring as the OpenAPI `description`, and this handler had none.
    # Coalesce + short-TTL cache the loopback admin bootstrap; run the
    # analyst/anonymous path fresh. The heavy work lives in _bootstrap_sync
    # (a sync def run in the threadpool).
    is_remote = getattr(request.state, "is_remote", False)
    # Analyst / anonymous: per-session, cheap — never cached.
    if is_remote:
        return await run_in_threadpool(_bootstrap_sync, request, service_id)

    # Loopback admin: the check-cache / check-inflight / create-future
    # critical section below contains NO await, so it runs atomically on the
    # single-threaded event loop — race-free without a lock.
    key = service_id or ""
    now = time.monotonic()
    cached = _bootstrap_cache.get(key)
    if cached is not None and (now - cached[0]) < _BOOTSTRAP_CACHE_TTL_S:
        return cached[1]
    inflight = _bootstrap_inflight.get(key)
    if inflight is None:
        inflight = asyncio.ensure_future(_bootstrap_compute_and_cache(request, service_id, key))
        _bootstrap_inflight[key] = inflight
    return await inflight


async def _bootstrap_compute_and_cache(request: Request, service_id: str | None, key: str) -> BootstrapResponse:
    try:
        result = await run_in_threadpool(_bootstrap_sync, request, service_id)
        _bootstrap_cache[key] = (time.monotonic(), result)
        return result
    finally:
        # Always release the in-flight slot. Errors propagate to every
        # awaiter and are never cached, so the next request recomputes.
        _bootstrap_inflight.pop(key, None)


def _bootstrap_sync(
    request: Request,
    service_id: str | None,
) -> BootstrapResponse:
    from backend.core import duckdb as _db
    from backend.core.duckdb import STORAGE_MODE
    from backend.services.service_manager import get_enriched_services
    from backend.utils.countries import COUNTRY_MAP
    from backend.utils.pop_utils import get_pop_lat_lon_map, get_pop_location_map

    # Cold-path attribution: time each major phase so the harness can pin
    # which section owns the bootstrap wall time. Each entry is
    # {"section": str, "time_ms": float} and surfaces via
    # BootstrapResponse._section_timings.
    timer = SectionTimer()
    section_timings = timer.entries

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

            analyst_session = timer.call("validate_analyst_session", _validate)
            if analyst_session is not None:
                request.state.analyst_session = analyst_session

    # Anonymous remote visitor: return a minimal stub so the frontend hides
    # admin nav and redirects to /share-login.
    #
    # Telemetry is intentionally omitted from this path: shipping the
    # SectionTimer entries to an unauthenticated caller gives them a
    # microsecond-precision oracle on validate_analyst_session execution
    # time, which strips network jitter from any timing attack on the
    # session cookie. The authenticated path below still emits
    # section_timings for the dev/admin perf harness.
    if is_remote and analyst_session is None:
        from backend.core.web_vitals_store import collection_enabled as _web_vitals_enabled

        return BootstrapResponse.with_telemetry(
            active_service_id=None,
            services=[],
            settings={
                "is_remote_analyst": True,
                "needs_login": True,
                # Even the pre-login page mounts WebVitalsReporter; mirror the
                # collection flag so it stays silent unless explicitly enabled.
                "web_vitals_enabled": _web_vitals_enabled(),
            },
        )

    src: dict | None = None
    if service_id:
        src = timer.call("get_source_for_service", lambda: _db.get_source_for_service(service_id))

    services = timer.call("get_enriched_services", lambda: get_enriched_services(service_id))

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

    # Resolve once and reuse across closures. Previously each closure called
    # get_source_for_service(valid_active_id) independently — 3 redundant
    # stat() + json.loads passes on the cold-load admin bootstrap path.
    active_src = _db.get_source_for_service(valid_active_id) if valid_active_id else None

    schema: list = []
    table_name: str | None = None

    # Use cached schema from config to avoid acquiring a DB lock.
    # table_name mirrors /api/schema's ``_safe_table_name(source.name)``
    # lookup so the frontend can seed its ['admin', 'schema', sid] cache
    # with the same {schema, table_name} payload the dedicated endpoint
    # returns. Both are folded together because they share the
    # valid_active_id gate and downstream consumers always need both.
    def _resolve_schema_and_table() -> None:
        nonlocal schema, table_name
        if not valid_active_id:
            return
        active_svc = next((s for s in services if s.get("service_id") == valid_active_id), None)
        if active_svc and active_svc.get("status"):
            schema = active_svc["status"].get("schema", []) or []
        from backend.core.duckdb import _safe_table_name

        if active_src:
            table_name = _safe_table_name(active_src["name"])

    timer.call("schema_lookup", _resolve_schema_and_table)

    # NOTE: the previous fallback opened a read-only DuckDB connection here
    # and ran get_schema() against the source on cold-cache loads. That call
    # acquired the per-service lock + did a parquet glob, costing 1-3s on
    # the very first /api/bootstrap after a backend restart and blocking
    # the whole admin UI from rendering. With the status-refresh cron
    # populating active_svc["status"]["schema"], the cache is the source
    # of truth — drop the fallback. If schema is empty here, the dashboard
    # renders without a hint banner; the user can refresh once the cron
    # has run (typically <60s after startup).

    pops = timer.call("get_pop_lat_lon_map", get_pop_lat_lon_map)
    pop_geo = timer.call("get_pop_location_map", get_pop_location_map)

    # Per the perf audit (F6): bootstrap's custom_fields_catalog was a
    # ~10-15 KB duplicate of what every chart page already fetches
    # separately from /api/log-fields/catalog, so the field was dropped
    # from the response (v2.1.0). The dashboard card hook only needs
    # custom_dashboard_cards and active_log_field_ids — both derived
    # from the catalog here.
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

    timer.call("custom_fields_catalog", _resolve_custom_fields)

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

    timer.call("log_fields_catalog", _resolve_log_fields_catalog)

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

    timer.call("sync_status", _resolve_sync_status)

    # Phase D-3: fold the lean share-status banner into bootstrap so
    # the header banner has its initial state on first render and
    # skips the first ~80 B / 1-RTT poll. Polling continues on its
    # 15-s cadence inside useShareStatusBanner for ongoing updates.
    # Admin-only — analyst sessions don't manage sharing.
    share_banner_payload: dict | None = None

    def _resolve_share_banner():
        nonlocal share_banner_payload
        if analyst_session is not None:
            return
        try:
            from backend.utils.tunnel import get_tunnel_manager

            mgr = get_tunnel_manager()
            share_banner_payload = {
                "sharing_active": mgr.is_sharing_active(),
                "public_url": mgr.public_url(),
            }
        except Exception:
            # Banner is non-essential UX; never break /api/bootstrap
            # if the tunnel manager is in a transient state.
            pass

    timer.call("share_banner", _resolve_share_banner)

    # Header badge + log extents: analyst-safe payloads projected
    # from the cached sync-status snapshot. Both available to BOTH
    # admin AND analyst.
    #   - header_badge: {latest_log_at, local_rows} — what
    #     SyncStatusBadge renders in the global header (closes the
    #     missing-header-for-analyst gap).
    #   - log_extents: {earliest_log_at, latest_log_at, configured} —
    #     what the FilterBar's auto-range snap-to-extents UX needs.
    #     Same shape /api/log-extents returns.
    header_badge_payload: dict | None = None
    log_extents_payload: dict | None = None

    def _resolve_header_badge_and_extents():
        nonlocal header_badge_payload, log_extents_payload
        if not valid_active_id:
            return
        # svcconfig.get_status is keyed on the service NAME, not the
        # service_id. They're often identical, but resolving via the
        # source dict matches the dedicated /api/sync-status handler
        # exactly so analyst/admin both look in the same place.
        if not active_src:
            return
        import re

        from backend import config as svcconfig
        from backend.core import metadata as metadata_db

        cached_status = svcconfig.get_status(active_src["name"]) or {}

        # Real-time SQLite metadata lookup overlay for bootstrap header/extents
        latest_file_at = None
        total_rows = 0
        try:
            summary = metadata_db.get_ingested_files_status_summary(active_src["name"])
            latest_file_name = summary.get("latest_file_name")
            total_rows = summary.get("total_rows") or 0
            if latest_file_name:
                fname = latest_file_name.split("/")[-1]
                m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
                if m:
                    latest_file_at = f"{m.group(1)}T{m.group(2).replace('-', ':').replace('.', ':')}Z"
        except Exception:
            pass

        latest = (
            latest_file_at
            or cached_status.get("latest_log_at")
            or cached_status.get("latest_available_file_at")
            or cached_status.get("latest_ingested_file_at")
        )
        earliest = cached_status.get("earliest_log_at")
        local_rows = cached_status.get("local_rows") or total_rows
        if latest is not None or local_rows is not None:
            header_badge_payload = {
                "latest_log_at": latest,
                "local_rows": local_rows,
            }
        # log_extents: emit even when both are None (with configured=True)
        # so the frontend can distinguish "no extents yet, keep polling"
        # from "service not configured" — matches the dedicated endpoint.
        log_extents_payload = {
            "configured": True,
            "earliest_log_at": earliest,
            "latest_log_at": latest,
        }

    timer.call("header_badge_and_extents", _resolve_header_badge_and_extents)

    # Admin DiagnosticsPanel dims its debug toggles when DEBUG_RESPONSES
    # is off on the backend. Folding the flag in here skips the
    # dedicated /api/debug/state round-trip on every admin page load.
    debug_state_payload: dict | None = None

    def _resolve_debug_state():
        nonlocal debug_state_payload
        if analyst_session is not None:
            return
        from backend.models.common import _debug_responses_enabled

        debug_state_payload = {"debug_responses_enabled": _debug_responses_enabled()}

    timer.call("debug_state", _resolve_debug_state)

    # Seed for the admin OperationsOverview three cards. Each sub-key
    # mirrors the payload shape the card's useQuery already expects, so
    # the frontend just calls ``queryClient.setQueryData(['admin',
    # 'overview', ...], seed)`` on bootstrap resolve and the cards paint
    # on first render. ADMIN ONLY — analysts can't reach /admin so the
    # seed isn't needed (and the underlying endpoints are 403 for them).
    ops_overview_payload: dict | None = None

    def _resolve_ops_overview() -> None:
        nonlocal ops_overview_payload
        if analyst_session is not None:
            return
        from backend.core import metadata as _meta_mod
        from backend.core.query_registry import query_registry as _query_registry

        out: dict = {}
        try:
            out["queries_summary"] = _query_registry.summary()
        except Exception:
            pass
        if valid_active_id:
            try:
                import time as _t

                since_utc = _t.time() - 24 * 3600
                out["slow_queries_count"] = {
                    "count": _meta_mod.count_slow_queries(valid_active_id, since_utc=since_utc, threshold_ms=1000.0),
                    "since_hours": 24,
                    "threshold_ms": 1000.0,
                }
            except Exception:
                pass
            # ``log_accounting`` is intentionally NOT seeded here. Its
            # ``compute_log_accounting`` helper chains up to 3 Fastly
            # Stats API calls on cache miss (~800 ms), which would
            # dominate every cold bootstrap. The IngestHealthCard's
            # existing 30 s ``refetchInterval`` + #13's TTL cache mean
            # the card paints with real data inside one tick of mount
            # without paying the cost on the bootstrap hot path.
        if out:
            ops_overview_payload = out

    timer.call("ops_overview", _resolve_ops_overview)

    # P1#5 (perf audit): cron_schedule is NOT folded into bootstrap.
    # build_cron_schedule_payload cost ~2.6s p50 / 3.2s p95 and sat on the
    # admin SSR first-paint critical path. It's a lazy-load instead: the
    # /logs cron tab refetches GET /api/cron-schedule on mount (see
    # frontend/app/logs/_state.ts — enabled on activeTab==='cron'), ONE
    # round-trip on the only page that needs it, with no feature loss.
    cron_runs_first_page_payload: dict | None = None

    def _resolve_cron_runs_first_page() -> None:
        nonlocal cron_runs_first_page_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.core.metadata.cron_log import get_cron_runs

            total, entries = get_cron_runs(valid_active_id, page=1, per_page=10, with_total=False)
            cron_runs_first_page_payload = {
                "total": total,
                "page": 1,
                "per_page": 10,
                "entries": entries,
            }
        except Exception:
            pass

    timer.call("cron_runs_first_page", _resolve_cron_runs_first_page)

    last_sync_payload: dict | None = None

    def _resolve_last_sync() -> None:
        nonlocal last_sync_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            sync_row = latest_cron_per_task(valid_active_id).get("sync")
            if sync_row:
                last_sync_payload = {
                    "started_at": sync_row.get("started_at"),
                    "status": sync_row.get("status"),
                    "duration_s": sync_row.get("duration_s"),
                }
        except Exception:
            # Header badge is a UX nicety — never break bootstrap.
            pass

    timer.call("last_sync", _resolve_last_sync)

    scoring_labels_payload: dict | None = None

    def _resolve_scoring_labels() -> None:
        nonlocal scoring_labels_payload
        if analyst_session is not None:
            return
        if not valid_active_id:
            return
        try:
            from backend.scoring import labels as _labels

            scoring_labels_payload = {
                "labels": _labels.list_labels(valid_active_id, limit=500),
                "counts": _labels.counts_by_label(valid_active_id),
            }
        except Exception:
            # Labels UI is a nicety — never break bootstrap.
            pass

    timer.call("scoring_labels", _resolve_scoring_labels)

    # P1#5 (perf audit): share_status is NOT folded into bootstrap.
    # build_share_status cost ~2.1s and sat on the admin SSR first-paint
    # critical path. It's a lazy-load instead: the /admin/share page
    # refetches GET /api/admin/share/status on mount (see
    # frontend/app/admin/share/page.tsx — unconditional mount-time useQuery
    # on SHARE_STATUS_QUERY_KEY), ONE round-trip on the only page that
    # needs it, with no feature loss. The small global share_banner
    # ({sharing_active, public_url}) is KEPT above for the header.
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

    views = timer.call("views", _resolve_views)

    # Force read_only for analyst sessions regardless of underlying source.
    if analyst_session is not None:
        access_level = "read_only"
    elif src and valid_active_id == service_id:
        access_level = src.get("access_level", "read_write")
    else:
        access_level = "read_write"

    # Admin shared-secret pickup: when ADMIN_SHARED_SECRET is configured the
    # middleware refuses admin endpoints whose X-Admin-Token doesn't match.
    # Expose the secret in the bootstrap response so the SPA can inject it
    # on every subsequent admin request. Gated to the admin branch
    # (analyst_session is None AND request is loopback) so a Fastly-fronted
    # analyst can't read the token even if they reach /api/bootstrap. With
    # the env unset (default) the field is None and the frontend interceptor
    # no-ops.
    admin_token: str | None = None
    # 003: Only provide token to actual local connections, not unauthenticated remote ones.
    if analyst_session is None and not getattr(request.state, "is_remote", False):
        from backend.utils.remote_access import _admin_shared_secret

        admin_token = _admin_shared_secret() or None

    from backend.core.web_vitals_store import collection_enabled as _web_vitals_collection_enabled

    return BootstrapResponse.with_telemetry(
        active_service_id=valid_active_id,
        services=services,
        schema=schema,
        table_name=table_name,
        countries=COUNTRY_MAP,
        pops=pops,
        pop_geo=pop_geo,
        settings={
            "access_level": access_level,
            "storage_mode": STORAGE_MODE,
            "is_remote_analyst": analyst_session is not None,
            "analyst_email": analyst_session.email if analyst_session else None,
            "analyst_name": analyst_session.name if analyst_session else None,
            # Per-invite PII masking flag, surfaced so the frontend can hide
            # IP drill-down affordances for masking analysts. The server-side
            # filter lock (RemoteAccessMiddleware) is the real guarantee; this
            # is the UX signal. False for admins / non-masking analysts.
            "mask_ips": mask_ips_for(analyst_session),
            "admin_token": admin_token,
            # Mirror the WEB_VITALS_COLLECT env flag so WebVitalsReporter
            # only POSTs when collection is enabled (default off).
            "web_vitals_enabled": _web_vitals_collection_enabled(),
        },
        custom_dashboard_cards=custom_dashboard_cards,
        active_log_field_ids=active_log_field_ids,
        views=views,
        log_fields_catalog=log_fields_catalog_payload,
        sync_status=sync_status_payload,
        share_banner=share_banner_payload,
        header_badge=header_badge_payload,
        log_extents=log_extents_payload,
        debug_state=debug_state_payload,
        ops_overview=ops_overview_payload,
        cron_runs_first_page=cron_runs_first_page_payload,
        last_sync=last_sync_payload,
        scoring_labels=scoring_labels_payload,
        section_timings=section_timings,
    )


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
    require_service_in_scope(request, source.get("name"))

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
    # Only gate when a specific service was requested; a None service_id
    # falls through to the default/admin catalog as before.
    if service_id is not None:
        require_service_in_scope(request, service_id)

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
    require_service_in_scope(request, source.get("name"))

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
