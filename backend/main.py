"""FastAPI application entry point.

Run from the project root:

    uvicorn backend.main:app --port 8000 --reload
"""

from __future__ import annotations

import logging
import os

# Mitigate macOS malloc/free C++ heap corruption (Abort trap 6)
# DuckDB and PyArrow both use jemalloc internally which causes fatal crashes
# when passing memory back and forth. Forcing PyArrow to use system malloc segregates them.
os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"

import resource

try:
    # Increase file descriptor limit for DuckDB and Iceberg operations
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # Target 65k or the hard limit, whichever is lower.
    # 256/1024 is often too low for high-parallelism DuckDB/Iceberg scans.
    target = min(hard, 65536)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
except Exception:
    pass

import sys
from contextlib import asynccontextmanager

# ── Logging ───────────────────────────────────────────────────────────────────
# Configure before uvicorn sets up its own handlers so cron/scheduler output
# is visible in the console alongside uvicorn's request logs.
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s - %(message)s",
    stream=sys.stdout,
    force=True,
)
logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

logger = logging.getLogger("backend.main")

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette_compress import CompressMiddleware

# ── Path setup ────────────────────────────────────────────────────────────────
# Ensure the project root is on sys.path so the backend package is importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import threading
from concurrent.futures import ThreadPoolExecutor

# ── Lifespan ──────────────────────────────────────────────────────────────────
from datetime import UTC, datetime

from backend import config as svcconfig
from backend.core import duckdb as _db


def _initialize_service(cfg: dict):
    """Initialise a single service's status cache in the background."""
    sid = cfg.get("service_id")
    if not sid:
        return
    # ThreadPoolExecutor worker threads don't inherit the parent's
    # ContextVar; process_context_scope sets both the ContextVar and pushes
    # onto the global active-contexts stack so the fsspec iothread fallback
    # (get_process_context_with_fallback) attributes this worker's iceberg
    # I/O correctly. Using set_process_context() here would race with
    # concurrent scheduler ticks: their process_context_scope exit pops the
    # empty stack and nulls the mirror, untagging any I/O still in flight.
    from backend.utils.telemetry import process_context_scope

    with process_context_scope(f"startup:init_service:{sid}"):
        try:
            # Reap any cron rows still marked 'running' from a previous process —
            # the in-memory progress dict (backend.cron_progress) is wiped on
            # restart, so those rows are guaranteed orphans. Without this, the UI
            # polls /api/cron-runs?status=running, sees the orphan, and the
            # CronLiveLog hangs on "Loading logs..." until the SSE 30s timeout.
            try:
                from backend.core import metadata_db

                n = metadata_db.reap_running_cron_runs(sid)
                if n:
                    logging.info("[fastapi] Service %s: reaped %d orphaned cron run(s).", sid, n)
            except Exception as e:
                logging.warning("[fastapi] Could not reap orphaned cron runs for %s: %s", sid, e)

            src = _db.get_source_for_service(sid)
            if src:
                _db.refresh_config_status(sid)
                _ensure_persistent_view(sid, src)
                # Data migrations: queues any pending one-time setup work
                # (e.g. the initial rollups backfill) onto a daemon thread
                # per service. Returns immediately so startup isn't gated
                # on a potentially multi-minute backfill. See
                # backend/core/data_migrations.py for the framework.
                try:
                    from backend.core import data_migrations

                    data_migrations.run_pending(sid, src)
                except Exception as e:
                    logging.warning("[fastapi] Service %s: could not queue data migrations: %s", sid, e)
                logging.info("[fastapi] Service %s initialised.", sid)
        except Exception as e:
            logging.warning("[fastapi] Could not initialise service %s: %s", sid, e)


def _ensure_persistent_view(sid: str, src: dict) -> None:
    """Build the per-service Iceberg view at startup if it's missing from
    the .duckdb file.

    Why this exists: ``refresh_config_status`` opens a read-only connection
    with ``skip_view_update=True``, so it can't (and won't) create the
    view. The scheduler's data sync builds the view only at the END of a
    full sync cycle (after potentially 1015+ files / 3+ minutes). So
    after any restart where the persisted view was previously dropped
    (manual DROP VIEW, .duckdb file deletion) the dashboard, security
    top-bots, and query page all surface "Catalog Error: Table with name
    logs_xxx does not exist" until the first sync completes.

    Building the view here uses only the locally-cached Iceberg catalog
    (data/services/<sid>/iceberg_catalog.db) + local parquet/buffer
    files — no cloud round-trip. When the view already exists, this is
    a single information_schema lookup and a no-op."""
    try:
        from backend.core import iceberg as _ice
        from backend.core.duckdb import get_connection

        # Always refresh the view at startup so the first dashboard load
        # after a restart doesn't pay the 500-900ms CREATE OR REPLACE VIEW
        # cost mid-request. Subsequent dashboard loads reuse the warm view
        # until a sync writer rebuilds it again. The previous "skip if
        # already exists" behaviour kept stale views around when buffer
        # files had been appended since the last writer close.
        logging.info("[fastapi] Service %s: pre-warming Iceberg view at startup", sid)
        writer = get_connection(source=src, read_only=False)
        try:
            _ice.update_iceberg_view(writer, src)
        finally:
            writer.close()
        logging.info("[fastapi] Service %s: view pre-warmed", sid)
    except Exception as e:
        logging.warning("[fastapi] Service %s: view pre-warm failed: %s", sid, e)


def _ensure_pop_cache():
    """Fetch POP location data at startup if the cache file is missing and a key is available."""
    try:
        from backend.utils.pop_utils import CACHE_FILE, fetch_pop_locations

        if os.path.exists(CACHE_FILE):
            return
        for cfg in svcconfig.list_configs():
            api_key = cfg.get("fastly_api_key", "").strip()
            if api_key:
                ok = fetch_pop_locations(api_key)
                if ok:
                    logging.info("[fastapi] POP locations cached at startup.")
                else:
                    logging.warning("[fastapi] POP locations fetch failed at startup.")
                break
    except Exception as e:
        logging.warning("[fastapi] Could not prefetch POP locations: %s", e)


def _ensure_scoring_matrix():
    """Pull the trained scoring matrix from FOS at startup for any
    service that has scoring enabled.

    Without this, the /scoring/evaluation endpoint falls back to the
    bundled matrix.default.json (empty transitions → AUC ≈ 0.5) until
    an operator manually drops compute/scorer/matrix.json into the
    container. The fetch is best-effort: missing FOS object, no scoring
    enabled, S3 timeout — all silently no-op so a slow FOS doesn't
    block startup.
    """
    try:
        from backend.provision.session_scoring_orchestrator import _MATRIX_PATH
        from backend.state_sync import fetch_matrix_from_fos

        for cfg in svcconfig.list_configs():
            if not (cfg.get("scoring") or {}).get("enabled"):
                continue
            sid = cfg.get("service_id") or cfg.get("name")
            try:
                matrix = fetch_matrix_from_fos(sid)
                if not matrix:
                    continue
                _MATRIX_PATH.parent.mkdir(parents=True, exist_ok=True)
                with _MATRIX_PATH.open("w") as f:
                    import json as _json

                    _json.dump(matrix, f)
                logging.info(
                    "[fastapi] Pulled scoring matrix from FOS for %s (version=%s)",
                    sid,
                    matrix.get("version", "?"),
                )
                # First-write-wins: with multiple scoring-enabled services,
                # the matrix file is global. They SHOULD all be the same
                # matrix (one trainer, one deploy), but if they differ
                # we use whichever loaded first and log a warning above.
                break
            except Exception as e:
                logging.warning("[fastapi] Could not pull scoring matrix for %s: %s", sid, e)
    except Exception as e:
        logging.warning("[fastapi] _ensure_scoring_matrix failed: %s", e)


def _background_startup():
    """Run initialisation tasks that should not block the web server startup."""
    # Tag everything done here so the s3fs/boto3 hooks attribute their
    # telemetry rows to "startup" instead of falling back to the thread name.
    # MUST be process_context_scope (not set_process_context): the scheduler
    # starts below and its first cron's process_context_scope exit would pop
    # the active-contexts stack and null the mirror, untagging any in-flight
    # iceberg I/O from the init_service workers. The scope keeps "startup" on
    # the stack as a base so the mirror falls back to "startup" instead of
    # None when nested scopes (cron, init_service) exit.
    from backend.utils.telemetry import process_context_scope

    with process_context_scope("startup"):
        try:
            _db.reload_default_source()
            logging.info("[fastapi] db module initialised.")
        except Exception as e:
            logging.warning("[fastapi] reload_default_source failed: %s", e)

        _ensure_pop_cache()
        _ensure_scoring_matrix()

        try:
            from backend.scheduler import get_scheduler

            scheduler = get_scheduler()
            scheduler.start()
            logging.info("[fastapi] Scheduler started.")

            configs = svcconfig.list_configs()
            logging.info("[fastapi] Initialising %d services...", len(configs))

            with ThreadPoolExecutor(max_workers=4) as executor:
                executor.map(_initialize_service, configs)

        except Exception as e:
            logging.error("[fastapi] Background startup error: %s", e, exc_info=True)


def _enforce_data_dir_mounted() -> None:
    """Refuse to start if STRICT_DATA_DIR_CHECK=1 and /app/data is not an
    actual mount point. Belt-and-suspenders against the broken-fstab
    failure mode where the bind-mount source dir exists on the boot disk
    but is NOT the intended persistent volume — containers would silently
    ingest into an ephemeral stub that vanishes on the next reboot.

    Set STRICT_DATA_DIR_CHECK=1 in production compose; local dev (where
    /app/data is a bind from the repo, not a mount point) leaves it unset.
    """
    if os.environ.get("STRICT_DATA_DIR_CHECK") != "1":
        return
    data_dir = "/app/data"
    if not os.path.ismount(data_dir):
        msg = (
            f"FATAL: {data_dir} is not a mount point. The persistent data volume "
            "appears to be missing or misconfigured. Refusing to start so ingested "
            "data isn't written to an ephemeral location. Fix the host fstab/mount, "
            "verify with `mountpoint /mnt/app-data`, and restart."
        )
        logging.critical(msg)
        raise RuntimeError(msg)


def _enforce_proxy_headers_configured() -> None:
    """Security regression guard for.

    The remote-access middleware reads ``request.client.host`` and trusts it as
    the client's real IP. That only works if uvicorn is launched with
    ``--proxy-headers --forwarded-allow-ips=<comma-separated-trusted-IPs>`` —
    without those flags the framework returns the loopback peer address for
    every Caddy-proxied request and every IP-based gate (rate-limiting, admin
    detection, whitelist) becomes ineffective.

    Production sets ``TRUSTED_PROXY_IPS=127.0.0.1`` in docker-compose.prod.yml
    alongside the uvicorn flags. If that env var is missing or empty at boot,
    refuse to start (or, for local dev where the var is unset, emit a loud
    WARNING) so a future config refactor cannot silently re-introduce the
    pre-patch vulnerability.

    Set ``REQUIRE_PROXY_HEADERS=1`` in production to make this a hard FATAL.
    Local dev / tests leave both env vars unset and the function is a no-op.

    Defense in depth: even when our own ``TRUSTED_PROXY_IPS`` env is set, we
    also probe uvicorn's own ``UVICORN_FORWARDED_ALLOW_IPS`` env var (the
    env-equivalent of the ``--forwarded-allow-ips`` CLI flag). If a future
    refactor passes the CLI flag without exporting our companion env var,
    uvicorn's variable lets us detect it.
    """
    trusted = (os.environ.get("TRUSTED_PROXY_IPS") or "").strip()
    uvicorn_trusted = (os.environ.get("UVICORN_FORWARDED_ALLOW_IPS") or "").strip()
    require_strict = os.environ.get("REQUIRE_PROXY_HEADERS") == "1" or os.environ.get("STRICT_DATA_DIR_CHECK") == "1"
    effective = trusted or uvicorn_trusted
    if effective:
        logging.info(
            "[fastapi] proxy-headers trust set: TRUSTED_PROXY_IPS=%s UVICORN_FORWARDED_ALLOW_IPS=%s",
            trusted or "(unset)",
            uvicorn_trusted or "(unset)",
        )
        return
    msg = (
        "TRUSTED_PROXY_IPS is unset. uvicorn must be launched with "
        "`--proxy-headers --forwarded-allow-ips=127.0.0.1` AND have "
        "TRUSTED_PROXY_IPS=127.0.0.1 in its environment so the remote-access "
        "middleware can read request.client.host as the real client IP. "
        "Without this, leftmost-XFF spoofing becomes exploitable "
        "and the admin Host-spoof bypass returns. See docker-compose.prod.yml."
    )
    if require_strict:
        logging.critical("FATAL: %s", msg)
        raise RuntimeError(msg)
    logging.warning("[fastapi] %s", msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    from backend.scheduler import get_scheduler

    # Data-volume sanity check FIRST — before any dependency / scheduler /
    # ingestion logic that would otherwise blindly write to the wrong path.
    _enforce_data_dir_mounted()

    # Proxy-headers regression guard (security). Production
    # must have TRUSTED_PROXY_IPS set in env (mirrors the uvicorn
    # --forwarded-allow-ips flag). Without it, IP-based gates become
    # ineffective and the Host-spoof admin bypass returns.
    _enforce_proxy_headers_configured()

    # Verify dependencies
    try:
        import pyarrow  # noqa: F401
        import pyiceberg  # noqa: F401
    except ImportError as e:
        logging.critical(
            "\n[CRITICAL] Missing required dependency: %s\n[CRITICAL] Please run: pip install -e .\n", e.name
        )

    # Stamp process start time immediately (no I/O)
    _db._PROCESS_START_UTC = datetime.now(UTC)

    # Eagerly start the local telemetry proxy. Idempotent —
    # boto3/DuckDB/PyIceberg would lazy-start it on first call otherwise; eager
    # start moves the ~hundreds-of-ms port bind out of the first request's
    # latency.
    try:
        from backend.utils import telemetry_proxy

        telemetry_proxy.start_proxy_server()
    except Exception as e:
        logging.warning("[fastapi] telemetry proxy failed to start at lifespan boot: %s", e)

    # All initialisation (scheduler, DuckDB views, status cache) runs in a
    # background daemon thread so slow S3/Iceberg calls never block startup.
    # Skip during testing to avoid background logging after sys.stdout is closed.
    if "pytest" not in sys.modules:
        threading.Thread(target=_background_startup, daemon=True).start()

    # Rehydrate any persisted analyst sessions so a uvicorn restart does not
    # silently log every analyst out mid-engagement.
    try:
        from backend.utils.tunnel import get_tunnel_manager

        kept = get_tunnel_manager().rehydrate_sessions()
        if kept:
            logging.info("[fastapi] Rehydrated %d analyst session(s) from share_db.", kept)
    except Exception:
        logging.exception("[fastapi] failed to rehydrate analyst sessions")

    yield

    # Shutdown
    try:
        get_scheduler().shutdown()
        logging.info("[fastapi] Scheduler stopped.")
    except Exception as e:
        logging.warning("[fastapi] Failed to stop scheduler: %s", e)

    _db.close_all_connections()
    logging.info("[fastapi] Shutdown complete.")


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fastly Log Analytics API",
    version="1.2.0",
    description=(
        "FastAPI backend for the Fastly Log Analytics tool. "
        "Serves the Next.js frontend and exposes an OpenAPI spec at /openapi.json."
    ),
    lifespan=lifespan,
)

# ── Middleware stack ──────────────────────────────────────────────────────────
#
# Declared order (outermost → innermost). Asserted at boot below via
# assert_middleware_order(); a divergence crashes startup rather than
# shipping a silently-broken request pipeline. See ADR-04 for the rationale
# behind each layer's position.
MIDDLEWARE_ORDER = (
    "CompressMiddleware",              # outermost — sees final response body
    "BaseHTTPMiddleware",              # @app.middleware('http') telemetry decorator
    "TelemetryResponseBodyMiddleware", # JSON-body backstop for debug panel
    "RemoteAccessMiddleware",          # analyst firewall — rejects before CORS
    "CORSMiddleware",                  # innermost — closest to FastAPI routing
)


def assert_middleware_order(_app: FastAPI) -> None:
    """Boot-time assertion that middleware order matches ADR-04.

    Crashes on mismatch — a reorder that compiles is no longer enough to
    ship. ``user_middleware`` is in outermost-first order (Starlette
    reverses the add-order internally), so the comparison is direct.
    """
    actual = tuple(m.cls.__name__ for m in _app.user_middleware)
    if actual != MIDDLEWARE_ORDER:
        raise RuntimeError(
            f"Middleware order violation (ADR-04). "
            f"expected={MIDDLEWARE_ORDER} actual={actual}"
        )


# INVARIANT: CORSMiddleware is innermost (see ADR-04).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# INVARIANT: RemoteAccessMiddleware above CORS, below telemetry (see ADR-04).
# Blocks analyst requests before they reach the telemetry layer so blocked
# analyst hits don't pollute usage_log with admin-scoped rows.
from backend.utils.remote_access import RemoteAccessMiddleware  # noqa: E402

app.add_middleware(RemoteAccessMiddleware)


# INVARIANT: TelemetryResponseBodyMiddleware inside Compress, outside
# RemoteAccess (see ADR-04). Reads uncompressed JSON bodies to inject
# debug fields; gated on DEBUG_RESPONSES.
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware  # noqa: E402

app.add_middleware(TelemetryResponseBodyMiddleware)


@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    """Initialise call tracking, set process context, and flush FOS/CDN ops after the request.

    Uses process_context_scope (not set_process_context) so the global
    _LATEST_PROCESS_CONTEXT mirror reverts when the request exits. Otherwise
    out-of-thread readers — fsspec iothread, pyiceberg pool — keep reading
    the last-completed request's context and attribute cron-driven CDN
    reads to whichever API request happened most recently (observed in
    the 2026-05-24 audit: dashboard's cdn.miss rows landed tagged as
    `api:GET /api/debug/recent-sqlite` because the debug poller ran last).
    """
    from backend import config as svcconfig
    from backend.utils.telemetry import process_context_scope, start_call_tracking

    start_call_tracking()
    ctx_name = f"api:{request.method} {request.url.path}"
    with process_context_scope(ctx_name):
        try:
            response = await call_next(request)
        finally:
            try:
                sid = (
                    request.query_params.get("service")
                    or request.headers.get("x-fastly-service-id")
                    or request.headers.get("x-service-id")
                )
                if sid and not svcconfig.load_config(sid):
                    # Treat unknown IDs as cdn_service_id and resolve to the logging service.
                    sid = svcconfig.get_cdn_service_id_map().get(sid, sid)
                if not sid:
                    sid = svcconfig.get_active_service_id()
                if sid:
                    from backend.utils.usage_logger import flush_usage_log

                    flush_usage_log(sid)
            except Exception:
                pass
    return response


# INVARIANT: CompressMiddleware is outermost (see ADR-04). Must wrap the
# final response body so Content-Encoding survives all the way to the
# client; an inner placement gets stripped by BaseHTTPMiddleware's
# buffer-and-reemit (audited 2026-06-09: 11490 B raw uncompressed when
# Compress sat inside the telemetry decorator).
app.add_middleware(CompressMiddleware, minimum_size=1024)

# Boot-time middleware-order assertion. Crashes on violation rather than
# shipping a silently-broken stack. See ADR-04 + assert_middleware_order().
assert_middleware_order(app)


# ── Routers ───────────────────────────────────────────────────────────────────

from backend.routers import alerts, dashboard, insights, network, origin, performance, query, security, sessions, views

app.include_router(dashboard.router)
app.include_router(insights.router)
app.include_router(sessions.router)
app.include_router(query.router)
app.include_router(network.router)
app.include_router(performance.router)
app.include_router(security.router)
app.include_router(views.router)
app.include_router(alerts.router)
app.include_router(origin.router)

from backend.routers import (
    admin,
    bootstrap,
    debug,
    provision,
    services,
    session_scoring,
    share_admin,
    share_auth,
    usage,
)

app.include_router(bootstrap.router)
app.include_router(services.router)
app.include_router(usage.router)
app.include_router(admin.router)
app.include_router(provision.router)
app.include_router(session_scoring.router)
app.include_router(debug.router)
app.include_router(share_auth.router)
app.include_router(share_admin.router)


# ── Health check ──────────────────────────────────────────────────────────────


@app.get("/api/health", tags=["meta"])
def health_check(
    deep: bool = False,
    stale_minutes: int = 30,
):
    """Liveness/readiness probe.

    Default (``?deep=0``) is a cheap liveness response — 200 as soon as the
    HTTP server is answering. Pass ``?deep=1`` to also verify ingest
    freshness per configured service. A service is reported ``degraded``
    when its newest ``ingested_files.ingested_at`` is older than
    ``stale_minutes`` (default 30) OR the last terminal ``sync`` cron run
    ended in ``error``. The endpoint returns HTTP 503 when ANY service is
    degraded; otherwise 200.

    Cost: hits SQLite only — no FOS, no CDN, no Fastly API. Safe to wire
    into a load-balancer health check.
    """
    from datetime import UTC, datetime, timedelta

    from fastapi.responses import JSONResponse

    from backend import config
    from backend.core import metadata_db

    payload: dict = {"status": "ok", "version": "1.0.0"}
    if not deep:
        return payload

    cutoff = (datetime.now(UTC) - timedelta(minutes=stale_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    services_report: list[dict] = []
    overall_ok = True

    for sid in config.list_service_ids():
        svc_state: dict = {"service_id": sid, "status": "ok"}
        try:
            con = metadata_db.get_con(sid)
            row = con.execute(
                "SELECT max(ingested_at) AS last_ingest FROM ingested_files WHERE source_name = ?",
                (sid,),
            ).fetchone()
            last_ingest = row["last_ingest"] if row else None
            svc_state["last_ingest"] = last_ingest

            cron_row = con.execute(
                "SELECT status, started_at, error_message FROM cron_runs "
                "WHERE task = 'sync' AND status != 'running' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if cron_row:
                svc_state["last_sync_status"] = cron_row["status"]
                svc_state["last_sync_at"] = cron_row["started_at"]
                if cron_row["status"] == "error":
                    svc_state["status"] = "degraded"
                    svc_state["reason"] = f"last sync errored: {cron_row['error_message'] or 'unknown'}"

            # A brand-new service legitimately has no ingest yet; don't flag
            # it as degraded. Only flag services that have ingested at least
            # once AND fell behind the cutoff.
            if last_ingest and last_ingest < cutoff and svc_state["status"] == "ok":
                svc_state["status"] = "degraded"
                svc_state["reason"] = f"no ingest since {last_ingest} (cutoff {cutoff})"
        except Exception as e:
            svc_state["status"] = "degraded"
            svc_state["reason"] = f"metadata_db query failed: {e}"

        if svc_state["status"] != "ok":
            overall_ok = False
        services_report.append(svc_state)

    payload["services"] = services_report
    payload["status"] = "ok" if overall_ok else "degraded"

    if not overall_ok:
        return JSONResponse(status_code=503, content=payload)
    return payload


# ── Static files (Next.js export) ────────────────────────────────────────────
# When the Next.js app is built with STATIC_EXPORT=1, it outputs to frontend/out/.
# Mount it here so FastAPI serves the frontend directly in production.
# In development the Next.js dev server runs separately; this mount is a no-op.
_STATIC_DIR = os.path.join(_ROOT, "frontend", "out")
if os.path.isdir(_STATIC_DIR):
    app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="static")
