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
# Activate structlog before uvicorn sets up its own handlers; this owns the root
# logger's handler set (stdlib calls are bridged through ProcessorFormatter), so
# every app/cron/scheduler log line renders through the same chain with OTel
# trace_id attached when a span is active. Format is console by default (what dev
# AND prod emit today); STRUCTLOG_FORMAT=json switches to JSON lines for a log
# aggregator when one is wired (none today — see ADR-08 §3).
#
# uvicorn's OWN loggers (uvicorn / uvicorn.error / uvicorn.access) are a special
# case: uvicorn re-installs private handlers on them at server start, AFTER this
# import. bridge_uvicorn_loggers() (called from lifespan startup, which runs
# after uvicorn's logging setup) re-points them at root so access lines render
# through the same chain — see its docstring for the boot-line caveat.
from backend.utils.structlog_config import bridge_uvicorn_loggers, configure_structlog  # noqa: E402

configure_structlog()
logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Install s3fs/botocore monkeypatches before anything else can touch s3fs.
# Importing the fs submodule has the side-effect of patching S3FileSystem;
# pyiceberg lazily instantiates S3FileSystem on first table access, so as
# long as this lands before the first iceberg call we're safe — but the
# earlier the better, since any future eager import would otherwise win.
from backend.core.iceberg import fs as _iceberg_fs_patches  # noqa: E402, F401

logger = logging.getLogger("backend.main")

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette_compress import CompressMiddleware, remove_compress_type

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
    # I/O correctly. A bare ContextVar setter (no scope) would race with
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
                from backend.core import metadata as metadata_db

                n = metadata_db.reap_running_cron_runs(sid)
                if n:
                    logging.info("[fastapi] Service %s: reaped %d orphaned cron run(s).", sid, n)
            except Exception as e:
                # Transient on the first boot after an unclean shutdown — SQLite
                # WAL recovery rolls the file forward on the next connection, so
                # subsequent calls succeed. Log INFO if it's the recoverable
                # malformed-image case, WARN otherwise.
                msg = str(e)
                if "malformed" in msg or "is locked" in msg:
                    logging.info(
                        "[fastapi] Service %s: orphan-cron reap deferred (%s); WAL recovery will resolve on the next connection.",
                        sid,
                        msg,
                    )
                else:
                    logging.warning("[fastapi] Could not reap orphaned cron runs for %s: %s", sid, e)

            src = _db.get_source_for_service(sid)
            if src:
                _db.refresh_config_status(sid)
                _ensure_persistent_view(sid, src)
                # Pre-warm compute_sync_status_cached so the very first
                # /api/sync-status?skip_fos=true after restart doesn't pay
                # the ~700ms _get_dir_size walk (19k files on a populated
                # rollups cache). The FilterBar, header badge, and every
                # CSR page fires this endpoint within the first second of
                # nav — landing here cold added 1.7s to /dashboard cold
                # load in the 2026-06-11 audit.
                try:
                    from backend.routers.admin import compute_sync_status_cached

                    compute_sync_status_cached(sid)
                except Exception as e:
                    logging.warning("[fastapi] Service %s: sync-status pre-warm failed: %s", sid, e)

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
    """Pull the trained scoring matrix from FOS at startup for every
    scoring-enabled service.

    Without this, the /scoring/evaluation endpoint falls back to the
    bundled matrix.default.json (empty transitions → AUC ≈ 0.5) until
    an operator manually drops a matrix into the container. The fetch
    is best-effort per service: missing FOS object, no scoring enabled,
    S3 timeout — all silently no-op so a slow FOS doesn't block startup.

    Each service's matrix lands at the tenant-scoped path
    ``matrix_{sid}.json`` (matches what ``_load_matrix`` checks first in
    [backend/routers/session_scoring.py](session_scoring.py)) so multiple
    scoring-enabled services don't trample each other. Pre-audit-finding-005
    the loop wrote everyone to the shared ``matrix.json`` and broke after
    the first success; service A's matrix would then serve service B until
    B's first retrain.
    """
    try:
        import json as _json

        from backend.provision.session_scoring_orchestrator import _tenant_matrix_path
        from backend.state_sync import fetch_matrix_from_fos

        for cfg in svcconfig.list_configs():
            if not (cfg.get("scoring") or {}).get("enabled"):
                continue
            sid = cfg.get("service_id") or cfg.get("name")
            if not sid:
                continue
            try:
                matrix = fetch_matrix_from_fos(sid)
                if not matrix:
                    continue
                tenant_path = _tenant_matrix_path(sid)
                tenant_path.parent.mkdir(parents=True, exist_ok=True)
                with tenant_path.open("w") as f:
                    _json.dump(matrix, f)
                logging.info(
                    "[fastapi] Pulled scoring matrix from FOS for %s (version=%s) → %s",
                    sid,
                    matrix.get("version", "?"),
                    tenant_path.name,
                )
            except Exception as e:
                logging.warning("[fastapi] Could not pull scoring matrix for %s: %s", sid, e)
    except Exception as e:
        logging.warning("[fastapi] _ensure_scoring_matrix failed: %s", e)


_startup_complete = False


def _background_startup():
    """Run initialisation tasks that should not block the web server startup."""
    global _startup_complete
    try:
        # Tag everything done here so the s3fs/boto3 hooks attribute their
        # telemetry rows to "startup" instead of falling back to the thread name.
        # MUST be process_context_scope (the context manager): the scheduler
        # starts below and its first cron's scope exit would pop the
        # active-contexts stack and null the mirror, untagging any in-flight
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

            # Pre-compile the bot-UA matcher off the request path. build_matcher
            # is mtime-cached, but a fresh process pays the full pattern compile
            # on the first /top-bots (or bot_name field-values) request otherwise
            # — ~300ms observed on prod after each deploy (2026-07-06). Best-
            # effort: on failure the first request compiles it as before.
            try:
                from backend.utils.bot_sources import build_matcher

                build_matcher()
                logging.info("[fastapi] Bot-UA matcher pre-compiled.")
            except Exception as e:
                logging.warning("[fastapi] bot matcher prewarm failed (first request pays the compile): %s", e)

            try:
                from backend.cron.scheduler import get_scheduler

                scheduler = get_scheduler()
                scheduler.start()
                logging.info("[fastapi] Scheduler started.")

                configs = svcconfig.list_configs()
                logging.info("[fastapi] Initialising %d services...", len(configs))

                executor = ThreadPoolExecutor(max_workers=4)
                futures = [executor.submit(_initialize_service, cfg) for cfg in configs]
                from concurrent.futures import wait as _futures_wait

                done, not_done = _futures_wait(futures, timeout=1.0)
                if not_done:
                    logging.info(
                        "[fastapi] Some services are taking longer to initialize; continuing startup in background."
                    )
                executor.shutdown(wait=False)

            except Exception as e:
                logging.error("[fastapi] Background startup error: %s", e, exc_info=True)
    finally:
        _startup_complete = True
        logging.info("[fastapi] Background startup complete. Server fully initialized.")


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


def _migrate_config_on_startup() -> None:
    """Auto-migrate legacy flat configs to nested structure on service startup.

    This runs BEFORE any code reads config files, ensuring all configs in memory
    are in the new nested format. Migration is idempotent: calling on already-nested
    configs returns them unchanged.
    """
    import json
    from pathlib import Path

    from backend.provision.declarative.config_migration import config_changed, migrate_config

    config_dir = Path.home() / ".fastly-log-analytics" / "configs"
    if not config_dir.exists():
        return

    for config_file in config_dir.glob("*.json"):
        try:
            with open(config_file) as f:
                original_cfg = json.load(f)

            migrated_cfg = migrate_config(original_cfg)

            if config_changed(original_cfg, migrated_cfg):
                with open(config_file, "w") as f:
                    json.dump(migrated_cfg, f, indent=2)
                logging.info("[fastapi] Migrated config file to v2.3.0 nested format: %s", config_file.name)
        except Exception as e:
            # Log but don't fail startup
            logging.warning("[fastapi] Failed to migrate config file %s: %s", config_file.name, e)


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
    import asyncio

    from backend.core.realtime.publisher import publisher as _rt_publisher
    from backend.cron.scheduler import get_scheduler
    from backend.cron_runs_publisher import publisher as _cron_runs_publisher
    from backend.sync_status_publisher import publisher as _sync_status_publisher

    # Bind all in-process SSE publishers to this loop so worker threads
    # can fan tick-updates out to connected subscribers via
    # loop.call_soon_threadsafe. Must happen on the loop that will serve
    # requests; cheap and synchronous.
    _running_loop = asyncio.get_running_loop()
    _sync_status_publisher.bind_loop(_running_loop)
    _cron_runs_publisher.bind_loop(_running_loop)
    _rt_publisher.bind_loop(_running_loop)

    # uvicorn has finished its own logging setup by now (it runs before
    # lifespan); re-point its loggers at the structlog root handler so access
    # lines render as JSON+trace_id in prod instead of plaintext.
    bridge_uvicorn_loggers()

    # Data-volume sanity check FIRST — before any dependency / scheduler /
    # ingestion logic that would otherwise blindly write to the wrong path.
    _enforce_data_dir_mounted()

    # Migrate legacy flat configs to nested structure BEFORE any code reads configs.
    # This is the critical enforcement point: all configs must be in nested format
    # from this point onward.
    _migrate_config_on_startup()

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
        _bounded_scheduler_shutdown(get_scheduler(), timeout_secs=60.0)
    except Exception as e:
        logging.warning("[fastapi] Failed to stop scheduler: %s", e)

    _db.close_all_connections()
    logging.info("[fastapi] Shutdown complete.")


def _bounded_scheduler_shutdown(scheduler, *, timeout_secs: float = 60.0) -> None:
    """Stop the scheduler from accepting new jobs and wait up to
    ``timeout_secs`` for any currently-running jobs to finish.

    ``BackgroundScheduler.shutdown(wait=True)`` (the default) blocks
    indefinitely waiting for running jobs — fine when nothing is running
    (returns in ms) but a 4-minute ``_run_service_cron`` mid-flight at
    SIGTERM would otherwise be killed at Docker's 10 s grace period
    every restart, producing partial buffers + the orphan ``in_flight``
    rows ``_recover_in_flight`` then has to reconcile.

    Bound the wait so a long-running cron gets a chance to land cleanly
    (sync.py's ``max_seconds=240`` is the hard ceiling on any one
    tick, but 60s covers the realistic finish-current-chunk case).
    If the deadline expires, log + return so the lifespan can keep
    going. The daemon worker threads die when the process exits;
    ``_recover_in_flight`` cleans up on next boot.

    Pairs with ``docker-compose.prod.yml``'s ``stop_grace_period: 75s``
    on the backend service (60 s drain budget + 15 s uvicorn /
    connection-close headroom). Without that bump, Docker SIGKILLs at
    10 s and the graceful wait is meaningless.

    Returns as soon as the scheduler reports done — if no crons are
    running, that's near-instant; the timeout is a ceiling, not a wait.
    """
    import time as _time

    done = threading.Event()
    err: list[BaseException] = []

    def _shutdown_worker() -> None:
        try:
            scheduler.shutdown(wait=True)
        except BaseException as e:  # noqa: BLE001 — surface unusual failures via logging
            err.append(e)
        finally:
            done.set()

    t0 = _time.monotonic()
    worker = threading.Thread(
        target=_shutdown_worker,
        name="scheduler_shutdown_wait",
        daemon=True,
    )
    worker.start()

    if done.wait(timeout=timeout_secs):
        elapsed = _time.monotonic() - t0
        if err:
            logging.warning(
                "[fastapi] Scheduler shutdown completed in %.2fs but raised: %s",
                elapsed,
                err[0],
            )
        else:
            logging.info("[fastapi] Scheduler stopped cleanly in %.2fs.", elapsed)
        return

    logging.warning(
        "[fastapi] Scheduler still draining after %.0fs grace; exiting anyway. "
        "Docker SIGKILL will reap stragglers; _recover_in_flight will reconcile on next boot.",
        timeout_secs,
    )


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fastly Log Analytics API",
    version="2.4.0",
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
    "CompressMiddleware",  # outermost — sees final response body
    "TelemetryResponseBodyMiddleware",  # JSON-body backstop for debug panel
    "RemoteAccessMiddleware",  # analyst firewall — rejects before CORS, sets analyst_session
    "BaseHTTPMiddleware",  # @app.middleware('http') telemetry decorator — INSIDE RemoteAccess so analyst_session is populated
    "CORSMiddleware",  # innermost — closest to FastAPI routing
)


def assert_middleware_order(_app: FastAPI) -> None:
    """Boot-time assertion that middleware order matches ADR-04.

    Crashes on mismatch — a reorder that compiles is no longer enough to
    ship. ``user_middleware`` is in outermost-first order (Starlette
    reverses the add-order internally), so the comparison is direct.
    """
    # getattr fallback because starlette types `m.cls` as `_MiddlewareFactory[P]`
    # which has no static `__name__`; at runtime middleware classes always do.
    actual = tuple(getattr(m.cls, "__name__", repr(m.cls)) for m in _app.user_middleware)
    if actual != MIDDLEWARE_ORDER:
        raise RuntimeError(f"Middleware order violation (ADR-04). expected={MIDDLEWARE_ORDER} actual={actual}")


# INVARIANT: CORSMiddleware is innermost (see ADR-04).
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get(
        "CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001,http://localhost:13002,http://127.0.0.1:13002",
    ).split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# INVARIANT: BaseHTTPMiddleware (the telemetry decorator) is INNER to
# RemoteAccessMiddleware (see ADR-04 + audit finding 003). Registered
# first here so it ends up innermost relative to RemoteAccess — that
# way ``request.state.analyst_session`` is already populated by the
# time the dispatch reads it for attribution. Pre-fix, telemetry sat
# OUTSIDE RemoteAccess and silently misattributed every analyst
# request to a generic admin-by-client-host.
@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    """Initialise call tracking, set process context, and flush FOS/CDN ops after the request.

    Uses process_context_scope (the context manager) so the global
    _LATEST_PROCESS_CONTEXT mirror reverts when the request exits. Otherwise
    out-of-thread readers — fsspec iothread, pyiceberg pool — keep reading
    the last-completed request's context and attribute cron-driven CDN
    reads to whichever API request happened most recently (observed in
    the 2026-05-24 audit: dashboard's cdn.miss rows landed tagged as
    `api:GET /api/debug/recent-sqlite` because the debug poller ran last).

    Also sets the Live Query Monitor's :data:`current_attribution`
    ContextVar here (NOT in :func:`build_request_context`) so the value
    propagates to sync dependencies and the route handler via FastAPI's
    ``run_in_threadpool`` — each threadpool call copies the parent
    context, and a ContextVar set INSIDE a dep doesn't flow forward to
    the route's separate threadpool call. Setting from the middleware
    avoids that gap.

    Registered INSIDE RemoteAccessMiddleware so ``request.state.analyst_session``
    is populated by the time we read it for attribution — see ADR-04 + audit
    finding 003. As a side benefit, blocked-by-RemoteAccess analyst requests
    no longer reach this layer, so they no longer pollute usage_log with
    admin-scoped rows.
    """
    from backend import config as svcconfig
    from backend.core.query_attribution import (
        Attribution as _Attribution,
    )
    from backend.core.query_attribution import (
        current_attribution as _current_attribution,
    )
    from backend.scoring import labels as _scoring_labels
    from backend.utils.active_requests import (
        decrement_active_requests as _dec_active_requests,
    )
    from backend.utils.active_requests import (
        increment_active_requests as _inc_active_requests,
    )
    from backend.utils.telemetry import process_context_scope, start_call_tracking

    # Active-request gate (perf #84): cron ticks check this counter at
    # their entry point and defer up to 30 s if non-zero, so a cold-cache
    # API request doesn't lose its DuckDB pool slot + Fastly bandwidth to
    # a sync tick that fired the same second.
    _inc_active_requests()
    start_call_tracking()
    # Open a per-request memoization scope for scoring_labels. The
    # /admin/session-scoring composite fires list_labels / counts_by_label
    # against the same service_id from 10+ sub-handlers; without this each
    # one independently opens the per-service SQLite handle and runs the
    # same SELECT. Cleared in the finally below so cron / test paths fall
    # through to the live DB read.
    _scoring_labels.init_request_cache()
    page_load_id = request.headers.get("X-Page-Load-ID") or request.headers.get("x-page-load-id")
    from backend.utils.telemetry import get_page_load_id, set_page_load_id

    _prev_plid = get_page_load_id()
    set_page_load_id(page_load_id)

    # Modify process context to include the page_load_id suffix so proxy reads match
    if page_load_id:
        ctx_name = f"api:{request.method} {request.url.path}#{page_load_id}"
    else:
        ctx_name = f"api:{request.method} {request.url.path}"

    # Best-effort attribution: analyst_session is set by RemoteAccessMiddleware
    # (which now sits OUTSIDE this middleware). Falls back to admin when
    # the request is local-only / non-analyst.
    analyst_session = getattr(request.state, "analyst_session", None)
    request_path = request.url.path
    # SRE-01: the attribution correlation id MUST be populated in every
    # environment. The OTel trace_id is only valid when an exporter is wired
    # (OTEL_EXPORTER != none); in the prod default it is invalid → blank,
    # which left slow_queries.attr_request_id uniformly empty and broke the
    # slow-request → query → who-ran-it pivot. Prefer the trace_id when it is
    # real (keeps trace correlation working once a collector exists), else
    # fall back to the app-level id minted by RemoteAccessMiddleware. Stamp
    # the resolved value back onto request.state so the outer access-log
    # lines (SRE-02) join on exactly the same id.
    request_id = getattr(request.state, "request_id", None)
    try:
        from opentelemetry import trace as _otel_trace

        _span = _otel_trace.get_current_span()
        _sctx = _span.get_span_context() if _span is not None else None
        if _sctx and _sctx.is_valid:
            request_id = format(_sctx.trace_id, "032x")
    except Exception:
        pass
    if not request_id:
        import secrets as _secrets

        request_id = _secrets.token_hex(8)
    try:
        request.state.request_id = request_id
    except Exception:
        pass
    if analyst_session is not None:
        attr = _Attribution.analyst(
            analyst_id=getattr(analyst_session, "session_id", None) or "unknown",
            analyst_name=getattr(analyst_session, "name", None) or None,
            request_path=request_path,
            request_id=request_id,
        )
    else:
        from backend.utils.remote_access import client_ip as _client_ip

        attr = _Attribution.admin(
            admin_id=_client_ip(request, default="unknown") or "admin",
            request_path=request_path,
            request_id=request_id,
        )
    _prev_attr = _current_attribution.get()
    _current_attribution.set(attr)

    with process_context_scope(ctx_name):
        try:
            response = await call_next(request)
        finally:
            # Active-request gate (perf #84): balance the increment above so the
            # cron-defer counter falls back to 0 when the request finishes.
            # Lives in the finally so an exception in call_next can't leak the
            # counter — a stuck +1 would cause should_defer_cron to return True
            # for up to max_defer_secs on every subsequent cron tick.
            try:
                _dec_active_requests()
            except Exception:
                pass
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
    # Restore the prior attribution value AFTER process_context_scope has
    # popped — so any final iothread drain still sees this request's
    # attribution, mirroring the rationale documented above for
    # _LATEST_PROCESS_CONTEXT.
    try:
        _current_attribution.set(_prev_attr)
    except Exception:
        pass
    # Close the scoring_labels per-request cache. Setting to None means
    # any post-response background work (or a subsequent request reusing
    # the same thread) sees a clean state and falls through to live reads
    # instead of stale cached rows.
    try:
        _scoring_labels.clear_request_cache()
    except Exception:
        pass
    try:
        set_page_load_id(_prev_plid)
    except Exception:
        pass
    return response


# INVARIANT: RemoteAccessMiddleware sits OUTSIDE the telemetry decorator
# (see ADR-04 + audit finding 003). Blocks analyst requests before they
# reach the telemetry layer so blocked analyst hits don't pollute
# usage_log with admin-scoped rows AND sets analyst_session early so
# the inner telemetry middleware can attribute correctly.
from backend.utils.remote_access import RemoteAccessMiddleware  # noqa: E402

app.add_middleware(RemoteAccessMiddleware)


# INVARIANT: TelemetryResponseBodyMiddleware inside Compress, outside
# RemoteAccess (see ADR-04). Reads uncompressed JSON bodies to inject
# debug fields; gated on DEBUG_RESPONSES.
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware  # noqa: E402

app.add_middleware(TelemetryResponseBodyMiddleware)


from fastapi.responses import JSONResponse  # noqa: E402

from backend.core.metadata.base import InvalidServiceIdError  # noqa: E402


@app.exception_handler(InvalidServiceIdError)
async def _invalid_service_id_handler(request: Request, exc: InvalidServiceIdError) -> JSONResponse:
    """Convert ``InvalidServiceIdError`` raised by ``metadata.base.db_path`` into
    a 422 instead of letting it bubble as an opaque 500 ``sqlite3.OperationalError:
    unable to open database file``. Triggered by routes whose ``service_id`` path
    parameter contains characters that would traverse the data directory or that
    APFS / strict Linux filesystems reject (e.g. unassigned-plane Unicode
    codepoints surfacing as ``OSError(Errno 92): Illegal byte sequence``).

    Body shape matches FastAPI's own ``HTTPValidationError`` schema so the
    response stays conformant to the OpenAPI spec.
    """
    return JSONResponse(
        status_code=422,
        content={
            "detail": [
                {
                    "loc": ["path", "service_id"],
                    "msg": str(exc),
                    "type": "value_error.invalid_service_id",
                }
            ],
        },
    )


# INVARIANT: CompressMiddleware is outermost (see ADR-04). Must wrap the
# final response body so Content-Encoding survives all the way to the
# client; an inner placement gets stripped by BaseHTTPMiddleware's
# buffer-and-reemit (audited 2026-06-09: 11490 B raw uncompressed when
# Compress sat inside the telemetry decorator).
# Disable compression for Server-Sent Events (SSE) to prevent response buffering
# and chunk boundaries corruption (which cause ERR_INVALID_CHUNKED_ENCODING on public endpoints).
remove_compress_type("text/event-stream")
app.add_middleware(CompressMiddleware, minimum_size=1024)

# Boot-time middleware-order assertion. Crashes on violation rather than
# shipping a silently-broken stack. See ADR-04 + assert_middleware_order().
assert_middleware_order(app)


# ── Routers ───────────────────────────────────────────────────────────────────

from backend.models.errors import DEFAULT_ERROR_RESPONSES  # noqa: E402
from backend.routers import (
    alerts,
    assets,
    cmcd,
    control_room,
    dashboard,
    insights,
    network,
    origin,
    performance,
    query,
    rum,
    security,
    sessions,
    value,
    views,
)

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
app.include_router(control_room.router)
app.include_router(value.router)
app.include_router(cmcd.router)
app.include_router(rum.router)
app.include_router(rum.asset_router)
app.include_router(assets.router)

from backend.routers import (
    admin,
    admin_queries,
    bootstrap,
    cmcd_admin,
    debug,
    provision,
    services,
    session_scoring,
    share_admin,
    share_auth,
    share_oauth,
    sharing_domain,
    usage,
    ux_events,
    web_vitals,
)

app.include_router(bootstrap.router)
app.include_router(services.router)
app.include_router(usage.router)
app.include_router(admin.router)
# Analyst-safe sync-status siblings carry the neutral "meta" tag (not the
# admin router's "admin" tag) — see backend/routers/admin/sync_status.py.
app.include_router(admin.sync_status.meta_router)
app.include_router(admin_queries.router)
app.include_router(provision.router)
app.include_router(sharing_domain.router)
app.include_router(session_scoring.router)
app.include_router(cmcd_admin.router)
app.include_router(debug.router)
app.include_router(share_auth.router)
app.include_router(share_oauth.router)
app.include_router(share_admin.router)
# E2E/dev-only in-process mock OIDC provider — mounted ONLY when OAUTH_MOCK_IDP=1
# so it never appears in the OpenAPI schema or in production.
from backend.routers import mock_idp  # noqa: E402

if mock_idp.mock_idp_enabled():
    app.include_router(mock_idp.router)
app.include_router(web_vitals.router)
app.include_router(ux_events.router)


# ── Health check ──────────────────────────────────────────────────────────────


# Single source of truth for the version reported by /api/health. Read from the
# installed package metadata (pyproject ``version``) so it can't silently drift
# from the frontend's package.json the way the prior hardcoded "1.0.0" did.
# Falls back to a literal only if metadata lookup fails (e.g. running from a
# tree that was never installed).
try:
    from importlib.metadata import version as _pkg_version

    _APP_VERSION = _pkg_version("fastly-log-analytics")
except Exception:
    _APP_VERSION = "2.4.0"


# Documents the canonical error codes this probe can surface — notably the
# real 503 it returns when a service is degraded (previously undocumented).
# 422 is deliberately left as FastAPI's auto-generated HTTPValidationError:
# this is one of the few endpoints with typed query params (deep / stale_minutes)
# AND it's live-fuzzed by tests/test_schemathesis_smoke.py, so the documented
# 422 must match the real request-validation body. There is no app-wide
# RequestValidationError reshaper, so claiming ErrorEnvelope here would be a
# documentation lie the fuzzer catches (and reshaping the body would be an
# out-of-scope wire-format change). See tests/test_error_envelope_contract.py.
_HEALTH_ERROR_RESPONSES = {code: spec for code, spec in DEFAULT_ERROR_RESPONSES.items() if code != 422}

# SRE-04: how long a sync row may sit in ``status='running'`` before deep
# /api/health treats it as a stall rather than a healthy in-progress sync. A
# normal sync caps at 240s (_run_service_cron max_seconds) and the orphan
# reaper reclaims leaked rows at 10 min; 15 min is comfortably past both, so a
# row still 'running' here means either a genuinely wedged ingest or a dead
# scheduler whose reaper never fired — both worth a 503.
_STUCK_SYNC_RUNNING_MINS = 15


@app.get("/api/health", tags=["meta"], responses=_HEALTH_ERROR_RESPONSES)
def health_check(
    request: Request,
    deep: bool = False,
    stale_minutes: int = 30,
):
    """Liveness/readiness probe.

    Default (``?deep=0``) is a cheap liveness response — 200 as soon as the
    HTTP server is answering. Pass ``?deep=1`` to also verify ingest
    freshness per configured service. A service is reported ``degraded``
    when ANY of:
      - its newest ``ingested_files.ingested_at`` is older than
        ``stale_minutes`` (default 30) — SRE-22: before degrading on this
        check, the cutoff is widened to the service's own historical p95
        gap between non-empty ingests (never narrower than
        ``stale_minutes``), so a low-traffic service's organic quiet
        periods don't false-positive;
      - the last terminal ``sync`` cron run ended in ``error``;
      - a ``sync`` row is stuck in ``running`` past ``_STUCK_SYNC_RUNNING_MINS``
        (SRE-04 — the orphan-stall / OOM-leak the success-only filter hides);
      - the latest terminal run of a critical non-sync ingest task
        (``commit`` / ``metadata_sync``) ended in ``error`` (SRE-05).
    The endpoint returns HTTP 503 when ANY service is degraded; otherwise 200.

    Cost: hits SQLite only — no FOS, no CDN, no Fastly API. Safe to wire
    into a load-balancer health check.

    Security (finding 003): the deep variant enumerates every configured
    service_id and reports per-service sync errors / staleness, which is
    operational metadata an analyst should not see. Force ``deep`` to
    False for remote (live-share) callers so analysts only ever get the
    shallow ok/degraded liveness signal.
    """
    from datetime import UTC, datetime, timedelta

    from fastapi.responses import JSONResponse

    from backend import config
    from backend.core import metadata as metadata_db
    from backend.utils.remote_access import is_request_remote

    if deep and is_request_remote(request):
        deep = False

    global _startup_complete
    import sys

    is_testing = "pytest" in sys.modules
    status = "ok" if (_startup_complete or is_testing) else "initializing"
    payload: dict = {"status": status, "version": _APP_VERSION}
    if not (_startup_complete or is_testing) or not deep:
        return payload

    # Clamp stale_minutes to a sane range — Schemathesis fuzzes this
    # with 2**63-1 which would OverflowError inside timedelta(...) and
    # 500 the liveness probe. 525 600 (1 year) is far past any realistic
    # operator-meaningful staleness window and well under the
    # timedelta(days) hard cap.
    from backend.utils.date_utils import iso_z

    stale_minutes = max(0, min(int(stale_minutes), 525_600))
    cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=stale_minutes))
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

            # SRE-04: a sync row stuck in 'running' (the documented orphan
            # stall / OOM-restart leak) is EXCLUDED by the filter above, so
            # without this the probe reports the older success as ok during
            # the stall window — and forever if the self-healing reaper never
            # runs (dead scheduler). Surface it as a stall once it outlives a
            # normal sync + the reaper window (_STUCK_SYNC_RUNNING_MINS). This
            # is an ADDITIVE degrade signal — wrapped so a query failure can
            # never flip a healthy service to 503 (worse than prior behavior);
            # the outer handler still degrades on genuine metadata-read loss.
            if svc_state["status"] == "ok":
                try:
                    stuck = con.execute(
                        "SELECT started_at FROM cron_runs "
                        "WHERE task = 'sync' AND status = 'running' "
                        "ORDER BY started_at DESC LIMIT 1"
                    ).fetchone()
                    if stuck and stuck["started_at"]:
                        stuck_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_STUCK_SYNC_RUNNING_MINS))
                        norm_started = str(stuck["started_at"]).replace(" ", "T").rstrip("Z")
                        norm_stuck_cutoff = str(stuck_cutoff).replace(" ", "T").rstrip("Z")
                        if norm_started < norm_stuck_cutoff:
                            svc_state["status"] = "degraded"
                            svc_state["reason"] = (
                                f"sync stuck running since {stuck['started_at']} (>{_STUCK_SYNC_RUNNING_MINS}m)"
                            )
                except Exception:
                    pass

            # SRE-05: the probe is otherwise task='sync'-blind. A commit cron
            # erroring every run (buffer growing, nothing reaching Iceberg) or
            # a read_only service's metadata_sync failing (its ONLY ingest
            # path) stays ok. Degrade if the latest terminal run of any
            # critical ingest task errored. Maintenance tasks (optimize /
            # expire / *_cleanup / rollup_compact) are intentionally excluded
            # — their failure doesn't freeze ingestion. Additive + fail-safe
            # for the same reason as SRE-04 above.
            if svc_state["status"] == "ok":
                try:
                    crit_row = con.execute(
                        "SELECT task, error_message FROM ("
                        "  SELECT task, status, error_message, "
                        "         ROW_NUMBER() OVER (PARTITION BY task ORDER BY started_at DESC, id DESC) AS rn "
                        "  FROM cron_runs WHERE task IN ('commit', 'metadata_sync') AND status != 'running'"
                        ") WHERE rn = 1 AND status = 'error' LIMIT 1"
                    ).fetchone()
                    if crit_row:
                        svc_state["status"] = "degraded"
                        svc_state["reason"] = (
                            f"{crit_row['task']} cron errored: {crit_row['error_message'] or 'unknown'}"
                        )
                except Exception:
                    pass

            # A brand-new service legitimately has no ingest yet; don't flag
            # it as degraded. Only flag services that have ingested at least
            # once AND fell behind the cutoff.
            if last_ingest and svc_state["status"] == "ok":
                norm_last = str(last_ingest).replace(" ", "T").rstrip("Z")
                norm_cutoff = str(cutoff).replace(" ", "T").rstrip("Z")
                if norm_last < norm_cutoff:
                    # SRE-22: a fixed stale_minutes cutoff false-positives on a
                    # low-traffic service's organic quiet periods. Before
                    # degrading, widen the cutoff to this service's own
                    # historical p95 inter-ingest gap (never narrower than
                    # stale_minutes) and re-check against that. Only reached
                    # once the naive check already looks stale, so the
                    # common healthy path pays zero extra query cost.
                    effective_minutes = metadata_db.adaptive_stale_minutes(con, default_minutes=stale_minutes)
                    effective_cutoff = cutoff
                    if effective_minutes > stale_minutes:
                        effective_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=effective_minutes))
                        svc_state["stale_minutes_used"] = effective_minutes
                    norm_effective_cutoff = str(effective_cutoff).replace(" ", "T").rstrip("Z")
                    if norm_last < norm_effective_cutoff:
                        svc_state["status"] = "stale"
                        svc_state["reason"] = f"no ingest since {last_ingest} (cutoff {effective_cutoff})"
        except Exception as e:
            svc_state["status"] = "degraded"
            svc_state["reason"] = f"metadata_db query failed: {e}"

        if svc_state["status"] not in ("ok", "stale"):
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
