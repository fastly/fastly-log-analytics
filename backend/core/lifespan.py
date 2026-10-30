"""Application lifespan, startup initialization, and shutdown orchestration."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend import config as svcconfig
from backend.core import clickhouse as clickhouse_client
from backend.core import clickhouse as clickhouse_observer
from backend.core import duckdb as _db
from backend.core import request_telemetry
from backend.high_scale import postgres_control as console_access
from backend.high_scale.ingest_controller import (
    get_high_scale_service_registry,
    register_high_scale_services_from_environment,
)
from backend.utils.remote_access import validate_admin_gateway_config
from backend.utils.telemetry import bridge_uvicorn_loggers

logger = logging.getLogger("backend.main")


def _initialize_service(cfg: dict):
    """Initialise a single service's status cache in the background."""
    sid = cfg.get("service_id")
    if not sid:
        return
    # ThreadPoolExecutor worker threads don't inherit the parent's
    # ContextVar; process_context_scope sets both the ContextVar and pushes
    # onto the global active-contexts stack so out-of-thread I/O fallback
    # (get_process_context_with_fallback) attributes this worker's DuckLake
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
                    logger.info("[fastapi] Service %s: reaped %d orphaned cron run(s).", sid, n)
            except Exception as e:
                # Transient on the first boot after an unclean shutdown — SQLite
                # WAL recovery rolls the file forward on the next connection, so
                # subsequent calls succeed. Log INFO if it's the recoverable
                # malformed-image case, WARN otherwise.
                msg = str(e)
                if "malformed" in msg or "is locked" in msg:
                    logger.info(
                        "[fastapi] Service %s: orphan-cron reap deferred (%s); WAL recovery will resolve on the next connection.",
                        sid,
                        msg,
                    )
                else:
                    logger.warning("[fastapi] Could not reap orphaned cron runs for %s: %s", sid, e)

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
                    _admin_mod = importlib.import_module("backend.routers.admin")
                    _admin_mod.compute_sync_status_cached(sid)
                except Exception as e:
                    logger.warning("[fastapi] Service %s: sync-status pre-warm failed: %s", sid, e)

                logger.info("[fastapi] Service %s initialised.", sid)
        except Exception as e:
            logger.warning("[fastapi] Could not initialise service %s: %s", sid, e)


def _ensure_persistent_view(sid: str, src: dict) -> None:
    """Build the per-service DuckLake view at startup if it's missing from
    the .duckdb file.

    Why this exists: ``refresh_config_status`` opens a read-only connection
    with ``skip_view_update=True``, so it can't (and won't) create the
    view. The scheduler's data sync builds the view only at the END of a
    full sync cycle (after potentially 1015+ files / 3+ minutes). So
    after any restart where the persisted view was previously dropped
    (manual DROP VIEW, .duckdb file deletion) the dashboard, security
    top-bots, and query page all surface "Catalog Error: Table with name
    logs_xxx does not exist" until the first sync completes.

    Building the view here uses only the DuckLake catalog + local
    parquet/buffer files — no FOS round-trip. When the view already exists, this is
    a single information_schema lookup and a no-op."""
    if svcconfig.is_durable_serving_mode(src):
        logger.info("[fastapi] Service %s: durable serving uses per-connection views; skipping file pre-warm", sid)
        return
    try:
        from backend.core import storage as _storage
        from backend.core.duckdb import get_connection

        # Always refresh the view at startup so the first dashboard load
        # after a restart doesn't pay the 500-900ms CREATE OR REPLACE VIEW
        # cost mid-request. Subsequent dashboard loads reuse the warm view
        # until a sync writer rebuilds it again. The previous "skip if
        # already exists" behaviour kept stale views around when buffer
        # files had been appended since the last writer close.
        logger.info("[fastapi] Service %s: pre-warming DuckLake view at startup", sid)
        writer = get_connection(source=src, read_only=False)
        try:
            _storage.update_storage_view(writer, src)
        finally:
            writer.close()
        logger.info("[fastapi] Service %s: view pre-warmed", sid)
    except Exception as e:
        logger.warning("[fastapi] Service %s: view pre-warm failed: %s", sid, e)


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
                    logger.info("[fastapi] POP locations cached at startup.")
                else:
                    logger.warning("[fastapi] POP locations fetch failed at startup.")
                break
    except Exception as e:
        logger.warning("[fastapi] Could not prefetch POP locations: %s", e)


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

        from backend.core.metadata.base import fetch_matrix_from_fos

        _tenant_matrix_path = importlib.import_module(
            "backend.provision.session_scoring_orchestrator"
        )._tenant_matrix_path

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
        # Tag everything done here so the boto3 hooks attribute their
        # telemetry rows to "startup" instead of falling back to the thread name.
        # MUST be process_context_scope (the context manager): the scheduler
        # starts below and its first cron's scope exit would pop the
        # active-contexts stack and null the mirror, untagging any in-flight
        # DuckLake I/O from the init_service workers. The scope keeps "startup" on
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
        for _mod_name in ("backend.routers.root_system", "backend.main"):
            _mod = sys.modules.get(_mod_name)
            if _mod is not None:
                _mod.__dict__["_startup_complete"] = True
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

    A wildcard (``*``) in either variable is ALWAYS fatal, strict mode or
    not: it makes uvicorn honor X-Forwarded-For from any TCP peer, so any
    direct client can stamp ``X-Forwarded-For: 127.0.0.1`` and be classified
    loopback-admin by the remote-access middleware. There is no deployment
    where that is a safe configuration — pin the proxy's real source
    IPs/subnet instead (e.g. the fixed compose-network subnet in
    docker-compose.multipod.yml).
    """
    trusted = (os.environ.get("TRUSTED_PROXY_IPS") or "").strip()
    uvicorn_trusted = (os.environ.get("UVICORN_FORWARDED_ALLOW_IPS") or "").strip()
    require_strict = os.environ.get("REQUIRE_PROXY_HEADERS") == "1" or os.environ.get("STRICT_DATA_DIR_CHECK") == "1"
    for var_name, var_value in (
        ("TRUSTED_PROXY_IPS", trusted),
        ("UVICORN_FORWARDED_ALLOW_IPS", uvicorn_trusted),
    ):
        if "*" in var_value:
            wildcard_msg = (
                f"FATAL: {var_name}={var_value!r} contains a wildcard. Trusting "
                "X-Forwarded-For from every peer lets any direct client spoof "
                "X-Forwarded-For: 127.0.0.1 and be classified loopback-admin. "
                "Pin the reverse proxy's real IPs or subnet instead (e.g. "
                "TRUSTED_PROXY_IPS=172.28.0.0/16 for the multipod compose "
                "network). Refusing to start."
            )
            logging.critical(wildcard_msg)
            raise RuntimeError(wildcard_msg)
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
    """Own ClickHouse cleanup even if a later startup/shutdown step raises."""
    from backend.core.request_telemetry import observer as request_metrics_observer

    try:
        async with _application_lifespan(app):
            try:
                # Serving observation is independent of ingestion cron guards.
                request_metrics_observer.start()
                yield
            finally:
                await asyncio.to_thread(request_metrics_observer.stop)
    finally:
        await asyncio.to_thread(request_telemetry.force_flush)
        await asyncio.to_thread(clickhouse_observer.stop_clickhouse_observer)
        await asyncio.to_thread(console_access.close_console_clients)
        await asyncio.to_thread(clickhouse_client.close_clickhouse_client)


@asynccontextmanager
async def _application_lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    from backend.celery_app import publisher as _sync_status_publisher
    from backend.core.realtime.publisher import publisher as _rt_publisher
    from backend.cron.scheduler import get_scheduler
    from backend.cron.scheduler import publisher as _cron_runs_publisher

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

    # Proxy-headers regression guard (security). Production
    # must have TRUSTED_PROXY_IPS set in env (mirrors the uvicorn
    # --forwarded-allow-ips flag). Without it, IP-based gates become
    # ineffective and the Host-spoof admin bypass returns.
    _enforce_proxy_headers_configured()
    validate_admin_gateway_config()

    # Ensure the Postgres metadata schema BEFORE anything issues a metadata
    # query. SQLite self-initialises via the pool's schema_fn; Postgres had
    # no equivalent, so a stack that skipped scripts/setup_pg_schema.py
    # booted clean and then failed every metadata query with
    # `relation "cron_runs" does not exist`. Idempotent, once per process,
    # cheap no-op when METADATA_DSN is unset, and non-fatal on failure (it
    # logs CRITICAL — see ensure_pg_schema).
    from backend.core.metadata.base import ensure_pg_schema

    ensure_pg_schema()

    # Deployment-mode sanity: DEPLOYMENT_MODE is the single gate for the
    # high-scale data plane (standard vs high_scale), and
    # high_scale mode requires a multi-writer (Postgres) DuckLake catalog.
    svcconfig.validate_deployment_mode()

    # Opt-in and fail explicitly; never claim a healthy ClickHouse dependency
    # or silently switch engines when this startup probe fails.
    clickhouse = clickhouse_client.get_clickhouse_client()
    if clickhouse is not None:
        await asyncio.to_thread(clickhouse.health)
        clickhouse_observer.start_clickhouse_observer()
        registered_high_scale = register_high_scale_services_from_environment(
            get_high_scale_service_registry(),
            client=clickhouse,
        )
        if registered_high_scale:
            logging.info("[fastapi] Registered %d high-scale service(s).", len(registered_high_scale))

            # Per-service least-privilege ClickHouse identities for analyst SQL (no-op without a console secret).
            def _provision() -> tuple[str, ...]:
                with console_access.provisioning_client(clickhouse) as admin:
                    return console_access.provision_configured_services(admin, registered_high_scale)

            provisioned = await asyncio.to_thread(_provision)
            if provisioned:
                logging.info("[fastapi] Provisioned analyst console access for %d service(s).", len(provisioned))

    # Verify dependencies
    try:
        import pyarrow  # noqa: F401
    except ImportError as e:
        logging.critical(
            "\n[CRITICAL] Missing required dependency: %s\n[CRITICAL] Please run: pip install -e .\n", e.name
        )

    # Eagerly start the local telemetry proxy. Idempotent —
    # boto3/DuckDB would lazy-start it on first call otherwise; eager
    # start moves the ~hundreds-of-ms port bind out of the first request's
    # latency.
    try:
        from backend.utils import telemetry as telemetry_proxy

        telemetry_proxy.start_proxy_server()
    except Exception as e:
        logging.warning("[fastapi] telemetry proxy failed to start at lifespan boot: %s", e)

    # All initialisation (scheduler, DuckDB views, status cache) runs in a
    # background daemon thread so slow S3/DuckLake calls never block startup.
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

    try:
        yield
    finally:
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
