"""FastAPI application entry point.

Run from the project root:

    uvicorn backend.main:app --port 8000 --reload
"""

from __future__ import annotations

import logging
import mimetypes
import os

# Explicitly register .mjs as application/javascript to pass strict browser MIME checks
mimetypes.add_type("application/javascript", ".mjs")

# Mitigate macOS malloc/free C++ heap corruption (Abort trap 6)
# DuckDB and PyArrow both use jemalloc internally which causes fatal crashes
# when passing memory back and forth. Forcing PyArrow to use system malloc segregates them.
os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"

import resource  # noqa: E402

try:
    # Increase file descriptor limit for DuckDB and DuckLake operations
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # Target 65k or the hard limit, whichever is lower.
    # 256/1024 is often too low for high-parallelism DuckDB/DuckLake scans.
    target = min(hard, 65536)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
except Exception:
    pass

import sys  # noqa: E402

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
from backend.utils.telemetry import configure_structlog  # noqa: E402

configure_structlog()
logging.getLogger("apscheduler").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Prevent OpenTelemetry FastAPI instrumentation from crashing on fastapi.routing._IncludedRouter
# during Match.PARTIAL route introspection (e.g. 405 Method Not Allowed).
# See MONKEYPATCHES.md for full details and removal criteria.
try:
    import fastapi.routing

    if hasattr(fastapi.routing, "_IncludedRouter"):
        setattr(fastapi.routing._IncludedRouter, "path", property(lambda self: ""))
except Exception:
    pass

logger = logging.getLogger("backend.main")

from fastapi import FastAPI  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

# ── Path setup ────────────────────────────────────────────────────────────────
# Ensure the project root is on sys.path so the backend package is importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.utils.runtime_modes import validate_mock_modes  # noqa: E402

validate_mock_modes()

from backend.core import lifespan as _lifespan  # noqa: E402
from backend.core.lifespan import lifespan  # noqa: E402
from backend.routers import (  # noqa: E402
    admin,
    alerts,
    audit,
    bootstrap,
    control_room,
    dashboard,
    insights,
    network,
    origin,
    performance,
    provision,
    query,
    rum,
    security,
    services,
    session_scoring,
    share_admin,
    usage,
    web_vitals,
)
from backend.routers import root_system as _root_system  # noqa: E402
from backend.routers.admin import high_scale  # noqa: E402
from backend.routers.root_system import configure_root_system  # noqa: E402
from backend.utils.module_shim import install_shim_module  # noqa: E402

# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fastly Log Analytics API",
    version="3.0.0-rc1",
    description=(
        "FastAPI backend for the Fastly Log Analytics tool. "
        "Serves the Next.js frontend and exposes an OpenAPI spec at /openapi.json."
    ),
    lifespan=lifespan,
)

configure_root_system(app)

# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(dashboard.router)
app.include_router(origin.router)
app.include_router(network.router)
app.include_router(security.router)
app.include_router(rum.router)
app.include_router(query.router)
app.include_router(performance.router)
app.include_router(high_scale.router)
app.include_router(insights.router)
app.include_router(session_scoring.sessions_router)
app.include_router(alerts.views_router)
app.include_router(alerts.alerts_router)
app.include_router(audit.router)
app.include_router(control_room.router)

app.include_router(services.router)
app.include_router(provision.router)
app.include_router(bootstrap.router)
app.include_router(usage.usage_router)
app.include_router(admin.router)
# Analyst-safe sync-status siblings carry the neutral "meta" tag (not the
# admin router's "admin" tag) — see backend/routers/admin/health.py.
app.include_router(admin.sync_status.meta_router)
app.include_router(share_admin.sharing_domain_router)
app.include_router(session_scoring.router)
app.include_router(bootstrap.debug_router)
app.include_router(web_vitals.router)
app.include_router(share_admin.router)
# E2E/dev-only in-process mock OIDC provider — mounted ONLY when OAUTH_MOCK_IDP=1
# so it never appears in the OpenAPI schema or in production.
if share_admin.mock_idp_enabled():
    app.include_router(share_admin.mock_idp_router)

# Attach root-level system routes directly as top-level APIRoutes on app.router
# so route-table tripwires inspecting app.routes see the root endpoints directly.
app.router.routes.extend(_root_system.router.routes)
app.router._mark_routes_changed()

# ── Static files (Next.js export) ────────────────────────────────────────────
# When the Next.js app is built with STATIC_EXPORT=1, it outputs to frontend/out/.
# Mount it here so FastAPI serves the frontend directly in production.
# In development the Next.js dev server runs separately; this mount is a no-op.
_STATIC_DIR = os.path.join(_ROOT, "frontend", "out")
if os.path.isdir(_STATIC_DIR):
    app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="static")

install_shim_module(
    __name__,
    (_lifespan, _root_system),
    skip_attrs=frozenset({"router"}),
    bidirectional_seed=True,
)
