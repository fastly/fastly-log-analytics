"""Per-query attribution — who triggered it + what code is running it.

The Live Query Monitor needs to answer two questions for every running SQL
statement: **who** (principal) and **what** (call site). Both are captured
once at register time via a single ContextVar plus a Python stack walk;
the rest of the registry stores the resulting :class:`Attribution`.

Why a single ContextVar (rather than three separate context sources):
register() runs in the SQL hot path. Branching on "is there a request? a
cron context? fall back to thread name?" pays the cost on every query.
Instead, every entrypoint (RequestContext construction in
:mod:`backend.core.request_context`, ``process_context_scope`` in
:mod:`backend.utils.telemetry`) writes a fully-formed :class:`Attribution`
into the ContextVar at entry. The registry's hot path is then one
ContextVar ``.get()`` plus the stack walk.

ContextVar propagation note: Python 3.11+ guarantees ContextVar copy across
``asyncio.to_thread`` and FastAPI's thread pool, so an analyst request that
hops threads inside the route still carries the right attribution. Cron
jobs scheduled by APScheduler enter via ``process_context_scope`` which
sets the ContextVar inside the worker thread; the same value flows through
any further ``copy_context()`` hops the cron makes.
"""

from __future__ import annotations

import sys
import threading
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

# Files that are part of the instrumentation/driver layer — skipped when
# walking the stack to find the application caller. Match by substring so
# both editable installs and packaged paths work.
_INSTRUMENTATION_PREFIXES: tuple[str, ...] = (
    "backend/core/query_registry",
    "backend/core/query_attribution",
    "backend/core/query_instrumentation",
    "backend/utils/sqlite_profiler",
    "backend/core/duckdb_pool",  # _instrument() helper
    # stdlib + driver frames we'd skip past anyway
    "/sqlite3/",
    "/duckdb/",
)


def _capture_caller(skip_frames: int = 2) -> tuple[str, str]:
    """Walk up the stack and return ``(qualname, "<rel-path>:<lineno>")`` of
    the first frame outside the instrumentation/driver layer.

    Returns ``("<unknown>", "<unknown>")`` if no application frame is found.
    Cost: ~5-10us per call (frame walks are cheap and we stop early). Safe
    to call from the SQL hot path.
    """
    try:
        frame: Any = sys._getframe(skip_frames)
    except ValueError:
        return ("<unknown>", "<unknown>")
    while frame is not None:
        path = frame.f_code.co_filename
        if not any(p in path for p in _INSTRUMENTATION_PREFIXES):
            qual = getattr(frame.f_code, "co_qualname", frame.f_code.co_name)
            # Trim to project-relative path when possible.
            display_path = path
            for marker in ("backend/", "frontend/"):
                idx = path.rfind(marker)
                if idx != -1:
                    display_path = path[idx:]
                    break
            return (qual, f"{display_path}:{frame.f_lineno}")
        frame = frame.f_back
    return ("<unknown>", "<unknown>")


@dataclass(slots=True)
class Attribution:
    """Structured attribution for a single in-flight or completed query.

    Exactly one of ``analyst_id`` / ``admin_id`` / ``cron_job`` is populated
    based on ``kind``. ``caller_qualname`` and ``caller_file`` are always
    set (fall back to ``"<unknown>"`` only when the stack walk fails).
    """

    # WHO — exactly one of these is populated per kind
    kind: str  # "analyst" | "admin" | "cron" | "system"
    analyst_id: str | None = None
    analyst_name: str | None = None
    admin_id: str | None = None
    cron_job: str | None = None
    cron_run_id: str | None = None

    # WHAT — always populated (captured at register time via _capture_caller)
    caller_qualname: str = "<unknown>"
    caller_file: str = "<unknown>"
    request_path: str | None = None
    request_id: str | None = None

    # Per-connection pool slot (DuckDB only) — filled by the registry when
    # the connection is known. Helps ops correlate with duckdb_pool stats.
    pool_slot: str | None = None

    @classmethod
    def analyst(
        cls,
        *,
        analyst_id: str,
        analyst_name: str | None,
        request_path: str | None,
        request_id: str | None,
    ) -> Attribution:
        return cls(
            kind="analyst",
            analyst_id=analyst_id,
            analyst_name=analyst_name,
            request_path=request_path,
            request_id=request_id,
        )

    @classmethod
    def admin(
        cls,
        *,
        admin_id: str,
        request_path: str | None,
        request_id: str | None,
    ) -> Attribution:
        return cls(
            kind="admin",
            admin_id=admin_id,
            request_path=request_path,
            request_id=request_id,
        )

    @classmethod
    def cron(cls, *, cron_job: str, cron_run_id: str | None = None) -> Attribution:
        return cls(kind="cron", cron_job=cron_job, cron_run_id=cron_run_id)

    @classmethod
    def system(cls, *, hint: str | None = None) -> Attribution:
        """Fallback when no request/cron context is active. The thread name
        is folded into the caller_qualname so an admin can still tell
        startup/pool-warmer/migration work apart."""
        thread_name = threading.current_thread().name
        return cls(
            kind="system",
            caller_qualname=hint or f"thread:{thread_name}",
            caller_file="<system>",
        )

    def principal_id(self) -> str | None:
        """The single ID that identifies who triggered this query. Used by
        the audit log + by frontend grouping."""
        if self.kind == "analyst":
            return self.analyst_id
        if self.kind == "admin":
            return self.admin_id
        if self.kind == "cron":
            return self.cron_run_id or self.cron_job
        return None

    def display_label(self) -> str:
        """Single-line label for the live monitor row."""
        if self.kind == "analyst":
            who = self.analyst_name or (f"Guest ({self.analyst_id[-4:] if self.analyst_id else '?'})")
            tail = f" — {self.request_path}" if self.request_path else ""
            return f"Analyst: {who}{tail}"
        if self.kind == "admin":
            tail = f" — {self.request_path}" if self.request_path else ""
            who = self.admin_id or "admin"
            return f"Admin: {who}{tail}"
        if self.kind == "cron":
            run = f" (run {self.cron_run_id})" if self.cron_run_id else ""
            return f"Cron: {self.cron_job}{run}"
        return f"System: {self.caller_qualname}"

    def with_caller(self, qualname: str, file_line: str) -> Attribution:
        """Return a copy with the caller frame filled in. Used by the
        registry after :func:`_capture_caller`."""
        new = Attribution(
            kind=self.kind,
            analyst_id=self.analyst_id,
            analyst_name=self.analyst_name,
            admin_id=self.admin_id,
            cron_job=self.cron_job,
            cron_run_id=self.cron_run_id,
            caller_qualname=qualname,
            caller_file=file_line,
            request_path=self.request_path,
            request_id=self.request_id,
            pool_slot=self.pool_slot,
        )
        return new

    def with_pool_slot(self, slot: str | None) -> Attribution:
        if slot is None or self.pool_slot == slot:
            return self
        return Attribution(
            kind=self.kind,
            analyst_id=self.analyst_id,
            analyst_name=self.analyst_name,
            admin_id=self.admin_id,
            cron_job=self.cron_job,
            cron_run_id=self.cron_run_id,
            caller_qualname=self.caller_qualname,
            caller_file=self.caller_file,
            request_path=self.request_path,
            request_id=self.request_id,
            pool_slot=slot,
        )


# Process-wide ContextVar set by request/cron entrypoints. ``None`` means
# "fall back to a synthesised system attribution" — covers boot-time work,
# pool warmers, and any thread that bypasses the entrypoint setters.
current_attribution: ContextVar[Attribution | None] = ContextVar("current_attribution", default=None)


def derive_from_process_context(process_ctx: str | None) -> Attribution | None:
    """Build a best-effort :class:`Attribution` from the legacy
    ``_PROCESS_CONTEXT`` string used by :mod:`backend.utils.telemetry`.

    The process_context string takes shapes like ``"cron:sync_svc1"``,
    ``"api:GET /admin/download-zip:..."``, ``"startup:init_service:svc1"``.

    Returns an attribution only for ``cron:`` / ``startup:`` / ``shutdown:``
    contexts. ``api:`` is INTENTIONALLY ignored — the telemetry middleware
    sets ``process_context_scope("api:...")`` on every HTTP request, but
    HTTP attribution belongs to
    :func:`backend.core.request_context._build_attribution_from_request`
    which has the real principal (analyst session or client IP). Returning
    an admin attribution here would shadow the proper one in scenarios
    where the SQL execution thread inherits the middleware's Context but
    the per-request RequestContext value didn't propagate (sync deps on
    the thread pool, fsspec iothread, etc.).
    """
    if not process_ctx:
        return None
    head, _, tail = process_ctx.partition(":")
    if head == "cron":
        return Attribution.cron(cron_job=tail or process_ctx)
    if head in ("startup", "shutdown"):
        return Attribution.system(hint=process_ctx)
    # "api:..." and anything else → defer to RequestContext / synthesised system.
    return None
