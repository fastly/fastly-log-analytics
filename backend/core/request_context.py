"""RequestContext — one object per request, owns everything per-request.

Phase 2 of the v2.0 cleanup. Replaces the :class:`backend.deps.AnalyticsDeps`
bundle + standalone :func:`backend.deps.require_service_access` calls with a
single FastAPI dependency that is impossible to construct without tenancy
enforcement having run.

Design constraints (ADR-02):

- **No re-resolution mid-request.** ``service_id`` / ``source`` / ``con``
  resolved once at construction, fixed for the request lifetime.
- **Tenancy is structural.** A ``RequestContext`` cannot be obtained
  without passing through ``require_service_access`` enforcement first.
  No route ever needs to call it explicitly.
- **``read_only`` is a constructor argument, NOT a dep parameter.** FastAPI
  converts primitive-typed dep params into query params, which would
  expose ``read_only=False`` to attackers (the documented "private
  attribute trick" we're now eliminating structurally).
- **``cached_temps`` is request-scoped.** First repository that builds a
  per-window temp inserts; subsequent repositories in the same request
  reuse via the shared dict. Removes the recurring
  "share live-hour temp between dashboard CTEs and top_n_rollups" rework.

Backward compat (Phase 2.7):

- ``AnalyticsDeps = RequestContext`` aliased in :mod:`backend.deps` through
  Phase 8. Any caller importing ``AnalyticsDeps`` keeps working.
- Existing ``get_source`` / ``get_con`` deps still exist; routes can keep
  using them. New routes prefer the ``RequestContext`` dependency.

The migration order is in ``pending-docs/cleanup_plan.md`` §Phase 2:
dashboard → query → security → alerts/network/performance/origin/sessions/
insights/views/bootstrap, then defer admin/provision/share to Phase 5.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request

from backend.core.request_telemetry import RequestTelemetry
from backend.deps import _ConnectionHolder, get_service_id

if TYPE_CHECKING:
    import duckdb


@dataclass(slots=True)
class RequestContext:
    """Per-request context object held on ``request.state.ctx``.

    See module docstring for the design rationale. Constructed via
    :func:`build_request_context` which is the FastAPI dependency.
    """

    service_id: str
    source: dict
    con: duckdb.DuckDBPyConnection
    telemetry: RequestTelemetry
    analyst_session: object | None = None
    cached_temps: dict[str, str] = field(default_factory=dict)
    read_only: bool = True

    # The connection holder is kept on the context so the dependency
    # generator can hand it back to the pool on request end. Not part
    # of the public surface; routes should never touch it.
    _holder: _ConnectionHolder | None = field(default=None, repr=False, compare=False)


def _enforce_service_access(
    request: Request,
    service_id: str | None,
) -> str:
    """Mirror of :func:`backend.deps.require_service_access` invoked
    inline during context construction.

    Raises 400 if no service is resolvable; 403 if an analyst session is
    present and doesn't have access to the resolved service. Admin requests
    (no analyst_session) pass through unrestricted.

    Returns the validated service_id (never None — empty/missing raises 400
    so the route never has to None-check).
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        if not service_id:
            raise HTTPException(
                status_code=400,
                detail={"error": "no_service", "no_service": True},
            )
        return service_id

    allowed = set(analyst_session.service_ids or [])
    if service_id is None:
        # Analyst calls with no explicit service default to the first of
        # their scoped services. Mirrors require_service_access semantics.
        chosen = next(iter(allowed), None)
        if chosen is None:
            raise HTTPException(
                status_code=400,
                detail={"error": "no_service", "no_service": True},
            )
        return chosen
    if service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": service_id},
        )
    return service_id


def build_request_context(
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    """FastAPI dependency that constructs (and yields) a RequestContext.

    The connection lives for the request lifetime; the dependency's
    ``finally`` block hands it back to the pool (or closes it on error).
    """
    # Enforce tenancy BEFORE opening any connection — no need to acquire
    # a pool slot for a request we're about to 403.
    resolved_sid = _enforce_service_access(request, service_id)

    # Resolve the source dict for the validated service. Local helper
    # mirrors the body of ``backend.deps.get_source`` — we don't call the
    # FastAPI-decorated dep directly because resolving its parameter chain
    # outside the FastAPI dependency graph is a brittle pattern.
    source = _resolve_source(resolved_sid)

    # Build the RequestTelemetry root span. Cheap when the SDK is not
    # initialised (test mode); ~100ns when it is.
    telemetry = RequestTelemetry(
        request_method=request.method,
        request_path=request.url.path,
    )
    telemetry.start_request()

    holder = _ConnectionHolder(source, read_only=True)
    try:
        with holder as con:
            ctx = RequestContext(
                service_id=resolved_sid,
                source=source,
                con=con,
                telemetry=telemetry,
                analyst_session=getattr(request.state, "analyst_session", None),
                read_only=True,
                _holder=holder,
            )
            # Park the context on request.state so downstream non-route
            # code (middleware, error handlers) can read it.
            request.state.ctx = ctx
            try:
                yield ctx
            finally:
                telemetry.end_request()
    except HTTPException:
        telemetry.end_request(status_code=400)
        raise


def _resolve_source(service_id: str) -> dict:
    """Inline copy of the source-resolution body from
    :func:`backend.deps.get_source`. Kept local so we don't reach inside
    the FastAPI-decorated dep just to call its body."""
    from backend.core import duckdb as db

    src = db.get_source_for_service(service_id)
    if src:
        return src
    raise HTTPException(
        status_code=400,
        detail={
            "error": "No active service configured. Please configure a service first.",
            "no_service": True,
        },
    )
