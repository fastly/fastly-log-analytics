"""FastAPI dependency functions.

Provides injectable dependencies for service resolution and DuckDB connection
management. All route handlers receive these via FastAPI's Depends() mechanism.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import Any

# Ensure the root project directory (parent of backend/) is on sys.path so
# that the backend package is importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import duckdb
from fastapi import Depends, Header, HTTPException, Query

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core.duckdb import DBBusyError, get_connection

# ── Service resolution ────────────────────────────────────────────────────────


def get_service_id(
    service: str | None = Query(default=None),
    sid: str | None = Query(default=None, alias="service_id"),
    x_fastly_service_id: str | None = Header(default=None, alias="x-fastly-service-id"),
    x_service_id: str | None = Header(default=None, alias="x-service-id"),
) -> str | None:
    """Resolve the active service ID from headers, query params or the first configured service.

    Accepts ``?service=`` or ``?service_id=`` for parity with frontend call sites,
    plus the ``x-fastly-service-id`` / ``x-service-id`` headers. If the provided
    ID matches a cdn_service_id in any config, it returns the corresponding
    logging service ID.
    """
    res_sid = service or sid or x_fastly_service_id or x_service_id
    if res_sid:
        if svcconfig.load_config(res_sid):
            return res_sid

        return svcconfig.get_cdn_service_id_map().get(res_sid, res_sid)

    return svcconfig.get_active_service_id()


def get_source(service_id: str | None = Depends(get_service_id)) -> dict:
    """Return the source config dict for the active service.

    Raises 400 if no service is configured.
    """
    if service_id:
        src = db.get_source_for_service(service_id)
        if src:
            return src
    raise HTTPException(
        status_code=400,
        detail={"error": "No active service configured. Please configure a service first.", "no_service": True},
    )


# ── DuckDB connection ─────────────────────────────────────────────────────────


class _ConnectionHolder:
    """Holds a single DuckDB connection for the lifetime of one request.

    Read-only requests check out a pooled, pre-warmed connection via
    ``duckdb_pool.checkout_connection`` (saves ~50ms per request of
    pragma / S3 / iceberg-view setup). Write-mode connections still take
    the always-fresh ``get_connection`` path because ingest holds the
    write lock and pooling would defeat its lifecycle.

    Used as a context-manager-style dependency so FastAPI returns the
    connection to the pool (or closes the fresh one) when the request
    finishes. On any exception the connection is discarded rather than
    pooled so a poisoned connection doesn't get reused.
    """

    def __init__(self, source: dict, skip_view_update: bool = False, read_only: bool = True):
        self._source = source
        self._skip_view_update = skip_view_update
        self._read_only = read_only
        self.con: duckdb.DuckDBPyConnection | None = None
        # Set when we exit cleanly so __exit__ knows to return-vs-discard.
        self._errored = False
        # Used only on the pooled path so __exit__ can release. Typed
        # ``Any | None`` because ``duckdb_pool.checkout_connection`` is a
        # contextmanager-decorated generator and mypy struggles to thread
        # its return type through.
        self._pool_cm: Any | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        # Write mode + skip_view_update fall back to the fresh-connection
        # path: the pool exists for the dominant read-only HTTP request
        # workload, not for ingest's exclusive writer or for callers that
        # explicitly opt out of view binding. The pool itself can also be
        # disabled globally via DUCKDB_CONNECTION_POOL=0 (tests + emergency
        # rollback); when disabled we go straight through ``get_connection``
        # so behaviour matches the pre-pool design exactly.
        from backend.core import duckdb_pool

        use_pool = self._read_only and not self._skip_view_update and duckdb_pool._pool_enabled()
        try:
            if use_pool:
                self._pool_cm = duckdb_pool.checkout_connection(self._source, max_wait=10.0)
                self.con = self._pool_cm.__enter__()
            else:
                self.con = get_connection(
                    source=self._source,
                    max_wait=10,
                    skip_view_update=self._skip_view_update,
                    read_only=self._read_only,
                )
        except DBBusyError as e:
            raise HTTPException(
                status_code=503,  # 503 Service Unavailable so frontend fetch throws and React Query keeps cached data
                detail={"error": str(e), "busy": True},
            )
        except Exception as e:
            # Pool exhaustion (after wait timeout) surfaces as _PoolBusy.
            # Translate to 503 so the frontend handles it the same as
            # DBBusyError instead of throwing an opaque 500.
            from backend.core.duckdb_pool import _PoolBusy

            if isinstance(e, _PoolBusy):
                raise HTTPException(
                    status_code=503,
                    detail={"error": str(e), "busy": True},
                )
            raise
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._errored = exc_type is not None
        if self._pool_cm is not None:
            # Forward the exception to the pool context manager so it can
            # mark the connection errored and discard.
            try:
                self._pool_cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass
            self._pool_cm = None
            self.con = None
            return False
        if self.con:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None
        return False


def get_con(source: dict = Depends(get_source)) -> Iterator[duckdb.DuckDBPyConnection]:
    """Dependency that yields a DuckDB connection and closes it after the request.

    Always opens in read-only mode for HTTP request handlers — write-mode
    connections are used only by the scheduler/cron pipeline, never by
    user-facing routes.

    Security: do NOT take ``read_only`` as a parameter. FastAPI converts
    primitive-typed dependency parameters into query parameters, so any
    request to a route using this dep could send ``?read_only=false`` and
    force an exclusive write-lock acquisition that blocks readers and the
    sync cron (503 DoS). The flag is hardcoded inside the holder instead.
    """
    holder = _ConnectionHolder(source, read_only=True)
    with holder as con:
        yield con


# ``AnalyticsDeps`` (bundle of get_source + get_con) was removed at the
# v2.0 cut. Routes now take :class:`backend.core.request_context.RequestContext`
# directly via ``Depends(build_request_context)`` — same connection +
# source surface, with structural tenancy enforcement on every route
# (the old bundle skipped it because ``require_service_access`` was
# never wired in as a sibling dep).


# ── Tenant-scope enforcement (security) ─────────────


def require_service_access(
    request,
    service_id: str | None = Depends(get_service_id),
) -> str | None:
    """Reject the request with 403 if the caller (analyst session) does not
    have access to the requested ``service_id``.

    Local admin requests (analyst_session is None) bypass this check entirely
    — admins have access to every configured service. Analysts must have the
    target ``service_id`` in their invite's ``service_ids`` list.

    Use as a dependency on any route that returns or mutates per-service
    data. Routes that take no ``service_id`` parameter and that expose a
    list of services across the whole tenant must filter the list manually
    using ``request.state.analyst_session.service_ids`` — this helper only
    enforces the single-service case.
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return service_id  # admin / local — unrestricted
    allowed = set(analyst_session.service_ids or [])
    if service_id is None:
        # Analyst calls with no explicit service must default to one of their
        # scoped services. Return the first one (or None if invite is empty).
        return next(iter(allowed), None)
    if service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": service_id},
        )
    return service_id


# ``get_meta_con`` (skip-view-update parallel path) removed at v2.0 cut.
# After the Phase 4 iceberg carve + duckdb_pool fingerprint check
# (backend/core/duckdb_pool.py:299), pool checkouts skip update_iceberg_view
# when the (view_cache identity, buffer mtime) tuple is unchanged — making
# the skip-on-purpose path of the old helper structurally unnecessary for
# the metadata-shaped read paths that used it.
