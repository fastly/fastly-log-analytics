"""FastAPI dependency functions.

Provides injectable dependencies for service resolution and DuckDB connection
management. All route handlers receive these via FastAPI's Depends() mechanism.
"""

from __future__ import annotations

import os
import sys

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

    Used as a context-manager-style dependency so FastAPI closes the
    connection when the request finishes.
    """

    def __init__(self, source: dict, skip_view_update: bool = False, read_only: bool = True):
        self._source = source
        self._skip_view_update = skip_view_update
        self._read_only = read_only
        self.con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        try:
            self.con = get_connection(
                source=self._source,
                max_wait=10,  # Increased wait slightly for safety
                skip_view_update=self._skip_view_update,
                read_only=self._read_only,
            )
        except DBBusyError as e:
            raise HTTPException(
                status_code=503,  # 503 Service Unavailable so frontend fetch throws and React Query keeps cached data
                detail={"error": str(e), "busy": True},
            )
        return self.con

    def __exit__(self, *_):
        if self.con:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None


def get_con(source: dict = Depends(get_source), read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Dependency that yields a DuckDB connection and closes it after the request.

    Defaults to read_only=True for dashboard queries to prevent blocking on crons.
    """
    holder = _ConnectionHolder(source, read_only=read_only)
    with holder as con:
        yield con


# ── Bundled analytics dependency ─────────────────────────────────────────────


class AnalyticsDeps:
    """Bundles the two common analytics dependencies into a single injectable.

    Usage in a route::

        @router.post("/api/my-endpoint")
        @query_errors()
        def my_endpoint(req: MyRequest, deps: AnalyticsDeps = Depends()):
            return repo.do_stuff(con=deps.con, src=deps.source, ...)
    """

    def __init__(
        self,
        source: dict = Depends(get_source),
        con: duckdb.DuckDBPyConnection = Depends(get_con),
    ):
        self.source = source
        self.con = con


def get_meta_con(source: dict = Depends(get_source), read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Dependency that yields a DuckDB connection, skipping the Iceberg view update.

    Use this for metadata routes (e.g. cron logs, admin settings) that don't
    need to query the main logs table, to avoid blocking on S3 manifest reads.
    """
    holder = _ConnectionHolder(source, skip_view_update=True, read_only=read_only)
    with holder as con:
        yield con
