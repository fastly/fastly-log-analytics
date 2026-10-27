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

Backward compat (Phase 2.7):

- ``AnalyticsDeps`` (the transitional ``RequestContext`` alias in
  :mod:`backend.deps`) was removed at the v2.0 cut — use the
  ``RequestContext`` dependency directly. See
  ``tests/test_deps.py::test_analytics_deps_symbol_removed``.
- Existing ``get_source`` / ``get_con`` deps still exist; routes can keep
  using them. New routes prefer the ``RequestContext`` dependency.

The migration order (cleanup Phase 2) was:
dashboard → query → security → alerts/network/performance/origin/sessions/
insights/views/bootstrap, then defer admin/provision/share to Phase 5.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import sys
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import Depends, HTTPException, Request

from backend.core.request_telemetry import RequestTelemetry

if TYPE_CHECKING:
    from collections.abc import Callable

    import duckdb

    from backend.high_scale.registry_bootstrap import HighScaleService
    from backend.utils.remote_access import TimeBounds

from collections.abc import Iterator
from typing import Annotated, Any

from fastapi import Header, Path, Query

from backend import config as svcconfig
from backend.config import SERVICE_ID_PATTERN
from backend.core import duckdb as db
from backend.core.duckdb import DBBusyError, get_connection
from backend.utils.structlog_config import redact_error_text

# Ensure the root project directory (parent of backend/) is on sys.path so
# that the backend package is importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import duckdb

logger = logging.getLogger(__name__)

# Path-param service_id with a defence-in-depth regex. The authoritative
# validation lives at ``load_config(service_id)`` (which rejects any id
# missing from CONFIGS_DIR), but tightening at the FastAPI boundary means
# malformed ids never reach SQL / filesystem code paths in the first
# place. Mirrors the alphabet S3 bucket names use (the same character set
# Fastly assigns to its service ids).

ServiceId = Annotated[str, Path(pattern=SERVICE_ID_PATTERN)]

# ── Service resolution ────────────────────────────────────────────────────────


def get_service_id(
    request: Request,
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
    res_sid = request.path_params.get("service_id") or service or sid or x_fastly_service_id or x_service_id
    if res_sid:
        if svcconfig.load_config(res_sid):
            return res_sid

        return svcconfig.get_cdn_service_id_map().get(res_sid, res_sid)

    return svcconfig.get_active_service_id()


def _resolve_source_or_400(service_id: str | None) -> dict:
    """Look up ``service_id``'s source dict, or raise 400 with the canonical
    ``no_service`` detail the frontend checks for."""
    if service_id:
        src = db.get_source_for_service(service_id)
        if src:
            return src
    raise HTTPException(
        status_code=400,
        detail={"error": "No active service configured. Please configure a service first.", "no_service": True},
    )


def get_source(service_id: str | None = Depends(get_service_id)) -> dict:
    """Return the source config dict for the active service.

    Raises 400 if no service is configured.
    """
    return _resolve_source_or_400(service_id)


# ── DuckDB connection ─────────────────────────────────────────────────────────


class _ConnectionHolder:
    """Holds a single DuckDB connection for the lifetime of one request.

    Read-only requests check out a pooled, pre-warmed connection via
    ``duckdb_pool.checkout_connection`` (saves ~50ms per request of
    pragma / S3 / lake-view setup). Write-mode connections still take
    the always-fresh ``get_connection`` path because ingest holds the
    write lock and pooling would defeat its lifecycle.

    Used as a context-manager-style dependency so FastAPI returns the
    connection to the pool (or closes the fresh one) when the request
    finishes. On any exception the connection is discarded rather than
    pooled so a poisoned connection doesn't get reused.
    """

    def __init__(
        self,
        source: dict,
        skip_view_update: bool = False,
        read_only: bool = True,
        heavy: bool = False,
        label: str | None = None,
    ):
        self._source = source
        self._label = label
        self._skip_view_update = skip_view_update
        self._read_only = read_only
        # Aggregate routes take a heavy slot so light requests keep pool capacity
        # (see the "Heavy/light bulkhead" note in backend.core.duckdb_pool).
        self._heavy = heavy
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

        use_pool = (
            self._read_only
            and not self._skip_view_update
            and duckdb_pool._pool_enabled()
            and not svcconfig.is_durable_serving_mode(self._source)
        )
        # Pool saturation is normal during dashboard fan-out: one composite
        # request can hold two connections while several cards mount at once.
        # Wait long enough for the bounded pool queue to drain instead of
        # turning an ordinary burst into a user-visible 503. This remains
        # bounded for a genuinely wedged connection and can be tuned lower
        # for constrained deployments via DUCKDB_POOL_MAX_WAIT_S.
        max_wait_env = os.getenv("DUCKDB_POOL_MAX_WAIT_S", "10.0")
        try:
            max_wait = max(1.0, float(max_wait_env))
        except (TypeError, ValueError):
            max_wait = 10.0

        try:
            if use_pool:
                self._pool_cm = duckdb_pool.checkout_connection(
                    self._source, max_wait=max_wait, heavy=self._heavy, label=self._label
                )
                self.con = self._pool_cm.__enter__()
            else:
                self.con = get_connection(
                    source=self._source,
                    max_wait=max_wait,
                    skip_view_update=self._skip_view_update,
                    read_only=self._read_only,
                )
        except DBBusyError as e:
            raise HTTPException(
                status_code=503,  # 503 Service Unavailable so frontend fetch throws and React Query keeps cached data
                detail={"error": redact_error_text(e), "busy": True},
            )
        except Exception as e:
            # Pool exhaustion (after wait timeout) surfaces as _PoolBusy.
            # Translate to 503 so the frontend handles it the same as
            # DBBusyError instead of throwing an opaque 500.
            from backend.core.duckdb_pool import _PoolBusy

            if isinstance(e, _PoolBusy):
                raise HTTPException(
                    status_code=503,
                    detail={"error": redact_error_text(e), "busy": True},
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
                logger.debug("RequestContext pool connection release failed", exc_info=True)
            self._pool_cm = None
            self.con = None
            return False
        if self.con:
            try:
                self.con.close()
            except Exception:
                logger.debug("RequestContext connection close failed", exc_info=True)
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


# ── Tenant-scope enforcement (security) ─────────────


def require_service_access(
    request: Request,
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

    path_sid = request.path_params.get("service_id")
    if path_sid:
        res_path_sid = path_sid
        if not svcconfig.load_config(path_sid):
            res_path_sid = svcconfig.get_cdn_service_id_map().get(path_sid, path_sid)
        if res_path_sid not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": path_sid},
            )

    return service_id


def require_admin(request: Request) -> None:
    """Reject analyst sessions with 403 — admin-only endpoints.

    Use as a dependency on mutation endpoints that should never be
    accessible to analysts regardless of service scope.
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        raise HTTPException(
            status_code=403,
            detail={"error": "admin_only", "message": "This operation requires admin access."},
        )


logger = logging.getLogger(__name__)


class RangeWindowRequest(Protocol):
    """Structural type for request bodies that carry a (relative or absolute) window."""

    range_token: str | None
    anchor: str | None
    start_time: str | None
    end_time: str | None


@dataclass(slots=True)
class RequestContext:
    """Per-request context object held on ``request.state.ctx``.

    See module docstring for the design rationale. Constructed via
    :func:`build_request_context` which is the FastAPI dependency.
    """

    service_id: str
    source: dict
    telemetry: RequestTelemetry
    analyst_session: object | None = None
    read_only: bool = True
    cached_temps: dict = field(default_factory=dict)
    # The analyst clamp window, resolved once in build_request_context (same
    # request.state.analyst_session source as get_analyst_time_bounds). None
    # only for RequestContexts built outside the dependency (e.g. tests),
    # where ``clamp`` falls back to an open window.
    time_bounds: TimeBounds | None = None
    _holder: _ConnectionHolder | None = field(default=None, repr=False, compare=False)
    _con_override: duckdb.DuckDBPyConnection | None = field(default=None, repr=False)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """Lazy-loaded DuckDB connection. Acquired on first access so high-scale
        ClickHouse queries don't check out DuckDB pool slots and saturate them."""
        if self._con_override is not None:
            return self._con_override
        if self._holder is not None:
            if self._holder.con is None:
                return self._holder.__enter__()
            return self._holder.con
        raise RuntimeError("RequestContext has no connection")

    @property
    def has_eager_con(self) -> bool:
        """True when a connection override or an already-opened pool connection is present."""
        return self._con_override is not None or (self._holder is not None and self._holder.con is not None)

    def con_factory(self) -> Callable[[], duckdb.DuckDBPyConnection]:
        """Return a zero-arg callable that lazily acquires ``self.con``."""
        return lambda: self.con

    def con_or_factory(
        self, *, force_eager: bool = False
    ) -> duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection]:
        """Return ``self.con`` when already open or ``force_eager`` is True, else ``self.con_factory()``."""
        return self.con if (self.has_eager_con or force_eager) else self.con_factory()

    async def prewarm_con(self) -> None:
        """Acquire ``self.con`` in a worker thread so the loop never blocks.

        The first ``con`` access does a BLOCKING pool checkout; evaluating it
        as an argument inside an ``async def`` handler runs it ON the event
        loop (GCE 2026-10-02 wedge). The await is cancellation-guarded: a
        bare cancelled ``to_thread`` leaves the checkout running detached, so
        the request-context ``finally`` races it and strands a pool slot.
        """
        task = asyncio.create_task(asyncio.to_thread(lambda: self.con))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    @property
    def high_scale(self) -> HighScaleService | None:
        """The high-scale (ClickHouse) service registered for this request's service, if any.

        The registry import is deferred so handlers on the standard DuckDB path never load
        the ClickHouse stack; each handler still lazily imports its own ``hs_*`` function.
        """
        from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

        return get_high_scale_service_registry().resolve(self.service_id)

    def resolve_window(self, req: RangeWindowRequest) -> tuple[str | None, str | None]:
        """Resolve a request's scan window, then clamp it to the invite ceiling.

        Keyed path: a recognized ``range_token`` makes the SERVER resolve the
        window from (token, anchor), ignoring FE-supplied absolute bounds so a
        crafted body can't widen the scan. Legacy path clamps the supplied
        ``start_time``/``end_time``. The clamp always runs AFTER the resolve so
        it is the single enforcement point: an analyst can't widen past their
        invite by picking ``"30d"``.
        """
        from backend import config as svcconfig
        from backend.utils.time_window import is_valid_range_token, resolve_window

        if is_valid_range_token(req.range_token):
            earliest_log_at = svcconfig.get_status(self.source["name"]).get("earliest_log_at")
            resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
            return self.clamp(resolved_start, resolved_end)
        return self.clamp(req.start_time, req.end_time)

    def clamp(self, start: str | None, end: str | None) -> tuple[str | None, str | None]:
        """Clamp a request's start/end against this request's analyst window.

        Thin wrapper over :func:`backend.utils.remote_access.clamp_or_400`
        using the pre-resolved ``time_bounds`` + ``analyst_session`` so the
        20 standard analytics handlers don't each carry the ``tb`` dependency
        + the verbatim clamp call.
        """
        from backend.utils.remote_access import TimeBounds, clamp_or_400

        tb = self.time_bounds if self.time_bounds is not None else TimeBounds()
        return clamp_or_400(tb, start, end, analyst_session=self.analyst_session)


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
    #
    # Use the matched route TEMPLATE, not request.url.path — several routes
    # embed service_id in the path (e.g. /api/services/{service_id}/scoring/
    # dashboard), and the template is what keeps span-name cardinality
    # bounded when a span-metrics processor (e.g. Tempo's) turns span names
    # into per-route RED metrics. Depends() runs after routing, so the match
    # is always present outside of 404s.
    route = request.scope.get("route")
    route_template = getattr(route, "path", None) or request.url.path
    telemetry = RequestTelemetry(
        request_method=request.method,
        request_path=route_template,
    )
    telemetry.start_request()

    from backend.core.duckdb_pool import is_heavy_route

    holder = _ConnectionHolder(source, read_only=True, heavy=is_heavy_route(route_template), label=route_template)
    analyst_session = getattr(request.state, "analyst_session", None)
    # Resolve the analyst clamp window once here (same request.state source
    # get_analyst_time_bounds reads) so handlers can call ctx.clamp(...).
    from backend.utils.remote_access import get_analyst_time_bounds

    time_bounds = get_analyst_time_bounds(request)
    ctx = RequestContext(
        service_id=resolved_sid,
        source=source,
        telemetry=telemetry,
        analyst_session=analyst_session,
        read_only=True,
        time_bounds=time_bounds,
        _holder=holder,
    )
    # Park the context on request.state so downstream non-route
    # code (middleware, error handlers) can read it.
    request.state.ctx = ctx
    import sys

    try:
        yield ctx
    except HTTPException:
        telemetry.end_request(status_code=400)
        raise
    finally:
        import sys

        try:
            holder.__exit__(*sys.exc_info())
        except Exception:
            logger.debug("build_request_context: holder.__exit__ failed (ignored)", exc_info=True)
        telemetry.end_request()
    # Note on Live Query Monitor attribution: the attribution ContextVar is
    # set/restored by ``telemetry_middleware`` in backend/main.py, NOT here.
    # FastAPI runs sync deps and the route handler in separate
    # ``run_in_threadpool`` calls — each copies the parent context at
    # submit time, so a ContextVar set inside this dep doesn't propagate
    # to the route's threadpool call. Setting it at the middleware layer
    # (which runs in the event loop's context that both copies share)
    # makes it visible everywhere downstream.


def _resolve_source(service_id: str) -> dict:
    """Thin alias for :func:`backend.deps._resolve_source_or_400` — kept
    as a module-local name so existing test patches on
    ``backend.core.request_context._resolve_source`` continue to work."""
    return _resolve_source_or_400(service_id)


# ── Opaque AES-GCM session-reference token (formerly backend.core.session_token) ──

NONCE_BYTES: Final[int] = 12
KEY_BYTES: Final[int] = 32  # AES-256
GCM_TAG_BYTES: Final[int] = 16
_ENV_SECRET: Final[str] = "SESSION_TOKEN_SECRET"


class SessionTokenError(Exception):
    """Raised when a session token is malformed, tampered with, minted for a
    different service, or sealed under a key this process no longer holds (e.g.
    after a restart with the ephemeral default). Callers treat all of these the
    same: the reference is stale — ask the client to reload the session list."""


_key: bytes | None = None
_key_lock = threading.Lock()


def _resolve_key() -> bytes:
    """Lazily resolve (and cache) the 32-byte AES key."""
    global _key
    if _key is None:
        with _key_lock:
            if _key is None:
                env = os.getenv(_ENV_SECRET, "").strip()
                _key = hashlib.sha256(env.encode("utf-8")).digest() if env else secrets.token_bytes(KEY_BYTES)
    return _key


def _b64url_encode(data: bytes) -> str:
    """Base64URL without padding — URL/JSON-safe, RFC 4648 §5."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def seal_aes_gcm(key: bytes, payload: object, aad: bytes) -> str:
    """JSON-serialize and seal ``payload`` with AES-256-GCM + ``aad`` into a base64url token."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    nonce = os.urandom(NONCE_BYTES)
    ciphertext = AESGCM(key).encrypt(nonce, raw, aad)
    return _b64url_encode(nonce + ciphertext)


def unseal_aes_gcm(key: bytes, token: str, aad: bytes, error_cls: type[Exception]) -> object:
    """Inverse of :func:`seal_aes_gcm`, raising ``error_cls`` on any decode or AEAD error."""
    try:
        raw = _b64url_decode(token)
    except Exception as e:
        raise error_cls(f"base64url decode failed: {e}") from e
    if len(raw) < NONCE_BYTES + GCM_TAG_BYTES:
        raise error_cls(f"token too short: {len(raw)} bytes")
    nonce, ciphertext = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
    try:
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, aad)
    except InvalidTag as e:
        raise error_cls("AEAD verification failed") from e
    try:
        return json.loads(plaintext)
    except (ValueError, TypeError) as e:
        raise error_cls(f"payload decode failed: {e}") from e


def seal_session_token(
    ip: str,
    ja4: str | None,
    session_start: str,
    session_end: str,
    *,
    service_id: str,
) -> str:
    """Seal a session identity tuple into an opaque token bound to ``service_id``."""
    return seal_aes_gcm(_resolve_key(), [ip, ja4, session_start, session_end], service_id.encode("utf-8"))


def open_session_token(token: str, *, service_id: str) -> tuple[str, str | None, str, str]:
    """Inverse of :func:`seal_session_token`. Returns ``(ip, ja4, start, end)``."""
    decoded = unseal_aes_gcm(_resolve_key(), token, service_id.encode("utf-8"), SessionTokenError)
    if not isinstance(decoded, list) or len(decoded) != 4:
        raise SessionTokenError("payload decode failed: expected 4 elements")
    ip, ja4, start, end = decoded
    return str(ip), (str(ja4) if ja4 is not None else None), str(start), str(end)


def verify_session_token(token: str, service_id: str = "") -> tuple[str, str | None] | None:
    """Compatibility helper returning ``(ip, ja4)`` or ``None`` on invalid token."""
    try:
        ip, ja4, _start, _end = open_session_token(token, service_id=service_id)
        return ip, ja4
    except SessionTokenError:
        return None


_this_module = sys.modules[__name__]
sys.modules["backend.core.session_token"] = _this_module
_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "session_token", _this_module)

sys.modules["backend.deps"] = _this_module
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None:
    setattr(_backend_pkg, "deps", _this_module)
    if hasattr(_backend_pkg, "_MODULE_ALIASES"):
        _backend_pkg._MODULE_ALIASES["backend.deps"] = "backend.core.request_context"
