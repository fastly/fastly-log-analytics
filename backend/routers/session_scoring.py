"""Session-scoring admin router.

Three endpoints (mirroring backend/routers/provision.py conventions):

  POST /api/services/{service_id}/scoring/enable
       SSE-stream the enable_scoring orchestrator's status events.

  POST /api/services/{service_id}/scoring/disable
       SSE-stream the disable_scoring orchestrator's status events.

  GET  /api/services/{service_id}/scoring/status
       Return the customer's current scoring block ({enabled, scoring_service_id,
       scoring_domain, ...}) or {"enabled": false} if not yet wired.

The actual work lives in
[backend/provision/session_scoring_orchestrator.py](backend/provision/session_scoring_orchestrator.py);
this router just wraps it in the existing SSE event-streaming infrastructure
([backend/provision/orchestrator.py::run_with_events](backend/provision/orchestrator.py#L128))
so the dashboard can render a progress UI later."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query, Request

from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.core.session_token import (
    SessionTokenError,
    open_session_token,
    seal_session_token,
)
from backend.deps import ServiceId, require_admin
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.dashboard import (
    SessionDetailRequest,
    SessionDetailResponse,
    SessionsRequest,
    SessionsResponse,
)
from backend.models.session_scoring import (
    ScoringAnalyticsResponse,
    ScoringAuditListResponse,
    ScoringComplianceBreakdownResponse,
    ScoringConfigCompositeResponse,
    ScoringCurvesResponse,
    ScoringDashboardResponse,
    ScoringEnforceStatusCodeBody,
    ScoringEnforceStatusCodePutResponse,
    ScoringEnforceStatusCodeState,
    ScoringEnforceThresholdPutResponse,
    ScoringEnforceThresholdResponse,
    ScoringEvaluationResponse,
    ScoringExcludeRegexBody,
    ScoringExcludeRegexPutResponse,
    ScoringExcludeRegexState,
    ScoringExcludeRegexValidateResponse,
    ScoringHealthResponse,
    ScoringL2EnforceBody,
    ScoringL2EnforcePutResponse,
    ScoringL2EnforceState,
    ScoringLabelCreate,
    ScoringLabelDeleteResponse,
    ScoringLabelRow,
    ScoringLabelsListResponse,
    ScoringLabelUpdate,
    ScoringLatencyTimeseriesResponse,
    ScoringMatrixRestoreResponse,
    ScoringMatrixVersionsResponse,
    ScoringPerReasonResponse,
    ScoringRetrainResponse,
    ScoringRotateKeyResponse,
    ScoringScoreDistributionResponse,
    ScoringSessionEventsResponse,
    ScoringStatusResponse,
    ScoringThresholdBody,
    ScoringThresholdPreviewResponse,
    ScoringThresholdState,
    ScoringTokenBody,
    ScoringTopFlaggedResponse,
)
from backend.provision.session_scoring_setup import (
    SCORING_ENABLED_AT_KEY as _SCORING_ENABLED_AT_KEY,
)
from backend.repositories import sessions as repo
from backend.utils.auth import mask_ips_for, require_service_in_scope
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import clamp_or_400, get_analyst_time_bounds
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    not_found,
    query_errors,
    raise_internal,
    resolve_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response, stream_iac_reconcile
from backend.utils.telemetry import process_context_scope

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/services", tags=["session-scoring"], responses=DEFAULT_ERROR_RESPONSES)
# Fields in the per-service scoring config that must never reach the UI.
# AES key is never persisted in config (defensive); request_secret IS in
# the config so it must be stripped before any response that surfaces the
# scoring block (status endpoint, enable SSE done event, etc.).
_SECRET_KEYS = frozenset({"aes_key_hex", "request_secret"})


def _enforce_scoring_service_scope(request: Request | None, service_id: str) -> None:
    """Enforce analyst service-scope tenancy on session-scoring endpoints."""
    if request is not None and hasattr(request, "state"):
        analyst_session = getattr(request.state, "analyst_session", None)
        if analyst_session is None or hasattr(analyst_session, "service_ids"):
            require_service_in_scope(request, service_id)


# ── In-process TTL cache for analytics endpoints ─────────────────────────────
#
# The 3 summary endpoints (top-flagged, score-distribution, compliance-
# breakdown) each open a fresh DuckDB connection and pay 30-80ms of
# `_configure_fos` setup under `_fos_proxy_secret_lock` — and that lock
# is process-wide, so 3 concurrent requests serialize. Under cron-writer
# contention the same path can stall up to `max_wait=3` seconds. The
# admin page re-fires all three on every navigation.
#
# A 20-second TTL cache wipes most of that without changing user-facing
# semantics: refreshAll() invalidates React Query keys; we mirror that
# by exposing a bust() that the future "Refresh" button can call (the
# current admin Refresh button only invalidates the client cache, which
# is harmless overlap with this server cache — the next request just
# pulls a fresh server snapshot 20s later, which is fine).
import threading
import time as _time

_ANALYTICS_TTL_SEC = 20.0
# Bounded + lazy-reaped. Pre-migration this was a plain dict whose TTL
# was only checked on hit — entries lingered until /scoring/* was hit
# again with the same key. Keys are (endpoint, service_id, since_hours,
# ...) tuples that fan out across the admin UI's 8-card mount + diverse
# time windows, so the cardinality climbed unboundedly across hours.
# 1000 entries × ~50KB scoring payloads = ~50MB worst case — well below
# the dashboard cache but still worth bounding.
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_analytics_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=1000, ttl_seconds=_ANALYTICS_TTL_SEC)
# Live active-version lookups for the scoring Compute service hit the Fastly
# API; the status panel polls, so cache the result for 60s per service to avoid
# a Fastly round-trip on every refresh.
_scoring_svc_version_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
# Cache positive (`True`) preflight-pragma checks for 60s per service; `False`
# is intentionally not cached so clicking "Check Again" in the prerequisite
# modal immediately re-probes Fastly's generated VCL.
_scoring_pragma_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
# Global lock guards _analytics_cache + _inflight dict mutations only —
# the actual `producer()` call runs under a PER-KEY lock so concurrent
# misses on DIFFERENT keys (the dashboard's 8-card mount pattern) run
# in parallel instead of queueing through one mutex. The cache's own
# RLock is held INSIDE this outer lock by way of the bounded-cache
# implementation; RLock re-entry from the same thread is safe.
_analytics_cache_lock = threading.Lock()
_inflight: dict[tuple, threading.Lock] = {}


def _finalize_cached(value, *, is_cached: bool) -> object:
    """Return *value* with `_is_cached` set, gating `_debug_*` on
    `DEBUG_RESPONSES` so production responses don't leak SQL/URLs.

    Mirrors `backend.models.common.BaseResponse._strip_debug_when_disabled`
    so endpoints that return plain dicts get the same gating as endpoints
    that return Pydantic responses. `_is_cached` is always included — it
    isn't sensitive and downstream verification depends on it.
    """
    from backend.utils.telemetry import debug_envelope

    if not isinstance(value, dict):
        return value
    return debug_envelope(value, gated=True, is_cached=is_cached)


def _cached(key: tuple, producer):
    """Return cached value if fresh, else produce + store.

    In-flight collapse: concurrent callers on the SAME key serialize
    through a per-key lock so the underlying query runs once and the
    rest get the cached value when they acquire the lock. Concurrent
    callers on DIFFERENT keys (the dashboard mount fires 8 endpoints
    with 8 different keys) run in parallel — they only contend on the
    global lock during the brief cache-lookup + per-key-lock-handoff
    window.

    Telemetry: snapshots the request-scoped `_QUERIES` / `_CALLS`
    contextvars (from `backend.utils.telemetry`) before producer() and
    captures the suffix added during producer(). The captured slice is
    baked into the stored value under `_debug_queries` / `_debug_calls`
    so cache hits return the same telemetry that populated the cache,
    paired with `_is_cached: True` to flag the timings as historical.
    `_query_logs` (and anything called transitively from a producer)
    appends to the same shared contextvar via `get_queries()`.
    """
    from backend.utils.telemetry import _CALLS as _telemetry_calls
    from backend.utils.telemetry import get_queries

    with _analytics_cache_lock:
        # Capture `now` INSIDE the lock so the freshness check evaluates
        # against the lock-acquisition timestamp, not a stale value from
        # before lock contention drained. A hot key under load can have
        # 10-50ms of lock-wait — using a pre-lock `now` would flag a
        # still-fresh entry as expired and trigger an extra producer call.
        now = _time.monotonic()
        entry = _analytics_cache.get(key)
        if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
            return _finalize_cached(entry[1], is_cached=True)
        # Miss — claim the per-key lock under the global lock so two
        # concurrent misses on the same key don't both create new locks.
        key_lock = _inflight.get(key)
        if key_lock is None:
            key_lock = threading.Lock()
            _inflight[key] = key_lock

    # Hold the per-key lock only. The first miss runs producer(); the
    # second-through-Nth miss waits here, then sees the cached entry on
    # the re-check inside the lock and returns it without re-running.
    #
    # The try/finally wraps BOTH the cache-hit early-return and the
    # producer path so _inflight is always dropped, not only on actual
    # misses. Without it, repeated cache hits would leak one stuck Lock
    # object per distinct key — bounded by key cardinality but still a
    # slow accumulation across the TTL window.
    with key_lock:
        try:
            with _analytics_cache_lock:
                now = _time.monotonic()
                entry = _analytics_cache.get(key)
                if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
                    return _finalize_cached(entry[1], is_cached=True)
            # Snapshot telemetry length so we can attribute only producer()'s
            # additions — middleware-level call tracking already populated
            # the contextvars before we got here, and we don't want to bake
            # pre-producer entries into the cached value.
            queries = get_queries()
            calls_initial = _telemetry_calls.get() or []
            q_start = len(queries)
            c_start = len(calls_initial)
            # Actual producer call happens OUTSIDE the global lock so other
            # keys can be served while this one is computing. Wrap with a
            # SectionTimer entry so /scoring/dashboard's per-sub-handler
            # breakdown surfaces in _section_timings — the perf harness
            # previously saw empty section_timings for the composite and
            # couldn't attribute which of (scoring-health, scoring-curves,
            # threshold-preview, top-flagged, …) owned the wall.
            _section_t0 = _time.perf_counter()
            value = producer()
            _section_ms = round((_time.perf_counter() - _section_t0) * 1000, 2)
            queries_after = get_queries()
            calls_after = _telemetry_calls.get() or []
            # Defensive slice: if downstream code reset the contextvar mid-
            # producer (start_call_tracking, an explicit clear, etc.) the
            # suffix index could exceed the current length. Fall back to
            # the full current list rather than crash on a slice error.
            added_queries = list(queries_after[q_start:] if len(queries_after) >= q_start else queries_after)
            added_calls = list(calls_after[c_start:] if len(calls_after) >= c_start else calls_after)
            # Bake telemetry into the stored value so cache hits surface
            # the same shape. `_is_cached` is added at return time, not
            # stored, so the cached dict carries only stable data.
            if isinstance(value, dict):
                stored = dict(value)
                stored.setdefault("_debug_queries", added_queries)
                stored.setdefault("_debug_calls", added_calls)
                # Append a producer-scoped timing under the first key
                # element so the perf harness can rank sub-handlers.
                section_name = str(key[0]) if key else "scoring:unknown"
                existing_st = list(stored.get("_section_timings") or [])
                existing_st.append({"section": section_name, "time_ms": _section_ms})
                stored["_section_timings"] = existing_st
            else:
                stored = value
            with _analytics_cache_lock:
                # Re-capture now after producer() so the TTL clock starts
                # from when the value was actually computed, not from when
                # we entered _cached.
                _analytics_cache[key] = (_time.monotonic(), stored)
            return _finalize_cached(stored, is_cached=False)
        finally:
            with _analytics_cache_lock:
                # Drop the per-key lock entry — small saving but bounds the
                # _inflight dict growth across the long-running TTL window.
                _inflight.pop(key, None)


def _bust_analytics_cache(service_id: str | None = None) -> None:
    """Drop cached entries. Called by mutating endpoints (label flag/
    delete) so the admin page sees their effects on next fetch.

    Cache keys are tuples like ``(endpoint_name, service_id, since_hours, ...)``
    so the service_id lives at index 1, not 0. Earlier code compared
    ``k[0] == service_id`` which is always the endpoint name — the bust
    was a silent no-op and label mutations only invalidated via the 20s
    TTL. Match by membership instead of position so the dedup is correct
    regardless of future key shape changes."""
    with _analytics_cache_lock:
        if service_id is None:
            _analytics_cache.clear()
            return
        for k in list(_analytics_cache.keys()):
            if service_id in k:
                del _analytics_cache[k]


def _tenant_matrix_path(service_id: str):
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH

    return _MATRIX_PATH.with_name(f"{_MATRIX_PATH.stem}_{service_id}{_MATRIX_PATH.suffix}")


def _read_matrix_file(path, label: str) -> dict | None:
    try:
        if path.exists():
            with path.open() as f:
                m = json.load(f)
            if isinstance(m, dict) and m:
                return m
    except Exception:
        logger.debug("[_load_matrix] %s read failed", label, exc_info=True)
    return None


def _load_matrix(service_id: str) -> dict | None:
    """Load the trained L2 transition matrix for ``service_id``.

    Resolution order:
      1. Tenant-scoped ``compute/scorer/matrix_{sid}.json`` on local disk —
         the path retrain writes (audit-finding-005 fix) so each service
         has its own matrix and never shadows another.
      2. FOS-published copy (``iceberg/meta/scoring_matrix.json``) —
         pulled by enable_scoring and retrain. Lets any backend host see
         the same matrix the admin host deployed without per-host scp.
      3. ``compute/scorer/matrix.default.json`` (always shipped in the
         image) — empty transitions, AUC will read ~0.5 / "BELOW
         THRESHOLD" so the StatusPanel pushes the operator to train.

    Returns None only when all three sources fail.
    """
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH

    # 1. Tenant-scoped local matrix.
    tenant_path = _tenant_matrix_path(service_id)
    if m := _read_matrix_file(tenant_path, f"local {tenant_path.name}"):
        return m

    # 2. FOS-published matrix.
    try:
        from backend.state_sync import fetch_matrix_from_fos

        if m := fetch_matrix_from_fos(service_id):
            return m
    except Exception:
        logger.debug("[_load_matrix] FOS matrix fetch failed", exc_info=True)

    # 3. Default-empty matrix bundled with the image.
    return _read_matrix_file(_MATRIX_PATH.parent / "matrix.default.json", "default matrix.json")


from backend.repositories import session_scoring as _scoring_repo


def _isoformat_ts(rows: list[dict], *fields: str) -> list[dict]:
    """Stringify DuckDB datetime columns (``timestamp`` / ``hour``) via
    ``.isoformat()`` in place so the typed ``response_model`` can declare them
    as plain ``str``.

    This keeps the wire bytes byte-identical to the pre-typing output: the old
    untyped dict responses serialized these datetimes through FastAPI's
    ``jsonable_encoder``, whose datetime encoder is also ``.isoformat()``.
    Attaching a response_model would otherwise re-serialize tz-aware datetimes
    through Pydantic and flip the UTC suffix from ``+00:00`` to ``Z`` on
    UTC-configured hosts. Strings (test fixtures) and None pass through
    untouched."""
    for r in rows:
        for f in fields:
            v = r.get(f)
            if v is not None and hasattr(v, "isoformat"):
                r[f] = v.isoformat()
    return rows


_table_columns_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=128, ttl_seconds=30.0)


def _table_columns(service_id: str) -> set[str]:
    """Column-name set for a service's logs table, or empty set on error.

    Used to gate optional columns (e.g. the scorer-latency fields
    ``edge_score_rtt_us`` / ``edge_score_exec_us`` added 2026-06-17) so
    endpoints degrade gracefully on services that haven't been
    re-provisioned since the column landed — mirrors the
    ``if "tcp_rtt" not in cols`` guard in core/rollups/network_rtt.py.
    A missing/not-yet-ready view returns an empty set (→ omit the
    columns) rather than 500-ing the whole card.
    """
    import os as _os

    from backend.core.duckdb import _safe_table_name

    in_test = bool(_os.getenv("PYTEST_CURRENT_TEST"))
    if not in_test:
        cached = _table_columns_cache.get(service_id)
        if cached is not None:
            return cached

    table = _safe_table_name(service_id)
    try:
        rows = _scoring_repo.describe_table(service_id, table)
    except Exception:  # noqa: BLE001 — view not ready / table missing → no columns
        return set()
    cols = {str(r.get("column_name")) for r in rows if r.get("column_name")}
    if cols and not in_test:
        _table_columns_cache[service_id] = cols
    return cols


# Synthesize-missing-scoring-columns helper lives in the repository (the
# canonical home, since this module imports the repo and not vice-versa) so
# the per-endpoint queries here and the per-sid event hydration in
# fetch_session_events share ONE definition of the column contract. Aliased
# back into this module's namespace so the call sites below (and the tests)
# read as a local helper. See repo docstring for the why.
_scoring_source = _scoring_repo.scoring_source


_RECONSTRUCT_CACHE_TTL = 5.0
_reconstruct_labeled_sessions_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=64, ttl_seconds=_RECONSTRUCT_CACHE_TTL)


def _labels_fingerprint(labels: list[dict]) -> str:
    """Stable hash of (label.id, label.label) tuples for cache keying.

    The reconstruct call always receives the FULL label list for a
    service (every caller pulls it via ``_labels.list_labels(service_id)``
    first). So a content hash here scopes the cache hit to "same
    label set" — a mutation that changes either the label values
    or the underlying label rows busts the cache on the next call.
    """
    pairs = sorted((str(lbl.get("id", "")), str(lbl.get("label", ""))) for lbl in labels)
    return hashlib.sha256(json.dumps(pairs, separators=(",", ":")).encode()).hexdigest()


def _reconstruct_labeled_sessions(service_id: str, labels: list[dict]) -> list[tuple[dict, str]]:
    """Reconstruct labeled-session tuples for ``service_id``.

    Result cached for ``_RECONSTRUCT_CACHE_TTL`` seconds keyed on the
    label-set fingerprint. The scoring_dashboard composite calls this
    helper 3-4 times in a single request (one each for evaluation,
    evaluation_per_reason, scoring_curves) — without the cache those
    each pay the same ~370-499 ms DuckDB GROUP BY edge_sid scan.
    """
    fp = _labels_fingerprint(labels)
    cache_key = (service_id, fp)
    cached = _reconstruct_labeled_sessions_cache.get(cache_key)
    if cached is not None:
        return cached
    result = _scoring_repo.reconstruct_labeled_sessions(service_id, labels)
    _reconstruct_labeled_sessions_cache[cache_key] = result
    return result


def _safe_scoring_dict(scoring: dict | None) -> dict:
    return {k: v for k, v in (scoring or {}).items() if k not in _SECRET_KEYS}


def _require_fastly_token(
    service_id: str, body: ScoringTokenBody | None, err_msg: str = "Fastly API token required"
) -> str:
    resolved_token = resolve_fastly_token(service_id, body.token if body else "")
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": err_msg})
    return resolved_token


@router.post("/{service_id}/scoring/enable", dependencies=[Depends(require_admin)])
def scoring_enable(
    service_id: str = Path(..., description="Logging service ID to enable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Enable session scoring for the given logging service.

    Streams SSE status events while the orchestrator runs through:
    Compute service provisioning → Wasm deploy → VCL clone → backend +
    snippets + custom fields + format update → validate → activate."""
    resolved_token = _require_fastly_token(
        service_id, body, "Fastly API token required (pass in JSON body or set in service config)"
    )

    from backend import config as svcconfig

    existing_cfg = svcconfig.load_config(service_id) or {}
    if not (existing_cfg.get("scoring") or {}).get("enabled"):
        from backend.provision.fastly_api import service_has_scoring_pragmas

        pragmas_ready = service_has_scoring_pragmas(service_id, resolved_token)
        if pragmas_ready is False:

            def _missing_pragma_stream():
                yield json.dumps(
                    {
                        "type": "error",
                        "code": "missing_scoring_pragmas",
                        "message": (
                            f"Preflight request body preservation is not enabled on service {service_id}. "
                            "Please contact your Fastly account team to enable the service pragmas that "
                            "preserve unsent request bodies across restarts and omit request bodies when "
                            "backend requests are rewritten to GET/HEAD, then try again."
                        ),
                    }
                )

            return sse_response(_missing_pragma_stream())

    svcconfig.mutate_config(
        service_id, lambda c: c.__setitem__("scoring", {**(c.get("scoring") or {}), "enabled": True})
    )

    safe_scoring_state: dict = {}

    def _done_event() -> dict:
        cfg = svcconfig.load_config(service_id) or {}
        safe_scoring = _safe_scoring_dict(cfg.get("scoring"))
        safe_scoring_state.update(safe_scoring)
        return {
            "type": "done",
            "message": "Session scoring enabled.",
            "scoring": safe_scoring,
        }

    def _after_done() -> None:
        _record_scoring_toggle_audit(
            service_id,
            "scoring_enabled",
            details={
                "scoring_service_id": safe_scoring_state.get("scoring_service_id"),
                "matrix_version": safe_scoring_state.get("matrix_version"),
            },
        )

    def _error_event(e: Exception) -> dict:
        from backend.provision.session_scoring_setup import EntitlementError

        err_event: dict = {"type": "error", "message": str(e)}
        if isinstance(e, EntitlementError):
            err_event["code"] = e.code
            if e.link:
                err_event["link"] = e.link
        return err_event

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Enabling session scoring for {service_id}...",
        done_event=_done_event,
        after_done=_after_done,
        error_event=_error_event,
        log_label="scoring_enable",
        logger_obj=logger,
    )


def _record_scoring_toggle_audit(service_id: str, event_type: str, details: dict | None = None) -> None:
    from backend.core import metadata as metadata_db

    metadata_db.record_scoring_audit(service_id, event_type, details=details)
    metadata_db.record_audit(
        service_id=service_id,
        event_type=event_type,
        details=details or {},
        actor="operator",
    )


def _check_min_samples(n_good: int, n_bad: int, **extra: object) -> dict | None:
    if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
        return {
            "has_min_samples": False,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": n_good,
            "n_bad": n_bad,
            **extra,
        }
    return None


@router.post("/{service_id}/scoring/disable", dependencies=[Depends(require_admin)])
def scoring_disable(
    service_id: str = Path(..., description="Logging service ID to disable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Disable session scoring. Reverse of enable_scoring."""
    resolved_token = _require_fastly_token(service_id, body)

    from backend import config as svcconfig

    def _disable(c: dict) -> None:
        if "scoring" in c:
            c["scoring"]["enabled"] = False

    svcconfig.mutate_config(service_id, _disable)

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Disabling session scoring for {service_id}...",
        done_event={"type": "done", "message": "Session scoring disabled."},
        after_done=lambda: _record_scoring_toggle_audit(service_id, "scoring_disabled"),
        log_label="scoring_disable",
        logger_obj=logger,
    )


@router.get(
    "/{service_id}/scoring/analytics", response_model=ScoringAnalyticsResponse, response_model_exclude_unset=True
)
def scoring_analytics_composite(
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Composite of the analytics endpoints
    (top-flagged, score-distribution, compliance-breakdown,
    latency-timeseries, health, evaluation, evaluation/per-reason) into a
    single round-trip. Each is already individually cached via `_cached` so
    repeated composite calls within the 20s TTL collapse to dict
    lookups; the composite primarily saves the per-request HTTP +
    auth-middleware overhead that the 7-card admin_session_scoring
    page paid on cold mount.

    Granular endpoints unchanged — frontend swap to use the composite
    is a separate commit so the per-card endpoints remain a rollback
    target.

    Analyst-vs-admin gate: the direct ``/evaluation/per-reason`` endpoint
    is admin-only via ``_ANALYST_BLOCKED_SCORING_SUFFIXES``. The composite
    therefore mirrors the gate at the data layer — analysts get the four
    analyst-safe sub-results; admins get all six. Without this, the path-
    suffix block was bypassable by routing through ``/scoring/analytics``.
    """
    # Cast params to plain ints — FastAPI resolves Query() objects when
    # called via HTTP, but direct Python calls receive the Query wrapper.
    _enforce_scoring_service_scope(request, service_id)
    sh = int(since_hours)

    result = {
        "top_flagged": scoring_top_flagged(request=request, service_id=service_id, since_hours=sh, limit=200),
        "score_distribution": scoring_score_distribution(request=request, service_id=service_id, since_hours=sh),
        "compliance_breakdown": scoring_compliance_breakdown(request=request, service_id=service_id, since_hours=sh),
        "latency_timeseries": scoring_latency_timeseries(request=request, service_id=service_id, since_hours=sh),
        "health": scoring_health(request=request, service_id=service_id, since_hours=sh),
    }
    if not _is_analyst_request(request):
        result["evaluation"] = scoring_evaluation(request=request, service_id=service_id)
        result["evaluation_per_reason"] = scoring_evaluation_per_reason(service_id=service_id)
    return result


@router.get(
    "/{service_id}/scoring/config",
    response_model=ScoringConfigCompositeResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_config_composite(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Composite of the four token-free /scoring/* config endpoints."""
    return {
        "status": scoring_status(service_id),
        **_build_scoring_config_bundle(service_id),
    }


@router.get(
    "/{service_id}/scoring/status",
    response_model=ScoringStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Return the scoring block from the service's config, or
    {"enabled": false} if scoring was never enabled."""
    cfg = load_service_config(service_id)
    scoring = cfg.get("scoring")
    if not scoring or not scoring.get("enabled"):
        result: dict = {"enabled": False}
        token = stored_fastly_token(cfg)
        if token:
            ready = _scoring_pragma_cache.get(service_id)
            if ready is None:
                from backend.provision.fastly_api import service_has_scoring_pragmas

                try:
                    ready = service_has_scoring_pragmas(service_id, token)
                except Exception:  # pragma: no cover - defensive
                    logger.warning("[scoring-status] pragma check failed for %s", service_id, exc_info=True)
                    ready = None
                if ready is True:
                    _scoring_pragma_cache[service_id] = True
            if ready is not None:
                result["preflight_pragmas_ready"] = ready
        return result
    result = _safe_scoring_dict(scoring)

    # Best-effort: surface the scoring Compute service's LIVE active version +
    # the time it was activated, so operators can confirm exactly what's
    # deployed at the edge (the runbook's "redeploy timestamp"). Cached 60s per
    # service so the polled status panel doesn't hit Fastly every refresh, and
    # never breaks the status page if the Fastly call fails.
    scoring_svc = scoring.get("scoring_service_id")
    token = stored_fastly_token(cfg)
    if scoring_svc and token:
        info = _scoring_svc_version_cache.get(scoring_svc)
        if info is None:
            from backend.core.fastly.client import get_active_version_info

            try:
                info = get_active_version_info(scoring_svc, token)
            except Exception:  # pragma: no cover - defensive; helper already swallows RuntimeError
                logger.warning("[scoring-status] active-version lookup failed for %s", scoring_svc, exc_info=True)
                info = None
            if info is not None:
                _scoring_svc_version_cache[scoring_svc] = info
        if info:
            result["scoring_active_version"] = info.get("number")
            result["scoring_activated_at"] = info.get("updated_at")

    # Edge drift: does the scorer build the backend would deploy *now* differ
    # from what was last pushed to the edge? Compares the committed Wasm package
    # sha + VCL generator fingerprint against the stamps written at the last
    # enable. Local-only (no Fastly call) and best-effort — a hash failure must
    # never break the status panel. An absent stamp (service enabled before this
    # shipped) means "unknown" → no drift; the first redeploy stamps it.
    result["scorer_drift"] = False
    result["drift_detail"] = None
    # drift_known is False when this service was enabled BEFORE drift stamping
    # shipped (cfg.scoring has no deployed_package_sha / deployed_vcl_sha), so
    # drift reads as "unknown" rather than a confident "no drift". The UI uses
    # this to show a soft "redeploy once to baseline drift detection" hint
    # instead of silently showing nothing. The first redeploy stamps both.
    result["drift_known"] = bool(scoring.get("deployed_package_sha")) and bool(scoring.get("deployed_vcl_sha"))
    try:
        from backend.provision.session_scoring_orchestrator import shipped_scorer_identity

        shipped = shipped_scorer_identity(service_id)
        pkg_drift = (
            bool(scoring.get("deployed_package_sha"))
            and shipped["package_sha"] is not None
            and shipped["package_sha"] != scoring.get("deployed_package_sha")
        )
        vcl_drift = bool(scoring.get("deployed_vcl_sha")) and shipped["vcl_sha"] != scoring.get("deployed_vcl_sha")
        detail = "+".join(name for name, drifted in (("wasm", pkg_drift), ("vcl", vcl_drift)) if drifted)
        result["scorer_drift"] = bool(detail)
        result["drift_detail"] = detail or None
    except Exception:  # pragma: no cover - defensive; never break the status panel
        logger.warning("[scoring-status] drift check failed for %s", service_id, exc_info=True)
    return result


# ── Labels (good / bad / neutral session tags) ──────────────────────────────

# Fields on a scoring-label row that carry per-session PII or operator-only
# attribution. Analyst responses must NOT include these — labels are designed
# as ML training metadata, not as an analyst-visible per-session view. The
# admin path (no analyst_session on request.state) returns the full row.
_LABEL_PII_FIELDS = frozenset({"notes", "flagged_by", "sample_ip", "sample_ua", "sample_url"})


def _project_label_for_analyst(label: dict) -> dict:
    """Strip PII / operator-attribution fields from a scoring-label row."""
    return {k: v for k, v in label.items() if k not in _LABEL_PII_FIELDS}


def _is_analyst_request(request: Request) -> bool:
    """Canonical analyst-vs-admin detection via the middleware-stamped session."""
    return getattr(request.state, "analyst_session", None) is not None


def _strip_pii_fields_for_analyst(request: Request, body) -> None:
    """Neutralize operator-attribution / PII fields on a label write body.

    The read path already projects ``_LABEL_PII_FIELDS`` out of analyst
    responses, but the WRITE path (create / update) passed analyst-supplied
    ``notes`` / ``flagged_by`` / ``sample_*`` straight to persistence — so an
    analyst could spoof attribution or inject PII. For analyst requests we
    reset each of those fields back to its model default before save, so the
    operation still succeeds but the restricted fields can't be written.
    Admin (loopback) requests are unaffected.
    """
    if not _is_analyst_request(request):
        return
    fields = type(body).model_fields
    for name in _LABEL_PII_FIELDS:
        if name in fields:
            setattr(body, name, fields[name].default)


def _finalize_label_write(request: Request, service_id: str, row: dict) -> dict:
    # Bust analytics cache so the next /top-flagged shows the new badge.
    _bust_analytics_cache(service_id)
    return _project_label_for_analyst(row) if _is_analyst_request(request) else row


def _scoring_time_window(request: Request, since_hours: int) -> tuple[str, str | None]:
    """SQL timestamp predicate for the ``[now-since_hours, now]`` window,
    clamped to the analyst's allowed window.

    Returns ``(predicate_sql, cache_discriminator)``:

      * Admin (no analyst session) → relative ``now() - INTERVAL n HOUR``
        predicate; discriminator ``None`` so the admin/composite shares one
        cache entry as before.
      * Analyst → an absolute ``[start, end)`` predicate clamped to the invite
        window via ``clamp_or_400`` (raises 400 if the request falls entirely
        outside it), plus a discriminator so a scoped result can never read the
        admin / wider-window cache entry for the same key. The since_hours
        ``Query(le=168)`` bound still caps the absolute width.
    """
    since_hours = int(since_hours)
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return f"timestamp >= now() - INTERVAL {since_hours} HOUR", None
    tb = get_analyst_time_bounds(request)
    now = datetime.now(UTC)
    start_iso, end_iso = clamp_or_400(
        tb,
        (now - timedelta(hours=since_hours)).isoformat(),
        now.isoformat(),
        analyst_session=session,
    )
    pred = f"timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}'"
    return pred, f"{start_iso}|{end_iso}"


@router.get(
    "/{service_id}/scoring/labels",
    response_model=ScoringLabelsListResponse,
    response_model_exclude_unset=True,
)
def scoring_labels_list(
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
    limit: int = Query(default=500, ge=1, le=10000),
) -> dict:
    """Return all session labels for a service, most recent first."""
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    rows = _labels.list_labels(service_id, limit=limit)
    counts = _labels.counts_by_label(service_id)
    if _is_analyst_request(request):
        rows = [_project_label_for_analyst(r) for r in rows]
    return {"labels": rows, "counts": counts}


@router.post("/{service_id}/scoring/labels", response_model=ScoringLabelRow, response_model_exclude_unset=True)
def scoring_labels_create(
    body: ScoringLabelCreate,
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Create or update a label. Upserts on (service_id, sid)."""
    from backend.scoring import labels as _labels

    require_service_in_scope(request, service_id)
    _strip_pii_fields_for_analyst(request, body)
    # save_label() itself validates sid + label and raises ValueError
    # with the same messages; the try/except below converts that into
    # HTTPException(400). Keeping the validation in one place (the
    # CRUD module) means in-process callers — not just HTTP — get the
    # same protection.
    try:
        row = _labels.save_label(
            service_id,
            sid=body.sid.strip(),
            label=body.label.strip(),
            notes=body.notes,
            flagged_by=body.flagged_by,
            sample_ip=body.sample_ip,
            sample_ua=body.sample_ua,
            sample_url=body.sample_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_label", str(e)))
    return _finalize_label_write(request, service_id, row)


@router.patch(
    "/{service_id}/scoring/labels/{label_id}", response_model=ScoringLabelRow, response_model_exclude_unset=True
)
def scoring_labels_update(
    body: ScoringLabelUpdate,
    request: Request,
    service_id: str = Path(...),
    label_id: str = Path(...),
) -> dict:
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    _strip_pii_fields_for_analyst(request, body)
    try:
        row = _labels.update_label(
            service_id,
            label_id,
            label=body.label,
            notes=body.notes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_label", str(e)))
    if not row:
        raise HTTPException(status_code=404, detail=not_found("label_not_found"))
    return _finalize_label_write(request, service_id, row)


@router.delete(
    "/{service_id}/scoring/labels/{label_id}",
    response_model=ScoringLabelDeleteResponse,
    response_model_exclude_unset=True,
)
def scoring_labels_delete(
    request: Request,
    service_id: str = Path(...),
    label_id: str = Path(...),
) -> dict:
    """Delete a session label.

    Security: gated on the analyst's service-scope set so an analyst
    invited to ``svc-A`` cannot DELETE ``svc-B``'s labels via the
    cross-tenant pattern ``DELETE /api/services/svc-B/scoring/labels/...``.
    Admin (loopback) bypasses the check — ``analyst_allowed_services``
    returns ``None`` there. This is defense-in-depth for the admin gate
    documented in the audit (loopback-only, not token-gated).
    """
    from backend.scoring import labels as _labels

    require_service_in_scope(request, service_id)

    result = _labels.delete_label(service_id, label_id)
    _bust_analytics_cache(service_id)
    return result


# ── Summary queries (top-flagged, distributions) ────────────────────────────
#
# SQL execution lives in backend/repositories/session_scoring.py (imported
# above as _query_logs / _fetch_session_events / _reconstruct_labeled_sessions).
# Route handlers build SQL strings (table-name validated via
# _safe_table_name) and delegate execution + telemetry attribution there.


def _prepare_scoring_query(
    request: Request, service_id: str, since_hours: int
) -> tuple[str, set[str], str, str | None]:
    from backend.core.duckdb import _safe_table_name

    _enforce_scoring_service_scope(request, service_id)
    cols = _table_columns(service_id)
    src = _scoring_source(_safe_table_name(service_id), cols)
    ts_pred, win_disc = _scoring_time_window(request, since_hours)
    return src, cols, ts_pred, win_disc


def _cached_scoring_rows(
    cache_key: tuple,
    since_hours: int,
    fetch_fn,
    ts_field: str = "hour",
    **extra: object,
) -> dict:
    return _cached(
        cache_key,
        lambda: {"rows": _isoformat_ts(fetch_fn(), ts_field), "since_hours": since_hours, **extra},
    )


@router.get(
    "/{service_id}/scoring/top-flagged", response_model=ScoringTopFlaggedResponse, response_model_exclude_unset=True
)
def scoring_top_flagged(
    request: Request,
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    """Recent rows with non-null edge_score, sorted by score DESC. Feeds
    the admin page's "Top flagged sessions" table.

    Returns up to ``limit`` rows. Each row carries enough context for the
    admin to label it: sid, ip, ua, url, the three score fields, and
    cookie_compliance. Joining against the labels table is left to the
    UI (so it can show "currently labeled X" badges without paying the
    JOIN cost server-side)."""
    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    # ``ip`` is masked for analysts by the response middleware's key-name
    # pass (the column is literally ``ip``); ``ua`` / ``url`` are left intact
    # because analysts triage flagged sessions on them.
    return _cached_scoring_rows(
        ("top-flagged", service_id, since_hours, limit, win_disc),
        since_hours,
        lambda: _scoring_repo.top_flagged_rows(service_id, src, ts_pred, limit),
        ts_field="timestamp",
    )


@router.get(
    "/{service_id}/scoring/score-distribution",
    response_model=ScoringScoreDistributionResponse,
    response_model_exclude_unset=True,
)
def scoring_score_distribution(
    request: Request,
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Hourly buckets × score buckets (0, 25, 50, 75, 100). Returns a flat
    list of {hour, bucket, count} rows; the frontend pivots for the
    histogram."""
    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    return _cached_scoring_rows(
        ("score-distribution", service_id, since_hours, win_disc),
        since_hours,
        lambda: _scoring_repo.score_distribution_rows(service_id, src, ts_pred),
    )


@router.get(
    "/{service_id}/scoring/compliance-breakdown",
    response_model=ScoringComplianceBreakdownResponse,
    response_model_exclude_unset=True,
)
def scoring_compliance_breakdown(
    request: Request,
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Hourly count grouped by edge_cookie_compliance (ok / missing /
    tampered / expired / unknown)."""
    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    return _cached_scoring_rows(
        ("compliance-breakdown", service_id, since_hours, win_disc),
        since_hours,
        lambda: _scoring_repo.compliance_breakdown_rows(service_id, src, ts_pred),
    )


@router.get(
    "/{service_id}/scoring/latency-timeseries",
    response_model=ScoringLatencyTimeseriesResponse,
    response_model_exclude_unset=True,
)
def scoring_latency_timeseries(
    request: Request,
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Time-series of scorer latency + fail-open errors.

    One row per bucket with: ``scored_count``, ``fail_open_count``
    (compute-unavailable-* — the scorer's 401-unauthorized lands here as
    compute-unavailable-401 once VCL rewrites the non-200; there is no bare
    "unauthorized" log reason, EC-05), and — when the latency columns
    exist — ``rtt_p50/p95/p99_us`` (edge round-trip) and
    ``exec_p50/p95_us`` (Wasm exec). The fail-open series works on any
    enabled service; the latency series fills in once the service has been
    re-provisioned with the latency fields. Powers ScorerLatencyChart +
    ScorerErrorsChart on the session-scoring admin page. ``has_latency``
    tells the chart whether to render the latency axis.

    Bucket granularity adapts to the window: a 1-hour window bucketed
    hourly collapses to a single bar, so short windows (``since_hours <= 1``)
    bucket by MINUTE; everything wider stays hourly. The bucket column keeps
    the ``hour`` alias (it's a timestamp the chart plots on a date axis
    regardless of width), and ``granularity`` ("minute"|"hour") is returned
    so the UI can label the axis.
    """
    src, cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    has_rtt = "edge_score_rtt_us" in cols
    has_exec = "edge_score_exec_us" in cols

    # A 1h window at hourly granularity is one bar; drop to minute buckets for
    # short windows so the chart is actually a time-series. Whitelisted literal
    # (never interpolated from user input) so it's safe in the f-string SQL.
    bucket = "minute" if int(since_hours) <= 1 else "hour"

    return _cached_scoring_rows(
        ("latency-timeseries", service_id, since_hours, win_disc),
        since_hours,
        lambda: _scoring_repo.latency_timeseries_rows(service_id, src, ts_pred, bucket, has_rtt, has_exec),
        has_latency=has_rtt or has_exec,
        granularity=bucket,
    )


@router.get("/{service_id}/scoring/health", response_model=ScoringHealthResponse, response_model_exclude_unset=True)
def scoring_health(
    request: Request,
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """High-level scoring health snapshot for the admin dashboard.

    Returns a single object with the metrics the operator wants at a
    glance: how often scoring fires vs how much edge traffic flows, what
    the score distribution looks like as summary stats (mean / p50 / p95),
    a top-N breakdown of the comma-separated edge_score_reason values
    (so 'cookie-missing' vs 'impossibly-fast' vs 'rare-transition' is
    visible without opening the Raw Logs table), and a fail-open error
    count (rows where the scorer's deliver-stage subfields didn't land —
    typically a Compute timeout or auth mismatch).
    """
    # Scorer-latency columns are optional (present only after a service is
    # re-provisioned post-2026-06-17). Detect them so the query degrades to
    # the pre-latency shape on older services instead of a binder error.
    src, cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    has_rtt = "edge_score_rtt_us" in cols
    has_exec = "edge_score_exec_us" in cols

    def _produce() -> dict:
        row = _scoring_repo.health_snapshot_row(service_id, src, ts_pred, has_rtt, has_exec)
        total = int(row.get("total_edge_rows") or 0)
        scored = int(row.get("scored_rows") or 0)
        fire_rate_pct = (scored / total * 100.0) if total else 0.0

        l2_evaluated = int(row.get("l2_evaluated") or 0)
        l2_high = int(row.get("l2_high_count") or 0)
        l2_high_pct = (l2_high / l2_evaluated * 100.0) if l2_evaluated else 0.0
        # Heuristic staleness band: research §7.5 calls it 3× the
        # baseline. We don't have a stored baseline yet, so flag any
        # window where MORE THAN 25% of L2-evaluated requests scored
        # high — that's the "the matrix doesn't know what normal looks
        # like anymore" threshold. UI surfaces this as a yellow chip
        # with a "Retrain now?" hint.
        matrix_stale = l2_evaluated >= 100 and l2_high_pct > 25.0

        # Scorer-latency snapshot (µs). Present only when the columns exist
        # AND at least one row carries a value; ``available=False`` tells
        # the UI to hide the tile (older service, or no scored traffic yet).
        latency = {
            "available": has_rtt or has_exec,
            **{
                k: (int(v) if (v := row.get(k)) is not None else None)
                for k in ("rtt_p50_us", "rtt_p95_us", "rtt_p99_us", "rtt_max_us", "exec_p50_us", "exec_p95_us")
            },
        }

        # L2-enforcement opt-in + deployment-age readiness gauge. Admin-only —
        # it reads the scoring_config ConfigStore (and the consuming card is
        # admin); analysts get None so the analyst-safe health composite stays a
        # pure DuckDB read. Best-effort + separately cached, so it can never
        # break this response. "What L2 would flag" is already surfaced above as
        # matrix_staleness.l2_high_pct — the UI reuses it rather than recomputing.
        l2_enforce = None
        if not _is_analyst_request(request):
            l2_enforce = l2_enforce_readiness_block(service_id)

        scorer_errors = int(row.get("scorer_errors") or 0)
        return {
            "since_hours": since_hours,
            "total_edge_rows": total,
            "scored_rows": scored,
            "fire_rate_pct": round(fire_rate_pct, 2),
            "distinct_sids": int(row.get("distinct_sids") or 0),
            "avg_score": float(row.get("avg_score") or 0),
            "p50_score": float(row.get("p50_score") or 0),
            "p95_score": float(row.get("p95_score") or 0),
            "max_score": int(row.get("max_score") or 0),
            "scorer_errors": scorer_errors,
            # SRE-15: traffic-normalized fail-open rate. The bare count above
            # rises with request volume, so a fixed count threshold cries wolf
            # under load and stays silent on a low-traffic spike; the rate is
            # the stable spike-arm the UI tones on.
            "fail_open_rate_pct": round(scorer_errors / total * 100.0, 2) if total else 0.0,
            "top_reasons": row.get("top_reasons") or [],
            "fail_open_breakdown": row.get("fail_open_breakdown") or [],
            "latency": latency,
            "matrix_staleness": {
                "l2_evaluated": l2_evaluated,
                "l2_high_count": l2_high,
                "l2_high_pct": round(l2_high_pct, 2),
                "is_stale": matrix_stale,
                "threshold_pct": 25.0,
            },
            "l2_enforce": l2_enforce,
        }

    return _cached(("scoring-health", service_id, since_hours, win_disc), _produce)


# ── Matrix quality (ROC-AUC against accumulated labels) ─────────────────────


# Below this many of EACH class (good / bad), AUC is too noisy to be a
# useful signal — surface a "need more labels" CTA instead so the operator
# isn't tempted to act on a 0.5 ± wildly-bouncing number from sub-3
# samples.
_MIN_LABELS_PER_CLASS = 3


def _label_counts(service_id: str) -> tuple[int, int, int]:
    from backend.scoring import labels as _labels

    counts = _labels.counts_by_label(service_id)
    return counts.get("good", 0), counts.get("bad", 0), counts.get("neutral", 0)


@router.get(
    "/{service_id}/scoring/evaluation", response_model=ScoringEvaluationResponse, response_model_exclude_unset=True
)
def scoring_evaluation(
    service_id: str = Path(...),
    request: Request = None,  # type: ignore[assignment]
) -> dict:
    """Compute the live matrix's ROC-AUC against the operator's accumulated
    good/bad labels and return the result for the StatusPanel.

    Below the per-class minimum (3 each) the endpoint reports
    ``has_min_samples: false`` and the StatusPanel renders a CTA pushing
    the operator to label a few more sessions — sub-3 AUC bounces between
    0 and 1 on a single label flip and would erode trust in the metric.

    Wiring:
      1. Pull all labels for the service from SQLite (cheap; <10k rows).
      2. Reconstruct each labeled sid's event sequence from DuckDB.
      3. Load the trained matrix JSON.
      4. Run evaluate() — AUC via Mann-Whitney U, no scipy dependency.

    Cached for 20s under the existing _cached pattern; the key includes
    the label count so a fresh label naturally invalidates the cache
    (also, _bust_analytics_cache fires on label POST/PATCH/DELETE).
    """
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, n_neutral = _label_counts(service_id)

    # The cache key includes the label count so a new label invalidates
    # the previous snapshot even if the explicit cache bust were to miss.
    cache_key = ("scoring-evaluation", service_id, n_good, n_bad, n_neutral)

    def _produce() -> dict:
        from backend.config import load_config

        cfg = load_config(service_id) or {}
        matrix_version = (cfg.get("scoring") or {}).get("matrix_version") or "unknown"

        min_check = _check_min_samples(n_good, n_bad, n_neutral=n_neutral, matrix_version=matrix_version)
        if min_check is not None:
            return min_check

        matrix = _load_matrix(service_id)
        if matrix is None:
            return {
                "has_min_samples": True,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
                "n_neutral": n_neutral,
                "matrix_version": matrix_version,
                "error": "Trained matrix is missing (no local compute/scorer/matrix_<service_id>.json "
                "and no FOS copy). Run scripts/scoring/train.py to produce one.",
            }

        label_rows = _labels.list_labels(service_id)
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        # If most labeled sids haven't landed in DuckDB yet (fresh label
        # → ingest lag), evaluate against what we have but flag the gap.
        n_reconstructed = len(labeled_sessions)
        from backend.scoring.evaluate import DEFAULT_MIN_AUC, evaluate_from_persisted_scores

        # Use persisted edge_score (L1+L2+compliance combined, what the
        # live scorer actually returned) rather than recomputing L2 from
        # events. Without this, single-URL bot probes ALWAYS score 0
        # (no transitions), which inverts AUC against any matrix the
        # bots' cookie-missing flag would have caught at the edge.
        result = evaluate_from_persisted_scores(labeled_sessions)
        # Prefer the matrix file's own `version` over whatever's in the
        # cfg — the cfg version tracks what's DEPLOYED to Wasm, the
        # matrix file tracks what was last trained. AUC is computed
        # against the trained matrix, so its version is the relevant one.
        effective_version = matrix.get("version") or matrix_version
        return {
            "has_min_samples": True,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": result.n_good,
            "n_bad": result.n_bad,
            "n_neutral": n_neutral,
            "n_reconstructed": n_reconstructed,
            "n_labels_total": len(label_rows),
            "auc": round(float(result.auc), 4),
            "passed": bool(result.passed),
            "threshold": float(result.pass_threshold),
            "default_min_auc": float(DEFAULT_MIN_AUC),
            "matrix_version": effective_version,
        }

    return _cached(cache_key, _produce)


# ── ROC + PR curves against accumulated labels ──────────────────────────────


@router.get("/{service_id}/scoring/curves", response_model=ScoringCurvesResponse, response_model_exclude_unset=True)
def scoring_curves(
    service_id: str = Path(...),
    request: Request = None,  # type: ignore[assignment]
) -> dict:
    """ROC + PR curve points for the operator's labeled sessions.

    Walks every integer threshold 0..100 and computes:
      ROC: (false_positive_rate, true_positive_rate) at that cutoff
      PR:  (recall, precision) at that cutoff

    Plus the scalar summaries (AUC = area under ROC; AP = average
    precision = area under PR). Both areas use the trapezoidal rule
    on the sorted threshold sweep.

    Returns has_min_samples=false when either class has <3 labels —
    same gate as /scoring/evaluation — so the UI renders the "label
    more sessions" CTA instead of a noisy curve.

    Cached under the existing ``_cached`` TTL with a label-count-aware
    key so a fresh label POST invalidates correctly via the regular
    cache-miss path (n_good/n_bad change → new key).
    """
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, _ = _label_counts(service_id)

    def _produce() -> dict:
        min_check = _check_min_samples(n_good, n_bad)
        if min_check is not None:
            return min_check

        label_rows = _labels.list_labels(service_id)
        # Reconstruct labeled sessions and extract their max persisted scores.
        # Same path the AUC endpoint uses, so the curve is consistent with
        # the headline AUC number.
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        scored = [
            (int(score), label)
            for session, label in labeled_sessions
            if label in ("good", "bad") and (score := session.get("max_edge_score")) is not None
        ]

        if not scored:
            return {
                "has_min_samples": False,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
                "note": "labels exist but none of their sids have landed in DuckDB yet",
            }

        total_pos = sum(1 for _, lbl in scored if lbl == "bad")
        total_neg = sum(1 for _, lbl in scored if lbl == "good")

        roc: list[dict] = []
        pr: list[dict] = []
        # Walk thresholds from 100 down to 0 so the ROC curve traces from
        # origin (0,0) toward (1,1) as we lower the cutoff. Each integer
        # threshold is a separate operating point; sub-integer resolution
        # isn't useful since the live scorer emits int 0-100.
        for t in range(100, -1, -1):
            tp = sum(1 for s, lbl in scored if lbl == "bad" and s >= t)
            fp = sum(1 for s, lbl in scored if lbl == "good" and s >= t)
            tpr = tp / total_pos if total_pos else 0.0
            fpr = fp / total_neg if total_neg else 0.0
            precision = (tp / (tp + fp)) if (tp + fp) else 1.0  # convention: empty flagged set → precision 1
            roc.append({"threshold": t, "fpr": round(fpr, 4), "tpr": round(tpr, 4)})
            pr.append({"threshold": t, "precision": round(precision, 4), "recall": round(tpr, 4)})

        # AUC via trapezoidal integration over the ROC points (sorted by
        # fpr ascending). AP same idea over PR.
        def _trapz(points: list[tuple[float, float]]) -> float:
            pts = sorted(points, key=lambda p: p[0])
            return sum((pts[i][0] - pts[i - 1][0]) * (pts[i - 1][1] + pts[i][1]) / 2.0 for i in range(1, len(pts)))

        auc = _trapz([(p["fpr"], p["tpr"]) for p in roc])
        ap = _trapz([(p["recall"], p["precision"]) for p in pr])

        return {
            "has_min_samples": True,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": total_neg,
            "n_bad": total_pos,
            "n_labels_total": len(label_rows),
            "auc": round(float(auc), 4),
            "average_precision": round(float(ap), 4),
            "roc": roc,
            "pr": pr,
        }

    return _cached(("scoring-curves", service_id, n_good, n_bad), _produce)


# ── Threshold preview (counterfactual: at threshold X, what flips?) ─────────


@router.get(
    "/{service_id}/scoring/threshold-preview",
    response_model=ScoringThresholdPreviewResponse,
    response_model_exclude_unset=True,
)
def scoring_threshold_preview(
    request: Request,
    service_id: str = Path(...),
    threshold: int = Query(default=75, ge=0, le=100),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Preview what happens at a given enforcement threshold.

    For the last ``since_hours`` of edge traffic, count:
      - total scored requests
      - how many would be flagged (edge_score >= threshold)
      - of those, how many are labeled good / bad / unlabeled
      - same breakdown for the un-flagged tail

    This is the underlying data for the operator-facing slider: drag
    threshold up → fewer flags but you start missing labeled-bad
    sessions; drag down → catches more bad but also flags some labeled-
    good (false positives). The 2x2 confusion matrix readout is enough
    to eyeball the right cutoff.

    Cached 30s under the existing ``_cached`` pattern; the cache key
    includes the threshold so dragging the slider re-fetches.
    """
    from backend.scoring import labels as _labels

    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    threshold_int = int(threshold)

    def _produce() -> dict:
        # Build the label index in Python — small (≤10k labels) and
        # avoids a JOIN against SQLite (which would need ATTACH overhead).
        label_rows = _labels.list_labels(service_id)
        sid_to_label = {row["sid"]: row["label"] for row in label_rows if row.get("sid")}

        # 009: push the bucketing into SQL so a service with millions of
        # distinct edge_sids in the window can't OOM the backend. The
        # old shape materialised one Python dict per sid before doing
        # any bucketing; for a high-traffic service that's a few
        # gigabytes of dicts.
        #
        # Single CTE pass: sid_scores enumerates one row per edge_sid;
        # a labels-VALUES clause carries the (sid, label) pairs; a
        # LEFT JOIN tags scores with their label (NULL when unlabeled).
        # One SELECT then emits the six bucket counts. The previous
        # two-query shape scanned the base table twice (once for the
        # aggregate, once for the labeled-sid filter); a single CTE +
        # hash-join over the same scan halves the cold per-call wait
        # (~523 ms p95 → ~280 ms).
        #
        # When no labels exist yet, the LEFT JOIN simplifies to a
        # no-op and the SELECT degenerates to the aggregate-only path
        # — handled by the separate branch below.
        labeled_pairs = [(s, lbl) for s, lbl in sid_to_label.items() if s]
        counts_row = _scoring_repo.threshold_preview_counts(service_id, src, ts_pred, threshold_int, labeled_pairs)
        total = int(counts_row.get("total") or 0)
        flagged_total = int(counts_row.get("flagged_total") or 0)
        passed_total = total - flagged_total
        flagged_good, flagged_bad, passed_good, passed_bad = (
            (int(counts_row.get(k) or 0) for k in ("flagged_good", "flagged_bad", "passed_good", "passed_bad"))
            if labeled_pairs
            else (0, 0, 0, 0)
        )

        # Unlabeled buckets fall out by subtraction — every flagged/passed
        # sid that didn't match a label-row is unlabeled.
        flagged_unlabeled = max(0, flagged_total - (flagged_good + flagged_bad))
        passed_unlabeled = max(0, passed_total - (passed_good + passed_bad))
        flagged = flagged_good + flagged_bad + flagged_unlabeled
        # Operator-friendly precision/recall against the labels we DO
        # have. precision = bad-among-flagged / flagged-labeled-total.
        # recall = bad-flagged / bad-total. Both are None when the
        # denominator is zero (which happens early in label collection).
        flagged_labeled = flagged_good + flagged_bad
        all_labeled_bad = flagged_bad + passed_bad
        precision = (flagged_bad / flagged_labeled) if flagged_labeled else None
        recall = (flagged_bad / all_labeled_bad) if all_labeled_bad else None

        return {
            "threshold": threshold_int,
            "since_hours": since_hours,
            "total_scored_sessions": total,
            "flagged": {"total": flagged, "good": flagged_good, "bad": flagged_bad, "unlabeled": flagged_unlabeled},
            "passed": {"good": passed_good, "bad": passed_bad, "unlabeled": passed_unlabeled},
            "precision": round(precision, 4) if precision is not None else None,
            "recall": round(recall, 4) if recall is not None else None,
        }

    # Label-count-aware cache key so a new label invalidates correctly.
    n_good, n_bad, _ = _label_counts(service_id)
    return _cached(("threshold-preview", service_id, threshold_int, since_hours, n_good + n_bad, win_disc), _produce)


# ── Admin / training endpoints (folded from session_scoring_admin) ──────────

_L2_ENABLED_AT_KEY = "l2_enabled_at"
_L2_ENFORCE_ENABLED_KEY = "l2_enforce_enabled"
_fetch_session_events = _scoring_repo.fetch_session_events

# ── module-private constants ──────────────────────────────────────────────────

_ENFORCE_THRESHOLD_KEY = "enforce_threshold"

# Process-local TTL cache for the scoring-config ConfigStore reads. The
# ``/scoring/enforce-threshold`` GET fires on every /admin/session-scoring
# mount and costs ~200-460 ms per call (Fastly ConfigStore round-trip) per
# the perf audit. 30 s TTL keeps repeated panel-refreshes / tab-toggles
# cheap without making the operator wait long after their own PUT — and
# the PUT counterpart busts the cache anyway so write-then-read is instant.
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_ENFORCE_THRESHOLD_CACHE_TTL = 300.0
_enforce_threshold_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)

# ── L2 enforcement opt-in (mirrors the Rust scorer's scoring_config gate) ─────
#
# L2's contribution to the *enforced* combined score is gated by an explicit
# operator opt-in (l2_enforce_enabled) and fades in over _L2_RAMP_DAYS from the
# l2_enabled_at anchor — NOT from deployment age. The 7-day deployment age is now
# only an advisory readiness gauge. These mirror the Rust constants in
# compute/scorer/src/scorer.rs (L2_RAMP_DAYS) + the day-7 readiness band.
_L2_RAMP_DAYS = 3.0
_L2_READINESS_DAYS = 7.0
# Process-local TTL cache for the L2-enforce ConfigStore reads (same pattern +
# TTL as the enforce-threshold cache). Shared by the GET endpoint AND the
# scoring_health readiness block, so a cold admin mount makes at most one Fastly
# round-trip per TTL regardless of which path warms it; the PUT busts it for an
# accurate read-after-write.
_l2_enforce_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)


def _l2_parse_epoch(raw: str | None) -> int | None:
    """Parse a UNIX-epoch-seconds ConfigStore value; None when absent/non-numeric."""
    if not raw:
        return None
    s = str(raw).strip()
    return int(s) if s.isdigit() else None


def _l2_enforce_unavailable() -> dict:
    """The L2-enforce/readiness block when it can't be read (scoring not enabled,
    no token, or an unreachable ConfigStore). All-off / not-ready so the UI
    degrades to a safe inert state instead of erroring."""
    return {
        "available": False,
        "enabled": False,
        "l2_enabled_at": None,
        "days_since_optin": None,
        "ramp_progress": 0.0,
        "fully_ramped": False,
        "warmup_days_remaining": None,
        "scoring_enabled_at": None,
        "deployment_age_days": None,
        "ready": False,
        "ramp_days": _L2_RAMP_DAYS,
        "readiness_days": _L2_READINESS_DAYS,
    }


def _build_l2_enforce_block(
    *,
    scoring_enabled_at_raw: str | None,
    l2_enforce_raw: str | None,
    l2_enabled_at_raw: str | None,
    now_epoch: int,
) -> dict:
    """Pure builder for the L2-enforce/readiness block from the three raw
    ConfigStore values + a fixed ``now`` (epoch seconds). Mirrors the Rust
    scorer's gate: ``enabled`` iff ``l2_enforce_enabled`` trims to "1"; the
    fade-in age comes from ``l2_enabled_at`` (the opt-in anchor), NOT deployment
    age; ``scoring_enabled_at`` drives only the advisory readiness gauge. Kept
    side-effect-free so the ramp math is unit-testable directly."""
    enabled = (l2_enforce_raw or "").strip() == "1"
    scoring_enabled_at = _l2_parse_epoch(scoring_enabled_at_raw)
    l2_enabled_at = _l2_parse_epoch(l2_enabled_at_raw)

    # Advisory deployment-age readiness gauge (never an actuator).
    deployment_age_days: float | None = None
    ready = False
    if scoring_enabled_at is not None:
        deployment_age_days = max(0.0, (now_epoch - scoring_enabled_at) / 86_400.0)
        ready = deployment_age_days >= _L2_READINESS_DAYS

    # Opt-in fade-in — only meaningful once the anchor exists. A future anchor
    # (clock skew) clamps to 0.0, mirroring the scorer's load_l2_days_since_optin.
    days_since_optin: float | None = None
    ramp_progress = 0.0
    fully_ramped = False
    warmup_days_remaining: float | None = None
    if l2_enabled_at is not None:
        days = 0.0 if now_epoch <= l2_enabled_at else (now_epoch - l2_enabled_at) / 86_400.0
        days_since_optin = days
        ramp_progress = max(0.0, min(1.0, days / _L2_RAMP_DAYS))
        fully_ramped = days >= _L2_RAMP_DAYS
        warmup_days_remaining = max(0.0, _L2_RAMP_DAYS - days)

    return {
        "available": True,
        "enabled": enabled,
        "l2_enabled_at": l2_enabled_at,
        "days_since_optin": round(days_since_optin, 3) if days_since_optin is not None else None,
        "ramp_progress": round(ramp_progress, 4),
        "fully_ramped": fully_ramped,
        "warmup_days_remaining": round(warmup_days_remaining, 3) if warmup_days_remaining is not None else None,
        "scoring_enabled_at": scoring_enabled_at,
        "deployment_age_days": round(deployment_age_days, 3) if deployment_age_days is not None else None,
        "ready": ready,
        "ramp_days": _L2_RAMP_DAYS,
        "readiness_days": _L2_READINESS_DAYS,
    }


def _l2_get_item(config_store_id: str, key: str, token: str) -> str:
    """GET one scoring_config item value; "" when the key is absent (404).
    Re-raises any other RuntimeError so a real Fastly outage surfaces (the GET
    endpoint maps it to 502; the best-effort readiness path swallows it)."""
    from backend.core.fastly.client import fastly

    try:
        item = fastly("GET", f"/resources/stores/config/{config_store_id}/item/{key}", token=token)
        return (item or {}).get("item_value", "") or ""
    except RuntimeError as exc:
        if "404" in str(exc):
            return ""
        raise


def _read_l2_enforce_state(service_id: str, config_store_id: str, token: str) -> dict:
    """Cached read+build of the L2-enforce/readiness block (mirrors the
    enforce-threshold GET cache). Assumes ``config_store_id`` + ``token`` are
    already resolved. Caches only successful reads under ``(service_id, store)``."""
    import datetime as _dt

    cache_key = (service_id, config_store_id)
    cached = _l2_enforce_cache.get(cache_key)
    if cached is not None:
        return cached
    block = _build_l2_enforce_block(
        scoring_enabled_at_raw=_l2_get_item(config_store_id, _SCORING_ENABLED_AT_KEY, token),
        l2_enforce_raw=_l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, token),
        l2_enabled_at_raw=_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, token),
        now_epoch=int(_dt.datetime.now(_dt.UTC).timestamp()),
    )
    _l2_enforce_cache[cache_key] = block
    return block


def _load_scoring_cfg(service_id: str) -> tuple[dict, dict]:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    return cfg, (cfg.get("scoring") or {})


def _require_confirm(confirm: bool, detail_msg: str) -> None:
    if not confirm:
        raise HTTPException(status_code=400, detail={"error": detail_msg})


def _push_matrix_to_kv_if_configured(service_id: str, cfg: dict | None, matrix_dict: dict | None = None) -> bool:
    matrix_store_id = ((cfg or {}).get("scoring") or {}).get("scoring_matrix_store_id", "")
    resolved_token = resolve_fastly_token(service_id)
    if not matrix_store_id or not resolved_token:
        if not matrix_store_id and matrix_dict is not None:
            logger.info("retrain: no scoring_matrix store id in config — KV push skipped (re-enable to provision it)")
        return False
    try:
        if matrix_dict is not None:
            from backend.core.fastly.client import fastly_raw
            from backend.provision.session_scoring_setup import MATRIX_KEY_NAME
            from backend.scoring.matrix import serialize_kv

            fastly_raw(
                "PUT",
                f"/resources/stores/kv/{matrix_store_id}/keys/{MATRIX_KEY_NAME}",
                serialize_kv(matrix_dict),
                content_type="application/octet-stream",
                token=resolved_token,
            )
        else:
            from backend.provision.session_scoring_orchestrator import _write_matrix_to_kv

            _write_matrix_to_kv(matrix_store_id, service_id, resolved_token)
        return True
    except Exception as exc:
        logger.warning(f"Could not write matrix to scoring_matrix KV: {exc}")
        return False


def l2_enforce_readiness_block(service_id: str) -> dict:
    """Best-effort L2-enforce/readiness block for the scoring_health composite.
    Degrades to the unavailable block on any missing-config / no-token / Fastly
    error so it can never break the (DuckDB-driven) health response. Called from
    ``scoring_health`` for admin requests only."""
    _cfg, scoring = _load_scoring_cfg(service_id)
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        return _l2_enforce_unavailable()
    token = resolve_fastly_token(service_id)
    if not token:
        return _l2_enforce_unavailable()
    try:
        return _read_l2_enforce_state(service_id, config_store_id, token)
    except Exception:  # noqa: BLE001 — advisory block must never break health
        return _l2_enforce_unavailable()


@router.post(
    "/{service_id}/scoring/retrain",
    response_model=ScoringRetrainResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_retrain(
    service_id: ServiceId,
    since_days: int = Query(default=7, ge=1, le=90, description="Window of DuckDB traffic to train on"),
    version: str | None = Query(default=None, description="Override matrix version label; defaults to today's date"),
) -> dict:
    """Build a fresh transition matrix from the last N days of DuckDB
    traffic, save it to ``compute/scorer/matrix.json``, publish to FOS,
    and evaluate AUC against the operator's accumulated labels.

    Synchronous — for a 7-day window with ~10k sessions the whole pipeline
    runs in <30s. The endpoint returns the new matrix metadata + AUC so
    the UI can show "matrix moved from 0.62 → 0.91 after retrain". The new
    matrix is pushed straight into the scoring_matrix KV Store, so the LIVE
    edge scorer picks it up as Compute instances recycle — no Wasm rebuild
    (the matrix is read from KV at runtime, not embedded).

    Pipeline:
      1. extract_traces from DuckDB → in-memory sessions
      2. build_matrix → TransitionMatrix
      3. evaluate AUC against labels (if >=3 each class)
      4. Save matrix.json to disk + publish to FOS
      4b. Write the matrix to the scoring_matrix KV Store (live scorer)
      5. Bust the /scoring/evaluation cache
    """
    import datetime as _dt

    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.scoring import fixtures as _fixtures
    from backend.scoring import labels as _labels
    from backend.scoring import matrix as _matrix
    from backend.scoring.evaluate import DEFAULT_MIN_AUC
    from backend.scoring.evaluate import evaluate as _evaluate

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    cfg = svcconfig.load_config(service_id) or {}
    matrix_version = version or _matrix.default_version()
    start = _dt.datetime.now(_dt.UTC) - _dt.timedelta(days=int(since_days))

    # 1. Extract sessions from DuckDB. The extract function expects a
    # live connection; reuse the same read-only path the analytics
    # endpoints use so we never block ingest writers.
    con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
    try:
        sessions_iter = _fixtures.extract_traces(con, service_id=service_id, start=start)
        # 2. Build matrix in one streaming pass.
        tmatrix, stats = _matrix.build_matrix(
            (s.to_jsonl_dict() for s in sessions_iter),
        )
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("scoring_retrain: con.close failed (ignored)", exc_info=True)

    matrix_dict = tmatrix.to_json_dict(version=matrix_version)

    # 3. Evaluate against accumulated labels if we have enough of each.
    auc_result = None
    label_rows = _labels.list_labels(service_id)
    n_good, n_bad, _ = _label_counts(service_id)
    if n_good >= _MIN_LABELS_PER_CLASS and n_bad >= _MIN_LABELS_PER_CLASS:
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        if labeled_sessions:
            er = _evaluate(matrix_dict, labeled_sessions)
            auc_result = {
                "auc": round(float(er.auc), 4),
                "passed": bool(er.passed),
                "threshold": float(er.pass_threshold),
                "n_good": er.n_good,
                "n_bad": er.n_bad,
            }

    # 4. Save matrix.json + publish to FOS. Local save is best-effort —
    # if the backend container can't write to compute/scorer/ (read-only
    # image mount), we still succeed by relying on FOS as the durable
    # store. _load_matrix() will pull from FOS next call.
    try:
        tenant_matrix_path = _tenant_matrix_path(service_id)
        tenant_matrix_path.parent.mkdir(parents=True, exist_ok=True)
        with tenant_matrix_path.open("w") as f:
            import json as _json

            _json.dump(matrix_dict, f)
        local_saved = True
    except Exception as exc:
        local_saved = False
        logger.warning(f"Could not write matrix.json locally: {exc}")

    fos_published = False
    try:
        from backend.state_sync import publish_matrix_to_fos

        publish_matrix_to_fos(service_id, matrix_dict)
        fos_published = True
    except Exception as exc:
        logger.warning(f"Could not publish matrix to FOS: {exc}")

    # 4b. Push the matrix into the scoring_matrix KV Store so the LIVE scorer
    #     picks it up — no Wasm rebuild. Writes the in-memory matrix directly
    #     (not the best-effort local file, which may have failed on a
    #     read-only mount). Best-effort: needs a Fastly token + a provisioned
    #     matrix store. On failure the matrix is still in FOS and a subsequent
    #     enable re-seeds KV. Takes effect as Compute instances recycle.
    kv_written = _push_matrix_to_kv_if_configured(service_id, cfg, matrix_dict)

    # 5. Bust analytics caches so the next StatusPanel hit sees the new AUC.
    _bust_analytics_cache(service_id)

    # Operator audit: every retrain is attributable + reviewable.
    _record_scoring_audit(
        service_id,
        "matrix_retrained",
        details={
            "matrix_version": matrix_version,
            "since_days": since_days,
            "sessions_trained_on": tmatrix.session_count,
            "auc_against_labels": auc_result,
            "fos_published": fos_published,
            "kv_written": kv_written,
        },
    )

    return {
        "ok": True,
        "matrix_version": matrix_version,
        "since_days": since_days,
        "sessions_trained_on": tmatrix.session_count,
        "transitions": tmatrix.transition_count,
        "vocab_size": len(tmatrix.vocab),
        "rejected": {
            "too_few_events": stats.sessions_dropped_short,
            "too_fast": stats.sessions_dropped_fast,
            "kept": stats.sessions_kept,
            "routes_seen": stats.routes_seen,
        },
        "auc_against_labels": auc_result,
        "default_min_auc": float(DEFAULT_MIN_AUC),
        "local_matrix_saved": local_saved,
        "fos_matrix_published": fos_published,
        "matrix_kv_written": kv_written,
        "deploy_hint": (
            "Live: matrix pushed to the scoring_matrix KV Store — the edge scorer "
            "picks it up as Compute instances recycle (typically minutes), no redeploy. "
            "The /scoring/evaluation endpoint reflects it immediately."
            if kv_written
            else (
                "Matrix saved to FOS but NOT pushed to the live scorer (no KV store / "
                "Fastly token available). Re-enable scoring for this service to "
                "provision the matrix KV store and seed it. The /scoring/evaluation "
                "endpoint reflects the new matrix immediately regardless."
            )
        ),
    }


# ── Session details (sid → page sequence) ────────────────────────────────────


@router.get(
    "/{service_id}/scoring/sessions/{sid}/events",
    response_model=ScoringSessionEventsResponse,
    response_model_exclude_unset=True,
)
def scoring_session_events(
    request: Request,
    service_id: ServiceId,
    sid: str = Path(..., description="Edge session id (12-hex chars)"),
    since_days: int = Query(default=30, ge=1, le=90),
) -> dict:
    """Return the event timeline for a single session — the URLs the
    session hit, in order, with per-request status/score/compliance/reason
    so the UI can render a 'view this labeled session' popover.

    The data is the same shape ``evaluate()`` consumes for AUC; this
    endpoint just exposes it through a public route keyed on the sid the
    operator clicked. Cap is 500 events per sid (any realistic browsing
    session well under that; the cap is a runaway-loop safety bound).

    Analyst-reachable, so the lookback is clamped to the invite window
    (since_days expressed as hours). The per-event ``ip`` is masked for
    analysts by the response middleware's key-name pass; ``ua`` / ``url`` stay
    intact (analysts triage on them). Admin requests keep the full window.
    """
    # Analyst → pass the clamped absolute predicate; admin (disc is None) keeps
    # the repo's own relative since_days DAY window untouched.
    ts_pred, disc = _scoring_time_window(request, int(since_days) * 24)
    grouped = _fetch_session_events(
        service_id, [sid], since_days=since_days, ts_predicate=(ts_pred if disc is not None else None)
    )
    events = grouped.get(sid, [])
    return {
        "sid": sid,
        "since_days": since_days,
        "event_count": len(events),
        "events": events,
    }


def _record_scoring_audit(service_id: str, action: str, details: dict) -> None:
    from backend.core import metadata as metadata_db

    metadata_db.record_scoring_audit(service_id, action, details=details)


def _require_scoring_config_store(service_id: str, token: str = "") -> tuple[dict, str, str]:
    """Resolve ``(cfg, config_store_id, resolved_token)`` or raise HTTP 400."""
    cfg, scoring = _load_scoring_cfg(service_id)
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    return cfg, config_store_id, resolved_token


def _upsert_config_store_item(api_token: str, store_id: str, key: str, value: str) -> None:
    """Upsert a ConfigStore item via PATCH, falling back to POST on first write."""
    from backend.core.fastly.client import fastly

    try:
        fastly(
            "PATCH",
            f"/resources/stores/config/{store_id}/item/{key}",
            {"item_value": value},
            token=api_token,
        )
    except Exception:
        fastly(
            "POST",
            f"/resources/stores/config/{store_id}/item",
            {"item_key": key, "item_value": value},
            token=api_token,
        )


@router.get(
    "/{service_id}/scoring/enforce-threshold",
    response_model=ScoringEnforceThresholdResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_threshold_get(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Read the live enforce_threshold value from the scoring_config
    Compute ConfigStore. None = no enforcement.

    The Rust scorer reads this on every request — when set AND the
    request's score >= threshold, it emits X-Edge-Score-Enforce: 1,
    which the SCORING_ENFORCE_NAME VCL snippet turns into a 429.
    """
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    cache_key = (service_id, config_store_id)
    cached = _enforce_threshold_cache.get(cache_key)
    if cached is not None:
        return {**cached, "_is_cached": True}

    try:
        raw = _l2_get_item(config_store_id, _ENFORCE_THRESHOLD_KEY, resolved_token)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="enforce_threshold_read_failed", status=502)
    threshold: int | None = int(raw) if raw and raw.isdigit() else None

    result = {
        "threshold": threshold,
        "enforced": threshold is not None,
        "key": _ENFORCE_THRESHOLD_KEY,
    }
    _enforce_threshold_cache[cache_key] = result
    return result


def _validate_threshold_0_100(threshold: int | None) -> int | None:
    if threshold is not None and not 0 <= threshold <= 100:
        raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})
    return threshold


@router.put(
    "/{service_id}/scoring/enforce-threshold",
    response_model=ScoringEnforceThresholdPutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_threshold_put(
    body: ScoringThresholdBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the enforcement change"),
) -> dict:
    """Write the live enforce_threshold to the scoring_config ConfigStore.
    Pass ``{"threshold": null}`` to clear (disable enforcement).

    Effective at the edge within seconds (next Compute invocation
    re-reads the ConfigStore). Audited to scoring_audit so the operator
    can review when enforcement was flipped on/off.

    Gated by ``?confirm=true`` (matches the matrix-restore pattern) so
    an accidental click can't silently flip enforcement at the edge."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change enforcement. This affects live edge blocking.")

    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    threshold = _validate_threshold_0_100(body.threshold)

    # Upsert: PATCH the item, falling back to POST if it doesn't exist
    # yet (first time enforcement is set for this service).
    value = str(threshold) if threshold is not None else ""
    try:
        _upsert_config_store_item(resolved_token, config_store_id, _ENFORCE_THRESHOLD_KEY, value)
    except Exception as e:
        raise_internal(logger, e, code="scoring_enforce_threshold_put_failed")

    # Drop the cached GET response so the operator's read-after-write is
    # accurate instead of returning the up-to-30s-old snapshot.
    _enforce_threshold_cache.pop((service_id, config_store_id), None)

    _record_scoring_audit(
        service_id,
        "threshold_enforce_disabled" if threshold is None else "threshold_enforced",
        details={"threshold": threshold},
    )

    return {
        "ok": True,
        "threshold": threshold,
        "enforced": threshold is not None,
        "message": (
            "Enforcement disabled — scorer will stop setting X-Edge-Score-Enforce on responses."
            if threshold is None
            else f"Enforcement live at threshold {threshold}. Scorer will set X-Edge-Score-Enforce=1 "
            "when score >= threshold; the Enforce VCL snippet 429s those requests."
        ),
    }


# ── L2 enforcement opt-in (operator decides when L2 joins the enforced score) ─


@router.get(
    "/{service_id}/scoring/l2-enforce",
    response_model=ScoringL2EnforceState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_l2_enforce_get(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Read the live L2-enforcement opt-in + fade-in state from the scoring_config
    ConfigStore, plus the advisory deployment-age readiness gauge.

    L2's sub-score is ALWAYS computed + logged at the edge; this endpoint reports
    only whether (and how strongly) it contributes to the *enforced* combined
    score. ``enabled`` is the operator's explicit opt-in; ``ramp_progress`` /
    ``days_since_optin`` / ``warmup_days_remaining`` track the fade-in from the
    opt-in anchor; ``ready`` / ``deployment_age_days`` are the advisory "enough
    observed L2 data yet?" gauge (deploy age ≥ 7 days), never an actuator.

    Read+cache pattern mirrors /scoring/enforce-threshold (300s TTL, busted by the
    PUT). 502 on a non-404 ConfigStore error rather than masking it as off."""
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    try:
        return _read_l2_enforce_state(service_id, config_store_id, resolved_token)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="l2_enforce_read_failed", status=502)


@router.put(
    "/{service_id}/scoring/l2-enforce",
    response_model=ScoringL2EnforcePutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_l2_enforce_put(
    body: ScoringL2EnforceBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the L2-enforcement change"),
) -> dict:
    """Opt edge Layer-2 in/out of the *enforced* combined score by writing the
    ``l2_enforce_enabled`` flag to the scoring_config ConfigStore.

    Enabling stamps a fresh ``l2_enabled_at`` anchor on the off→on transition (or
    when none exists yet) so L2 fades in over ~3 days from the moment of consent;
    re-confirming an already-on service does NOT reset an in-progress fade-in.
    Disabling writes "0" and leaves the anchor as-is, so a later off→on re-enable
    overwrites it and restarts the fade-in. L2's sub-score keeps being computed +
    logged regardless — only its contribution to the enforced score is gated.

    Effective at the edge within seconds (the scorer re-reads the ConfigStore each
    request). Audited. Gated by ``?confirm=true`` — the same kill switch as
    enforce-threshold — because it changes live edge blocking."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change L2 enforcement. This affects live edge blocking.")

    import datetime as _dt

    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)

    enabled = bool(body.enabled)

    try:
        l2_enabled_at: int | None
        if enabled:
            # Stamp a fresh fade-in anchor on off→on (or when none exists yet),
            # but preserve an in-progress ramp when re-confirming an already-on
            # service. Read both current values first.
            currently_on = _l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, resolved_token).strip() == "1"
            existing_anchor = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
            _upsert_config_store_item(resolved_token, config_store_id, _L2_ENFORCE_ENABLED_KEY, "1")
            if (not currently_on) or (existing_anchor is None):
                l2_enabled_at = int(_dt.datetime.now(_dt.UTC).timestamp())
                _upsert_config_store_item(resolved_token, config_store_id, _L2_ENABLED_AT_KEY, str(l2_enabled_at))
            else:
                l2_enabled_at = existing_anchor
        else:
            _upsert_config_store_item(resolved_token, config_store_id, _L2_ENFORCE_ENABLED_KEY, "0")
            # Leave l2_enabled_at as-is; a later off→on re-enable overwrites it.
            l2_enabled_at = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
    except Exception as e:
        raise_internal(logger, e, code="scoring_l2_enforce_put_failed")

    # Read-after-write accuracy: drop the cached GET block + bust the analytics
    # cache (scoring_health's readiness block reuses the same numbers).
    _l2_enforce_cache.pop((service_id, config_store_id), None)
    _bust_analytics_cache(service_id)

    _record_scoring_audit(
        service_id,
        "l2_enforce_enabled" if enabled else "l2_enforce_disabled",
        details={"enabled": enabled, "l2_enabled_at": l2_enabled_at},
    )

    return {
        "ok": True,
        "enabled": enabled,
        "l2_enabled_at": l2_enabled_at,
        "message": (
            f"L2 now contributes to the enforced score, fading in over {int(_L2_RAMP_DAYS)} days "
            "from opt-in. The edge scorer picks this up within seconds."
            if enabled
            else "L2 enforcement disabled — L2 returns to observe-only (sub-score still logged, "
            "no contribution to the enforced score)."
        ),
    }


# ── Recv exclusion regex (URLs that bypass the scorer) ─────────────────────


@router.get(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_get(service_id: ServiceId) -> dict:
    """Return the operator-configured URL-exclusion regex for the recv snippet.

    URLs that match this regex are NOT routed to the Compute scorer
    (saves cost on static assets / health checks / etc.). The default
    matches common static-asset file extensions; the operator can
    override it via the PUT endpoint below.

    Response shape:
      {
        "current":      str,    # the stored value (literal default after
                                # first enable_scoring; or operator override)
        "is_default":   bool,   # true when current is empty OR equals the
                                # built-in default literal
        "default":      str,    # the built-in default regex
        "effective":    str,    # what's actually interpolated into VCL
      }
    """
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        resolve_exclude_url_regex,
    )

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("exclude_url_regex") or ""
    effective = resolve_exclude_url_regex(current or None)
    return {
        "current": current,
        # Empty cfg (legacy services from before enable_scoring populated
        # the default) AND services whose stored value happens to equal
        # the bundled default both count as "default" for UI purposes —
        # the admin shouldn't see "custom override" when nothing's actually
        # been customised.
        "is_default": (not current) or current == DEFAULT_ASSET_EXT_REGEX,
        "default": DEFAULT_ASSET_EXT_REGEX,
        "effective": effective,
    }


def _require_scoring_enabled_and_token(service_id: str, token: str = "") -> tuple[dict, dict, str]:
    cfg, scoring = _load_scoring_cfg(service_id)
    if not scoring.get("enabled"):
        raise HTTPException(status_code=400, detail={"error": "Session scoring is not enabled for this service"})
    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})
    return cfg, scoring, resolved_token


def _validate_service_exclude_regex(service_id: str, raw: str):
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import validate_recv_exclusion_regex_with_lint

    return validate_recv_exclusion_regex_with_lint(
        raw,
        build_full_snippet=lambda cleaned_regex: recv_snippet(service_id, exclude_url_regex=cleaned_regex or None),
        require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "1") != "0",
    )


@router.put(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexPutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_put(
    body: ScoringExcludeRegexBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the change"),
) -> dict:
    """Update the URL-exclusion regex for the scoring recv snippet.

    Validation pipeline (must pass all four to land):
      1. Input policy (length cap, no quote / control chars, valid regex).
      2. Falco static analysis on the assembled recv-snippet body.
      3. Fastly's VCL ``validate`` endpoint on the cloned version.
      4. ``activate_version`` (Fastly's compiler runs again).

    Re-deploys ONLY the recv snippet — Compute service, Wasm, log
    format, and the other 5 scoring snippets stay untouched. Takes
    ~5-10s end-to-end.

    Pass ``{"regex": ""}`` to reset to the built-in default. Body shape:
        { "regex": str }

    Gated by ``?confirm=true`` because a typo here can disable scoring
    entirely (regex matches everything) or DoS Compute (regex matches
    nothing → every request scored). The confirm flag matches the
    enforce-threshold + matrix-restore precedent.
    """
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )

    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex
    from backend.utils.vcl_validator import RegexValidationError

    _cfg, _scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    try:
        cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": exc.message, "reason": exc.reason},
        )

    # Layers 3 + 4: clone → swap → validate → activate via the
    # orchestrator helper.
    try:
        result = update_recv_exclusion_regex(service_id, resolved_token, new_regex=cleaned)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="exclude_regex_update_failed", status=502)

    _record_scoring_audit(
        service_id,
        "scoring_exclude_regex_changed",
        details={
            "is_default": result["is_default"],
            "effective_regex": result["effective_regex"][:200],
            "logging_service_active_version": result["logging_service_active_version"],
            "lint_warnings": lint.warnings[:5],
        },
    )

    return {
        "ok": True,
        **result,
        "lint_warnings": lint.warnings,
        "message": (
            "Reset to default URL exclusion regex."
            if result["is_default"]
            else "Custom URL exclusion regex applied. Effective at the edge after Fastly version activation."
        ),
    }


# ── Dry-run validator for the exclude-regex (no persistence, no VCL) ──────


@router.post(
    "/{service_id}/scoring/exclude-regex/validate",
    response_model=ScoringExcludeRegexValidateResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_validate(
    body: ScoringExcludeRegexBody,
    service_id: ServiceId,
) -> dict:
    """Run the 2-layer pre-publish validator on a candidate regex WITHOUT
    persisting it or touching Fastly.

    Drives the admin UI's on-blur lint check: the operator types a regex,
    tabs out of the textarea, and gets immediate feedback on whether the
    value would pass input policy (length / quote / control-char / Python
    re.compile) AND falco's static analysis on the assembled snippet,
    BEFORE they commit to a publish flow.

    Response shape:
      Success:  {"ok": true,  "lint_warnings": [...]}
      Failure:  {"ok": false, "error": "...", "reason": "..."}

    The third layer (Fastly's own VCL compiler during version activate)
    only runs on real publish — we don't burn a clone/activate round-trip
    for a preview. False-positives between falco and Fastly's compiler are
    rare; the publish flow still catches them.
    """
    from backend.utils.vcl_validator import RegexValidationError

    try:
        _cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        return {
            "ok": False,
            "error": exc.message,
            "reason": exc.reason,
        }

    return {
        "ok": True,
        "lint_warnings": lint.warnings,
    }


# ── Enforce response status code (default 429, operator-overridable) ──────


@router.get(
    "/{service_id}/scoring/enforce-status-code",
    response_model=ScoringEnforceStatusCodeState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_status_code_get(service_id: ServiceId) -> dict:
    """Return the operator-configured HTTP status code that the enforce
    snippet returns when the scorer flags a request.

    Defaults to 429 (Too Many Requests). Operators can pick any 4xx/5xx
    code via the PUT endpoint below.

    Response shape:
      {
        "current":     int,    # operator's override, or null when default
        "default":     int,    # built-in default (429)
        "effective":   int,    # what's actually baked into the VCL
        "min":         int,    # min allowed value (400)
        "max":         int,    # max allowed value (599)
        "is_default":  bool,
      }
    """
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
        DEFAULT_ENFORCE_STATUS_CODE,
        resolve_enforce_status_code,
    )

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("enforce_status_code")
    effective = resolve_enforce_status_code(current)
    return {
        "current": current,
        "default": DEFAULT_ENFORCE_STATUS_CODE,
        "effective": effective,
        "min": _ENFORCE_STATUS_CODE_MIN,
        "max": _ENFORCE_STATUS_CODE_MAX,
        "is_default": effective == DEFAULT_ENFORCE_STATUS_CODE,
    }


@router.put(
    "/{service_id}/scoring/enforce-status-code",
    response_model=ScoringEnforceStatusCodePutResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_status_code_put(
    body: ScoringEnforceStatusCodeBody,
    service_id: ServiceId,
    token: str = Header(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the change"),
) -> dict:
    """Update the HTTP status code returned by the enforce snippet.

    Body shape: ``{"status_code": int | null}``. Pass ``null`` (or omit)
    to reset to the default 429.

    Validation:
      - Must be int in 400-599 (4xx/5xx HTTP error range).
      - Anything else → 400 with explanation.

    Re-deploys ONLY the enforce snippet — Compute service, Wasm, log
    format, and the other 5 scoring snippets stay untouched. Takes
    ~5-10s end-to-end.

    Gated by ``?confirm=true`` because the change affects live edge
    response codes seen by real users — same precedent as
    enforce-threshold and exclude-regex.
    """
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )

    from backend.provision.session_scoring_orchestrator import update_enforce_status_code
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
    )

    new_code = body.status_code
    if new_code is not None and not (_ENFORCE_STATUS_CODE_MIN <= new_code <= _ENFORCE_STATUS_CODE_MAX):
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"status_code must be in {_ENFORCE_STATUS_CODE_MIN}-{_ENFORCE_STATUS_CODE_MAX} (HTTP 4xx/5xx)"
            },
        )

    _cfg, _scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    try:
        result = update_enforce_status_code(service_id, resolved_token, new_status_code=new_code)
    except RuntimeError as exc:
        raise_internal(logger, exc, code="enforce_status_code_update_failed", status=502)

    _record_scoring_audit(
        service_id,
        "scoring_enforce_status_code_changed",
        details={
            "is_default": result["is_default"],
            "effective_status_code": result["effective_status_code"],
            "logging_service_active_version": result["logging_service_active_version"],
        },
    )

    return {
        "ok": True,
        **result,
        "message": (
            "Reset to default enforce status code (429)."
            if result["is_default"]
            else f"Enforce status code → {result['effective_status_code']}. Effective at the edge after Fastly version activation."
        ),
    }


# ── Matrix version history + rollback ──────────────────────────────────────


@router.get(
    "/{service_id}/scoring/matrix-versions",
    response_model=ScoringMatrixVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_list(service_id: ServiceId) -> dict:
    """List historical scoring matrices archived in FOS.

    publish_matrix_to_fos snapshots the prior current matrix to
    ``iceberg/meta/scoring_matrix_history/{version}.json`` before
    overwriting, so the operator can roll back to any prior trained
    matrix. Returns most-recent first."""
    from backend.state_sync import list_scoring_matrix_versions

    _cfg, scoring = _load_scoring_cfg(service_id)
    current_version = scoring.get("matrix_version")
    return {
        "versions": list_scoring_matrix_versions(service_id),
        "current_version": current_version,
    }


@router.post(
    "/{service_id}/scoring/matrix-versions/{version}/restore",
    response_model=ScoringMatrixRestoreResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_restore(
    service_id: ServiceId,
    version: str = Path(
        ...,
        description="Matrix version string to restore",
        pattern=r"^[A-Za-z0-9._-]+$",
        max_length=64,
    ),
    confirm: bool = Query(default=False, description="Set true to actually perform the restore"),
) -> dict:
    """Restore a historical matrix to the current scoring_matrix.json
    key in FOS. Also deletes the local matrix.json so the next
    /scoring/evaluation call sees the FOS-restored matrix, and pushes the
    restored matrix into the scoring_matrix KV Store so the LIVE edge scorer
    picks it up (as Compute instances recycle) — no redeploy. The
    /scoring/evaluation AUC reflects the restored matrix immediately.

    Gated by ``?confirm=true`` so an accidental click can't silently
    rewind the live AUC numbers."""
    _require_confirm(confirm, "Pass ?confirm=true to actually restore. This will replace the current matrix.")

    from backend import config as svcconfig
    from backend.state_sync import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Matrix version {version!r} not found in FOS history"},
        )

    # Drop the tenant-scoped local matrix (matrix_<sid>.json) so
    # _load_matrix falls through to the FOS-restored version instead of
    # shadowing it. (_load_matrix's resolution order consults only the
    # tenant path — the pre-audit-005 shared matrix.json is never read,
    # and prod was verified clean of it on 2026-07-06.)
    tenant_matrix_path = _tenant_matrix_path(service_id)
    try:
        if tenant_matrix_path.exists():
            tenant_matrix_path.unlink()
    except Exception as exc:
        logger.warning(f"Could not remove local {tenant_matrix_path.name} after restore: {exc}")

    # Update cfg.scoring.matrix_version so /scoring/status reflects the rollback.
    cfg = svcconfig.mutate_config(
        service_id, lambda c: c.setdefault("scoring", {}).__setitem__("matrix_version", version)
    )

    # Push the FOS-restored matrix into the scoring_matrix KV Store so the live
    # scorer rolls back too. _write_matrix_to_kv resolves via the FOS chain
    # (the local copy was just deleted). Best-effort — needs a token + store.
    kv_written = _push_matrix_to_kv_if_configured(service_id, cfg)

    _bust_analytics_cache(service_id)

    _record_scoring_audit(
        service_id,
        "matrix_restored",
        details={"restored_version": version, "restored_at": result["restored_at"], "kv_written": kv_written},
    )

    return {
        "ok": True,
        "restored_version": version,
        "restored_at": result["restored_at"],
        "matrix_kv_written": kv_written,
        "deploy_hint": (
            "Live: restored matrix pushed to the scoring_matrix KV Store — the edge "
            "scorer rolls back as Compute instances recycle (minutes), no redeploy."
            if kv_written
            else (
                "Backend AUC + evaluation reflect the restored matrix. It was NOT pushed "
                "to the live scorer (no KV store / token); re-enable scoring to seed KV."
            )
        ),
    }


# ── AES key rotation ────────────────────────────────────────────────────────


@router.post(
    "/{service_id}/scoring/rotate-key",
    response_model=ScoringRotateKeyResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
@query_errors(500, code="scoring_rotate_key_failed", logger_obj=logger)
def scoring_rotate_key(
    service_id: ServiceId,
    token: str = Header(default=""),
) -> dict:
    """Rotate the AES-GCM cookie-state encryption key.

    Moves the current key to ``previous_key_hex`` (grace window for
    in-flight cookies still using the old key) and writes a fresh
    32-byte key as the new ``current_key_hex``. The Rust scorer's
    cookie codec already tries previous as a fallback so existing
    sessions keep decoding for one rotation cycle.

    Returns rotation metadata — the new key itself is NOT returned in
    the response (only stored in the Fastly ConfigStore + audit log).
    """
    from backend.provision.session_scoring_setup import rotate_aes_key

    load_service_config(service_id)
    _cfg, scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)

    scoring_keys_store_id = scoring.get("scoring_keys_store_id")
    if not scoring_keys_store_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Service has no scoring_keys_store_id (was scoring enabled before key rotation was supported?)"
            },
        )

    result = rotate_aes_key(scoring_keys_store_id, token=resolved_token)

    # Record the rotation in the audit log (without the key value).
    _record_scoring_audit(
        service_id,
        "key_rotated",
        details={
            "rotated_at": result["rotated_at"],
            "previous_key_grace": bool(result.get("previous_key_hex")),
        },
    )

    # Don't echo the key itself.
    return {
        "ok": True,
        "rotated_at": result["rotated_at"],
        "previous_key_grace": bool(result.get("previous_key_hex")),
        "message": (
            "AES key rotated. Cookies signed with the previous key keep "
            "decoding via the previous_key_hex grace slot — clear that "
            "slot by rotating again after the idle-expire window (~hours)."
        ),
    }


# ── Operator audit log ──────────────────────────────────────────────────────


@router.get(
    "/{service_id}/scoring/audit",
    response_model=ScoringAuditListResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_audit_list(
    service_id: ServiceId,
    limit: int = Query(default=100, ge=1, le=1000),
    since: str | None = Query(default=None, description="ISO timestamp lower bound (inclusive)"),
) -> dict:
    """List recent operator actions on this service's scoring config.

    Tracks: scoring_enabled, scoring_disabled, threshold_committed,
    threshold_cleared, threshold_enforced, threshold_enforce_disabled,
    matrix_retrained, matrix_restored, key_rotated. Each row has
    timestamp, action, actor, details (JSON). Used for compliance review
    + "who broke prod last Tuesday?" triage.

    ``since`` (optional ISO timestamp) filters to rows at or after that
    instant — handy for the admin UI to poll for new events without
    re-rendering the entire history."""
    from backend.core import metadata as metadata_db

    # 404 when the service itself isn't known — mirrors /scoring/status so
    # the UI gets a consistent shape across the audit + status pair.
    load_service_config(service_id)

    rows = metadata_db.list_scoring_audit(service_id, limit=limit, since=since)
    return {"audit": rows, "limit": limit}


# ── Operator's chosen threshold (persisted, not enforced) ───────────────────


@router.get(
    "/{service_id}/scoring/threshold",
    response_model=ScoringThresholdState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_threshold_get(service_id: ServiceId) -> dict:
    """Return the operator's chosen score threshold.

    NOT enforced — the live scorer doesn't read this. It's a persisted
    operator preference so the threshold slider can remember the
    'committed' value across sessions, and the StatusPanel can show
    'committed threshold: X' as a stable reference. Actual enforcement
    requires a Rust scorer change + Wasm redeploy and is deferred to
    a future release once the operator is confident in the value.
    """
    _cfg, scoring = _load_scoring_cfg(service_id)
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,  # See docstring — preview-only
    }


@router.put(
    "/{service_id}/scoring/threshold",
    response_model=ScoringThresholdState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_threshold_put(
    body: ScoringThresholdBody,
    service_id: ServiceId,
) -> dict:
    """Persist the operator's chosen threshold (0-100) into the per-service
    config. Pass ``{"threshold": null}`` to clear. Always returns the
    new state. Does NOT push to Compute — preview-only."""
    import datetime as _dt

    from backend import config as svcconfig

    threshold = _validate_threshold_0_100(body.threshold)

    cfg = load_service_config(service_id)
    prior_threshold = cfg.get("scoring", {}).get("operator_threshold")
    set_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")

    def _apply_threshold(c: dict) -> None:
        scoring = c.setdefault("scoring", {})
        if threshold is None:
            scoring.pop("operator_threshold", None)
            scoring.pop("operator_threshold_set_at", None)
        else:
            scoring["operator_threshold"] = threshold
            scoring["operator_threshold_set_at"] = set_at

    # Operator audit trail — every threshold change is attributable.
    _record_scoring_audit(
        service_id,
        "threshold_committed" if threshold is not None else "threshold_cleared",
        details={"prior_threshold": prior_threshold, "new_threshold": threshold},
    )
    updated_cfg = svcconfig.mutate_config(service_id, _apply_threshold)
    if not isinstance(updated_cfg, dict):
        _apply_threshold(cfg)
        updated_cfg = cfg
    scoring = updated_cfg.get("scoring", {})

    _bust_analytics_cache(service_id)  # so /scoring/status reflects it next fetch
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
    }


# ── Per-reason AUC breakdown ────────────────────────────────────────────────


@router.get(
    "/{service_id}/scoring/evaluation/per-reason",
    response_model=ScoringPerReasonResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_evaluation_per_reason(
    service_id: ServiceId,
) -> dict:
    """AUC broken down by L1/L2 rule (cookie-missing, impossibly-fast,
    robotic-consistency, rare-transition, low-transition-prob).

    Same min-samples gate as /scoring/evaluation but applied per-bucket
    (so a reason with <3 labels in either class shows a 'need more
    labels with reason=X' CTA instead of a noisy AUC). The headline
    /scoring/evaluation gives the combined AUC; this answers 'which
    rule contributed most to AUC' once enough per-reason labels exist.
    """
    from backend.scoring import labels as _labels
    from backend.scoring.evaluate import evaluate_per_reason

    n_good, n_bad, n_neutral = _label_counts(service_id)

    cache_key = ("scoring-evaluation-per-reason", service_id, n_good, n_bad, n_neutral)

    def _produce() -> dict:
        if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
            # No point bucketing — the headline AUC isn't even computable.
            return {
                "has_min_samples_overall": False,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
                "buckets": [],
            }
        label_rows = _labels.list_labels(service_id)
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        result = evaluate_per_reason(labeled_sessions, min_per_class=_MIN_LABELS_PER_CLASS)
        result["has_min_samples_overall"] = True
        result["n_good"] = n_good
        result["n_bad"] = n_bad
        return result

    return _cached(cache_key, _produce)


# ── Composite dashboard endpoint ────────────────────────────────────────────
#
# Single round-trip variant of the 8 endpoints the session-scoring admin page
# mounts (status, evaluation, health, top-flagged, score-distribution,
# compliance-breakdown, curves, threshold-preview). Opens ONE read-only
# DuckDB connection, builds ONE filtered temp table, runs each aggregation
# against it.
#
# Wire-compat: this is purely additive — the 8 existing endpoints stay
# mounted with their current cache-key contracts and TTL behavior. The
# frontend can opt in by calling /scoring/dashboard instead of the 8
# individual queries, or keep fanning out for now.


def _build_scoring_config_bundle(service_id: str) -> dict:
    return {
        "threshold": scoring_threshold_get(service_id),
        "exclude_regex": scoring_exclude_regex_get(service_id),
        "enforce_status_code": scoring_enforce_status_code_get(service_id),
    }


@router.get(
    "/{service_id}/scoring/dashboard",
    response_model=ScoringDashboardResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_dashboard(
    request: Request,
    service_id: ServiceId,
    since_hours: int = Query(default=24, ge=1, le=168),
    threshold: int = Query(default=75, ge=0, le=100, description="Preview cutoff for threshold-preview block"),
) -> dict:
    """One-shot dashboard payload. Returns:

    ```
    {
        since_hours, threshold,
        status: {...},                                  # /scoring/status
        evaluation: {...},                              # /scoring/evaluation
        health: {...},                                  # /scoring/health
        top_flagged: {rows: [...], since_hours},        # /scoring/top-flagged
        score_distribution: {rows: [...]},              # /scoring/score-distribution
        compliance_breakdown: {rows: [...]},            # /scoring/compliance-breakdown
        curves: {...},                                  # /scoring/curves
        threshold_preview: {...},                       # /scoring/threshold-preview
    }
    ```

    Each sub-object is byte-identical to the corresponding individual
    endpoint's response — the frontend can swap to
    ``dashboard.top_flagged`` without changing card-level contracts.

    Cache key includes ``since_hours``, ``threshold``, and the per-class
    label counts so label mutations + slider drags invalidate naturally.
    """
    from backend import config as svcconfig

    n_good, n_bad, n_neutral = _label_counts(service_id)

    cache_key = (
        "scoring-dashboard",
        service_id,
        since_hours,
        threshold,
        n_good,
        n_bad,
        n_neutral,
    )

    def _produce() -> dict:
        # --- /scoring/status (no DuckDB) ---
        cfg = svcconfig.load_config(service_id)
        status_block: dict = scoring_status(service_id=service_id) if cfg else {"enabled": False}

        # Build the dashboard in a single payload by delegating to the
        # existing per-endpoint producers. Each handles its own _query_logs
        # call — meaning 6 DuckDB connections instead of 1 (the audit's
        # ideal). The win this iteration captures is the in-flight collapse:
        # one composite request → one cache key → one set of fetches that
        # serializes through the per-key lock instead of 8 frontend
        # requests racing through the proxy + react-query.
        #
        # The shared-temp-table optimization stays available for a future
        # PR — wiring it requires refactoring each per-endpoint producer
        # to accept an open connection + table name, which touches 5
        # endpoints worth of test surface. Punting that to v1.2.0 keeps
        # this change additive + zero-risk.
        import contextvars
        from concurrent.futures import ThreadPoolExecutor

        # Pre-warm _table_columns once so the 5 sub-handlers below share the
        # cached schema + view binding instead of opening 5 concurrent
        # DESCRIBE connections on cold cache.
        _table_columns(service_id)

        tasks = {
            "evaluation": lambda: scoring_evaluation(service_id=service_id),
            "evaluation_per_reason": lambda: scoring_evaluation_per_reason(service_id=service_id),
            "health": lambda: scoring_health(request=request, service_id=service_id, since_hours=since_hours),
            "top_flagged": lambda: scoring_top_flagged(
                request=request, service_id=service_id, since_hours=since_hours, limit=50
            ),
            "score_distribution": lambda: scoring_score_distribution(
                request=request, service_id=service_id, since_hours=since_hours
            ),
            "compliance_breakdown": lambda: scoring_compliance_breakdown(
                request=request, service_id=service_id, since_hours=since_hours
            ),
            "curves": lambda: scoring_curves(service_id=service_id),
            "threshold_preview": lambda: scoring_threshold_preview(
                request=request, service_id=service_id, threshold=threshold, since_hours=since_hours
            ),
        }
        results: dict = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = {k: pool.submit(contextvars.copy_context().run, fn) for k, fn in tasks.items()}
            for k, fut in futs.items():
                results[k] = fut.result()

        return {
            "since_hours": since_hours,
            "threshold": threshold,
            "status": status_block,
            "evaluation": results["evaluation"],
            "evaluation_per_reason": results["evaluation_per_reason"],
            "health": results["health"],
            "top_flagged": results["top_flagged"],
            "score_distribution": results["score_distribution"],
            "compliance_breakdown": results["compliance_breakdown"],
            "curves": results["curves"],
            "threshold_preview": results["threshold_preview"],
            # `config_threshold` is intentionally distinct from the
            # top-level `threshold` (which is the per-request slider
            # cutoff fed into threshold_preview). The config block
            # mirrors /scoring/config's shape so the frontend can read
            # the persisted operator preferences off the same payload.
            **{
                ("config_threshold" if k == "threshold" else k): v
                for k, v in _build_scoring_config_bundle(service_id).items()
            },
        }

    return _cached(cache_key, _produce)


# A-3 (CacheRegistry): register the analytics + reconstruct caches.
# Same leak pattern as the iceberg / dashboard caches the R-1 work
# uncovered — _reconstruct_labeled_sessions_cache especially is a
# TTL-cached function that defeats per-test patches on
# reconstruct_labeled_sessions when a prior test populated it first.
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("session_scoring._analytics_cache", _analytics_cache)
_CacheRegistry.register("session_scoring._inflight", _inflight)
_CacheRegistry.register("session_scoring._reconstruct_labeled_sessions_cache", _reconstruct_labeled_sessions_cache)
_CacheRegistry.register("session_scoring._table_columns_cache", _table_columns_cache)
_CacheRegistry.register("session_scoring._scoring_pragma_cache", _scoring_pragma_cache)
_CacheRegistry.register("session_scoring._scoring_svc_version_cache", _scoring_svc_version_cache)
_CacheRegistry.register("session_scoring_admin._enforce_threshold_cache", _enforce_threshold_cache)
_CacheRegistry.register("session_scoring_admin._l2_enforce_cache", _l2_enforce_cache)


# ── Sessions endpoints (folded from backend.routers.sessions) ───────────────

sessions_router = APIRouter(prefix="/api/sessions", tags=["sessions"], responses=DEFAULT_ERROR_RESPONSES)


def _dispatch_sessions_query(ctx: RequestContext, hs_fn, duck_fn) -> dict:
    if ctx.high_scale is not None:
        return hs_fn(ctx.high_scale)
    return execute_with_stale_view_retry(ctx.con, ctx.source, duck_fn)


def _seal_session_rows(rows: list[dict], service_id: str) -> None:
    for row in rows:
        row["session_token"] = seal_session_token(
            row.get("ip", ""),
            row.get("ja4"),
            row.get("session_start", ""),
            row.get("session_end", ""),
            service_id=service_id,
        )


@sessions_router.post("", response_model=SessionsResponse)
@query_errors()
def sessions_endpoint(
    req: SessionsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    with process_context_scope("sessions:list"):
        start_time, end_time = ctx.resolve_window(req)

        # Guard against unbounded scans (14–20s observed) when the frontend hasn't
        # sent a time range yet. Default to the last 7 days — matches the max
        # window the repository enforces when a range IS provided.
        if not start_time or not end_time:
            _now = datetime.now(UTC)
            end_time = end_time or _now.isoformat()
            start_time = start_time or (_now - timedelta(days=7)).isoformat()

        def _hs_list(hs):
            from backend.high_scale.sessions import sessions_endpoint as hs_sessions

            return hs_sessions(hs, req, start_time, end_time)

        result = _dispatch_sessions_query(
            ctx,
            _hs_list,
            lambda con: repo.get_sessions(
                con=con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
                page=req.page,
                limit=req.limit,
                sort_by=req.sort_by,
                sort_dir=req.sort_dir,
                flagged_only=req.flagged_only,
                min_reqs_flag=req.min_reqs_flag,
                min_4xx_pct_flag=req.min_4xx_pct_flag,
                streaming_only=req.streaming_only,
            ),
        )

        # Mint an opaque token per row from the REAL ip/window — before the analyst
        # masking middleware rewrites ``ip`` on the way out. The detail endpoint
        # unseals it, so a masking analyst can drill in without the masked `ip`
        # (which can never match) being used as the lookup key. Uniform for admin +
        # analyst; the token is opaque ciphertext, so emitting it to everyone is safe.
        _seal_session_rows(result.get("sessions", []), ctx.service_id)
        return SessionsResponse.with_telemetry(**result)


@sessions_router.post("/detail", response_model=SessionDetailResponse)
@query_errors()
def sessions_detail(
    req: SessionDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    with process_context_scope("sessions:detail"):
        ip, ja4 = req.ip, req.ja4
        start_raw, end_raw = req.start_time, req.end_time

        if req.session_token:
            # Token is the source of truth: derive the whole tuple from it and
            # ignore client-supplied ip/window. Prevents a token-pinned-IP +
            # widened-window mini-oracle, and is the only path open to a masking
            # analyst (who never holds the real ip).
            try:
                ip, ja4, start_raw, end_raw = open_session_token(req.session_token, service_id=ctx.service_id)
            except SessionTokenError as e:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "session_token_invalid",
                        "message": "Session reference expired — reload the page.",
                    },
                ) from e
        elif mask_ips_for(ctx.analyst_session):
            # No token, but a masking analyst: a raw/masked top-level ``ip`` would
            # be a presence oracle (the masked value can never match a real IP, and
            # the PII filter-lock only inspects ``filters``, not this top-level
            # field). Masking analysts must drill in via the opaque token.
            raise HTTPException(status_code=403, detail={"error": "pii_policy_violation", "field": "ip"})

        if not ip or not start_raw or not end_raw:
            raise HTTPException(status_code=400, detail={"error": "ip, session_start, and session_end are required"})
        # A single-request session has session_start == session_end (a zero-width
        # window — ~28% of real sessions). ``ctx.clamp`` rejects an empty range, so
        # such a session would 400 ("time_range_empty") and the detail modal showed
        # "No results" for every role. Widen a zero/negative window by 1s on each
        # side: the repo's inclusive ``BETWEEN`` still captures the session's
        # request(s), and ctx.clamp re-caps the widened window to the analyst's
        # allowed bounds (so this can't be used to escape a query-window invite).
        s_dt, e_dt = parse_iso_utc(start_raw), parse_iso_utc(end_raw)
        if s_dt is not None and e_dt is not None and e_dt <= s_dt:
            start_raw = (s_dt - timedelta(seconds=1)).isoformat()
            end_raw = (e_dt + timedelta(seconds=1)).isoformat()
        start_time, end_time = ctx.clamp(start_raw, end_raw)
        # clamp_or_400 returns (None, None) only when both inputs are None AND
        # there's no analyst session; we already required both inputs above,
        # so the clamp always returns concrete ISO strings here.
        if start_time is None or end_time is None:
            raise RuntimeError("clamp returned no bounds although both were supplied")

        def _hs_detail(hs):
            from backend.high_scale.sessions import sessions_detail as hs_detail

            return hs_detail(hs, req, start_time, end_time, ip, ja4)

        res = _dispatch_sessions_query(
            ctx,
            _hs_detail,
            lambda con: repo.get_session_detail(
                con=con,
                src=ctx.source,
                ip=ip,
                ja4=ja4,
                session_start=start_time,
                session_end=end_time,
            ),
        )
        return SessionDetailResponse.with_telemetry(**res)


sys.modules["backend.routers.sessions"] = sys.modules[__name__]
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "sessions", sys.modules[__name__])
sys.modules["backend.routers.session_scoring_admin"] = sys.modules[__name__]
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "session_scoring_admin", sys.modules[__name__])
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    sys.modules["backend"]._MODULE_ALIASES["backend.routers.session_scoring_admin"] = "backend.routers.session_scoring"
