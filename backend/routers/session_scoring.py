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
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Path, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.session_scoring import (
    ScoringAnalyticsResponse,
    ScoringComplianceBreakdownResponse,
    ScoringCurvesResponse,
    ScoringEvaluationResponse,
    ScoringHealthResponse,
    ScoringLabelCreate,
    ScoringLabelsListResponse,
    ScoringLabelUpdate,
    ScoringLatencyTimeseriesResponse,
    ScoringScoreDistributionResponse,
    ScoringThresholdPreviewResponse,
    ScoringTokenBody,
    ScoringTopFlaggedResponse,
)
from backend.utils.remote_access import clamp_or_400, get_analyst_time_bounds
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error, not_found

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/services", tags=["session-scoring"], responses=DEFAULT_ERROR_RESPONSES)
# Fields in the per-service scoring config that must never reach the UI.
# AES key is never persisted in config (defensive); request_secret IS in
# the config so it must be stripped before any response that surfaces the
# scoring block (status endpoint, enable SSE done event, etc.).
_SECRET_KEYS = frozenset({"aes_key_hex", "request_secret"})


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
    from backend.models.common import _debug_responses_enabled

    if not isinstance(value, dict):
        return value
    out = dict(value)
    out["_is_cached"] = is_cached
    if not _debug_responses_enabled():
        out.pop("_debug_queries", None)
        out.pop("_debug_calls", None)
    return out


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
    import json as _json

    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH

    # 1. Tenant-scoped local matrix.
    tenant_path = _MATRIX_PATH.with_name(f"{_MATRIX_PATH.stem}_{service_id}{_MATRIX_PATH.suffix}")
    try:
        if tenant_path.exists():
            with tenant_path.open() as f:
                m = _json.load(f)
            if isinstance(m, dict) and m:
                return m
    except Exception:
        logger.debug(f"[_load_matrix] local {tenant_path.name} read failed", exc_info=True)

    # 2. FOS-published matrix.
    try:
        from backend.state_sync import fetch_matrix_from_fos

        m = fetch_matrix_from_fos(service_id)
        if m:
            return m
    except Exception:
        logger.debug("[_load_matrix] FOS matrix fetch failed", exc_info=True)

    # 3. Default-empty matrix bundled with the image.
    default_path = _MATRIX_PATH.parent / "matrix.default.json"
    try:
        if default_path.exists():
            with default_path.open() as f:
                m = _json.load(f)
            if isinstance(m, dict) and m:
                return m
    except Exception:
        logger.debug("[_load_matrix] default matrix.json read failed", exc_info=True)

    return None


from backend.repositories import session_scoring as _scoring_repo


# Convenience wrappers — exist so unit tests can monkey-patch
# ``backend.repositories.session_scoring.query_logs`` (etc.) and have the
# patches intercept calls from this module. Plain ``from X import Y as _Y``
# binds ``_Y`` to the function object at import time and ignores later
# attribute rebinding on the source module; going through the module
# attribute each call sidesteps that.
def _query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    return _scoring_repo.query_logs(service_id, sql, params)


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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    try:
        rows = _query_logs(service_id, f"DESCRIBE {table}")
    except Exception:  # noqa: BLE001 — view not ready / table missing → no columns
        return set()
    return {str(r.get("column_name")) for r in rows if r.get("column_name")}


# Synthesize-missing-scoring-columns helpers live in the repository (the
# canonical home, since this module imports the repo and not vice-versa) so
# the per-endpoint queries here and the per-sid event hydration in
# fetch_session_events share ONE definition of the column contract. Aliased
# back into this module's namespace so the call sites below (and the tests)
# read as local helpers. See repo docstring for the why.
_SCORING_COLUMN_TYPES = _scoring_repo._SCORING_COLUMN_TYPES
_scoring_source = _scoring_repo.scoring_source


def _fetch_session_events(
    service_id: str,
    sids: list[str],
    since_days: int = 30,
    limit_per_sid: int = 500,
    *,
    ts_predicate: str | None = None,
) -> dict[str, list[dict]]:
    return _scoring_repo.fetch_session_events(service_id, sids, since_days, limit_per_sid, ts_predicate=ts_predicate)


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


def _resolve_token(service_id: str, override_token: str = "") -> str:
    """Use the provided token, or fall back to the service-config's
    fastly_api_key. Returns empty string if neither is available — the
    caller raises an HTTPException in that case."""
    if override_token:
        return override_token
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if cfg:
        return cfg.get("fastly_api_key", "") or ""
    return ""


@router.post("/{service_id}/scoring/enable")
def scoring_enable(
    service_id: str = Path(..., description="Logging service ID to enable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Enable session scoring for the given logging service.

    Streams SSE status events while the orchestrator runs through:
    Compute service provisioning → Wasm deploy → VCL clone → backend +
    snippets + custom fields + format update → validate → activate."""
    token = body.token if body else ""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required (pass in JSON body or set in service config)"},
        )

    from backend.provision.orchestrator import run_with_events
    from backend.provision.session_scoring_orchestrator import enable_scoring

    def stream():
        import json

        yield json.dumps({"type": "status", "message": f"Enabling session scoring for {service_id}..."})

        try:
            for event in run_with_events(enable_scoring, service_id, resolved_token):
                yield json.dumps(event)
            # run_with_events captures the return value in its own scope; we
            # don't have direct access here. Re-load the config to surface
            # the post-state in the final SSE event.
            from backend import config as svcconfig

            cfg = svcconfig.load_config(service_id) or {}
            scoring = cfg.get("scoring", {})
            safe_scoring = {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}
            yield json.dumps(
                {
                    "type": "done",
                    "message": "Session scoring enabled.",
                    "scoring": safe_scoring,
                }
            )
            from backend.core import metadata as metadata_db

            metadata_db.record_scoring_audit(
                service_id,
                "scoring_enabled",
                details={
                    "scoring_service_id": safe_scoring.get("scoring_service_id"),
                    "matrix_version": safe_scoring.get("matrix_version"),
                },
            )
            metadata_db.record_audit(
                service_id=service_id,
                event_type="scoring_enabled",
                details={
                    "scoring_service_id": safe_scoring.get("scoring_service_id"),
                    "matrix_version": safe_scoring.get("matrix_version"),
                },
                actor="operator",
            )
        except Exception as e:
            logger.exception("scoring_enable failed for %s", service_id)
            from backend.provision.session_scoring_setup import EntitlementError

            err_event = {"type": "error", "message": str(e)}
            # A required Fastly product (Compute / Config Store / KV Store) isn't
            # enabled — surface the machine code + manage.fastly.com deep link so
            # the SSE modal renders a clickable "enable it" message instead of a
            # raw HTTP 4xx string.
            if isinstance(e, EntitlementError):
                err_event["code"] = e.code
                if e.link:
                    err_event["link"] = e.link
            yield json.dumps(err_event)

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


@router.post("/{service_id}/scoring/disable")
def scoring_disable(
    service_id: str = Path(..., description="Logging service ID to disable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Disable session scoring. Reverse of enable_scoring."""
    token = body.token if body else ""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required"},
        )

    from backend.provision.orchestrator import run_with_events
    from backend.provision.session_scoring_orchestrator import disable_scoring

    def stream():
        import json

        yield json.dumps({"type": "status", "message": f"Disabling session scoring for {service_id}..."})

        try:
            for event in run_with_events(disable_scoring, service_id, resolved_token):
                yield json.dumps(event)
            yield json.dumps({"type": "done", "message": "Session scoring disabled."})
            from backend.core import metadata as metadata_db

            metadata_db.record_scoring_audit(service_id, "scoring_disabled")
            metadata_db.record_audit(
                service_id=service_id,
                event_type="scoring_disabled",
                details={},
                actor="operator",
            )
        except Exception as e:
            logger.exception("scoring_disable failed for %s", service_id)
            yield json.dumps({"type": "error", "message": str(e)})

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


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
    sh = int(since_hours)

    result = {
        "top_flagged": scoring_top_flagged(request=request, service_id=service_id, since_hours=sh, limit=200),
        "score_distribution": scoring_score_distribution(request=request, service_id=service_id, since_hours=sh),
        "compliance_breakdown": scoring_compliance_breakdown(request=request, service_id=service_id, since_hours=sh),
        "latency_timeseries": scoring_latency_timeseries(request=request, service_id=service_id, since_hours=sh),
        "health": scoring_health(request=request, service_id=service_id, since_hours=sh),
    }
    if not _is_analyst_request(request):
        from backend.routers.session_scoring_admin import scoring_evaluation_per_reason

        result["evaluation"] = scoring_evaluation(service_id=service_id)
        result["evaluation_per_reason"] = scoring_evaluation_per_reason(service_id=service_id)
    return result


@router.get("/{service_id}/scoring/config")
def scoring_config_composite(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Composite of the four token-free /scoring/* config endpoints
    (status, threshold, exclude-regex, enforce-status-code). The admin
    session-scoring page was firing four parallel GETs on mount; each
    is a sub-50ms local config read so cold-load cost is dominated by
    HTTP overhead rather than computation. Combining them into one
    round-trip saves ~300-500ms on the cold-load waterfall.

    Excluded: /scoring/enforce-threshold (requires a Fastly API token
    and makes a network round-trip out — frontend should fetch that
    one separately if it needs the live edge-side value).

    Granular endpoints unchanged so the frontend can keep using them
    individually during a rollback.
    """
    from backend.routers.session_scoring_admin import (
        scoring_enforce_status_code_get,
        scoring_exclude_regex_get,
        scoring_threshold_get,
    )

    return {
        "status": scoring_status(service_id),
        "threshold": scoring_threshold_get(service_id),
        "exclude_regex": scoring_exclude_regex_get(service_id),
        "enforce_status_code": scoring_enforce_status_code_get(service_id),
    }


@router.get("/{service_id}/scoring/status")
def scoring_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Return the scoring block from the service's config, or
    {"enabled": false} if scoring was never enabled."""
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    scoring = cfg.get("scoring")
    if not scoring or not scoring.get("enabled"):
        return {"enabled": False}
    result = {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}

    # Best-effort: surface the scoring Compute service's LIVE active version +
    # the time it was activated, so operators can confirm exactly what's
    # deployed at the edge (the runbook's "redeploy timestamp"). Cached 60s per
    # service so the polled status panel doesn't hit Fastly every refresh, and
    # never breaks the status page if the Fastly call fails.
    scoring_svc = scoring.get("scoring_service_id")
    token = (cfg.get("fastly_api_key") or "").strip()
    if scoring_svc and token:
        info = _scoring_svc_version_cache.get(scoring_svc)
        if info is None:
            from backend.core.fastly.service import get_active_version_info

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

    rows = _labels.list_labels(service_id, limit=limit)
    counts = _labels.counts_by_label(service_id)
    if _is_analyst_request(request):
        rows = [_project_label_for_analyst(r) for r in rows]
    return {"labels": rows, "counts": counts}


@router.post("/{service_id}/scoring/labels")
def scoring_labels_create(
    body: ScoringLabelCreate,
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Create or update a label. Upserts on (service_id, sid)."""
    from backend.scoring import labels as _labels
    from backend.utils.auth import require_service_in_scope

    require_service_in_scope(request, service_id)

    _strip_pii_fields_for_analyst(request, body)
    sid = body.sid.strip()
    label = body.label.strip()
    # save_label() itself validates sid + label and raises ValueError
    # with the same messages; the try/except below converts that into
    # HTTPException(400). Keeping the validation in one place (the
    # CRUD module) means in-process callers — not just HTTP — get the
    # same protection.
    try:
        row = _labels.save_label(
            service_id,
            sid=sid,
            label=label,
            notes=body.notes,
            flagged_by=body.flagged_by,
            sample_ip=body.sample_ip,
            sample_ua=body.sample_ua,
            sample_url=body.sample_url,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_label", str(e)))
    # Bust analytics cache so the next /top-flagged shows the new badge.
    _bust_analytics_cache(service_id)
    if _is_analyst_request(request):
        row = _project_label_for_analyst(row)
    return row


@router.patch("/{service_id}/scoring/labels/{label_id}")
def scoring_labels_update(
    body: ScoringLabelUpdate,
    request: Request,
    service_id: str = Path(...),
    label_id: str = Path(...),
) -> dict:
    from backend.scoring import labels as _labels

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
    _bust_analytics_cache(service_id)
    if _is_analyst_request(request):
        row = _project_label_for_analyst(row)
    return row


@router.delete("/{service_id}/scoring/labels/{label_id}")
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
    from backend.utils.auth import require_service_in_scope

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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    src = _scoring_source(table, _table_columns(service_id))
    ts_pred, win_disc = _scoring_time_window(request, since_hours)
    sql = f"""
        SELECT
            timestamp,
            edge_sid,
            edge_score,
            edge_score_l1,
            edge_score_l2,
            edge_cookie_compliance,
            edge_score_reason,
            ip,
            ua,
            url,
            status,
            country
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        ORDER BY edge_score DESC, timestamp DESC
        LIMIT {int(limit)}
    """
    # ``ip`` is masked for analysts by the response middleware's key-name
    # pass (the column is literally ``ip``); ``ua`` / ``url`` are left intact
    # because analysts triage flagged sessions on them.
    return _cached(
        ("top-flagged", service_id, since_hours, limit, win_disc),
        lambda: {"rows": _isoformat_ts(_query_logs(service_id, sql), "timestamp"), "since_hours": since_hours},
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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    src = _scoring_source(table, _table_columns(service_id))
    ts_pred, win_disc = _scoring_time_window(request, since_hours)
    sql = f"""
        SELECT
            date_trunc('hour', timestamp) AS hour,
            CASE
                WHEN edge_score < 25 THEN '0-25'
                WHEN edge_score < 50 THEN '25-50'
                WHEN edge_score < 75 THEN '50-75'
                ELSE '75-100'
            END AS bucket,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return _cached(
        ("score-distribution", service_id, since_hours, win_disc),
        lambda: {"rows": _isoformat_ts(_query_logs(service_id, sql), "hour"), "since_hours": since_hours},
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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    src = _scoring_source(table, _table_columns(service_id))
    ts_pred, win_disc = _scoring_time_window(request, since_hours)
    sql = f"""
        SELECT
            date_trunc('hour', timestamp) AS hour,
            edge_cookie_compliance AS compliance,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_cookie_compliance IS NOT NULL
          AND edge_cookie_compliance != ''
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return _cached(
        ("compliance-breakdown", service_id, since_hours, win_disc),
        lambda: {"rows": _isoformat_ts(_query_logs(service_id, sql), "hour"), "since_hours": since_hours},
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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    ts_pred, win_disc = _scoring_time_window(request, since_hours)

    cols = _table_columns(service_id)
    src = _scoring_source(table, cols)
    has_rtt = "edge_score_rtt_us" in cols
    has_exec = "edge_score_exec_us" in cols

    # A 1h window at hourly granularity is one bar; drop to minute buckets for
    # short windows so the chart is actually a time-series. Whitelisted literal
    # (never interpolated from user input) so it's safe in the f-string SQL.
    bucket = "minute" if int(since_hours) <= 1 else "hour"

    select_parts = [
        f"date_trunc('{bucket}', timestamp) AS hour",
        "COUNT(*) FILTER (WHERE edge_score IS NOT NULL) AS scored_count",
        "COUNT(*) FILTER (WHERE edge_score_reason ILIKE '%compute-unavailable%') AS fail_open_count",
        "COUNT(*) AS total_count",
    ]
    if has_rtt:
        select_parts += [
            "CAST(quantile_cont(edge_score_rtt_us, 0.5)  FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p50_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.95) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p95_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.99) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p99_us",
        ]
    if has_exec:
        select_parts += [
            "CAST(quantile_cont(edge_score_exec_us, 0.5)  FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p50_us",
            "CAST(quantile_cont(edge_score_exec_us, 0.95) FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p95_us",
        ]
    select_sql = ",\n            ".join(select_parts)
    sql = f"""
        SELECT
            {select_sql}
        FROM {src}
        -- Scored rows are edge requests even when the scoring restart left
        -- edge != true on them; include them so the distribution isn't empty.
        WHERE (edge = true OR edge_score IS NOT NULL)
          AND {ts_pred}
        GROUP BY 1
        ORDER BY 1
    """
    return _cached(
        ("latency-timeseries", service_id, since_hours, win_disc),
        lambda: {
            "rows": _isoformat_ts(_query_logs(service_id, sql), "hour"),
            "since_hours": since_hours,
            "has_latency": has_rtt or has_exec,
            "granularity": bucket,
        },
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
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    ts_pred, win_disc = _scoring_time_window(request, since_hours)

    # Scorer-latency columns are optional (present only after a service is
    # re-provisioned post-2026-06-17). Detect them so the query degrades to
    # the pre-latency shape on older services instead of a binder error.
    cols = _table_columns(service_id)
    src = _scoring_source(table, cols)
    has_rtt = "edge_score_rtt_us" in cols
    has_exec = "edge_score_exec_us" in cols
    _rtt_cte = ",\n                edge_score_rtt_us" if has_rtt else ""
    _exec_cte = ",\n                edge_score_exec_us" if has_exec else ""
    _lat_parts: list[str] = []
    if has_rtt:
        # RTT over ALL routed edge rows (incl. fail-opens, which sit ≈the
        # timeout budget) — that's the distribution to compare against the
        # backend timeout when tuning the fail-open rate.
        _lat_parts += [
            "(SELECT quantile_cont(edge_score_rtt_us, 0.5)  FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p50_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.95) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p95_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.99) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p99_us",
            "(SELECT MAX(edge_score_rtt_us) FROM edge_rows)                                                     AS rtt_max_us",
        ]
    if has_exec:
        # Exec over SCORED rows only (fail-opens never carry a self-time).
        _lat_parts += [
            "(SELECT quantile_cont(edge_score_exec_us, 0.5)  FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p50_us",
            "(SELECT quantile_cont(edge_score_exec_us, 0.95) FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p95_us",
        ]
    _latency_select = (",\n            " + ",\n            ".join(_lat_parts)) if _lat_parts else ""

    sql = f"""
        WITH edge_rows AS (
            SELECT
                edge_score,
                edge_score_l2,
                edge_score_reason,
                edge_cookie_compliance,
                edge_sid{_rtt_cte}{_exec_cte}
            FROM {src}
            -- Scored rows ARE edge requests (the scorer only runs at the edge),
            -- but the scoring restart means the capture VCL may not stamp
            -- edge=true on them. Include them explicitly so scored traffic isn't
            -- dropped from the health view (Fire Rate 0% despite live scoring).
            WHERE (edge = true OR edge_score IS NOT NULL)
              AND {ts_pred}
        ),
        scored AS (
            SELECT * FROM edge_rows WHERE edge_score IS NOT NULL
        ),
        reason_rows AS (
            SELECT trim(reason) AS reason
            FROM (
                SELECT unnest(string_split(edge_score_reason, ',')) AS reason
                FROM scored
                WHERE edge_score_reason IS NOT NULL AND edge_score_reason != ''
            )
            WHERE trim(reason) != ''
        ),
        top_reasons AS (
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            GROUP BY reason
            ORDER BY n DESC
            LIMIT 10
        ),
        fail_open_reasons AS (
            -- Fail-open breakdown by EXACT reason string so the operator can
            -- tell compute-unavailable-503 (scorer timeout/unavailable) from
            -- -500 (Wasm trap) / -401 (auth mismatch) / internal-error-keys
            -- (KV/key load) — each is a distinct remediation. The VCL deliver
            -- snippet rewrites every non-200 scorer response to
            -- "compute-unavailable-<status>", so the scorer's 401-unauthorized
            -- lands here as "compute-unavailable-401" (caught by the first
            -- filter) — there is no bare "unauthorized" log reason (EC-05). The
            -- Rust scorer returns 200 + a bare "internal-error-keys" reason on a
            -- key-load failure (caught by the second filter). NOT limited (unlike
            -- top_reasons) — fail-opens are rare and every class matters.
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            WHERE reason ILIKE 'compute-unavailable%'
               OR reason ILIKE 'internal-error%'
            GROUP BY reason
            ORDER BY n DESC
        )
        SELECT
            (SELECT COUNT(*) FROM edge_rows)                                    AS total_edge_rows,
            (SELECT COUNT(*) FROM scored)                                       AS scored_rows,
            (SELECT COUNT(DISTINCT edge_sid) FROM scored WHERE edge_sid <> '') AS distinct_sids,
            (SELECT AVG(edge_score) FROM scored)                                AS avg_score,
            (SELECT quantile_cont(edge_score, 0.5) FROM scored)                 AS p50_score,
            (SELECT quantile_cont(edge_score, 0.95) FROM scored)                AS p95_score,
            (SELECT MAX(edge_score) FROM scored)                                AS max_score,
            (SELECT COUNT(*) FROM scored
              WHERE edge_score_reason ILIKE '%compute-unavailable%')            AS scorer_errors,
            (SELECT list({{'reason': reason, 'count': n}}) FROM top_reasons)    AS top_reasons,
            (SELECT list({{'reason': reason, 'count': n}}) FROM fail_open_reasons) AS fail_open_breakdown,
            -- Matrix-staleness signal: fraction of scored rows whose L2
            -- transition score is "high" (≥50), meaning the matrix gave
            -- that transition low probability. When this fraction climbs
            -- (3× baseline per research §7.5), the matrix is drifting
            -- relative to current traffic and a retrain is warranted.
            -- L2 of 0 means "didn't trip a rare-transition rule" — used
            -- in the denominator of the staleness fraction filtered to
            -- rows we ACTUALLY evaluated L2 on (excludes cookie-missing
            -- shortcuts that bypass the matrix).
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 IS NOT NULL)       AS l2_evaluated,
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 >= 50)             AS l2_high_count{_latency_select}
    """

    def _produce() -> dict:
        rows = _query_logs(service_id, sql)
        row = rows[0] if rows else {}
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
        def _opt_int(key: str) -> int | None:
            v = row.get(key)
            return int(v) if v is not None else None

        latency = {
            "available": has_rtt or has_exec,
            "rtt_p50_us": _opt_int("rtt_p50_us"),
            "rtt_p95_us": _opt_int("rtt_p95_us"),
            "rtt_p99_us": _opt_int("rtt_p99_us"),
            "rtt_max_us": _opt_int("rtt_max_us"),
            "exec_p50_us": _opt_int("exec_p50_us"),
            "exec_p95_us": _opt_int("exec_p95_us"),
        }

        # L2-enforcement opt-in + deployment-age readiness gauge. Admin-only —
        # it reads the scoring_config ConfigStore (and the consuming card is
        # admin); analysts get None so the analyst-safe health composite stays a
        # pure DuckDB read. Best-effort + separately cached, so it can never
        # break this response. "What L2 would flag" is already surfaced above as
        # matrix_staleness.l2_high_pct — the UI reuses it rather than recomputing.
        l2_enforce = None
        if not _is_analyst_request(request):
            from backend.routers.session_scoring_admin import l2_enforce_readiness_block

            l2_enforce = l2_enforce_readiness_block(service_id)

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
            "scorer_errors": int(row.get("scorer_errors") or 0),
            # SRE-15: traffic-normalized fail-open rate. The bare count above
            # rises with request volume, so a fixed count threshold cries wolf
            # under load and stays silent on a low-traffic spike; the rate is
            # the stable spike-arm the UI tones on.
            "fail_open_rate_pct": round(int(row.get("scorer_errors") or 0) / total * 100.0, 2) if total else 0.0,
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


@router.get(
    "/{service_id}/scoring/evaluation", response_model=ScoringEvaluationResponse, response_model_exclude_unset=True
)
def scoring_evaluation(
    service_id: str = Path(...),
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

    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)
    n_neutral = counts.get("neutral", 0)

    # The cache key includes the label count so a new label invalidates
    # the previous snapshot even if the explicit cache bust were to miss.
    cache_key = ("scoring-evaluation", service_id, n_good, n_bad, n_neutral)

    def _produce() -> dict:
        from backend.config import load_config

        cfg = load_config(service_id) or {}
        matrix_version = (cfg.get("scoring") or {}).get("matrix_version") or "unknown"

        if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
            return {
                "has_min_samples": False,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
                "n_neutral": n_neutral,
                "matrix_version": matrix_version,
            }

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

    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)

    def _produce() -> dict:
        if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
            return {
                "has_min_samples": False,
                "min_per_class": _MIN_LABELS_PER_CLASS,
                "n_good": n_good,
                "n_bad": n_bad,
            }

        label_rows = _labels.list_labels(service_id)
        # Reconstruct labeled sessions and extract their max persisted scores.
        # Same path the AUC endpoint uses, so the curve is consistent with
        # the headline AUC number.
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        scored: list[tuple[int, str]] = []
        for session, label in labeled_sessions:
            if label not in ("good", "bad"):
                continue
            score = session.get("max_edge_score")
            if score is None:
                continue
            scored.append((int(score), label))

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
            fn = total_pos - tp
            tpr = tp / total_pos if total_pos else 0.0
            fpr = fp / total_neg if total_neg else 0.0
            precision = (tp / (tp + fp)) if (tp + fp) else 1.0  # convention: empty flagged set → precision 1
            recall = tpr
            roc.append({"threshold": t, "fpr": round(fpr, 4), "tpr": round(tpr, 4)})
            pr.append({"threshold": t, "precision": round(precision, 4), "recall": round(recall, 4)})

        # AUC via trapezoidal integration over the ROC points (sorted by
        # fpr ascending). AP same idea over PR.
        def _trapz(points: list[tuple[float, float]]) -> float:
            if len(points) < 2:
                return 0.0
            pts = sorted(points, key=lambda p: p[0])
            area = 0.0
            for i in range(1, len(pts)):
                x0, y0 = pts[i - 1]
                x1, y1 = pts[i]
                area += (x1 - x0) * (y0 + y1) / 2.0
            return area

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
    from backend.core.duckdb import _safe_table_name
    from backend.scoring import labels as _labels

    table = _safe_table_name(service_id)
    src = _scoring_source(table, _table_columns(service_id))
    ts_pred, win_disc = _scoring_time_window(request, since_hours)
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
        flagged_good = flagged_bad = passed_good = passed_bad = 0
        labeled_pairs = [(s, lbl) for s, lbl in sid_to_label.items() if s]
        if labeled_pairs:
            values_placeholders = ", ".join("(?, ?)" for _ in labeled_pairs)
            values_params: tuple = tuple(p for pair in labeled_pairs for p in pair)
            single_sql = f"""
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    -- edge_score IS NOT NULL already scopes to scored (edge)
                    -- requests; an explicit edge = true would drop scored rows
                    -- whose edge flag the scoring restart left unset.
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                ),
                labels(edge_sid, label) AS (
                    VALUES {values_placeholders}
                ),
                joined AS (
                    SELECT s.max_score, l.label
                    FROM sid_scores s
                    LEFT JOIN labels l USING (edge_sid)
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold_int} THEN 1 ELSE 0 END) AS flagged_total,
                    SUM(CASE WHEN max_score >= {threshold_int} AND label = 'good' THEN 1 ELSE 0 END) AS flagged_good,
                    SUM(CASE WHEN max_score >= {threshold_int} AND label = 'bad'  THEN 1 ELSE 0 END) AS flagged_bad,
                    SUM(CASE WHEN max_score <  {threshold_int} AND label = 'good' THEN 1 ELSE 0 END) AS passed_good,
                    SUM(CASE WHEN max_score <  {threshold_int} AND label = 'bad'  THEN 1 ELSE 0 END) AS passed_bad
                FROM joined
            """
            rows = _query_logs(service_id, single_sql, values_params)
            r = rows[0] if rows else {}
            total = int(r.get("total") or 0)
            flagged_total = int(r.get("flagged_total") or 0)
            passed_total = total - flagged_total
            flagged_good = int(r.get("flagged_good") or 0)
            flagged_bad = int(r.get("flagged_bad") or 0)
            passed_good = int(r.get("passed_good") or 0)
            passed_bad = int(r.get("passed_bad") or 0)
        else:
            # No labels yet — single aggregate scan with no labels join.
            agg_sql = f"""
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    -- edge_score IS NOT NULL already scopes to scored (edge)
                    -- requests; an explicit edge = true would drop scored rows
                    -- whose edge flag the scoring restart left unset.
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold_int} THEN 1 ELSE 0 END) AS flagged_total
                FROM sid_scores
            """
            agg_rows = _query_logs(service_id, agg_sql)
            agg = agg_rows[0] if agg_rows else {}
            total = int(agg.get("total") or 0)
            flagged_total = int(agg.get("flagged_total") or 0)
            passed_total = total - flagged_total

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
            "flagged": {
                "total": flagged,
                "good": flagged_good,
                "bad": flagged_bad,
                "unlabeled": flagged_unlabeled,
            },
            "passed": {
                "good": passed_good,
                "bad": passed_bad,
                "unlabeled": passed_unlabeled,
            },
            "precision": round(precision, 4) if precision is not None else None,
            "recall": round(recall, 4) if recall is not None else None,
        }

    # Label-count-aware cache key so a new label invalidates correctly.
    counts = _labels.counts_by_label(service_id)
    n_labels = counts.get("good", 0) + counts.get("bad", 0)
    return _cached(("threshold-preview", service_id, threshold_int, since_hours, n_labels, win_disc), _produce)


# ── Admin / training endpoints (carved out for file-size budget) ────────────
#
# Imported for side effects: registers the admin endpoints on ``router``
# via decorator. Must be at the BOTTOM of the file so this module's
# top-level definitions (router, logger, helpers, constants) are bound
# before the admin module pulls them.
from backend.routers import session_scoring_admin  # noqa: F401,E402

# A-3 (CacheRegistry): register the analytics + reconstruct caches.
# Same leak pattern as the iceberg / dashboard caches the R-1 work
# uncovered — _reconstruct_labeled_sessions_cache especially is a
# TTL-cached function that defeats per-test patches on
# reconstruct_labeled_sessions when a prior test populated it first.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("session_scoring._analytics_cache", _analytics_cache)
_CacheRegistry.register("session_scoring._inflight", _inflight)
_CacheRegistry.register("session_scoring._reconstruct_labeled_sessions_cache", _reconstruct_labeled_sessions_cache)
