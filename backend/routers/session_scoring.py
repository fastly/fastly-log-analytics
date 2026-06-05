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

import logging
import os

from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.responses import StreamingResponse

from backend.utils.router_utils import SSE_HEADERS as _SSE_HEADERS
from backend.utils.router_utils import sse_flush_preamble as _sse_flush

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/services", tags=["session-scoring"])

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
# Global lock guards _analytics_cache + _inflight dict mutations only —
# the actual `producer()` call runs under a PER-KEY lock so concurrent
# misses on DIFFERENT keys (the dashboard's 8-card mount pattern) run
# in parallel instead of queueing through one mutex. The cache's own
# RLock is held INSIDE this outer lock by way of the bounded-cache
# implementation; RLock re-entry from the same thread is safe.
_analytics_cache_lock = threading.Lock()
_inflight: dict[tuple, threading.Lock] = {}


def _cached(key: tuple, producer):
    """Return cached value if fresh, else produce + store.

    In-flight collapse: concurrent callers on the SAME key serialize
    through a per-key lock so the underlying query runs once and the
    rest get the cached value when they acquire the lock. Concurrent
    callers on DIFFERENT keys (the dashboard mount fires 8 endpoints
    with 8 different keys) run in parallel — they only contend on the
    global lock during the brief cache-lookup + per-key-lock-handoff
    window."""
    with _analytics_cache_lock:
        # Capture `now` INSIDE the lock so the freshness check evaluates
        # against the lock-acquisition timestamp, not a stale value from
        # before lock contention drained. A hot key under load can have
        # 10-50ms of lock-wait — using a pre-lock `now` would flag a
        # still-fresh entry as expired and trigger an extra producer call.
        now = _time.monotonic()
        entry = _analytics_cache.get(key)
        if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
            return entry[1]
        # Miss — claim the per-key lock under the global lock so two
        # concurrent misses on the same key don't both create new locks.
        key_lock = _inflight.get(key)
        if key_lock is None:
            key_lock = threading.Lock()
            _inflight[key] = key_lock

    # Hold the per-key lock only. The first miss runs producer(); the
    # second-through-Nth miss waits here, then sees the cached entry on
    # the re-check inside the lock and returns it without re-running.
    with key_lock:
        with _analytics_cache_lock:
            now = _time.monotonic()
            entry = _analytics_cache.get(key)
            if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
                return entry[1]
        # Actual producer call happens OUTSIDE the global lock so other
        # keys can be served while this one is computing.
        value = producer()
        with _analytics_cache_lock:
            # Re-capture now after producer() so the TTL clock starts
            # from when the value was actually computed, not from when
            # we entered _cached.
            _analytics_cache[key] = (_time.monotonic(), value)
            # Drop the per-key lock entry — small saving but bounds the
            # _inflight dict growth across the long-running TTL window.
            _inflight.pop(key, None)
        return value


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


def _load_matrix(service_id: str | None = None) -> dict | None:
    """Load the trained L2 transition matrix.

    Resolution order:
      1. ``compute/scorer/matrix.json`` on local disk (trained + present)
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

    # 1. Local trained matrix.
    try:
        if _MATRIX_PATH.exists():
            with _MATRIX_PATH.open() as f:
                m = _json.load(f)
            if isinstance(m, dict) and m:
                return m
    except Exception:
        logger.debug("[_load_matrix] local matrix.json read failed", exc_info=True)

    # 2. FOS-published matrix (only when a service id is in scope; the
    # AUC endpoint always has one).
    if service_id:
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


def _fetch_session_events(
    service_id: str,
    sids: list[str],
    since_days: int = 30,
    limit_per_sid: int = 500,
) -> dict[str, list[dict]]:
    """Return ``{sid: [{ts, url, status, ip, ua, edge_score, edge_cookie_compliance, edge_score_reason}, ...]}``
    for every sid in ``sids`` whose events landed in DuckDB within the
    last ``since_days`` days.

    Sids that have no rows in the window are dropped from the result
    (not present in the returned dict). The per-sid event cap is a
    safety bound — a runaway session with 10k+ requests would otherwise
    bloat the response; 500 events covers any realistic browsing pattern.
    """
    if not sids:
        return {}

    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    placeholders = ",".join("?" for _ in sids)
    # 010: push the per-sid LIMIT into SQL via ``row_number() OVER
    # (PARTITION BY edge_sid ORDER BY timestamp)``. The previous shape
    # let DuckDB materialise the full result set in Python before the
    # ``len(bucket) >= limit_per_sid`` guard ran — a single attacker
    # session with millions of events could OOM the backend before any
    # Python code saw a row. The CTE caps at ``limit_per_sid`` rows
    # per sid AT THE STORAGE LAYER so the worst-case memory footprint
    # is ``len(sids) × limit_per_sid`` regardless of attacker volume.
    per_sid_cap = int(limit_per_sid)
    sql = f"""
        WITH ranked AS (
            SELECT edge_sid, timestamp AS ts, url, status, ip, ua,
                   edge_score, edge_cookie_compliance, edge_score_reason,
                   row_number() OVER (PARTITION BY edge_sid ORDER BY timestamp) AS _rn
            FROM {table}
            WHERE edge_sid IN ({placeholders})
              AND timestamp >= now() - INTERVAL {int(since_days)} DAY
        )
        SELECT edge_sid, ts, url, status, ip, ua,
               edge_score, edge_cookie_compliance, edge_score_reason
        FROM ranked
        WHERE _rn <= {per_sid_cap}
        ORDER BY edge_sid, ts
    """
    rows = _query_logs(service_id, sql, tuple(sids))

    grouped: dict[str, list[dict]] = {}
    for r in rows:
        sid = r.get("edge_sid")
        if not sid:
            continue
        bucket = grouped.setdefault(sid, [])
        if len(bucket) >= limit_per_sid:
            continue
        # Stringify the timestamp for JSON serialization. DuckDB returns
        # datetime objects which FastAPI's default JSON encoder rejects
        # in nested arrays (only the top-level Pydantic model serializer
        # handles them).
        ts = r.get("ts")
        bucket.append(
            {
                "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts is not None else None,
                "url": r.get("url") or "/",
                "status": r.get("status"),
                "ip": r.get("ip"),
                "ua": r.get("ua"),
                "edge_score": r.get("edge_score"),
                "edge_cookie_compliance": r.get("edge_cookie_compliance"),
                "edge_score_reason": r.get("edge_score_reason"),
            }
        )
    return grouped


def _reconstruct_labeled_sessions(service_id: str, labels: list[dict]) -> list[tuple[dict, str]]:
    """Replay each labeled sid into the {session_id, events:[{ts,url}]}
    shape that ``evaluate()`` expects.

    Each label stores only ``sid`` + sample fields. The actual event
    sequence lives in DuckDB as one row per request. We issue ONE query
    grouped by edge_sid + ordered by timestamp, then bucket rows into
    sessions in Python (DuckDB's ``list()`` aggregate would also work
    but the Python side is clearer and the volume is small — at most
    ``len(labels)`` sids).

    Returns (session_dict, label) tuples ready to pass to evaluate().
    Sids that don't appear in DuckDB (haven't been ingested yet, or were
    rotated away) are dropped silently — they contribute nothing to AUC
    either way.
    """
    if not labels:
        return []
    sid_to_label = {row["sid"]: row["label"] for row in labels if row.get("sid")}
    if not sid_to_label:
        return []
    grouped = _fetch_session_events(service_id, list(sid_to_label.keys()), since_days=30)
    out: list[tuple[dict, str]] = []
    for sid, label in sid_to_label.items():
        events = grouped.get(sid, [])
        if not events:
            continue  # sid never landed in DuckDB; can't evaluate
        # max_edge_score is what `evaluate_from_persisted_scores` consumes:
        # the actual score the live scorer returned (L1 + L2 + compliance
        # combined). Taking the MAX across the session matches the
        # production VCL behavior — a session is operationally caught at
        # its worst single transition, not its average. None-valued
        # rows are excluded so a sid with only un-scored events doesn't
        # collapse to max_edge_score=0.
        scored_values = [e.get("edge_score") for e in events if e.get("edge_score") is not None]
        max_score = max(scored_values) if scored_values else None
        out.append(
            (
                {
                    "session_id": sid,
                    "events": events,
                    "max_edge_score": max_score,
                },
                label,
            )
        )
    return out


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
    token: str = Query(default=""),
):
    """Enable session scoring for the given logging service.

    Streams SSE status events while the orchestrator runs through:
    Compute service provisioning → Wasm deploy → VCL clone → backend +
    snippets + custom fields + format update → validate → activate."""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required (pass ?token= or set in service config)"},
        )

    from backend.provision.orchestrator import run_with_events
    from backend.provision.session_scoring_orchestrator import enable_scoring
    from backend.utils.router_utils import sse_event

    def stream():
        yield from _sse_flush()
        yield from sse_event({"type": "status", "message": f"Enabling session scoring for {service_id}..."})

        try:
            for event in run_with_events(enable_scoring, service_id, resolved_token):
                yield from sse_event(event)
                yield f": {' ' * 256}\n\n"
            # run_with_events captures the return value in its own scope; we
            # don't have direct access here. Re-load the config to surface
            # the post-state in the final SSE event.
            from backend import config as svcconfig

            cfg = svcconfig.load_config(service_id) or {}
            scoring = cfg.get("scoring", {})
            safe_scoring = {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}
            yield from sse_event(
                {
                    "type": "done",
                    "message": "Session scoring enabled.",
                    "scoring": safe_scoring,
                }
            )
            from backend.core import metadata_db

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
            yield from sse_event({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/{service_id}/scoring/disable")
def scoring_disable(
    service_id: str = Path(..., description="Logging service ID to disable scoring on"),
    token: str = Query(default=""),
):
    """Disable session scoring. Reverse of enable_scoring."""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required"},
        )

    from backend.provision.orchestrator import run_with_events
    from backend.provision.session_scoring_orchestrator import disable_scoring
    from backend.utils.router_utils import sse_event

    def stream():
        yield from _sse_flush()
        yield from sse_event({"type": "status", "message": f"Disabling session scoring for {service_id}..."})

        try:
            for event in run_with_events(disable_scoring, service_id, resolved_token):
                yield from sse_event(event)
                yield f": {' ' * 256}\n\n"
            yield from sse_event({"type": "done", "message": "Session scoring disabled."})
            from backend.core import metadata_db

            metadata_db.record_scoring_audit(service_id, "scoring_disabled")
            metadata_db.record_audit(
                service_id=service_id,
                event_type="scoring_disabled",
                details={},
                actor="operator",
            )
        except Exception as e:
            logger.exception("scoring_disable failed for %s", service_id)
            yield from sse_event({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream", headers=_SSE_HEADERS)


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
    return {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}


# ── Labels (good / bad / neutral session tags) ──────────────────────────────


@router.get("/{service_id}/scoring/labels")
def scoring_labels_list(
    service_id: str = Path(..., description="Logging service ID"),
    limit: int = Query(default=500, ge=1, le=10000),
) -> dict:
    """Return all session labels for a service, most recent first."""
    from backend.scoring import labels as _labels

    rows = _labels.list_labels(service_id, limit=limit)
    counts = _labels.counts_by_label(service_id)
    return {"labels": rows, "counts": counts}


@router.post("/{service_id}/scoring/labels")
def scoring_labels_create(
    body: dict,
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Create or update a label. Upserts on (service_id, sid)."""
    from backend.scoring import labels as _labels

    sid = (body.get("sid") or "").strip()
    label = (body.get("label") or "").strip()
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
            notes=body.get("notes", ""),
            flagged_by=body.get("flagged_by", "admin"),
            sample_ip=body.get("sample_ip", ""),
            sample_ua=body.get("sample_ua", ""),
            sample_url=body.get("sample_url", ""),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    # Bust analytics cache so the next /top-flagged shows the new badge.
    _bust_analytics_cache(service_id)
    return row


@router.patch("/{service_id}/scoring/labels/{label_id}")
def scoring_labels_update(
    body: dict,
    service_id: str = Path(...),
    label_id: str = Path(...),
) -> dict:
    from backend.scoring import labels as _labels

    try:
        row = _labels.update_label(
            service_id,
            label_id,
            label=body.get("label"),
            notes=body.get("notes"),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    if not row:
        raise HTTPException(status_code=404, detail={"error": "label not found"})
    _bust_analytics_cache(service_id)
    return row


@router.delete("/{service_id}/scoring/labels/{label_id}")
def scoring_labels_delete(
    service_id: str = Path(...),
    label_id: str = Path(...),
) -> dict:
    from backend.scoring import labels as _labels

    result = _labels.delete_label(service_id, label_id)
    _bust_analytics_cache(service_id)
    return result


# ── Summary queries (top-flagged, distributions) ────────────────────────────


def _query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Tiny helper — run a SELECT against the per-service logs view and
    return list[dict].

    Why the try/finally + explicit close: get_connection() opens a fresh
    DuckDB connection per call by design (independent connections beat
    shared-cursor serialization under load — see backend/core/duckdb.py).
    Leaving them open here was the root cause of constant .duckdb-wal /
    .duckdb-shm file churn that ate ~1.5GB of mds_stores + VS Code
    extension-host RAM during the 2026-06-01 admin-page polling crash.
    Mirrors the canonical pattern from backend/routers/query.py.

    ``params`` is passed through to ``con.execute`` so callers can use
    parametrized queries (e.g. ``WHERE edge_sid IN (?, ?, ?)``) without
    string-formatting user-controlled values into the SQL."""
    from backend.core.duckdb import get_connection, get_source_for_service

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    con = None
    try:
        con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
        rows = con.execute(sql, params).fetchall() if params else con.execute(sql).fetchall()
        cols = [d[0] for d in con.description] if con.description else []
        return [dict(zip(cols, r)) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


@router.get("/{service_id}/scoring/top-flagged")
def scoring_top_flagged(
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
        FROM {table}
        WHERE edge_score IS NOT NULL
          AND timestamp >= now() - INTERVAL {int(since_hours)} HOUR
        ORDER BY edge_score DESC, timestamp DESC
        LIMIT {int(limit)}
    """
    return _cached(
        ("top-flagged", service_id, since_hours, limit),
        lambda: {"rows": _query_logs(service_id, sql), "since_hours": since_hours},
    )


@router.get("/{service_id}/scoring/score-distribution")
def scoring_score_distribution(
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Hourly buckets × score buckets (0, 25, 50, 75, 100). Returns a flat
    list of {hour, bucket, count} rows; the frontend pivots for the
    histogram."""
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
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
        FROM {table}
        WHERE edge_score IS NOT NULL
          AND timestamp >= now() - INTERVAL {int(since_hours)} HOUR
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return _cached(
        ("score-distribution", service_id, since_hours),
        lambda: {"rows": _query_logs(service_id, sql), "since_hours": since_hours},
    )


@router.get("/{service_id}/scoring/compliance-breakdown")
def scoring_compliance_breakdown(
    service_id: str = Path(...),
    since_hours: int = Query(default=24, ge=1, le=168),
) -> dict:
    """Hourly count grouped by edge_cookie_compliance (ok / missing /
    tampered / expired / unknown)."""
    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    sql = f"""
        SELECT
            date_trunc('hour', timestamp) AS hour,
            edge_cookie_compliance AS compliance,
            COUNT(*) AS count
        FROM {table}
        WHERE edge_cookie_compliance IS NOT NULL
          AND edge_cookie_compliance != ''
          AND timestamp >= now() - INTERVAL {int(since_hours)} HOUR
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return _cached(
        ("compliance-breakdown", service_id, since_hours),
        lambda: {"rows": _query_logs(service_id, sql), "since_hours": since_hours},
    )


@router.get("/{service_id}/scoring/health")
def scoring_health(
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
    interval = int(since_hours)
    sql = f"""
        WITH edge_rows AS (
            SELECT
                edge_score,
                edge_score_l2,
                edge_score_reason,
                edge_cookie_compliance,
                edge_sid
            FROM {table}
            WHERE edge = true
              AND timestamp >= now() - INTERVAL {interval} HOUR
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
              WHERE edge_score_reason ILIKE '%compute-unavailable%'
                 OR edge_score_reason ILIKE '%unauthorized%')                   AS scorer_errors,
            (SELECT list({{'reason': reason, 'count': n}}) FROM top_reasons)    AS top_reasons,
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
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 >= 50)             AS l2_high_count
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
            "top_reasons": row.get("top_reasons") or [],
            "matrix_staleness": {
                "l2_evaluated": l2_evaluated,
                "l2_high_count": l2_high,
                "l2_high_pct": round(l2_high_pct, 2),
                "is_stale": matrix_stale,
                "threshold_pct": 25.0,
            },
        }

    return _cached(("scoring-health", service_id, since_hours), _produce)


# ── Matrix quality (ROC-AUC against accumulated labels) ─────────────────────


# Below this many of EACH class (good / bad), AUC is too noisy to be a
# useful signal — surface a "need more labels" CTA instead so the operator
# isn't tempted to act on a 0.5 ± wildly-bouncing number from sub-3
# samples.
_MIN_LABELS_PER_CLASS = 3


@router.get("/{service_id}/scoring/evaluation")
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
                "error": "Trained matrix is missing on disk (compute/scorer/matrix.json). "
                "Run scripts/scoring/train.py to produce one.",
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


@router.get("/{service_id}/scoring/curves")
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
    """
    from backend.scoring import labels as _labels

    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)

    if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
        return {
            "has_min_samples": False,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": n_good,
            "n_bad": n_bad,
        }

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


# ── Threshold preview (counterfactual: at threshold X, what flips?) ─────────


@router.get("/{service_id}/scoring/threshold-preview")
def scoring_threshold_preview(
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
    interval = int(since_hours)
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
        # Two queries now run:
        #   (a) one aggregate row across all sids (total + flagged +
        #       passed counts — fixed-size result regardless of fleet
        #       size);
        #   (b) only the LABELED sids (bounded by label storage; the
        #       UI caps practical label sets in the low thousands).
        #
        # Python then computes the labeled splits and derives the
        # unlabeled splits by subtraction. Worst-case materialisation
        # is ``len(sid_to_label)`` rows — no longer attacker-controlled.
        agg_sql = f"""
            WITH sid_scores AS (
                SELECT edge_sid, MAX(edge_score) AS max_score
                FROM {table}
                WHERE edge = true
                  AND edge_score IS NOT NULL
                  AND edge_sid IS NOT NULL
                  AND edge_sid <> ''
                  AND timestamp >= now() - INTERVAL {interval} HOUR
                GROUP BY edge_sid
            )
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN max_score >= {threshold_int} THEN 1 ELSE 0 END) AS flagged_total,
                SUM(CASE WHEN max_score <  {threshold_int} THEN 1 ELSE 0 END) AS passed_total
            FROM sid_scores
        """
        agg_rows = _query_logs(service_id, agg_sql)
        agg = agg_rows[0] if agg_rows else {}
        total = int(agg.get("total") or 0)
        flagged_total = int(agg.get("flagged_total") or 0)
        passed_total = int(agg.get("passed_total") or 0)

        flagged_good = flagged_bad = passed_good = passed_bad = 0
        labeled_sids = [s for s in sid_to_label if s]
        if labeled_sids:
            placeholders = ",".join("?" for _ in labeled_sids)
            label_sql = f"""
                SELECT edge_sid, MAX(edge_score) AS max_score
                FROM {table}
                WHERE edge_sid IN ({placeholders})
                  AND edge = true
                  AND edge_score IS NOT NULL
                  AND timestamp >= now() - INTERVAL {interval} HOUR
                GROUP BY edge_sid
            """
            label_rows_sql = _query_logs(service_id, label_sql, tuple(labeled_sids))
            for r in label_rows_sql:
                sid = r.get("edge_sid")
                score = r.get("max_score") or 0
                label = sid_to_label.get(sid)
                if label not in ("good", "bad"):
                    continue
                if score >= threshold_int:
                    if label == "good":
                        flagged_good += 1
                    else:
                        flagged_bad += 1
                else:
                    if label == "good":
                        passed_good += 1
                    else:
                        passed_bad += 1

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
    return _cached(("threshold-preview", service_id, threshold_int, since_hours, n_labels), _produce)


# ── Retrain pipeline ────────────────────────────────────────────────────────


@router.post("/{service_id}/scoring/retrain")
def scoring_retrain(
    service_id: str = Path(...),
    since_days: int = Query(default=7, ge=1, le=90, description="Window of DuckDB traffic to train on"),
    version: str | None = Query(default=None, description="Override matrix version label; defaults to today's date"),
) -> dict:
    """Build a fresh transition matrix from the last N days of DuckDB
    traffic, save it to ``compute/scorer/matrix.json``, publish to FOS,
    and evaluate AUC against the operator's accumulated labels.

    Synchronous — for a 7-day window with ~10k sessions the whole pipeline
    runs in <30s. The endpoint returns the new matrix metadata + AUC so
    the UI can show "matrix moved from 0.62 → 0.91 after retrain". The
    Wasm build + Compute deploy is a separate step (requires Fastly CLI
    + Rust toolchain on the operator's box — not Docker-friendly): the
    response includes a hint pointing at ``scripts/scoring/deploy_wasm.sh``.

    Pipeline:
      1. extract_traces from DuckDB → in-memory sessions
      2. build_matrix → TransitionMatrix
      3. evaluate AUC against labels (if >=3 each class)
      4. Save matrix.json to disk + publish to FOS
      5. Bust the /scoring/evaluation cache
    """
    import datetime as _dt

    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH
    from backend.scoring import fixtures as _fixtures
    from backend.scoring import labels as _labels
    from backend.scoring import matrix as _matrix
    from backend.scoring.evaluate import DEFAULT_MIN_AUC
    from backend.scoring.evaluate import evaluate as _evaluate

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    cfg = svcconfig.load_config(service_id) or {}
    matrix_version = version or _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d-r")
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
            pass

    matrix_dict = tmatrix.to_json_dict(version=matrix_version)

    # 3. Evaluate against accumulated labels if we have enough of each.
    auc_result = None
    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    if counts.get("good", 0) >= _MIN_LABELS_PER_CLASS and counts.get("bad", 0) >= _MIN_LABELS_PER_CLASS:
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
        _MATRIX_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _MATRIX_PATH.open("w") as f:
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

    # 5. Bust analytics caches so the next StatusPanel hit sees the new AUC.
    _bust_analytics_cache(service_id)

    # Operator audit: every retrain is attributable + reviewable.
    from backend.core import metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_retrained",
        details={
            "matrix_version": matrix_version,
            "since_days": since_days,
            "sessions_trained_on": tmatrix.session_count,
            "auc_against_labels": auc_result,
            "fos_published": fos_published,
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
        "deploy_hint": (
            "Run scripts/scoring/deploy_wasm.sh --service-id "
            f"{(cfg.get('scoring') or {}).get('scoring_service_id', '?')} from your local box "
            "to embed this matrix into the Wasm and push to Fastly Compute. "
            "Until then the live scorer keeps using its previously-embedded matrix; "
            "the /scoring/evaluation endpoint will reflect the new matrix immediately "
            "(it reads matrix.json + FOS, not the deployed Wasm)."
        ),
    }


# ── Session details (sid → page sequence) ────────────────────────────────────


@router.get("/{service_id}/scoring/sessions/{sid}/events")
def scoring_session_events(
    service_id: str = Path(...),
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
    """
    grouped = _fetch_session_events(service_id, [sid], since_days=since_days)
    events = grouped.get(sid, [])
    return {
        "sid": sid,
        "since_days": since_days,
        "event_count": len(events),
        "events": events,
    }


# ── Threshold enforcement (live blocking via Compute ConfigStore) ──────────


_ENFORCE_THRESHOLD_KEY = "enforce_threshold"


@router.get("/{service_id}/scoring/enforce-threshold")
def scoring_enforce_threshold_get(
    service_id: str = Path(...),
    token: str = Query(default=""),
) -> dict:
    """Read the live enforce_threshold value from the scoring_config
    Compute ConfigStore. None = no enforcement.

    The Rust scorer reads this on every request — when set AND the
    request's score >= threshold, it emits X-Edge-Score-Enforce: 1,
    which the SCORING_ENFORCE_NAME VCL snippet turns into a 429.
    """
    from backend import config as svcconfig
    from backend.core.fastly.client import fastly

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        item = fastly(
            "GET",
            f"/resources/stores/config/{config_store_id}/item/{_ENFORCE_THRESHOLD_KEY}",
            token=resolved_token,
        )
        raw = (item or {}).get("item_value", "")
        threshold: int | None = int(raw) if raw and raw.isdigit() else None
    except RuntimeError as exc:
        # 404 from ConfigStore = key not present = enforcement not set.
        # Mirrors the pattern in session_scoring_orchestrator.py:307-311.
        if "404" in str(exc):
            threshold = None
        else:
            logger.exception("scoring_enforce_threshold_get failed for %s", service_id)
            raise HTTPException(
                status_code=502,
                detail={"error": f"failed to read enforce threshold: {exc}"},
            )

    return {
        "threshold": threshold,
        "enforced": threshold is not None,
        "key": _ENFORCE_THRESHOLD_KEY,
    }


@router.put("/{service_id}/scoring/enforce-threshold")
def scoring_enforce_threshold_put(
    body: dict,
    service_id: str = Path(...),
    token: str = Query(default=""),
    confirm: bool = Query(default=False, description="Set true to actually apply the enforcement change"),
) -> dict:
    """Write the live enforce_threshold to the scoring_config ConfigStore.
    Pass ``{"threshold": null}`` to clear (disable enforcement).

    Effective at the edge within seconds (next Compute invocation
    re-reads the ConfigStore). Audited to scoring_audit so the operator
    can review when enforcement was flipped on/off.

    Gated by ``?confirm=true`` (matches the matrix-restore pattern) so
    an accidental click can't silently flip enforcement at the edge."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={"error": "Pass ?confirm=true to actually change enforcement. This affects live edge blocking."},
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.core.fastly.client import fastly

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})

    raw = body.get("threshold")
    threshold: int | None
    if raw is None:
        threshold = None
    else:
        try:
            threshold = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"error": "threshold must be int 0-100 or null"})
        if not 0 <= threshold <= 100:
            raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    # Upsert: PATCH the item, falling back to POST if it doesn't exist
    # yet (first time enforcement is set for this service).
    value = str(threshold) if threshold is not None else ""
    try:
        try:
            fastly(
                "PATCH",
                f"/resources/stores/config/{config_store_id}/item/{_ENFORCE_THRESHOLD_KEY}",
                {"item_value": value},
                token=resolved_token,
            )
        except Exception:
            fastly(
                "POST",
                f"/resources/stores/config/{config_store_id}/item",
                {"item_key": _ENFORCE_THRESHOLD_KEY, "item_value": value},
                token=resolved_token,
            )
    except Exception as e:
        logger.exception("scoring_enforce_threshold_put failed for %s", service_id)
        raise HTTPException(status_code=500, detail={"error": str(e)})

    metadata_db.record_scoring_audit(
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


# ── Recv exclusion regex (URLs that bypass the scorer) ─────────────────────


@router.get("/{service_id}/scoring/exclude-regex")
def scoring_exclude_regex_get(service_id: str = Path(...)) -> dict:
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        resolve_exclude_url_regex,
    )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
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


@router.put("/{service_id}/scoring/exclude-regex")
def scoring_exclude_regex_put(
    body: dict,
    service_id: str = Path(...),
    token: str = Query(default=""),
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
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
            },
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import (
        RegexValidationError,
        validate_recv_exclusion_regex_with_lint,
    )

    raw = body.get("regex", "")
    if not isinstance(raw, str):
        raise HTTPException(status_code=400, detail={"error": "body.regex must be a string"})

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(
            status_code=400,
            detail={"error": "Session scoring is not enabled for this service"},
        )
    request_secret = scoring.get("request_secret") or ""
    if not request_secret:
        raise HTTPException(
            status_code=400,
            detail={"error": "Internal: request_secret missing from cfg. Re-run enable_scoring."},
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    # Layers 1 + 2: input policy + falco static analysis on the
    # assembled snippet. We close over the per-service ids so the
    # validator can build the full snippet body.
    def _build(cleaned_regex: str) -> str:
        return recv_snippet(service_id, request_secret, exclude_url_regex=cleaned_regex or None)

    try:
        cleaned, lint = validate_recv_exclusion_regex_with_lint(
            raw,
            build_full_snippet=_build,
            # Production keeps falco mandatory; tests / local dev where
            # falco isn't on PATH can override via env.
            require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "0") == "1",
        )
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
        raise HTTPException(status_code=502, detail={"error": str(exc)})

    metadata_db.record_scoring_audit(
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


@router.post("/{service_id}/scoring/exclude-regex/validate")
def scoring_exclude_regex_validate(
    body: dict,
    service_id: str = Path(...),
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import recv_snippet
    from backend.utils.vcl_validator import (
        RegexValidationError,
        validate_recv_exclusion_regex_with_lint,
    )

    raw = body.get("regex", "")
    if not isinstance(raw, str):
        raise HTTPException(status_code=400, detail={"error": "body.regex must be a string"})

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    # The validator needs a request_secret to build the assembled snippet
    # for falco lint — that's a VCL substitution, not anything the lint
    # inspects semantically. Use a stable placeholder when scoring isn't
    # enabled yet so the operator can still pre-validate before turn-on.
    request_secret = scoring.get("request_secret") or "PLACEHOLDER_FOR_LINT_ONLY"

    def _build(cleaned_regex: str) -> str:
        return recv_snippet(service_id, request_secret, exclude_url_regex=cleaned_regex or None)

    try:
        _cleaned, lint = validate_recv_exclusion_regex_with_lint(
            raw,
            build_full_snippet=_build,
            require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "0") == "1",
        )
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


@router.get("/{service_id}/scoring/enforce-status-code")
def scoring_enforce_status_code_get(service_id: str = Path(...)) -> dict:
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
    from backend import config as svcconfig
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
        DEFAULT_ENFORCE_STATUS_CODE,
        resolve_enforce_status_code,
    )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
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


@router.put("/{service_id}/scoring/enforce-status-code")
def scoring_enforce_status_code_put(
    body: dict,
    service_id: str = Path(...),
    token: str = Query(default=""),
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
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
            },
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import update_enforce_status_code
    from backend.provision.session_scoring_vcl import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
    )

    raw = body.get("status_code")
    new_code: int | None
    if raw is None:
        new_code = None
    else:
        try:
            new_code = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail={"error": "status_code must be an integer or null"},
            )
        if not (_ENFORCE_STATUS_CODE_MIN <= new_code <= _ENFORCE_STATUS_CODE_MAX):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"status_code must be in {_ENFORCE_STATUS_CODE_MIN}-{_ENFORCE_STATUS_CODE_MAX} (HTTP 4xx/5xx)"
                },
            )

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(
            status_code=400,
            detail={"error": "Session scoring is not enabled for this service"},
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        result = update_enforce_status_code(service_id, resolved_token, new_status_code=new_code)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail={"error": str(exc)})

    metadata_db.record_scoring_audit(
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


@router.get("/{service_id}/scoring/matrix-versions")
def scoring_matrix_versions_list(service_id: str = Path(...)) -> dict:
    """List historical scoring matrices archived in FOS.

    publish_matrix_to_fos snapshots the prior current matrix to
    ``iceberg/meta/scoring_matrix_history/{version}.json`` before
    overwriting, so the operator can roll back to any prior trained
    matrix. Returns most-recent first."""
    from backend import config as svcconfig
    from backend.state_sync import list_scoring_matrix_versions

    cfg = svcconfig.load_config(service_id) or {}
    current_version = (cfg.get("scoring") or {}).get("matrix_version")
    return {
        "versions": list_scoring_matrix_versions(service_id),
        "current_version": current_version,
    }


@router.post("/{service_id}/scoring/matrix-versions/{version}/restore")
def scoring_matrix_versions_restore(
    service_id: str = Path(...),
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
    /scoring/evaluation call sees the FOS-restored matrix.

    Live edge scorer (Wasm) keeps using its previously-embedded matrix
    until the operator re-runs deploy_wasm.sh. The /scoring/evaluation
    AUC will reflect the restored matrix immediately.

    Gated by ``?confirm=true`` so an accidental click can't silently
    rewind the live AUC numbers."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={"error": "Pass ?confirm=true to actually restore. This will replace the current matrix."},
        )

    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH
    from backend.state_sync import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Matrix version {version!r} not found in FOS history"},
        )

    # Drop the local matrix.json so _load_matrix falls through to the
    # FOS-restored version instead of shadowing it.
    try:
        if _MATRIX_PATH.exists():
            _MATRIX_PATH.unlink()
    except Exception as exc:
        logger.warning(f"Could not remove local matrix.json after restore: {exc}")

    # Update cfg.scoring.matrix_version so /scoring/status reflects the rollback.
    cfg = svcconfig.load_config(service_id)
    if cfg:
        scoring = cfg.setdefault("scoring", {})
        scoring["matrix_version"] = version
        svcconfig.save_config(service_id, cfg)

    _bust_analytics_cache(service_id)

    metadata_db.record_scoring_audit(
        service_id,
        "matrix_restored",
        details={"restored_version": version, "restored_at": result["restored_at"]},
    )

    return {
        "ok": True,
        "restored_version": version,
        "restored_at": result["restored_at"],
        "deploy_hint": (
            "Backend AUC + evaluation endpoints now reflect the restored matrix. "
            "Live edge scorer keeps using its previously-embedded matrix until "
            "you re-run scripts/scoring/deploy_wasm.sh."
        ),
    }


# ── AES key rotation ────────────────────────────────────────────────────────


@router.post("/{service_id}/scoring/rotate-key")
def scoring_rotate_key(
    service_id: str = Path(...),
    token: str = Query(default=""),
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
    from backend import config as svcconfig
    from backend.core import metadata_db
    from backend.provision.session_scoring_setup import rotate_aes_key

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})

    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise HTTPException(status_code=400, detail={"error": "Scoring is not enabled for this service"})

    scoring_keys_store_id = scoring.get("scoring_keys_store_id")
    if not scoring_keys_store_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Service has no scoring_keys_store_id (was scoring enabled before key rotation was supported?)"
            },
        )

    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})

    try:
        result = rotate_aes_key(scoring_keys_store_id, token=resolved_token)
    except Exception as e:
        logger.exception("scoring_rotate_key failed for %s", service_id)
        raise HTTPException(status_code=500, detail={"error": str(e)})

    # Record the rotation in the audit log (without the key value).
    metadata_db.record_scoring_audit(
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


@router.get("/{service_id}/scoring/audit")
def scoring_audit_list(
    service_id: str = Path(...),
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
    from backend import config as svcconfig
    from backend.core import metadata_db

    # 404 when the service itself isn't known — mirrors /scoring/status so
    # the UI gets a consistent shape across the audit + status pair.
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})

    rows = metadata_db.list_scoring_audit(service_id, limit=limit, since=since)
    return {"audit": rows, "limit": limit}


# ── Operator's chosen threshold (persisted, not enforced) ───────────────────


@router.get("/{service_id}/scoring/threshold")
def scoring_threshold_get(service_id: str = Path(...)) -> dict:
    """Return the operator's chosen score threshold.

    NOT enforced — the live scorer doesn't read this. It's a persisted
    operator preference so the threshold slider can remember the
    'committed' value across sessions, and the StatusPanel can show
    'committed threshold: X' as a stable reference. Actual enforcement
    requires a Rust scorer change + Wasm redeploy and is deferred to
    a future release once the operator is confident in the value.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    scoring = cfg.get("scoring") or {}
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,  # See docstring — preview-only
    }


@router.put("/{service_id}/scoring/threshold")
def scoring_threshold_put(
    body: dict,
    service_id: str = Path(...),
) -> dict:
    """Persist the operator's chosen threshold (0-100) into the per-service
    config. Pass ``{"threshold": null}`` to clear. Always returns the
    new state. Does NOT push to Compute — preview-only."""
    import datetime as _dt

    from backend import config as svcconfig

    raw = body.get("threshold")
    threshold: int | None
    if raw is None:
        threshold = None
    else:
        try:
            threshold = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"error": "threshold must be int 0-100 or null"})
        if not 0 <= threshold <= 100:
            raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    scoring = cfg.setdefault("scoring", {})
    prior_threshold = scoring.get("operator_threshold")
    if threshold is None:
        scoring.pop("operator_threshold", None)
        scoring.pop("operator_threshold_set_at", None)
    else:
        scoring["operator_threshold"] = threshold
        scoring["operator_threshold_set_at"] = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
    # Operator audit trail — every threshold change is attributable.
    from backend.core import metadata_db

    metadata_db.record_scoring_audit(
        service_id,
        "threshold_committed" if threshold is not None else "threshold_cleared",
        details={"prior_threshold": prior_threshold, "new_threshold": threshold},
    )
    svcconfig.save_config(service_id, cfg)

    _bust_analytics_cache(service_id)  # so /scoring/status reflects it next fetch
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
    }


# ── Per-reason AUC breakdown ────────────────────────────────────────────────


@router.get("/{service_id}/scoring/evaluation/per-reason")
def scoring_evaluation_per_reason(
    service_id: str = Path(...),
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

    label_rows = _labels.list_labels(service_id)
    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)
    n_neutral = counts.get("neutral", 0)

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


@router.get("/{service_id}/scoring/dashboard")
def scoring_dashboard(
    service_id: str = Path(...),
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
    from backend.scoring import labels as _labels

    counts = _labels.counts_by_label(service_id)
    n_good = counts.get("good", 0)
    n_bad = counts.get("bad", 0)
    n_neutral = counts.get("neutral", 0)

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
        cfg = svcconfig.load_config(service_id) or {}
        scoring = cfg.get("scoring") or {}
        if not scoring.get("enabled"):
            status_block: dict = {"enabled": False}
        else:
            status_block = {k: v for k, v in scoring.items() if k not in _SECRET_KEYS}

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
        evaluation = scoring_evaluation(service_id=service_id)
        health = scoring_health(service_id=service_id, since_hours=since_hours)
        top_flagged = scoring_top_flagged(service_id=service_id, since_hours=since_hours, limit=50)
        score_distribution = scoring_score_distribution(service_id=service_id, since_hours=since_hours)
        compliance_breakdown = scoring_compliance_breakdown(service_id=service_id, since_hours=since_hours)
        curves = scoring_curves(service_id=service_id)
        threshold_preview = scoring_threshold_preview(
            service_id=service_id, threshold=threshold, since_hours=since_hours
        )

        return {
            "since_hours": since_hours,
            "threshold": threshold,
            "status": status_block,
            "evaluation": evaluation,
            "health": health,
            "top_flagged": top_flagged,
            "score_distribution": score_distribution,
            "compliance_breakdown": compliance_breakdown,
            "curves": curves,
            "threshold_preview": threshold_preview,
        }

    return _cached(cache_key, _produce)
