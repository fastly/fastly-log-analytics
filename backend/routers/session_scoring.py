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
from backend.provision.session_scoring_orchestrator import (
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
_SECRET_KEYS = frozenset({"aes_key_hex", "request_secret"})


def _enforce_scoring_service_scope(request: Request | None, service_id: str) -> None:
    if request is not None and hasattr(request, "state"):
        analyst_session = getattr(request.state, "analyst_session", None)
        if analyst_session is None or hasattr(analyst_session, "service_ids"):
            require_service_in_scope(request, service_id)


import threading
import time as _time

from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_ANALYTICS_TTL_SEC = 20.0
_analytics_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=1000, ttl_seconds=_ANALYTICS_TTL_SEC)
_scoring_svc_version_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
_scoring_pragma_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
_analytics_cache_lock = threading.Lock()
_inflight: dict[tuple, threading.Lock] = {}


def _finalize_cached(value, *, is_cached: bool) -> object:
    from backend.utils.telemetry import debug_envelope

    if not isinstance(value, dict):
        return value
    return debug_envelope(value, gated=True, is_cached=is_cached)


def _cached(key: tuple, producer):
    from backend.utils.telemetry import _CALLS as _telemetry_calls
    from backend.utils.telemetry import get_queries

    with _analytics_cache_lock:
        now = _time.monotonic()
        entry = _analytics_cache.get(key)
        if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
            return _finalize_cached(entry[1], is_cached=True)
        key_lock = _inflight.get(key)
        if key_lock is None:
            key_lock = threading.Lock()
            _inflight[key] = key_lock
    with key_lock:
        try:
            with _analytics_cache_lock:
                now = _time.monotonic()
                entry = _analytics_cache.get(key)
                if entry and (now - entry[0]) < _ANALYTICS_TTL_SEC:
                    return _finalize_cached(entry[1], is_cached=True)
            queries = get_queries()
            calls_initial = _telemetry_calls.get() or []
            q_start, c_start = len(queries), len(calls_initial)
            _section_t0 = _time.perf_counter()
            value = producer()
            _section_ms = round((_time.perf_counter() - _section_t0) * 1000, 2)
            queries_after = get_queries()
            calls_after = _telemetry_calls.get() or []
            added_queries = list(queries_after[q_start:] if len(queries_after) >= q_start else queries_after)
            added_calls = list(calls_after[c_start:] if len(calls_after) >= c_start else calls_after)
            if isinstance(value, dict):
                stored = dict(value)
                stored.setdefault("_debug_queries", added_queries)
                stored.setdefault("_debug_calls", added_calls)
                section_name = str(key[0]) if key else "scoring:unknown"
                existing_st = list(stored.get("_section_timings") or [])
                existing_st.append({"section": section_name, "time_ms": _section_ms})
                stored["_section_timings"] = existing_st
            else:
                stored = value
            with _analytics_cache_lock:
                _analytics_cache[key] = (_time.monotonic(), stored)
            return _finalize_cached(stored, is_cached=False)
        finally:
            with _analytics_cache_lock:
                _inflight.pop(key, None)


def _bust_analytics_cache(service_id: str | None = None) -> None:
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
    from backend.provision.session_scoring_orchestrator import _MATRIX_PATH

    tenant_path = _tenant_matrix_path(service_id)
    if m := _read_matrix_file(tenant_path, f"local {tenant_path.name}"):
        return m
    try:
        from backend.state_sync import fetch_matrix_from_fos

        if m := fetch_matrix_from_fos(service_id):
            return m
    except Exception:
        logger.debug("[_load_matrix] FOS matrix fetch failed", exc_info=True)
    return _read_matrix_file(_MATRIX_PATH.parent / "matrix.default.json", "default matrix.json")


from backend.repositories import session_scoring as _scoring_repo


def _isoformat_ts(rows: list[dict], *fields: str) -> list[dict]:
    for r in rows:
        for f in fields:
            v = r.get(f)
            if v is not None and hasattr(v, "isoformat"):
                r[f] = v.isoformat()
    return rows


_table_columns_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=128, ttl_seconds=30.0)


def _table_columns(service_id: str) -> set[str]:
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


_scoring_source = _scoring_repo.scoring_source
_RECONSTRUCT_CACHE_TTL = 5.0
_reconstruct_labeled_sessions_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=64, ttl_seconds=_RECONSTRUCT_CACHE_TTL)


def _labels_fingerprint(labels: list[dict]) -> str:
    pairs = sorted((str(lbl.get("id", "")), str(lbl.get("label", ""))) for lbl in labels)
    return hashlib.sha256(json.dumps(pairs, separators=(",", ":")).encode()).hexdigest()


def _reconstruct_labeled_sessions(service_id: str, labels: list[dict]) -> list[tuple[dict, str]]:
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
        from backend.provision.session_scoring_orchestrator import EntitlementError

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
    """Composite of the analytics endpoints into a single round-trip."""
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


def _cached_fastly_probe(cache: _BoundedTTLCache, key: str, fn, log_label: str, *, cache_if=lambda v: v is not None):
    val = cache.get(key)
    if val is None:
        try:
            val = fn()
        except Exception:  # pragma: no cover - defensive
            logger.warning("[scoring-status] %s failed for %s", log_label, key, exc_info=True)
            val = None
        if cache_if(val):
            cache[key] = val
    return val


@router.get(
    "/{service_id}/scoring/status",
    response_model=ScoringStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Return the scoring block from the service's config, or {"enabled": false}."""
    cfg = load_service_config(service_id)
    scoring = cfg.get("scoring")
    token = stored_fastly_token(cfg)
    if not scoring or not scoring.get("enabled"):
        result: dict = {"enabled": False}
        if token:
            from backend.provision.fastly_api import service_has_scoring_pragmas

            ready = _cached_fastly_probe(
                _scoring_pragma_cache,
                service_id,
                lambda: service_has_scoring_pragmas(service_id, token),
                "pragma check",
                cache_if=lambda v: v is True,
            )
            if ready is not None:
                result["preflight_pragmas_ready"] = ready
        return result
    result = _safe_scoring_dict(scoring)
    scoring_svc = scoring.get("scoring_service_id")
    if scoring_svc and token:
        from backend.core.fastly.client import get_active_version_info

        info = _cached_fastly_probe(
            _scoring_svc_version_cache,
            scoring_svc,
            lambda: get_active_version_info(scoring_svc, token),
            "active-version lookup",
        )
        if info:
            result["scoring_active_version"] = info.get("number")
            result["scoring_activated_at"] = info.get("updated_at")
    result["scorer_drift"] = False
    result["drift_detail"] = None
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


_LABEL_PII_FIELDS = frozenset({"notes", "flagged_by", "sample_ip", "sample_ua", "sample_url"})


def _project_label_for_analyst(label: dict) -> dict:
    return {k: v for k, v in label.items() if k not in _LABEL_PII_FIELDS}


def _is_analyst_request(request: Request) -> bool:
    return getattr(request.state, "analyst_session", None) is not None


def _strip_pii_fields_for_analyst(request: Request, body) -> None:
    if not _is_analyst_request(request):
        return
    fields = type(body).model_fields
    for name in _LABEL_PII_FIELDS:
        if name in fields:
            setattr(body, name, fields[name].default)


def _finalize_label_write(request: Request, service_id: str, row: dict) -> dict:
    _bust_analytics_cache(service_id)
    return _project_label_for_analyst(row) if _is_analyst_request(request) else row


def _scoring_time_window(request: Request, since_hours: int) -> tuple[str, str | None]:
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
    """Delete a session label (gated on the caller's service scope)."""
    from backend.scoring import labels as _labels

    require_service_in_scope(request, service_id)
    result = _labels.delete_label(service_id, label_id)
    _bust_analytics_cache(service_id)
    return result


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
    """Recent rows with non-null edge_score, sorted by score DESC."""
    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
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
    """Hourly buckets × score buckets (0, 25, 50, 75, 100)."""
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
    """Hourly count grouped by edge_cookie_compliance."""
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
    """Time-series of scorer latency + fail-open errors (minute buckets for <=1h, hourly otherwise)."""
    src, cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    has_rtt = "edge_score_rtt_us" in cols
    has_exec = "edge_score_exec_us" in cols
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
    """High-level scoring health snapshot for the admin dashboard."""
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
        matrix_stale = l2_evaluated >= 100 and l2_high_pct > 25.0
        latency = {
            "available": has_rtt or has_exec,
            **{
                k: (int(v) if (v := row.get(k)) is not None else None)
                for k in ("rtt_p50_us", "rtt_p95_us", "rtt_p99_us", "rtt_max_us", "exec_p50_us", "exec_p95_us")
            },
        }
        l2_enforce = None if _is_analyst_request(request) else l2_enforce_readiness_block(service_id)
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
    """Compute the live matrix's ROC-AUC against accumulated good/bad labels."""
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, n_neutral = _label_counts(service_id)
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
        n_reconstructed = len(labeled_sessions)
        from backend.scoring.evaluate import DEFAULT_MIN_AUC, evaluate_from_persisted_scores

        result = evaluate_from_persisted_scores(labeled_sessions)
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


@router.get("/{service_id}/scoring/curves", response_model=ScoringCurvesResponse, response_model_exclude_unset=True)
def scoring_curves(
    service_id: str = Path(...),
    request: Request = None,  # type: ignore[assignment]
) -> dict:
    """ROC + PR curve points for the operator's labeled sessions across thresholds 0..100."""
    from backend.scoring import labels as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, _ = _label_counts(service_id)

    def _produce() -> dict:
        min_check = _check_min_samples(n_good, n_bad)
        if min_check is not None:
            return min_check
        label_rows = _labels.list_labels(service_id)
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
        for t in range(100, -1, -1):
            tp = sum(1 for s, lbl in scored if lbl == "bad" and s >= t)
            fp = sum(1 for s, lbl in scored if lbl == "good" and s >= t)
            tpr = tp / total_pos if total_pos else 0.0
            fpr = fp / total_neg if total_neg else 0.0
            precision = (tp / (tp + fp)) if (tp + fp) else 1.0
            roc.append({"threshold": t, "fpr": round(fpr, 4), "tpr": round(tpr, 4)})
            pr.append({"threshold": t, "precision": round(precision, 4), "recall": round(tpr, 4)})

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
    """Preview flagged vs passed confusion counts at a given enforcement threshold."""
    from backend.scoring import labels as _labels

    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    threshold_int = int(threshold)

    def _produce() -> dict:
        label_rows = _labels.list_labels(service_id)
        sid_to_label = {row["sid"]: row["label"] for row in label_rows if row.get("sid")}
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
        flagged_unlabeled = max(0, flagged_total - (flagged_good + flagged_bad))
        passed_unlabeled = max(0, passed_total - (passed_good + passed_bad))
        flagged = flagged_good + flagged_bad + flagged_unlabeled
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

    n_good, n_bad, _ = _label_counts(service_id)
    return _cached(("threshold-preview", service_id, threshold_int, since_hours, n_good + n_bad, win_disc), _produce)


_L2_ENABLED_AT_KEY = "l2_enabled_at"
_L2_ENFORCE_ENABLED_KEY = "l2_enforce_enabled"
_fetch_session_events = _scoring_repo.fetch_session_events
_ENFORCE_THRESHOLD_KEY = "enforce_threshold"
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache

_ENFORCE_THRESHOLD_CACHE_TTL = 300.0
_enforce_threshold_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)
_L2_RAMP_DAYS = 3.0
_L2_READINESS_DAYS = 7.0
_l2_enforce_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)


def _l2_parse_epoch(raw: str | None) -> int | None:
    if not raw:
        return None
    s = str(raw).strip()
    return int(s) if s.isdigit() else None


def _l2_enforce_unavailable() -> dict:
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
    enabled = (l2_enforce_raw or "").strip() == "1"
    scoring_enabled_at = _l2_parse_epoch(scoring_enabled_at_raw)
    l2_enabled_at = _l2_parse_epoch(l2_enabled_at_raw)
    deployment_age_days: float | None = None
    ready = False
    if scoring_enabled_at is not None:
        deployment_age_days = max(0.0, (now_epoch - scoring_enabled_at) / 86_400.0)
        ready = deployment_age_days >= _L2_READINESS_DAYS
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
    from backend.core.fastly.client import fastly

    try:
        item = fastly("GET", f"/resources/stores/config/{config_store_id}/item/{key}", token=token)
        return (item or {}).get("item_value", "") or ""
    except RuntimeError as exc:
        if "404" in str(exc):
            return ""
        raise


def _read_l2_enforce_state(service_id: str, config_store_id: str, token: str) -> dict:
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
            from backend.provision.session_scoring_orchestrator import MATRIX_KEY_NAME
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
    """Build a transition matrix from DuckDB traffic, publish to FOS + KV, and evaluate AUC."""
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
    con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
    try:
        sessions_iter = _fixtures.extract_traces(con, service_id=service_id, start=start)
        tmatrix, stats = _matrix.build_matrix(
            (s.to_jsonl_dict() for s in sessions_iter),
        )
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("scoring_retrain: con.close failed (ignored)", exc_info=True)
    matrix_dict = tmatrix.to_json_dict(version=matrix_version)
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
    kv_written = _push_matrix_to_kv_if_configured(service_id, cfg, matrix_dict)
    _bust_analytics_cache(service_id)
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
    """Return the event timeline for a single session (up to 500 events)."""
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
    cfg, scoring = _load_scoring_cfg(service_id)
    config_store_id = scoring.get("scoring_config_store_id")
    if not config_store_id:
        raise HTTPException(status_code=400, detail={"error": "Scoring not enabled or config store missing"})
    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})
    return cfg, config_store_id, resolved_token


def _upsert_config_store_item(api_token: str, store_id: str, key: str, value: str) -> None:
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
    """Read the live enforce_threshold value from the scoring_config ConfigStore (None = disabled)."""
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
    """Write the live enforce_threshold to the scoring_config ConfigStore (null disables enforcement)."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change enforcement. This affects live edge blocking.")
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)
    threshold = _validate_threshold_0_100(body.threshold)
    value = str(threshold) if threshold is not None else ""
    try:
        _upsert_config_store_item(resolved_token, config_store_id, _ENFORCE_THRESHOLD_KEY, value)
    except Exception as e:
        raise_internal(logger, e, code="scoring_enforce_threshold_put_failed")
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
    """Read the live L2-enforcement opt-in + fade-in state from the scoring_config ConfigStore."""
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
    """Opt edge Layer-2 in/out of the enforced combined score via scoring_config ConfigStore."""
    _require_confirm(confirm, "Pass ?confirm=true to actually change L2 enforcement. This affects live edge blocking.")
    import datetime as _dt

    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)
    enabled = bool(body.enabled)
    try:
        l2_enabled_at: int | None
        if enabled:
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
            l2_enabled_at = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
    except Exception as e:
        raise_internal(logger, e, code="scoring_l2_enforce_put_failed")
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


@router.get(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_get(service_id: ServiceId) -> dict:
    """Return the operator-configured URL-exclusion regex for the recv snippet."""
    from backend.provision.session_scoring_orchestrator import (
        DEFAULT_ASSET_EXT_REGEX,
        resolve_exclude_url_regex,
    )

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("exclude_url_regex") or ""
    effective = resolve_exclude_url_regex(current or None)
    return {
        "current": current,
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
    from backend.provision.session_scoring_orchestrator import recv_snippet
    from backend.utils.vcl_validator import validate_recv_exclusion_regex_with_lint

    return validate_recv_exclusion_regex_with_lint(
        raw,
        build_full_snippet=lambda cleaned_regex: recv_snippet(service_id, exclude_url_regex=cleaned_regex or None),
        require_falco=os.environ.get("SCORING_REQUIRE_FALCO", "1") != "0",
    )


def _apply_vcl_snippet_update(
    service_id: str,
    update_fn,
    error_code: str,
    audit_action: str,
    build_audit_details,
    build_message,
    **extra_fields: object,
) -> dict:
    try:
        result = update_fn()
    except RuntimeError as exc:
        raise_internal(logger, exc, code=error_code, status=502)
    _record_scoring_audit(service_id, audit_action, details=build_audit_details(result))
    return {"ok": True, **result, **extra_fields, "message": build_message(result)}


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
    """Update the URL-exclusion regex for the scoring recv snippet."""
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )
    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex
    from backend.utils.vcl_validator import RegexValidationError

    _cfg, _scoring, resolved_token = _require_scoring_enabled_and_token(service_id, token)
    try:
        cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        raise HTTPException(status_code=400, detail={"error": exc.message, "reason": exc.reason})
    return _apply_vcl_snippet_update(
        service_id,
        lambda: update_recv_exclusion_regex(service_id, resolved_token, new_regex=cleaned),
        "exclude_regex_update_failed",
        "scoring_exclude_regex_changed",
        lambda r: {
            "is_default": r["is_default"],
            "effective_regex": r["effective_regex"][:200],
            "logging_service_active_version": r["logging_service_active_version"],
            "lint_warnings": lint.warnings[:5],
        },
        lambda r: (
            "Reset to default URL exclusion regex."
            if r["is_default"]
            else "Custom URL exclusion regex applied. Effective at the edge after Fastly version activation."
        ),
        lint_warnings=lint.warnings,
    )


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
    """Run the 2-layer pre-publish validator on a candidate regex without persisting it."""
    from backend.utils.vcl_validator import RegexValidationError

    try:
        _cleaned, lint = _validate_service_exclude_regex(service_id, body.regex)
    except RegexValidationError as exc:
        return {"ok": False, "error": exc.message, "reason": exc.reason}
    return {"ok": True, "lint_warnings": lint.warnings}


@router.get(
    "/{service_id}/scoring/enforce-status-code",
    response_model=ScoringEnforceStatusCodeState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_status_code_get(service_id: ServiceId) -> dict:
    """Return the operator-configured HTTP status code returned by the enforce snippet."""
    from backend.provision.session_scoring_orchestrator import (
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
    """Update the HTTP status code returned by the enforce snippet (400-599 or null for 429)."""
    _require_confirm(
        confirm, "Pass ?confirm=true to actually apply the change. This re-publishes the active VCL version."
    )
    from backend.provision.session_scoring_orchestrator import (
        _ENFORCE_STATUS_CODE_MAX,
        _ENFORCE_STATUS_CODE_MIN,
        update_enforce_status_code,
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
    return _apply_vcl_snippet_update(
        service_id,
        lambda: update_enforce_status_code(service_id, resolved_token, new_status_code=new_code),
        "enforce_status_code_update_failed",
        "scoring_enforce_status_code_changed",
        lambda r: {
            "is_default": r["is_default"],
            "effective_status_code": r["effective_status_code"],
            "logging_service_active_version": r["logging_service_active_version"],
        },
        lambda r: (
            "Reset to default enforce status code (429)."
            if r["is_default"]
            else f"Enforce status code → {r['effective_status_code']}. Effective at the edge after Fastly version activation."
        ),
    )


@router.get(
    "/{service_id}/scoring/matrix-versions",
    response_model=ScoringMatrixVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_list(service_id: ServiceId) -> dict:
    """List historical scoring matrices archived in FOS."""
    from backend.state_sync import list_scoring_matrix_versions

    _cfg, scoring = _load_scoring_cfg(service_id)
    return {
        "versions": list_scoring_matrix_versions(service_id),
        "current_version": scoring.get("matrix_version"),
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
    """Restore a historical matrix in FOS, clear the local shadow copy, and push to KV."""
    _require_confirm(confirm, "Pass ?confirm=true to actually restore. This will replace the current matrix.")
    from backend import config as svcconfig
    from backend.state_sync import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Matrix version {version!r} not found in FOS history"},
        )
    tenant_matrix_path = _tenant_matrix_path(service_id)
    try:
        if tenant_matrix_path.exists():
            tenant_matrix_path.unlink()
    except Exception as exc:
        logger.warning(f"Could not remove local {tenant_matrix_path.name} after restore: {exc}")
    cfg = svcconfig.mutate_config(
        service_id, lambda c: c.setdefault("scoring", {}).__setitem__("matrix_version", version)
    )
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
    """Rotate the AES-GCM cookie-state encryption key (preserving previous_key_hex grace slot)."""
    from backend.provision.session_scoring_orchestrator import rotate_aes_key

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
    _record_scoring_audit(
        service_id,
        "key_rotated",
        details={
            "rotated_at": result["rotated_at"],
            "previous_key_grace": bool(result.get("previous_key_hex")),
        },
    )
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
    """List recent operator actions on this service's scoring config."""
    from backend.core import metadata as metadata_db

    load_service_config(service_id)
    rows = metadata_db.list_scoring_audit(service_id, limit=limit, since=since)
    return {"audit": rows, "limit": limit}


@router.get(
    "/{service_id}/scoring/threshold",
    response_model=ScoringThresholdState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_threshold_get(service_id: ServiceId) -> dict:
    """Return the operator's chosen preview-only score threshold."""
    _cfg, scoring = _load_scoring_cfg(service_id)
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
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
    """Persist the operator's chosen preview-only threshold (0-100 or null) into per-service config."""
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
    _bust_analytics_cache(service_id)
    return {
        "threshold": scoring.get("operator_threshold"),
        "set_at": scoring.get("operator_threshold_set_at"),
        "enforced": False,
    }


@router.get(
    "/{service_id}/scoring/evaluation/per-reason",
    response_model=ScoringPerReasonResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_evaluation_per_reason(
    service_id: ServiceId,
) -> dict:
    """AUC broken down by L1/L2 rule."""
    from backend.scoring import labels as _labels
    from backend.scoring.evaluate import evaluate_per_reason

    n_good, n_bad, n_neutral = _label_counts(service_id)
    cache_key = ("scoring-evaluation-per-reason", service_id, n_good, n_bad, n_neutral)

    def _produce() -> dict:
        if n_good < _MIN_LABELS_PER_CLASS or n_bad < _MIN_LABELS_PER_CLASS:
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
    """One-shot composite dashboard payload across status, evaluation, health, charts, and config."""
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
        import contextvars
        from concurrent.futures import ThreadPoolExecutor

        cfg = svcconfig.load_config(service_id)
        status_block: dict = scoring_status(service_id=service_id) if cfg else {"enabled": False}
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
            **results,
            **{
                ("config_threshold" if k == "threshold" else k): v
                for k, v in _build_scoring_config_bundle(service_id).items()
            },
        }

    return _cached(cache_key, _produce)


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("session_scoring._analytics_cache", _analytics_cache)
_CacheRegistry.register("session_scoring._inflight", _inflight)
_CacheRegistry.register("session_scoring._reconstruct_labeled_sessions_cache", _reconstruct_labeled_sessions_cache)
_CacheRegistry.register("session_scoring._table_columns_cache", _table_columns_cache)
_CacheRegistry.register("session_scoring._scoring_pragma_cache", _scoring_pragma_cache)
_CacheRegistry.register("session_scoring._scoring_svc_version_cache", _scoring_svc_version_cache)
_CacheRegistry.register("session_scoring_admin._enforce_threshold_cache", _enforce_threshold_cache)
_CacheRegistry.register("session_scoring_admin._l2_enforce_cache", _l2_enforce_cache)
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
            raise HTTPException(status_code=403, detail={"error": "pii_policy_violation", "field": "ip"})
        if not ip or not start_raw or not end_raw:
            raise HTTPException(status_code=400, detail={"error": "ip, session_start, and session_end are required"})
        s_dt, e_dt = parse_iso_utc(start_raw), parse_iso_utc(end_raw)
        if s_dt is not None and e_dt is not None and e_dt <= s_dt:
            start_raw = (s_dt - timedelta(seconds=1)).isoformat()
            end_raw = (e_dt + timedelta(seconds=1)).isoformat()
        start_time, end_time = ctx.clamp(start_raw, end_raw)
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
