"""Session scoring analytics, evaluation, labels, dashboard composite, and sessions endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time as _time
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from backend.core.request_context import (
    RequestContext,
    ServiceId,
    SessionTokenError,
    build_request_context,
    open_session_token,
    require_admin,
    seal_session_token,
)
from backend.core.storage import execute_with_stale_view_retry
from backend.models.analytics import (
    ScoringAnalyticsResponse,
    ScoringComplianceBreakdownResponse,
    ScoringCurvesResponse,
    ScoringDashboardResponse,
    ScoringEvaluationResponse,
    ScoringHealthResponse,
    ScoringLabelCreate,
    ScoringLabelDeleteResponse,
    ScoringLabelRow,
    ScoringLabelsListResponse,
    ScoringLabelUpdate,
    ScoringLatencyTimeseriesResponse,
    ScoringPerReasonResponse,
    ScoringScoreDistributionResponse,
    ScoringSessionEventsResponse,
    ScoringThresholdPreviewResponse,
    ScoringTopFlaggedResponse,
    SessionDetailRequest,
    SessionDetailResponse,
    SessionsRequest,
    SessionsResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories import sessions as _scoring_repo
from backend.repositories import sessions as repo
from backend.utils.auth import mask_ips_for, require_service_in_scope
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import clamp_or_400, get_analyst_time_bounds
from backend.utils.router_utils import make_error, not_found, query_errors
from backend.utils.telemetry import process_context_scope

from .scoring_vcl import scoring_status

logger = logging.getLogger(__name__)
router = APIRouter()
sessions_router = APIRouter(prefix="/api/sessions", tags=["sessions"], responses=DEFAULT_ERROR_RESPONSES)


def _enforce_scoring_service_scope(request: Request | None, service_id: str) -> None:
    if request is not None and hasattr(request, "state"):
        analyst_session = getattr(request.state, "analyst_session", None)
        if analyst_session is None or hasattr(analyst_session, "service_ids"):
            require_service_in_scope(request, service_id)


_ANALYTICS_TTL_SEC = 20.0
_analytics_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=1000, ttl_seconds=_ANALYTICS_TTL_SEC)
_analytics_cache_lock = threading.Lock()
_inflight: dict[tuple, threading.Lock] = {}


def _finalize_cached(value, *, is_cached: bool) -> object:
    from backend.utils.telemetry import debug_envelope

    return debug_envelope(value, gated=True, is_cached=is_cached) if isinstance(value, dict) else value


def _cached(key: tuple, producer):
    from backend.utils.telemetry import _CALLS as _telemetry_calls
    from backend.utils.telemetry import get_queries

    with _analytics_cache_lock:
        entry = _analytics_cache.get(key)
        if entry and (_time.monotonic() - entry[0]) < _ANALYTICS_TTL_SEC:
            return _finalize_cached(entry[1], is_cached=True)
        key_lock = _inflight.setdefault(key, threading.Lock())
    with key_lock:
        try:
            with _analytics_cache_lock:
                entry = _analytics_cache.get(key)
                if entry and (_time.monotonic() - entry[0]) < _ANALYTICS_TTL_SEC:
                    return _finalize_cached(entry[1], is_cached=True)
            q_start, c_start = len(get_queries()), len(_telemetry_calls.get() or [])
            _section_t0 = _time.perf_counter()
            value = producer()
            _section_ms = round((_time.perf_counter() - _section_t0) * 1000, 2)
            queries_after, calls_after = get_queries(), _telemetry_calls.get() or []
            added_queries = list(queries_after[q_start:] if len(queries_after) >= q_start else queries_after)
            added_calls = list(calls_after[c_start:] if len(calls_after) >= c_start else calls_after)
            if isinstance(value, dict):
                stored = dict(value)
                stored.setdefault("_debug_queries", added_queries)
                stored.setdefault("_debug_calls", added_calls)
                section_name = str(key[0]) if key else "scoring:unknown"
                stored["_section_timings"] = [
                    *list(stored.get("_section_timings") or []),
                    {"section": section_name, "time_ms": _section_ms},
                ]
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
        for k in [k for k in list(_analytics_cache.keys()) if service_id in k]:
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
        from backend.core.metadata.base import fetch_matrix_from_fos

        if m := fetch_matrix_from_fos(service_id):
            return m
    except Exception:
        logger.debug("[_load_matrix] FOS matrix fetch failed", exc_info=True)
    return _read_matrix_file(_MATRIX_PATH.parent / "matrix.default.json", "default matrix.json")


def _isoformat_ts(rows: list[dict], *fields: str) -> list[dict]:
    for r in rows:
        for f in fields:
            if (v := r.get(f)) is not None and hasattr(v, "isoformat"):
                r[f] = v.isoformat()
    return rows


_table_columns_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=128, ttl_seconds=30.0)


def _table_columns(service_id: str) -> set[str]:
    from backend.core.duckdb import _safe_table_name

    in_test = bool(os.getenv("PYTEST_CURRENT_TEST"))
    if not in_test and (cached := _table_columns_cache.get(service_id)) is not None:
        return cached
    try:
        rows = _scoring_repo.describe_table(service_id, _safe_table_name(service_id))
    except Exception:  # noqa: BLE001
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
    cache_key = (service_id, _labels_fingerprint(labels))
    if (cached := _reconstruct_labeled_sessions_cache.get(cache_key)) is not None:
        return cached
    result = _scoring_repo.reconstruct_labeled_sessions(service_id, labels)
    _reconstruct_labeled_sessions_cache[cache_key] = result
    return result


_MIN_LABELS_PER_CLASS = 3


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


def _label_counts(service_id: str) -> tuple[int, int, int]:
    import backend.scoring as _labels

    counts = _labels.counts_by_label(service_id)
    return counts.get("good", 0), counts.get("bad", 0), counts.get("neutral", 0)


_LABEL_PII_FIELDS = frozenset({"notes", "flagged_by", "sample_ip", "sample_ua", "sample_url"})


def _project_label_for_analyst(label: dict) -> dict:
    return {k: v for k, v in label.items() if k not in _LABEL_PII_FIELDS}


def _is_analyst_request(request: Request) -> bool:
    return getattr(request.state, "analyst_session", None) is not None


def _strip_pii_fields_for_analyst(request: Request, body) -> None:
    if not _is_analyst_request(request):
        return
    fields = type(body).model_fields
    for name in _LABEL_PII_FIELDS & fields.keys():
        setattr(body, name, fields[name].default)


def _finalize_label_write(request: Request, service_id: str, row: dict) -> dict:
    _bust_analytics_cache(service_id)
    return _project_label_for_analyst(row) if _is_analyst_request(request) else row


def _scoring_time_window(request: Request, since_hours: int) -> tuple[str, str | None]:
    since_hours = int(since_hours)
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return f"timestamp >= now() - INTERVAL {since_hours} HOUR", None
    now = datetime.now(UTC)
    start_iso, end_iso = clamp_or_400(
        get_analyst_time_bounds(request),
        (now - timedelta(hours=since_hours)).isoformat(),
        now.isoformat(),
        analyst_session=session,
    )
    return f"timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}'", f"{start_iso}|{end_iso}"


def _prepare_scoring_query(
    request: Request, service_id: str, since_hours: int
) -> tuple[str, set[str], str, str | None]:
    from backend.core.duckdb import _safe_table_name

    _enforce_scoring_service_scope(request, service_id)
    cols = _table_columns(service_id)
    return _scoring_source(_safe_table_name(service_id), cols), cols, *_scoring_time_window(request, since_hours)


def _cached_scoring_rows(cache_key: tuple, since_hours: int, fetch_fn, ts_field: str = "hour", **extra: object) -> dict:
    return _cached(
        cache_key, lambda: {"rows": _isoformat_ts(fetch_fn(), ts_field), "since_hours": since_hours, **extra}
    )


_fetch_session_events = _scoring_repo.fetch_session_events


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


@router.get("/{service_id}/scoring/labels", response_model=ScoringLabelsListResponse, response_model_exclude_unset=True)
def scoring_labels_list(
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
    limit: int = Query(default=500, ge=1, le=10000),
) -> dict:
    """Return all session labels for a service, most recent first."""
    import backend.scoring as _labels

    _enforce_scoring_service_scope(request, service_id)
    rows = _labels.list_labels(service_id, limit=limit)
    if _is_analyst_request(request):
        rows = [_project_label_for_analyst(r) for r in rows]
    return {"labels": rows, "counts": _labels.counts_by_label(service_id)}


@router.post("/{service_id}/scoring/labels", response_model=ScoringLabelRow, response_model_exclude_unset=True)
def scoring_labels_create(
    body: ScoringLabelCreate,
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    """Create or update a label. Upserts on (service_id, sid)."""
    import backend.scoring as _labels

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
    import backend.scoring as _labels

    _enforce_scoring_service_scope(request, service_id)
    _strip_pii_fields_for_analyst(request, body)
    try:
        row = _labels.update_label(service_id, label_id, label=body.label, notes=body.notes)
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
def scoring_labels_delete(request: Request, service_id: str = Path(...), label_id: str = Path(...)) -> dict:
    """Delete a session label (gated on the caller's service scope)."""
    import backend.scoring as _labels

    require_service_in_scope(request, service_id)
    result = _labels.delete_label(service_id, label_id)
    _bust_analytics_cache(service_id)
    return result


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
    request: Request, service_id: str = Path(...), since_hours: int = Query(default=24, ge=1, le=168)
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
    request: Request, service_id: str = Path(...), since_hours: int = Query(default=24, ge=1, le=168)
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
    request: Request, service_id: str = Path(...), since_hours: int = Query(default=24, ge=1, le=168)
) -> dict:
    """Time-series of scorer latency + fail-open errors (minute buckets for <=1h, hourly otherwise)."""
    src, cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    has_rtt, has_exec = "edge_score_rtt_us" in cols, "edge_score_exec_us" in cols
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
    request: Request, service_id: str = Path(...), since_hours: int = Query(default=24, ge=1, le=168)
) -> dict:
    """High-level scoring health snapshot for the admin dashboard."""
    src, cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    has_rtt, has_exec = "edge_score_rtt_us" in cols, "edge_score_exec_us" in cols

    def _produce() -> dict:
        row = _scoring_repo.health_snapshot_row(service_id, src, ts_pred, has_rtt, has_exec)
        total, scored = int(row.get("total_edge_rows") or 0), int(row.get("scored_rows") or 0)
        l2_evaluated, l2_high = int(row.get("l2_evaluated") or 0), int(row.get("l2_high_count") or 0)
        l2_high_pct = (l2_high / l2_evaluated * 100.0) if l2_evaluated else 0.0
        scorer_errors = int(row.get("scorer_errors") or 0)
        return {
            "since_hours": since_hours,
            "total_edge_rows": total,
            "scored_rows": scored,
            "fire_rate_pct": round((scored / total * 100.0) if total else 0.0, 2),
            "distinct_sids": int(row.get("distinct_sids") or 0),
            "avg_score": float(row.get("avg_score") or 0),
            "p50_score": float(row.get("p50_score") or 0),
            "p95_score": float(row.get("p95_score") or 0),
            "max_score": int(row.get("max_score") or 0),
            "scorer_errors": scorer_errors,
            "fail_open_rate_pct": round(scorer_errors / total * 100.0, 2) if total else 0.0,
            "top_reasons": row.get("top_reasons") or [],
            "fail_open_breakdown": row.get("fail_open_breakdown") or [],
            "latency": {
                "available": has_rtt or has_exec,
                **{
                    k: (int(v) if (v := row.get(k)) is not None else None)
                    for k in ("rtt_p50_us", "rtt_p95_us", "rtt_p99_us", "rtt_max_us", "exec_p50_us", "exec_p95_us")
                },
            },
            "matrix_staleness": {
                "l2_evaluated": l2_evaluated,
                "l2_high_count": l2_high,
                "l2_high_pct": round(l2_high_pct, 2),
                "is_stale": l2_evaluated >= 100 and l2_high_pct > 25.0,
                "threshold_pct": 25.0,
            },
            "l2_enforce": None if _is_analyst_request(request) else l2_enforce_readiness_block(service_id),
        }

    return _cached(("scoring-health", service_id, since_hours, win_disc), _produce)


@router.get(
    "/{service_id}/scoring/evaluation", response_model=ScoringEvaluationResponse, response_model_exclude_unset=True
)
def scoring_evaluation(
    service_id: str = Path(...),
    request: Request = None,  # type: ignore[assignment]
) -> dict:
    """Compute the live matrix's ROC-AUC against accumulated good/bad labels."""
    import backend.scoring as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, n_neutral = _label_counts(service_id)

    def _produce() -> dict:
        from backend.config import load_config
        from backend.scoring import DEFAULT_MIN_AUC, evaluate_from_persisted_scores

        matrix_version = ((load_config(service_id) or {}).get("scoring") or {}).get("matrix_version") or "unknown"
        if (
            min_check := _check_min_samples(n_good, n_bad, n_neutral=n_neutral, matrix_version=matrix_version)
        ) is not None:
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
                "and no FOS copy). Run Retrain Matrix from the Session Scoring page (POST /api/session-scoring/retrain) to produce one.",
            }
        label_rows = _labels.list_labels(service_id)
        labeled_sessions = _reconstruct_labeled_sessions(service_id, label_rows)
        result = evaluate_from_persisted_scores(labeled_sessions)
        return {
            "has_min_samples": True,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": result.n_good,
            "n_bad": result.n_bad,
            "n_neutral": n_neutral,
            "n_reconstructed": len(labeled_sessions),
            "n_labels_total": len(label_rows),
            "auc": round(float(result.auc), 4),
            "passed": bool(result.passed),
            "threshold": float(result.pass_threshold),
            "default_min_auc": float(DEFAULT_MIN_AUC),
            "matrix_version": matrix.get("version") or matrix_version,
        }

    return _cached(("scoring-evaluation", service_id, n_good, n_bad, n_neutral), _produce)


@router.get("/{service_id}/scoring/curves", response_model=ScoringCurvesResponse, response_model_exclude_unset=True)
def scoring_curves(
    service_id: str = Path(...),
    request: Request = None,  # type: ignore[assignment]
) -> dict:
    """ROC + PR curve points for the operator's labeled sessions across thresholds 0..100."""
    import backend.scoring as _labels

    _enforce_scoring_service_scope(request, service_id)
    n_good, n_bad, _ = _label_counts(service_id)

    def _produce() -> dict:
        if (min_check := _check_min_samples(n_good, n_bad)) is not None:
            return min_check
        label_rows = _labels.list_labels(service_id)
        scored = [
            (int(score), label)
            for session, label in _reconstruct_labeled_sessions(service_id, label_rows)
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
        roc, pr = [], []
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

        return {
            "has_min_samples": True,
            "min_per_class": _MIN_LABELS_PER_CLASS,
            "n_good": total_neg,
            "n_bad": total_pos,
            "n_labels_total": len(label_rows),
            "auc": round(float(_trapz([(p["fpr"], p["tpr"]) for p in roc])), 4),
            "average_precision": round(float(_trapz([(p["recall"], p["precision"]) for p in pr])), 4),
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
    import backend.scoring as _labels

    src, _cols, ts_pred, win_disc = _prepare_scoring_query(request, service_id, since_hours)
    threshold_int = int(threshold)

    def _produce() -> dict:
        sid_to_label = {row["sid"]: row["label"] for row in _labels.list_labels(service_id) if row.get("sid")}
        labeled_pairs = [(s, lbl) for s, lbl in sid_to_label.items() if s]
        counts_row = _scoring_repo.threshold_preview_counts(service_id, src, ts_pred, threshold_int, labeled_pairs)
        total, flagged_total = int(counts_row.get("total") or 0), int(counts_row.get("flagged_total") or 0)
        passed_total = total - flagged_total
        flagged_good, flagged_bad, passed_good, passed_bad = (
            (int(counts_row.get(k) or 0) for k in ("flagged_good", "flagged_bad", "passed_good", "passed_bad"))
            if labeled_pairs
            else (0, 0, 0, 0)
        )
        flagged_unlabeled = max(0, flagged_total - (flagged_good + flagged_bad))
        passed_unlabeled = max(0, passed_total - (passed_good + passed_bad))
        flagged_labeled, all_labeled_bad = flagged_good + flagged_bad, flagged_bad + passed_bad
        return {
            "threshold": threshold_int,
            "since_hours": since_hours,
            "total_scored_sessions": total,
            "flagged": {
                "total": flagged_labeled + flagged_unlabeled,
                "good": flagged_good,
                "bad": flagged_bad,
                "unlabeled": flagged_unlabeled,
            },
            "passed": {"good": passed_good, "bad": passed_bad, "unlabeled": passed_unlabeled},
            "precision": round(flagged_bad / flagged_labeled, 4) if flagged_labeled else None,
            "recall": round(flagged_bad / all_labeled_bad, 4) if all_labeled_bad else None,
        }

    n_good, n_bad, _ = _label_counts(service_id)
    return _cached(("threshold-preview", service_id, threshold_int, since_hours, n_good + n_bad, win_disc), _produce)


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
    return {"sid": sid, "since_days": since_days, "event_count": len(events), "events": events}


@router.get(
    "/{service_id}/scoring/evaluation/per-reason",
    response_model=ScoringPerReasonResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_evaluation_per_reason(service_id: ServiceId) -> dict:
    """AUC broken down by L1/L2 rule."""
    import backend.scoring as _labels
    from backend.scoring import evaluate_per_reason

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
        labeled_sessions = _reconstruct_labeled_sessions(service_id, _labels.list_labels(service_id))
        result = evaluate_per_reason(labeled_sessions, min_per_class=_MIN_LABELS_PER_CLASS)
        result["has_min_samples_overall"] = True
        result["n_good"] = n_good
        result["n_bad"] = n_bad
        return result

    return _cached(cache_key, _produce)


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
    cache_key = ("scoring-dashboard", service_id, since_hours, threshold, n_good, n_bad, n_neutral)

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
def sessions_endpoint(req: SessionsRequest, ctx: RequestContext = Depends(build_request_context)):
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
def sessions_detail(req: SessionDetailRequest, ctx: RequestContext = Depends(build_request_context)):
    with process_context_scope("sessions:detail"):
        ip, ja4, start_raw, end_raw = req.ip, req.ja4, req.start_time, req.end_time
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
                con=con, src=ctx.source, ip=ip, ja4=ja4, session_start=start_time, session_end=end_time
            ),
        )
        return SessionDetailResponse.with_telemetry(**res)


def _build_scoring_config_bundle(service_id: str) -> dict:
    from . import scoring_admin

    return scoring_admin._build_scoring_config_bundle(service_id)


def l2_enforce_readiness_block(service_id: str) -> dict:
    from . import scoring_admin

    return scoring_admin.l2_enforce_readiness_block(service_id)
