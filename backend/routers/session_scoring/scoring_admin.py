"""Session scoring admin config, L2 enforcement, retrain, matrix history, key rotation, and audit endpoints."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query

from backend.core import metadata as metadata_db
from backend.core.request_context import ServiceId, require_admin
from backend.models.analytics import (
    ScoringAuditListResponse,
    ScoringConfigCompositeResponse,
    ScoringEnforceThresholdPutResponse,
    ScoringEnforceThresholdResponse,
    ScoringL2EnforceBody,
    ScoringL2EnforcePutResponse,
    ScoringL2EnforceState,
    ScoringMatrixRestoreResponse,
    ScoringMatrixVersionsResponse,
    ScoringRetrainResponse,
    ScoringRotateKeyResponse,
    ScoringThresholdBody,
    ScoringThresholdState,
)
from backend.provision.session_scoring_orchestrator import SCORING_ENABLED_AT_KEY as _SCORING_ENABLED_AT_KEY
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache
from backend.utils.router_utils import (
    load_service_config,
    query_errors,
    raise_internal,
    resolve_fastly_token,
)

from .scoring_analytics import (
    _MIN_LABELS_PER_CLASS,
    _bust_analytics_cache,
    _label_counts,
    _reconstruct_labeled_sessions,
    _tenant_matrix_path,
)
from .scoring_vcl import (
    _load_scoring_cfg,
    _record_scoring_audit,
    _require_confirm,
    _require_scoring_enabled_and_token,
    scoring_enforce_status_code_get,
    scoring_exclude_regex_get,
    scoring_status,
)

logger = logging.getLogger(__name__)
router = APIRouter()

_L2_ENABLED_AT_KEY = "l2_enabled_at"
_L2_ENFORCE_ENABLED_KEY = "l2_enforce_enabled"
_ENFORCE_THRESHOLD_KEY = "enforce_threshold"
_ENFORCE_THRESHOLD_CACHE_TTL = 300.0
_enforce_threshold_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)
_L2_RAMP_DAYS = 3.0
_L2_READINESS_DAYS = 7.0
_l2_enforce_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=512, ttl_seconds=_ENFORCE_THRESHOLD_CACHE_TTL)


def _l2_parse_epoch(raw: str | None) -> int | None:
    s = str(raw or "").strip()
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
    deployment_age_days = (
        max(0.0, (now_epoch - scoring_enabled_at) / 86_400.0) if scoring_enabled_at is not None else None
    )
    ready = bool(deployment_age_days is not None and deployment_age_days >= _L2_READINESS_DAYS)
    days_since_optin = (
        (0.0 if now_epoch <= l2_enabled_at else (now_epoch - l2_enabled_at) / 86_400.0)
        if l2_enabled_at is not None
        else None
    )
    ramp_progress = max(0.0, min(1.0, days_since_optin / _L2_RAMP_DAYS)) if days_since_optin is not None else 0.0
    fully_ramped = bool(days_since_optin is not None and days_since_optin >= _L2_RAMP_DAYS)
    warmup_days_remaining = max(0.0, _L2_RAMP_DAYS - days_since_optin) if days_since_optin is not None else None
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
    cache_key = (service_id, config_store_id)
    cached = _l2_enforce_cache.get(cache_key)
    if cached is not None:
        return cached
    block = _build_l2_enforce_block(
        scoring_enabled_at_raw=_l2_get_item(config_store_id, _SCORING_ENABLED_AT_KEY, token),
        l2_enforce_raw=_l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, token),
        l2_enabled_at_raw=_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, token),
        now_epoch=int(datetime.now(UTC).timestamp()),
    )
    _l2_enforce_cache[cache_key] = block
    return block


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
            from backend.scoring import serialize_kv

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
    token = resolve_fastly_token(service_id) if config_store_id else ""
    if not config_store_id or not token:
        return _l2_enforce_unavailable()
    try:
        return _read_l2_enforce_state(service_id, config_store_id, token)
    except Exception:  # noqa: BLE001 — advisory block must never break health
        return _l2_enforce_unavailable()


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
        fastly("PATCH", f"/resources/stores/config/{store_id}/item/{key}", {"item_value": value}, token=api_token)
    except Exception:
        fastly(
            "POST", f"/resources/stores/config/{store_id}/item", {"item_key": key, "item_value": value}, token=api_token
        )


def _validate_threshold_0_100(threshold: int | None) -> int | None:
    if threshold is not None and not 0 <= threshold <= 100:
        raise HTTPException(status_code=400, detail={"error": "threshold must be 0-100"})
    return threshold


@router.get(
    "/{service_id}/scoring/config",
    response_model=ScoringConfigCompositeResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_config_composite(service_id: str = Path(..., description="Logging service ID")) -> dict:
    """Composite of the four token-free /scoring/* config endpoints."""
    return {"status": scoring_status(service_id), **_build_scoring_config_bundle(service_id)}


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
    import backend.scoring as _fixtures
    import backend.scoring as _labels
    import backend.scoring as _matrix
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.scoring import DEFAULT_MIN_AUC
    from backend.scoring import evaluate as _evaluate

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    cfg = svcconfig.load_config(service_id) or {}
    matrix_version = version or _matrix.default_version()
    start = datetime.now(UTC) - timedelta(days=int(since_days))
    con = get_connection(source=src, max_wait=3, skip_view_update=True, read_only=True)
    try:
        sessions_iter = _fixtures.extract_traces(con, service_id=service_id, start=start)
        tmatrix, stats = _matrix.build_matrix(s.to_jsonl_dict() for s in sessions_iter)
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
            json.dump(matrix_dict, f)
        local_saved = True
    except Exception as exc:
        local_saved = False
        logger.warning(f"Could not write matrix.json locally: {exc}")
    fos_published = False
    try:
        from backend.core.metadata.base import publish_matrix_to_fos

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
            else "Matrix saved to FOS but NOT pushed to the live scorer (no KV store / "
            "Fastly token available). Re-enable scoring for this service to "
            "provision the matrix KV store and seed it. The /scoring/evaluation "
            "endpoint reflects the new matrix immediately regardless."
        ),
    }


@router.get(
    "/{service_id}/scoring/enforce-threshold",
    response_model=ScoringEnforceThresholdResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_enforce_threshold_get(service_id: ServiceId, token: str = Header(default="")) -> dict:
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
    result = {"threshold": threshold, "enforced": threshold is not None, "key": _ENFORCE_THRESHOLD_KEY}
    _enforce_threshold_cache[cache_key] = result
    return result


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
def scoring_l2_enforce_get(service_id: ServiceId, token: str = Header(default="")) -> dict:
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
    _cfg, config_store_id, resolved_token = _require_scoring_config_store(service_id, token)
    enabled = bool(body.enabled)
    try:
        l2_enabled_at: int | None
        if enabled:
            currently_on = _l2_get_item(config_store_id, _L2_ENFORCE_ENABLED_KEY, resolved_token).strip() == "1"
            existing_anchor = _l2_parse_epoch(_l2_get_item(config_store_id, _L2_ENABLED_AT_KEY, resolved_token))
            _upsert_config_store_item(resolved_token, config_store_id, _L2_ENFORCE_ENABLED_KEY, "1")
            if (not currently_on) or (existing_anchor is None):
                l2_enabled_at = int(datetime.now(UTC).timestamp())
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
    "/{service_id}/scoring/matrix-versions",
    response_model=ScoringMatrixVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_list(service_id: ServiceId) -> dict:
    """List historical scoring matrices archived in FOS."""
    from backend.core.metadata.base import list_scoring_matrix_versions

    _cfg, scoring = _load_scoring_cfg(service_id)
    return {"versions": list_scoring_matrix_versions(service_id), "current_version": scoring.get("matrix_version")}


@router.post(
    "/{service_id}/scoring/matrix-versions/{version}/restore",
    response_model=ScoringMatrixRestoreResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_matrix_versions_restore(
    service_id: ServiceId,
    version: str = Path(
        ..., description="Matrix version string to restore", pattern=r"^[A-Za-z0-9._-]+$", max_length=64
    ),
    confirm: bool = Query(default=False, description="Set true to actually perform the restore"),
) -> dict:
    """Restore a historical matrix in FOS, clear the local shadow copy, and push to KV."""
    _require_confirm(confirm, "Pass ?confirm=true to actually restore. This will replace the current matrix.")
    from backend import config as svcconfig
    from backend.core.metadata.base import restore_scoring_matrix_version

    result = restore_scoring_matrix_version(service_id, version)
    if not result:
        raise HTTPException(status_code=404, detail={"error": f"Matrix version {version!r} not found in FOS history"})
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
def scoring_rotate_key(service_id: ServiceId, token: str = Header(default="")) -> dict:
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
        details={"rotated_at": result["rotated_at"], "previous_key_grace": bool(result.get("previous_key_hex"))},
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
    load_service_config(service_id)
    return {"audit": metadata_db.list_scoring_audit(service_id, limit=limit, since=since), "limit": limit}


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
def scoring_threshold_put(body: ScoringThresholdBody, service_id: ServiceId) -> dict:
    """Persist the operator's chosen preview-only threshold (0-100 or null) into per-service config."""
    from backend import config as svcconfig

    threshold = _validate_threshold_0_100(body.threshold)
    cfg = load_service_config(service_id)
    prior_threshold = cfg.get("scoring", {}).get("operator_threshold")
    set_at = datetime.now(UTC).isoformat(timespec="seconds")

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


def _build_scoring_config_bundle(service_id: str) -> dict:
    return {
        "threshold": scoring_threshold_get(service_id),
        "exclude_regex": scoring_exclude_regex_get(service_id),
        "enforce_status_code": scoring_enforce_status_code_get(service_id),
    }


from . import scoring_analytics as _scoring_analytics_mod  # noqa: E402

_scoring_analytics_mod._build_scoring_config_bundle = _build_scoring_config_bundle
_scoring_analytics_mod.l2_enforce_readiness_block = l2_enforce_readiness_block
