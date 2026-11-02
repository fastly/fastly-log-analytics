"""Session scoring VCL, edge deployment, status, and snippet configuration endpoints."""

from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query

from backend.core import metadata as metadata_db
from backend.core.request_context import ServiceId, require_admin
from backend.models.analytics import (
    ScoringEnforceStatusCodeBody,
    ScoringEnforceStatusCodePutResponse,
    ScoringEnforceStatusCodeState,
    ScoringExcludeRegexBody,
    ScoringExcludeRegexPutResponse,
    ScoringExcludeRegexState,
    ScoringExcludeRegexValidateResponse,
    ScoringStatusResponse,
    ScoringTokenBody,
)
from backend.utils.bounded_cache import BoundedTTLCache as _BoundedTTLCache
from backend.utils.router_utils import (
    load_service_config,
    raise_internal,
    resolve_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response, stream_iac_reconcile

logger = logging.getLogger(__name__)
router = APIRouter()

_SECRET_KEYS = frozenset({"aes_key_hex", "request_secret"})
_scoring_svc_version_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
_scoring_pragma_cache: _BoundedTTLCache = _BoundedTTLCache(maxsize=256, ttl_seconds=60.0)


def _safe_scoring_dict(scoring: dict | None) -> dict:
    return {k: v for k, v in (scoring or {}).items() if k not in _SECRET_KEYS}


def _require_fastly_token(
    service_id: str, body: ScoringTokenBody | None, err_msg: str = "Fastly API token required"
) -> str:
    if not (resolved_token := resolve_fastly_token(service_id, body.token if body else "")):
        raise HTTPException(status_code=400, detail={"error": err_msg})
    return resolved_token


def _record_scoring_toggle_audit(service_id: str, event_type: str, details: dict | None = None) -> None:
    metadata_db.record_scoring_audit(service_id, event_type, details=details)
    metadata_db.record_audit(service_id=service_id, event_type=event_type, details=details or {}, actor="operator")


def _record_scoring_audit(service_id: str, action: str, details: dict) -> None:
    metadata_db.record_scoring_audit(service_id, action, details=details)


def _load_scoring_cfg(service_id: str) -> tuple[dict, dict]:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    return cfg, (cfg.get("scoring") or {})


def _require_confirm(confirm: bool, detail_msg: str) -> None:
    if not confirm:
        raise HTTPException(status_code=400, detail={"error": detail_msg})


def _require_scoring_enabled_and_token(service_id: str, token: str = "") -> tuple[dict, dict, str]:
    cfg, scoring = _load_scoring_cfg(service_id)
    if not scoring.get("enabled"):
        raise HTTPException(status_code=400, detail={"error": "Session scoring is not enabled for this service"})
    resolved_token = resolve_fastly_token(service_id, token)
    if not resolved_token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token required"})
    return cfg, scoring, resolved_token


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


def _validate_service_exclude_regex(service_id: str, raw: str):
    from backend.core.vcl_generator import validate_recv_exclusion_regex_with_lint
    from backend.provision.session_scoring_orchestrator import recv_snippet

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


@router.post("/{service_id}/scoring/enable", dependencies=[Depends(require_admin)])
def scoring_enable(
    service_id: str = Path(..., description="Logging service ID to enable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Enable session scoring for the given logging service (streams SSE status events)."""
    resolved_token = _require_fastly_token(
        service_id, body, "Fastly API token required (pass in JSON body or set in service config)"
    )
    from backend import config as svcconfig

    existing_cfg = svcconfig.load_config(service_id) or {}
    if not (existing_cfg.get("scoring") or {}).get("enabled"):
        from backend.provision.fastly_api import service_has_scoring_pragmas

        if service_has_scoring_pragmas(service_id, resolved_token) is False:
            msg = (
                f"Preflight request body preservation is not enabled on service {service_id}. "
                "Please contact your Fastly account team to enable the service pragmas that "
                "preserve unsent request bodies across restarts and omit request bodies when "
                "backend requests are rewritten to GET/HEAD, then try again."
            )
            return sse_response(
                iter([json.dumps({"type": "error", "code": "missing_scoring_pragmas", "message": msg})])
            )
    svcconfig.mutate_config(
        service_id, lambda c: c.__setitem__("scoring", {**(c.get("scoring") or {}), "enabled": True})
    )
    safe_scoring_state: dict = {}

    def _done_event() -> dict:
        safe_scoring = _safe_scoring_dict((svcconfig.load_config(service_id) or {}).get("scoring"))
        safe_scoring_state.update(safe_scoring)
        return {"type": "done", "message": "Session scoring enabled.", "scoring": safe_scoring}

    def _after_done() -> None:
        _record_scoring_toggle_audit(
            service_id,
            "scoring_enabled",
            details={k: safe_scoring_state.get(k) for k in ("scoring_service_id", "matrix_version")},
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


@router.post("/{service_id}/scoring/disable", dependencies=[Depends(require_admin)])
def scoring_disable(
    service_id: str = Path(..., description="Logging service ID to disable scoring on"),
    body: ScoringTokenBody | None = None,
):
    """Disable session scoring. Reverse of enable_scoring."""
    resolved_token = _require_fastly_token(service_id, body)
    from backend import config as svcconfig

    svcconfig.mutate_config(
        service_id, lambda c: c["scoring"].__setitem__("enabled", False) if "scoring" in c else None
    )
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
    "/{service_id}/scoring/status",
    response_model=ScoringStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_status(service_id: str = Path(..., description="Logging service ID")) -> dict:
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
    if (scoring_svc := scoring.get("scoring_service_id")) and token:
        from backend.core.fastly.client import get_active_version_info

        if info := _cached_fastly_probe(
            _scoring_svc_version_cache,
            scoring_svc,
            lambda: get_active_version_info(scoring_svc, token),
            "active-version lookup",
        ):
            result["scoring_active_version"] = info.get("number")
            result["scoring_activated_at"] = info.get("updated_at")
    result.update(
        scorer_drift=False,
        drift_detail=None,
        drift_known=bool(scoring.get("deployed_package_sha")) and bool(scoring.get("deployed_vcl_sha")),
    )
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
    except Exception:  # pragma: no cover
        logger.warning("[scoring-status] drift check failed for %s", service_id, exc_info=True)
    return result


@router.get(
    "/{service_id}/scoring/exclude-regex",
    response_model=ScoringExcludeRegexState,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def scoring_exclude_regex_get(service_id: ServiceId) -> dict:
    """Return the operator-configured URL-exclusion regex for the recv snippet."""
    from backend.provision.session_scoring_orchestrator import DEFAULT_ASSET_EXT_REGEX, resolve_exclude_url_regex

    _cfg, scoring = _load_scoring_cfg(service_id)
    current = scoring.get("exclude_url_regex") or ""
    return {
        "current": current,
        "is_default": (not current) or current == DEFAULT_ASSET_EXT_REGEX,
        "default": DEFAULT_ASSET_EXT_REGEX,
        "effective": resolve_exclude_url_regex(current or None),
    }


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
    from backend.core.vcl_generator import RegexValidationError
    from backend.provision.session_scoring_orchestrator import update_recv_exclusion_regex

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
def scoring_exclude_regex_validate(body: ScoringExcludeRegexBody, service_id: ServiceId) -> dict:
    """Run the 2-layer pre-publish validator on a candidate regex without persisting it."""
    from backend.core.vcl_generator import RegexValidationError

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
