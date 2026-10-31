# ruff: noqa: T201
"""Declarative reconciliation control loops (VCL, CDN service, and multi-service IaC)."""

from __future__ import annotations

import copy
import json
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from backend.core.fastly.client import fastly, find_service_by_name
from backend.core.fastly.client import find_service_by_name as _orig_find_service_by_name
from backend.provision.declarative.fastly_ops import (
    _activate_draft,
    _apply_diff,
    _clone_active_version,
    _ensure_cdn_domain,
    _fetch_active_version,
    _fetch_backends,
    _fetch_dictionaries,
    _fetch_highest_version,
    _fetch_logging_endpoints,
    _fetch_snippets,
    _purge_rum_surrogate_key,
    _update_local_config,
    _upload_rum_tracker_best_effort,
    _validate_draft,
    acquire_vcl_lock,
)
from backend.provision.declarative.models import (
    MANAGED_BACKEND_NAMES,
    MANAGED_DICTIONARY_NAMES,
    Backend,
    CmcdConfig,
    DeclarativeComputeService,
    DeclarativeKVStore,
    DeclarativeVCLService,
    DiffResult,
    FeatureState,
    LoggingEndpoint,
    ReconciliationResult,
    ScoringConfig,
    ServiceDictionary,
    UnifiedDesiredState,
    VCLSnippet,
    VclValidationError,
    compute_diff,
)
from backend.provision.declarative.vcl_builder import (
    _format_field_names,
    _is_managed_snippet,
    desired_backends,
    desired_dictionaries,
    desired_logging_endpoints,
    desired_snippets,
)
from backend.provision.session_scoring_orchestrator import (
    _SCORER_PACKAGE,
    _SCORING_FIELD_NAMES,
    MATRIX_STORE_NAME_TEMPLATE,
    _deploy_wasm_package,
    _find_kv_store,
    _remove_scoring_custom_fields,
    _write_matrix_to_kv,
    delete_scoring_service,
    ensure_scoring_service,
    scorer_vcl_fingerprint,
)

fastly_integration: Any = sys.modules[__name__]
logger = logging.getLogger(__name__)


def _record_reconcile_audit(
    service_id: str,
    diff: DiffResult,
    current_endpoints: list[LoggingEndpoint],
    desired_endpoints: list[LoggingEndpoint],
    version: int,
) -> None:
    """Write a ``log_format_change`` audit row for an activated reconcile."""
    before = _format_field_names(current_endpoints)
    after = _format_field_names(desired_endpoints)
    details = {
        "source": "reconciler",
        "version": version,
        "groups_before": [],
        "groups_after": [],
        "fields_added": sorted(after - before),
        "fields_removed": sorted(before - after),
        "changes": diff.summary(),
    }
    try:
        from backend.core import metadata as metadata_db

        metadata_db.record_audit(service_id, "log_format_change", details, actor="reconciler")
    except Exception:
        logger.warning("Failed to record reconcile audit for %s", service_id, exc_info=True)


def _load_or_bootstrap_feature_state(service_id: str, token: str, *, persist_bootstrap: bool = False) -> FeatureState:
    from backend.config import config_lock, save_config
    from backend.config import config_path as _config_path

    config_path = _config_path(service_id)
    if not config_path.exists():
        state = _bootstrap_featurestate_from_fastly(service_id, token)
        if persist_bootstrap:
            try:
                with config_lock(service_id):
                    if not config_path.exists():
                        save_config(service_id, asdict(state))
            except OSError as e:
                logging.getLogger("backend.scheduler").info(f"Skipped updating read-only config cache: {e}")
        return state
    return FeatureState.from_config(json.loads(config_path.read_text()))


def reconcile_vcl_state(
    service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop (Steps 1-8)."""
    result = ReconciliationResult(service_id=service_id)
    start_time = time.time()
    try:
        with acquire_vcl_lock(service_id):
            desired_state = _load_or_bootstrap_feature_state(service_id, token, persist_bootstrap=True)
            if status_cb:
                status_cb("⏳ Fetching active configuration from Fastly...")
            current_snippets = _fetch_snippets(service_id, token)
            current_endpoints = _fetch_logging_endpoints(service_id, token)
            current_backends = [b for b in _fetch_backends(service_id, token) if b.name in MANAGED_BACKEND_NAMES]
            current_dictionaries = [
                d for d in _fetch_dictionaries(service_id, token) if d.name in MANAGED_DICTIONARY_NAMES
            ]
            if status_cb:
                status_cb("⏳ Computing VCL configuration difference...")
            desired_snippets_list = desired_snippets(desired_state)
            desired_endpoints_list = desired_logging_endpoints(desired_state)
            desired_backends_list = desired_backends(desired_state)
            desired_dictionaries_list = desired_dictionaries(desired_state)
            for ep in desired_endpoints_list:
                if ep.format_string and len(ep.format_string) > 12288:
                    raise VclValidationError(
                        service_id=service_id,
                        draft_version=0,
                        errors=(
                            f"LOG_FORMAT_TOO_LONG: Logging endpoint '{ep.name}' format string length is {len(ep.format_string)} characters, "
                            "which exceeds Fastly's maximum allowed limit of 12288 characters. "
                            "Please disable some unused log field groups or shorten custom field expressions to fit within the limit."
                        ),
                    )
            diff = compute_diff(
                current_snippets,
                desired_snippets_list,
                current_endpoints,
                desired_endpoints_list,
                current_backends,
                desired_backends_list,
                current_dictionaries,
                desired_dictionaries_list,
            )
            diff.snippets_to_remove = [n for n in diff.snippets_to_remove if _is_managed_snippet(n)]
            if diff.is_empty():
                if desired_state.rum_enabled:
                    _upload_rum_tracker_best_effort(service_id, token, status_cb)
                return _finalize_reconcile_early_exit(result, service_id, token, start_time)
            if dry_run:
                result.changes_applied = diff.summary()
                return _finalize_reconcile_early_exit(result, service_id, token, start_time)
            draft_version = None
            try:
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(service_id, token, desired_state)
                if status_cb:
                    status_cb("⏳ Applying custom snippets and VCL log format...")
                _apply_diff(
                    service_id,
                    token,
                    draft_version,
                    diff,
                    desired_endpoints=desired_endpoints_list,
                    desired_state=desired_state,
                    status_cb=status_cb,
                    current_snippets=current_snippets,
                )
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                if validation_errors := _validate_draft(service_id, token, draft_version):
                    raise VclValidationError(service_id, draft_version, validation_errors)
                result.draft_version = draft_version
                if activate:
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(service_id, token, draft_version)
                    result.activated_version = draft_version
                    _record_reconcile_audit(service_id, diff, current_endpoints, desired_endpoints_list, draft_version)
                    if desired_state.rum_enabled:
                        _upload_rum_tracker_best_effort(service_id, token, status_cb)
                    _purge_rum_surrogate_key(service_id, token)

                    def _set_last_activated_version(c: dict[str, Any]) -> None:
                        c["last_activated_version"] = draft_version

                    _update_local_config(service_id, _set_last_activated_version)
                elif status_cb:
                    status_cb(
                        f"✓ Configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                    )
                result.changes_applied = diff.summary()
            except Exception as rollback_err:
                if draft_version is not None:
                    if status_cb:
                        status_cb(
                            f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft (Fastly does not support deleting individual draft versions)."
                        )
                    logging.getLogger("backend.scheduler").warning(
                        f"Reconcile failed: {rollback_err}. Broken draft version {draft_version} left in Fastly configuration."
                    )
                raise rollback_err
    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)
    return result


def _finalize_reconcile_early_exit(
    result: ReconciliationResult,
    service_id: str,
    token: str,
    start_time: float,
    *,
    active_ver: int | None = None,
    lookup_active: bool = True,
) -> ReconciliationResult:
    if lookup_active:
        try:
            result.activated_version = _fetch_active_version(service_id, token)
        except RuntimeError:
            result.activated_version = None
    else:
        result.activated_version = active_ver
    result.duration_ms = int((time.time() - start_time) * 1000)
    return result


def reconcile_cdn_service_state(
    logging_service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop for the FOS Proxy / CDN service (Steps 1-8)."""
    from backend.config import config_path as _config_path
    from backend.core.fastly.client import SHIELD_MAP, load_vcl, region_endpoint
    from backend.provision.fastly_api import _CDN_SNIPPETS

    cdn_managed_backend_names = {"fos_origin"}
    cdn_managed_dictionary_names = {"fos_credentials", "cdn_auth"}
    start_time = time.time()
    config_path = _config_path(logging_service_id)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    cfg = json.loads(config_path.read_text())
    fos_proxy_cfg = cfg.get("fos_proxy", {})
    cdn_service_id = fos_proxy_cfg.get("service_id") or cfg.get("cdn_service_id", "")
    cdn_service_name = (
        fos_proxy_cfg.get("service_name")
        or cfg.get("cdn_service_name", "")
        or f"Log Analysis CDN Service for {logging_service_id}"
    )
    cdn_url = fos_proxy_cfg.get("domain") or cfg.get("cdn_url", "")
    cdn_shield = fos_proxy_cfg.get("shield") or cfg.get("cdn_shield", "")
    cdn_secret = fos_proxy_cfg.get("secret") or cfg.get("cdn_secret", "")
    rate_limiting_enabled = (
        fos_proxy_cfg["rate_limiting_enabled"]
        if "rate_limiting_enabled" in fos_proxy_cfg
        else cfg.get("rate_limiting", True)
    )
    fos_access_key = cfg.get("fos_access_key_id", "")
    fos_secret_key = cfg.get("fos_secret_access_key", "")
    fos_bucket = cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", "")
    fos_region = cfg.get("fos_region", "us-east-1")
    fos_host = region_endpoint(fos_region)
    if not cdn_url:
        raise ValueError("CDN URL is required to reconcile CDN service state.")
    domain = cdn_url.replace("https://", "").replace("http://", "").split("/")[0]
    if not cdn_service_id:
        if status_cb:
            status_cb(f"⏳ Finding existing CDN service named '{cdn_service_name}'...")
        from backend.core.fastly import client as _fastly_service

        _find_fn = (
            _fastly_service.find_service_by_name
            if _fastly_service.find_service_by_name is not _orig_find_service_by_name
            else find_service_by_name
        )
        existing = _find_fn(cdn_service_name, token)
        if existing and existing.get("id") == logging_service_id:
            existing = None
            if status_cb:
                status_cb(
                    f"⚠️ Ignoring service {logging_service_id} as CDN candidate; the analytics CDN must be a separate service."
                )
        if existing:
            cdn_service_id = existing["id"]
            if status_cb:
                status_cb(f"✓ Found existing CDN service: {cdn_service_id}")
            if not dry_run:
                _ensure_cdn_domain(cdn_service_id, domain, token, status_cb=status_cb)
        else:
            if dry_run:
                if status_cb:
                    status_cb(f"ℹ️ [Dry Run] Would create new CDN service '{cdn_service_name}'")
                return ReconciliationResult(service_id="dry-run-cdn")
            if status_cb:
                status_cb(f"⏳ Creating new Fastly CDN service '{cdn_service_name}'...")
            svc = fastly("POST", "/service", {"name": cdn_service_name, "type": "vcl"}, token=token)
            cdn_service_id = svc["id"]
            if status_cb:
                status_cb(f"✓ Created CDN service {cdn_service_id}")
            service_comment = (
                f"CDN fronting service for the Fastly Object Storage log bucket associated with "
                f"service {logging_service_id}. Provides authenticated read access to stored log "
                f"files for the Fastly Log Analysis tool."
            )
            fastly("PUT", f"/service/{cdn_service_id}", {"comment": service_comment}, token=token)
            if status_cb:
                status_cb(f"⏳ Adding domain '{domain}' to CDN service...")
            fastly(
                "POST",
                f"/service/{cdn_service_id}/version/1/domain",
                {"name": domain, "comment": "Log Analysis CDN"},
                token=token,
            )
            if status_cb:
                status_cb("✓ Domain added to CDN service")

        def _persist_cdn(c: dict[str, Any]) -> None:
            c.setdefault("fos_proxy", {}).update(
                {
                    "service_id": cdn_service_id,
                    "service_name": cdn_service_name,
                    "domain": cdn_url,
                    "shield": cdn_shield,
                    "secret": cdn_secret,
                }
            )
            c["cdn_service_id"] = cdn_service_id

        _update_local_config(logging_service_id, _persist_cdn)
    result = ReconciliationResult(service_id=cdn_service_id)
    try:
        with acquire_vcl_lock(cdn_service_id):
            if status_cb:
                status_cb("⏳ Fetching active CDN configuration from Fastly...")
            current_snippets = _fetch_snippets(cdn_service_id, token)
            current_backends = [
                b for b in _fetch_backends(cdn_service_id, token) if b.name in cdn_managed_backend_names
            ]
            current_dictionaries = [
                d for d in _fetch_dictionaries(cdn_service_id, token) if d.name in cdn_managed_dictionary_names
            ]
            active_ver = fastly_integration.fetch_active_version(cdn_service_id, token)
            current_vcl_content = ""
            if active_ver is not None:
                try:
                    vcl_resp = fastly("GET", f"/service/{cdn_service_id}/version/{active_ver}/vcl/main", token=token)
                    if isinstance(vcl_resp, dict):
                        current_vcl_content = vcl_resp.get("content", "")
                except Exception:
                    pass
            desired_vcl_content = load_vcl(rate_limiting=rate_limiting_enabled)
            desired_snippets_list = [
                VCLSnippet(name=name, subroutine=stype, body=content, priority=priority)
                for name, stype, content, priority in _CDN_SNIPPETS
            ]
            shield_pop = cdn_shield or SHIELD_MAP.get(fos_region, "iad-va-us")
            shield_val = shield_pop if shield_pop.lower() != "none" else ""
            desired_backends_list = [
                Backend(
                    name="fos_origin",
                    address=fos_host,
                    port=443,
                    ssl_check_cert=True,
                    ssl_hostname=fos_host,
                    use_ssl=True,
                    override_host=fos_host,
                    auto_loadbalance=False,
                    shield=shield_val,
                    connect_timeout=5000,
                    first_byte_timeout=60000,
                    between_bytes_timeout=30000,
                )
            ]
            fos_items = (
                {"access_key": fos_access_key, "secret_key": fos_secret_key, "bucket": fos_bucket, "region": fos_region}
                if fos_access_key
                else {}
            )
            desired_dictionaries_list = [
                ServiceDictionary(name="fos_credentials", write_only=True, items=fos_items),
                ServiceDictionary(name="cdn_auth", write_only=True, items={"secret": cdn_secret} if cdn_secret else {}),
            ]
            diff = compute_diff(
                current_snippets,
                desired_snippets_list,
                [],
                [],
                current_backends,
                desired_backends_list,
                current_dictionaries,
                desired_dictionaries_list,
            )
            vcl_needs_update = current_vcl_content != desired_vcl_content
            if diff.is_empty() and not vcl_needs_update:
                if status_cb:
                    status_cb("✓ CDN service configuration is up-to-date and matches desired state. No changes needed.")
                return _finalize_reconcile_early_exit(
                    result, cdn_service_id, token, start_time, active_ver=active_ver, lookup_active=False
                )
            if dry_run:
                result.changes_applied = {**diff.summary(), "main_vcl_updated": vcl_needs_update}
                return _finalize_reconcile_early_exit(
                    result, cdn_service_id, token, start_time, active_ver=active_ver, lookup_active=False
                )
            draft_version = None
            try:
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(cdn_service_id, token)
                if status_cb:
                    status_cb("⏳ Applying custom snippets and dictionaries...")
                _apply_diff(
                    cdn_service_id,
                    token,
                    draft_version,
                    diff,
                    desired_endpoints=[],
                    desired_state=None,
                    status_cb=status_cb,
                    current_snippets=current_snippets,
                )
                if vcl_needs_update:
                    if status_cb:
                        status_cb("⏳ Updating main VCL content...")
                    try:
                        fastly(
                            "PUT",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl/main",
                            {"content": desired_vcl_content},
                            token=token,
                        )
                    except Exception:
                        fastly(
                            "POST",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl",
                            {"name": "main", "content": desired_vcl_content, "main": True},
                            token=token,
                        )
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                if validation_errors := _validate_draft(cdn_service_id, token, draft_version):
                    raise VclValidationError(cdn_service_id, draft_version, validation_errors)
                result.draft_version = draft_version
                if activate:
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(cdn_service_id, token, draft_version)
                    result.activated_version = draft_version

                    def _persist_activation(c: dict[str, Any]) -> None:
                        c.setdefault("fos_proxy", {})["last_activated_version"] = draft_version
                        c["last_activated_version_cdn"] = draft_version

                    _update_local_config(logging_service_id, _persist_activation)
                elif status_cb:
                    status_cb(
                        f"✓ CDN configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                    )
                result.changes_applied = {**diff.summary(), "main_vcl_updated": vcl_needs_update}
            except Exception as rollback_err:
                if draft_version is not None and status_cb:
                    status_cb(
                        f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft."
                    )
                raise rollback_err
    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)
    return result


def _bootstrap_featurestate_from_fastly(service_id: str, token: str) -> FeatureState:
    """Introspect active (or draft) Fastly version and reverse-engineer FeatureState (Gotcha 1)."""
    source_ver = fastly_integration.fetch_active_version(service_id, token) or _fetch_highest_version(service_id, token)
    if source_ver is None:
        return FeatureState(
            service_id=service_id,
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
        )
    snippets = fastly_integration.fetch_snippets(service_id, source_ver, token)
    endpoints = fastly_integration.fetch_logging_endpoints(service_id, source_ver, token)
    snippet_names = {s.name for s in snippets}
    rum_enabled = any("RUM" in name for name in snippet_names)
    scoring_enabled = any("Session Scoring" in name for name in snippet_names)
    cmcd_enabled = any("CMCD" in name for name in snippet_names)
    if not rum_enabled:
        try:
            vcl_resp = fastly("GET", f"/service/{service_id}/version/{source_ver}/vcl", token=token)
            if isinstance(vcl_resp, dict) and "items" in vcl_resp:
                vcl_content = " ".join([v.get("content", "") for v in vcl_resp["items"]])
                if "fos_origin" in vcl_content or "F_fos_origin" in vcl_content or "rum" in vcl_content.lower():
                    rum_enabled = True
                if "session_scorer" in vcl_content or "Session Scoring" in vcl_content:
                    scoring_enabled = True
        except Exception:
            pass
    log_period = 60
    rum_log_period = None
    for endpoint in endpoints:
        if (
            "Fastly Log Analytics" in endpoint.name or "Fastly Object Storage Logs" in endpoint.name
        ) and "RUM" not in endpoint.name:
            log_period = endpoint.period
        elif "RUM" in endpoint.name:
            rum_log_period = endpoint.period
    return FeatureState(
        service_id=service_id,
        log_period=log_period,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="raw",
        fos_endpoint="fos.example.com",
        rum_enabled=rum_enabled,
        rum_log_period=rum_log_period,
        cmcd=CmcdConfig(enabled=cmcd_enabled),
        scoring=ScoringConfig(enabled=scoring_enabled, domain="" if not scoring_enabled else "scorer.example.com"),
    )


def build_desired_state(logging_service_id: str, token: str) -> UnifiedDesiredState:
    """Build the complete, unified desired infrastructure state from configuration."""
    state = _load_or_bootstrap_feature_state(logging_service_id, token, persist_bootstrap=False)
    vcl_service = DeclarativeVCLService(
        name=state.logging_endpoint_name,
        custom_fields=state.log_fields.custom_fields,
        logging_endpoints=[ep if isinstance(ep, dict) else ep.to_dict() for ep in desired_logging_endpoints(state)],
        backends=[b if isinstance(b, dict) else b.to_dict() for b in desired_backends(state)],
        snippets=[s if isinstance(s, dict) else s.to_dict() for s in desired_snippets(state)],
    )
    compute_scorer = None
    kv_stores: dict[str, DeclarativeKVStore] = {}
    if state.scoring.enabled:
        from backend.provision.session_scoring_orchestrator import _resolve_tenant_matrix_for_deploy
        from backend.scoring import serialize_kv

        store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
        compute_scorer = DeclarativeComputeService(
            name=f"Session Scoring Service for {logging_service_id}",
            package_path=str(_SCORER_PACKAGE),
            domains=[state.scoring.domain],
            linked_kv_stores=[store_name],
        )
        keys: dict[str, str] = {}
        if (matrix_path := _resolve_tenant_matrix_for_deploy(logging_service_id)) and matrix_path.exists():
            with matrix_path.open() as f:
                keys["matrix"] = serialize_kv(json.load(f)).hex()
        kv_stores[store_name] = DeclarativeKVStore(name=store_name, keys=keys, link_to_services=[compute_scorer.name])
    return UnifiedDesiredState(
        service_id=logging_service_id, vcl_service=vcl_service, compute_scorer=compute_scorer, kv_stores=kv_stores
    )


def discover_current_state(logging_service_id: str, token: str) -> dict[str, Any]:
    """Retrieve the active live infrastructure matching names in our scope."""
    store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
    store = _find_kv_store(store_name, token)
    return {
        "kv_stores": {store_name: store} if store else {},
        "compute_scorer": find_service_by_name(f"Session Scoring Service for {logging_service_id}", token),
    }


def verify_compute_scorer_readiness(
    compute_def: DeclarativeComputeService, token: str, status_cb: Callable[[str], None] | None = None
) -> None:
    """Probes the Compute Scorer domain to guarantee DNS and Edge Routing are active."""
    import httpx
    from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

    url = f"https://{compute_def.domains[0]}/health"
    max_retries = 10
    for attempt in Retrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1.0, exp_base=1.5, min=1.0, max=5.0),
        retry=retry_if_exception_type(Exception),
        sleep=lambda s: time.sleep(s),
        reraise=False,
    ):
        with attempt:
            idx = attempt.retry_state.attempt_number
            if status_cb:
                status_cb(f"🔍 Issuing HTTP probe to {url} (attempt {idx}/{max_retries})...")
            try:
                response = httpx.get(url, headers={"X-Edge-Scorer-Auth": "probe-sentinel"}, timeout=2.0)
            except Exception as e:
                if status_cb:
                    status_cb(f"⚠️ Probe {idx} failed: {type(e).__name__}. Retrying...")
                raise
            if response.status_code in (200, 401, 404):
                if status_cb:
                    status_cb(f"✅ Compute Scorer is hot (status {response.status_code}).")
                return
            raise RuntimeError(f"Unexpected status {response.status_code}")
    raise RuntimeError(f"Readiness probe failed: Scorer service at {url} did not respond within timeout window.")


def reconcile_infrastructure(
    logging_service_id: str, token: str, dry_run: bool = False, status_cb: Callable[[str], None] | None = None
) -> None:
    """The central unified IaC control loop managing all multi-service resource operations."""
    desired = build_desired_state(logging_service_id, token)
    if status_cb:
        status_cb("⏳ Discovering active edge resources on Fastly account...")
    current = discover_current_state(logging_service_id, token)
    if dry_run:
        return
    from backend.config import config_path as _config_path
    from backend.config import mutate_config, snapshot_config

    config_path = _config_path(logging_service_id)
    if (orig_config_content := snapshot_config(logging_service_id)) is None:
        raise RuntimeError(f"Configuration file {config_path} is required for live deployment.")
    cfg = json.loads(orig_config_content)
    orig_scoring = copy.deepcopy(cfg.get("scoring"))
    orig_scoring_fields = [
        copy.deepcopy(cf)
        for cf in (cfg.get("log_fields") or {}).get("custom_fields") or []
        if cf.get("name") in _SCORING_FIELD_NAMES
    ]
    persisted = False

    def _persist(apply: Callable[[dict[str, Any]], None]) -> None:
        nonlocal persisted
        persisted = True
        if mutate_config(logging_service_id, apply) is None:
            raise RuntimeError(f"Configuration file {config_path} disappeared during reconciliation.")

    def _rollback_owned_keys(disk_cfg: dict[str, Any]) -> None:
        if orig_scoring is None:
            disk_cfg.pop("scoring", None)
        else:
            disk_cfg["scoring"] = copy.deepcopy(orig_scoring)
        if orig_scoring_fields:
            _remove_scoring_custom_fields(disk_cfg)
            lf = disk_cfg.setdefault("log_fields", {})
            lf["custom_fields"] = [*(lf.get("custom_fields") or []), *copy.deepcopy(orig_scoring_fields)]

    try:
        apply_phase3: Callable[[dict[str, Any]], None] | None = None
        if desired.compute_scorer:
            if status_cb:
                status_cb("⏳ Phase 1: Reconciling edge KV Stores...")
                status_cb("⏳ Phase 2: Reconciling Compute Scorer Service and uploading WASM package...")
            scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
            scoring_service_id, matrix_store_id = (
                scoring_meta["scoring_service_id"],
                scoring_meta.get("scoring_matrix_store_id", ""),
            )
            _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)
            package_meta = _deploy_wasm_package(scoring_service_id, matrix_store_id, token, status_cb=status_cb)
            resolved_domain = scoring_meta.get("scoring_domain") or (
                desired.compute_scorer.domains[0] if desired.compute_scorer.domains else ""
            )
            if resolved_domain:
                desired.compute_scorer.domains = [resolved_domain]
            cfg_scoring = cfg.get("scoring") or {}
            new_scoring: dict[str, Any] = {
                "enabled": True,
                "scoring_service_id": scoring_service_id,
                **{
                    k: scoring_meta.get(k, "")
                    for k in (
                        "scoring_service_name",
                        "scoring_domain",
                        "scoring_keys_store_id",
                        "scoring_config_store_id",
                    )
                },
                "scoring_matrix_store_id": matrix_store_id,
                "request_secret": scoring_meta.get("request_secret", cfg_scoring.get("request_secret", "")),
                "aes_key_hex": scoring_meta.get("aes_key_hex", "") or cfg_scoring.get("aes_key_hex", ""),
                "deployed_package_sha": package_meta["sha"],
                "deployed_package_files_hash": package_meta["files_hash"],
                "deployed_vcl_sha": scorer_vcl_fingerprint(logging_service_id),
            }

            def _set_scoring(disk_cfg: dict[str, Any]) -> None:
                existing_scoring = disk_cfg.get("scoring")
                base_scoring = copy.deepcopy(existing_scoring) if isinstance(existing_scoring, dict) else {}
                base_scoring.update(copy.deepcopy(new_scoring))
                disk_cfg["scoring"] = base_scoring

            apply_phase3 = _set_scoring
            if status_cb:
                status_cb("⏳ Phase 2.5: Issuing HTTP readiness checks on deployed domain...")
            verify_compute_scorer_readiness(desired.compute_scorer, token, status_cb=status_cb)
        elif current["compute_scorer"]:
            if status_cb:
                status_cb("⏳ Phase 4: Executing safe reverse-topological teardown of disabled assets...")
                status_cb("⏳ Stripping VCL scoring backend and snippets first...")

            def _disable_scoring_keys(disk_cfg: dict[str, Any]) -> None:
                _remove_scoring_custom_fields(disk_cfg)
                if "scoring" in disk_cfg:
                    disk_cfg["scoring"]["enabled"] = False

            apply_phase3 = _disable_scoring_keys
        if status_cb:
            status_cb("⏳ Phase 3: Syncing VCL Edge Logging Configurations, Custom Fields, and Backends...")
        if apply_phase3 is not None:
            _persist(apply_phase3)
        reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb)
        if not desired.compute_scorer and current["compute_scorer"]:
            if status_cb:
                status_cb("⏳ Sleeping/Draining edge requests for 5 seconds to ensure clean cutover...")
            time.sleep(5.0)
            if status_cb:
                status_cb("⏳ Deleting serverless Compute Scorer service and resources last...")
            old_matrix_store_id = (
                current["kv_stores"].get(MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id), {}).get("id", "")
            )
            prior_scoring = json.loads(orig_config_content).get("scoring") or {}
            delete_scoring_service(
                scoring_service_id=current["compute_scorer"].get("id"),
                scoring_keys_store_id=prior_scoring.get("scoring_keys_store_id", ""),
                scoring_config_store_id=prior_scoring.get("scoring_config_store_id", ""),
                scoring_matrix_store_id=old_matrix_store_id,
                token=token,
                status_cb=status_cb,
            )
            _persist(lambda disk_cfg: disk_cfg.pop("scoring", None))
        if status_cb:
            status_cb("✅ Infrastructure state reconciliation completed successfully.")
    except Exception as err:
        logger.error("IaC reconciliation failure, executing transactional configuration rollback", exc_info=True)
        if status_cb:
            status_cb(f"❌ Error during reconciliation: {err}. Rolling back to previous configuration...")
        if persisted:
            mutate_config(logging_service_id, _rollback_owned_keys)
        raise err
