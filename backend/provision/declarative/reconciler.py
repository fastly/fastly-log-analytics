# ruff: noqa: T201
"""VCL & Multi-Service Declarative Reconciliation Control Loop (Steps 1-8 + IaC Engine).

Consolidates ``backend.provision.declarative.reconciler`` (and ``diff``) with
``backend.provision.declarative.iac_reconciler``.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from backend.core.fastly.client import fastly, find_service_by_name
from backend.core.fastly.client import find_service_by_name as _orig_find_service_by_name
from backend.provision.declarative.state import _AUTO_INJECTED_NAMES, CmcdConfig, FeatureState, ScoringConfig
from backend.provision.rum_assets import upload_rum_tracker_js

logger = logging.getLogger(__name__)


# ============================================================================
# Diff Data Models & Computation (folded from diff.py)
# ============================================================================


@dataclass(unsafe_hash=True)
class VCLSnippet:
    """A single VCL snippet (name, priority, body, subroutine)."""

    name: str
    priority: int
    body: str
    subroutine: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LoggingEndpoint:
    """A single logging endpoint (S3, etc)."""

    name: str
    endpoint_type: str  # "s3"
    path: str
    period: int
    response_condition: str
    format_string: str
    placement: str | None  # "waf_log", "none", etc
    response_object_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.endpoint_type,
            "path": self.path,
            "period": self.period,
            "response_condition": self.response_condition,
            "format": self.format_string,
            "placement": self.placement,
            "response_object_name": self.response_object_name,
        }

    def __hash__(self) -> int:
        return hash((self.name, self.endpoint_type, self.path, self.period))


@dataclass
class Backend:
    """A single backend definition."""

    name: str
    address: str
    port: int
    ssl_check_cert: bool = True
    ssl_hostname: str = ""
    connect_timeout: int = 1000
    first_byte_timeout: int = 15000
    between_bytes_timeout: int = 10000
    auto_loadbalance: bool = True
    use_ssl: bool = True
    override_host: str = ""
    shield: str = ""
    request_condition: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __hash__(self) -> int:
        return hash((self.name, self.address, self.port))


@dataclass
class ServiceDictionary:
    """A single Fastly service dictionary (ConfigStore)."""

    name: str
    write_only: bool = field(default=False, compare=False)
    items: dict[str, str] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.name, tuple(sorted(self.items.items()))))


@dataclass
class DiffResult:
    """Computed diff between current and desired state."""

    snippets_to_add: list[VCLSnippet] = field(default_factory=list)
    snippets_to_update: list[VCLSnippet] = field(default_factory=list)
    snippets_to_remove: list[str] = field(default_factory=list)  # names only

    endpoints_to_add: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_update: list[LoggingEndpoint] = field(default_factory=list)
    endpoints_to_remove: list[str] = field(default_factory=list)  # names only

    backends_to_add: list[Backend] = field(default_factory=list)
    backends_to_update: list[Backend] = field(default_factory=list)
    backends_to_remove: list[str] = field(default_factory=list)  # names only

    dictionaries_to_add: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_update: list[ServiceDictionary] = field(default_factory=list)
    dictionaries_to_remove: list[str] = field(default_factory=list)  # names only

    def is_empty(self) -> bool:
        """True if no changes needed."""
        return (
            not self.snippets_to_add
            and not self.snippets_to_update
            and not self.snippets_to_remove
            and not self.endpoints_to_add
            and not self.endpoints_to_update
            and not self.endpoints_to_remove
            and not self.backends_to_add
            and not self.backends_to_update
            and not self.backends_to_remove
            and not self.dictionaries_to_add
            and not self.dictionaries_to_update
            and not self.dictionaries_to_remove
        )

    def summary(self) -> dict[str, Any]:
        """Return a summary of changes for logging."""
        return {
            "snippets_added": len(self.snippets_to_add),
            "snippets_updated": len(self.snippets_to_update),
            "snippets_removed": len(self.snippets_to_remove),
            "endpoints_added": len(self.endpoints_to_add),
            "endpoints_updated": len(self.endpoints_to_update),
            "endpoints_removed": len(self.endpoints_to_remove),
            "backends_added": len(self.backends_to_add),
            "backends_updated": len(self.backends_to_update),
            "backends_removed": len(self.backends_to_remove),
            "dictionaries_added": len(self.dictionaries_to_add),
            "dictionaries_updated": len(self.dictionaries_to_update),
            "dictionaries_removed": len(self.dictionaries_to_remove),
        }


def _diff_named_resources(
    current_items: list[Any],
    desired_items: list[Any],
    label: str,
) -> tuple[list[Any], list[Any], list[str]]:
    current_by_name = {item.name: item for item in current_items}
    desired_by_name = {item.name: item for item in desired_items}

    to_add: list[Any] = []
    to_update: list[Any] = []
    to_remove: list[str] = []

    for name, desired_item in desired_by_name.items():
        if name not in current_by_name:
            to_add.append(desired_item)
        elif current_by_name[name] != desired_item:
            to_update.append(desired_item)

    for name in current_by_name:
        if name not in desired_by_name:
            to_remove.append(name)

    overlap = {item.name for item in to_add} & {item.name for item in to_update}
    if overlap:
        raise RuntimeError(f"{label} in both to_add and to_update: {overlap}")

    return to_add, to_update, to_remove


def compute_diff(
    current_snippets: list[VCLSnippet],
    desired_snippets: list[VCLSnippet],
    current_endpoints: list[LoggingEndpoint],
    desired_endpoints: list[LoggingEndpoint],
    current_backends: list[Backend],
    desired_backends: list[Backend],
    current_dictionaries: list[ServiceDictionary] | None = None,
    desired_dictionaries: list[ServiceDictionary] | None = None,
) -> DiffResult:
    """Compute diff between current and desired state."""
    diff = DiffResult()
    current_dictionaries = current_dictionaries or []
    desired_dictionaries = desired_dictionaries or []

    diff.snippets_to_add, diff.snippets_to_update, diff.snippets_to_remove = _diff_named_resources(
        current_snippets, desired_snippets, "Snippets"
    )
    diff.endpoints_to_add, diff.endpoints_to_update, diff.endpoints_to_remove = _diff_named_resources(
        current_endpoints, desired_endpoints, "Endpoints"
    )
    diff.backends_to_add, diff.backends_to_update, diff.backends_to_remove = _diff_named_resources(
        current_backends, desired_backends, "Backends"
    )
    diff.dictionaries_to_add, diff.dictionaries_to_update, diff.dictionaries_to_remove = _diff_named_resources(
        current_dictionaries, desired_dictionaries, "Dictionaries"
    )

    return diff


sys.modules["backend.provision.declarative.diff"] = sys.modules[__name__]

from backend.provision.declarative.generators import (  # noqa: E402
    desired_backends,
    desired_dictionaries,
    desired_logging_endpoints,
    desired_snippets,
)

# ============================================================================
# Constants & Guards
# ============================================================================

MANAGED_BACKEND_NAMES = {
    "session_scorer",
    "fos_origin",
    "F_fos_origin",
    "F_F_fos_origin",
    "rum_collector",
}

MANAGED_DICTIONARY_NAMES = {"fos_credentials"}


class VclValidationError(Exception):
    """Raised when Fastly VCL validation fails."""

    def __init__(self, service_id: str, draft_version: int, errors: str) -> None:
        self.service_id = service_id
        self.draft_version = draft_version
        self.errors = errors
        super().__init__(f"VCL validation failed for {service_id} v{draft_version}: {errors}")


class LockAcquisitionTimeout(Exception):
    """Raised when VCL lock cannot be acquired within timeout."""

    def __init__(self, service_id: str, elapsed_seconds: float) -> None:
        self.service_id = service_id
        self.elapsed_seconds = elapsed_seconds
        super().__init__(f"Failed to acquire VCL lock for {service_id} after {elapsed_seconds:.1f}s")


@dataclass
class ReconciliationResult:
    """Result of a reconciliation run."""

    service_id: str
    activated_version: int | None = None
    draft_version: int | None = None
    changes_applied: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: int = 0
    audit_log_id: int | None = None

    def __post_init__(self) -> None:
        if self.changes_applied is None:
            self.changes_applied = {}


# ============================================================================
# Lock Management (Step 1)
# ============================================================================


class FileLock:
    """Simple file-based lock for concurrency control."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path

    def __enter__(self) -> FileLock:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.lock_path.exists():
            self.lock_path.unlink()


def acquire_vcl_lock(service_id: str, timeout_sec: int = 300) -> FileLock:
    """Acquire a file-based lock for reconciliation."""
    lock_path = Path(f"data/services/{service_id}.vcl.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    from contextlib import suppress

    from tenacity import Retrying, retry_if_exception_type, wait_exponential

    start = time.time()
    for attempt in Retrying(
        wait=wait_exponential(multiplier=0.1, exp_base=1.5, min=0.1, max=1.0),
        retry=retry_if_exception_type(FileExistsError),
        sleep=time.sleep,
        reraise=True,
    ):
        with attempt:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.write(fd, f"{os.getpid()} {int(time.time())}".encode())
                os.close(fd)
                return FileLock(lock_path)
            except FileExistsError:
                elapsed = time.time() - start
                if elapsed > timeout_sec:
                    raise LockAcquisitionTimeout(service_id, elapsed)
                with suppress(FileNotFoundError):
                    if time.time() - lock_path.stat().st_mtime > 300:
                        lock_path.unlink(missing_ok=True)
                raise
    raise LockAcquisitionTimeout(service_id, time.time() - start)


# ============================================================================
# Main Reconciliation Control Loop (Steps 2-8)
# ============================================================================


def _upload_rum_tracker_best_effort(service_id: str, token: str, status_cb: Callable[[str], None] | None) -> None:
    """Upload the RUM tracker JS, logging (not raising) on failure."""
    if status_cb:
        status_cb("⏳ Ensuring RUM tracker JS is uploaded to FOS...")
    try:
        upload_rum_tracker_js(service_id, token, status_cb=status_cb)
    except Exception as e:
        logger.warning(f"Failed to upload RUM tracker JS: {e}")
        if status_cb:
            status_cb(f"⚠️  RUM JS upload warning: {e}")


def _update_local_config(service_id: str, mutate: Callable[[dict[str, Any]], None]) -> None:
    """Best-effort locked edit of the local config cache."""
    from backend.config import mutate_config

    try:
        mutate_config(service_id, mutate)
    except OSError as e:
        logger.info("Skipped updating read-only config cache: %s", e)


_FORMAT_KEY_RE = re.compile(r'"([A-Za-z0-9_]+)"\s*:')


def _format_field_names(endpoints: list[LoggingEndpoint]) -> set[str]:
    return {m for ep in endpoints if ep.format_string for m in _FORMAT_KEY_RE.findall(ep.format_string)}


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


def _load_or_bootstrap_feature_state(
    service_id: str,
    token: str,
    *,
    persist_bootstrap: bool = False,
) -> FeatureState:
    from backend.config import config_path as _config_path

    config_path = _config_path(service_id)
    if not config_path.exists():
        state = _bootstrap_featurestate_from_fastly(service_id, token)
        if persist_bootstrap:
            try:
                from backend.config import config_lock, save_config

                with config_lock(service_id):
                    if not config_path.exists():
                        save_config(service_id, asdict(state))
            except OSError as e:
                logging.getLogger("backend.scheduler").info(f"Skipped updating read-only config cache: {e}")
        return state
    cfg = json.loads(config_path.read_text())
    return FeatureState.from_config(cfg)


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
                            f"which exceeds Fastly's maximum allowed limit of 12288 characters. "
                            f"Please disable some unused log field groups or shorten custom field expressions to fit within the limit."
                        ),
                    )

            diff = compute_diff(
                current_snippets=current_snippets,
                desired_snippets=desired_snippets_list,
                current_endpoints=current_endpoints,
                desired_endpoints=desired_endpoints_list,
                current_backends=current_backends,
                desired_backends=desired_backends_list,
                current_dictionaries=current_dictionaries,
                desired_dictionaries=desired_dictionaries_list,
            )

            _detect_and_queue_legacy_cleanup(current_snippets, diff, status_cb)
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
                validation_errors = _validate_draft(service_id, token, draft_version)
                if validation_errors:
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
                    _update_local_config(service_id, lambda c: c.__setitem__("last_activated_version", draft_version))
                else:
                    if status_cb:
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
    from backend.core.fastly.client import SHIELD_MAP, load_vcl, region_endpoint
    from backend.provision.fastly_api import _CDN_SNIPPETS

    cdn_managed_backend_names = {"fos_origin"}
    cdn_managed_dictionary_names = {"fos_credentials", "cdn_auth"}

    start_time = time.time()
    from backend.config import config_path as _config_path

    config_path = _config_path(logging_service_id)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    cfg = json.loads(config_path.read_text())

    fos_proxy_cfg = cfg.get("fos_proxy", {})
    cdn_service_id = fos_proxy_cfg.get("service_id") or cfg.get("cdn_service_id", "")
    cdn_service_name = fos_proxy_cfg.get("service_name") or cfg.get("cdn_service_name", "")
    cdn_url = fos_proxy_cfg.get("domain") or cfg.get("cdn_url", "")
    cdn_shield = fos_proxy_cfg.get("shield") or cfg.get("cdn_shield", "")
    cdn_secret = fos_proxy_cfg.get("secret") or cfg.get("cdn_secret", "")
    rate_limiting_enabled = (
        fos_proxy_cfg.get("rate_limiting_enabled")
        if "rate_limiting_enabled" in fos_proxy_cfg
        else cfg.get("rate_limiting", True)
    )

    fos_access_key = cfg.get("fos_access_key_id", "")
    fos_secret_key = cfg.get("fos_secret_access_key", "")
    fos_bucket = cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", "")
    fos_region = cfg.get("fos_region", "us-east-1")
    fos_host = region_endpoint(fos_region)

    if not cdn_service_name:
        cdn_service_name = f"Log Analysis CDN Service for {logging_service_id}"

    if not cdn_url:
        raise ValueError("CDN URL is required to reconcile CDN service state.")

    domain = cdn_url.replace("https://", "").replace("http://", "").split("/")[0]

    if not cdn_service_id:
        if status_cb:
            status_cb(f"⏳ Finding existing CDN service named '{cdn_service_name}'...")
        from backend.core.fastly import service as _fastly_service

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
                    f"⚠️ Ignoring service {logging_service_id} as CDN candidate; "
                    "the analytics CDN must be a separate service."
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
            proxy = c.setdefault("fos_proxy", {})
            proxy["service_id"] = cdn_service_id
            proxy["service_name"] = cdn_service_name
            proxy["domain"] = cdn_url
            proxy["shield"] = cdn_shield
            proxy["secret"] = cdn_secret
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

            desired_dictionaries_list = [
                ServiceDictionary(
                    name="fos_credentials",
                    write_only=True,
                    items={
                        "access_key": fos_access_key,
                        "secret_key": fos_secret_key,
                        "bucket": fos_bucket,
                        "region": fos_region,
                    }
                    if fos_access_key
                    else {},
                ),
                ServiceDictionary(
                    name="cdn_auth",
                    write_only=True,
                    items={"secret": cdn_secret} if cdn_secret else {},
                ),
            ]

            diff = compute_diff(
                current_snippets=current_snippets,
                desired_snippets=desired_snippets_list,
                current_endpoints=[],
                desired_endpoints=[],
                current_backends=current_backends,
                desired_backends=desired_backends_list,
                current_dictionaries=current_dictionaries,
                desired_dictionaries=desired_dictionaries_list,
            )

            vcl_needs_update = current_vcl_content != desired_vcl_content

            if diff.is_empty() and not vcl_needs_update:
                if status_cb:
                    status_cb("✓ CDN service configuration is up-to-date and matches desired state. No changes needed.")
                return _finalize_reconcile_early_exit(
                    result, cdn_service_id, token, start_time, active_ver=active_ver, lookup_active=False
                )

            if dry_run:
                changes = diff.summary()
                changes["main_vcl_updated"] = vcl_needs_update
                result.changes_applied = changes
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
                validation_errors = _validate_draft(cdn_service_id, token, draft_version)
                if validation_errors:
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
                else:
                    if status_cb:
                        status_cb(
                            f"✓ CDN configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                        )

                changes = diff.summary()
                changes["main_vcl_updated"] = vcl_needs_update
                result.changes_applied = changes

            except Exception as rollback_err:
                if draft_version is not None and status_cb:
                    status_cb(
                        f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft."
                    )
                raise rollback_err

    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)

    return result


# ============================================================================
# Helper Functions (Fastly API Integration)
# ============================================================================

from backend.provision.declarative import fastly_integration  # noqa: E402


def _bootstrap_featurestate_from_fastly(service_id: str, token: str) -> FeatureState:
    """Introspect active (or draft) Fastly version and reverse-engineer FeatureState (Gotcha 1)."""
    source_ver = fastly_integration.fetch_active_version(service_id, token)
    if source_ver is None:
        source_ver = _fetch_highest_version(service_id, token)

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
    sample_rate = 100
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
        sample_rate=sample_rate,
        edge_only=False,
        custom_condition="",
        fos_prefix="raw",
        fos_endpoint="fos.example.com",
        rum_enabled=rum_enabled,
        rum_log_period=rum_log_period,
        cmcd=CmcdConfig(enabled=cmcd_enabled),
        scoring=ScoringConfig(
            enabled=scoring_enabled,
            domain="" if not scoring_enabled else "scorer.example.com",
        ),
    )


def _fetch_highest_version(service_id: str, token: str, *, raise_on_error: bool = False) -> int | None:
    """Return the highest existing version number for ``service_id``, or ``None``."""
    try:
        resp = fastly("GET", f"/service/{service_id}/version", token=token)
    except Exception as e:
        if raise_on_error:
            raise RuntimeError(f"Cannot determine version to clone for {service_id}: {e}")
        return None
    items = resp if isinstance(resp, list) else (resp.get("items", []) if isinstance(resp, dict) else [])
    versions = sorted([int(v.get("number", 0)) for v in items if isinstance(v, dict)])
    return versions[-1] if versions else None


def _fetch_from_active(service_id: str, token: str, fetcher: Callable[[str, int, str], list[Any]]) -> list[Any]:
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    return fetcher(service_id, active_ver, token) if active_ver is not None else []


def _fetch_snippets(service_id: str, token: str) -> list[VCLSnippet]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_snippets)


def _fetch_logging_endpoints(service_id: str, token: str) -> list[LoggingEndpoint]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_logging_endpoints)


def _fetch_backends(service_id: str, token: str) -> list[Backend]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_backends)


def _fetch_dictionaries(service_id: str, token: str) -> list[ServiceDictionary]:
    return _fetch_from_active(service_id, token, fastly_integration.fetch_dictionaries)


def _ensure_cdn_domain(
    service_id: str, domain: str, token: str, status_cb: Callable[[str], None] | None = None
) -> None:
    """Add ``domain`` to a REUSED CDN service if it isn't already attached."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return
    existing_domains = fastly("GET", f"/service/{service_id}/version/{active_ver}/domain", token=token)
    names = {d.get("name", "").lower() for d in existing_domains} if isinstance(existing_domains, list) else set()
    if domain.lower() in names:
        return
    if status_cb:
        status_cb(f"⏳ Domain '{domain}' missing from existing CDN service; adding...")
    draft_version = fastly_integration.clone_version(service_id, active_ver, token, f"Add missing domain {domain}")
    fastly("POST", f"/service/{service_id}/version/{draft_version}/domain", {"name": domain}, token=token)
    error = _validate_draft(service_id, token, draft_version)
    if error:
        raise RuntimeError(f"Failed to validate CDN domain addition for {service_id}: {error}")
    _activate_draft(service_id, token, draft_version)
    if status_cb:
        status_cb(f"✓ Domain '{domain}' added to existing CDN service")


def _fetch_active_version(service_id: str, token: str) -> int:
    ver = fastly_integration.fetch_active_version(service_id, token)
    if ver is None:
        raise RuntimeError(f"No active version found for {service_id}")
    return ver


def _clone_active_version(service_id: str, token: str, desired_state: FeatureState | None = None) -> int:
    """Clone active version to a new draft version."""
    import datetime as dt

    ts = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    comment = f"Fastly Log Analytics Reconciliation at {ts}"

    if desired_state is not None:
        enabled_features = []
        if desired_state.logging_enabled:
            enabled_features.append("Request Logs")
        if desired_state.rum_enabled:
            enabled_features.append("RUM")
        if desired_state.scoring and desired_state.scoring.enabled:
            enabled_features.append("Session Scoring")
        if desired_state.cmcd and desired_state.cmcd.enabled:
            enabled_features.append("CMCD")

        user_custom_fields = (
            [cf for cf in desired_state.log_fields.custom_fields if cf.get("name") not in _AUTO_INJECTED_NAMES]
            if desired_state.log_fields
            else []
        )
        num_custom_fields = len(user_custom_fields)
        if num_custom_fields > 0:
            enabled_features.append(f"{num_custom_fields} Custom Field{'s' if num_custom_fields > 1 else ''}")

        if enabled_features:
            comment += f" (Enabled: {', '.join(enabled_features)})"
        else:
            comment += " (All Features Disabled / Teardown)"

    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is not None:
        return fastly_integration.clone_version(service_id, active_ver, token, comment)

    highest = _fetch_highest_version(service_id, token, raise_on_error=True)
    if highest is not None:
        return fastly_integration.clone_version(service_id, highest, token, comment)

    raise RuntimeError(f"Cannot determine version to clone for {service_id}")


def _apply_diff(
    service_id: str,
    token: str,
    draft_version: int,
    diff: DiffResult,
    desired_endpoints: list[LoggingEndpoint] | None = None,
    desired_state: FeatureState | None = None,
    status_cb: Callable[[str], None] | None = None,
    current_snippets: list[VCLSnippet] | None = None,
) -> None:
    """Apply diff mutations on the draft version (Step 6)."""
    desired_endpoints = desired_endpoints or []

    unmanaged_snippets = [s for s in (current_snippets or []) if not _is_managed_snippet(s.name)]
    vars_to_deconflict = [
        "fosAccessKey",
        "fosSecretKey",
        "fosBucket",
        "fosRegion",
        "fosHost",
        "canonicalHeaders",
        "signedHeaders",
        "canonicalRequest",
        "canonicalQuery",
        "stringToSign",
        "dateStamp",
        "signature",
        "scope",
    ]
    for snippet in diff.snippets_to_add + diff.snippets_to_update:
        body = snippet.body
        subroutine = snippet.subroutine
        for var_name in vars_to_deconflict:
            other_declares = any(
                subroutine == s.subroutine and f"declare local var.{var_name}" in s.body for s in unmanaged_snippets
            )
            if other_declares:
                reason = "unmanaged snippet"
                if status_cb:
                    status_cb(
                        f"ℹ️  {reason} already declares 'var.{var_name}' in '{subroutine}'. Skipping duplicate declaration."
                    )
                body = body.replace(
                    f"declare local var.{var_name} STRING;",
                    f"# declare local var.{var_name} STRING; (omitted to prevent collision with {reason})",
                )
        snippet.body = body

    bucket_name = None
    domain = None
    access_key = None
    secret_key = None
    from backend.config import config_path as _config_path

    config_path = _config_path(service_id)
    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text())
            bucket_name = cfg.get("fos_bucket") or cfg.get("fos_bucket_name")
            region = cfg.get("fos_region") or "us-east-1"
            from backend.core.fastly.client import region_endpoint

            domain = region_endpoint(region)
            access_key = cfg.get("fos_access_key_id") or cfg.get("fos_access_key")
            secret_key = cfg.get("fos_secret_access_key") or cfg.get("fos_secret_key")
        except Exception:
            pass

    def _purge_outdated(
        names: list[str],
        label: str,
        deleter: Callable[[str, int, str, str], None],
        *,
        whitelist: set[str] | None = None,
        err_label: str = "",
        skip_fn: Callable[[str], bool] | None = None,
    ) -> None:
        for name in names:
            if skip_fn and skip_fn(name):
                continue
            if whitelist is not None and name not in whitelist:
                raise RuntimeError(
                    f"CRITICAL: Attempted to delete non-whitelisted {err_label} '{name}'. Whitelisted: {whitelist}"
                )
            if status_cb:
                status_cb(f"🧹 Removing outdated {label} '{name}'...")
            try:
                deleter(service_id, draft_version, name, token)
            except Exception:
                pass

    _purge_outdated(
        diff.snippets_to_remove,
        "VCL snippet",
        fastly_integration.delete_snippet,
        skip_fn=lambda n: not _is_managed_snippet(n),
    )
    _purge_outdated(diff.endpoints_to_remove, "logging endpoint", fastly_integration.delete_logging_endpoint)
    _purge_outdated(
        diff.backends_to_remove,
        "backend",
        fastly_integration.delete_backend,
        whitelist=MANAGED_BACKEND_NAMES,
        err_label="backend",
    )
    _purge_outdated(
        diff.dictionaries_to_remove,
        "edge dictionary",
        fastly_integration.delete_dictionary,
        whitelist=MANAGED_DICTIONARY_NAMES,
        err_label="dictionary",
    )

    from backend.core.fastly.client import ensure_condition

    def _ensure_cond_safe(name: str, statement: str, cond_type: str = "RESPONSE") -> None:
        try:
            ensure_condition(
                name=name,
                statement=statement,
                type=cond_type,
                service_id=service_id,
                version=draft_version,
                token=token,
            )
        except Exception as e:
            logging.warning(f"WARNING: Failed to ensure {name}: {e}")

    has_main_cond = any(ep.response_condition == "log_analytics_condition" for ep in desired_endpoints)
    if has_main_cond:
        if status_cb:
            status_cb("➕ Configuring log analytics condition 'log_analytics_condition'...")
        cond_parts = ["!segmented_caching.is_inner_req"]
        if desired_state and desired_state.rum_enabled:
            cond_parts.append('req.url.path != "/rum-beacon"')
        if desired_state:
            scoring_enabled = desired_state.scoring.enabled
            if desired_state.edge_only:
                from backend.provision.fastly_api import _log_sampling_edge_clause

                cond_parts.append(_log_sampling_edge_clause(scoring_enabled))
            if desired_state.sample_rate < 100:
                cond_parts.append(f"randombool({desired_state.sample_rate}, 100)")
            if desired_state.custom_condition and desired_state.custom_condition.strip():
                cond_parts.append(f"({desired_state.custom_condition.strip()})")

        statement = " && ".join(cond_parts)
        _ensure_cond_safe("log_analytics_condition", statement)

    has_rum_cond = any(ep.response_condition == "rum_log_condition" for ep in desired_endpoints)
    if has_rum_cond:
        if status_cb:
            status_cb("➕ Configuring RUM routing condition 'rum_log_condition'...")
        statement = 'req.url.path == "/rum-beacon" && req.method == "POST"'
        if (
            desired_state
            and getattr(desired_state, "rum_custom_condition", None)
            and desired_state.rum_custom_condition.strip()
        ):
            statement = f"{statement} && ({desired_state.rum_custom_condition.strip()})"
        _ensure_cond_safe("rum_log_condition", statement)

    desired_backends_list = desired_backends(desired_state) if desired_state else []
    has_false_cond = any(ep.response_condition == "fastly_log_analytics_false" for ep in desired_endpoints) or any(
        b.request_condition == "fastly_log_analytics_false" for b in desired_backends_list
    )
    if has_false_cond:
        if status_cb:
            status_cb("➕ Configuring request condition 'fastly_log_analytics_false'...")
        _ensure_cond_safe("fastly_log_analytics_false", "false", cond_type="REQUEST")

    for backend in diff.backends_to_add + diff.backends_to_update:
        if status_cb:
            status_cb(f"➕ Installing backend '{backend.name}'...")
        try:
            fastly_integration.create_or_update_backend(service_id, draft_version, backend, token)
        except Exception as e:
            logging.warning(f"WARNING: Backend creation failed for {backend.name}: {e}")
            raise

    for dictionary in diff.dictionaries_to_add + diff.dictionaries_to_update:
        if status_cb:
            status_cb(f"➕ Installing edge dictionary '{dictionary.name}'...")
        try:
            fastly_integration.create_or_update_dictionary(service_id, draft_version, dictionary, token)
        except Exception as e:
            logger.warning("Failed to install/update edge dictionary '%s': %s", dictionary.name, e)

    for snippet in diff.snippets_to_add + diff.snippets_to_update:
        if status_cb:
            status_cb(f"➕ Installing VCL snippet '{snippet.name}'...")
        try:
            fastly_integration.create_or_update_snippet(service_id, draft_version, snippet, token)
        except Exception:
            pass

    for endpoint in diff.endpoints_to_add + diff.endpoints_to_update:
        if status_cb:
            status_cb(f"➕ Installing logging endpoint '{endpoint.name}'...")
        try:
            fastly_integration.create_or_update_logging_endpoint(
                service_id,
                draft_version,
                endpoint,
                token,
                bucket_name=bucket_name,
                domain=domain,
                access_key=access_key,
                secret_key=secret_key,
            )
        except Exception as e:
            logger.exception(f"Failed to install/update logging endpoint '{endpoint.name}': {e}")
            if status_cb:
                status_cb(f"❌ Failed to install logging endpoint '{endpoint.name}': {e}")


def _is_managed_snippet(name: str) -> bool:
    if name.startswith("Fastly Log Analytics - "):
        return True
    return _is_legacy_snippet(name)


def _is_legacy_snippet(name: str) -> bool:
    if name.startswith("Fastly Log Analytics - "):
        return False
    legacy_prefixes = [
        "Fastly Log Analysis",
        "Fastly Log Analytics",
        "RUM -",
        "Session Scoring",
        "CMCD",
    ]
    return any(name.startswith(prefix) for prefix in legacy_prefixes)


def _detect_and_queue_legacy_cleanup(
    current_snippets: list[VCLSnippet],
    diff: DiffResult,
    status_cb: Callable[[str], None] | None = None,
) -> None:
    legacy = [s for s in current_snippets if _is_legacy_snippet(s.name)]

    if legacy:
        if status_cb:
            status_cb(
                f"⏳ Detected {len(legacy)} legacy VCL snippet(s) from prior deployment — will remove during reconciliation…"
            )
        for snippet in legacy:
            if snippet.name not in diff.snippets_to_remove:
                diff.snippets_to_remove.append(snippet.name)


def _validate_draft(service_id: str, token: str, draft_version: int) -> str:
    try:
        if not fastly_integration.validate_version(service_id, draft_version, token):
            return f"VCL validation failed: version {draft_version} did not validate (status not ok)"
        return ""
    except RuntimeError as e:
        return str(e)


def _activate_draft(service_id: str, token: str, draft_version: int) -> None:
    fastly_integration.activate_version(service_id, draft_version, token)


RUM_SURROGATE_KEY = "rum-js"


def _purge_rum_surrogate_key(service_id: str, token: str) -> None:
    """Purge the ``rum-js`` surrogate key so the edge drops every cached RUM script."""
    from backend.core.fastly.client import fastly as fastly_client

    sched_logger = logging.getLogger("backend.scheduler")
    try:
        sched_logger.info(f"Purging RUM surrogate key {RUM_SURROGATE_KEY} on service {service_id}")
        fastly_client(
            "POST",
            f"/service/{service_id}/purge/{RUM_SURROGATE_KEY}",
            token=token,
            expect_empty=True,
        )
    except Exception as e:
        sched_logger.warning(f"Failed to purge surrogate key {RUM_SURROGATE_KEY} for service {service_id}: {e}")


# ============================================================================
# Unified Multi-Service Declarative IaC Engine (folded from iac_reconciler.py)
# ============================================================================

# Register module alias BEFORE importing session_scoring_orchestrator
sys.modules["backend.provision.declarative.iac_reconciler"] = sys.modules[__name__]

from backend.provision.session_scoring_orchestrator import (  # noqa: E402
    _SCORER_PACKAGE,
    _SCORING_FIELD_NAMES,
    _deploy_wasm_package,
    _remove_scoring_custom_fields,
    _write_matrix_to_kv,
    scorer_vcl_fingerprint,
)
from backend.provision.session_scoring_setup import (  # noqa: E402
    MATRIX_STORE_NAME_TEMPLATE,
    _find_kv_store,
    delete_scoring_service,
    ensure_scoring_service,
)


class DeclarativeKVStore(BaseModel):
    name: str
    keys: dict[str, str] = Field(default_factory=dict)
    link_to_services: list[str] = Field(default_factory=list)


class DeclarativeComputeService(BaseModel):
    name: str
    package_path: str
    domains: list[str] = Field(default_factory=list)
    linked_kv_stores: list[str] = Field(default_factory=list)


class DeclarativeVCLService(BaseModel):
    name: str
    custom_fields: list[dict[str, Any]] = Field(default_factory=list)
    logging_endpoints: list[dict[str, Any]] = Field(default_factory=list)
    backends: list[dict[str, Any]] = Field(default_factory=list)
    snippets: list[dict[str, Any]] = Field(default_factory=list)


class UnifiedDesiredState(BaseModel):
    service_id: str
    vcl_service: DeclarativeVCLService
    compute_scorer: DeclarativeComputeService | None = None
    kv_stores: dict[str, DeclarativeKVStore] = Field(default_factory=dict)


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
        store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
        compute_scorer = DeclarativeComputeService(
            name=f"Session Scoring Service for {logging_service_id}",
            package_path=str(_SCORER_PACKAGE),
            domains=[state.scoring.domain],
            linked_kv_stores=[store_name],
        )

        keys: dict[str, str] = {}
        from backend.provision.session_scoring_orchestrator import _resolve_tenant_matrix_for_deploy
        from backend.scoring.matrix import serialize_kv

        matrix_path = _resolve_tenant_matrix_for_deploy(logging_service_id)
        if matrix_path and matrix_path.exists():
            with matrix_path.open() as f:
                serialized = serialize_kv(json.load(f))
                keys["matrix"] = serialized.hex()

        kv_stores[store_name] = DeclarativeKVStore(
            name=store_name,
            keys=keys,
            link_to_services=[compute_scorer.name],
        )

    return UnifiedDesiredState(
        service_id=logging_service_id,
        vcl_service=vcl_service,
        compute_scorer=compute_scorer,
        kv_stores=kv_stores,
    )


def discover_current_state(logging_service_id: str, token: str) -> dict[str, Any]:
    """Retrieve the active live infrastructure matching names in our scope."""
    kv_stores: dict[str, Any] = {}
    store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
    store = _find_kv_store(store_name, token)
    if store:
        kv_stores[store_name] = store

    compute_name = f"Session Scoring Service for {logging_service_id}"
    compute_service = find_service_by_name(compute_name, token)

    return {
        "kv_stores": kv_stores,
        "compute_scorer": compute_service,
    }


def verify_compute_scorer_readiness(
    compute_def: DeclarativeComputeService,
    token: str,
    status_cb: Callable[[str], None] | None = None,
) -> None:
    """Probes the Compute Scorer domain to guarantee DNS and Edge Routing are active."""
    import httpx
    from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

    domain = compute_def.domains[0]
    url = f"https://{domain}/health"
    max_retries = 10

    for attempt in Retrying(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1.0, exp_base=1.5, min=1.0, max=5.0),
        retry=retry_if_exception_type(Exception),
        sleep=time.sleep,
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
    logging_service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
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
    orig_config_content = snapshot_config(logging_service_id)
    if orig_config_content is None:
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
        scoring_meta: dict[str, Any] = {}
        apply_phase3: Callable[[dict[str, Any]], None] | None = None
        if desired.compute_scorer:
            if status_cb:
                status_cb("⏳ Phase 1: Reconciling edge KV Stores...")
                status_cb("⏳ Phase 2: Reconciling Compute Scorer Service and uploading WASM package...")

            scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
            scoring_service_id = scoring_meta["scoring_service_id"]
            matrix_store_id = scoring_meta.get("scoring_matrix_store_id", "")

            _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)

            package_meta = _deploy_wasm_package(
                scoring_service_id,
                matrix_store_id,
                token,
                status_cb=status_cb,
            )

            resolved_domain = scoring_meta.get("scoring_domain") or (
                desired.compute_scorer.domains[0] if desired.compute_scorer.domains else ""
            )
            if resolved_domain:
                desired.compute_scorer.domains = [resolved_domain]

            new_scoring: dict[str, Any] = {
                "enabled": True,
                "scoring_service_id": scoring_service_id,
                "scoring_service_name": scoring_meta.get("scoring_service_name", ""),
                "scoring_domain": scoring_meta.get("scoring_domain", ""),
                "scoring_keys_store_id": scoring_meta.get("scoring_keys_store_id", ""),
                "scoring_config_store_id": scoring_meta.get("scoring_config_store_id", ""),
                "scoring_matrix_store_id": matrix_store_id,
                "request_secret": scoring_meta.get("request_secret", cfg.get("scoring", {}).get("request_secret", "")),
                "aes_key_hex": scoring_meta.get("aes_key_hex", "") or cfg.get("scoring", {}).get("aes_key_hex", ""),
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

            old_scoring_service_id = current["compute_scorer"].get("id")
            old_matrix_store_id = (
                current["kv_stores"].get(MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id), {}).get("id", "")
            )

            prior_scoring = json.loads(orig_config_content).get("scoring") or {}

            delete_scoring_service(
                scoring_service_id=old_scoring_service_id,
                scoring_keys_store_id=prior_scoring.get("scoring_keys_store_id", ""),
                scoring_config_store_id=prior_scoring.get("scoring_config_store_id", ""),
                scoring_matrix_store_id=old_matrix_store_id,
                token=token,
                status_cb=status_cb,
            )

            def _drop_scoring(disk_cfg: dict[str, Any]) -> None:
                disk_cfg.pop("scoring", None)

            _persist(_drop_scoring)

        if status_cb:
            status_cb("✅ Infrastructure state reconciliation completed successfully.")

    except Exception as err:
        logger.error("IaC reconciliation failure, executing transactional configuration rollback", exc_info=True)
        if status_cb:
            status_cb(f"❌ Error during reconciliation: {err}. Rolling back to previous configuration...")
        if persisted:
            mutate_config(logging_service_id, _rollback_owned_keys)
        raise err
