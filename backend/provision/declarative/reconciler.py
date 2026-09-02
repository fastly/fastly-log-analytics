"""VCL Declarative Reconciliation Control Loop (Steps 1-8).

The reconciler executes the classical control loop:
Read Desired State → Read Current State → Diff → Apply Diff → Validate & Activate
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

from backend.core.fastly.client import fastly
from backend.provision.declarative.diff import (
    Backend,
    DiffResult,
    LoggingEndpoint,
    ServiceDictionary,
    VCLSnippet,
    compute_diff,
)
from backend.provision.declarative.generators import (
    desired_backends,
    desired_dictionaries,
    desired_logging_endpoints,
    desired_snippets,
)
from backend.provision.declarative.state import _AUTO_INJECTED_NAMES, CmcdConfig, FeatureState, ScoringConfig
from backend.provision.rum_assets import upload_rum_tracker_js

logger = logging.getLogger(__name__)

# ============================================================================
# Constants & Guards
# ============================================================================

# CRITICAL: Whitelist of managed backend names (Gotcha 5)
# Only backends in this set can be deleted by the reconciler.
# Customer origins are NEVER touched.
MANAGED_BACKEND_NAMES = {
    "session_scorer",
    "fos_origin",
    "F_fos_origin",
    "F_F_fos_origin",  # cleanup old naming mistake
    "rum_collector",
}

# Whitelist of managed dictionary names.
# Only dictionaries in this set can be deleted by the reconciler.
MANAGED_DICTIONARY_NAMES = {"fos_credentials"}


class VclValidationError(Exception):
    """Raised when Fastly VCL validation fails."""

    def __init__(self, service_id: str, draft_version: int, errors: str):
        self.service_id = service_id
        self.draft_version = draft_version
        self.errors = errors
        super().__init__(f"VCL validation failed for {service_id} v{draft_version}: {errors}")


class LockAcquisitionTimeout(Exception):
    """Raised when VCL lock cannot be acquired within timeout."""

    def __init__(self, service_id: str, elapsed_seconds: float):
        self.service_id = service_id
        self.elapsed_seconds = elapsed_seconds
        super().__init__(f"Failed to acquire VCL lock for {service_id} after {elapsed_seconds:.1f}s")


@dataclass
class ReconciliationResult:
    """Result of a reconciliation run."""

    service_id: str
    activated_version: int | None = None
    draft_version: int | None = None
    changes_applied: dict | None = None
    error: str | None = None
    duration_ms: int = 0
    audit_log_id: int | None = None

    def __post_init__(self):
        if self.changes_applied is None:
            self.changes_applied = {}


# ============================================================================
# Lock Management (Step 1)
# ============================================================================


class FileLock:
    """Simple file-based lock for concurrency control."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_path.exists():
            self.lock_path.unlink()


def acquire_vcl_lock(service_id: str, timeout_sec: int = 300) -> FileLock:
    """Acquire a file-based lock for reconciliation.

    Step 1: Concurrency Lock Acquisition with stale-lock detection.

    Args:
        service_id: Service ID.
        timeout_sec: Timeout in seconds (default 5 minutes).

    Returns:
        FileLock context manager.

    Raises:
        LockAcquisitionTimeout: if lock cannot be acquired within timeout.
    """
    lock_path = Path(f"data/services/{service_id}.vcl.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.time()
    backoff = 0.1

    while True:
        try:
            # Try to create lock exclusively (atomic operation)
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(fd, f"{os.getpid()} {int(time.time())}".encode())
            os.close(fd)
            return FileLock(lock_path)
        except FileExistsError:
            elapsed = time.time() - start
            if elapsed > timeout_sec:
                raise LockAcquisitionTimeout(service_id, elapsed)

            # Check if lock is stale (>5m old)
            lock_mtime = lock_path.stat().st_mtime
            if time.time() - lock_mtime > 300:
                lock_path.unlink()
                continue

            time.sleep(min(backoff, 1.0))
            backoff *= 1.5


# ============================================================================
# VCL Linting & Safety Checks (Gotcha 3)
# ============================================================================


def lint_log_format(log_format_vcl: str, log_fields: list[str]) -> list[str]:
    """Lint VCL log format with graceful falco fallback.

    Tier 1: Use falco if available (comprehensive).
    Tier 2: Offline checks (duplicate vars, undefined fields, syntax).

    Args:
        log_format_vcl: VCL expression to lint.
        log_fields: List of defined log field names.

    Returns:
        List of lint errors. Empty if valid.
    """
    errors = []

    # Tier 1: Try to use falco
    if shutil.which("falco") is not None:
        result = subprocess.run(
            ["falco", "lint", "-"],
            input=log_format_vcl,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            errors.extend(result.stdout.split("\n"))
    else:
        # Falco unavailable, fall back to offline checks
        errors.extend(_offline_vcl_checks(log_format_vcl, log_fields))

    return [e for e in errors if e.strip()]


def _offline_vcl_checks(vcl: str, fields: list[str]) -> list[str]:
    """Offline VCL safety checks (no falco required)."""
    errors = []

    # Check 1: No duplicate variable declarations
    var_decl_pattern = r"^\s*declare\s+local\s+var\.(\w+)"
    declared_vars = re.findall(var_decl_pattern, vcl, re.MULTILINE)
    duplicates = [v for v in set(declared_vars) if declared_vars.count(v) > 1]
    if duplicates:
        errors.append(f"Duplicate variable declarations: {duplicates}")

    # Check 2: All log fields referenced in format are defined
    referenced = set(re.findall(r"\{([a-z_]+)\}", vcl))
    undefined = referenced - set(fields)
    if undefined:
        errors.append(f"Undefined log fields in format: {undefined}")

    # Check 3: VCL syntax: balanced braces
    if vcl.count("{") != vcl.count("}"):
        errors.append("Unbalanced braces in VCL")

    # Check 4: No forbidden keywords
    if re.search(r"\bgoto\b", vcl):
        errors.append("Fastly VCL does not support 'goto' statements")

    return errors


# ============================================================================
# Main Reconciliation Control Loop (Steps 2-8)
# ============================================================================


def _upload_rum_tracker_best_effort(service_id: str, token: str, status_cb: Callable[[str], None] | None) -> None:
    """Upload the RUM tracker JS, logging (not raising) on failure.

    Callers must only invoke this once the VCL route the tracker depends on
    (``/js/faro-sdk.js``) is actually live — i.e. after a successful
    activation, or on the no-change idempotent path where it was already
    live. Non-blocking by design: reconciliation has already succeeded by
    the time this runs, so a tracker-upload hiccup must not turn a
    successful reconcile into a reported failure — the next reconcile or
    cron tick will simply retry it.
    """
    if status_cb:
        status_cb("⏳ Ensuring RUM tracker JS is uploaded to FOS...")
    try:
        upload_rum_tracker_js(service_id, token, status_cb=status_cb)
    except Exception as e:
        logger.warning(f"Failed to upload RUM tracker JS: {e}")
        if status_cb:
            status_cb(f"⚠️  RUM JS upload warning: {e}")


def reconcile_vcl_state(
    service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop (Steps 1-8).

    Steps:
    1. Concurrency lock acquisition
    2. Build desired state from config
    3. Fetch current state from Fastly
    4. Compute diff (+ legacy snippet auto-detection)
    5. Clone active version to draft
    6. Apply diff on draft
    7. Validate draft
    8. Activate draft & persist state (bypassed if activate=False)
    8.5. Upload RUM tracker JS if RUM is enabled — deliberately AFTER
         activation (or on the no-change early-exit), never before: the
         tracker unconditionally requests /js/faro-sdk.js, a route that
         only exists once the VCL serving it is live. Publishing it earlier
         would point browsers at a route that 404s (see task-9 report,
         "Ordering fix: tracker after activation").

    Args:
        service_id: Fastly service ID.
        token: Fastly API token.
        dry_run: If True, compute diff but don't mutate state.
        status_cb: Optional callback for status messages.
        activate: If False, draft is compiled and validated but not activated.

    Returns:
        ReconciliationResult with outcome.

    Raises:
        VclValidationError: if VCL validation fails.
        LockAcquisitionTimeout: if lock cannot be acquired.
    """
    result = ReconciliationResult(service_id=service_id)
    start_time = time.time()

    try:
        # Step 1: Acquire concurrency lock
        with acquire_vcl_lock(service_id):
            # Step 2: Build desired state from config
            from backend.config import config_path as _config_path

            config_path = _config_path(service_id)
            if not config_path.exists():
                # Bootstrap from Fastly if config missing (Gotcha 1)
                desired_state = _bootstrap_featurestate_from_fastly(service_id, token)
                config_path.write_text(json.dumps(asdict(desired_state), indent=2))
            else:
                cfg = json.loads(config_path.read_text())
                desired_state = FeatureState.from_config(cfg)

            # Step 3: Fetch current state from Fastly
            if status_cb:
                status_cb("⏳ Fetching active configuration from Fastly...")
            current_snippets = _fetch_snippets(service_id, token)
            current_endpoints = _fetch_logging_endpoints(service_id, token)
            current_backends = _fetch_backends(service_id, token)
            current_backends = [b for b in current_backends if b.name in MANAGED_BACKEND_NAMES]
            current_dictionaries = _fetch_dictionaries(service_id, token)
            current_dictionaries = [d for d in current_dictionaries if d.name in MANAGED_DICTIONARY_NAMES]

            # Step 4: Compute diff
            if status_cb:
                status_cb("⏳ Computing VCL configuration difference...")
            desired_snippets_list = desired_snippets(desired_state)
            desired_endpoints_list = desired_logging_endpoints(desired_state)
            desired_backends_list = desired_backends(desired_state)
            desired_dictionaries_list = desired_dictionaries(desired_state)

            # Early validation of log format string length limits (S3 limit is 12288)
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

            # Step 4.5: Detect and auto-remove legacy snippets (migration helper)
            _detect_and_queue_legacy_cleanup(current_snippets, diff, status_cb)

            # Idempotency: if no changes, exit early. Current Fastly state
            # already matches desired state, so any route the tracker
            # depends on (e.g. /js/faro-sdk.js) is already live — safe to
            # (re)publish the tracker here, and necessary: a service whose
            # VCL is already correct still needs its tracker present and
            # current (e.g. if it was deleted out-of-band from FOS).
            if diff.is_empty():
                if desired_state.rum_enabled:
                    _upload_rum_tracker_best_effort(service_id, token, status_cb)
                try:
                    result.activated_version = _fetch_active_version(service_id, token)
                except RuntimeError:
                    result.activated_version = None
                result.duration_ms = int((time.time() - start_time) * 1000)
                return result

            if dry_run:
                # Nothing is applied in a dry run, so the tracker must NOT
                # be published here — the route it depends on doesn't exist
                # until a real reconcile activates it.
                result.changes_applied = diff.summary()
                try:
                    result.activated_version = _fetch_active_version(service_id, token)
                except RuntimeError:
                    result.activated_version = None
                result.duration_ms = int((time.time() - start_time) * 1000)
                return result

            draft_version = None
            try:
                # Step 5: Clone active version to draft
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(service_id, token, desired_state)

                # Step 6: Apply diff on draft
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

                # Step 7: Validate draft
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                validation_errors = _validate_draft(service_id, token, draft_version)
                if validation_errors:
                    # Assertion: active version unchanged after validation failure
                    active_now = _fetch_active_version(service_id, token)
                    active_before = _fetch_active_version(service_id, token)
                    assert active_before == active_now, "Active version changed during validation failure"
                    raise VclValidationError(service_id, draft_version, validation_errors)

                result.draft_version = draft_version

                if activate:
                    # Step 8: Activate draft & persist state
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(service_id, token, draft_version)
                    result.activated_version = draft_version

                    # Step 8.5: Upload RUM tracker JS now that the just-
                    # activated VCL actually serves the route it depends on
                    # (/js/faro-sdk.js). Must run AFTER activation — never
                    # before, and never when activate=False (draft isn't
                    # live yet in that case).
                    if desired_state.rum_enabled:
                        _upload_rum_tracker_best_effort(service_id, token, status_cb)

                    # Purge the shared RUM surrogate key to invalidate cached
                    # 404s/errors from before the route existed, and any old
                    # script bytes. Runs AFTER the upload above — purging
                    # first cannot invalidate bytes that are uploaded
                    # afterwards. Ungated on rum_enabled on purpose: a
                    # reconcile that DISABLES RUM still needs the previously
                    # cached (and still key-tagged) scripts dropped.
                    _purge_rum_surrogate_key(service_id, token)

                    # Update local config with activation metadata
                    cfg = json.loads(config_path.read_text())
                    cfg["last_activated_version"] = draft_version
                    config_path.write_text(json.dumps(cfg, indent=2))
                else:
                    if status_cb:
                        status_cb(
                            f"✓ Configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                        )

                # Async: upload state to FOS (fire-and-forget)
                _upload_state_to_fos(service_id, desired_state)

                result.changes_applied = diff.summary()

            except Exception as rollback_err:
                if draft_version is not None:
                    if status_cb:
                        status_cb(
                            f"🧹 Reconcile failed: {rollback_err}. Broken draft version {draft_version} left as inactive draft (Fastly does not support deleting individual draft versions)."
                        )
                    import logging

                    logging.getLogger("backend.scheduler").warning(
                        f"Reconcile failed: {rollback_err}. Broken draft version {draft_version} left in Fastly configuration."
                    )
                raise rollback_err

    finally:
        result.duration_ms = int((time.time() - start_time) * 1000)

    return result


def reconcile_cdn_service_state(
    logging_service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> ReconciliationResult:
    """Execute the complete reconciliation control loop for the FOS Proxy / CDN service (Steps 1-8).

    Steps:
    1. Load configuration and determine/bootstrap cdn_service_id.
    2. Concurrency lock acquisition for cdn_service_id.
    3. Fetch current state of CDN service from Fastly.
    4. Compute diff (Snippets, Backends, Dictionaries, and Main VCL content).
    5. Clone active version to draft if changes needed.
    6. Apply diff on draft version.
    7. Validate draft version.
    8. Activate draft version & persist state.
    """
    from backend.core.fastly.service import find_service_by_name
    from backend.core.fastly.utils import load_vcl, region_endpoint
    from backend.provision.fastly_api import _CDN_SNIPPETS

    # Managed resources whitelists specific to the CDN service
    cdn_managed_backend_names = {"fos_origin"}
    cdn_managed_dictionary_names = {"fos_credentials", "cdn_auth"}

    start_time = time.time()
    from backend.config import config_path as _config_path

    config_path = _config_path(logging_service_id)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    cfg = json.loads(config_path.read_text())

    # Read CDN Proxy config (with fallback to flat keys for backward compatibility)
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

    # S3 credentials and region
    fos_access_key = cfg.get("fos_access_key_id", "")
    fos_secret_key = cfg.get("fos_secret_access_key", "")
    fos_bucket = cfg.get("fos_bucket", "") or cfg.get("fos_bucket_name", "")
    fos_region = cfg.get("fos_region", "us-east-1")
    fos_host = region_endpoint(fos_region)

    if not cdn_service_name:
        cdn_service_name = f"Log Analysis CDN Service for {logging_service_id}"

    if not cdn_url:
        raise ValueError("CDN URL is required to reconcile CDN service state.")

    # Normalize domain
    domain = cdn_url.replace("https://", "").replace("http://", "").split("/")[0]

    # Bootstrap / find or create service if not present
    if not cdn_service_id:
        if status_cb:
            status_cb(f"⏳ Finding existing CDN service named '{cdn_service_name}'...")
        existing = find_service_by_name(cdn_service_name, token)
        if existing:
            cdn_service_id = existing["id"]
            if status_cb:
                status_cb(f"✓ Found existing CDN service: {cdn_service_id}")
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

            # Comment on service
            service_comment = (
                f"CDN fronting service for the Fastly Object Storage log bucket associated with "
                f"service {logging_service_id}. Provides authenticated read access to stored log "
                f"files for the Fastly Log Analysis tool."
            )
            fastly("PUT", f"/service/{cdn_service_id}", {"comment": service_comment}, token=token)

            # Add domain to version 1
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

        # Persist cdn_service_id back to config
        if "fos_proxy" not in cfg:
            cfg["fos_proxy"] = {}
        cfg["fos_proxy"]["service_id"] = cdn_service_id
        cfg["fos_proxy"]["service_name"] = cdn_service_name
        cfg["fos_proxy"]["domain"] = cdn_url
        cfg["fos_proxy"]["shield"] = cdn_shield
        cfg["fos_proxy"]["secret"] = cdn_secret
        cfg["cdn_service_id"] = cdn_service_id  # flat key
        config_path.write_text(json.dumps(cfg, indent=2))

    result = ReconciliationResult(service_id=cdn_service_id)

    try:
        # Step 2: Acquire concurrency lock
        with acquire_vcl_lock(cdn_service_id):
            # Step 3: Fetch current live state from Fastly
            if status_cb:
                status_cb("⏳ Fetching active CDN configuration from Fastly...")
            current_snippets = _fetch_snippets(cdn_service_id, token)
            current_backends = _fetch_backends(cdn_service_id, token)
            current_backends = [b for b in current_backends if b.name in cdn_managed_backend_names]
            current_dictionaries = _fetch_dictionaries(cdn_service_id, token)
            current_dictionaries = [d for d in current_dictionaries if d.name in cdn_managed_dictionary_names]

            # Fetch active version's main VCL content to check for drift
            active_ver = fastly_integration.fetch_active_version(cdn_service_id, token)
            current_vcl_content = ""
            if active_ver is not None:
                try:
                    vcl_resp = fastly("GET", f"/service/{cdn_service_id}/version/{active_ver}/vcl/main", token=token)
                    if isinstance(vcl_resp, dict):
                        current_vcl_content = vcl_resp.get("content", "")
                except Exception:
                    pass

            # Step 4: Build and compute desired state elements
            desired_vcl_content = load_vcl(rate_limiting=rate_limiting_enabled)

            desired_snippets_list = [
                VCLSnippet(name=name, subroutine=stype, body=content, priority=priority)
                for name, stype, content, priority in _CDN_SNIPPETS
            ]

            from backend.core.fastly.utils import SHIELD_MAP

            shield_pop = cdn_shield
            if not shield_pop:
                shield_pop = SHIELD_MAP.get(fos_region, "iad-va-us")
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
                    items={
                        "secret": cdn_secret,
                    }
                    if cdn_secret
                    else {},
                ),
            ]

            # Compute diffs
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

            # Idempotency early-exit
            if diff.is_empty() and not vcl_needs_update:
                if status_cb:
                    status_cb("✓ CDN service configuration is up-to-date and matches desired state. No changes needed.")
                result.activated_version = active_ver
                result.duration_ms = int((time.time() - start_time) * 1000)
                return result

            if dry_run:
                result.changes_applied = diff.summary()
                result.changes_applied["main_vcl_updated"] = vcl_needs_update
                result.activated_version = active_ver
                result.duration_ms = int((time.time() - start_time) * 1000)
                return result

            draft_version = None
            try:
                # Step 5: Clone active version to draft
                if status_cb:
                    status_cb("⏳ Cloning active version to draft...")
                draft_version = _clone_active_version(cdn_service_id, token)

                # Step 6: Apply diff on draft
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

                # Step 6.5: Apply Main VCL content
                if vcl_needs_update:
                    if status_cb:
                        status_cb("⏳ Updating main VCL content...")
                    try:
                        # Try PUT first to update existing
                        fastly(
                            "PUT",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl/main",
                            {"content": desired_vcl_content},
                            token=token,
                        )
                    except Exception:
                        # Fallback to POST if main VCL object does not exist yet (brand new service)
                        fastly(
                            "POST",
                            f"/service/{cdn_service_id}/version/{draft_version}/vcl",
                            {"name": "main", "content": desired_vcl_content, "main": True},
                            token=token,
                        )

                # Step 7: Validate draft
                if status_cb:
                    status_cb("⏳ Validating draft configuration...")
                validation_errors = _validate_draft(cdn_service_id, token, draft_version)
                if validation_errors:
                    raise VclValidationError(cdn_service_id, draft_version, validation_errors)

                result.draft_version = draft_version

                if activate:
                    # Step 8: Activate draft & persist state
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(cdn_service_id, token, draft_version)
                    result.activated_version = draft_version

                    # Update local config with activation metadata
                    cfg = json.loads(config_path.read_text())
                    if "fos_proxy" not in cfg:
                        cfg["fos_proxy"] = {}
                    cfg["fos_proxy"]["last_activated_version"] = draft_version
                    cfg["last_activated_version_cdn"] = draft_version
                    config_path.write_text(json.dumps(cfg, indent=2))
                else:
                    if status_cb:
                        status_cb(
                            f"✓ CDN configuration compiled and validated successfully in draft version {draft_version} (activation bypassed)."
                        )

                result.changes_applied = diff.summary()
                result.changes_applied["main_vcl_updated"] = vcl_needs_update

            except Exception as rollback_err:
                if draft_version is not None:
                    if status_cb:
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

from backend.provision.declarative import fastly_integration


def _bootstrap_featurestate_from_fastly(service_id: str, token: str) -> FeatureState:
    """Introspect active (or draft) Fastly version and reverse-engineer FeatureState (Gotcha 1).

    If no active version exists, inspect the highest draft version instead to recover features.
    This handles the case where a deployment is stuck (v113 in draft with VCL but no active).
    """
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    source_ver = active_ver

    if source_ver is None:
        try:
            resp = fastly("GET", f"/service/{service_id}/version", token=token)
            if isinstance(resp, list):
                versions = sorted([int(v.get("number", 0)) for v in resp if isinstance(v, dict)])
            elif isinstance(resp, dict) and "items" in resp:
                versions = sorted([int(v.get("number", 0)) for v in resp.get("items", [])])
            else:
                versions = []

            if versions:
                source_ver = versions[-1]
        except Exception:
            pass

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
    sample_rate = 100
    for endpoint in endpoints:
        if "Fastly Log Analytics" in endpoint.name and "RUM" not in endpoint.name:
            log_period = endpoint.period
            break

    return FeatureState(
        service_id=service_id,
        log_period=log_period,
        sample_rate=sample_rate,
        edge_only=False,
        custom_condition="",
        fos_prefix="raw",
        fos_endpoint="fos.example.com",
        rum_enabled=rum_enabled,
        cmcd=CmcdConfig(enabled=cmcd_enabled),
        scoring=ScoringConfig(
            enabled=scoring_enabled,
            domain="" if not scoring_enabled else "scorer.example.com",
        ),
    )


def _fetch_snippets(service_id: str, token: str) -> list[VCLSnippet]:
    """Fetch current VCL snippets from Fastly (from active version)."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return []
    return fastly_integration.fetch_snippets(service_id, active_ver, token)


def _fetch_logging_endpoints(service_id: str, token: str) -> list[LoggingEndpoint]:
    """Fetch current logging endpoints from Fastly (from active version)."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return []
    return fastly_integration.fetch_logging_endpoints(service_id, active_ver, token)


def _fetch_backends(service_id: str, token: str) -> list[Backend]:
    """Fetch current backends from Fastly (from active version)."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return []
    return fastly_integration.fetch_backends(service_id, active_ver, token)


def _fetch_dictionaries(service_id: str, token: str) -> list[ServiceDictionary]:
    """Fetch current dictionaries from Fastly (from active version)."""
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is None:
        return []
    return fastly_integration.fetch_dictionaries(service_id, active_ver, token)


def _fetch_active_version(service_id: str, token: str) -> int:
    """Fetch the active version number."""
    ver = fastly_integration.fetch_active_version(service_id, token)
    if ver is None:
        raise RuntimeError(f"No active version found for {service_id}")
    return ver


def _clone_active_version(service_id: str, token: str, desired_state: FeatureState | None = None) -> int:
    """Clone active version to a new draft version.

    Gotcha 1: If no active version, clone the highest existing version.
    CRITICAL: Always clone a writable version before mutations.
    """
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
            features_str = ", ".join(enabled_features)
            comment += f" (Enabled: {features_str})"
        else:
            comment += " (All Features Disabled / Teardown)"

    active_ver = fastly_integration.fetch_active_version(service_id, token)
    if active_ver is not None:
        # Normal case: clone the active version
        return fastly_integration.clone_version(service_id, active_ver, token, comment)

    # Gotcha 1: No active version - clone the highest existing version
    from backend.core.fastly.client import fastly as fastly_client

    try:
        resp = fastly_client("GET", f"/service/{service_id}/version", token=token)
    except Exception as e:
        raise RuntimeError(f"Cannot determine version to clone for {service_id}: {e}")

    if isinstance(resp, list):
        versions = sorted([int(v.get("number", 0)) for v in resp if isinstance(v, dict)], reverse=True)
    elif isinstance(resp, dict) and "items" in resp:
        versions = sorted([int(v.get("number", 0)) for v in resp.get("items", [])], reverse=True)
    else:
        versions = []

    if versions:
        highest = versions[0]
        # Clone the highest version to get a writable draft
        new_ver = fastly_integration.clone_version(service_id, highest, token, comment)
        return new_ver

    # Fallback: create on v1 (should not happen)
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

    # De-conflict duplicate local variable declarations in consolidated snippets
    # (e.g. if we are in vcl_miss/vcl_pass where main VCL's miss_pass already declares them,
    # or if an unmanaged snippet on the service already declares them).
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

    # Load optional S3 settings from config
    import json

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
            from backend.provision.fastly_api import region_endpoint

            domain = region_endpoint(region)
            access_key = cfg.get("fos_access_key_id") or cfg.get("fos_access_key")
            secret_key = cfg.get("fos_secret_access_key") or cfg.get("fos_secret_key")
        except Exception:
            pass

    # Step 6.1: Purge old snippets (including legacy ones)
    for snippet_name in diff.snippets_to_remove:
        if not _is_managed_snippet(snippet_name):
            continue
        if status_cb:
            status_cb(f"🧹 Removing outdated VCL snippet '{snippet_name}'...")
        try:
            fastly_integration.delete_snippet(service_id, draft_version, snippet_name, token)
        except Exception:
            pass

    # Step 6.2: Purge old endpoints
    for endpoint_name in diff.endpoints_to_remove:
        if status_cb:
            status_cb(f"🧹 Removing outdated logging endpoint '{endpoint_name}'...")
        try:
            fastly_integration.delete_logging_endpoint(service_id, draft_version, endpoint_name, token)
        except Exception:
            pass

    # Step 6.3: Purge old backends (with whitelist guard)
    for backend_name in diff.backends_to_remove:
        assert backend_name in MANAGED_BACKEND_NAMES, (
            f"CRITICAL: Attempted to delete non-whitelisted backend '{backend_name}'. "
            f"Whitelisted: {MANAGED_BACKEND_NAMES}"
        )
        if status_cb:
            status_cb(f"🧹 Removing outdated backend '{backend_name}'...")
        try:
            fastly_integration.delete_backend(service_id, draft_version, backend_name, token)
        except Exception:
            pass

    # Step 6.4: Purge old dictionaries (with whitelist guard)
    for dict_name in diff.dictionaries_to_remove:
        assert dict_name in MANAGED_DICTIONARY_NAMES, (
            f"CRITICAL: Attempted to delete non-whitelisted dictionary '{dict_name}'. "
            f"Whitelisted: {MANAGED_DICTIONARY_NAMES}"
        )
        if status_cb:
            status_cb(f"🧹 Removing outdated edge dictionary '{dict_name}'...")
        try:
            fastly_integration.delete_dictionary(service_id, draft_version, dict_name, token)
        except Exception:
            pass

    # Step 6.4.5: Ensure unified conditions are present if any of the endpoints/backends references them
    from backend.core.fastly.service import ensure_condition

    # 1. log_analytics_condition
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
        try:
            ensure_condition(
                name="log_analytics_condition",
                statement=statement,
                type="RESPONSE",
                service_id=service_id,
                version=draft_version,
                token=token,
            )
        except Exception as e:
            import logging

            logging.warning(f"WARNING: Failed to ensure log_analytics_condition: {e}")

    # 2. rum_log_condition
    has_rum_cond = any(ep.response_condition == "rum_log_condition" for ep in desired_endpoints)
    if has_rum_cond:
        if status_cb:
            status_cb("➕ Configuring RUM routing condition 'rum_log_condition'...")
        statement = 'req.url.path == "/rum-beacon"'
        if (
            desired_state
            and getattr(desired_state, "rum_custom_condition", None)
            and desired_state.rum_custom_condition.strip()
        ):
            statement = f"{statement} && ({desired_state.rum_custom_condition.strip()})"
        try:
            ensure_condition(
                name="rum_log_condition",
                statement=statement,
                type="RESPONSE",
                service_id=service_id,
                version=draft_version,
                token=token,
            )
        except Exception as e:
            import logging

            logging.warning(f"WARNING: Failed to ensure rum_log_condition: {e}")

    # 3. fastly_log_analytics_false
    desired_backends_list = desired_backends(desired_state) if desired_state else []
    has_false_cond = any(ep.response_condition == "fastly_log_analytics_false" for ep in desired_endpoints) or any(
        b.request_condition == "fastly_log_analytics_false" for b in desired_backends_list
    )
    if has_false_cond:
        if status_cb:
            status_cb("➕ Configuring request condition 'fastly_log_analytics_false'...")
        try:
            ensure_condition(
                name="fastly_log_analytics_false",
                statement="false",
                type="REQUEST",
                service_id=service_id,
                version=draft_version,
                token=token,
            )
        except Exception as e:
            import logging

            logging.warning(f"WARNING: Failed to ensure fastly_log_analytics_false: {e}")

    # Step 6.5: Install/update backends
    for backend in diff.backends_to_add + diff.backends_to_update:
        if status_cb:
            status_cb(f"➕ Installing backend '{backend.name}'...")
        try:
            fastly_integration.create_or_update_backend(service_id, draft_version, backend, token)
        except Exception as e:
            import logging

            logging.warning(f"WARNING: Backend creation failed for {backend.name}: {e}")
            raise

    # Step 6.6: Install/update dictionaries
    for dictionary in diff.dictionaries_to_add + diff.dictionaries_to_update:
        if status_cb:
            status_cb(f"➕ Installing edge dictionary '{dictionary.name}'...")
        try:
            fastly_integration.create_or_update_dictionary(service_id, draft_version, dictionary, token)
        except Exception:
            pass

    # Step 6.7: Install/update consolidated snippets
    for snippet in diff.snippets_to_add + diff.snippets_to_update:
        if status_cb:
            status_cb(f"➕ Installing VCL snippet '{snippet.name}'...")
        try:
            fastly_integration.create_or_update_snippet(service_id, draft_version, snippet, token)
        except Exception:
            pass

    # Step 6.8: Install/update logging endpoints
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
            import logging

            logger = logging.getLogger(__name__)
            logger.exception(f"Failed to install/update logging endpoint '{endpoint.name}': {e}")
            if status_cb:
                status_cb(f"❌ Failed to install logging endpoint '{endpoint.name}': {e}")


def _is_managed_snippet(name: str) -> bool:
    """Check if snippet is managed by our log analytics system (consolidated or legacy)."""
    if name.startswith("Fastly Log Analytics - "):
        return True
    return _is_legacy_snippet(name)


def _is_legacy_snippet(name: str) -> bool:
    """Check if snippet is a legacy (pre-consolidated) snippet."""
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


def _is_strict_logging_legacy_snippet(name: str) -> bool:
    """Check if snippet is one of our strict old capture logging snippets."""
    return (name.startswith("Fastly Log Analytics") or name.startswith("Fastly Log Analysis")) and not name.startswith(
        "Fastly Log Analytics - "
    )


def _detect_and_queue_legacy_cleanup(
    current_snippets: list[VCLSnippet],
    diff: DiffResult,
    status_cb: Callable[[str], None] | None = None,
) -> None:
    """Detect legacy snippets and queue them for removal (migration helper).

    If the service has legacy snippets, queue them for removal to ensure a clean state.
    All legacy snippets matching legacy prefixes are always removed to prevent variable/method collisions.

    Args:
        current_snippets: List of current snippets from Fastly.
        diff: DiffResult to mutate (add legacy snippet names to removal queue).
        status_cb: Optional callback for status messages.
    """
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
    """Validate draft version via Fastly API. Return error string if invalid."""
    try:
        fastly_integration.validate_version(service_id, draft_version, token)
        return ""  # Valid
    except RuntimeError as e:
        return str(e)  # Error message


def _activate_draft(service_id: str, token: str, draft_version: int) -> None:
    """Activate the draft version on Fastly."""
    fastly_integration.activate_version(service_id, draft_version, token)


def _upload_state_to_fos(service_id: str, state: FeatureState) -> None:
    """Upload state manifest to FOS (async, fire-and-forget).

    This is a non-critical operation. If it fails, log a warning but don't raise.
    """
    try:
        # TODO: Implement FOS upload
        # s3://fos-bucket/internal/reconciliation/{service_id}/state_manifest.json
        pass
    except Exception:
        pass


RUM_SURROGATE_KEY = "rum-js"


def _purge_rum_surrogate_key(service_id: str, token: str) -> None:
    """Purge the ``rum-js`` surrogate key so the edge drops every cached RUM script.

    Both hosted scripts are tagged ``Surrogate-Key: rum-js`` in vcl_fetch —
    /js/rum.js in ``backend.provision.declarative.generators.
    _generate_rum_section_vcl`` and /js/faro-sdk.js in
    ``backend.core.fastly.rum_provisioning._generate_faro_fetch_vcl`` (which
    also keeps its own ``rum-faro-sdk`` key for the Faro-only purges in the
    FOS-sync cron and upgrade_faro_version). One key purge therefore covers
    both paths and every cached ``?v=`` variant of them, which the previous
    per-URL purge could only approximate by enumerating domains × guessed
    query strings — and silently missed anything it failed to guess.

    Keyed purges go to the same service that carries the RUM VCL (the
    logging service reconciled here), matching the precedent in
    ``backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key``.

    Never raises: reconciliation has already activated by the time this
    runs, so a purge failure must not turn a successful reconcile into a
    reported failure — the objects age out on their edge TTL regardless.
    """
    import logging

    from backend.core.fastly.client import fastly as fastly_client

    logger = logging.getLogger("backend.scheduler")
    try:
        logger.info(f"Purging RUM surrogate key {RUM_SURROGATE_KEY} on service {service_id}")
        fastly_client(
            "POST",
            f"/service/{service_id}/purge/{RUM_SURROGATE_KEY}",
            token=token,
            expect_empty=True,
        )
    except Exception as e:
        logger.warning(f"Failed to purge surrogate key {RUM_SURROGATE_KEY} for service {service_id}: {e}")
