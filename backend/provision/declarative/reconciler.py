"""VCL Declarative Reconciliation Control Loop (Steps 1-8).

The reconciler executes the classical control loop:
Read Desired State → Read Current State → Diff → Apply Diff → Validate & Activate
"""

from __future__ import annotations

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

from backend.core.fastly.client import fastly
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

    def summary(self) -> dict[str, int]:
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


def _update_local_config(service_id: str, mutate: Callable[[dict], None]) -> None:
    """Best-effort locked edit of the local config cache.

    Goes through ``mutate_config`` (cross-process lock + fresh read) so activation metadata
    never clobbers or races a concurrent writer; a read-only volume is skipped, as before.
    """
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
    """Write a ``log_format_change`` audit row for an activated reconcile.

    Failure is logged, never raised: the new version is already live, so a
    missing history row must not turn a successful reconcile into an error.
    """
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
                try:
                    from backend.config import config_lock, save_config

                    with config_lock(service_id):
                        if not config_path.exists():
                            save_config(service_id, asdict(desired_state))
                except OSError as e:
                    import logging

                    logging.getLogger("backend.scheduler").info(f"Skipped updating read-only config cache: {e}")
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

            # compute_diff lists every snippet on the service that isn't desired,
            # including the operator's own VCL. Only managed snippets are ever
            # deleted, so keep just those: the dry-run summary then states what
            # an apply would remove, and an already-correct service is empty
            # (idempotent) instead of cloning a new version on every run.
            diff.snippets_to_remove = [n for n in diff.snippets_to_remove if _is_managed_snippet(n)]

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
                    # No active-version guard here: the draft was never activated, and an out-of-band
                    # activation (e.g. Fastly console) must not mask the validation errors we report.
                    raise VclValidationError(service_id, draft_version, validation_errors)

                result.draft_version = draft_version

                if activate:
                    # Step 8: Activate draft & persist state
                    if status_cb:
                        status_cb("⏳ Activating draft version...")
                    _activate_draft(service_id, token, draft_version)
                    result.activated_version = draft_version
                    _record_reconcile_audit(service_id, diff, current_endpoints, desired_endpoints_list, draft_version)

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
    from backend.core.fastly.client import find_service_by_name
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
        if existing and existing.get("id") == logging_service_id:
            # A customer service may share the requested display name. Never
            # adopt it as the analytics CDN or allow rollback to delete it.
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
        def _persist_cdn(c: dict) -> None:
            proxy = c.setdefault("fos_proxy", {})
            proxy["service_id"] = cdn_service_id
            proxy["service_name"] = cdn_service_name
            proxy["domain"] = cdn_url
            proxy["shield"] = cdn_shield
            proxy["secret"] = cdn_secret
            c["cdn_service_id"] = cdn_service_id  # flat key

        _update_local_config(logging_service_id, _persist_cdn)

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

                    def _persist_activation(c: dict) -> None:
                        c.setdefault("fos_proxy", {})["last_activated_version"] = draft_version
                        c["last_activated_version_cdn"] = draft_version

                    _update_local_config(logging_service_id, _persist_activation)
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


def _fetch_from_active(service_id: str, token: str, fetcher: Callable[[str, int, str], list]) -> list:
    active_ver = fastly_integration.fetch_active_version(service_id, token)
    return fetcher(service_id, active_ver, token) if active_ver is not None else []


def _fetch_snippets(service_id: str, token: str) -> list[VCLSnippet]:
    """Fetch current VCL snippets from Fastly (from active version)."""
    return _fetch_from_active(service_id, token, fastly_integration.fetch_snippets)


def _fetch_logging_endpoints(service_id: str, token: str) -> list[LoggingEndpoint]:
    """Fetch current logging endpoints from Fastly (from active version)."""
    return _fetch_from_active(service_id, token, fastly_integration.fetch_logging_endpoints)


def _fetch_backends(service_id: str, token: str) -> list[Backend]:
    """Fetch current backends from Fastly (from active version)."""
    return _fetch_from_active(service_id, token, fastly_integration.fetch_backends)


def _fetch_dictionaries(service_id: str, token: str) -> list[ServiceDictionary]:
    """Fetch current dictionaries from Fastly (from active version)."""
    return _fetch_from_active(service_id, token, fastly_integration.fetch_dictionaries)


def _ensure_cdn_domain(
    service_id: str, domain: str, token: str, status_cb: Callable[[str], None] | None = None
) -> None:
    """Add ``domain`` to a REUSED CDN service if it isn't already attached.

    A service adopted via find-by-name (not freshly created) skips the
    "Add domain to version 1" step above entirely, since that step only
    runs in the create branch. Left unchecked, a service originally
    provisioned under an older cdn_prefix convention (e.g. bucket-name-
    based) silently keeps that domain forever while config.cdn_url reflects
    whatever the CURRENT default computes (e.g. service-id-based) — every
    read through the CDN then 500s with Fastly's "unknown domain" error
    because the domain in config was never actually registered.
    """
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
    highest = _fetch_highest_version(service_id, token, raise_on_error=True)
    if highest is not None:
        return fastly_integration.clone_version(service_id, highest, token, comment)

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
            from backend.core.fastly.utils import region_endpoint

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

    # Step 6.1–6.4: Purge old snippets, endpoints, backends, and dictionaries
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

    # Step 6.4.5: Ensure unified conditions are present if any of the endpoints/backends references them
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
            import logging

            logging.warning(f"WARNING: Failed to ensure {name}: {e}")

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
        _ensure_cond_safe("log_analytics_condition", statement)

    # 2. rum_log_condition
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

    # 3. fastly_log_analytics_false
    desired_backends_list = desired_backends(desired_state) if desired_state else []
    has_false_cond = any(ep.response_condition == "fastly_log_analytics_false" for ep in desired_endpoints) or any(
        b.request_condition == "fastly_log_analytics_false" for b in desired_backends_list
    )
    if has_false_cond:
        if status_cb:
            status_cb("➕ Configuring request condition 'fastly_log_analytics_false'...")
        _ensure_cond_safe("fastly_log_analytics_false", "false", cond_type="REQUEST")

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
        except Exception as e:
            logger.warning("Failed to install/update edge dictionary '%s': %s", dictionary.name, e)

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
        if not fastly_integration.validate_version(service_id, draft_version, token):
            return f"VCL validation failed: version {draft_version} did not validate (status not ok)"
        return ""  # Valid
    except RuntimeError as e:
        return str(e)  # Error message


def _activate_draft(service_id: str, token: str, draft_version: int) -> None:
    """Activate the draft version on Fastly."""
    fastly_integration.activate_version(service_id, draft_version, token)


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


sys.modules["backend.provision.declarative.diff"] = sys.modules[__name__]
