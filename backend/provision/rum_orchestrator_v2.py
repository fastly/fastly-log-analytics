"""Refactored RUM orchestration using declarative reconciliation (Phase 2).

Instead of imperatively mutating VCL snippets, we now:
1. Update the config file
2. Call reconcile_vcl_state() to apply the desired state
3. Return the result

This eliminates state collisions and orphaned assets.
"""

from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

from backend import config as svcconfig
from backend.provision.declarative.reconciler import reconcile_vcl_state
from backend.provision.fos_setup import ensure_fos_bucket  # noqa: F401
from backend.provision.rum_assets import upload_rum_tracker_js
from backend.provision.utils import BOLD, _c, fail, info, ok, warn

logger = logging.getLogger(__name__)


# RUM custom-field definitions the orchestrator adds/removes when enabling/disabling.
# Kept as a single source of truth so disable_rum can find them by name to undo cleanly.
_RUM_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": "rum_cid",
        "label": "RUM Session ID",
        "description": "Derived from session scoring sid; per-session correlation key for joining with edge_sid.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_cid",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 12,
        "enabled": True,
    },
    {
        "name": "fastly_req_id",
        "label": "RUM Request ID",
        "description": "Per-request correlation key (minted at edge) for joining client-reported TTFB with server-side metrics.",
        "vcl_log_expression": "req.http.x-fos-edge-data:fastly_req_id",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 12,
        "enabled": True,
    },
    {
        "name": "rum_metric_name",
        "label": "RUM Metric Name",
        "description": "Web Vital name (LCP, INP, CLS, FCP, TTFB) or event type (navigation, error, console).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_metric_name",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 12,
        "enabled": True,
    },
    {
        "name": "rum_metric_value",
        "label": "RUM Metric Value",
        "description": "Numeric value of the metric (milliseconds for timing, unitless for rating).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_metric_value",
        "collection_stage": "beacon",
        "duckdb_type": "DOUBLE",
        "value_type": "numeric",
        "bytes_estimate": 8,
        "enabled": True,
    },
    {
        "name": "rum_metric_rating",
        "label": "RUM Metric Rating",
        "description": "Web Vital rating: good, needs-improvement, poor.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_metric_rating",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 18,
        "enabled": True,
    },
    {
        "name": "rum_error_message",
        "label": "RUM Error Message",
        "description": "JavaScript error message (PII scrubbed, byte-capped).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_error_message",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 200,
        "byte_limit": 500,
        "enabled": True,
    },
    {
        "name": "rum_error_stack",
        "label": "RUM Error Stack",
        "description": "JavaScript stack trace (PII scrubbed, byte-capped).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_error_stack",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 400,
        "byte_limit": 2000,
        "enabled": True,
    },
    {
        "name": "rum_trace_id",
        "label": "RUM Trace ID",
        "description": "OpenTelemetry trace_id from Faro Web Tracing (32 hex chars).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_trace_id",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 32,
        "byte_limit": 40,
        "enabled": True,
    },
    {
        "name": "rum_span_id",
        "label": "RUM Span ID",
        "description": "OpenTelemetry span_id from Faro Web Tracing (16 hex chars).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_span_id",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 16,
        "byte_limit": 24,
        "enabled": True,
    },
    {
        "name": "rum_pathname",
        "label": "RUM Page Pathname",
        "description": "Client-reported window.location.pathname.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_pathname",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 256,
        "enabled": True,
    },
    {
        "name": "rum_connection_speed",
        "label": "RUM Connection Speed",
        "description": "navigator.connection.effectiveType (4g, 3g, 2g, slow-2g, unknown).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_connection_speed",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 10,
        "enabled": True,
    },
    {
        "name": "rum_dns_ms",
        "label": "RUM DNS Time (ms)",
        "description": "Navigation timing: domainLookupEnd - domainLookupStart.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_dns_ms",
        "collection_stage": "beacon",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "rum_tcp_ms",
        "label": "RUM TCP Time (ms)",
        "description": "Navigation timing: connectEnd - connectStart.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_tcp_ms",
        "collection_stage": "beacon",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "rum_tls_ms",
        "label": "RUM TLS Time (ms)",
        "description": "Navigation timing: requestStart - secureConnectionStart.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_tls_ms",
        "collection_stage": "beacon",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "rum_ttfb_ms",
        "label": "RUM TTFB (ms)",
        "description": "Navigation timing: responseStart - requestStart.",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_ttfb_ms",
        "collection_stage": "beacon",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "rum_raw_query",
        "label": "RUM Raw Query String",
        "description": "Complete raw querystring from beacon (event_N_* params encoded as one opaque string).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_raw_query",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 800,
        "byte_limit": 8192,
        "enabled": True,
    },
]
_RUM_FIELD_NAMES = {cf["name"] for cf in _RUM_CUSTOM_FIELDS}


def merge_rum_custom_fields(custom_fields: list[dict] | None) -> list[dict]:
    """Return ``custom_fields`` with the canonical RUM fields re-applied.

    Drops any existing entries whose name collides with a RUM field, then
    appends fresh copies of ``_RUM_CUSTOM_FIELDS`` (code is the source of truth).
    """
    if not isinstance(custom_fields, list):
        custom_fields = []
    kept = [cf for cf in custom_fields if isinstance(cf, dict) and cf.get("name") not in _RUM_FIELD_NAMES]
    return kept + [dict(cf) for cf in _RUM_CUSTOM_FIELDS]


def rum_vcl_fingerprint(logging_service_id: str) -> str:
    """Content hash of the RUM VCL the current generator produces for this service.
    Used for drift detection in /rum/status (comparing deployed vs. current)."""
    import hashlib
    import json

    from backend.core.fastly.rum_provisioning import generate_rum_vcl

    snippets = generate_rum_vcl(logging_service_id)
    return hashlib.sha256(json.dumps(snippets, sort_keys=True).encode()).hexdigest()


def enable_rum(
    logging_service_id: str,
    token: str,
    *,
    activate: bool = True,
    status_cb=None,
) -> dict[str, Any]:
    """Enable RUM for the logging service via declarative reconciliation.

    Updates config file and calls reconcile_vcl_state() to apply the desired state.

    Args:
        logging_service_id: Fastly service ID.
        token: Fastly API token.
        status_cb: Optional callback for status updates.

    Returns:
        {
            "logging_service_active_version": int (post-activate),
            "enabled_at": str (ISO timestamp),
            "activated": bool (whether new version was activated),
        }

    Raises:
        RuntimeError: if enable fails.
    """
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    info(f"Enabling RUM for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Enabling RUM for {logging_service_id}...")

    # Step 1: Update config to enable RUM
    if cfg.get("rum_enabled", False):
        ok("RUM already enabled")
        return {
            "logging_service_active_version": cfg.get("last_activated_version", 1),
            "enabled_at": cfg.get("rum_enabled_at", ""),
            "activated": False,
        }

    cfg["rum_enabled"] = True
    cfg["rum_enabled_at"] = _dt.datetime.now(_dt.UTC).isoformat()
    cfg["rum_vcl_sha"] = rum_vcl_fingerprint(logging_service_id)

    # Save updated config
    svcconfig.save_config(logging_service_id, cfg)
    ok("Config updated: rum_enabled=True, rum_vcl_sha saved")

    # Step 2: Verify FOS bucket (use existing if configured, skip provisioning)
    # RUM shares the bucket from request logging if enabled, or uses its own if standalone.
    # If config already has fos_bucket (from logging or prior RUM), skip provisioning.
    bucket_name = cfg.get("fos_bucket")
    region = cfg.get("fos_region")
    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")

    if not all([bucket_name, region, access_key, secret_key]):
        raise RuntimeError(
            f"Service {logging_service_id} missing FOS configuration (bucket, region, access_key_id, secret_access_key)"
        )

    ok(f"Using FOS bucket: {bucket_name}")
    if status_cb:
        status_cb(f"✅ Using FOS bucket '{bucket_name}'.")

    # Step 3: Upload RUM tracker JS to FOS (blocks RUM enable if upload fails)
    # The asset-fetch VCL snippet (backend/core/fastly/rum_provisioning.py) routes
    # client requests from GET /js/rum.js to /rum/rum-tracker.js on FOS.
    # This path MUST match the upload path (rum_assets.py: "rum/rum-tracker.js").
    try:
        if status_cb:
            status_cb("⏳ Uploading RUM tracker JS...")

        upload_rum_tracker_js(logging_service_id, token, status_cb=status_cb)
        ok("RUM tracker JS uploaded")

    except Exception as e:
        # Rollback config on JS upload failure
        cfg["rum_enabled"] = False
        cfg.pop("rum_vcl_sha", None)
        svcconfig.save_config(logging_service_id, cfg)
        fail(f"JS upload failed: {e}")
        raise

    if status_cb:
        status_cb("⏳ Reconciling VCL state...")

    # Step 4: Reconcile desired state with Fastly
    try:
        result = reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb, activate=activate)
    except Exception as e:
        # Rollback config
        cfg["rum_enabled"] = False
        cfg.pop("rum_vcl_sha", None)
        svcconfig.save_config(logging_service_id, cfg)
        fail(f"Reconciliation failed: {e}")
        raise

    if result.activated_version:
        ok(f"RUM enabled: version {result.activated_version} activated")
    elif result.draft_version:
        ok(f"RUM enabled in draft version {result.draft_version} (activation bypassed)")
    else:
        ok("RUM already in desired state (no changes needed)")

    return {
        "logging_service_active_version": result.activated_version or result.draft_version,
        "enabled_at": cfg.get("rum_enabled_at", ""),
        "activated": result.activated_version is not None,
    }


def disable_rum(
    logging_service_id: str,
    token: str,
    *,
    remove_cloud_files: bool = False,
    remove_bucket: bool = False,
    activate: bool = True,
    status_cb=None,
) -> dict[str, Any]:
    """Disable RUM for the logging service via declarative reconciliation.

    Updates config file and calls reconcile_vcl_state() to apply the desired state.
    Old RUM snippets and endpoints are automatically cleaned up.

    Args:
        logging_service_id: Fastly service ID.
        token: Fastly API token.
        remove_cloud_files: Optional flag to delete RUM logs from FOS.
        remove_bucket: Optional flag to delete the FOS bucket entirely.
        status_cb: Optional callback for status updates.

    Returns:
        {
            "logging_service_active_version": int (post-activate),
            "deactivated": bool (whether new version was activated),
        }

    Raises:
        RuntimeError: if disable fails.
    """
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    info(f"Disabling RUM for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Disabling RUM for {logging_service_id}...")

    # Step 1: Check if already disabled
    if not cfg.get("rum_enabled", False):
        ok("RUM already disabled")
        return {
            "logging_service_active_version": cfg.get("last_activated_version", 1),
            "deactivated": False,
        }

    # Step 2: Update config to disable RUM
    cfg["rum_enabled"] = False
    cfg.pop("rum_vcl_sha", None)
    svcconfig.save_config(logging_service_id, cfg)
    ok("Config updated: rum_enabled=False, rum_vcl_sha cleared")

    # Step 3: Delete JS from FOS (NON-BLOCKING)
    if status_cb:
        status_cb("⏳ Deleting RUM tracker JS…")

    from backend.provision.rum_assets import delete_rum_tracker_js

    try:
        delete_rum_tracker_js(logging_service_id, token, status_cb=status_cb)
    except Exception as e:
        # Non-blocking: log warning and continue with reconciliation
        logger.warning(f"Failed to delete RUM tracker JS: {e}")
        if status_cb:
            status_cb(f"⚠️  JS deletion failed (non-blocking): {e}")

    if status_cb:
        status_cb("⏳ Reconciling VCL state...")

    # Step 4: Reconcile desired state with Fastly
    # Old RUM snippets and endpoints are automatically cleaned up by the reconciler
    try:
        result = reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb, activate=activate)
    except Exception as e:
        # Rollback config
        cfg["rum_enabled"] = True
        cfg["rum_vcl_sha"] = rum_vcl_fingerprint(logging_service_id)
        svcconfig.save_config(logging_service_id, cfg)
        fail(f"Reconciliation failed: {e}")
        raise

    if result.activated_version:
        ok(f"RUM disabled: version {result.activated_version} activated")
    elif result.draft_version:
        ok(f"RUM disabled in draft version {result.draft_version} (activation bypassed)")
    else:
        ok("RUM already in desired state (no changes needed)")

    # Optional cloud file/bucket deletions
    fos_bucket = cfg.get("fos_bucket", "")
    fos_region = cfg.get("fos_region", "us-east-1")
    fos_access_key = cfg.get("fos_access_key_id", "")
    fos_secret_key = cfg.get("fos_secret_access_key", "")

    if remove_cloud_files and fos_bucket and fos_access_key and fos_secret_key:
        try:
            from backend.provision.fos_setup import delete_fos_prefix

            prefix = cfg.get("fos_prefix", "")
            rum_prefix = f"{prefix.strip('/')}/raw/rum/" if prefix else "raw/rum/"
            if status_cb:
                status_cb("⏳ Deleting RUM cloud files...")
            delete_fos_prefix(
                fos_bucket,
                fos_region,
                fos_access_key,
                fos_secret_key,
                rum_prefix,
                status_cb=status_cb,
                service_id=logging_service_id,
            )
        except Exception as e:
            logger.warning(f"Failed to delete RUM cloud files: {e}")
            if status_cb:
                status_cb(f"⚠️ Warning: Failed to delete RUM cloud files: {e}")

    if remove_bucket and fos_bucket and fos_access_key and fos_secret_key:
        if cfg.get("logging_enabled", False):
            warn("Cannot delete FOS bucket while request logging is still enabled (bucket is shared)")
            if status_cb:
                status_cb("⚠️ Cannot delete bucket: request logging is still active")
        else:
            try:
                from backend.provision.fos_setup import delete_fos_bucket

                if status_cb:
                    status_cb("⏳ Deleting FOS bucket...")
                delete_fos_bucket(
                    fos_bucket,
                    fos_region,
                    fos_access_key,
                    fos_secret_key,
                    status_cb=status_cb,
                    service_id=logging_service_id,
                )
            except Exception as e:
                logger.warning(f"Failed to delete FOS bucket: {e}")
                if status_cb:
                    status_cb(f"⚠️ Warning: Failed to delete FOS bucket: {e}")

    return {
        "logging_service_active_version": result.activated_version or result.draft_version,
        "deactivated": result.activated_version is not None,
    }


def get_rum_status(logging_service_id: str, token: str) -> dict[str, Any]:
    """Get RUM status for a logging service.

    Checks if RUM is enabled in config and detects any drift from Fastly.

    Args:
        logging_service_id: Fastly service ID.
        token: Fastly API token.

    Returns:
        {
            "enabled": bool (in config),
            "enabled_at": str (ISO timestamp, or empty),
            "last_activated_version": int,
            "drift_detected": bool (config != Fastly),
        }
    """
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        return {
            "enabled": False,
            "enabled_at": "",
            "last_activated_version": None,
            "drift_detected": False,
        }

    return {
        "enabled": cfg.get("rum_enabled", False),
        "enabled_at": cfg.get("rum_enabled_at", ""),
        "last_activated_version": cfg.get("last_activated_version"),
        "drift_detected": False,  # TODO: Implement drift detection
    }
