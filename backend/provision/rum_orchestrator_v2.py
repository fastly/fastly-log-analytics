"""Refactored RUM orchestration using declarative reconciliation (Phase 2).

Instead of imperatively mutating VCL snippets, we now:
1. Update the config file
2. Call reconcile_vcl_state() to apply the desired state
3. Return the result

This eliminates state collisions and orphaned assets.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
from typing import Any

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.fastly.rum_provisioning import _assert_faro_version_safe
from backend.provision.declarative.reconciler import reconcile_vcl_state
from backend.provision.fos_setup import ensure_fos_bucket  # noqa: F401
from backend.provision.rum_assets import cleanup_old_faro_versions, download_and_upload_faro, upload_rum_tracker_js
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
    {
        "name": "rum_body",
        "label": "RUM Request Body",
        "description": "Raw JSON POST body from Faro Web SDK beacon (capped at 8KB).",
        "vcl_log_expression": "req.http.x-fos-edge-data:rum_body",
        "collection_stage": "beacon",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 1024,
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


def rum_vcl_fingerprint(logging_service_id: str, cfg: dict[str, Any] | None = None) -> str:
    """Content hash of the RUM VCL the current generator produces for this service.

    Used for drift detection in /rum/status (comparing deployed vs. current).

    Covers BOTH the Phase 1 snippets (recv beacon + deliver cookie) and the
    Phase 3 Faro asset-fetch snippets (asset routing, SigV4 object-path
    rewrite, fetch-cache policy) — the latter is where a pinned
    ``faro_version`` actually surfaces in generated VCL. Before this fix
    (F-4 audit finding) this function called ``generate_rum_vcl()`` alone,
    whose Phase 1 output never varies with ``faro_version`` at all — the
    fingerprint was structurally incapable of detecting drift in the faro
    snippets, e.g. a service whose VCL was generated with
    ``faro_version=None`` (never reconciled after a version was pinned)
    would report no drift.

    Args:
        logging_service_id: Fastly service ID.
        cfg: When given, used directly instead of reloading config from
            disk — callers that are mid-mutation of their own in-memory cfg
            (``enable_rum``, ``disable_rum``'s rollback, ``upgrade_faro_version``)
            pass it here so the fingerprint reflects the state they are
            about to persist, not whatever is still on disk. Callers that
            just want "what would today's generator produce for this
            service's current on-disk config" (e.g. ``/rum/status``) omit it.
    """
    import hashlib
    import json

    from backend.core.fastly.rum_provisioning import generate_rum_asset_fetch_vcl, generate_rum_vcl

    if cfg is None:
        cfg = svcconfig.load_config(logging_service_id) or {}
    rum_raw = cfg.get("rum")
    rum_cfg = rum_raw if isinstance(rum_raw, dict) else {}
    faro_version = rum_cfg.get("faro_version")

    shield_pop = cfg.get("cdn_shield") or ""
    if not shield_pop:
        from backend.core.fastly.utils import SHIELD_MAP

        shield_pop = SHIELD_MAP.get(cfg.get("fos_region", "us-east-1"), "iad-va-us")

    snippets = generate_rum_vcl(logging_service_id, faro_version)
    snippets.update(generate_rum_asset_fetch_vcl(shield_pop, faro_version))
    return hashlib.sha256(json.dumps(snippets, sort_keys=True).encode()).hexdigest()


def legacy_rum_vcl_fingerprint(logging_service_id: str) -> str:
    """Reproduce the pre-F-4 ``rum_vcl_fingerprint`` algorithm.

    Used purely to migrate a ``rum_vcl_sha`` stored before the F-4 fix
    (#2 audit finding on top of F-4): the old algorithm hashed only the
    Phase 1 (recv beacon + deliver cookie) snippets and never passed
    ``faro_version`` into the generator (it always used the implicit
    default, ``None``) — so for a given ``logging_service_id`` it produces
    the SAME hash regardless of the actual pinned Faro version or shield
    config. Every service enabled/upgraded before F-4 shipped has this
    stale, permanently-non-matching value sitting in ``rum_vcl_sha``, which
    would otherwise false-positive ``vcl_drift`` forever — including
    immediately after a correct reconcile — since the new algorithm's
    output can never equal it again.

    ``/rum/status`` uses this to recognize "this stored sha predates F-4"
    (an exact match, not a heuristic) and migrate it to the current
    algorithm's output instead of reporting permanent false drift.
    """
    import hashlib
    import json

    from backend.core.fastly.rum_provisioning import generate_rum_vcl

    snippets = generate_rum_vcl(logging_service_id)
    return hashlib.sha256(json.dumps(snippets, sort_keys=True).encode()).hexdigest()


def _set_faro_version(cfg: dict[str, Any], version: str | None) -> None:
    """Set (or clear) ``cfg["rum"]["faro_version"]`` in place, preserving other keys."""
    rum_cfg = dict(cfg.get("rum") or {})
    if version is None:
        rum_cfg.pop("faro_version", None)
    else:
        rum_cfg["faro_version"] = version
    cfg["rum"] = rum_cfg


def _purge_faro_surrogate_key(logging_service_id: str, token: str) -> None:
    """Fire-and-forget surrogate-key purge on the logging service after a Faro bundle upload.

    Purges on ``logging_service_id`` — NOT the CDN service fronting FOS for
    log downloads (F-1 audit finding). ``Surrogate-Key: rum-faro-sdk`` is
    set only in the RUM fetch-cache snippet
    (``backend.core.fastly.rum_provisioning._generate_faro_fetch_vcl``),
    which is deployed to the instrumented *logging* service by
    ``reconcile_vcl_state(logging_service_id, ...)`` — it never exists on
    the CDN service, so a purge targeting the CDN service can never match
    anything. This mirrors (and previously miscopied) the CDN-purge
    precedent in ``backend.core.iceberg._core._purge_surrogate_key``, where
    the CDN service IS the correct target for a different, unrelated key.

    Deliberately duplicated (not imported) from
    ``backend.cron.jobs.rum_sync._faro_purge_surrogate_key`` — provision code
    must not import from cron modules (see task-6 report). Keep both in sync
    if the purge shape ever changes, including this never-raise contract: a
    missing service id/token or a purge failure must never surface here —
    the upload/reconcile this purge follows has already succeeded, and a
    stale cache is a warning, not a failed upgrade. Callers may still wrap
    this in their own try/except for status_cb reporting; that's belt and
    suspenders, not a requirement.
    """
    if not logging_service_id or not token:
        return
    try:
        from backend.core.fastly.client import fastly

        fastly("POST", f"/service/{logging_service_id}/purge/rum-faro-sdk", token=token, expect_empty=True)
    except Exception:
        logger.warning("Faro surrogate-key purge failed for %s (non-fatal)", logging_service_id, exc_info=True)


def enable_rum(
    logging_service_id: str,
    token: str,
    *,
    activate: bool = True,
    status_cb=None,
    faro_version: str | None = None,
) -> dict[str, Any]:
    """Enable RUM for the logging service via declarative reconciliation.

    Updates config file and calls reconcile_vcl_state() to apply the desired state.

    Args:
        logging_service_id: Fastly service ID.
        token: Fastly API token.
        status_cb: Optional callback for status updates.
        faro_version: Pinned Faro Web SDK version (``X.Y.Z``) to self-host
            alongside enabling RUM. When omitted, defaults to
            ``backend.core.faro_versions.DEFAULT_FARO_VERSION`` — RUM can no
            longer be enabled without a self-hosted bundle behind it, since
            the tracker JS unconditionally loads the first-party, relative
            ``/js/faro-sdk.js`` (there is no third-party CDN fallback).

    Note:
        If RUM is already enabled for this service, this function returns
        immediately (the pre-existing idempotent early-return below) WITHOUT
        looking at ``faro_version`` at all — no validation, upload, or config
        write happens, even if a version is passed. Calling
        ``enable_rum(..., faro_version="X.Y.Z")`` on an already-enabled service
        is silently a no-op with respect to the version. Use
        ``upgrade_faro_version`` to pin/change the version on a service that
        already has RUM enabled. A service that was enabled before this
        default existed and never got a version pinned self-heals via the
        RUM sync cron's per-tick reconcile instead
        (backend/cron/jobs/rum_sync.py::_reconcile_faro_bundle).

    Returns:
        {
            "logging_service_active_version": int (post-activate),
            "enabled_at": str (ISO timestamp),
            "activated": bool (whether new version was activated),
        }

    Raises:
        ValueError: if faro_version is provided but not a plain X.Y.Z string.
        RuntimeError: if enable fails.
    """
    resolved_faro_version = faro_version if faro_version is not None else DEFAULT_FARO_VERSION
    _assert_faro_version_safe(resolved_faro_version)

    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    info(f"Enabling RUM for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Enabling RUM for {logging_service_id}...")

    # Step 1: Idempotency check — see the "already enabled" Note above:
    # faro_version is deliberately not consulted here.
    if cfg.get("rum_enabled", False):
        ok("RUM already enabled")
        return {
            "logging_service_active_version": cfg.get("last_activated_version", 1),
            "enabled_at": cfg.get("rum_enabled_at", ""),
            "activated": False,
        }

    # Step 2: Verify FOS bucket (use existing if configured, skip provisioning)
    # RUM shares the bucket from request logging if enabled, or uses its own if standalone.
    # If config already has fos_bucket (from logging or prior RUM), skip provisioning.
    #
    # MUST run before any config mutation/save below: this check can raise,
    # and nothing has been persisted yet at this point, so there is nothing
    # to roll back — a service must never end up with rum_enabled=True or a
    # pinned faro_version on disk when FOS credentials were never even present.
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

    # Step 3: Update config to enable RUM (+ pin faro_version if given)
    previous_rum_cfg = dict(cfg.get("rum") or {})

    cfg["rum_enabled"] = True
    cfg["rum_enabled_at"] = _dt.datetime.now(_dt.UTC).isoformat()

    # Persist the pinned version BEFORE reconciling so the generator (which
    # reads cfg["rum"]["faro_version"]) sees it. If anything below fails, the
    # rollback below restores previous_rum_cfg so config never claims a
    # version that isn't actually deployed. Always pinned now — there is no
    # more "enabled but unpinned" state, since the tracker's unconditional
    # /js/faro-sdk.js load requires a bundle to always be behind that path.
    _set_faro_version(cfg, resolved_faro_version)

    # Computed AFTER faro_version is set above: rum_vcl_fingerprint's hash
    # depends on the pinned version (F-4 fix), so it must be derived from
    # THIS in-memory cfg (not a stale on-disk read from before the version
    # was set) to reflect the state about to be deployed.
    cfg["rum_vcl_sha"] = rum_vcl_fingerprint(logging_service_id, cfg)

    # Save updated config
    svcconfig.save_config(logging_service_id, cfg)
    ok("Config updated: rum_enabled=True, rum_vcl_sha saved")

    def _rollback_config() -> None:
        # Reload from disk first (mirrors upgrade_faro_version's rollback):
        # a step between here and the save above (e.g. download_and_upload_faro)
        # may have written its own config changes that the stale in-memory
        # `cfg` wouldn't reflect. Mutate `cfg` in place (rather than rebind it)
        # so its declared type stays non-Optional for mypy's benefit.
        latest = svcconfig.load_config(logging_service_id)
        if latest is not None:
            cfg.clear()
            cfg.update(latest)
        cfg["rum_enabled"] = False
        cfg.pop("rum_vcl_sha", None)
        cfg["rum"] = dict(previous_rum_cfg)
        svcconfig.save_config(logging_service_id, cfg)

    # Step 4: Upload the pinned Faro Web SDK bundle to FOS (blocks RUM enable
    # if it fails) so the VCL the reconciler is about to deploy routes
    # /js/faro-sdk.js to an object that actually exists. Unconditional now:
    # the tracker JS always loads /js/faro-sdk.js, so RUM can never be
    # enabled without a bundle behind that path. This MUST stay before the
    # reconcile below — the bundle has to exist in FOS before the VCL
    # starts routing to it.
    try:
        if status_cb:
            status_cb(f"⏳ Downloading and uploading Faro Web SDK v{resolved_faro_version}...")
        asyncio.run(download_and_upload_faro(logging_service_id, resolved_faro_version, token, status_cb=status_cb))
        ok(f"Faro Web SDK v{resolved_faro_version} uploaded")
    except Exception as e:
        _rollback_config()
        fail(f"Faro Web SDK upload failed: {e}")
        raise

    if status_cb:
        status_cb("⏳ Reconciling VCL state...")

    # Step 5: Reconcile desired state with Fastly
    try:
        result = reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb, activate=activate)
    except Exception as e:
        _rollback_config()
        fail(f"Reconciliation failed: {e}")
        raise

    # Step 6: Upload RUM tracker JS to FOS now that the VCL is actually live
    # and serving /js/faro-sdk.js — deliberately AFTER a successful
    # reconcile, never before. Publishing the tracker earlier (the pre-fix
    # ordering) would point browsers at a route that doesn't exist yet.
    # The asset-fetch VCL snippet (backend/core/fastly/rum_provisioning.py)
    # routes client requests from GET /js/rum.js to /rum/rum-tracker.js on
    # FOS. This path MUST match the upload path (rum_assets.py:
    # "rum/rum-tracker.js").
    #
    # Skipped when nothing was actually activated (activate=False): the
    # draft was compiled/validated but never went live, so the route the
    # tracker depends on isn't live yet either.
    #
    # Non-blocking (log a warning, don't fail the enable or roll back
    # config): the Fastly reconcile already succeeded and is already live,
    # so rolling back local config here would desync it from reality. The
    # reconciler's own idempotent no-change path retries this on the next
    # reconcile, and reconcile_vcl_state's internal upload (also
    # post-activation) already covers most cases — this call exists so
    # enable_rum doesn't depend on that internal behavior alone.
    if result.activated_version is not None:
        try:
            if status_cb:
                status_cb("⏳ Uploading RUM tracker JS...")
            upload_rum_tracker_js(logging_service_id, token, status_cb=status_cb)
            ok("RUM tracker JS uploaded")
        except Exception as e:
            logger.warning(f"RUM tracker JS upload failed after successful enable for {logging_service_id}: {e}")
            if status_cb:
                status_cb(f"⚠️  RUM tracker JS upload warning: {e}")

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


def upgrade_faro_version(
    logging_service_id: str,
    version: str,
    token: str,
    *,
    activate: bool = True,
    status_cb=None,
) -> dict[str, Any]:
    """Upgrade the self-hosted Faro Web SDK bundle to a new pinned version.

    Downloads and uploads the new bundle to FOS, persists the version to
    config, then reconciles VCL state so the deployed routing matches. On
    upload or reconcile failure, config is rolled back to the previous
    ``faro_version`` so it never claims a version that isn't actually
    deployed (Task 3's cron and the status endpoint both trust this
    invariant). A best-effort surrogate-key purge and old-version cleanup
    follow a successful reconcile; neither failure fails the upgrade.

    Deliberately does NOT call ``upload_rum_tracker_js``: the tracker body
    is a static wrapper that always loads the constant path
    ``/js/faro-sdk.js`` regardless of which Faro version is pinned behind
    it (see ``generate_rum_tracker_js``'s docstring), so a version bump has
    nothing new to publish there — only the Faro bundle itself changes.

    Args:
        logging_service_id: Fastly service ID.
        version: New Faro Web SDK version to pin (plain ``X.Y.Z``).
        token: Fastly API token.
        activate: If False, draft is compiled and validated but not activated.
        status_cb: Optional callback for status updates.

    Returns:
        {
            "previous_version": str | None,
            "version": str,
            "logging_service_active_version": int (post-activate),
            "activated": bool (whether new version was activated),
        }

    Raises:
        ValueError: if version is not a plain X.Y.Z string.
        RuntimeError: if RUM is not enabled, or the upgrade fails.
    """
    _assert_faro_version_safe(version)

    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    if not cfg.get("rum_enabled", False):
        raise RuntimeError(f"RUM is not enabled for {logging_service_id}; enable RUM before upgrading Faro version")

    previous_rum_cfg = dict(cfg.get("rum") or {})
    previous_version = previous_rum_cfg.get("faro_version")

    info(f"Upgrading Faro Web SDK for {_c(BOLD, logging_service_id)} to v{version}")
    if status_cb:
        status_cb(f"⏳ Upgrading Faro Web SDK to v{version}...")

    # Step 1: Download + upload the new bundle. download_and_upload_faro
    # persists cfg["rum"]["faro_version"]/faro_content_hash itself on success,
    # so config already reflects the new version once this returns.
    try:
        if status_cb:
            status_cb(f"⏳ Downloading and uploading Faro Web SDK v{version}...")
        asyncio.run(download_and_upload_faro(logging_service_id, version, token, status_cb=status_cb))
        ok(f"Faro Web SDK v{version} uploaded")
    except Exception as e:
        fail(f"Faro Web SDK upload failed: {e}")
        raise

    if status_cb:
        status_cb("⏳ Reconciling VCL state...")

    # Step 2: Reconcile so the deployed VCL routes to the new version.
    try:
        result = reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb, activate=activate)
    except Exception as e:
        # Reconcile failed after the bundle was already uploaded/persisted:
        # config must not claim a version whose VCL isn't actually deployed.
        # Restore the whole previous rum block (not just faro_version) so a
        # stale faro_content_hash from the failed upgrade can't linger either.
        cfg = svcconfig.load_config(logging_service_id) or cfg
        cfg["rum"] = dict(previous_rum_cfg)
        svcconfig.save_config(logging_service_id, cfg)
        fail(f"Reconciliation failed: {e}")
        raise

    if result.activated_version:
        ok(f"Faro Web SDK upgraded to v{version}: version {result.activated_version} activated")
    elif result.draft_version:
        ok(f"Faro Web SDK upgraded to v{version} in draft version {result.draft_version} (activation bypassed)")
    else:
        ok("Faro Web SDK already in desired state (no changes needed)")

    # Step 2.5: Refresh the stored VCL fingerprint once the new version is
    # actually live. Without this, /rum/status's vcl_drift would read
    # "drifted" forever after every upgrade — rum_vcl_fingerprint's hash
    # depends on faro_version (F-4 fix), so the fingerprint stored at the
    # last enable/upgrade (the OLD version) would never again match the
    # generator's current output (the NEW version) even though the VCL is
    # correctly reconciled. Gated on activation for the same reason as the
    # cleanup below (F-6): with activate=False the live VCL still serves the
    # old version, so the stored fingerprint must keep reflecting that.
    if result.activated_version is not None:
        cfg = svcconfig.load_config(logging_service_id) or cfg
        cfg["rum_vcl_sha"] = rum_vcl_fingerprint(logging_service_id, cfg)
        svcconfig.save_config(logging_service_id, cfg)

    # Step 3: Best-effort purge of the cached bundle so the edge stops
    # serving the old version. A purge failure is a warning, never an
    # upgrade failure.
    try:
        if status_cb:
            status_cb("⏳ Purging cached Faro bundle...")
        _purge_faro_surrogate_key(logging_service_id, token)
        ok("Purged rum-faro-sdk surrogate key")
    except Exception as e:
        warn(f"Failed to purge rum-faro-sdk surrogate key: {e}")
        if status_cb:
            status_cb(f"⚠️  Purge failed (non-blocking): {e}")

    # Step 4: Best-effort cleanup of now-stale bundle versions in FOS — only
    # once the new version is actually live (F-6 audit finding). With
    # activate=False the draft was validated but never activated, so the
    # OLD bundle is still what the live VCL serves; deleting it here would
    # take down production while config already claims the new,
    # not-yet-live pin.
    if result.activated_version is not None:
        asyncio.run(cleanup_old_faro_versions(logging_service_id, keep_current=True))

    return {
        "previous_version": previous_version,
        "version": version,
        "logging_service_active_version": result.activated_version or result.draft_version,
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
        cfg["rum_vcl_sha"] = rum_vcl_fingerprint(logging_service_id, cfg)
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
            rum_prefix = f"{prefix.strip('/')}/raw_rum/" if prefix else "raw_rum/"
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
