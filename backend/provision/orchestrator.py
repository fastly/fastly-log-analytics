"""Core provisioning orchestrator and state management."""

import json
import logging
import os
import shutil
import threading
import time
from typing import Any

from backend.core import log_fields as lf
from backend.core.duckdb import rum_duckdb_path
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.fastly.client import is_mock_mode, region_endpoint
from backend.provision.cli import ok, run_with_events, step, warn
from backend.provision.cmcd_fields import (
    _CMCD_CUSTOM_FIELDS as _CMCD_CUSTOM_FIELDS,
)
from backend.provision.cmcd_fields import (
    _CMCD_FIELD_NAMES as _CMCD_FIELD_NAMES,
)
from backend.provision.cmcd_fields import (
    CMCD_SNIPPET_NAME as CMCD_SNIPPET_NAME,
)
from backend.provision.cmcd_fields import (
    _bool_v1 as _bool_v1,
)
from backend.provision.cmcd_fields import (
    _bool_v2 as _bool_v2,
)
from backend.provision.cmcd_fields import (
    _cmcd_field_assignments_vcl as _cmcd_field_assignments_vcl,
)
from backend.provision.cmcd_fields import (
    _header_mode_vcl as _header_mode_vcl,
)
from backend.provision.cmcd_fields import (
    _query_string_mode_vcl as _query_string_mode_vcl,
)
from backend.provision.cmcd_fields import (
    _remove_cmcd_custom_fields as _remove_cmcd_custom_fields,
)
from backend.provision.cmcd_fields import (
    cmcd_snippet_names as cmcd_snippet_names,
)
from backend.provision.cmcd_fields import (
    generate_cmcd_vcl as generate_cmcd_vcl,
)
from backend.provision.cmcd_fields import (
    get_cmcd_fields as get_cmcd_fields,
)
from backend.provision.cmcd_fields import (
    reconcile_cfg_system_custom_fields as reconcile_cfg_system_custom_fields,
)
from backend.provision.cmcd_fields import (
    reconcile_cmcd_custom_fields as reconcile_cmcd_custom_fields,
)
from backend.provision.cmcd_fields import (
    reconcile_system_custom_fields as reconcile_system_custom_fields,
)
from backend.provision.cmcd_fields import (
    system_feature_flags as system_feature_flags,
)
from backend.provision.fos_setup import (
    _PROVISION_CLASS_A as _PROVISION_CLASS_A,
)
from backend.provision.fos_setup import (
    _get_fos_s3_client as _get_fos_s3_client,
)
from backend.provision.fos_setup import (
    _MockS3ClientForProvisioning as _MockS3ClientForProvisioning,
)
from backend.provision.fos_setup import (
    _ProvisioningPaginator as _ProvisioningPaginator,
)
from backend.provision.fos_setup import (
    _ProvisioningTracker as _ProvisioningTracker,
)
from backend.provision.fos_setup import (
    delete_fos_access_key,
    delete_fos_bucket,
    delete_fos_prefix,
    delete_fos_tokens_for_service,
    ensure_fos_access_key,
    ensure_fos_bucket,
)
from backend.provision.fos_setup import (
    find_fos_key as find_fos_key,
)
from backend.provision.fos_setup import (
    object_storage_enabled as object_storage_enabled,
)
from backend.provision.fos_setup import (
    product_enabled as product_enabled,
)
from backend.provision.rum_assets import (
    _NO_EXPECTATION as _NO_EXPECTATION,
)
from backend.provision.rum_assets import (
    _S3_LIST_XMLNS as _S3_LIST_XMLNS,
)
from backend.provision.rum_assets import (
    FARO_KEY_PREFIX as FARO_KEY_PREFIX,
)
from backend.provision.rum_assets import (
    FARO_KEY_SUFFIX as FARO_KEY_SUFFIX,
)
from backend.provision.rum_assets import (
    _faro_fos_key as _faro_fos_key,
)
from backend.provision.rum_assets import (
    _FaroPinChanged as _FaroPinChanged,
)
from backend.provision.rum_assets import (
    cleanup_old_faro_versions as cleanup_old_faro_versions,
)
from backend.provision.rum_assets import (
    delete_rum_tracker_js as delete_rum_tracker_js,
)
from backend.provision.rum_assets import (
    detect_faro_version_change as detect_faro_version_change,
)
from backend.provision.rum_assets import (
    download_and_upload_faro as download_and_upload_faro,
)
from backend.provision.rum_assets import (
    faro_bundle_intact as faro_bundle_intact,
)
from backend.provision.rum_assets import (
    faro_tracker_ready as faro_tracker_ready,
)
from backend.provision.rum_assets import (
    generate_rum_tracker_js as generate_rum_tracker_js,
)
from backend.provision.rum_assets import (
    sign_fos_request as sign_fos_request,
)
from backend.provision.rum_assets import (
    upload_rum_tracker_js as upload_rum_tracker_js,
)

logger = logging.getLogger(__name__)
_ORIG_DELETE_FOS_TOKENS = delete_fos_tokens_for_service

from backend.provision.declarative.reconciler import FeatureState, reconcile_vcl_state  # noqa: E402
from backend.provision.fastly_api import (  # noqa: E402
    delete_cdn_service,
    ensure_cdn_service,
    remove_logging_endpoint,
    validate_log_format,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _FARO_PIN_KEYS as _FARO_PIN_KEYS,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _RUM_CUSTOM_FIELDS as _RUM_CUSTOM_FIELDS,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _RUM_FIELD_NAMES as _RUM_FIELD_NAMES,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _purge_faro_surrogate_key as _purge_faro_surrogate_key,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _restore_faro_pin as _restore_faro_pin,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    _set_faro_version as _set_faro_version,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    disable_rum as disable_rum,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    enable_rum as enable_rum,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    legacy_rum_vcl_fingerprint as legacy_rum_vcl_fingerprint,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    rum_vcl_fingerprint as rum_vcl_fingerprint,
)
from backend.provision.rum_orchestrator_v2 import (  # noqa: E402
    upgrade_faro_version as upgrade_faro_version,
)
from backend.utils.terraform_gen import (  # noqa: E402
    _dump as _dump,
)
from backend.utils.terraform_gen import (  # noqa: E402
    _terraform_template_escape as _terraform_template_escape,
)
from backend.utils.terraform_gen import (  # noqa: E402
    generate_terraform as generate_terraform,
)


def _sync_crontab():
    """Refresh the background scheduler if running within the web app."""
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass


def _upload_faro_bundle_sync(service_id: str, version: str, token: str, *, status_cb=None):
    """Sync wrapper around the async ``download_and_upload_faro`` for use with ``run_with_events``."""
    import asyncio

    from backend.provision.rum_assets import download_and_upload_faro

    return asyncio.run(download_and_upload_faro(service_id, version, token, status_cb=status_cb))


def _reject_unsafe_fos_component(field: str, value: str, *, allow_slash: bool) -> None:
    """Reject path-traversal tokens in an FOS path component before persisting."""
    if not value:
        return
    bad = ("\\", "..", "\x00") if allow_slash else ("/", "\\", "..", "\x00")
    for tok in bad:
        if tok in value:
            raise ValueError(f"{field} contains an illegal path token {tok!r}")


def write_service_config(state: dict):
    """Write a service config JSON file to configs/{service_id}.json."""
    from backend import config as svcconfig

    service_id = state.get("logging_service_id") or state.get("service_id")
    if not service_id:
        raise ValueError("ingest state missing logging_service_id / service_id")
    db_path = svcconfig.duckdb_path(service_id)

    def _build_cfg(existing_cfg: dict) -> dict:
        fos_key = state.get("fos_access_key_id") or state.get("fos_access_key", "")
        fos_secret = state.get("fos_secret_access_key") or state.get("fos_secret_key", "")
        bucket = state.get("fos_bucket") or state.get("fos_bucket_name", "")
        fos_prefix = ""
        _reject_unsafe_fos_component("fos_bucket", bucket, allow_slash=False)
        region = state.get("fos_region", "us-east-1")
        cdn_url = state.get("cdn_url", "")
        incoming_lf = dict(state.get("log_fields") or {})
        incoming_custom = incoming_lf.get("custom_fields")
        existing_custom = list((existing_cfg.get("log_fields") or {}).get("custom_fields") or [])
        if not incoming_custom and existing_custom:
            incoming_lf["custom_fields"] = lf._filter_user_custom_fields(existing_custom)
        else:
            incoming_lf["custom_fields"] = lf._filter_user_custom_fields(incoming_lf.get("custom_fields", []))
        import datetime as _dt

        created_at = existing_cfg.get("created_at") or _dt.datetime.now(_dt.UTC).isoformat()
        cfg = {
            "service_id": service_id,
            "name": state.get("name") or state.get("service_name") or service_id,
            "created_at": created_at,
            "storage_mode": state.get("storage_mode", "cloud"),
            "fos_endpoint": region_endpoint(region),
            "fos_access_key_id": fos_key,
            "fos_secret_access_key": fos_secret,
            "fos_bucket": bucket,
            "fos_prefix": fos_prefix,
            "raw_layout_version": 3,
            "fos_region": region,
            "cdn_url": cdn_url,
            "cdn_secret": state.get("cdn_secret", ""),
            "cdn_service_id": state.get("cdn_service_id", ""),
            "fastly_api_key": state.get("fastly_api_key") or state.get("admin_token", ""),
            "log_retention_days": int(state.get("log_retention_days", 30)),
            "duckdb_path": db_path,
            "log_fields": incoming_lf,
        }
        for preserved_key in ("scoring", "cmcd", "ngwaf_workspace_id", "rum"):
            if preserved_key not in state and preserved_key in existing_cfg:
                cfg[preserved_key] = existing_cfg[preserved_key]
            elif preserved_key in state:
                cfg[preserved_key] = state[preserved_key]
        logging_enabled = state.get("logging_enabled")
        if logging_enabled is None:
            logging_enabled = existing_cfg.get("logging_enabled", True)
        rum_enabled = state.get("rum_enabled")
        if rum_enabled is None:
            rum_enabled = existing_cfg.get("rum_enabled", False)
        cfg["logging_enabled"] = bool(logging_enabled)
        cfg["rum_enabled"] = bool(rum_enabled)
        if cfg["rum_enabled"] and not cfg.get("rum"):
            import datetime as _dt

            cfg["rum"] = {"enabled": True, "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")}
        if "log_period" in state:
            cfg["log_period"] = state["log_period"]
        elif "log_period" in state.get("provisioning", {}):
            cfg["log_period"] = state["provisioning"]["log_period"]
        log_period_secs = int(cfg.get("log_period", 120))
        sync_interval_seconds = max(5, log_period_secs)
        commit_interval_mins = max(max(1, sync_interval_seconds // 60), int(state.get("commit_interval_mins", 5)))
        _rate_limiting = state.get("rate_limiting")
        if _rate_limiting is None:
            _rate_limiting = state.get("provisioning", {}).get("rate_limiting")
        if _rate_limiting is None:
            _rate_limiting = (existing_cfg.get("provisioning") or {}).get("rate_limiting")
        rate_limiting_val = True if _rate_limiting is None else bool(_rate_limiting)
        prov_in = state.get("provisioning", {})
        cron_sync_in = prov_in.get("cron_sync", {})
        cron_compact_in = prov_in.get("cron_compact", {})

        def _int_opt(key: str, default: int, *, sub: dict = cron_sync_in) -> int:
            return int(state.get(key, sub.get(key, default)))

        cfg["provisioning"] = {
            "endpoint_name": prov_in.get("endpoint_name", "Fastly Object Storage Logs"),
            "cdn_service_id": state.get("cdn_service_id", prov_in.get("cdn_service_id", "")),
            "cdn_url": cdn_url,
            "cdn_shield": state.get("cdn_shield", prov_in.get("cdn_shield", "")),
            "sample_rate": state.get("sample_rate", prov_in.get("sample_rate", "100")),
            "edge_only": state.get("edge_only", prov_in.get("edge_only", True)),
            "custom_condition": state.get("custom_condition", prov_in.get("custom_condition", "")),
            "rate_limiting": rate_limiting_val,
            "cron_sync": {
                "enabled": state.get("enable_cron_sync", cron_sync_in.get("enabled", True)),
                "delete_after": state.get("delete_after", cron_sync_in.get("delete_after", True)),
                "interval_seconds": sync_interval_seconds,
                "commit_interval_mins": commit_interval_mins,
                "log_enabled": True,
                "log_retention_days": int(state.get("log_retention_days", 30)),
                "data_retention_days": _int_opt("data_retention_days", 30),
                "rum_retention_days": _int_opt("rum_retention_days", 30),
                "cache_retention_days": _int_opt("cache_retention_days", 90),
                "rollup_retention_months": _int_opt("rollup_retention_months", 12),
            },
            "cron_compact": {
                "enabled": state.get("enable_cron_compact", cron_compact_in.get("enabled", True)),
                "interval_mins": 1440,
                "log_enabled": True,
                "log_retention_days": int(state.get("log_retention_days", 30)),
            },
            "temp_admin_key_id": state.get("temp_admin_key_id", prov_in.get("temp_admin_key_id")),
        }
        return cfg

    def _apply(target: dict) -> None:
        target.update(_build_cfg(target))

    with svcconfig.config_lock(service_id):
        if svcconfig.mutate_config(service_id, _apply) is None:
            first_cfg: dict = {}
            _apply(first_cfg)
            svcconfig.save_config(service_id, first_cfg)
    ok(f"Service config written → configs/{service_id}.json")


class _PreflightError(Exception):
    pass


def _state_file_path() -> str:
    """Absolute path to the provisioning resume-state file."""
    from backend import config as svcconfig

    svcconfig._ensure_dirs()
    return str(svcconfig.SYSTEM_DATA_DIR / "setup-state.json")


def save_state(state: dict):
    try:
        with open(_state_file_path(), "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.debug("Failed to save provisioning state: %s", e)


def load_state() -> dict:
    try:
        path = _state_file_path()
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception as e:
        logger.debug("Failed to load provisioning state: %s", e)
    return {}


def ensure_logging_via_reconciler(state: dict, token: str, status_cb=None) -> int:
    """Apply logging and VCL configuration via declarative reconciliation."""
    from backend.core.fastly.client import get_active_version
    from backend.provision.cli import info, ok

    service_id = state.get("logging_service_id")
    if not service_id:
        raise ValueError("state missing logging_service_id")
    info("Reconciling VCL state via declarative model…")
    if status_cb:
        status_cb(f"🔄 Reconciling VCL state for {service_id}...")
    state_for_feature = dict(state)
    state_for_feature["service_id"] = service_id
    try:
        FeatureState.from_config(state_for_feature)
    except (KeyError, ValueError) as e:
        raise RuntimeError(f"Invalid feature state configuration: {e}") from e
    try:
        result = reconcile_vcl_state(service_id, token, dry_run=False, status_cb=status_cb)
    except Exception as e:
        raise RuntimeError(f"VCL reconciliation failed: {e}") from e
    if result.activated_version is None:
        active_version = get_active_version(service_id, token)
        if active_version is None:
            raise RuntimeError(f"Service {service_id} has no active version")
        ok(f"Logging configuration already in desired state (version {active_version})")
        if status_cb:
            status_cb("✅ Logging configuration already in desired state.")
        return active_version
    else:
        ok(f"Logging and VCL configuration deployed (version {result.activated_version})")
        if status_cb:
            status_cb("✅ Logging and VCL configuration deployed.")
        return result.activated_version


def provision(cfg: dict, _resume_from_state: bool = False):
    token = cfg["admin_token"]
    state = load_state() if _resume_from_state else {}
    state.update(cfg)
    config_existed = False
    _check_service_id = cfg.get("logging_service_id")
    if _check_service_id:
        try:
            from backend import config as svcconfig

            if os.path.exists(svcconfig.config_path(_check_service_id)):
                config_existed = True
        except Exception:
            pass
    if cfg.get("fos_access_key_id") and cfg.get("fos_secret_access_key"):
        state.setdefault("fos_access_key", cfg["fos_access_key_id"])
        state.setdefault("fos_secret_key", cfg["fos_secret_access_key"])
        state.setdefault("fos_key_id", cfg["fos_access_key_id"])
    total = 8
    try:
        yield {"type": "progress", "current": 0, "total": total}
        using_falco = bool(shutil.which("falco"))
        validator = "falco" if using_falco else "built-in checks"
        yield {"type": "status", "message": f"✈️ Step 1/8: Pre-flight validation ({validator})..."}
        step(1, total, f"Pre-flight validation ({validator})")
        fmt_errors = validate_log_format(cfg.get("log_fields"))
        if fmt_errors:
            for err in fmt_errors:
                yield {"type": "status", "message": f"  ERROR: {err}"}
            raise _PreflightError(f"Log format validation failed ({len(fmt_errors)} error(s)).")
        yield {"type": "status", "message": "✅ Pre-flight: Log format OK."}
        yield {"type": "progress", "current": 1, "total": total}
        yield {"type": "status", "message": "🔑 Step 2/8: Creating temporary Admin Fastly Object Storage key..."}
        step(2, total, "Creating temporary Admin Fastly Object Storage key")
        temp_desc = f"fos-log-analysis-temp-admin-{cfg['logging_service_id']}"
        temp_key = yield from run_with_events(
            ensure_fos_access_key, temp_desc, state, token, permission="read-write-admin"
        )
        state["temp_admin_access_key"] = temp_key["access_key"]
        state["temp_admin_secret_key"] = temp_key["secret_key"]
        state["temp_admin_key_id"] = temp_key["id"]
        save_state(state)
        yield {"type": "progress", "current": 2, "total": total}
        yield {"type": "status", "message": "🪣 Step 3/8: Creating Fastly Object Storage bucket..."}
        step(3, total, "Creating Fastly Object Storage bucket")
        yield from run_with_events(
            ensure_fos_bucket,
            cfg["fos_bucket_name"],
            cfg["fos_region"],
            state["temp_admin_access_key"],
            state["temp_admin_secret_key"],
            service_id=cfg["logging_service_id"],
        )
        save_state(state)
        yield {"type": "progress", "current": 3, "total": total}
        yield {"type": "status", "message": "🔐 Step 4/8: Creating permanent Scoped Fastly Object Storage key..."}
        step(4, total, "Creating permanent Scoped Fastly Object Storage key")
        key_desc = f"fos-log-analysis-{cfg['logging_service_id']}"
        key = yield from run_with_events(
            ensure_fos_access_key,
            key_desc,
            state,
            token,
            permission="read-write-objects",
            buckets=[cfg["fos_bucket_name"]],
        )
        state["fos_access_key"] = key["access_key"]
        state["fos_secret_key"] = key["secret_key"]
        state["fos_key_id"] = key["id"]
        state["fos_key_description"] = key_desc
        save_state(state)
        try:
            write_service_config(state)
            ok("Service config persisted early (FOS ready) — survives stream interruption")
        except Exception as early_cfg_exc:  # noqa: BLE001
            warn(f"Early service-config write failed (non-fatal, retried at finalize): {early_cfg_exc}")
        yield {"type": "progress", "current": 4, "total": total}
        yield {"type": "status", "message": "🧹 Step 5/8: Cleaning up temporary admin key..."}
        step(5, total, "Cleaning up temporary admin key")
        if state.get("temp_admin_key_id"):
            yield from run_with_events(delete_fos_access_key, state["temp_admin_key_id"], token)
            for k in ["temp_admin_access_key", "temp_admin_secret_key", "temp_admin_key_id"]:
                state.pop(k, None)
            save_state(state)
        else:
            ok("Temporary key already removed")
        yield {"type": "progress", "current": 5, "total": total}
        yield {
            "type": "status",
            "message": "🌐 Step 6/8: Creating Fastly CDN service to front Fastly Object Storage...",
        }
        step(6, total, "Creating Fastly CDN service to front Fastly Object Storage")

        def _record_cdn_service_id(svc_id):
            state["cdn_service_id"] = svc_id
            save_state(state)

        cdn_svc = yield from run_with_events(
            ensure_cdn_service,
            cfg,
            state["fos_access_key"],
            state["fos_secret_key"],
            token,
            on_created=_record_cdn_service_id,
        )
        state["cdn_service_id"] = cdn_svc["id"]
        if cdn_svc.get("rate_limiting") is not None:
            state["rate_limiting"] = cdn_svc["rate_limiting"]
        save_state(state)
        yield {"type": "progress", "current": 6, "total": total}
        yield {"type": "status", "message": "📡 Step 7/8: Reconciling logging configuration via declarative model..."}
        step(7, total, "Reconciling logging configuration")
        write_service_config(state)
        ok("Service config written (for reconciler)")
        if state.get("rum_enabled"):
            rum_state_raw = state.get("rum")
            rum_state_cfg = rum_state_raw if isinstance(rum_state_raw, dict) else {}
            resolved_faro_version = rum_state_cfg.get("faro_version") or DEFAULT_FARO_VERSION
            yield {"type": "status", "message": f"⏳ Uploading Faro Web SDK v{resolved_faro_version}..."}
            try:
                yield from run_with_events(
                    _upload_faro_bundle_sync, state["logging_service_id"], resolved_faro_version, token
                )
            except Exception as faro_exc:  # noqa: BLE001
                logger.warning(
                    "[provision] Faro bundle upload failed for %s (non-fatal — "
                    "the RUM sync cron self-heals it on its next tick): %s",
                    state.get("logging_service_id"),
                    faro_exc,
                    exc_info=True,
                )
                warn(f"Faro Web SDK upload failed (non-fatal, cron will restore it): {faro_exc}")
                yield {
                    "type": "status",
                    "message": (
                        f"⚠ Faro Web SDK upload failed (non-fatal): {faro_exc}. "
                        "The RUM sync cron will restore the bundle automatically."
                    ),
                }
            else:
                from backend import config as svcconfig

                refreshed_cfg = svcconfig.load_config(state["logging_service_id"]) or {}
                refreshed_rum = refreshed_cfg.get("rum")
                if isinstance(refreshed_rum, dict):
                    state["rum"] = refreshed_rum
                else:
                    rum_state_cfg = dict(rum_state_cfg)
                    rum_state_cfg["faro_version"] = resolved_faro_version
                    state["rum"] = rum_state_cfg
                write_service_config(state)
                ok(f"Faro Web SDK v{resolved_faro_version} uploaded (for reconciler)")
        new_ver = yield from run_with_events(ensure_logging_via_reconciler, state, token)
        state["activated_logging_version"] = new_ver
        save_state(state)
        yield {"type": "progress", "current": 7, "total": total}
        yield {"type": "status", "message": "⚙️ Step 8/8: Finalizing service configuration..."}
        step(8, total, "Finalizing configuration")
        write_service_config(state)
        if is_mock_mode():
            yield {"type": "status", "message": "⚠ Skipping DuckLake initialization in mock mode."}
            yield {"type": "progress", "current": 8, "total": total}
            yield {"type": "done", "message": "🎉 Provisioning complete!"}
            return
        try:
            from backend.core import duckdb as db
            from backend.core import storage as db_storage

            src = db.get_source_for_service(state["logging_service_id"])
            if not src:
                logger.warning(
                    "[provision] %s: no source resolved after config write — "
                    "DuckLake table will be created lazily on the first commit",
                    state.get("logging_service_id"),
                )
                warn("DuckLake table not initialized yet (no source resolved) — created on first commit")
                yield {
                    "type": "status",
                    "message": "⚠ DuckLake table not initialized yet (no source resolved) — "
                    "it will be created on the first commit.",
                }
            else:
                yield {"type": "status", "message": "🧊 Initializing DuckLake table..."}
                from tenacity import Retrying, retry_if_exception_type, stop_after_attempt

                _ICE_INIT_ATTEMPTS = 3
                ice_exc: Exception | None = None
                try:
                    for attempt in Retrying(
                        stop=stop_after_attempt(_ICE_INIT_ATTEMPTS),
                        retry=retry_if_exception_type(OSError),
                        reraise=True,
                    ):
                        with attempt:
                            idx = attempt.retry_state.attempt_number
                            if idx > 1:
                                yield {
                                    "type": "status",
                                    "message": (
                                        f"… Object Storage not ready yet "
                                        f"(attempt {idx - 1}/{_ICE_INIT_ATTEMPTS}); retrying…"
                                    ),
                                }
                                time.sleep(1.5 * (idx - 1))
                            db_storage.init_storage_table(src)
                except Exception as e:
                    ice_exc = e
                if ice_exc is None:
                    ok("DuckLake table ready")
                    yield {"type": "status", "message": "✓ DuckLake table ready."}
                else:
                    logger.error(
                        "[provision] DuckLake table init failed after %d attempt(s) (deferred to first commit)",
                        _ICE_INIT_ATTEMPTS,
                        exc_info=ice_exc,
                    )
                    warn(f"DuckLake table init deferred to first commit: {ice_exc}")
                    yield {"type": "status", "message": f"⚠ DuckLake table init deferred to first commit: {ice_exc}"}
                c = db.get_connection(source=src)
                initial_lf = state.get("log_fields", {})
                if initial_lf:
                    details = {
                        "preset": initial_lf.get("preset"),
                        "groups_before": [],
                        "groups_after": sorted(initial_lf.get("groups", [])),
                        "fields_added": [],
                        "fields_removed": [],
                        "format_hash": lf.format_hash(initial_lf),
                    }
                    from backend.core import metadata as metadata_db

                    metadata_db.record_audit(src["name"], "log_format_change", details, actor="provisioning")
                c.close()
        except Exception:
            logger.exception("[provision] post-provision DuckLake/audit step failed (non-fatal)")
        yield {"type": "progress", "current": 8, "total": total}
        yield {"type": "done", "message": "🎉 Provisioning complete!"}
    except _PreflightError as e:
        yield {"type": "error", "message": str(e)}
        return
    except Exception as e:
        yield {"type": "status", "message": f"Provisioning failed: {str(e)}. Starting full cleanup rollback..."}
        yield from perform_teardown(state, token)
        service_id = state.get("logging_service_id")
        if service_id and not config_existed:
            from backend import config as svcconfig

            cfg_path = svcconfig.config_path(service_id)
            if os.path.exists(cfg_path):
                os.remove(cfg_path)
        yield {"type": "error", "message": str(e)}
        raise e


def perform_teardown(state: dict, token: str, opts: dict | None = None):
    if opts is None:
        opts = {
            "remove_logging": True,
            "remove_rum": True,
            "remove_cdn": True,
            "remove_bucket": True,
            "remove_scoring": True,
            "remove_fos_tokens": True,
        }
    total_steps = 5
    yield {"type": "progress", "current": 1, "total": total_steps}
    scoring_meta = state.get("scoring") or {}
    do_scoring = opts.get("remove_scoring", True) and (
        scoring_meta.get("enabled") or state.get("scoring_enabled") or bool(scoring_meta.get("scoring_service_id"))
    )
    step(1, total_steps, "Tearing down session scoring" if do_scoring else "Skipping session scoring teardown")
    if do_scoring:
        try:
            from backend.provision.session_scoring_orchestrator import teardown_scoring_resources

            failed = yield from run_with_events(
                teardown_scoring_resources, state["logging_service_id"], scoring_meta, token
            )
            for label, store_id in failed or []:
                yield {
                    "type": "status",
                    "message": f"Warning: scoring {label} store {store_id} not deleted — remove it manually.",
                }
        except Exception as exc:
            yield {
                "type": "status",
                "message": (
                    f"❌ Scoring teardown ABORTED (Compute service left in place on purpose): {exc} "
                    "Re-run teardown after resolving; continuing with the remaining steps."
                ),
            }
    yield {"type": "progress", "current": 2, "total": total_steps}
    step(
        2,
        total_steps,
        "Removing logging endpoint" if opts.get("remove_logging") else "Skipping logging endpoint removal",
    )
    endpoint_name = state.get("provisioning", {}).get("endpoint_name") or state.get("endpoint_name", "")
    if (opts.get("remove_logging") or opts.get("remove_rum")) and state.get("logging_service_id") and endpoint_name:
        try:
            yield from run_with_events(
                remove_logging_endpoint,
                state["logging_service_id"],
                endpoint_name,
                token,
                remove_logging=opts.get("remove_logging", True),
                remove_rum=opts.get("remove_rum", True),
            )
        except Exception as exc:
            yield {"type": "status", "message": f"Warning: {exc}"}
    yield {"type": "progress", "current": 3, "total": total_steps}
    step(
        3,
        total_steps,
        "Deleting FOS access keys" if opts.get("remove_fos_tokens") else "Skipping FOS access keys deletion",
    )
    if opts.get("remove_fos_tokens"):
        try:
            from backend.provision import fos_setup as _fos_setup_mod

            _del_tokens = (
                delete_fos_tokens_for_service
                if delete_fos_tokens_for_service is not _ORIG_DELETE_FOS_TOKENS
                else _fos_setup_mod.delete_fos_tokens_for_service
            )
            yield from run_with_events(_del_tokens, state["logging_service_id"], token)
        except Exception:
            pass
    yield {"type": "progress", "current": 4, "total": total_steps}
    step(4, total_steps, "Deleting FOS bucket" if opts.get("remove_bucket") else "Skipping FOS bucket deletion")
    remove_bucket = bool(opts.get("remove_bucket"))
    remove_cloud_files = not remove_bucket and bool(opts.get("remove_cloud_files"))
    if (remove_bucket or remove_cloud_files) and state.get("fos_bucket_name"):
        try:
            temp_desc = f"fos-log-analysis-temp-teardown-{state.get('logging_service_id', 'unknown')}"
            temp_key = yield from run_with_events(
                ensure_fos_access_key, temp_desc, {}, token, permission="read-write-admin"
            )
            try:
                if remove_bucket:
                    yield from run_with_events(
                        delete_fos_bucket,
                        state["fos_bucket_name"],
                        state["fos_region"],
                        temp_key["access_key"],
                        temp_key["secret_key"],
                        service_id=state.get("logging_service_id"),
                    )
                else:
                    prefix = state.get("fos_prefix", "")
                    yield from run_with_events(
                        delete_fos_prefix,
                        state["fos_bucket_name"],
                        state["fos_region"],
                        temp_key["access_key"],
                        temp_key["secret_key"],
                        prefix,
                        service_id=state.get("logging_service_id"),
                    )
            finally:
                yield from run_with_events(delete_fos_access_key, temp_key["id"], token)
        except Exception as exc:
            if remove_cloud_files:
                yield {"type": "status", "message": f"Warning: failed to delete cloud files: {exc}"}
    yield {"type": "progress", "current": 5, "total": total_steps}
    step(5, total_steps, "Deleting CDN service" if opts.get("remove_cdn") else "Skipping CDN service deletion")
    if (
        opts.get("remove_cdn")
        and state.get("cdn_service_id")
        and state.get("cdn_service_name")
        and state.get("cdn_service_id") != state.get("logging_service_id")
    ):
        try:
            yield from run_with_events(delete_cdn_service, state["cdn_service_id"], state["cdn_service_name"], token)
        except Exception:
            pass
    elif opts.get("remove_cdn") and state.get("cdn_service_id") == state.get("logging_service_id"):
        logger.error(
            "[provision] refusing to delete CDN service %s because it is the logging service",
            state.get("cdn_service_id"),
        )
        yield {
            "type": "status",
            "message": (
                f"Refusing to delete CDN service {state.get('cdn_service_id')}: it matches the logging service."
            ),
        }


_DUCKLAKE_DROP_TIMEOUT_S = 60.0


def _drop_ducklake_state_bounded(service_id: str) -> None:
    """Drop the service's DuckLake tables and local data, never blocking teardown on a hung catalog."""
    from backend.core.duckdb import drop_service_ducklake_state

    outcome: dict[str, Any] = {}

    def _run() -> None:
        try:
            outcome["result"] = drop_service_ducklake_state(service_id)
        except Exception as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=_run, name=f"teardown-ducklake-{service_id}", daemon=True)
    worker.start()
    worker.join(_DUCKLAKE_DROP_TIMEOUT_S)
    if worker.is_alive():
        warn(f"DuckLake cleanup for {service_id} timed out after {_DUCKLAKE_DROP_TIMEOUT_S:.0f}s; local data kept")
    elif "error" in outcome:
        warn(f"DuckLake cleanup for {service_id} failed: {outcome['error']}")
    else:
        result = outcome["result"]
        ok(f"Dropped {result['tables_dropped']} DuckLake table(s) and removed local DuckLake data")


def cleanup_local_data(service_id: str, bucket: str = None, remove_data: bool = False):
    """Remove local config files, database, and cache associated with a service."""
    from backend import config as svcconfig

    if service_id and remove_data:
        _drop_ducklake_state_bounded(service_id)
    if service_id:
        cfg_path = svcconfig.config_path(service_id)
        if os.path.exists(cfg_path):
            os.remove(cfg_path)
            ok(f"Removed service config: {cfg_path}")
    if remove_data:
        db_path = (
            svcconfig.duckdb_path(service_id)
            if service_id
            else os.path.join(os.path.dirname(__file__), "..", "data", "logs.duckdb")
        )
        rum_db_path = rum_duckdb_path(db_path)
        for target_db, label in ((db_path, "local database"), (rum_db_path, "local RUM database")):
            if os.path.exists(target_db):
                os.remove(target_db)
                wal = target_db + ".wal"
                if os.path.exists(wal):
                    os.remove(wal)
                ok(f"Removed {label}: {target_db}")
        if service_id:
            try:
                from backend.core import metadata as metadata_db

                metadata_db.teardown(service_id)
                ok(f"Cleaned up metadata for {service_id}")
            except Exception:
                pass
        if bucket:
            if any(c in bucket for c in ("/", "\\", "..", "\x00")):
                logger.warning("[teardown] refusing to remove cache for bucket=%r with path-shape characters", bucket)
            else:
                for base in [os.getcwd(), os.path.join(os.path.dirname(__file__), "..", "..")]:
                    cache_root = os.path.realpath(os.path.join(base, "cache"))
                    svc_cache_dir = os.path.realpath(os.path.join(cache_root, bucket))
                    try:
                        common = os.path.commonpath([cache_root, svc_cache_dir])
                    except ValueError:
                        continue
                    if common != cache_root:
                        logger.warning(
                            "[teardown] refusing to remove cache: resolved path %s escapes %s",
                            svc_cache_dir,
                            cache_root,
                        )
                        continue
                    if os.path.exists(svc_cache_dir):
                        shutil.rmtree(svc_cache_dir)
                        ok(f"Removed local cache: {svc_cache_dir}")
    _sync_crontab()


def _build_log_fields_config(args) -> dict:
    preset_name = getattr(args, "preset", None) or "standard"
    preset = lf.PRESETS.get(preset_name)
    if preset is None:
        raise ValueError(f"Unknown log-fields preset: {preset_name!r}")
    groups = list(preset["groups"])
    for g in getattr(args, "enable_group", None) or []:
        if g not in groups:
            groups.append(g)
    for g in getattr(args, "disable_group", None) or []:
        if g in groups:
            groups.remove(g)
    field_overrides = dict(preset.get("field_overrides") or {})
    field_overrides.update({fid: True for fid in (getattr(args, "enable_field", None) or [])})
    field_overrides.update({fid: False for fid in (getattr(args, "disable_field", None) or [])})
    return {"schema_version": 2, "preset": preset_name, "groups": sorted(groups), "field_overrides": field_overrides}
