import json
import logging
import os
import queue
import shutil
import threading
import time

logger = logging.getLogger(__name__)

from backend.core import field_registry as lf
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.fastly.client import fastly
from backend.core.fastly.mock_fixtures import is_mock_mode
from backend.core.fastly.utils import (
    region_endpoint,
)
from backend.provision.declarative.reconciler import reconcile_vcl_state
from backend.provision.declarative.state import FeatureState
from backend.provision.fastly_api import (
    delete_cdn_service,
    ensure_cdn_service,
    remove_logging_endpoint,
    validate_log_format,
)
from backend.provision.fos_setup import (
    delete_fos_access_key,
    delete_fos_bucket,
    ensure_fos_access_key,
    ensure_fos_bucket,
)
from backend.provision.utils import ok, step, warn


def _sync_crontab():
    """Refresh the background scheduler if running within the web app."""
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass


def _upload_faro_bundle_sync(service_id: str, version: str, token: str, *, status_cb=None):
    """Sync wrapper around the async ``download_and_upload_faro`` for use with
    ``run_with_events`` (which calls its target as a plain sync callable with
    a ``status_cb`` kwarg, same pattern as every other provisioning step)."""
    import asyncio

    from backend.provision.rum_assets import download_and_upload_faro

    return asyncio.run(download_and_upload_faro(service_id, version, token, status_cb=status_cb))


def _reject_unsafe_fos_component(field: str, value: str, *, allow_slash: bool) -> None:
    """Reject path-traversal tokens in an FOS path component before persisting.

    L8: ``fos_bucket`` composes into the local cache path (``cache/<bucket>``),
    so a value like ``../../tmp`` would escape the cache root when DuckDB / the
    teardown path later joins it. The ``/execute`` + teardown paths already
    reject these tokens; the persist path (ingest / register_existing →
    write_service_config) did not. Buckets never contain ``/``; ``fos_prefix``
    is an S3 key prefix that legitimately may, so ``/`` is allowed there while
    ``..`` / backslash / NUL are rejected everywhere.
    """
    if not value:
        return
    bad = ("\\", "..", "\x00") if allow_slash else ("/", "\\", "..", "\x00")
    for tok in bad:
        if tok in value:
            raise ValueError(f"{field} contains an illegal path token {tok!r}")


def write_service_config(state: dict):
    """Write a service config JSON file to configs/{service_id}.json.

    PRESERVE-ON-RE-RUN: this function is called from /api/provision/ingest
    (analyst-join, wizard re-run, key rotation). The ``state`` dict is the
    request body — it has no awareness of code-managed keys that
    ``enable_scoring`` / ``ngwaf_workspace_id`` PATCH / log_fields PATCH
    may have injected into the existing config. Without preserving those
    keys, re-running the wizard silently strips ``cfg["scoring"]``,
    ``cfg["log_fields"]["custom_fields"]``, and ``cfg["ngwaf_workspace_id"]``
    — same bug class as the 2026-06-02 state_sync incident, just with the
    request body as the stale-overwriter instead of FOS admin_state.json.
    """
    from backend import config as svcconfig

    service_id = state.get("logging_service_id") or state.get("service_id")
    if not service_id:
        raise ValueError("ingest state missing logging_service_id / service_id")
    db_path = svcconfig.duckdb_path(service_id)

    # Snapshot the existing on-disk cfg so we can preserve code-managed
    # keys that the request body doesn't carry. None on first-ever ingest
    # (which is fine — there's nothing to preserve).
    existing_cfg = svcconfig.load_config(service_id) or {}

    fos_key = state.get("fos_access_key_id") or state.get("fos_access_key", "")
    fos_secret = state.get("fos_secret_access_key") or state.get("fos_secret_key", "")
    bucket = state.get("fos_bucket") or state.get("fos_bucket_name", "")
    fos_prefix = state.get("fos_prefix", "")
    # L8: validate path-shape before persisting (bucket composes into the
    # local cache root; prefix checked defensively for traversal).
    _reject_unsafe_fos_component("fos_bucket", bucket, allow_slash=False)
    _reject_unsafe_fos_component("fos_prefix", fos_prefix, allow_slash=True)
    region = state.get("fos_region", "us-east-1")
    cdn_url = state.get("cdn_url", "")

    # Build log_fields: prefer the request body, but if the request body
    # omits custom_fields (or sends an empty list) AND we have existing
    # custom_fields on disk, preserve them. System fields (CMCD, Scoring)
    # are generated on-demand by FeatureState.from_config() and are NOT
    # persisted in the config file.
    incoming_lf = dict(state.get("log_fields") or {})
    incoming_custom = incoming_lf.get("custom_fields")
    existing_custom = list((existing_cfg.get("log_fields") or {}).get("custom_fields") or [])
    if not incoming_custom and existing_custom:
        # Filter out system fields from existing custom_fields
        incoming_lf["custom_fields"] = lf._filter_user_custom_fields(existing_custom)
    else:
        # Filter out system fields from incoming custom_fields
        incoming_lf["custom_fields"] = lf._filter_user_custom_fields(incoming_lf.get("custom_fields", []))

    import datetime as _dt

    created_at = existing_cfg.get("created_at") or _dt.datetime.now(_dt.UTC).isoformat()

    cfg = {
        "service_id": service_id,
        "name": state.get("name") or state.get("service_name") or service_id,
        "created_at": created_at,
        "access_level": state.get("access_level", "read_write"),
        "storage_mode": state.get("storage_mode", "cloud"),
        "fos_endpoint": region_endpoint(region),
        "fos_access_key_id": fos_key,
        "fos_secret_access_key": fos_secret,
        "fos_bucket": bucket,
        "fos_prefix": fos_prefix,
        "fos_region": region,
        "cdn_url": cdn_url,
        "cdn_secret": state.get("cdn_secret", ""),
        "cdn_service_id": state.get("cdn_service_id", ""),
        "fastly_api_key": state.get("fastly_api_key") or state.get("admin_token", ""),
        "log_retention_days": int(state.get("log_retention_days", 30)),
        "duckdb_path": db_path,
        "log_fields": incoming_lf,
    }

    # Preserve code-managed top-level keys that the request body doesn't
    # carry — primarily ``scoring`` (set by enable_scoring) and
    # ``ngwaf_workspace_id`` (set by the NGWAF-config PATCH). Anything else
    # the existing cfg has that the wizard body lacks survives the rewrite.
    for preserved_key in ("scoring", "cmcd", "ngwaf_workspace_id", "rum"):
        if preserved_key not in state and preserved_key in existing_cfg:
            cfg[preserved_key] = existing_cfg[preserved_key]
        elif preserved_key in state:
            cfg[preserved_key] = state[preserved_key]

    # Resolve features selection (logging_enabled, rum_enabled)
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

        cfg["rum"] = {
            "enabled": True,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        }

    if "log_period" in state:
        cfg["log_period"] = state["log_period"]
    elif "log_period" in state.get("provisioning", {}):
        cfg["log_period"] = state["provisioning"]["log_period"]

    # Honor the operator's chosen log_period as the sync cadence, with a hard
    # 5s floor — matching the scheduler's own ``max(5, …)`` clamp and the
    # settings-PATCH writer (which never applied a 30s floor). Picking a sub-30s
    # period is an explicit operator choice trading extra FOS LIST/GET cost for
    # dashboard freshness; it is no longer silently rounded up to 30s.
    log_period_secs = int(cfg.get("log_period", 120))
    sync_interval_seconds = max(5, log_period_secs)
    commit_interval_mins = max(max(1, sync_interval_seconds // 60), int(state.get("commit_interval_mins", 5)))

    # Account-level edge rate-limiting entitlement (set by ensure_cdn_service's
    # proactive probe). Prefer the freshly-detected state, then the carried-over
    # request provisioning block, then the on-disk value, defaulting True so the
    # cli.py:handle_update_cdn reader and reactive fallback behave as before when
    # never detected.
    _rate_limiting = state.get("rate_limiting")
    if _rate_limiting is None:
        _rate_limiting = state.get("provisioning", {}).get("rate_limiting")
    if _rate_limiting is None:
        _rate_limiting = (existing_cfg.get("provisioning") or {}).get("rate_limiting")
    rate_limiting_val = True if _rate_limiting is None else bool(_rate_limiting)

    cfg["provisioning"] = {
        "endpoint_name": state.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs"),
        "cdn_service_id": state.get("cdn_service_id", state.get("provisioning", {}).get("cdn_service_id", "")),
        "cdn_url": cdn_url,
        "cdn_shield": state.get("cdn_shield", state.get("provisioning", {}).get("cdn_shield", "")),
        "sample_rate": state.get("sample_rate", state.get("provisioning", {}).get("sample_rate", "100")),
        "edge_only": state.get("edge_only", state.get("provisioning", {}).get("edge_only", True)),
        "custom_condition": state.get("custom_condition", state.get("provisioning", {}).get("custom_condition", "")),
        "rate_limiting": rate_limiting_val,
        "cron_sync": {
            "enabled": state.get(
                "enable_cron_sync", state.get("provisioning", {}).get("cron_sync", {}).get("enabled", True)
            ),
            "delete_after": state.get(
                "delete_after", state.get("provisioning", {}).get("cron_sync", {}).get("delete_after", True)
            ),
            "interval_seconds": sync_interval_seconds,
            "commit_interval_mins": commit_interval_mins,
            "log_enabled": True,
            "log_retention_days": int(state.get("log_retention_days", 30)),
            "data_retention_days": int(
                state.get(
                    "data_retention_days",
                    state.get("provisioning", {}).get("cron_sync", {}).get("data_retention_days", 30),
                )
            ),
            "rum_retention_days": int(
                state.get(
                    "rum_retention_days",
                    state.get("provisioning", {}).get("cron_sync", {}).get("rum_retention_days", 90),
                )
            ),
            "cache_retention_days": int(
                state.get("provisioning", {}).get("cron_sync", {}).get("cache_retention_days", 90)
            ),
        },
        "cron_compact": {
            "enabled": state.get(
                "enable_cron_compact", state.get("provisioning", {}).get("cron_compact", {}).get("enabled", True)
            ),
            "interval_mins": 1440,
            "log_enabled": True,
            "log_retention_days": int(state.get("log_retention_days", 30)),
        },
        "temp_admin_key_id": state.get("temp_admin_key_id", state.get("provisioning", {}).get("temp_admin_key_id")),
    }
    svcconfig.save_config(service_id, cfg)
    ok(f"Service config written → configs/{service_id}.json")


def run_with_events(func, *args, raise_on_error: bool = True, **kwargs):
    q: queue.Queue[str | None] = queue.Queue()
    kwargs["status_cb"] = q.put
    res = []
    err = []

    def worker():
        try:
            res.append(func(*args, **kwargs))
        except Exception as e:
            err.append(e)
        finally:
            q.put(None)

    t = threading.Thread(target=worker)
    t.start()

    while True:
        msg = q.get()
        if msg is None:
            break
        yield {"type": "status", "message": msg}

    t.join()
    if err:
        if raise_on_error:
            raise err[0]
        yield {"type": "error", "message": str(err[0])}
        return None
    return res[0]


class _PreflightError(Exception):
    pass


def _state_file_path() -> str:
    """Absolute path to the provisioning resume-state file.

    Resolved from ``backend.config.SYSTEM_DATA_DIR`` at call time so tests
    that monkeypatch ``SYSTEM_DATA_DIR`` (see ``isolate_metadata_db``)
    redirect transparently. Historically this was a CWD-relative
    ``setup-state.json`` which leaked into whichever directory the wizard
    happened to be invoked from (notably ``frontend/`` under Playwright).
    """
    from backend import config as svcconfig

    svcconfig._ensure_dirs()
    return str(svcconfig.SYSTEM_DATA_DIR / "setup-state.json")


def save_state(state: dict):
    try:
        with open(_state_file_path(), "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def load_state() -> dict:
    try:
        path = _state_file_path()
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def ensure_logging_via_reconciler(state: dict, token: str, status_cb=None) -> int:
    """Apply logging and VCL configuration via declarative reconciliation.

    Replaces the imperative ensure_logging_endpoint by using reconcile_vcl_state
    to handle all VCL snippets, logging endpoints, backends, and custom fields.

    Args:
        state: Provisioning state dict containing config and credentials.
        token: Fastly API token.
        status_cb: Optional callback for status updates.

    Returns:
        Activated version number.

    Raises:
        RuntimeError: if reconciliation fails.
    """
    from backend.core.fastly.service import get_active_version
    from backend.provision.utils import info, ok

    service_id = state.get("logging_service_id")
    if not service_id:
        raise ValueError("state missing logging_service_id")

    info("Reconciling VCL state via declarative model…")
    if status_cb:
        status_cb(f"🔄 Reconciling VCL state for {service_id}...")

    # Normalize state dict: FeatureState.from_config() expects 'service_id' key,
    # but provisioning state uses 'logging_service_id'. Add both for compatibility.
    state_for_feature = dict(state)
    state_for_feature["service_id"] = service_id

    # Verify FeatureState can be built from config (validates all required fields)
    try:
        feature_state = FeatureState.from_config(state_for_feature)
    except (KeyError, ValueError) as e:
        raise RuntimeError(f"Invalid feature state configuration: {e}") from e

    # Call reconciler to apply desired state
    # The reconciler reads from configs/{service_id}.json which was already written
    try:
        result = reconcile_vcl_state(service_id, token, dry_run=False, status_cb=status_cb)
    except Exception as e:
        raise RuntimeError(f"VCL reconciliation failed: {e}") from e

    if result.activated_version is None:
        # No changes were needed (already in desired state)
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

        # Persist a readable service config NOW — the moment permanent FOS
        # creds + bucket exist — BEFORE the fallible CDN/logging steps and
        # the finalize write at step 8. The config write used to run ONLY as
        # the last step, and it sits AFTER a `yield` in this SSE-streamed
        # generator: if the wizard's stream was interrupted (client
        # disconnect, or the backend torn down mid-provision) any time after
        # the bucket was created, the generator was closed at that yield and
        # the config never landed — orphaning the FOS bucket with no config
        # the backend could read (telemetry-proxy "no config for service_id"
        # → FOS 403 → empty/"internally inconsistent" dashboard). Writing it
        # here makes the service readable as soon as FOS is set up; the
        # finalize write below remains the authoritative, rollback-gated
        # write that fills in cdn_service_id + the activated logging version.
        # Best-effort: a failure here must NOT abort the flow — the finalize
        # write is the real gate (and on a real error it triggers teardown).
        try:
            write_service_config(state)
            ok("Service config persisted early (FOS ready) — survives stream interruption")
        except Exception as early_cfg_exc:  # noqa: BLE001 — best-effort; finalize write is authoritative
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
            # Persist the CDN service id the moment it's created so a failure
            # later inside ensure_cdn_service (e.g. VCL validation) still leaves
            # it in state for perform_teardown to delete — no orphan service.
            # Runs on the worker thread while this generator is parked in
            # run_with_events; the queue + join there synchronise the write.
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
        # Persist the proactively-detected account rate-limiting entitlement only
        # when conclusive; a None (unknown) leaves the default-True read path +
        # reactive validate fallback intact.
        if cdn_svc.get("rate_limiting") is not None:
            state["rate_limiting"] = cdn_svc["rate_limiting"]
        save_state(state)
        yield {"type": "progress", "current": 6, "total": total}

        yield {"type": "status", "message": "📡 Step 7/8: Reconciling logging configuration via declarative model..."}
        step(7, total, "Reconciling logging configuration")

        # Write config early so reconciler can read it from configs/{service_id}.json
        write_service_config(state)
        ok("Service config written (for reconciler)")

        # RUM: upload the pinned Faro Web SDK bundle to FOS BEFORE the
        # reconciler below activates VCL that routes /js/faro-sdk.js to it
        # (F-2 audit finding). Without this, a fresh service provisioned
        # with rum_enabled + a pinned faro_version (the wizard always pins
        # one — see routers/provision.py) went live with VCL routing to an
        # object that was never uploaded: the tracker requests
        # /js/faro-sdk.js and gets a 404 forever, since there is no CDN
        # fallback. Mirrors the upload-before-reconcile ordering in
        # rum_orchestrator_v2.enable_rum. Resolves DEFAULT_FARO_VERSION here
        # too, defensively, in case a caller invokes provision() directly
        # with rum_enabled but no faro_version ever resolved upstream.
        if state.get("rum_enabled"):
            rum_state_raw = state.get("rum")
            rum_state_cfg = rum_state_raw if isinstance(rum_state_raw, dict) else {}
            resolved_faro_version = rum_state_cfg.get("faro_version") or DEFAULT_FARO_VERSION
            yield {"type": "status", "message": f"⏳ Uploading Faro Web SDK v{resolved_faro_version}..."}
            try:
                yield from run_with_events(
                    _upload_faro_bundle_sync, state["logging_service_id"], resolved_faro_version, token
                )
            except Exception as faro_exc:  # noqa: BLE001 — deliberately non-fatal, see below
                # Non-fatal (#3 audit finding): this upload used to sit
                # inside provision()'s try block, so an unpkg outage or a
                # transient FOS error here fell into the except below and
                # ran perform_teardown with remove_logging/remove_cdn/
                # remove_bucket/remove_fos_tokens all True — a third-party
                # CDN hiccup deleting the just-created CDN service, FOS
                # bucket, and FOS access keys. faro_version is already
                # pinned in config (write_service_config above, BEFORE this
                # upload), which is exactly the "pinned, bundle missing"
                # state the RUM sync cron's self-heal restore branch
                # (backend/cron/jobs/rum_sync.py::_reconcile_faro_bundle)
                # already converges on its next tick — so warning and
                # continuing is strictly safer than tearing down
                # freshly-provisioned infrastructure over it.
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
                # download_and_upload_faro already persisted faro_version +
                # faro_content_hash + faro_fos_etag_md5 into
                # configs/{service_id}.json itself (its own save_config
                # call in rum_assets.py). Re-read that rather than writing
                # back the pre-upload `rum_state_cfg` snapshot (#1 audit
                # finding) — the snapshot predates the upload and would
                # clobber the hashes it just wrote with a bare
                # {"faro_version": ...}, leaving the cron's cheap FOS
                # integrity check (_faro_bundle_intact) with no stored ETag
                # to compare against, forcing a redundant unpkg
                # download + FOS PUT + purge on the very next tick.
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

        try:
            from backend.core import duckdb as db
            from backend.core import iceberg as db_iceberg

            src = db.get_source_for_service(state["logging_service_id"])
            if not src:
                # No source config resolved (config race / missing fields). The
                # table can't be created here, but commit_buffer self-heals it on
                # the first commit. Surface it instead of skipping silently — a
                # swallowed failure here is exactly what shipped a fresh service
                # with no Iceberg table and a commit cron crashing every cycle.
                logger.warning(
                    "[provision] %s: no source resolved after config write — "
                    "Iceberg table will be created lazily on the first commit",
                    state.get("logging_service_id"),
                )
                warn("Iceberg table not initialized yet (no source resolved) — created on first commit")
                yield {
                    "type": "status",
                    "message": "⚠ Iceberg table not initialized yet (no source resolved) — "
                    "it will be created on the first commit.",
                }
            elif is_mock_mode():
                # Mock mode (e2e/contract backend): skip the FOS-backed table
                # init. _get_catalog → catalog.load_table/create_table does a
                # pyarrow HeadObject on metadata.json that, with no real FOS,
                # is routed through the telemetry proxy to the unreachable
                # endpoint and returns a synthetic 502 — slow + noisy + the
                # source of the provision-teardown e2e's intermittent 502s.
                # Real mode is unchanged (production never sets FASTLY_MOCK_MODE);
                # the table is created lazily on first commit regardless.
                ok("Iceberg table ready")
                yield {"type": "status", "message": "✓ Iceberg table init skipped (mock mode)."}
            else:
                yield {"type": "status", "message": "🧊 Initializing Iceberg table in Fastly Object Storage..."}
                # FOS occasionally returns a transient 5xx (observed: HTTP 500 →
                # OSError(121)) on the metadata HEAD that pyiceberg runs before
                # writing a new table's metadata.json. s3fs does NOT retry
                # generic 5xx, so a single blip would otherwise punt table
                # creation onto the first-commit fallback below. init_iceberg_
                # table is idempotent (per-service lock + load-or-create), so
                # retry a few times with a short backoff first — a momentary FOS
                # hiccup shouldn't surface during provisioning. Only I/O errors
                # (OSError: FOS 5xx / timeout / reset) are retried; a config or
                # programming error falls straight through to the fallback.
                _ICE_INIT_ATTEMPTS = 3
                ice_exc: Exception | None = None
                for _attempt in range(_ICE_INIT_ATTEMPTS):
                    try:
                        db_iceberg.init_iceberg_table(src)
                        ice_exc = None
                        break
                    except OSError as e:
                        ice_exc = e
                        if _attempt < _ICE_INIT_ATTEMPTS - 1:
                            yield {
                                "type": "status",
                                "message": (
                                    f"… Object Storage not ready yet "
                                    f"(attempt {_attempt + 1}/{_ICE_INIT_ATTEMPTS}); retrying…"
                                ),
                            }
                            time.sleep(1.5 * (_attempt + 1))
                    except Exception as e:
                        ice_exc = e
                        break
                if ice_exc is None:
                    ok("Iceberg table ready")
                    yield {"type": "status", "message": "✓ Iceberg table ready."}
                else:
                    # Best-effort: do NOT abort the wizard (commit_buffer creates
                    # the table on the first commit). But never swallow it
                    # silently — log with traceback + surface in the wizard so a
                    # broken fresh install is visible at provision time.
                    logger.error(
                        "[provision] Iceberg table init failed after %d attempt(s) (deferred to first commit)",
                        _ICE_INIT_ATTEMPTS,
                        exc_info=ice_exc,
                    )
                    warn(f"Iceberg table init deferred to first commit: {ice_exc}")
                    yield {
                        "type": "status",
                        "message": f"⚠ Iceberg table init deferred to first commit: {ice_exc}",
                    }
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
            # Non-fatal (the service is already provisioned); but log it so a
            # post-provision audit/connection failure isn't invisible.
            logger.exception("[provision] post-provision Iceberg/audit step failed (non-fatal)")

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
            "remove_cdn": True,
            "remove_bucket": True,
            "remove_scoring": True,
            "remove_fos_tokens": True,
        }

    total_steps = 5
    # ── TEARDOWN ORDER: DEREFERENCE, THEN DELETE. ────────────────────────────
    # Every step that removes VCL from the CUSTOMER's live service must complete
    # before any step that deletes a service that VCL points at. Otherwise the
    # customer's active version is left routing to a deleted host — its own
    # traffic breaks, and the failure is ours, not theirs.
    #
    #   1. scoring VCL strip + Compute delete  (strip is fatal-on-failure, so
    #      the Compute service is never deleted while still referenced)
    #   2. logging endpoint + ALL analytics-owned snippets/conditions/
    #      dictionaries/backends off the customer service
    #   3. FOS access keys
    #   4. FOS bucket
    #   5. analytics-owned log-fronting CDN service (last: the logging endpoint
    #      removed in step 2 is what referenced it)
    #
    # Step 1 stays ahead of step 2 because both clone+activate the customer
    # service, and the scoring strip is the one with a live-traffic dependency.
    # Gated on opts.get(..., True) so the internal provision()-failure rollback
    # (no opts) still tears scoring down. The scoring ids ride in
    # state['scoring'] (the cfg['scoring'] block); disable_scoring can't be used
    # here because the config file is already gone.
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
            # teardown_scoring_resources aborts (raises) rather than delete the
            # Compute service while the customer's active version may still
            # route to it. Surface that loudly — it is an action item, not a
            # cosmetic warning, and the operator must re-run teardown.
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
    if opts.get("remove_logging") and state.get("logging_service_id") and endpoint_name:
        try:
            yield from run_with_events(remove_logging_endpoint, state["logging_service_id"], endpoint_name, token)
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
            from backend.provision.fos_setup import delete_fos_tokens_for_service

            yield from run_with_events(delete_fos_tokens_for_service, state["logging_service_id"], token)
        except Exception:
            pass

    yield {"type": "progress", "current": 4, "total": total_steps}
    step(4, total_steps, "Deleting FOS bucket" if opts.get("remove_bucket") else "Skipping FOS bucket deletion")
    if opts.get("remove_bucket") and state.get("fos_bucket_name"):
        try:
            temp_desc = f"fos-log-analysis-temp-teardown-{state.get('logging_service_id', 'unknown')}"
            temp_key = yield from run_with_events(
                ensure_fos_access_key, temp_desc, {}, token, permission="read-write-admin"
            )
            try:
                yield from run_with_events(
                    delete_fos_bucket,
                    state["fos_bucket_name"],
                    state["fos_region"],
                    temp_key["access_key"],
                    temp_key["secret_key"],
                    service_id=state.get("logging_service_id"),
                )
            finally:
                yield from run_with_events(delete_fos_access_key, temp_key["id"], token)
        except Exception:
            pass
    elif opts.get("remove_cloud_files") and state.get("fos_bucket_name"):
        try:
            temp_desc = f"fos-log-analysis-temp-teardown-{state.get('logging_service_id', 'unknown')}"
            temp_key = yield from run_with_events(
                ensure_fos_access_key, temp_desc, {}, token, permission="read-write-admin"
            )
            try:
                from backend.provision.fos_setup import delete_fos_prefix

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
            yield {"type": "status", "message": f"Warning: failed to delete cloud files: {exc}"}

    yield {"type": "progress", "current": 5, "total": total_steps}
    step(5, total_steps, "Deleting CDN service" if opts.get("remove_cdn") else "Skipping CDN service deletion")
    if opts.get("remove_cdn") and state.get("cdn_service_id") and state.get("cdn_service_name"):
        try:
            yield from run_with_events(delete_cdn_service, state["cdn_service_id"], state["cdn_service_name"], token)
        except Exception:
            pass


def cleanup_local_data(service_id: str, bucket: str = None, remove_data: bool = False):
    """Remove local config files, database, and cache associated with a service."""
    from backend import config as svcconfig

    # Remove service config
    if service_id:
        cfg_path = svcconfig.config_path(service_id)
        if os.path.exists(cfg_path):
            os.remove(cfg_path)
            ok(f"Removed service config: {cfg_path}")

    # Remove local database and cache if requested
    if remove_data:
        db_path = (
            svcconfig.duckdb_path(service_id)
            if service_id
            else os.path.join(os.path.dirname(__file__), "..", "data", "logs.duckdb")
        )
        if os.path.exists(db_path):
            os.remove(db_path)
            wal = db_path + ".wal"
            if os.path.exists(wal):
                os.remove(wal)
            ok(f"Removed local database: {db_path}")

        # Delete RUM DuckDB if it exists
        rum_db_path = (
            db_path.replace(".duckdb", ".rum.duckdb") if db_path.endswith(".duckdb") else db_path + ".rum.duckdb"
        )
        if os.path.exists(rum_db_path):
            os.remove(rum_db_path)
            rum_wal = rum_db_path + ".wal"
            if os.path.exists(rum_wal):
                os.remove(rum_wal)
            ok(f"Removed local RUM database: {rum_db_path}")

        if service_id:
            try:
                from backend.core import metadata as metadata_db

                metadata_db.teardown(service_id)
                ok(f"Cleaned up metadata for {service_id}")
            except Exception:
                pass

        if bucket:
            # Security: ``bucket`` is supplied via the provisioning
            # API and historically had no path-shape validation. A payload
            # like ``../../../tmp/anything`` would compose with
            # os.path.join to produce a path outside the cache root and
            # shutil.rmtree would happily wipe whatever lived there.
            # Reject any separator/traversal token up front, then
            # additionally verify the resolved path stays under the
            # resolved cache root (defense in depth — catches edge cases
            # like symlink escapes from inside an attacker-writable
            # parent dir).
            if any(c in bucket for c in ("/", "\\", "..", "\x00")):
                logger.warning("[teardown] refusing to remove cache for bucket=%r with path-shape characters", bucket)
            else:
                for base in [os.getcwd(), os.path.join(os.path.dirname(__file__), "..", "..")]:
                    cache_root = os.path.realpath(os.path.join(base, "cache"))
                    svc_cache_dir = os.path.realpath(os.path.join(cache_root, bucket))
                    # Reject anything that resolved outside the cache root —
                    # belt-and-suspenders for symlinks pointing elsewhere.
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


def generate_analyst_invite(service_id: str) -> dict:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"Service {service_id} not found")
    if cfg.get("access_level") != "read_write":
        raise RuntimeError("Invite generation requires a read_write service configuration")
    api_token = cfg.get("fastly_api_key", "").strip()
    # Fail fast when the stored token is missing. Without this, the Fastly
    # API call below would go out with token="" and either time out or
    # return an error envelope; either way the downstream key["access_key"]
    # would raise an unhelpful KeyError instead of a clean 400-style message.
    # Caller (route handler) wraps RuntimeError → HTTPException(400).
    if not api_token:
        raise RuntimeError(
            f"Service {service_id} has no stored fastly_api_key. Rotate the credential before generating a viewer key."
        )
    bucket = cfg.get("fos_bucket", "")
    region = cfg.get("fos_region", "us-east-1")
    key = fastly(
        "POST",
        "/resources/object-storage/access-keys",
        {
            "permission": "read-only-objects",
            "description": f"fos-log-analysis-analyst-{service_id}-{int(time.time())}",
            "buckets": [bucket],
        },
        token=api_token,
    )
    # Defensive: a malformed Fastly response shouldn't bubble up as a raw
    # KeyError on access_key / secret_key — surface a clear error instead.
    if not isinstance(key, dict) or "access_key" not in key or "secret_key" not in key:
        raise RuntimeError(
            f"Fastly access-key API returned unexpected shape (keys={list(key.keys()) if isinstance(key, dict) else type(key).__name__}); "
            "cannot generate analyst invite."
        )

    iceberg_metadata_location = None
    try:
        from backend.core import iceberg as db_iceberg

        src = svcconfig.config_to_source(cfg)
        catalog = db_iceberg._get_catalog(src)  # type: ignore[attr-defined]
        table = catalog.load_table(db_iceberg._table_identifier(src))  # type: ignore[attr-defined]
        iceberg_metadata_location = table.metadata_location
    except Exception:
        pass

    return {
        "name": cfg.get("name", service_id),
        "service_id": service_id,
        "fos_bucket": bucket,
        "fos_region": region,
        "fos_endpoint": cfg.get("fos_endpoint", f"{region}.object.fastlystorage.app"),
        "fos_prefix": cfg.get("fos_prefix", ""),
        "access_key_id": key["access_key"],
        "secret_key": key["secret_key"],
        "iceberg_metadata_location": iceberg_metadata_location,
        "cdn_url": cfg.get("cdn_url", ""),
        "cdn_service_id": cfg.get("cdn_service_id") or "",
        "cdn_secret": cfg.get("cdn_secret") or "",
    }


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
    # Seed from the preset's own field_overrides so preset-level opt-ins/outs
    # apply (e.g. security → ``tls_ciphers_sha: True``). Without this a preset's
    # declared overrides were silently dropped, and — post default_off — an
    # opt-in field a preset wanted on could never be turned on through a preset.
    # CLI --enable-field / --disable-field then layer on top (disable last so it
    # wins). This is also the path that turns a ``default_off`` field (e.g.
    # cookie_session) ON: ``--enable-field cookie_session``.
    field_overrides = dict(preset.get("field_overrides") or {})
    field_overrides.update({fid: True for fid in (getattr(args, "enable_field", None) or [])})
    field_overrides.update({fid: False for fid in (getattr(args, "disable_field", None) or [])})
    return {"schema_version": 2, "preset": preset_name, "groups": sorted(groups), "field_overrides": field_overrides}
