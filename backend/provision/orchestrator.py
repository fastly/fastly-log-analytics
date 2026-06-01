import json
import os
import queue
import shutil
import threading
import time

from backend.core import log_fields as lf
from backend.core.fastly.client import fastly
from backend.core.fastly.utils import (
    region_endpoint,
)
from backend.provision.fastly_api import (
    delete_cdn_service,
    ensure_cdn_service,
    ensure_logging_endpoint,
    remove_logging_endpoint,
    validate_log_format,
)
from backend.provision.fos_setup import (
    delete_fos_access_key,
    delete_fos_bucket,
    ensure_fos_access_key,
    ensure_fos_bucket,
)
from backend.provision.utils import ok, step


def _sync_crontab():
    """Refresh the background scheduler if running within the web app."""
    try:
        from backend.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass


def write_service_config(state: dict):
    """Write a service config JSON file to configs/{service_id}.json."""
    from backend import config as svcconfig

    service_id = state.get("logging_service_id") or state.get("service_id")
    db_path = svcconfig.duckdb_path(service_id)

    fos_key = state.get("fos_access_key_id") or state.get("fos_access_key", "")
    fos_secret = state.get("fos_secret_access_key") or state.get("fos_secret_key", "")
    bucket = state.get("fos_bucket") or state.get("fos_bucket_name", "")
    region = state.get("fos_region", "us-east-1")
    cdn_url = state.get("cdn_url", "")

    cfg = {
        "service_id": service_id,
        "name": state.get("name") or state.get("service_name") or service_id,
        "access_level": state.get("access_level", "read_write"),
        "storage_mode": state.get("storage_mode", "cloud"),
        "fos_endpoint": region_endpoint(region),
        "fos_access_key_id": fos_key,
        "fos_secret_access_key": fos_secret,
        "fos_bucket": bucket,
        "fos_prefix": state.get("fos_prefix", ""),
        "fos_region": region,
        "cdn_url": cdn_url,
        "cdn_secret": state.get("cdn_secret", ""),
        "cdn_service_id": state.get("cdn_service_id", ""),
        "fastly_api_key": state.get("fastly_api_key") or state.get("admin_token", ""),
        "log_retention_days": int(state.get("log_retention_days", 30)),
        "duckdb_path": db_path,
        "log_fields": state.get("log_fields", {}),
    }

    if "log_period" in state:
        cfg["log_period"] = state["log_period"]
    elif "log_period" in state.get("provisioning", {}):
        cfg["log_period"] = state["provisioning"]["log_period"]

    log_period_secs = int(cfg.get("log_period", 120))
    # log_period ≤ 5 is the "real-time" tier: sync every 5s to catch each
    # rotation. Anything else stays on the conservative 30s floor so dashboard
    # freshness doesn't drive surprise CDN traffic.
    if log_period_secs <= 5:
        sync_interval_seconds = 5
    else:
        sync_interval_seconds = max(30, log_period_secs)
    commit_interval_mins = max(max(1, sync_interval_seconds // 60), int(state.get("commit_interval_mins", 5)))

    cfg["provisioning"] = {
        "fos_key_id": state.get("provisioning", {}).get("fos_key_id", ""),
        "endpoint_name": state.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs"),
        "cdn_service_id": state.get("cdn_service_id", state.get("provisioning", {}).get("cdn_service_id", "")),
        "cdn_url": cdn_url,
        "cdn_shield": state.get("cdn_shield", state.get("provisioning", {}).get("cdn_shield", "")),
        "sample_rate": state.get("sample_rate", state.get("provisioning", {}).get("sample_rate", "100")),
        "edge_only": state.get("edge_only", state.get("provisioning", {}).get("edge_only", True)),
        "custom_condition": state.get("custom_condition", state.get("provisioning", {}).get("custom_condition", "")),
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
                state.get("provisioning", {}).get("cron_sync", {}).get("data_retention_days", 30)
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


def run_with_events(func, *args, **kwargs):
    q = queue.Queue()
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
        raise err[0]
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


def provision(cfg: dict, _resume_from_state: bool = False):
    token = cfg["admin_token"]
    state = load_state() if _resume_from_state else {}
    state.update(cfg)

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
        cdn_svc = yield from run_with_events(
            ensure_cdn_service, cfg, state["fos_access_key"], state["fos_secret_key"], token
        )
        state["cdn_service_id"] = cdn_svc["id"]
        save_state(state)
        yield {"type": "progress", "current": 6, "total": total}

        yield {"type": "status", "message": "📡 Step 7/8: Adding logging endpoint to target service..."}
        step(7, total, "Adding logging endpoint to target service")
        new_ver = yield from run_with_events(
            ensure_logging_endpoint, cfg, state["fos_access_key"], state["fos_secret_key"], token
        )
        state["activated_logging_version"] = new_ver
        save_state(state)
        yield {"type": "progress", "current": 7, "total": total}

        yield {"type": "status", "message": "⚙️ Step 8/8: Finalizing service configuration..."}
        step(8, total, "Writing service config")
        write_service_config(state)

        try:
            from backend.core import duckdb as db
            from backend.core import iceberg as db_iceberg

            src = db.get_source_for_service(state["logging_service_id"])
            if src:
                yield {"type": "status", "message": "🧊 Initializing Iceberg table in Fastly Object Storage..."}
                try:
                    db_iceberg.init_iceberg_table(src)
                except Exception:
                    pass
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
                    from backend.core import metadata_db

                    metadata_db.record_audit(src["name"], "log_format_change", details, actor="provisioning")
                c.close()
        except Exception:
            pass

        yield {"type": "progress", "current": 8, "total": total}
        yield {"type": "done", "message": "🎉 Provisioning complete!"}

    except _PreflightError as e:
        yield {"type": "error", "message": str(e)}
        return
    except Exception as e:
        yield {"type": "status", "message": f"Provisioning failed: {str(e)}. Starting full cleanup rollback..."}
        yield from perform_teardown(state, token)
        service_id = state.get("logging_service_id")
        if service_id:
            from backend import config as svcconfig

            cfg_path = svcconfig.config_path(service_id)
            if os.path.exists(cfg_path):
                os.remove(cfg_path)
        yield {"type": "error", "message": str(e)}
        raise e


def perform_teardown(state: dict, token: str, opts: dict | None = None):
    if opts is None:
        opts = {"remove_logging": True, "remove_cdn": True, "remove_bucket": True}

    total_steps = 4
    yield {"type": "progress", "current": 1, "total": total_steps}
    step(
        1,
        total_steps,
        "Removing logging endpoint" if opts.get("remove_logging") else "Skipping logging endpoint removal",
    )
    if opts.get("remove_logging") and state.get("logging_service_id") and state.get("endpoint_name"):
        try:
            yield from run_with_events(
                remove_logging_endpoint, state["logging_service_id"], state["endpoint_name"], token
            )
        except Exception as exc:
            yield {"type": "status", "message": f"Warning: {exc}"}

    yield {"type": "progress", "current": 2, "total": total_steps}
    step(
        2, total_steps, "Deleting FOS access keys" if opts.get("remove_bucket") else "Skipping FOS access keys deletion"
    )
    if opts.get("remove_bucket"):
        try:
            target_desc = f"fos-log-analysis-{state['logging_service_id']}"
            temp_admin_desc = f"fos-log-analysis-temp-admin-{state['logging_service_id']}"
            resp = fastly("GET", "/resources/object-storage/access-keys", token=token)
            for key in resp.get("data", []):
                desc = key.get("description", "")
                if desc == target_desc or desc == temp_admin_desc or desc.startswith("temp-teardown-"):
                    fastly(
                        "DELETE",
                        f"/resources/object-storage/access-keys/{key['access_key']}",
                        token=token,
                        expect_empty=True,
                    )
        except Exception:
            pass
        if state.get("fos_key_id"):
            try:
                yield from run_with_events(delete_fos_access_key, state["fos_key_id"], token)
            except Exception:
                pass

    yield {"type": "progress", "current": 3, "total": total_steps}
    step(3, total_steps, "Deleting FOS bucket" if opts.get("remove_bucket") else "Skipping FOS bucket deletion")
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

    yield {"type": "progress", "current": 4, "total": total_steps}
    step(4, total_steps, "Deleting CDN service" if opts.get("remove_cdn") else "Skipping CDN service deletion")
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

        if service_id:
            try:
                from backend.core import metadata_db

                metadata_db.teardown(service_id)
                ok(f"Cleaned up metadata for {service_id}")
            except Exception:
                pass

        if bucket:
            # Look for cache dir in both common locations
            for base in [os.getcwd(), os.path.join(os.path.dirname(__file__), "..", "..")]:
                svc_cache_dir = os.path.join(base, "cache", bucket)
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

    iceberg_metadata_location = None
    try:
        from backend.core import iceberg as db_iceberg

        src = svcconfig.config_to_source(cfg)
        catalog = db_iceberg._get_catalog(src)
        table = catalog.load_table(db_iceberg._table_identifier(src))
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
    groups = list(preset["groups"])
    for g in getattr(args, "enable_group", None) or []:
        if g not in groups:
            groups.append(g)
    for g in getattr(args, "disable_group", None) or []:
        if g in groups:
            groups.remove(g)
    field_overrides = {fid: True for fid in (getattr(args, "enable_field", None) or [])}
    field_overrides.update({fid: False for fid in (getattr(args, "disable_field", None) or [])})
    return {"schema_version": 2, "preset": preset_name, "groups": sorted(groups), "field_overrides": field_overrides}
