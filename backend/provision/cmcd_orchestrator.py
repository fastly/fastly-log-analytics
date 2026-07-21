"""Enable / disable CMCD (Common Media Client Data) collection.

Follows the same clone -> mutate -> validate -> activate pattern as
session scoring, but simpler (no Compute service, no Wasm).
"""

from __future__ import annotations

import datetime as _dt
import logging
import urllib.parse
from typing import Any

from backend import config as svcconfig
from backend.core.fastly.client import fastly
from backend.core.fastly.service import (
    ensure_vcl_snippet,
    get_active_version,
    list_s3_endpoints,
    list_vcl_snippets,
)
from backend.provision.cmcd_fields import (
    _CMCD_FIELD_NAMES,
    merge_cmcd_custom_fields,
)
from backend.provision.cmcd_vcl import (
    CMCD_SNIPPET_NAME,
    CMCD_SNIPPET_PRIORITY,
    cmcd_snippet_names,
    generate_cmcd_vcl,
)

logger = logging.getLogger(__name__)


def _remove_cmcd_custom_fields(cfg: dict) -> None:
    lf = cfg.get("log_fields") or {}
    cfs = lf.get("custom_fields")
    if cfs:
        lf["custom_fields"] = [cf for cf in cfs if cf.get("name") not in _CMCD_FIELD_NAMES]


def _add_cmcd_custom_fields(cfg: dict) -> None:
    lf = cfg.setdefault("log_fields", {})
    lf["custom_fields"] = merge_cmcd_custom_fields(lf.get("custom_fields"))


def enable_cmcd(
    logging_service_id: str,
    token: str,
    *,
    mode: str = "query_string",
    version: int = 1,
    status_cb=None,
) -> dict[str, Any]:
    """Enable CMCD collection for the given logging service.

    1. Write CMCD config block + custom fields to service config
    2. Clone active VCL version
    3. Install CMCD extraction snippet (version-aware)
    4. Regenerate capture VCL + log format
    5. Validate -> Activate (rollback on failure)
    """
    if version not in (1, 2):
        raise ValueError(f"Unknown CMCD version: {version!r} (expected 1 or 2)")

    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    if status_cb:
        status_cb(f"Enabling CMCD v{version} collection for {logging_service_id}...")

    cfg["cmcd"] = {
        "enabled": True,
        "mode": mode,
        "version": version,
        "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
    }

    _add_cmcd_custom_fields(cfg)
    svcconfig.save_config(logging_service_id, cfg)
    n_fields = len(_CMCD_FIELD_NAMES)
    if status_cb:
        status_cb(f"Stashed CMCD config + {n_fields} custom fields")

    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")

    if status_cb:
        status_cb(f"Cloning active version {active_ver}...")
    clone = fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{active_ver}/clone",
        token=token,
    )
    new_ver = int(clone["number"])
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{new_ver}",
        {"comment": f"Enable CMCD v{version} collection (mode={mode}) {ts}"},
        token=token,
    )

    try:
        if status_cb:
            status_cb("Installing CMCD extraction VCL snippet...")
        vcl_snippets = generate_cmcd_vcl(mode=mode, version=version)
        ensure_vcl_snippet(
            CMCD_SNIPPET_NAME,
            "recv",
            vcl_snippets[CMCD_SNIPPET_NAME],
            CMCD_SNIPPET_PRIORITY,
            logging_service_id,
            new_ver,
            token,
        )

        if status_cb:
            status_cb("Updating log format to include CMCD fields...")
        from backend.provision.fastly_api import install_capture_snippets, load_log_format

        scoring_enabled = bool((cfg.get("scoring") or {}).get("enabled"))
        install_capture_snippets(
            logging_service_id,
            new_ver,
            cfg.get("log_fields"),
            token,
            scoring_enabled=scoring_enabled,
        )

        endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
        existing_endpoints = list_s3_endpoints(logging_service_id, new_ver, token)
        if endpoint_name in existing_endpoints:
            new_format = load_log_format(cfg.get("log_fields"))
            encoded = urllib.parse.quote(endpoint_name, safe="")
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{new_ver}/logging/s3/{encoded}",
                {"format": new_format, "format_version": 2},
                token=token,
            )

        if status_cb:
            status_cb(f"Validating draft version {new_ver}...")
        result = fastly(
            "GET",
            f"/service/{logging_service_id}/version/{new_ver}/validate",
            token=token,
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")

        if status_cb:
            status_cb(f"Activating version {new_ver}...")
        fastly(
            "PUT",
            f"/service/{logging_service_id}/version/{new_ver}/activate",
            token=token,
        )
        if status_cb:
            status_cb(f"CMCD v{version} collection enabled — version {new_ver} active.")

        try:
            from backend.state_sync import export_admin_state

            export_admin_state(logging_service_id)
        except Exception:
            logger.warning("Could not export admin_state after CMCD enable (non-fatal)", exc_info=True)

        return {
            "enabled": True,
            "mode": mode,
            "version": version,
            "logging_service_active_version": new_ver,
        }

    except Exception:
        logger.exception("enable_cmcd failed for %s", logging_service_id)
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        try:
            fresh = svcconfig.load_config(logging_service_id) or cfg
        except Exception:
            fresh = cfg
        fresh.pop("cmcd", None)
        _remove_cmcd_custom_fields(fresh)
        svcconfig.save_config(logging_service_id, fresh)
        raise


def disable_cmcd(
    logging_service_id: str,
    token: str,
    *,
    status_cb=None,
) -> None:
    """Disable CMCD collection. Reverse of enable_cmcd."""
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    cmcd = cfg.get("cmcd") or {}
    if not cmcd.get("enabled"):
        if status_cb:
            status_cb("CMCD collection already disabled.")
        return

    if status_cb:
        status_cb(f"Disabling CMCD collection for {logging_service_id}...")

    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    clone = fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{active_ver}/clone",
        token=token,
    )
    new_ver = int(clone["number"])
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{new_ver}",
        {"comment": f"Disable CMCD collection {ts}"},
        token=token,
    )

    try:
        snippets = list_vcl_snippets(logging_service_id, new_ver, token)
        for name in cmcd_snippet_names():
            if name in snippets:
                encoded = urllib.parse.quote(name, safe="")
                try:
                    fastly(
                        "DELETE",
                        f"/service/{logging_service_id}/version/{new_ver}/snippet/{encoded}",
                        token=token,
                    )
                except RuntimeError:
                    pass

        _remove_cmcd_custom_fields(cfg)
        svcconfig.save_config(logging_service_id, cfg)

        from backend.provision.fastly_api import install_capture_snippets, load_log_format

        scoring_enabled = bool((cfg.get("scoring") or {}).get("enabled"))
        install_capture_snippets(
            logging_service_id,
            new_ver,
            cfg.get("log_fields"),
            token,
            scoring_enabled=scoring_enabled,
        )

        endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
        existing_endpoints = list_s3_endpoints(logging_service_id, new_ver, token)
        if endpoint_name in existing_endpoints:
            new_format = load_log_format(cfg.get("log_fields"))
            encoded = urllib.parse.quote(endpoint_name, safe="")
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{new_ver}/logging/s3/{encoded}",
                {"format": new_format, "format_version": 2},
                token=token,
            )

        result = fastly(
            "GET",
            f"/service/{logging_service_id}/version/{new_ver}/validate",
            token=token,
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
        fastly(
            "PUT",
            f"/service/{logging_service_id}/version/{new_ver}/activate",
            token=token,
        )
        if status_cb:
            status_cb(f"CMCD collection disabled — version {new_ver} active.")
    except Exception:
        logger.exception("disable_cmcd VCL phase failed for %s", logging_service_id)
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        raise

    try:
        fresh = svcconfig.load_config(logging_service_id) or cfg
    except Exception:
        fresh = cfg
    fresh.pop("cmcd", None)
    svcconfig.save_config(logging_service_id, fresh)

    try:
        from backend.state_sync import export_admin_state

        export_admin_state(logging_service_id)
    except Exception:
        logger.warning("Could not export admin_state after CMCD disable (non-fatal)", exc_info=True)

    if status_cb:
        status_cb("CMCD collection disabled.")
