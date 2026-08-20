"""Auto-cleanup of legacy logging endpoint names during reconciliation."""

from __future__ import annotations

import logging
import urllib.parse
from collections.abc import Callable

from backend.core.fastly.client import fastly
from backend.core.fastly.service import get_active_version

logger = logging.getLogger(__name__)


def cleanup_legacy_logging_endpoints(
    service_id: str,
    token: str,
    status_cb: Callable[[str], None] | None = None,
    activate: bool = True,
) -> bool:
    """Remove legacy logging endpoints from a Fastly service.

    Searches for and removes endpoints matching these legacy naming patterns:
    - "Fastly Object Storage Logs"
    - "Fastly Log Analytics" (old name before RUM split)
    - "Fastly RUM Logs" (old name)

    These have been superseded by:
    - "Fastly Log Analytics Request Logs"
    - "Fastly Log Analytics RUM Logs"

    Returns True if any endpoints were removed, False otherwise.
    """
    if not activate:
        return False

    legacy_patterns = [
        "Fastly Object Storage Logs",
        "Fastly Log Analytics",
        "Fastly RUM Logs",
    ]

    try:
        if status_cb:
            status_cb("⏳ Checking for legacy logging endpoints...")

        # Get active version
        active_ver = get_active_version(service_id, token)
        if not active_ver:
            logger.warning(f"No active version found for service {service_id}")
            return False

        # Get logging endpoints for this version
        endpoints = fastly(
            "GET",
            f"/service/{service_id}/version/{active_ver}/logging/s3",
            token=token,
        )

        if not isinstance(endpoints, list):
            logger.warning(f"Could not fetch logging endpoints for {service_id}")
            return False

        to_remove = []
        for ep in endpoints:
            ep_name = ep.get("name", "")
            if ep_name in legacy_patterns:
                to_remove.append(ep_name)

        if not to_remove:
            logger.info(f"No legacy logging endpoints found in {service_id}")
            return False

        logger.info(f"Found {len(to_remove)} legacy endpoint(s) to remove: {to_remove}")
        if status_cb:
            status_cb(f"⏳ Removing {len(to_remove)} legacy logging endpoint(s)...")

        # Clone the version for modification
        new_ver = fastly(
            "PUT",
            f"/service/{service_id}/version/{active_ver}/clone",
            token=token,
        )
        new_ver_num = new_ver.get("number")

        # Delete each legacy endpoint
        for ep_name in to_remove:
            encoded_name = urllib.parse.quote(ep_name, safe="")
            try:
                fastly(
                    "DELETE",
                    f"/service/{service_id}/version/{new_ver_num}/logging/s3/{encoded_name}",
                    token=token,
                )
                logger.info(f"✓ Removed legacy endpoint: {ep_name}")
            except Exception as e:
                logger.warning(f"Failed to remove {ep_name}: {e}")

        # Validate and activate the new version
        fastly(
            "GET",
            f"/service/{service_id}/version/{new_ver_num}/validate",
            token=token,
        )
        fastly(
            "PUT",
            f"/service/{service_id}/version/{new_ver_num}/activate",
            token=token,
        )
        logger.info(f"✓ Activated clean version {new_ver_num}")
        if status_cb:
            status_cb(f"✓ Cleaned {len(to_remove)} legacy endpoints, activated version {new_ver_num}")

        return True

    except Exception as e:
        logger.error(f"Error cleaning up legacy endpoints: {e}")
        if status_cb:
            status_cb(f"⚠️  Legacy endpoint cleanup warning: {e}")
        # Non-blocking: don't fail the reconciliation if cleanup errors
        return False
