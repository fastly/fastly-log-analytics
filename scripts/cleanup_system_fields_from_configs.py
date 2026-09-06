#!/usr/bin/env python3
"""Clean up system-managed fields from existing service configs.

System fields (CMCD and Scoring) should only be generated on-demand from
feature toggles, not persisted in the config file. This script removes them
from the custom_fields list in all service configs while preserving user-
defined custom fields.

Run from repo root:
  python scripts/cleanup_system_fields_from_configs.py
"""

import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def is_system_field(field_name: str) -> bool:
    """Check if field is system-managed."""
    return field_name.startswith("cmcd_") or field_name.startswith("edge_")


def is_orphaned_toplevel_field(field_name: str) -> bool:
    """Check if field is orphaned/operational and should be removed from top-level config."""
    return field_name in ("last_reconciliation_at", "endpoint_name")


def filter_user_custom_fields(custom_fields: list) -> list:
    """Filter out system fields, keeping only user-defined fields."""
    return [f for f in custom_fields if not is_system_field(f.get("name", ""))]


def cleanup_config(config_path: Path) -> tuple[bool, list[str]]:
    """Clean up system fields from a single config file.

    Returns (changed: bool, removed_field_names: list[str])
    """
    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read {config_path}: {e}")
        return False, []

    removed_fields = []

    # Clean up custom_fields (system-managed CMCD/edge fields)
    log_fields = cfg.get("log_fields", {})
    if isinstance(log_fields, dict):
        custom_fields = log_fields.get("custom_fields", [])
        if isinstance(custom_fields, list):
            # Find system fields to remove
            system_field_names = [f.get("name") for f in custom_fields if is_system_field(f.get("name", ""))]
            if system_field_names:
                user_fields = filter_user_custom_fields(custom_fields)
                log_fields["custom_fields"] = user_fields
                cfg["log_fields"] = log_fields
                removed_fields.extend(system_field_names)

    # Clean up orphaned top-level fields
    for field_name in list(cfg.keys()):
        if is_orphaned_toplevel_field(field_name):
            del cfg[field_name]
            removed_fields.append(field_name)

    if not removed_fields:
        return False, []

    # Write back
    try:
        with open(config_path, "w") as f:
            json.dump(cfg, f, indent=2)
            f.write("\n")
        logger.info(f"Cleaned up {config_path}: removed {len(removed_fields)} field(s)")
        return True, removed_fields
    except Exception as e:
        logger.error(f"Failed to write {config_path}: {e}")
        return False, []


def main():
    """Scan configs/ and clean up all service configs."""
    configs_dir = Path("configs")

    if not configs_dir.exists():
        logger.error("configs/ directory not found (run from repo root)")
        return 1

    config_files = sorted(configs_dir.glob("*.json"))
    if not config_files:
        logger.info("No service configs found in configs/")
        return 0

    total_cleaned = 0
    total_fields_removed = 0

    for config_path in config_files:
        changed, removed = cleanup_config(config_path)
        if changed:
            total_cleaned += 1
            total_fields_removed += len(removed)

    logger.info(
        f"\nSummary: cleaned {total_cleaned} config(s), removed {total_fields_removed} field(s) (system and orphaned)"
    )
    return 0


if __name__ == "__main__":
    exit(main())
