"""Config migration: auto-convert flat fields to nested structure for v2.3.0+."""

from __future__ import annotations

from typing import Any


def migrate_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Migrate flat CMCD/Scoring fields to nested structure.

    Idempotent: calling on already-nested config returns it unchanged.

    Args:
        cfg: Raw config dict (may have flat cmcd_* / scoring_* fields).

    Returns:
        Config dict with flat fields consolidated to nested objects.
        Original dict is not mutated; returns a new dict with nested structure.
    """
    if not cfg:
        return cfg

    result = dict(cfg)

    # Migrate CMCD: flat fields → nested object (if not already nested)
    has_flat_cmcd = any(k in result for k in ("cmcd_enabled", "cmcd_mode", "cmcd_version"))
    has_nested_cmcd = "cmcd" in result and isinstance(result.get("cmcd"), dict)

    if has_flat_cmcd and not has_nested_cmcd:
        result["cmcd"] = {
            "enabled": result.pop("cmcd_enabled", False),
            "mode": result.pop("cmcd_mode", "query_string"),
            "version": result.pop("cmcd_version", 1),
        }
    elif has_flat_cmcd and has_nested_cmcd:
        # Both present: flat fields override nested (backwards compat), then consolidate
        result["cmcd"]["enabled"] = result.pop("cmcd_enabled", result["cmcd"].get("enabled", False))
        result["cmcd"]["mode"] = result.pop("cmcd_mode", result["cmcd"].get("mode", "query_string"))
        result["cmcd"]["version"] = result.pop("cmcd_version", result["cmcd"].get("version", 1))

    # Migrate Scoring: flat fields → nested object (if not already nested)
    has_flat_scoring = any(
        k in result
        for k in (
            "scoring_enabled",
            "scoring_domain",
            "scoring_request_secret",
            "scoring_exclude_url_regex",
            "scoring_enforce_status_code",
        )
    )
    has_nested_scoring = "scoring" in result and isinstance(result.get("scoring"), dict)

    if has_flat_scoring and not has_nested_scoring:
        result["scoring"] = {
            "enabled": result.pop("scoring_enabled", False),
            "domain": result.pop("scoring_domain", ""),
            "request_secret": result.pop("scoring_request_secret", ""),
            "exclude_url_regex": result.pop("scoring_exclude_url_regex", ""),
            "enforce_status_code": result.pop("scoring_enforce_status_code", 429),
        }
    elif has_flat_scoring and has_nested_scoring:
        # Both present: flat fields override nested, then consolidate
        result["scoring"]["enabled"] = result.pop("scoring_enabled", result["scoring"].get("enabled", False))
        result["scoring"]["domain"] = result.pop("scoring_domain", result["scoring"].get("domain", ""))
        result["scoring"]["request_secret"] = result.pop(
            "scoring_request_secret", result["scoring"].get("request_secret", "")
        )
        result["scoring"]["exclude_url_regex"] = result.pop(
            "scoring_exclude_url_regex", result["scoring"].get("exclude_url_regex", "")
        )
        result["scoring"]["enforce_status_code"] = result.pop(
            "scoring_enforce_status_code", result["scoring"].get("enforce_status_code", 429)
        )

    return result


def config_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Detect if migration changed the config structure.

    Args:
        before: Original config dict.
        after: Migrated config dict.

    Returns:
        True if any flat fields were consolidated (config should be rewritten).
    """
    # Check if any flat fields were removed
    flat_fields = {
        "cmcd_enabled",
        "cmcd_mode",
        "cmcd_version",
        "scoring_enabled",
        "scoring_domain",
        "scoring_request_secret",
        "scoring_exclude_url_regex",
        "scoring_enforce_status_code",
    }
    return any(k in before for k in flat_fields) and before != after
