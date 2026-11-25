"""Log fields a feature turns on for itself, and when they may be turned off again.

A feature that needs a catalog field (RUM needs ``rid`` to match client timings to request
logs) turns it on when the feature is enabled and records that it did. On disable it turns
off only the fields it turned on, and only when no other enabled feature still needs them:
a field that was already on, or that another feature shares, stays on.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from backend.config import is_rum_enabled
from backend.core import log_fields as lf_module
from backend.core.catalog import get_lf_config, resolve_enabled_fields

FEATURE_LOG_FIELDS: dict[str, tuple[str, ...]] = {"rum": ("rid",)}
_FEATURE_ENABLED: dict[str, Callable[[dict[str, Any]], bool]] = {"rum": is_rum_enabled}
_OWNED_KEY = "feature_enabled_fields"


def _log_fields(cfg: dict[str, Any]) -> dict[str, Any]:
    log_fields = dict(get_lf_config(cfg))
    log_fields["field_overrides"] = dict(log_fields.get("field_overrides") or {})
    cfg["log_fields"] = log_fields
    return log_fields


def claim_feature_fields(cfg: dict[str, Any], feature: str) -> None:
    """Turn on ``feature``'s fields that are off, remembering which ones it turned on."""
    log_fields = _log_fields(cfg)
    enabled = resolve_enabled_fields(log_fields)
    missing = [field for field in FEATURE_LOG_FIELDS[feature] if field not in enabled]
    if not missing:
        return
    owned = cfg.setdefault(_OWNED_KEY, {}).setdefault(feature, [])
    for field in missing:
        log_fields["field_overrides"][field] = True
        if field not in owned:
            owned.append(field)
    log_fields["format_hash"] = lf_module.format_hash(log_fields)


def release_feature_fields(cfg: dict[str, Any], feature: str) -> None:
    """Turn off the fields ``feature`` turned on, unless another enabled feature needs them."""
    owned_by_feature = cfg.get(_OWNED_KEY) or {}
    owned = owned_by_feature.pop(feature, [])
    if not owned:
        return
    log_fields = _log_fields(cfg)
    for field in owned:
        heir = next(
            (
                other
                for other, fields in FEATURE_LOG_FIELDS.items()
                if other != feature and field in fields and _FEATURE_ENABLED[other](cfg)
            ),
            None,
        )
        if heir is not None:
            heirs = owned_by_feature.setdefault(heir, [])
            if field not in heirs:
                heirs.append(field)
        elif log_fields["field_overrides"].get(field) is True:
            del log_fields["field_overrides"][field]
    cfg[_OWNED_KEY] = owned_by_feature
    log_fields["format_hash"] = lf_module.format_hash(log_fields)
