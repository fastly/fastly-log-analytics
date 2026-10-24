"""CMCD (Common Media Client Data) custom-field cleanup helper."""

from __future__ import annotations

from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES


def _remove_cmcd_custom_fields(cfg: dict) -> None:
    lf = cfg.get("log_fields") or {}
    cfs = lf.get("custom_fields")
    if cfs:
        lf["custom_fields"] = [cf for cf in cfs if cf.get("name") not in _CMCD_FIELD_NAMES]
