"""Reconciliation of system-managed custom fields (session scoring + CMCD).

Both features inject a canonical set of ``log_fields.custom_fields`` entries
that are generated from code, not authored by the admin. ``_is_system_field``
in [backend/core/field_registry.py] hides them from the user-editable
custom-field list, which means every writer that persists ``log_fields`` from
a list it did not author will omit them:

  - the log-fields UI round-trip (``api_service_log_fields_set``)
  - a ``state_sync`` pull of a remote ``admin_state.json``
  - a provisioning reconcile whose ``log_fields`` is built from groups alone

An omission is not cosmetic. ``reconcile_vcl_state`` regenerates the Fastly log
format from the persisted config, so dropping a system field removes it from
the log format while its extraction VCL stays installed and running. The edge
keeps parsing the data into ``req.http.x-cmcd:*`` / ``req.http.x-edge-score:*``
and nothing writes it to the log line — every column ingests empty, and the
feature's page renders all zeros with no error to explain why.

Two production incidents came from exactly this:

  - 2026-06-02 — ``state_sync`` overwrote scoring's 8 fields on every tick.
  - 2026-08-12 — the SE-demo service lost all 14 CMCD fields; CMCD had been
    enabled since 2026-07-13 and never collected a single value.

Route every such write through ``reconcile_system_custom_fields``, keyed on the
CURRENT feature state so enabling and disabling both converge. Do not key it on
a state transition: a reconcile that changes nothing about the feature must
still re-assert the fields, which is precisely the case the transition-only
guard in ``update_service_config`` used to miss.
"""

from __future__ import annotations

from typing import Any


def system_feature_flags(cfg: dict[str, Any]) -> tuple[bool, bool]:
    """Return ``(scoring_enabled, cmcd_enabled)`` for a service config."""
    scoring_enabled = bool((cfg.get("scoring") or {}).get("enabled"))
    cmcd_enabled = bool((cfg.get("cmcd") or {}).get("enabled"))
    return scoring_enabled, cmcd_enabled


def reconcile_system_custom_fields(
    custom_fields: list[dict] | None,
    *,
    scoring_enabled: bool,
    cmcd_enabled: bool,
) -> list[dict]:
    """Return ``custom_fields`` with system-managed entries re-asserted.

    Strips any entry whose name belongs to a system feature (however stale,
    partial, or different) and re-adds the canonical entries from code for
    each feature that is currently enabled. User-defined fields pass through
    untouched and keep their relative order.

    System entries are appended in a DETERMINISTIC order (scoring, then CMCD,
    each in its canonical declaration order). This matters beyond tidiness:
    Iceberg assigns a field id when a column is first added, so the order the
    fields appear here decides the ids on a freshly-built table. Two tables
    built from the same config in different orders end up with the same columns
    under different ids — and then ``add_files`` between them is impossible,
    because Iceberg binds by id. That is exactly what blocked recovery of the
    2026-08 SE-demo rollback: 27 columns, identical names and types, different
    ids on each branch. Do not make this order depend on dict iteration,
    feature-toggle timing, or the incoming list.
    """
    from backend.provision.cmcd_fields import reconcile_cmcd_custom_fields
    from backend.provision.session_scoring_orchestrator import (
        _SCORING_CUSTOM_FIELDS,
        _SCORING_FIELD_NAMES,
    )

    out = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    if scoring_enabled:
        out.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
    return reconcile_cmcd_custom_fields(out, enabled=cmcd_enabled)


def reconcile_cfg_system_custom_fields(cfg: dict[str, Any]) -> list[dict]:
    """Re-assert system fields on ``cfg['log_fields']['custom_fields']`` in place.

    Returns the reconciled list. Safe to call on a config with no
    ``log_fields`` block — it creates one with the schema-v2 default shape.
    """
    scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
    lf = cfg.get("log_fields")
    if not isinstance(lf, dict):
        lf = {"schema_version": 2, "custom_fields": []}
        cfg["log_fields"] = lf
    reconciled = reconcile_system_custom_fields(
        lf.get("custom_fields"),
        scoring_enabled=scoring_enabled,
        cmcd_enabled=cmcd_enabled,
    )
    lf["custom_fields"] = reconciled
    return reconciled
