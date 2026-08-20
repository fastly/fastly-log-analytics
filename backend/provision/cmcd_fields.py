"""CMCD custom field definitions for the DuckDB schema and VCL log format.

Mirrors ``_SCORING_CUSTOM_FIELDS`` from ``session_scoring_orchestrator.py``.
All names prefixed ``cmcd_`` to avoid collision with existing fields.

The 14 default-enabled fields cover the analytics-useful subset of CMCD v1/v2.
Low-analytics-value fields (``nor``, ``nrr``, ``pr``, ``v``) are omitted.
"""

from __future__ import annotations

from typing import Any

_CMCD_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": "cmcd_sid",
        "label": "CMCD Session ID",
        "vcl_log_expression": "req.http.x-cmcd:sid",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
        # CMCD v1 sid is a UUID (36 chars); 288//6 = 48 chars of headroom.
        "byte_limit": 288,
        "enabled": True,
    },
    {
        "name": "cmcd_cid",
        "label": "CMCD Content ID",
        "vcl_log_expression": "req.http.x-cmcd:cid",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 30,
        # content id / hashUrl output; 288//6 = 48 chars.
        "byte_limit": 288,
        "enabled": True,
    },
    {
        "name": "cmcd_br",
        "label": "Encoded Bitrate (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:br",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_bl",
        "label": "Buffer Length (ms)",
        "vcl_log_expression": "req.http.x-cmcd:bl",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_bs",
        "label": "Buffer Starvation",
        "vcl_log_expression": "req.http.x-cmcd:bs",
        "collection_stage": "edge",
        "duckdb_type": "BOOLEAN",
        "value_type": "boolean",
        "bytes_estimate": 1,
        "enabled": True,
    },
    {
        "name": "cmcd_d",
        "label": "Object Duration (ms)",
        "vcl_log_expression": "req.http.x-cmcd:d",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_dl",
        "label": "Deadline (ms)",
        "vcl_log_expression": "req.http.x-cmcd:dl",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_mtp",
        "label": "Measured Throughput (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:mtp",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_ot",
        "label": "Object Type",
        "vcl_log_expression": "req.http.x-cmcd:ot",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_sf",
        "label": "Streaming Format",
        "vcl_log_expression": "req.http.x-cmcd:sf",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_st",
        "label": "Stream Type",
        "vcl_log_expression": "req.http.x-cmcd:st",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_su",
        "label": "Startup",
        "vcl_log_expression": "req.http.x-cmcd:su",
        "collection_stage": "edge",
        "duckdb_type": "BOOLEAN",
        "value_type": "boolean",
        "bytes_estimate": 1,
        "enabled": True,
    },
    {
        "name": "cmcd_tb",
        "label": "Top Bitrate (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:tb",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_rtp",
        "label": "Requested Max Throughput (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:rtp",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
]

_CMCD_FIELD_NAMES = {cf["name"] for cf in _CMCD_CUSTOM_FIELDS}


def get_cmcd_fields(enabled: bool) -> list[dict]:
    """Return CMCD custom fields if enabled, else empty list.

    Used by field registry and reconcilers to generate system fields on-demand
    based on feature toggles, without persisting them in the config file.
    """
    if enabled:
        return [dict(cf) for cf in _CMCD_CUSTOM_FIELDS]
    return []


def reconcile_cmcd_custom_fields(custom_fields: list[dict] | None, *, enabled: bool) -> list[dict]:
    """Return ``custom_fields`` with the canonical CMCD fields applied or stripped.

    CMCD fields are system-managed — code is the source of truth, and
    ``_is_system_field`` hides them from the user-editable custom-field list.
    That means any writer that persists ``log_fields`` from a list it did not
    author (a UI round-trip, a ``state_sync`` pull, a provisioning reconcile
    built from groups alone) will omit them, silently stripping CMCD from the
    generated log format while ``cmcd.enabled`` stays true. The edge keeps
    extracting CMCD into ``req.http.x-cmcd:*`` and nothing logs it, so every
    ``cmcd_*`` column ingests empty and /streaming renders all zeros with no
    error — the 2026-08-12 SE-demo incident.

    Every such writer must route its list through here, keyed on the CURRENT
    ``cmcd.enabled`` state, so enabling and disabling both converge.
    """
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _CMCD_FIELD_NAMES]
    return kept + get_cmcd_fields(enabled)
