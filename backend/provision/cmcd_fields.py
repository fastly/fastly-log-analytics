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
    error — the 2026-08-12 production incident.

    Every such writer must route its list through here, keyed on the CURRENT
    ``cmcd.enabled`` state, so enabling and disabling both converge.
    """
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _CMCD_FIELD_NAMES]
    return kept + get_cmcd_fields(enabled)


def _remove_cmcd_custom_fields(cfg: dict) -> None:
    lf = cfg.get("log_fields") or {}
    cfs = lf.get("custom_fields")
    if cfs:
        lf["custom_fields"] = [cf for cf in cfs if cf.get("name") not in _CMCD_FIELD_NAMES]


CMCD_SNIPPET_NAME = "CMCD Extraction"


def _bool_v1(padded_source: str, key: str) -> str:
    """v1 bare-key-presence: match ``,key,`` against a pre-padded source var."""
    return f'if({padded_source} ~ ",{key},", "1", "")'


def _bool_v2(source: str, key: str) -> str:
    """v2 RFC 8941 boolean: ``subfield()`` returns ``?1``."""
    return f'if(subfield({source}, "{key}", ",") == "?1", "1", "")'


def _header_mode_vcl(version: int) -> str:
    bf = _bool_v1 if version == 1 else _bool_v2
    tag = f"v{version}"

    if version == 1:
        su = bf("var.cmcd_req_pad", "su")
        bs = bf("var.cmcd_status_pad", "bs")
        pad_decl = """\

  # Pad header values with commas for bare-key boolean matching.
  declare local var.cmcd_req_pad STRING;
  set var.cmcd_req_pad = "," req.http.CMCD-Request ",";
  declare local var.cmcd_status_pad STRING;
  set var.cmcd_status_pad = "," req.http.CMCD-Status ",";
"""
    else:
        su = bf("req.http.CMCD-Request", "su")
        bs = bf("req.http.CMCD-Status", "bs")
        pad_decl = ""

    return f"""\
# CMCD {tag} header-mode extraction.
if (req.http.CMCD-Object != "" || req.http.CMCD-Request != "" || req.http.CMCD-Session != "" || req.http.CMCD-Status != "") {{
{pad_decl}
  # CMCD-Object fields
  set req.http.x-cmcd:br = subfield(req.http.CMCD-Object, "br", ",");
  set req.http.x-cmcd:d = subfield(req.http.CMCD-Object, "d", ",");
  set req.http.x-cmcd:ot = subfield(req.http.CMCD-Object, "ot", ",");
  set req.http.x-cmcd:tb = subfield(req.http.CMCD-Object, "tb", ",");

  # CMCD-Request fields
  set req.http.x-cmcd:bl = subfield(req.http.CMCD-Request, "bl", ",");
  set req.http.x-cmcd:dl = subfield(req.http.CMCD-Request, "dl", ",");
  set req.http.x-cmcd:mtp = subfield(req.http.CMCD-Request, "mtp", ",");
  set req.http.x-cmcd:su = {su};

  # CMCD-Session fields — strip quotes from string values.
  set req.http.x-cmcd:sid = regsuball(subfield(req.http.CMCD-Session, "sid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:cid = regsuball(subfield(req.http.CMCD-Session, "cid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:sf = subfield(req.http.CMCD-Session, "sf", ",");
  set req.http.x-cmcd:st = subfield(req.http.CMCD-Session, "st", ",");

  # CMCD-Status fields
  set req.http.x-cmcd:bs = {bs};
  set req.http.x-cmcd:rtp = subfield(req.http.CMCD-Status, "rtp", ",");

  # Strip CMCD headers before forwarding to origin.
  unset req.http.CMCD-Object;
  unset req.http.CMCD-Request;
  unset req.http.CMCD-Session;
  unset req.http.CMCD-Status;
}}"""


def _query_string_mode_vcl(version: int) -> str:
    bf = _bool_v1 if version == 1 else _bool_v2
    tag = f"v{version}"

    if version == 1:
        su = bf("var.cmcd_pad", "su")
        bs = bf("var.cmcd_pad", "bs")
    else:
        su = bf("var.cmcd", "su")
        bs = bf("var.cmcd", "bs")

    pad_block = (
        """
  declare local var.cmcd_pad STRING;
  set var.cmcd_pad = "," var.cmcd ",";
"""
        if version == 1
        else ""
    )

    return f"""\
# CMCD {tag} query-string-mode extraction.
declare local var.cmcd STRING;
set var.cmcd = urldecode(querystring.get(req.url, "CMCD"));

if (var.cmcd != "") {{
{pad_block}
  # CMCD-Object fields
  set req.http.x-cmcd:br = subfield(var.cmcd, "br", ",");
  set req.http.x-cmcd:d = subfield(var.cmcd, "d", ",");
  set req.http.x-cmcd:ot = subfield(var.cmcd, "ot", ",");
  set req.http.x-cmcd:tb = subfield(var.cmcd, "tb", ",");

  # CMCD-Request fields
  set req.http.x-cmcd:bl = subfield(var.cmcd, "bl", ",");
  set req.http.x-cmcd:dl = subfield(var.cmcd, "dl", ",");
  set req.http.x-cmcd:mtp = subfield(var.cmcd, "mtp", ",");
  set req.http.x-cmcd:su = {su};

  # CMCD-Session fields — strip quotes from string values.
  set req.http.x-cmcd:sid = regsuball(subfield(var.cmcd, "sid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:cid = regsuball(subfield(var.cmcd, "cid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:sf = subfield(var.cmcd, "sf", ",");
  set req.http.x-cmcd:st = subfield(var.cmcd, "st", ",");

  # CMCD-Status fields
  set req.http.x-cmcd:bs = {bs};
  set req.http.x-cmcd:rtp = subfield(var.cmcd, "rtp", ",");

  # Strip CMCD from URL to prevent cache key pollution.
  set req.url = querystring.filter(req.url, "CMCD");
}}"""


def generate_cmcd_vcl(mode: str = "query_string", version: int = 1) -> dict[str, str]:
    """Return ``{CMCD_SNIPPET_NAME: vcl_body}`` for the configured mode + version.

    ``mode`` is ``"query_string"`` (default) or ``"headers"``.
    ``version`` is ``1`` (CTA-5004) or ``2`` (CTA-5004-2).
    """
    if version not in (1, 2):
        raise ValueError(f"Unknown CMCD version: {version!r} (expected 1 or 2)")
    if mode == "headers":
        body = _header_mode_vcl(version)
    elif mode == "query_string":
        body = _query_string_mode_vcl(version)
    else:
        raise ValueError(f"Unknown CMCD mode: {mode!r} (expected 'headers' or 'query_string')")
    return {CMCD_SNIPPET_NAME: body}


def cmcd_snippet_names() -> list[str]:
    """Names of the CMCD snippets we install."""
    return [CMCD_SNIPPET_NAME]


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
    """Return ``custom_fields`` with system-managed entries re-asserted."""
    from backend.provision.session_scoring_orchestrator import (
        _SCORING_CUSTOM_FIELDS,
        _SCORING_FIELD_NAMES,
    )

    out = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    if scoring_enabled:
        out.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
    return reconcile_cmcd_custom_fields(out, enabled=cmcd_enabled)


def reconcile_cfg_system_custom_fields(cfg: dict[str, Any]) -> list[dict]:
    """Re-assert system fields on ``cfg['log_fields']['custom_fields']`` in place."""
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
