"""CMCD custom field definitions and VCL extraction snippet generators."""

from __future__ import annotations

import sys
from typing import Any

_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.pop("backend.provision.cmcd_fields", None)

_CMCD_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": "cmcd_sid",
        "label": "CMCD Session ID",
        "vcl_log_expression": "req.http.x-cmcd:sid",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
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
        "vcl_log_expression_note": "ms",
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
for _cf in _CMCD_CUSTOM_FIELDS:
    _cf.pop("vcl_log_expression_note", None)
_CMCD_FIELD_NAMES = {cf["name"] for cf in _CMCD_CUSTOM_FIELDS}


def get_cmcd_fields(enabled: bool) -> list[dict]:
    """Return CMCD custom fields if enabled, else empty list."""
    if enabled:
        return [dict(cf) for cf in _CMCD_CUSTOM_FIELDS]
    return []


def reconcile_cmcd_custom_fields(custom_fields: list[dict] | None, *, enabled: bool) -> list[dict]:
    """Return ``custom_fields`` with the canonical CMCD fields applied or stripped."""
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _CMCD_FIELD_NAMES]
    return kept + get_cmcd_fields(enabled)


def _remove_cmcd_custom_fields(cfg: dict) -> None:
    lf_dict = cfg.get("log_fields") or {}
    cfs = lf_dict.get("custom_fields")
    if cfs:
        lf_dict["custom_fields"] = [cf for cf in cfs if cf.get("name") not in _CMCD_FIELD_NAMES]


CMCD_SNIPPET_NAME = "CMCD Extraction"


def _bool_v1(padded_source: str, key: str) -> str:
    """v1 bare-key-presence: match ``,key,`` against a pre-padded source var."""
    return f'if({padded_source} ~ ",{key},", "1", "")'


def _bool_v2(source: str, key: str) -> str:
    """v2 RFC 8941 boolean: ``subfield()`` returns ``?1``."""
    return f'if(subfield({source}, "{key}", ",") == "?1", "1", "")'


def _cmcd_field_assignments_vcl(
    obj_src: str, req_src: str, sess_src: str, status_src: str, su_expr: str, bs_expr: str
) -> str:
    return f"""\
  # CMCD-Object fields
  set req.http.x-cmcd:br = subfield({obj_src}, "br", ",");
  set req.http.x-cmcd:d = subfield({obj_src}, "d", ",");
  set req.http.x-cmcd:ot = subfield({obj_src}, "ot", ",");
  set req.http.x-cmcd:tb = subfield({obj_src}, "tb", ",");

  # CMCD-Request fields
  set req.http.x-cmcd:bl = subfield({req_src}, "bl", ",");
  set req.http.x-cmcd:dl = subfield({req_src}, "dl", ",");
  set req.http.x-cmcd:mtp = subfield({req_src}, "mtp", ",");
  set req.http.x-cmcd:su = {su_expr};

  # CMCD-Session fields — strip quotes from string values.
  set req.http.x-cmcd:sid = regsuball(subfield({sess_src}, "sid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:cid = regsuball(subfield({sess_src}, "cid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:sf = subfield({sess_src}, "sf", ",");
  set req.http.x-cmcd:st = subfield({sess_src}, "st", ",");

  # CMCD-Status fields
  set req.http.x-cmcd:bs = {bs_expr};
  set req.http.x-cmcd:rtp = subfield({status_src}, "rtp", ",");"""


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
    assignments = _cmcd_field_assignments_vcl(
        "req.http.CMCD-Object", "req.http.CMCD-Request", "req.http.CMCD-Session", "req.http.CMCD-Status", su, bs
    )
    return f"""\
# CMCD {tag} header-mode extraction.
if (req.http.CMCD-Object != "" || req.http.CMCD-Request != "" || req.http.CMCD-Session != "" || req.http.CMCD-Status != "") {{
{pad_decl}
{assignments}

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
    assignments = _cmcd_field_assignments_vcl("var.cmcd", "var.cmcd", "var.cmcd", "var.cmcd", su, bs)
    return f"""\
# CMCD {tag} query-string-mode extraction.
declare local var.cmcd STRING;
set var.cmcd = urldecode(querystring.get(req.url, "CMCD"));

if (var.cmcd != "") {{
{pad_block}
{assignments}

  # Strip CMCD from URL to prevent cache key pollution.
  set req.url = querystring.filter(req.url, "CMCD");
}}"""


def generate_cmcd_vcl(mode: str = "query_string", version: int = 1) -> dict[str, str]:
    """Return ``{CMCD_SNIPPET_NAME: vcl_body}`` for the configured mode + version."""
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
    custom_fields: list[dict] | None, *, scoring_enabled: bool, cmcd_enabled: bool
) -> list[dict]:
    """Return ``custom_fields`` with system-managed entries re-asserted."""
    from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS, _SCORING_FIELD_NAMES

    out = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    if scoring_enabled:
        out.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
    return reconcile_cmcd_custom_fields(out, enabled=cmcd_enabled)


def reconcile_cfg_system_custom_fields(cfg: dict[str, Any]) -> list[dict]:
    """Re-assert system fields on ``cfg['log_fields']['custom_fields']`` in place."""
    scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
    lf_dict = cfg.get("log_fields")
    if not isinstance(lf_dict, dict):
        lf_dict = {"schema_version": 2, "custom_fields": []}
        cfg["log_fields"] = lf_dict
    reconciled = reconcile_system_custom_fields(
        lf_dict.get("custom_fields"), scoring_enabled=scoring_enabled, cmcd_enabled=cmcd_enabled
    )
    lf_dict["custom_fields"] = reconciled
    return reconciled
