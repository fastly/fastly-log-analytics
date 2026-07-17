"""VCL snippet generator for CMCD (Common Media Client Data) extraction.

Generates a single vcl_recv snippet that extracts CMCD fields from either
HTTP request headers (CMCD-Object, CMCD-Request, CMCD-Session, CMCD-Status)
or a query-string parameter (?CMCD=...) and stashes them as subfields of
``req.http.x-cmcd``.  The capture VCL reads these via ``req.http.x-cmcd:br``
etc. in the log format, same pattern as ``req.http.x-edge-score:*``.

Supports both CMCD v1 (CTA-5004) and v2 (CTA-5004-2):

  - **v1** booleans use bare-key-presence encoding (key present = true).
  - **v2** booleans use RFC 8941 ``?1`` / ``?0`` syntax.

All other field types (integers, tokens, quoted strings) parse identically
between versions — ``subfield()`` handles both.

Two transport modes:

  - **headers** — CMCD data in 4 HTTP request headers.
  - **query_string** — CMCD data in ``?CMCD=...`` URL parameter.
"""

from __future__ import annotations

CMCD_SNIPPET_NAME = "CMCD Extraction"
CMCD_SNIPPET_PRIORITY = -50


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
