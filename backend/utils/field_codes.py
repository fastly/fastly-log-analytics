"""Compact single-char encoding for fixed-enum Fastly geo fields.

Applied at the VCL level (provision.py EDGE_DATA_MAPPING) to reduce raw log size.
Decoded at ETL ingest time (ingest.py) before writing Parquet, so the query layer
always sees human-readable values.

Forward compatibility: unknown values (including new ones Fastly adds in the future)
pass through unchanged at both the VCL and SQL layers — no crash, no data loss.
"""

# ── Encode tables (human-readable → single-char code) ─────────────────────────
# Sources:
#   client.geo.conn_speed:        https://www.fastly.com/documentation/reference/vcl/variables/geolocation/client-geo-conn-speed/
#   client.geo.proxy_type:        https://www.fastly.com/documentation/reference/vcl/variables/geolocation/client-geo-proxy-type/
#   client.geo.proxy_description: https://www.fastly.com/documentation/reference/vcl/variables/geolocation/client-geo-proxy-description/

CONN_SPEED_ENCODE: dict[str, str] = {
    "broadband": "B",
    "cable": "C",
    "dialup": "D",
    "mobile": "M",
    "oc12": "2",
    "oc3": "3",
    "t1": "1",
    "t3": "T",
    "satellite": "S",
    "wireless": "W",
    "xdsl": "X",
}

PROXY_TYPE_ENCODE: dict[str, str] = {
    "anonymous": "A",
    "aol": "O",
    "blackberry": "K",
    "consumer-privacy": "P",
    "corporate": "C",
    "edu": "E",
    "hosting": "H",
    "public": "U",
    "transparent": "T",
}

PROXY_DESC_ENCODE: dict[str, str] = {
    "apple": "a",
    "cloud": "c",
    "cloud-security": "s",
    "dns": "d",
    "google": "g",
    "tor-exit": "x",
    "tor-relay": "r",
    "vpn": "v",
    "web-browser": "w",
}


def vcl_encode_chain(vcl_var: str, encode: dict[str, str]) -> str:
    """Return a VCL if/else expression that encodes known values to single chars.

    Structure:
      1. Fastly's "?" geo sentinel → "" (preserves existing behaviour)
      2. Known values → their single-char code
      3. Anything else (future Fastly additions) → vcl_var unchanged

    vcl_var must be a simple VCL variable reference (e.g. client.geo.conn_speed)
    because it is repeated verbatim as the passthrough terminal.
    """
    expr = vcl_var  # terminal: unknown new value passes through as-is
    for value, code in reversed(list(encode.items())):
        expr = f'if({vcl_var} == "{value}", "{code}", {expr})'
    # "?" is Fastly's sentinel for unresolvable geo IPs — always map to ""
    expr = f'if({vcl_var} == "?", "", {expr})'
    return expr


def duckdb_decode_case(col: str, encode: dict[str, str]) -> str:
    """Return a SQL CASE expression that maps single-char codes back to full values.

    Covers both sides of the deployment boundary:
    - New logs (post-VCL-deploy): single-char code → full value
    - Old logs (pre-VCL-deploy): full string → full string (identity)
    - Truly unknown values (future additions): pass through via ELSE
    """
    decode = {code: value for value, code in encode.items()}
    clauses = []
    for code, value in decode.items():
        clauses.append(f"WHEN \"{col}\" = '{code}' THEN '{value}'")
    for value in encode:
        clauses.append(f"WHEN \"{col}\" = '{value}' THEN '{value}'")
    return f'CASE {" ".join(clauses)} ELSE "{col}" END'
