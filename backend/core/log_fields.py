"""Log field catalog and format generator for field-aware logging configuration.

Every loggable field is defined here: its VCL expression, DuckDB type, typical
byte cost, and which insights require it.  Nothing else in the codebase should
hard-code VCL log format strings.

Usage
-----
    from backend.core.log_fields import generate_log_format, estimate_log_line_bytes, PRESETS

    cfg = {"groups": ["A", "C", "D", "I"], "field_overrides": {"referer": False}}
    fmt = generate_log_format(cfg)         # → compact single-line JSON VCL string
    size = estimate_log_line_bytes(cfg)    # → ~490

Group IDs
---------
    None  Always-on (locked, cannot be disabled)
    A     Request Identity
    B     Cache Deep-Dive
    C     Infrastructure
    D     Geolocation Basic
    E     Geolocation Precision  (requires D)
    F     Network Quality Core
    G     Network Quality Deep   (requires F)
    H     Security: TLS Fingerprinting
    I     Security: Proxy & Anonymization
    J     WAF / NGWAF
    K     QUIC / HTTP3
    L     Origin Metrics
"""

import hashlib
import re

# ---------------------------------------------------------------------------
# Fastly runtime limits (distinct from the template-size cap enforced at
# provision time in backend/provision/fastly_api.py).
#
# Per https://docs.fastly.com/products/network-services-resource-limits the
# emitted log LINE is capped at 16 KiB for Deliver services (64 KiB for
# Compute). Past the cap Fastly silently truncates — there is no error
# surfaced to the customer, so a config whose typical line lands close to
# 16 KiB will start emitting corrupt JSON the moment per-request values
# (long URLs, fat headers) push it over. The template-size gate (8000
# chars) does NOT protect against this.
#
# We compare the estimate against a headroom-aware threshold rather than the
# raw cap so configs that *typically* fit are still flagged when their
# worst-case lines have no slack. ``estimate_log_line_bytes`` returns
# average bytes; production traffic regularly drives 2-3x that on URL- or
# UA-heavy requests, so the threshold sits at ~60% of the cap.
# ---------------------------------------------------------------------------

FASTLY_LOG_LINE_DELIVER_MAX = 16 * 1024  # 16 KiB hard cap, silent truncation
FASTLY_LOG_LINE_SAFE_MAX = int(FASTLY_LOG_LINE_DELIVER_MAX * 0.60)  # 9830 bytes

# ---------------------------------------------------------------------------
# Custom field validation constants
# ---------------------------------------------------------------------------

VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")

_DUCKDB_RESERVED = frozenset(
    {
        "select",
        "from",
        "where",
        "table",
        "column",
        "index",
        "view",
        "join",
        "on",
        "as",
        "is",
        "in",
        "not",
        "null",
        "true",
        "false",
        "and",
        "or",
        "case",
        "when",
        "then",
        "else",
        "end",
        "order",
        "group",
        "by",
        "having",
        "limit",
        "offset",
        "union",
        "all",
        "distinct",
        "with",
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "add",
        "set",
        "values",
        "into",
        "exists",
        "between",
        "like",
        "ilike",
        "cast",
        "try_cast",
        "extract",
        "interval",
        "timestamp",
        "date",
        "time",
        "integer",
        "varchar",
        "boolean",
        "double",
        "bigint",
        "float",
        "struct",
        "list",
        "map",
        # Partition columns used internally
        "dt",
        "timestamp_hour",
    }
)

_DUCKDB_TYPE_VALUE_TYPE_COMPAT: dict[str, set[str]] = {
    "VARCHAR": {"string", "ip", "url"},
    "INTEGER": {"numeric"},
    "BIGINT": {"numeric"},
    "DOUBLE": {"numeric"},
    "BOOLEAN": {"boolean"},
}

# ---------------------------------------------------------------------------
# Field catalog
# ---------------------------------------------------------------------------

LOG_FIELD_CATALOG = [
    # ── Always-on ─────────────────────────────────────────────────────────
    {
        "id": "timestamp",
        "group": None,
        "label": "Timestamp",
        "description": "UTC timestamp of the request start time (ISO 8601 with timezone).",
        "vcl": '"timestamp":"%{strftime(\\{"%Y-%m-%dT%H:%M:%S%z"\\},time.start)}V"',
        "duckdb_type": "TIMESTAMP",
        "typical_bytes": 40,
        "required_by": [],
    },
    {
        "id": "ip",
        "group": None,
        "label": "Client IP",
        "description": "Client IP address. Captured at the real edge via x-fos-edge-data header.",
        "vcl": '"ip":"%{json.escape(if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, req.http.Fastly-Client-IP))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 22,
        "required_by": ["low_and_slow", "botnet_grouping"],
    },
    {
        "id": "status",
        "group": None,
        "label": "Response Status",
        "description": "HTTP response status code (e.g. 200, 404, 503).",
        "formatter": "status",
        "vcl": '"status":%{if(resp.status > 0, "" + resp.status, "null")}V',
        "duckdb_type": "USMALLINT",
        "typical_bytes": 17,
        "required_by": ["error_spikes", "city_error_spikes", "waf_signal_spikes", "image_optimization_opportunities"],
    },
    {
        "id": "elapsed",
        "group": None,
        "label": "Elapsed Time (µs)",
        "description": "Total request processing time in microseconds.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"elapsed":%{if(time.elapsed.usec != "", time.elapsed.usec, "null")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 18,
        "required_by": [
            "latency_regression",
            "city_latency_regressions",
            "network_asn_health",
            "tail_latency",
            "region_latency",
        ],
    },
    {
        "id": "cache",
        "group": None,
        "label": "Cache State",
        "description": "Fastly cache state: HIT, MISS, PASS, SYNTH, etc.",
        "vcl": '"cache":"%{json.escape(fastly_info.state)}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 18,
        "required_by": ["cache_collapse", "cache_pressure"],
    },
    {
        "id": "resp_bytes",
        "group": None,
        "label": "Response Bytes",
        "description": "Bytes delivered to the client in the response body.",
        "formatter": "bytes",
        "vcl": '"resp_bytes":%{if(resp.bytes_written > 0, "" + resp.bytes_written, "0")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 18,
        "required_by": ["cache_pressure", "network_asn_health", "image_optimization_opportunities"],
    },
    # ── Group A — Request Identity ─────────────────────────────────────────
    {
        "id": "host",
        "group": "A",
        "label": "Host",
        "description": "HTTP Host header (domain name) captured at the true client edge before any rewrites.",
        "vcl": '"host":"%{json.escape(substr(if(req.http.x-fos-edge-data:host != "", req.http.x-fos-edge-data:host, req.http.Host), 0, 512))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 22,
        "required_by": ["new_probe_urls"],
    },
    {
        "id": "url",
        "group": "A",
        "label": "URL",
        "description": "Request URL path and query string. Average ~30 bytes; varies widely.",
        "vcl": '"url":"%{json.escape(substr(req.url, 0, 2000))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 37,
        "required_by": [
            "error_spikes",
            "latency_regression",
            "new_probe_urls",
            "low_and_slow",
            "tail_latency",
            "image_optimization_opportunities",
        ],
    },
    {
        "id": "method",
        "group": "A",
        "label": "HTTP Method",
        "description": "Request method: GET, POST, HEAD, PUT, DELETE, etc.",
        "vcl": '"method":"%{json.escape(substr(req.method, 0, 128))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 19,
        "required_by": [],
    },
    {
        "id": "proto",
        "group": "A",
        "label": "HTTP Version",
        "description": "HTTP protocol version: 1.0, 1.1, 2.0, or 3.0.",
        "formatter": "number",
        "precision": 1,
        "vcl": '"proto":"%{if(req.proto != "", regsub(req.proto, "^HTTP/", ""), "")}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 15,
        "required_by": [],
    },
    {
        "id": "ua",
        "group": "A",
        "label": "User-Agent",
        "description": "Client browser or bot identifier. Largest single field — bots inflate this significantly.",
        "note": "Largest single field — bots tend to have verbose user-agents.",
        "vcl": '"ua":"%{json.escape(substr(if(req.http.x-fos-edge-data:ua != "", req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, 1000))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 90,
        "individually_toggleable": True,
        "required_by": ["ua_monoculture", "botnet_grouping", "image_optimization_opportunities"],
    },
    {
        "id": "referer",
        "group": "A",
        "label": "Referer",
        "description": "Referring URL. Often empty; useful for traffic source analysis.",
        "vcl": '"referer":"%{json.escape(substr(if(req.http.x-fos-edge-data:referer != "", req.http.x-fos-edge-data:referer, req.http.Referer), 0, 1000))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 44,
        "individually_toggleable": True,
        "required_by": [],
    },
    {
        "id": "req_bytes",
        "group": "A",
        "label": "Request Body Size",
        "description": "Request body size in bytes from Content-Length header. Zero for GET/HEAD or any request without Content-Length.",
        "formatter": "bytes",
        # Use only req.http.Content-Length (always defined at log time) and
        # regex-validate digits, so any synth/error path that never set the
        # header still renders "0" instead of empty (which would yield
        # invalid JSON like `"req_bytes":,`). We previously fell back to
        # bereq.body_bytes_written for chunked uploads, but bereq is
        # undefined on synth/restart paths and any access error there
        # collapses the entire %{...}V to "" and produces malformed lines.
        "vcl": '"req_bytes":%{if(req.http.Content-Length ~ "^[0-9]+$", req.http.Content-Length, "0")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 13,
        "required_by": ["request_size_anomaly"],
    },
    {
        "id": "req_header_bytes",
        "group": "A",
        "label": "Request Header Size",
        "description": "Total bytes in the request headers. Large values are an injection or WAF bypass signal.",
        "formatter": "bytes",
        "vcl": '"req_header_bytes":%{if(req.header_bytes_read > 0, "" + req.header_bytes_read, "0")}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 20,
        "required_by": ["request_size_anomaly"],
    },
    # ── Group B — Cache Deep-Dive ──────────────────────────────────────────
    {
        "id": "ttl",
        "group": "B",
        "label": "Object TTL",
        "description": "Time-to-live assigned by origin headers. Null when object is not cacheable.",
        "formatter": "number",
        "precision": 0,
        "unit": "s",
        # Strip the trailing "s" *and* the fractional part: Fastly's obj.ttl is
        # serialized as e.g. "3600.027s" with several µs of internal jitter, so
        # the prior `regsub(..., "s$", "")` left float keys that split Top-N
        # GROUP BY into many near-duplicate buckets. TTLs are integer seconds
        # in the underlying Cache-Control headers anyway.
        "vcl": '"ttl":%{if(obj.ttl > 0s, regsub("" + obj.ttl, "(\\.[0-9]+)?s$", ""), "null")}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 18,
        "required_by": ["cache_pressure", "cache_ttl_mismatch"],
    },
    {
        "id": "age",
        "group": "B",
        "label": "Object Age",
        "description": "How long the object has been in the Fastly cache (seconds).",
        "formatter": "number",
        "precision": 0,
        "unit": "s",
        # Same fractional-strip as ttl: obj.age comes through as "12.0s" or
        # "12.000001s" depending on the moon phase — both round to integer
        # seconds for display purposes.
        "vcl": '"age":%{if(obj.age > 0s, regsub("" + obj.age, "(\\.[0-9]+)?s$", ""), "null")}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 17,
        "required_by": ["cache_pressure", "cache_ttl_mismatch"],
    },
    {
        "id": "hits",
        "group": "B",
        "label": "Object Hit Count",
        "description": "Number of times this cached object has been served.",
        "vcl": '"hits":%{if(obj.hits > 0, "" + obj.hits, "null")}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 14,
        "required_by": ["cache_ttl_mismatch"],
    },
    {
        "id": "digest",
        "group": "B",
        "label": "Content Digest",
        "description": "Content hash for exact object identity. Required for Cache Pressure Analysis.",
        "note": "Required for Cache Pressure Analysis (eviction detection).",
        "vcl": '"digest":"%{req.digest}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 47,
        "required_by": ["cache_pressure"],
    },
    # ── Group C — Infrastructure ───────────────────────────────────────────
    {
        "id": "pop",
        "group": "C",
        "label": "Edge PoP",
        "description": "Fastly Point of Presence code (e.g. JFK, LHR, SYD).",
        "formatter": "pop",
        "vcl": '"pop":"%{server.datacenter}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 18,
        "required_by": ["cache_pressure"],
    },
    {
        "id": "backend",
        "group": "C",
        "label": "Backend",
        "description": "Origin backend name as configured in Fastly.",
        "vcl": '"backend":"%{json.escape(req.backend)}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 21,
        "required_by": [],
    },
    {
        "id": "edge",
        "group": "C",
        "label": "Edge Hit",
        "description": "True when the request hit the real edge (not a shield or restart).",
        "vcl": '"edge":%{if(fastly.ff.visits_this_service == 0, "1", "0")}V',
        "duckdb_type": "BOOLEAN",
        "typical_bytes": 9,
        "required_by": [],
    },
    {
        "id": "ttfb",
        "group": "C",
        "label": "Time to First Byte (s)",
        "description": "Seconds from request receipt to first byte of response from origin. Subtract from elapsed to isolate Fastly processing time.",
        "formatter": "number",
        "precision": 3,
        "unit": "s",
        "vcl": '"ttfb":%{if(time.to_first_byte > 0s, regsub("" + time.to_first_byte, "s$", ""), "null")}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 14,
        "required_by": ["region_latency"],
    },
    {
        "id": "server_region",
        "group": "C",
        "label": "Server Region",
        "description": "Fastly billing region of the serving PoP (e.g. NA, EU, APAC). Captured at edge for accurate attribution through shields.",
        "vcl": '"server_region":"%{json.escape(if(req.http.x-fos-edge-data:srv_region != "", req.http.x-fos-edge-data:srv_region, server.region))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 20,
        "required_by": ["region_latency"],
    },
    {
        "id": "is_ipv6",
        "group": "C",
        "label": "IPv6",
        "description": "True when the client connected over IPv6. IPv6 clients can have different routing and latency profiles.",
        "vcl": '"is_ipv6":%{if(req.http.x-fos-edge-data:is_ipv6 ~ "^[0-9]+$", req.http.x-fos-edge-data:is_ipv6, if(req.is_ipv6, "1", "0"))}V',
        "duckdb_type": "BOOLEAN",
        "typical_bytes": 12,
        "required_by": [],
    },
    {
        "id": "conn_requests",
        "group": "C",
        "label": "Conn. Request Count",
        "description": "Number of requests made on this TCP/QUIC connection. High values indicate HTTP/2 keep-alive multiplexing.",
        "vcl": '"conn_requests":%{if(req.http.x-fos-edge-data:conn_reqs ~ "^[0-9]+$", req.http.x-fos-edge-data:conn_reqs, if(client.requests > 0, "" + client.requests, "null"))}V',
        "duckdb_type": "USMALLINT",
        "typical_bytes": 20,
        "required_by": ["connection_abuse"],
    },
    {
        "id": "tls",
        "group": "C",
        "label": "TLS Version",
        "description": "TLS protocol version as a float: 1.2 or 1.3.",
        "formatter": "number",
        "precision": 1,
        "vcl": '"tls":"%{json.escape(if(req.http.x-fos-edge-data:tls != "", req.http.x-fos-edge-data:tls, if(tls.client.protocol != "", regsub(tls.client.protocol, "^TLSv", ""), "")))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 10,
        "required_by": [],
    },
    # ── Group D — Geolocation Basic ────────────────────────────────────────
    {
        "id": "country",
        "group": "D",
        "label": "Country",
        "description": "ISO 3166-1 alpha-2 country code (e.g. US, DE, JP). Enables world map.",
        "formatter": "country",
        "vcl": '"country":"%{json.escape(if(req.http.x-fos-edge-data:country != "", req.http.x-fos-edge-data:country, client.geo.country_code))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 15,
        "individually_toggleable": True,
        "required_by": [
            "new_country_traffic",
            "city_surges",
            "city_error_spikes",
            "city_latency_regressions",
            "new_city_traffic",
        ],
    },
    {
        "id": "city",
        "group": "D",
        "label": "City",
        "description": "City name from Fastly geo-IP. Variable length.",
        "formatter": "city",
        "vcl": '"city":"%{json.escape(if(req.http.x-fos-edge-data:city != "", req.http.x-fos-edge-data:city, client.geo.city))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 18,
        "individually_toggleable": True,
        "required_by": ["city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"],
    },
    {
        "id": "region",
        "group": "D",
        "label": "Region",
        "description": "ISO 3166-2 region/state/province code.",
        "formatter": "region",
        "vcl": '"region":"%{json.escape(if(req.http.x-fos-edge-data:region != "", req.http.x-fos-edge-data:region, if(client.geo.region == "?", "", client.geo.region)))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 14,
        "individually_toggleable": True,
        "required_by": [],
    },
    # ── Group E — Geolocation Precision (requires D) ───────────────────────
    {
        "id": "lat",
        "group": "E",
        "label": "Latitude",
        "description": "Client latitude (-90 to 90). Null for unresolvable IPs.",
        "formatter": "number",
        "precision": 4,
        "vcl": '"lat":%{if(req.http.x-fos-edge-data:lat ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lat, if(client.geo.country_code != "?", "" + client.geo.latitude, "null"))}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 12,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "lon",
        "group": "E",
        "label": "Longitude",
        "description": "Client longitude (-180 to 180). Null for unresolvable IPs.",
        "formatter": "number",
        "precision": 4,
        "vcl": '"lon":%{if(req.http.x-fos-edge-data:lon ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lon, if(client.geo.country_code != "?", "" + client.geo.longitude, "null"))}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 13,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "metro",
        "group": "E",
        "label": "Metro Code",
        "description": "US DMA metro area code (e.g. 501 = New York City). Empty for non-US.",
        "vcl": '"metro":%{if(req.http.x-fos-edge-data:metro ~ "^[0-9]+$", req.http.x-fos-edge-data:metro, if(client.geo.metro_code > 0, "" + client.geo.metro_code, "null"))}V',
        "duckdb_type": "USMALLINT",
        "typical_bytes": 14,
        "required_by": [],
    },
    # ── Group F — Network Quality Core ────────────────────────────────────
    {
        "id": "asn",
        "group": "F",
        "label": "ASN",
        "description": "Client Autonomous System Number (ISP identity). Enables ASN-level analysis.",
        "vcl": '"asn":%{if(req.http.x-fos-edge-data:asn ~ "^[0-9]+$", req.http.x-fos-edge-data:asn, if(client.as.number > 0, "" + client.as.number, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 11,
        "required_by": ["asn_concentration", "network_asn_health", "region_latency"],
    },
    {
        "id": "tcp_rtt",
        "group": "F",
        "label": "TCP RTT (µs)",
        "description": "TCP round-trip time in microseconds at the Fastly edge.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"tcp_rtt":%{if(req.http.x-fos-edge-data:rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt, if(client.socket.tcpi_rtt > 0, "" + client.socket.tcpi_rtt, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 19,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "transport",
        "group": "F",
        "label": "Transport Protocol",
        "description": "Transport protocol: 'tcp' or 'quic'. Low-cardinality; essentially free in Parquet.",
        "vcl": '"transport":"%{json.escape(if(req.http.x-fos-edge-data:transport != "", req.http.x-fos-edge-data:transport, transport.type))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 18,
        "required_by": ["network_asn_health"],
    },
    # ── Group G — Network Quality Deep (requires F) ────────────────────────
    {
        "id": "ploss",
        "group": "G",
        "label": "Packet Loss",
        "description": "Packet loss fraction (0.0–1.0). Direct indicator of network congestion.",
        "formatter": "percent",
        "precision": 4,
        "vcl": '"ploss":%{if(req.http.x-fos-edge-data:ploss ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:ploss, if(client.socket.ploss > 0, "" + client.socket.ploss, "null"))}V',
        "duckdb_type": "FLOAT",
        "typical_bytes": 18,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "rtt_min",
        "group": "G",
        "label": "Minimum RTT (µs)",
        "description": "Minimum RTT seen on this TCP connection (geography baseline). Delta from tcp_rtt isolates congestion.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"rtt_min":%{if(req.http.x-fos-edge-data:rtt_min ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_min, if(client.socket.tcpi_min_rtt > 0, "" + client.socket.tcpi_min_rtt, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 19,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "rtt_var",
        "group": "G",
        "label": "RTT Variance / Jitter (µs)",
        "description": "RTT variance in microseconds. Jitter causes streaming buffer stalls more than raw latency.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"rtt_var":%{if(req.http.x-fos-edge-data:rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_var, if(client.socket.tcpi_rttvar > 0, "" + client.socket.tcpi_rttvar, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 18,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "retrans",
        "group": "G",
        "label": "TCP Retransmissions",
        "description": "TCP retransmission delta since previous sample. Direct congestion signal.",
        "formatter": "number",
        "vcl": '"retrans":%{if(req.http.x-fos-edge-data:retrans ~ "^[0-9]+$", req.http.x-fos-edge-data:retrans, if(client.socket.tcpi_delta_retrans > 0, "" + client.socket.tcpi_delta_retrans, "null"))}V',
        "duckdb_type": "UTINYINT",
        "typical_bytes": 15,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "bw",
        "group": "K",
        "label": "Bandwidth Estimate",
        "description": "Fastly's estimated bandwidth for this connection (bytes/sec or bits/sec — see note). Only applicable for QUIC; TCP connections should use delivery_rate instead.",
        "formatter": "bytes",
        "vcl": '"bw":%{if(req.http.x-fos-edge-data:bw ~ "^[0-9]+$", req.http.x-fos-edge-data:bw, if(transport.bw_estimate > 0, "" + transport.bw_estimate, "null"))}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 17,
        "required_by": [],
    },
    {
        "id": "c_speed",
        "group": "G",
        "label": "Connection Speed Class",
        "description": "Geo-IP speed classification: broadband, cable, dsl, mobile, satellite, dialup. Low-cardinality.",
        "vcl": '"c_speed":"%{json.escape(if(req.http.x-fos-edge-data:c_speed != "", req.http.x-fos-edge-data:c_speed, if(client.geo.conn_speed == "?", "", client.geo.conn_speed)))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 14,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "c_type",
        "group": "G",
        "label": "Connection Type",
        "description": "Geo-IP connection type: residential, commercial, cellular, corporate. Low-cardinality.",
        "vcl": '"c_type":"%{json.escape(if(req.http.x-fos-edge-data:c_type != "", req.http.x-fos-edge-data:c_type, if(client.geo.conn_type == "?", "", client.geo.conn_type)))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 27,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "delivery_rate",
        "group": "G",
        "label": "TCP Delivery Rate",
        "description": "Actual TCP delivery rate in bytes/sec measured by the kernel. More reliable than bandwidth estimate for TCP connections.",
        "formatter": "bytes",
        "vcl": '"delivery_rate":%{if(req.http.x-fos-edge-data:del_rate ~ "^[0-9]+$", req.http.x-fos-edge-data:del_rate, if(client.socket.tcpi_delivery_rate > 0, "" + client.socket.tcpi_delivery_rate, "null"))}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 22,
        "required_by": ["network_asn_health"],
    },
    {
        "id": "data_segs_out",
        "group": "G",
        "label": "TCP Data Segments Out",
        "description": "Total TCP data segments sent on this connection. Enables retransmit ratio: retrans / data_segs_out.",
        "formatter": "number",
        "vcl": '"data_segs_out":%{if(req.http.x-fos-edge-data:data_segs ~ "^[0-9]+$", req.http.x-fos-edge-data:data_segs, if(client.socket.tcpi_data_segs_out > 0, "" + client.socket.tcpi_data_segs_out, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 21,
        "required_by": ["network_asn_health"],
    },
    # ── Group H — Security: TLS Fingerprinting ────────────────────────────
    {
        "id": "ja3",
        "group": "H",
        "label": "JA3 Fingerprint",
        "description": "MD5 TLS client fingerprint. Older standard; widely supported. 41 bytes avg.",
        "vcl": '"ja3":"%{json.escape(if(req.http.x-fos-edge-data:ja3 != "", req.http.x-fos-edge-data:ja3, tls.client.ja3_md5))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 41,
        "individually_toggleable": True,
        "required_by": ["botnet_grouping"],
    },
    {
        "id": "ja4",
        "group": "H",
        "label": "JA4 Fingerprint",
        "description": "Newer, richer TLS fingerprint standard. 43 bytes avg.",
        "vcl": '"ja4":"%{json.escape(if(req.http.x-fos-edge-data:ja4 != "", req.http.x-fos-edge-data:ja4, tls.client.ja4))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 43,
        "individually_toggleable": True,
        "required_by": ["botnet_grouping"],
    },
    {
        "id": "tls_ciphers_sha",
        "group": "H",
        "label": "TLS Cipher Suite SHA",
        "description": "SHA fingerprint of the client's offered cipher suite list. Evasion-resistant complement to JA3/JA4 for bot farm detection.",
        "vcl": '"tls_ciphers_sha":"%{json.escape(if(req.http.x-fos-edge-data:tls_csha != "", req.http.x-fos-edge-data:tls_csha, tls.client.ciphers_list_sha))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 48,
        "individually_toggleable": True,
        "required_by": ["cipher_spread"],
    },
    # ── Group I — Security: Proxy & Anonymization ─────────────────────────
    {
        "id": "p_type",
        "group": "I",
        "label": "Proxy Type",
        "description": "Anonymizing proxy type: VPN, Tor, DCH (data center), etc.",
        "vcl": '"p_type":"%{json.escape(if(req.http.x-fos-edge-data:p_type != "", req.http.x-fos-edge-data:p_type, if(client.geo.proxy_type == "?", "", client.geo.proxy_type)))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 10,
        "required_by": ["proxy_surge"],
    },
    {
        "id": "p_desc",
        "group": "I",
        "label": "Proxy Description",
        "description": "Anonymizing proxy provider name.",
        "vcl": '"p_desc":"%{json.escape(if(req.http.x-fos-edge-data:p_desc != "", req.http.x-fos-edge-data:p_desc, if(client.geo.proxy_description == "?", "", client.geo.proxy_description)))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 10,
        "required_by": ["proxy_surge"],
    },
    # ── Group J — WAF / NGWAF ─────────────────────────────────────────────
    {
        "id": "waf",
        "group": "J",
        "label": "WAF Executed",
        "description": "Whether NGWAF (Signal Sciences) processed this request.",
        "vcl": '"waf":%{if(waf.executed, "1", "0")}V',
        "duckdb_type": "BOOLEAN",
        "typical_bytes": 8,
        "required_by": ["waf_signal_spikes"],
    },
    {
        "id": "waf_resp",
        "group": "J",
        "label": "WAF Agent Response",
        "description": "NGWAF agent decision code (HTTP status equivalent).",
        "formatter": "status",
        "vcl": '"waf_resp":%{if(waf.executed, if(req.http.x-sigsci-agentresponse, req.http.x-sigsci-agentresponse, "null"), "null")}V',
        "duckdb_type": "USMALLINT",
        "typical_bytes": 16,
        "required_by": ["waf_signal_spikes"],
    },
    {
        "id": "waf_ms",
        "group": "J",
        "label": "WAF Latency (ms)",
        "description": "Milliseconds the NGWAF inspection added to the request.",
        "formatter": "number",
        "unit": "ms",
        "vcl": '"waf_ms":%{if(waf.executed, if(req.http.x-sigsci-decision-ms, req.http.x-sigsci-decision-ms, "null"), "null")}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 13,
        "required_by": [],
    },
    {
        "id": "waf_sig",
        "group": "J",
        "label": "WAF Signal Tags",
        "description": "NGWAF signal tags (e.g. SQLI, XSS, CMDEXE).",
        "vcl": '"waf_sig":"%{if(waf.executed, if(req.http.x-sigsci-tags != "", json.escape(req.http.x-sigsci-tags), ""), "")}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 13,
        "required_by": ["waf_signal_spikes"],
    },
    {
        "id": "waf_req_id",
        "group": "J",
        "label": "WAF Request ID",
        "description": "NGWAF request correlation ID for cross-referencing with Signal Sciences.",
        "vcl": '"waf_req_id":"%{if(waf.executed, if(req.http.x-fastly-ngwaf:requestid != "", json.escape(req.http.x-fastly-ngwaf:requestid), if(req.http.x-sigsci-requestid != "", json.escape(req.http.x-sigsci-requestid), "")), "")}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 16,
        "required_by": [],
    },
    # ── Group K — QUIC / HTTP3 ────────────────────────────────────────────
    {
        "id": "q_rtt",
        "group": "K",
        "label": "QUIC Smoothed RTT (µs)",
        "description": "QUIC smoothed RTT in microseconds. Null for TCP connections.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"q_rtt":%{if(req.http.x-fos-edge-data:q_rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt, if(transport.type == "quic", "" + quic.rtt.smoothed, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 19,
        "required_by": [],
    },
    {
        "id": "q_rtt_var",
        "group": "K",
        "label": "QUIC RTT Variance (µs)",
        "description": "QUIC RTT variance in microseconds. Null for TCP connections.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"q_rtt_var":%{if(req.http.x-fos-edge-data:q_rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt_var, if(transport.type == "quic", "" + quic.rtt.variance, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 19,
        "required_by": [],
    },
    {
        "id": "q_lost",
        "group": "K",
        "label": "QUIC Packets Lost",
        "description": "QUIC packets lost counter. Null for TCP connections.",
        "formatter": "number",
        "vcl": '"q_lost":%{if(req.http.x-fos-edge-data:q_lost ~ "^[0-9]+$", req.http.x-fos-edge-data:q_lost, if(transport.type == "quic", "" + quic.num_packets.lost, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 17,
        "required_by": [],
    },
    {
        "id": "q_cwnd",
        "group": "K",
        "label": "QUIC Congestion Window",
        "description": "QUIC congestion window size. Null for TCP connections.",
        "formatter": "number",
        "vcl": '"q_cwnd":%{if(req.http.x-fos-edge-data:q_cwnd ~ "^[0-9]+$", req.http.x-fos-edge-data:q_cwnd, if(transport.type == "quic", "" + quic.cc.cwnd, "null"))}V',
        "duckdb_type": "UINTEGER",
        "typical_bytes": 16,
        "required_by": [],
    },
    # ── Group L — Origin Metrics ───────────────────────────────────────────
    # Security: each origin-metric field interpolates the value of a
    # client-spoofable internal header (``x-of-ttfb`` etc.). Without a
    # regex guard on the value, an attacker who reached vcl_recv with a
    # crafted header like ``x-of-ttfb: 0, "waf": 1`` would break out of
    # the unquoted numeric slot and inject arbitrary JSON keys into the
    # log line. The ``~ "^[0-9]+$"`` test gates each numeric field to
    # digit-only values; ``x-of-oip`` (the only string field) gets
    # ``json.escape(...)`` so quotes / backslashes / control bytes
    # serialize as their JSON-escape equivalents instead of breaking
    # out of the string literal. the earlier fix also unsets all
    # these headers on inbound req, so this is belt-and-suspenders.
    {
        "id": "ottfb",
        "group": "L",
        "label": "Origin TTFB (µs)",
        "description": "µs from fetch start to first byte of origin/shield response headers. Null on HITs.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"ottfb":%{if(req.http.x-of-ttfb ~ "^[0-9]+$", req.http.x-of-ttfb, "null")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 16,
        "required_by": ["origin_latency_spike", "region_latency"],
    },
    {
        "id": "ottlb",
        "group": "L",
        "label": "Origin TTLB (µs)",
        "description": "µs from fetch start to full response body received. Null on HITs.",
        "formatter": "number",
        "unit": "µs",
        "vcl": '"ottlb":%{if(req.http.x-of-ttlb ~ "^[0-9]+$", req.http.x-of-ttlb, "null")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 16,
        "required_by": ["origin_latency_spike"],
    },
    {
        "id": "ost",
        "group": "L",
        "label": "Origin Status",
        "description": "HTTP status returned by origin or shield. Null on HITs.",
        "formatter": "status",
        "vcl": '"ost":%{if(req.http.x-of-status ~ "^[0-9]+$", req.http.x-of-status, "null")}V',
        "duckdb_type": "USMALLINT",
        "typical_bytes": 10,
        "required_by": ["origin_error_rate", "origin_ip_failure"],
    },
    {
        "id": "obytes",
        "group": "L",
        "label": "Origin Bytes",
        "description": "Bytes written in the response (resp.bytes_written). Null on HITs. Same variable as resp_bytes but null-on-HIT makes it queryable as 'total bytes fetched from origin'.",
        # resp.bytes_written is a Fastly-internal counter (not from a header),
        # so no JSON-injection risk; the x-of-start guard is preserved as-is.
        "vcl": '"obytes":%{if(req.http.x-of-start ~ "^[0-9]+$", "" + resp.bytes_written, "null")}V',
        "duckdb_type": "UBIGINT",
        "typical_bytes": 15,
        "required_by": [],
    },
    {
        "id": "oip",
        "group": "L",
        "label": "Origin IP",
        "description": "IP address of the backend server that handled the fetch. Null on HITs.",
        # json.escape converts the value to JSON-string-safe form so
        # quotes / backslashes / control bytes get their \\uXXXX escapes
        # instead of terminating the literal early.
        "vcl": '"oip":"%{json.escape(if(req.http.x-of-oip, req.http.x-of-oip, ""))}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 15,
        "required_by": ["origin_ip_failure"],
    },
    {
        "id": "oretries",
        "group": "L",
        "label": "Origin Retries",
        "description": "Backend connection retry count before success or failure. Null on HITs.",
        "formatter": "number",
        "vcl": '"oretries":%{if(req.http.x-of-oretries ~ "^[0-9]+$", req.http.x-of-oretries, "null")}V',
        "duckdb_type": "UTINYINT",
        "typical_bytes": 13,
        "required_by": ["origin_retries"],
    },
    {
        "id": "rid",
        "group": "L",
        "label": "Request ID",
        "description": "8-char random ID generated at this POP. Always set. Use with prid to correlate edge + shield log lines.",
        "vcl": '"rid":"%{req.http.x-req-id}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 16,
        "required_by": [],
    },
    {
        "id": "prid",
        "group": "L",
        "label": "Parent Request ID",
        "description": "Edge POP's rid forwarded to the shield. Non-null only on shield log lines (edge=0, cache=MISS).",
        "vcl": '"prid":"%{req.http.x-edge-req-id}V"',
        "duckdb_type": "VARCHAR",
        "typical_bytes": 16,
        "required_by": [],
    },
    # ── Metrics ───────────────────────────────────────────────────────────
    {
        "id": "requests",
        "group": "METRICS",
        "label": "Requests",
        "description": "Total number of requests.",
        "formatter": "number",
        "vcl": None,
        "duckdb_type": "BIGINT",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "hit_rate",
        "group": "METRICS",
        "label": "Cache Hit Rate",
        "description": "Percentage of requests served from cache (HIT or HIT-STALE).",
        "formatter": "percent",
        "unit": "%",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "5xx",
        "group": "METRICS",
        "label": "5xx Errors",
        "description": "Percentage of requests with 5xx status codes.",
        "formatter": "percent",
        "unit": "%",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "4xx",
        "group": "METRICS",
        "label": "4xx Errors",
        "description": "Percentage of requests with 4xx status codes.",
        "formatter": "percent",
        "unit": "%",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "p50_latency",
        "group": "METRICS",
        "label": "P50 Latency",
        "description": "Median request processing time (milliseconds).",
        "formatter": "number",
        "unit": "ms",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "p95_latency",
        "group": "METRICS",
        "label": "P95 Latency",
        "description": "95th percentile request processing time (milliseconds).",
        "formatter": "number",
        "unit": "ms",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "p99_latency",
        "group": "METRICS",
        "label": "P99 Latency",
        "description": "99th percentile request processing time (milliseconds).",
        "formatter": "number",
        "unit": "ms",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "throughput",
        "group": "METRICS",
        "label": "Throughput",
        "description": "Estimated bandwidth delivered for cache hits (bytes/second).",
        "formatter": "bytes",
        "unit": "B/s",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "req_size",
        "group": "METRICS",
        "label": "Request Size",
        "description": "Median total request size (headers + body).",
        "formatter": "bytes",
        "unit": "B",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "ttfb_ms",
        "group": "METRICS",
        "label": "TTFB",
        "description": "Median time to first byte (milliseconds).",
        "formatter": "number",
        "unit": "ms",
        "vcl": None,
        "duckdb_type": "DOUBLE",
        "typical_bytes": 0,
        "required_by": [],
    },
    # ── Virtual ───────────────────────────────────────────────────────────
    {
        "id": "_bot_name",
        "group": "VIRTUAL",
        "label": "Fastly Bots",
        "description": "Virtual field derived from User-Agent and IP to identify known bots.",
        "vcl": None,
        "duckdb_type": "VARCHAR",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "_ngwaf_bot_name",
        "group": "VIRTUAL",
        "label": "NGWAF Verified Bots",
        "description": "Virtual field enriched with NGWAF bot signal data.",
        "vcl": None,
        "duckdb_type": "VARCHAR",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "waf_sig_ind",
        "group": "VIRTUAL",
        "label": "NGWAF Signals",
        "description": "Individual NGWAF signals extracted from the waf_sig list.",
        "vcl": None,
        "duckdb_type": "VARCHAR",
        "typical_bytes": 0,
        "required_by": [],
    },
    {
        "id": "edge_score_reason_ind",
        "group": "VIRTUAL",
        "label": "Score Reasons",
        "description": (
            "Individual scoring reasons extracted from the comma-separated "
            "edge_score_reason field (e.g. 'cookie-missing', 'impossibly-fast', "
            "'robotic-consistency', 'rare-transition'). Lets the dashboard "
            "show top-N reason breakdowns and filter by a single reason "
            "even when one request triggers multiple."
        ),
        "vcl": None,
        "duckdb_type": "VARCHAR",
        "typical_bytes": 0,
        "required_by": [],
    },
    # ── Internal ──────────────────────────────────────────────────────────
    {
        "id": "_source_file",
        "group": "INTERNAL",
        "label": "Source File",
        "description": "Original raw log file in Fastly Object Storage.",
        "vcl": None,
        "duckdb_type": "VARCHAR",
        "typical_bytes": 60,
        "required_by": [],
    },
]

# ---------------------------------------------------------------------------
# Group metadata
# ---------------------------------------------------------------------------

GROUP_INFO = {
    None: {
        "label": "Core Delivery",
        "description": "Always-on fields required for basic metrics: error rates, latency, hit rates, throughput.",
        "locked": True,
        "requires": None,
    },
    "A": {
        "label": "Request Identity",
        "description": "Host, URL, HTTP method/version, User-Agent, Referer, and request body size.",
        "locked": False,
        "requires": None,
    },
    "B": {
        "label": "Cache Deep-Dive",
        "description": "TTL, age, hit count, and content digest. Enable for cache pressure analysis.",
        "locked": False,
        "requires": None,
    },
    "C": {
        "label": "Infrastructure",
        "description": "Edge PoP, backend, edge/shield flag, TTFB, TLS version, billing region, IPv6 flag, and connection request count.",
        "locked": False,
        "requires": None,
    },
    "D": {
        "label": "Geolocation — Basic",
        "description": "Country, city, and region. Country alone enables the world map.",
        "locked": False,
        "requires": None,
    },
    "E": {
        "label": "Geolocation — Precision",
        "description": "Latitude, longitude, and US metro code. Requires Basic Geolocation.",
        "locked": False,
        "requires": "D",
    },
    "F": {
        "label": "Network Quality — Core",
        "description": "ASN (ISP identity), TCP RTT, and transport protocol.",
        "locked": False,
        "requires": None,
    },
    "G": {
        "label": "Network Quality — Deep",
        "description": "Packet loss, RTT variance/jitter, retransmissions, TCP delivery rate, data segments, and connection type. Requires Network Core.",
        "locked": False,
        "requires": "F",
    },
    "H": {
        "label": "Security: TLS Fingerprinting",
        "description": "JA3, JA4, TLS handshake failure codes, and cipher suite fingerprints for botnet grouping and scanner detection.",
        "locked": False,
        "requires": None,
    },
    "I": {
        "label": "Security: Proxy Detection",
        "description": "Anonymizing proxy type and provider name (VPN, Tor, DCH).",
        "locked": False,
        "requires": None,
    },
    "J": {
        "label": "WAF / NGWAF",
        "description": "Signal Sciences / NGWAF fields. All null if NGWAF is not deployed on this service.",
        "locked": False,
        "requires": None,
        "note": "All fields are null/empty if NGWAF is not deployed on this service.",
    },
    "K": {
        "label": "QUIC / HTTP3",
        "description": "QUIC-specific RTT, variance, packet loss, congestion window, and bandwidth estimate. All null for TCP connections.",
        "locked": False,
        "requires": None,
        "note": "All fields are null for TCP connections. Only useful if your service has meaningful HTTP/3 traffic.",
    },
    "L": {
        "label": "Origin Metrics",
        "description": "Origin/shield fetch timing, bytes, IP, and retries on cache misses and passes. VCL hooks applied automatically. ottfb/ottlb/ost/obytes/oip/oretries are null on HITs; rid is always set; prid set only on shield log lines.",
        "locked": False,
        "requires": None,
        "note": "Enabling this group deploys additional VCL timing snippets to your service automatically.",
        "recommended_with": ["C"],
    },
    "METRICS": {
        "label": "Aggregate Metrics",
        "description": "Computed aggregate metrics used for charts and dashboards.",
        "locked": True,
        "requires": None,
    },
    "VIRTUAL": {
        "label": "Virtual Fields",
        "description": "Derived or enriched fields that are not present in the raw logs but computed during analysis.",
        "locked": True,
        "requires": None,
    },
}

# Group dependency rules: group → required group
GROUP_DEPENDENCIES = {g: info["requires"] for g, info in GROUP_INFO.items() if info.get("requires")}

# ---------------------------------------------------------------------------
# Preset bundles
# ---------------------------------------------------------------------------

PRESETS = {
    "minimal": {
        "label": "Minimal",
        "description": "Always-on fields only. Error rates, latency, hit rates, throughput.",
        "groups": [],
    },
    "standard": {
        "label": "Standard",
        "description": "Recommended for most sites. Request details, infrastructure, basic geo, proxy detection.",
        "groups": ["A", "C", "D", "I"],
    },
    "security": {
        "label": "Security",
        "description": "Standard + TLS fingerprinting and WAF. For security monitoring.",
        "groups": ["A", "C", "D", "H", "I", "J"],
        "field_overrides": {"tls_ciphers_sha": True},
    },
    "performance": {
        "label": "Performance",
        "description": "Standard + cache deep-dive, network quality core, and origin metrics. For delivery optimization.",
        "groups": ["A", "B", "C", "D", "F", "L"],
    },
    "streaming": {
        "label": "Streaming",
        "description": "Standard + precision geo and full network telemetry. For streaming video analysis.",
        "groups": ["A", "C", "D", "E", "F", "G"],
    },
    "full": {
        "label": "Full",
        "description": "All groups enabled. Maximum data collection.",
        "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"],
    },
}

# ---------------------------------------------------------------------------
# Insight definitions
# ---------------------------------------------------------------------------

INSIGHT_DEFINITIONS = [
    {
        "id": "error_spikes",
        "title": "Error Spikes",
        "description": "URLs with abnormally elevated 5xx error rates in the window vs. baseline",
        "required_fields": ["status", "url"],
        "required_groups": ["A"],
    },
    {
        "id": "botnet_grouping",
        "title": "Botnet Grouping",
        "description": "TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline — attackers rotate IPs but rarely change TLS stacks",
        "required_fields": ["ja3", "ja4"],
        "required_groups": ["H"],
    },
    {
        "id": "low_and_slow",
        "title": "Low and Slow Scans",
        "description": "IPs making few, spread-out requests to admin panels and known vulnerability paths — designed to evade rate limits",
        "required_fields": ["ip", "url"],
        "required_groups": ["A"],
    },
    {
        "id": "city_surges",
        "title": "City Traffic Surges",
        "description": "Cities with traffic volumes significantly higher than their historical baseline",
        "required_fields": ["city", "country"],
        "required_groups": ["D"],
    },
    {
        "id": "city_error_spikes",
        "title": "City Error Spikes",
        "description": "Cities experiencing abnormally high error rates compared to their own baseline",
        "required_fields": ["city", "status"],
        "required_groups": ["D"],
    },
    {
        "id": "city_latency_regressions",
        "title": "City Latency Regressions",
        "description": "Cities where response times (P95) have significantly slowed down compared to their baseline",
        "required_fields": ["city", "elapsed"],
        "required_groups": ["D"],
    },
    {
        "id": "new_city_traffic",
        "title": "New City Traffic",
        "description": "Cities with zero baseline presence now sending traffic",
        "required_fields": ["city"],
        "required_groups": ["D"],
    },
    {
        "id": "new_country_traffic",
        "title": "New Country Traffic",
        "description": "Countries with zero baseline presence now sending traffic",
        "required_fields": ["country"],
        "required_groups": ["D"],
    },
    {
        "id": "latency_regression",
        "title": "URL Latency Regressions",
        "description": "URLs where response times (P95) have significantly slowed down compared to their baseline",
        "required_fields": ["url", "elapsed"],
        "required_groups": ["A"],
    },
    {
        "id": "asn_concentration",
        "title": "ASN Concentration",
        "description": "ISPs (ASNs) with a disproportionately large share of total traffic compared to the baseline",
        "required_fields": ["asn"],
        "required_groups": ["F"],
    },
    {
        "id": "proxy_surge",
        "title": "Proxy Traffic Surge",
        "description": "Significant increase in traffic from known anonymizing proxies (VPN, Tor, etc.)",
        "required_fields": ["p_type"],
        "required_groups": ["I"],
    },
    {
        "id": "ua_monoculture",
        "title": "User-Agent Monoculture",
        "description": "A single User-Agent string responsible for a massive percentage of traffic — typical for scraping or DDoS bots",
        "required_fields": ["ua"],
        "required_groups": ["A"],
    },
    {
        "id": "request_size_anomaly",
        "title": "Request Size Anomalies",
        "description": "Drastic increase in average request body or header size — signal for data exfiltration or buffer overflow attempts",
        "required_fields": ["req_bytes", "req_header_bytes"],
        "required_groups": ["A"],
    },
    {
        "id": "cache_ttl_mismatch",
        "title": "Cache TTL Mismatches",
        "description": "Objects being served from cache with very low hits and low TTLs — indicates inefficient caching strategy",
        "required_fields": ["cache", "ttl", "age", "hits"],
        "required_groups": ["B"],
    },
    {
        "id": "waf_signal_spikes",
        "title": "WAF Signal Spikes",
        "description": "Abnormal increase in specific NGWAF signals (e.g. SQLi, XSS) across multiple IPs",
        "required_fields": ["waf", "waf_sig", "waf_resp", "status"],
        "required_groups": ["J"],
    },
    {
        "id": "network_asn_health",
        "title": "Network Path (ASN) Health",
        "description": "ASNs experiencing packet loss or high jitter spikes vs. baseline",
        "required_fields": [
            "asn",
            "tcp_rtt",
            "transport",
            "ploss",
            "rtt_var",
            "rtt_min",
            "retrans",
            "c_speed",
            "c_type",
            "delivery_rate",
            "data_segs_out",
            "lat",
            "lon",
            "elapsed",
            "resp_bytes",
        ],
        "required_groups": ["F", "G", "E"],
    },
    {
        "id": "region_latency",
        "title": "Billing Region Latency",
        "description": "Fastly regions showing elevated edge latency or TTFB spikes",
        "required_fields": ["server_region", "elapsed", "ttfb", "asn", "ottfb"],
        "required_groups": ["C", "F", "L"],
    },
]

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def resolve_enabled_fields(cfg: dict) -> set:
    """Expand group selections and per-field overrides into a flat set of enabled field IDs."""
    if cfg is None:
        # Default to standard groups if no config provided
        cfg = {"groups": PRESETS["standard"]["groups"], "field_overrides": {}}

    # Start with always-on fields
    enabled = {f["id"] for f in LOG_FIELD_CATALOG if f["group"] is None}

    # Add all fields from enabled groups (respecting dependency order)
    enabled_groups = set(cfg.get("groups", []))

    # Enforce dependencies: if a group requires another group, auto-enable it
    changed = True
    while changed:
        changed = False
        for grp, required in GROUP_DEPENDENCIES.items():
            if grp in enabled_groups and required not in enabled_groups:
                enabled_groups.add(required)
                changed = True

    for field in LOG_FIELD_CATALOG:
        if field["group"] in enabled_groups:
            enabled.add(field["id"])

    # Apply per-field overrides
    for field_id, on in cfg.get("field_overrides", {}).items():
        if on:
            enabled.add(field_id)
        else:
            enabled.discard(field_id)

    return enabled


def get_required_edge_headers(log_fields_config: dict) -> set:
    """Return the set of x-fos-edge-data keys required by the enabled log fields.

    Analyzes the VCL expressions of all enabled fields to determine which
    subfields of the x-fos-edge-data header must be captured at the edge.
    """
    enabled = resolve_enabled_fields(log_fields_config)
    required = set()
    # Regex to find x-fos-edge-data subfields in VCL expressions
    pattern = re.compile(r"req\.http\.x-fos-edge-data:([a-z0-9_]+)")
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            matches = pattern.findall(field["vcl"])
            required.update(matches)
    return required


def generate_log_format(log_fields_config: dict) -> str:
    """Build the VCL log format string from a log_fields config dict.

    Returns a compact single-line JSON string suitable for Fastly's logging endpoint.
    Fastly's S3/FOS logging endpoint emits one JSON object per line, so the format
    must not contain internal newlines — DuckDB's newline_delimited reader requires this.
    """
    enabled = resolve_enabled_fields(log_fields_config)
    limits = log_fields_config.get("field_limits") or {}

    parts = []
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            vcl = field["vcl"]
            if vcl is None:
                continue
            # Inject dynamic limits
            if field["id"] == "url":
                limit = limits.get("url", 2000)
                # Overwrite the static substr limit in the built-in VCL
                vcl = vcl.replace("substr(req.url, 0, 2000)", f"substr(req.url, 0, {limit})")
            elif field["id"] == "ua":
                # Security: keep the substr cap even when generating the
                # alternative VCL variant. The edge-side substr (in vcl_recv)
                # is a *first* truncation — but we never want a 100 KB header
                # to slip through if the edge snippet is missing or fails to
                # run (e.g., on a request that bypasses our snippet stack).
                # An unbounded UA can truncate the entire JSON log line at
                # the 16 KB Fastly limit, dropping the request from the audit
                # trail entirely (repudiation attack).
                ua_limit = limits.get("ua", 1000)
                vcl = (
                    f'"ua":"%{{json.escape(substr(if(req.http.x-fos-edge-data:ua != "",'
                    f' req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, {ua_limit}))}}V"'
                )
            elif field["id"] == "referer":
                # Same reasoning as above — keep the substr cap.
                ref_limit = limits.get("referer", 1000)
                vcl = (
                    f'"referer":"%{{json.escape(substr(if(req.http.x-fos-edge-data:referer != "",'
                    f' req.http.x-fos-edge-data:referer, req.http.Referer), 0, {ref_limit}))}}V"'
                )

            parts.append(vcl)

    # Append enabled custom fields in alphabetical order for determinism
    for cf in sorted(log_fields_config.get("custom_fields", []), key=lambda x: x["name"]):
        if not cf.get("enabled", True):
            continue

        name = cf["name"]
        stage = cf.get("collection_stage", "edge")
        value_type = cf.get("value_type", "string")

        if stage == "deliver":
            # Deliver-stage fields (session-scoring) need TWO gates:
            #   1. edge-only (fastly.ff.visits_this_service == 0) — the
            #      shield POP never ran our scoring snippets, so the
            #      req.http subfields don't exist there.
            #   2. non-empty value — avoid breaking JSON.
            # Combined into ONE if() with compound AND so we don't end up
            # with nested if(if(...) != "", ...) which Fastly's parser
            # rejects ("if() condition must be a simple expression, not a
            # function call").
            raw_expr = cf.get("vcl_log_expression") or f"req.http.x-fos-edge-data:{name}"
            if value_type in ("numeric", "boolean"):
                # 014: ``!= ""`` only rejects empty strings — any other
                # text (`"true"`, ``"abc"``, ``"]"``) flows straight into
                # the JSON log line unquoted and breaks the JSON
                # structure, dropping the line from ingestion (log
                # injection / repudiation). Match a strict numeric form
                # so non-digit values fall through to ``"null"``.
                vcl_macro = (
                    f"if(fastly.ff.visits_this_service == 0 && "
                    f'{raw_expr} ~ "^-?[0-9]+(\\.[0-9]+)?$", {raw_expr}, "null")'
                )
                entry = f'"{name}":%{{{vcl_macro}}}V'
            else:
                # 016: clamp the string-field value to a sane length
                # (default 2000) BEFORE json.escape so a multi-megabyte
                # attacker-controlled custom field cannot push the log
                # line past Fastly's 16 KB limit and silently drop the
                # whole entry. The substr is INSIDE json.escape so the
                # encoded length stays bounded.
                cf_limit = int(cf.get("byte_limit") or limits.get(name) or 2000)
                vcl_macro = (
                    f'json.escape(if(fastly.ff.visits_this_service == 0, substr({raw_expr}, 0, {cf_limit}), ""))'
                )
                entry = f'"{name}":"%{{{vcl_macro}}}V"'
            parts.append(entry)
            continue

        if stage == "edge":
            expr = f"req.http.x-fos-edge-data:{name}"
        elif stage == "origin":
            expr = f"req.http.x-fos-origin-data:{name}"
        else:
            # Fallback if there's old data
            expr = f"req.http.x-fos-edge-data:{name}"

        if value_type in ("numeric", "boolean"):
            # 014: see deliver-stage comment above — strict numeric
            # regex instead of ``!= ""`` so a custom-field header value
            # like ``"]"`` cannot break out of the JSON log line.
            vcl_macro = f'if({expr} ~ "^-?[0-9]+(\\.[0-9]+)?$", {expr}, "null")'
            entry = f'"{name}":%{{{vcl_macro}}}V'
        else:
            # 016: substr-clamp the value before json.escape so an
            # oversized custom string field cannot push the line past
            # Fastly's 16 KB log-line limit.
            cf_limit = int(cf.get("byte_limit") or limits.get(name) or 2000)
            vcl_macro = f"json.escape(substr({expr}, 0, {cf_limit}))"
            entry = f'"{name}":"%{{{vcl_macro}}}V"'

        parts.append(entry)

    raw = "{" + ",".join(parts) + "}"
    # Collapse all whitespace to single spaces (same as the old load_log_format did)
    return re.sub(r"\s+", " ", raw).strip()


def estimate_log_line_bytes(log_fields_config: dict) -> int:
    """Return the estimated average uncompressed log line size in bytes."""
    enabled = resolve_enabled_fields(log_fields_config)
    field_bytes = sum(f["typical_bytes"] for f in LOG_FIELD_CATALOG if f["id"] in enabled)
    # JSON structural overhead: braces (2) + key quotes (2 per field) + colon (1) + comma+space (2 per field except last)
    structural = 2 + len(enabled) * 5

    # Add enabled custom fields
    custom_fields = log_fields_config.get("custom_fields", [])
    custom_count = 0
    for cf in custom_fields:
        if cf.get("enabled", True):
            field_bytes += cf.get("bytes_estimate", 0)
            custom_count += 1
    structural += custom_count * 5

    return structural + field_bytes


def check_log_line_budget(log_fields_config: dict) -> dict | None:
    """Return a warning dict when the estimated emitted log line approaches the
    Fastly Deliver 16 KiB cap, or None when the config is comfortably under.

    The returned shape matches the ``waf_warning`` envelope used elsewhere in
    the log-fields response so the frontend can render it without a new
    component. ``severity`` is "error" past the hard cap and "warn" past the
    safe-max headroom threshold.

    Why a soft threshold: ``estimate_log_line_bytes`` returns the *average*
    line size based on typical_bytes; real-request URLs and UAs routinely run
    2-3x that. A config whose average is already 12 KB will see truncation
    on long-URL traffic without any config-time signal. The safe-max sits at
    ~60% of the cap so the warning fires before production starts losing
    bytes.
    """
    estimate = estimate_log_line_bytes(log_fields_config)
    if estimate >= FASTLY_LOG_LINE_DELIVER_MAX:
        return {
            "code": "LOG_LINE_TOO_LARGE",
            "severity": "error",
            "estimate_bytes": estimate,
            "deliver_max_bytes": FASTLY_LOG_LINE_DELIVER_MAX,
            "safe_max_bytes": FASTLY_LOG_LINE_SAFE_MAX,
            "message": (
                f"Estimated log line is {estimate} bytes; Fastly Deliver services "
                f"silently truncate at {FASTLY_LOG_LINE_DELIVER_MAX} bytes (16 KiB). "
                "Disable some fields to avoid corrupt JSON in ingested logs."
            ),
        }
    if estimate >= FASTLY_LOG_LINE_SAFE_MAX:
        return {
            "code": "LOG_LINE_APPROACHING_LIMIT",
            "severity": "warn",
            "estimate_bytes": estimate,
            "deliver_max_bytes": FASTLY_LOG_LINE_DELIVER_MAX,
            "safe_max_bytes": FASTLY_LOG_LINE_SAFE_MAX,
            "message": (
                f"Estimated log line is {estimate} bytes; Fastly silently truncates "
                f"at {FASTLY_LOG_LINE_DELIVER_MAX} bytes (16 KiB). Per-request "
                "values (long URLs, fat headers) can push real lines past the cap. "
                "Consider trimming optional fields."
            ),
        }
    return None


def estimate_daily_bytes(log_fields_config: dict, req_per_day: int = 1_000_000) -> dict:
    """Return storage estimates for the given config and daily request volume."""
    line_bytes = estimate_log_line_bytes(log_fields_config)
    raw_bytes_day = line_bytes * req_per_day
    # Parquet compressed is roughly 10% of raw JSON (DuckDB ZSTD + dictionary encoding)
    parquet_bytes_day = int(raw_bytes_day * 0.10)
    parquet_bytes_30d = parquet_bytes_day * 30
    return {
        "line_bytes": line_bytes,
        "raw_mb_day": round(raw_bytes_day / (1024 * 1024), 1),
        "parquet_mb_day": round(parquet_bytes_day / (1024 * 1024), 1),
        "parquet_gb_30d": round(parquet_bytes_30d / (1024**3), 2),
    }


def format_hash(log_fields_config: dict) -> str:
    """Return a SHA256 fingerprint of the generated log format for drift detection."""
    fmt = generate_log_format(log_fields_config)
    return "sha256:" + hashlib.sha256(fmt.encode()).hexdigest()


def get_catalog_for_api(field_limits: dict[str, int] | None = None) -> list:
    """Return a simplified catalog suitable for the /api/log-fields/catalog endpoint."""
    result = []
    limits = field_limits or {}
    for f in LOG_FIELD_CATALOG:
        entry = {
            "id": f["id"],
            "group": f["group"],
            "label": f["label"],
            "description": f["description"],
            "duckdb_type": f["duckdb_type"],
            "typical_bytes": f["typical_bytes"],
            "required_by": f.get("required_by", []),
            "formatter": f.get("formatter"),
            "unit": f.get("unit"),
            "precision": f.get("precision"),
        }
        if f.get("individually_toggleable"):
            entry["individually_toggleable"] = True
        if f.get("note"):
            entry["note"] = f["note"]

        if f["id"] == "url":
            entry["limit"] = limits.get("url", 2000)
            entry["has_limit"] = True
        elif f["id"] == "ua":
            entry["limit"] = limits.get("ua", 1000)
            entry["has_limit"] = True
        elif f["id"] == "referer":
            entry["limit"] = limits.get("referer", 1000)
            entry["has_limit"] = True

        result.append(entry)
    return result


def get_groups_for_api() -> list:
    """Return group metadata suitable for the /api/log-fields/catalog endpoint."""
    result = []
    ordered_groups = [None, "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "METRICS"]
    for gid in ordered_groups:
        info = GROUP_INFO[gid]
        fields = [f for f in LOG_FIELD_CATALOG if f["group"] == gid]
        total_bytes = sum(f["typical_bytes"] for f in fields)
        result.append(
            {
                "id": gid,
                "label": info["label"],
                "description": info["description"],
                "locked": info.get("locked", False),
                "requires": info.get("requires"),
                "note": info.get("note"),
                "total_bytes": total_bytes,
                "fields": [f["id"] for f in fields],
            }
        )
    return result


def validate_group_deps(groups: list) -> list:
    """Return a list of error strings for any unsatisfied group dependencies."""
    errors = []
    for grp in groups:
        required = GROUP_DEPENDENCIES.get(grp)
        if required and required not in groups:
            errors.append(
                f"Group {grp} ({GROUP_INFO[grp]['label']}) requires Group {required} "
                f"({GROUP_INFO[required]['label']}) to also be enabled."
            )
    return errors


# ---------------------------------------------------------------------------
# Custom field support
# ---------------------------------------------------------------------------

_BUILTIN_FIELD_NAMES = frozenset(f["id"] for f in LOG_FIELD_CATALOG)
# Iceberg partition columns that must not be used as field names
_RESERVED_PARTITION_COLS = frozenset({"dt", "timestamp_hour"})


def validate_custom_field(field: dict, existing_names: list[str]) -> list[str]:
    """Validate a custom field definition dict.

    Parameters
    ----------
    field : dict
        The candidate custom field (user-supplied, not yet saved).
    existing_names : list[str]
        Names of all currently saved custom fields for this service
        (excluding the field being validated, for update operations).

    Returns
    -------
    list[str]
        List of human-readable error strings. Empty means valid.
        Warnings are prefixed with "WARN: ".
    """
    errors: list[str] = []
    name = field.get("name", "")

    # 1. Required keys
    for key in ("name", "label", "vcl_log_expression", "duckdb_type", "value_type", "bytes_estimate"):
        if key not in field or field[key] is None:
            errors.append(f"Missing required field: '{key}'")
    if errors:
        return errors

    # 2. Name regex
    if not VALID_NAME_RE.match(name):
        errors.append("Field name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars")

    # 3. DuckDB/SQL reserved word
    if name in _DUCKDB_RESERVED:
        errors.append(f"'{name}' is a DuckDB/SQL reserved word and cannot be used as a field name")

    # 4. Built-in field collision
    if name in _BUILTIN_FIELD_NAMES:
        errors.append(f"'{name}' is already a built-in field name")

    # 5. Duplicate custom field
    if name in existing_names:
        errors.append(f"A custom field named '{name}' already exists on this service")

    # 6. Label length
    label = field.get("label", "")
    if not (1 <= len(label) <= 80):
        errors.append("'label' must be 1–80 characters")

    # 7. Description length
    desc = field.get("description", "")
    if len(desc) > 500:
        errors.append("'description' must not exceed 500 characters")

    # 8. VCL expression
    expr = field.get("vcl_log_expression", "")
    if not expr.strip():
        errors.append("'vcl_log_expression' must not be empty")
    elif len(expr) > 512:
        errors.append(f"'vcl_log_expression' must be ≤ 512 characters (got {len(expr)})")
    elif "\n" in expr:
        errors.append("'vcl_log_expression' must not contain raw newlines")
    else:
        # VCL injection protection — semicolons end statements; comments hide injected code.
        # Curly braces are intentionally allowed: %{variable}V is standard VCL interpolation.
        if ";" in expr:
            errors.append("'vcl_log_expression' must not contain semicolons (;)")
        if "//" in expr or "/*" in expr or "#" in expr:
            errors.append("'vcl_log_expression' must not contain VCL comments (//, /*, or #)")

    # 9. duckdb_type enum
    valid_types = set(_DUCKDB_TYPE_VALUE_TYPE_COMPAT)
    duckdb_type = field.get("duckdb_type", "")
    if duckdb_type not in valid_types:
        errors.append(f"'duckdb_type' must be one of: {', '.join(sorted(valid_types))}")

    # 10. value_type enum
    all_value_types = {"string", "numeric", "boolean", "ip", "url"}
    value_type = field.get("value_type", "")
    if value_type not in all_value_types:
        errors.append(f"'value_type' must be one of: {', '.join(sorted(all_value_types))}")

    # 11. duckdb_type / value_type compatibility
    if duckdb_type in _DUCKDB_TYPE_VALUE_TYPE_COMPAT and value_type in all_value_types:
        if value_type not in _DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]:
            compat = ", ".join(sorted(_DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]))
            errors.append(
                f"'value_type' '{value_type}' is not compatible with 'duckdb_type' '{duckdb_type}'. "
                f"Compatible value_types: {compat}"
            )

    # 12. bytes_estimate range
    bytes_est = field.get("bytes_estimate", 0)
    try:
        bytes_est = int(bytes_est)
        if not (1 <= bytes_est <= 1024):
            errors.append("'bytes_estimate' must be between 1 and 1024")
    except (TypeError, ValueError):
        errors.append("'bytes_estimate' must be an integer")

    # 13. Validate collection_stage
    stage = field.get("collection_stage", "edge")
    if stage not in ("edge", "origin"):
        errors.append(f"'collection_stage' must be 'edge' or 'origin' (got '{stage}')")

    # 14. Warn on suspiciously low bytes_estimate
    if isinstance(bytes_est, int) and isinstance(name, str):
        min_bytes = len(name) + 5  # key + quotes + colon + value
        if 1 <= bytes_est < min_bytes:
            errors.append(
                f"WARN: 'bytes_estimate' ({bytes_est}) is less than the field name overhead "
                f'({min_bytes} bytes for "{name}":). The estimate is likely too low.'
            )

    return errors


def get_custom_fields_catalog_entries(log_fields_config: dict) -> list[dict]:
    """Return custom fields in the same shape as built-in catalog entries."""
    return [
        {
            "id": cf["name"],
            "label": cf["label"],
            "group": "custom",
            "duckdb_type": cf.get("duckdb_type", "VARCHAR"),
            "description": cf.get("description", ""),
            "show_in_dashboard": cf.get("show_in_dashboard", False),
            "show_in_logs": cf.get("show_in_logs", True),
            "filterable": cf.get("filterable", True),
            "value_type": cf.get("value_type", "string"),
            "is_custom": True,
        }
        for cf in log_fields_config.get("custom_fields", [])
        if cf.get("enabled", True)
    ]


_DEFAULT_LF_CONFIG: dict = {"schema_version": 2, "custom_fields": []}


def get_lf_config(cfg: dict) -> dict:
    """Return the log_fields config from a service config dict, with a safe default.

    Centralises the ``cfg.get("log_fields") or {...}`` pattern used across routers.
    """
    return cfg.get("log_fields") or _DEFAULT_LF_CONFIG.copy()
