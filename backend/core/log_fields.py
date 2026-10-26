"""Log field catalog and format generator for field-aware logging configuration.

Every loggable field is defined here: its VCL expression, DuckDB type, typical
byte cost, and which insights require it.  Nothing else in the codebase should
hard-code VCL log format strings.

Relationship to ``backend.core.field_registry``
-----------------------------------------------
A frozen-dataclass view of this catalog lives at
``backend/core/field_registry.py`` (`REGISTRY`, `BY_CODE`, `BY_GROUP`,
`WIRE_ORDER`); the registry is derived from `LOG_FIELD_CATALOG` below at
import time and stays byte-for-byte equivalent — a parity test in
``tests/core/test_field_registry.py`` guards both views. The dual surface
is intentional, not a transitional state: this module is the canonical
data source, and the registry exposes the same data in a typed shape for
callers that prefer that ergonomics. New fields must be added HERE (as a
dict entry); the registry picks them up automatically.

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

Opt-in (``default_off``) fields
-------------------------------
A field may carry ``"default_off": True`` in its catalog entry. Such a field
is EXCLUDED from ``resolve_enabled_fields`` even when its group is enabled;
it is emitted ONLY when explicitly turned on through the per-field override
mechanism (``field_overrides[id] = True`` — the admin log-fields toggle or
the provisioning ``--enable-field``). This is the deliberate-opt-in gate for
sensitive captures such as ``cookie_session`` (a stable per-session client
identifier): selecting the security/full preset must NOT silently start
hashing+storing session cookies. The flag is general — add it to any future
field that should require an explicit operator choice.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import sys as _sys
import tempfile
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

import structlog
from fastapi import HTTPException

from backend.core.fastly.client import fastly

logger = structlog.get_logger(__name__)

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
# Numeric/boolean custom fields can't be long (a u64 is ≤20 chars), so their
# substr() reservation is clamped to this instead of the 2000-char string
# default — otherwise they starve the line budget for later fields.
_NUMERIC_FIELD_MAX_LEN = 20

# ---------------------------------------------------------------------------
# Custom field validation constants

VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
for _early_mod in (
    "backend.core._log_fields_data",
    "backend.core.field_registry",
    "backend.core.metric_expressions",
    "backend.utils.vcl_utils",
    "backend.utils.vcl_validator",
    "backend.utils.fastly_auth",
    "backend.utils.vcl_safety",
):
    _sys.modules[_early_mod] = _sys.modules[__name__]

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


# ──────────────────────────────────────────────────────────────────────────
# Field catalog, group metadata, presets, and insight definitions
# ──────────────────────────────────────────────────────────────────────────
# fmt: off


def _f(
    id: str, group: str | None, label: str, description: str, vcl: str | None,
    duckdb_type: str = "VARCHAR", typical_bytes: int = 0, required_by: list[str] | None = None, *,
    formatter: str | None = None, precision: int | None = None, unit: str | None = None,
    note: str | None = None, individually_toggleable: bool | None = None,
    pii: bool | None = None, default_off: bool | None = None,
) -> dict[str, Any]:
    d: dict[str, Any] = {"id": id, "group": group, "label": label, "description": description}
    for k, v in (("formatter", formatter), ("precision", precision), ("unit", unit), ("note", note)):
        if v is not None:
            d[k] = v
    d.update({"vcl": vcl, "duckdb_type": duckdb_type, "typical_bytes": typical_bytes})
    for k, v in (("individually_toggleable", individually_toggleable), ("pii", pii), ("default_off", default_off)):
        if v is not None:
            d[k] = v
    d["required_by"] = list(required_by) if required_by is not None else []
    return d


def _m(id: str, label: str, description: str, formatter: str, duckdb_type: str = "DOUBLE", unit: str | None = None) -> dict[str, Any]:
    return _f(id, "METRICS", label, description, None, duckdb_type, 0, formatter=formatter, unit=unit)


def _v(id: str, label: str, description: str) -> dict[str, Any]:
    return _f(id, "VIRTUAL", label, description, None, "VARCHAR", 0)


def _g(label: str, description: str, *, locked: bool = False, requires: str | None = None, note: str | None = None, recommended_with: list[str] | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {"label": label, "description": description, "locked": locked, "requires": requires}
    if note is not None:
        d["note"] = note
    if recommended_with is not None:
        d["recommended_with"] = recommended_with
    return d


def _p(label: str, description: str, groups: list[str], field_overrides: dict[str, bool] | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {"label": label, "description": description, "groups": groups}
    if field_overrides is not None:
        d["field_overrides"] = field_overrides
    return d


def _i(id: str, category: str, title: str, description: str, required_fields: list[str] | None = None, required_groups: list[str] | None = None) -> dict[str, Any]:
    return {"id": id, "category": category, "title": title, "description": description, "required_fields": list(required_fields) if required_fields is not None else [], "required_groups": list(required_groups) if required_groups is not None else []}


LOG_FIELD_CATALOG: list[dict[str, Any]] = [
    # ── Always-on ─────────────────────────────────────────────────────────
    _f('timestamp', None, 'Timestamp', 'UTC timestamp of the request start time (ISO 8601 with timezone).', '"timestamp":"%{strftime(\\{"%Y-%m-%dT%H:%M:%S%z"\\},time.start)}V"', 'TIMESTAMP', 40),
    _f('ip', None, 'Client IP', 'Client IP address. Captured at the real edge via x-fos-edge-data header; falls back to client.ip for requests answered before capture ran (e.g. redirects).', '"ip":"%{json.escape(if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, if(req.http.Fastly-Client-IP != "", req.http.Fastly-Client-IP, client.ip)))}V"', 'VARCHAR', 22, ['low_and_slow', 'credential_enumeration', 'content_discovery', 'botnet_grouping']),
    _f('status', None, 'Response Status', 'HTTP response status code (e.g. 200, 404, 503).', '"status":%{if(resp.status > 0, "" + resp.status, "null")}V', 'USMALLINT', 17, ['error_spikes', 'city_error_spikes', 'credential_enumeration', 'content_discovery', 'waf_signal_spikes', 'image_optimization_opportunities'], formatter='status'),
    _f('elapsed', None, 'Elapsed Time (µs)', 'Total request processing time in microseconds.', '"elapsed":%{if(time.elapsed.usec != "", time.elapsed.usec, "null")}V', 'UBIGINT', 18, ['latency_regression', 'city_latency_regressions', 'network_asn_health', 'tail_latency', 'region_latency'], formatter='number', unit='µs'),
    _f('cache', None, 'Cache State', 'Fastly cache state: HIT, MISS, PASS, SYNTH, etc.', '"cache":"%{json.escape(fastly_info.state)}V"', 'VARCHAR', 18, ['cache_collapse', 'cache_hit_cliff', 'cache_pressure']),
    _f('resp_bytes', None, 'Response Bytes', 'Bytes delivered to the client in the response body.', '"resp_bytes":%{if(resp.bytes_written > 0, "" + resp.bytes_written, "0")}V', 'UBIGINT', 18, ['cache_pressure', 'network_asn_health', 'image_optimization_opportunities'], formatter='bytes'),
    # ── Group A — Request Identity ─────────────────────────────────────────
    _f('host', 'A', 'Host', 'HTTP Host header (domain name) captured at the true client edge before any rewrites.', '"host":"%{json.escape(substr(if(req.http.x-fos-edge-data:host != "", req.http.x-fos-edge-data:host, req.http.Host), 0, 512))}V"', 'VARCHAR', 22, ['new_probe_urls']),
    _f('url', 'A', 'URL', 'Request URL path and query string. Average ~30 bytes; varies widely.', '"url":"%{json.escape(substr(req.url, 0, 2000))}V"', 'VARCHAR', 37, ['error_spikes', 'latency_regression', 'new_probe_urls', 'low_and_slow', 'credential_enumeration', 'content_discovery', 'tail_latency', 'image_optimization_opportunities']),
    _f('method', 'A', 'HTTP Method', 'Request method: GET, POST, HEAD, PUT, DELETE, etc.', '"method":"%{json.escape(substr(req.method, 0, 128))}V"', 'VARCHAR', 19, ['method_drift']),
    _f('proto', 'A', 'HTTP Version', 'HTTP protocol version: 1.0, 1.1, 2.0, or 3.0.', '"proto":"%{if(req.proto != "", regsub(req.proto, "^HTTP/", ""), "")}V"', 'VARCHAR', 15, formatter='number', precision=1),
    _f('ua', 'A', 'User-Agent', 'Client browser or bot identifier. Largest single field — bots inflate this significantly.', '"ua":"%{json.escape(substr(if(req.http.x-fos-edge-data:ua != "", req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, 1000))}V"', 'VARCHAR', 90, ['ua_monoculture', 'botnet_grouping', 'image_optimization_opportunities'], note='Largest single field — bots tend to have verbose user-agents.', individually_toggleable=True),
    _f('referer', 'A', 'Referer', 'Referring URL. Often empty; useful for traffic source analysis.', '"referer":"%{json.escape(substr(if(req.http.x-fos-edge-data:referer != "", req.http.x-fos-edge-data:referer, req.http.Referer), 0, 1000))}V"', 'VARCHAR', 44, ['referer_monoculture'], individually_toggleable=True),
    # Use only req.http.Content-Length (always defined at log time) and
    # regex-validate digits, so any synth/error path that never set the
    # header still renders "0" instead of empty (which would yield
    # invalid JSON like `"req_bytes":,`). We previously fell back to
    # bereq.body_bytes_written for chunked uploads, but bereq is
    # undefined on synth/restart paths and any access error there
    # collapses the entire %{...}V to "" and produces malformed lines.
    _f('req_bytes', 'A', 'Request Body Size', 'Request body size in bytes from Content-Length header. Zero for GET/HEAD or any request without Content-Length.', '"req_bytes":%{if(req.http.Content-Length ~ "^[0-9]+$", req.http.Content-Length, "0")}V', 'UBIGINT', 13, ['request_size_anomaly'], formatter='bytes'),
    _f('req_header_bytes', 'A', 'Request Header Size', 'Total bytes in the request headers. Large values are an injection or WAF bypass signal.', '"req_header_bytes":%{if(req.header_bytes_read > 0, "" + req.header_bytes_read, "0")}V', 'UINTEGER', 20, ['request_size_anomaly'], formatter='bytes'),
    # Response header, read at log time (like status / cache). json.escape
    # so a weird / multi-value Content-Encoding cannot break out of the JSON
    # string; an absent header renders as an empty string.
    _f('resp_header_content_encoding', 'A', 'Response Content-Encoding', 'Content-Encoding of the response served to the client (gzip / br / zstd, or empty when the response was sent uncompressed). Low-cardinality. Read directly from the final response at log time — no edge capture needed.', '"resp_header_content_encoding":"%{json.escape(resp.http.Content-Encoding)}V"', 'VARCHAR', 38, ['payload_compression_regression'], individually_toggleable=True),
    # ── Group B — Cache Deep-Dive ──────────────────────────────────────────
    # Strip the trailing "s" *and* the fractional part: Fastly's obj.ttl is
    # serialized as e.g. "3600.027s" with several µs of internal jitter, so
    # the prior `regsub(..., "s$", "")` left float keys that split Top-N
    # GROUP BY into many near-duplicate buckets. TTLs are integer seconds
    # in the underlying Cache-Control headers anyway.
    _f('ttl', 'B', 'Object TTL', 'Time-to-live assigned by origin headers. Null when object is not cacheable.', '"ttl":%{if(obj.ttl > 0s, regsub("" + obj.ttl, "(\\.[0-9]+)?s$", ""), "null")}V', 'FLOAT', 18, ['cache_pressure', 'cache_ttl_mismatch'], formatter='number', precision=0, unit='s'),
    # Same fractional-strip as ttl: obj.age comes through as "12.0s" or
    # "12.000001s" depending on the moon phase — both round to integer
    # seconds for display purposes.
    _f('age', 'B', 'Object Age', 'How long the object has been in the Fastly cache (seconds).', '"age":%{if(obj.age > 0s, regsub("" + obj.age, "(\\.[0-9]+)?s$", ""), "null")}V', 'FLOAT', 17, ['cache_pressure', 'cache_ttl_mismatch'], formatter='number', precision=0, unit='s'),
    _f('hits', 'B', 'Object Hit Count', 'Number of times this cached object has been served.', '"hits":%{if(obj.hits > 0, "" + obj.hits, "null")}V', 'UINTEGER', 14, ['cache_ttl_mismatch']),
    _f('digest', 'B', 'Content Digest', 'Content hash for exact object identity. Required for Cache Pressure Analysis.', '"digest":"%{req.digest}V"', 'VARCHAR', 47, ['cache_pressure', 'pop_latency_regression'], note='Required for Cache Pressure Analysis (eviction detection).'),
    # ── Group C — Infrastructure ───────────────────────────────────────────
    _f('pop', 'C', 'Edge PoP', 'Fastly Point of Presence code (e.g. JFK, LHR, SYD).', '"pop":"%{server.datacenter}V"', 'VARCHAR', 18, ['cache_pressure'], formatter='pop'),
    _f('backend', 'C', 'Backend', 'Origin backend name as configured in Fastly.', '"backend":"%{json.escape(req.backend)}V"', 'VARCHAR', 21),
    _f('edge', 'C', 'Edge Hit', 'True when the request hit the real edge (not a shield or restart).', '"edge":%{if(fastly.ff.visits_this_service == 0, "1", "0")}V', 'BOOLEAN', 9),
    _f('ttfb', 'C', 'Time to First Byte (s)', 'Seconds from request receipt to first byte of response from origin. Subtract from elapsed to isolate Fastly processing time.', '"ttfb":%{if(time.to_first_byte > 0s, regsub("" + time.to_first_byte, "s$", ""), "null")}V', 'FLOAT', 14, ['region_latency'], formatter='number', precision=3, unit='s'),
    _f('server_region', 'C', 'Server Region', 'Fastly billing region of the serving PoP (e.g. NA, EU, APAC). Captured at edge for accurate attribution through shields.', '"server_region":"%{json.escape(if(req.http.x-fos-edge-data:srv_region != "", req.http.x-fos-edge-data:srv_region, server.region))}V"', 'VARCHAR', 20, ['region_latency']),
    _f('is_ipv6', 'C', 'IPv6', 'True when the client connected over IPv6. IPv6 clients can have different routing and latency profiles.', '"is_ipv6":%{if(req.http.x-fos-edge-data:is_ipv6 ~ "^[0-9]+$", req.http.x-fos-edge-data:is_ipv6, if(req.is_ipv6, "1", "0"))}V', 'BOOLEAN', 12),
    _f('conn_requests', 'C', 'Conn. Request Count', 'Number of requests made on this TCP/QUIC connection. High values indicate HTTP/2 keep-alive multiplexing.', '"conn_requests":%{if(req.http.x-fos-edge-data:conn_reqs ~ "^[0-9]+$", req.http.x-fos-edge-data:conn_reqs, if(client.requests > 0, "" + client.requests, "null"))}V', 'USMALLINT', 20, ['connection_abuse']),
    _f('tls', 'C', 'TLS Version', 'TLS protocol version as a float: 1.2 or 1.3.', '"tls":"%{json.escape(if(req.http.x-fos-edge-data:tls != "", req.http.x-fos-edge-data:tls, if(tls.client.protocol != "", regsub(tls.client.protocol, "^TLSv", ""), "")))}V"', 'VARCHAR', 10, formatter='number', precision=1),
    # ── Group D — Geolocation Basic ────────────────────────────────────────
    _f('country', 'D', 'Country', 'ISO 3166-1 alpha-2 country code (e.g. US, DE, JP). Enables world map.', '"country":"%{json.escape(if(req.http.x-fos-edge-data:country != "", req.http.x-fos-edge-data:country, client.geo.country_code))}V"', 'VARCHAR', 15, ['new_country_traffic', 'city_surges', 'city_error_spikes', 'city_latency_regressions', 'new_city_traffic'], formatter='country', individually_toggleable=True),
    _f('city', 'D', 'City', 'City name from Fastly geo-IP. Variable length.', '"city":"%{json.escape(if(req.http.x-fos-edge-data:city != "", req.http.x-fos-edge-data:city, client.geo.city))}V"', 'VARCHAR', 18, ['city_surges', 'city_error_spikes', 'city_latency_regressions', 'new_city_traffic'], formatter='city', individually_toggleable=True),
    _f('region', 'D', 'Region', 'ISO 3166-2 region/state/province code.', '"region":"%{json.escape(if(req.http.x-fos-edge-data:region != "", req.http.x-fos-edge-data:region, if(client.geo.region == "?", "", client.geo.region)))}V"', 'VARCHAR', 14, formatter='region', individually_toggleable=True),
    # ── Group E — Geolocation Precision (requires D) ───────────────────────
    _f('lat', 'E', 'Latitude', 'Client latitude (-90 to 90). Null for unresolvable IPs.', '"lat":%{if(req.http.x-fos-edge-data:lat ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lat, if(client.geo.country_code != "?", "" + client.geo.latitude, "null"))}V', 'FLOAT', 12, ['network_asn_health'], formatter='number', precision=4),
    _f('lon', 'E', 'Longitude', 'Client longitude (-180 to 180). Null for unresolvable IPs.', '"lon":%{if(req.http.x-fos-edge-data:lon ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lon, if(client.geo.country_code != "?", "" + client.geo.longitude, "null"))}V', 'FLOAT', 13, ['network_asn_health'], formatter='number', precision=4),
    _f('metro', 'E', 'Metro Code', 'US DMA metro area code (e.g. 501 = New York City). Empty for non-US.', '"metro":%{if(req.http.x-fos-edge-data:metro ~ "^[0-9]+$", req.http.x-fos-edge-data:metro, if(client.geo.metro_code > 0, "" + client.geo.metro_code, "null"))}V', 'USMALLINT', 14, ['metro_delivery_degradation']),
    # ── Group F — Network Quality Core ────────────────────────────────────
    _f('asn', 'F', 'ASN', 'Client Autonomous System Number (ISP identity). Enables ASN-level analysis.', '"asn":%{if(req.http.x-fos-edge-data:asn ~ "^[0-9]+$", req.http.x-fos-edge-data:asn, if(client.as.number > 0, "" + client.as.number, "null"))}V', 'UINTEGER', 11, ['asn_concentration', 'asn_hosting_shift', 'new_asn_traffic', 'network_asn_health', 'region_latency']),
    _f('tcp_rtt', 'F', 'TCP RTT (µs)', 'TCP round-trip time in microseconds at the Fastly edge.', '"tcp_rtt":%{if(req.http.x-fos-edge-data:rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt, if(client.socket.tcpi_rtt > 0, "" + client.socket.tcpi_rtt, "null"))}V', 'UINTEGER', 19, ['network_asn_health'], formatter='number', unit='µs'),
    _f('transport', 'F', 'Transport Protocol', "Transport protocol: 'tcp' or 'quic'. Low-cardinality; essentially free in Parquet.", '"transport":"%{json.escape(if(req.http.x-fos-edge-data:transport != "", req.http.x-fos-edge-data:transport, transport.type))}V"', 'VARCHAR', 18, ['network_asn_health', 'http3_fallback']),
    # ── Group G — Network Quality Deep (requires F) ────────────────────────
    _f('ploss', 'G', 'Packet Loss', 'Packet loss fraction (0.0–1.0). Direct indicator of network congestion.', '"ploss":%{if(req.http.x-fos-edge-data:ploss ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:ploss, if(client.socket.ploss > 0, "" + client.socket.ploss, "null"))}V', 'FLOAT', 18, ['network_asn_health'], formatter='percent', precision=4),
    _f('rtt_min', 'G', 'Minimum RTT (µs)', 'Minimum RTT seen on this TCP connection (geography baseline). Delta from tcp_rtt isolates congestion.', '"rtt_min":%{if(req.http.x-fos-edge-data:rtt_min ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_min, if(client.socket.tcpi_min_rtt > 0, "" + client.socket.tcpi_min_rtt, "null"))}V', 'UINTEGER', 19, ['network_asn_health'], formatter='number', unit='µs'),
    _f('rtt_var', 'G', 'RTT Variance / Jitter (µs)', 'RTT variance in microseconds. Jitter causes streaming buffer stalls more than raw latency.', '"rtt_var":%{if(req.http.x-fos-edge-data:rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_var, if(client.socket.tcpi_rttvar > 0, "" + client.socket.tcpi_rttvar, "null"))}V', 'UINTEGER', 18, ['network_asn_health'], formatter='number', unit='µs'),
    _f('retrans', 'G', 'TCP Retransmissions', 'TCP retransmission delta since previous sample. Direct congestion signal.', '"retrans":%{if(req.http.x-fos-edge-data:retrans ~ "^[0-9]+$", req.http.x-fos-edge-data:retrans, if(client.socket.tcpi_delta_retrans > 0, "" + client.socket.tcpi_delta_retrans, "null"))}V', 'UTINYINT', 15, ['network_asn_health'], formatter='number'),
    _f('bw', 'K', 'Bandwidth Estimate', "Fastly's estimated bandwidth for this connection (bytes/sec or bits/sec — see note). Only applicable for QUIC; TCP connections should use delivery_rate instead.", '"bw":%{if(req.http.x-fos-edge-data:bw ~ "^[0-9]+$", req.http.x-fos-edge-data:bw, if(transport.bw_estimate > 0, "" + transport.bw_estimate, "null"))}V', 'UBIGINT', 17, formatter='bytes'),
    _f('c_speed', 'G', 'Connection Speed Class', 'Geo-IP speed classification: broadband, cable, dsl, mobile, satellite, dialup. Low-cardinality.', '"c_speed":"%{json.escape(if(req.http.x-fos-edge-data:c_speed != "", req.http.x-fos-edge-data:c_speed, if(client.geo.conn_speed == "?", "", client.geo.conn_speed)))}V"', 'VARCHAR', 14, ['network_asn_health', 'connection_type_mix']),
    _f('c_type', 'G', 'Connection Type', 'Geo-IP connection type: residential, commercial, cellular, corporate. Low-cardinality.', '"c_type":"%{json.escape(if(req.http.x-fos-edge-data:c_type != "", req.http.x-fos-edge-data:c_type, if(client.geo.conn_type == "?", "", client.geo.conn_type)))}V"', 'VARCHAR', 27, ['network_asn_health', 'connection_type_mix']),
    _f('delivery_rate', 'G', 'TCP Delivery Rate', 'Actual TCP delivery rate in bytes/sec measured by the kernel. More reliable than bandwidth estimate for TCP connections.', '"delivery_rate":%{if(req.http.x-fos-edge-data:del_rate ~ "^[0-9]+$", req.http.x-fos-edge-data:del_rate, if(client.socket.tcpi_delivery_rate > 0, "" + client.socket.tcpi_delivery_rate, "null"))}V', 'UBIGINT', 22, ['network_asn_health', 'metro_delivery_degradation'], formatter='bytes'),
    _f('data_segs_out', 'G', 'TCP Data Segments Out', 'Total TCP data segments sent on this connection. Enables retransmit ratio: retrans / data_segs_out.', '"data_segs_out":%{if(req.http.x-fos-edge-data:data_segs ~ "^[0-9]+$", req.http.x-fos-edge-data:data_segs, if(client.socket.tcpi_data_segs_out > 0, "" + client.socket.tcpi_data_segs_out, "null"))}V', 'UINTEGER', 21, ['network_asn_health'], formatter='number'),
    # ── Group H — Security: TLS Fingerprinting ────────────────────────────
    _f('ja3', 'H', 'JA3 Fingerprint', 'MD5 TLS client fingerprint. Older standard; widely supported. 41 bytes avg.', '"ja3":"%{json.escape(if(req.http.x-fos-edge-data:ja3 != "", req.http.x-fos-edge-data:ja3, tls.client.ja3_md5))}V"', 'VARCHAR', 41, ['botnet_grouping'], individually_toggleable=True),
    _f('ja4', 'H', 'JA4 Fingerprint', 'Newer, richer TLS fingerprint standard. 43 bytes avg.', '"ja4":"%{json.escape(if(req.http.x-fos-edge-data:ja4 != "", req.http.x-fos-edge-data:ja4, tls.client.ja4))}V"', 'VARCHAR', 43, ['botnet_grouping'], individually_toggleable=True),
    _f('tls_ciphers_sha', 'H', 'TLS Cipher Suite SHA', "SHA fingerprint of the client's offered cipher suite list. Evasion-resistant complement to JA3/JA4 for bot farm detection.", '"tls_ciphers_sha":"%{json.escape(if(req.http.x-fos-edge-data:tls_csha != "", req.http.x-fos-edge-data:tls_csha, tls.client.ciphers_list_sha))}V"', 'VARCHAR', 48, ['cipher_spread'], individually_toggleable=True),
    _f('cookie_session', 'H', 'Session Cookie (hashed)', "SHA-256 hash of the client's session cookie — the FIRST present among a common framework allowlist (sessionid, PHPSESSID, JSESSIONID, ASP.NET_SessionId, connect.sid, session, sid). Hashed AT THE EDGE via digest.hash_sha256, so the raw cookie value is never logged or forwarded past Fastly. Empty when no session cookie is present. PRIVACY: this is a stable per-session client identifier — it MUST be masked for analysts exactly like the client IP (see mask_ips policy).", '"cookie_session":"%{json.escape(req.http.x-fos-edge-data:cookie_session)}V"', 'VARCHAR', 40, ['session_harvesting'], individually_toggleable=True, pii=True, default_off=True),
    # ── Group I — Security: Proxy & Anonymization ─────────────────────────
    _f('p_type', 'I', 'Proxy Type', 'Anonymizing proxy type: VPN, Tor, DCH (data center), etc.', '"p_type":"%{json.escape(if(req.http.x-fos-edge-data:p_type != "", req.http.x-fos-edge-data:p_type, if(client.geo.proxy_type == "?", "", client.geo.proxy_type)))}V"', 'VARCHAR', 10, ['asn_hosting_shift', 'proxy_surge']),
    _f('p_desc', 'I', 'Proxy Description', 'Anonymizing proxy provider name.', '"p_desc":"%{json.escape(if(req.http.x-fos-edge-data:p_desc != "", req.http.x-fos-edge-data:p_desc, if(client.geo.proxy_description == "?", "", client.geo.proxy_description)))}V"', 'VARCHAR', 10, ['proxy_surge']),
    # ── Group J — WAF / NGWAF ─────────────────────────────────────────────
    _f('waf', 'J', 'WAF Executed', 'Whether NGWAF (Signal Sciences) processed this request.', '"waf":%{if(waf.executed, "1", "0")}V', 'BOOLEAN', 8, ['waf_signal_spikes']),
    _f('waf_resp', 'J', 'WAF Agent Response', 'NGWAF agent decision code (HTTP status equivalent).', '"waf_resp":%{if(waf.executed, if(req.http.x-sigsci-agentresponse ~ "^[0-9]+$", req.http.x-sigsci-agentresponse, "null"), "null")}V', 'USMALLINT', 16, ['waf_signal_spikes'], formatter='status'),
    _f('waf_ms', 'J', 'WAF Latency (ms)', 'Milliseconds the NGWAF inspection added to the request.', '"waf_ms":%{if(waf.executed, if(req.http.x-sigsci-decision-ms ~ "^[0-9]+$", req.http.x-sigsci-decision-ms, "null"), "null")}V', 'UINTEGER', 13, formatter='number', unit='ms'),
    _f('waf_sig', 'J', 'WAF Signal Tags', 'NGWAF signal tags (e.g. SQLI, XSS, CMDEXE).', '"waf_sig":"%{if(waf.executed, if(req.http.x-sigsci-tags != "", json.escape(req.http.x-sigsci-tags), ""), "")}V"', 'VARCHAR', 13, ['waf_signal_spikes']),
    _f('waf_req_id', 'J', 'WAF Request ID', 'NGWAF request correlation ID for cross-referencing with Signal Sciences.', '"waf_req_id":"%{if(waf.executed, if(req.http.x-fastly-ngwaf:requestid != "", json.escape(req.http.x-fastly-ngwaf:requestid), if(req.http.x-sigsci-requestid != "", json.escape(req.http.x-sigsci-requestid), "")), "")}V"', 'VARCHAR', 16),
    # ── Group K — QUIC / HTTP3 ────────────────────────────────────────────
    _f('q_rtt', 'K', 'QUIC Smoothed RTT (µs)', 'QUIC smoothed RTT in microseconds. Null for TCP connections.', '"q_rtt":%{if(req.http.x-fos-edge-data:q_rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt, if(transport.type == "quic", "" + quic.rtt.smoothed, "null"))}V', 'UINTEGER', 19, formatter='number', unit='µs'),
    _f('q_rtt_var', 'K', 'QUIC RTT Variance (µs)', 'QUIC RTT variance in microseconds. Null for TCP connections.', '"q_rtt_var":%{if(req.http.x-fos-edge-data:q_rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt_var, if(transport.type == "quic", "" + quic.rtt.variance, "null"))}V', 'UINTEGER', 19, formatter='number', unit='µs'),
    _f('q_lost', 'K', 'QUIC Packets Lost', 'QUIC packets lost counter. Null for TCP connections.', '"q_lost":%{if(req.http.x-fos-edge-data:q_lost ~ "^[0-9]+$", req.http.x-fos-edge-data:q_lost, if(transport.type == "quic", "" + quic.num_packets.lost, "null"))}V', 'UINTEGER', 17, formatter='number'),
    _f('q_cwnd', 'K', 'QUIC Congestion Window', 'QUIC congestion window size. Null for TCP connections.', '"q_cwnd":%{if(req.http.x-fos-edge-data:q_cwnd ~ "^[0-9]+$", req.http.x-fos-edge-data:q_cwnd, if(transport.type == "quic", "" + quic.cc.cwnd, "null"))}V', 'UINTEGER', 16, formatter='number'),
    # ── Group L — Origin Metrics ───────────────────────────────────────────
    _f('ottfb', 'L', 'Origin TTFB (µs)', 'µs from fetch start to first byte of origin/shield response headers. Null on HITs.', '"ottfb":%{if(req.http.x-of-ttfb ~ "^[0-9]+$", req.http.x-of-ttfb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike', 'region_latency'], formatter='number', unit='µs'),
    _f('ottlb', 'L', 'Origin TTLB (µs)', 'µs from fetch start to full response body received. Null on HITs.', '"ottlb":%{if(req.http.x-of-ttlb ~ "^[0-9]+$", req.http.x-of-ttlb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike'], formatter='number', unit='µs'),
    _f('oconnect_ms', 'L', 'Origin Connect (ms)', 'Milliseconds to establish the connection (TCP + TLS handshake) to origin/shield, from beresp.handshake_time_to_origin_ms. Distinct from ottfb (first byte). Null on HITs and on failed fetches that never connected — connection timeouts land in vcl_error, where beresp (and therefore the handshake timing) is unavailable.', '"oconnect_ms":%{if(req.http.x-of-connect ~ "^[0-9]+$", req.http.x-of-connect, "null")}V', 'UINTEGER', 16, ['timeout_split'], formatter='number', unit='ms'),
    _f('ost', 'L', 'Origin Status', 'HTTP status returned by origin or shield. Null on HITs.', '"ost":%{if(req.http.x-of-status ~ "^[0-9]+$", req.http.x-of-status, "null")}V', 'USMALLINT', 10, ['origin_error_rate', 'origin_ip_failure'], formatter='status'),
    _f('obytes', 'L', 'Origin Bytes', "Bytes written in the response (resp.bytes_written). Null on HITs. Same variable as resp_bytes but null-on-HIT makes it queryable as 'total bytes fetched from origin'.", '"obytes":%{if(req.http.x-of-start ~ "^[0-9]+$", "" + resp.bytes_written, "null")}V', 'UBIGINT', 15),
    _f('oip', 'L', 'Origin IP', 'IP address of the backend server that handled the fetch. Null on HITs.', '"oip":"%{json.escape(if(req.http.x-of-oip, req.http.x-of-oip, ""))}V"', 'VARCHAR', 15, ['origin_ip_failure']),
    _f('oretries', 'L', 'Origin Retries', 'Backend connection retry count before success or failure. Null on HITs.', '"oretries":%{if(req.http.x-of-oretries ~ "^[0-9]+$", req.http.x-of-oretries, "null")}V', 'UTINYINT', 13, ['origin_retries'], formatter='number'),
    _f('rid', 'L', 'Request ID', '8-char random ID generated at this POP. Always set. Use with prid to correlate edge + shield log lines.', '"rid":"%{req.http.x-req-id}V"', 'VARCHAR', 16),
    _f('prid', 'L', 'Parent Request ID', "Edge POP's rid forwarded to the shield. Non-null only on shield log lines (edge=0, cache=MISS).", '"prid":"%{json.escape(req.http.x-edge-req-id)}V"', 'VARCHAR', 16),
    # ── Group M: Image Optimizer Metrics (from Fastly-Io-Transform-Stats header) ─────
    _f('io_input_bytes', 'M', 'IO Input Bytes', 'Original image size in bytes before IO processing, from Fastly-Io-Transform-Stats ifsz field.', '"io_input_bytes":%{if(req.http.x-fos-io-ifsz ~ "^[0-9]+$", req.http.x-fos-io-ifsz, "null")}V', 'UBIGINT', 20, formatter='bytes'),
    _f('io_output_bytes', 'M', 'IO Output Bytes', 'Optimized image size in bytes after IO processing, from Fastly-Io-Transform-Stats ofsz field.', '"io_output_bytes":%{if(req.http.x-fos-io-ofsz ~ "^[0-9]+$", req.http.x-fos-io-ofsz, "null")}V', 'UBIGINT', 20, formatter='bytes'),
    _f('io_input_format', 'M', 'IO Input Format', 'Original image format before IO processing, from Fastly-Io-Transform-Stats ifmt field.', '"io_input_format":"%{json.escape(if(req.http.x-fos-io-ifmt != "", req.http.x-fos-io-ifmt, ""))}V"', 'VARCHAR', 20),
    _f('io_output_format', 'M', 'IO Output Format', 'Output image format after IO processing, from Fastly-Io-Transform-Stats ofmt field.', '"io_output_format":"%{json.escape(if(req.http.x-fos-io-ofmt != "", req.http.x-fos-io-ofmt, ""))}V"', 'VARCHAR', 20),
    # ── Metrics ───────────────────────────────────────────────────────────
    _m("requests", "Requests", "Total number of requests.", "number", "BIGINT"),
    _m('hit_rate', 'Cache Hit Rate', 'Percentage of requests served from cache (HIT or HIT-STALE).', 'percent', 'DOUBLE', unit='%'),
    _m("5xx", "5xx Errors", "Percentage of requests with 5xx status codes.", "percent", "DOUBLE", unit="%"),
    _m("4xx", "4xx Errors", "Percentage of requests with 4xx status codes.", "percent", "DOUBLE", unit="%"),
    _m("p50_latency", "P50 Latency", "Median request processing time (milliseconds).", "number", "DOUBLE", unit="ms"),
    _m('p95_latency', 'P95 Latency', '95th percentile request processing time (milliseconds).', 'number', 'DOUBLE', unit='ms'),
    _m('p99_latency', 'P99 Latency', '99th percentile request processing time (milliseconds).', 'number', 'DOUBLE', unit='ms'),
    _m('throughput', 'Throughput', 'Estimated bandwidth delivered for cache hits (bytes/second).', 'bytes', 'DOUBLE', unit='B/s'),
    _m("req_size", "Request Size", "Median total request size (headers + body).", "bytes", "DOUBLE", unit="B"),
    _m("ttfb_ms", "TTFB", "Median time to first byte (milliseconds).", "number", "DOUBLE", unit="ms"),
    # ── Virtual ───────────────────────────────────────────────────────────
    _v("_bot_name", "Fastly Bots", "Virtual field derived from User-Agent and IP to identify known bots."),
    _v("_ngwaf_bot_name", "NGWAF Verified Bots", "Virtual field enriched with NGWAF bot signal data."),
    _v("waf_sig_ind", "NGWAF Signals", "Individual NGWAF signals extracted from the waf_sig list."),
    _v('edge_score_reason_ind', 'Score Reasons', "Individual scoring reasons extracted from the comma-separated edge_score_reason field (e.g. 'cookie-missing', 'impossibly-fast', 'robotic-consistency', 'rare-transition'). Lets the dashboard show top-N reason breakdowns and filter by a single reason even when one request triggers multiple."),
    # ── Internal ──────────────────────────────────────────────────────────
    _f('_source_file', 'INTERNAL', 'Source File', 'Original raw log file in Fastly Object Storage.', None, 'VARCHAR', 60),
]

GROUP_INFO: dict[str | None, dict[str, Any]] = {
    None: _g('Core Delivery', 'Always-on fields required for basic metrics: error rates, latency, hit rates, throughput.', locked=True),
    "A": _g('Request Identity', 'Host, URL, HTTP method/version, User-Agent, Referer, request body size, and response Content-Encoding.'),
    "B": _g("Cache Deep-Dive", "TTL, age, hit count, and content digest. Enable for cache pressure analysis."),
    "C": _g('Infrastructure', 'Edge PoP, backend, edge/shield flag, TTFB, TLS version, billing region, IPv6 flag, and connection request count.'),
    "D": _g("Geolocation — Basic", "Country, city, and region. Country alone enables the world map."),
    "E": _g('Geolocation — Precision', 'Latitude, longitude, and US metro code. Requires Basic Geolocation.', requires='D'),
    "F": _g("Network Quality — Core", "ASN (ISP identity), TCP RTT, and transport protocol."),
    "G": _g('Network Quality — Deep', 'Packet loss, RTT variance/jitter, retransmissions, TCP delivery rate, data segments, and connection type. Requires Network Core.', requires='F'),
    "H": _g('Security: TLS Fingerprinting', 'JA3, JA4, cipher suite fingerprints, and a hashed session-cookie id for botnet grouping, scanner detection, and session-harvesting analysis.'),
    "I": _g("Security: Proxy Detection", "Anonymizing proxy type and provider name (VPN, Tor, DCH)."),
    "J": _g('WAF / NGWAF', 'Signal Sciences / NGWAF fields. All null if NGWAF is not deployed on this service.', note='All fields are null/empty if NGWAF is not deployed on this service.'),
    "K": _g('QUIC / HTTP3', 'QUIC-specific RTT, variance, packet loss, congestion window, and bandwidth estimate. All null for TCP connections.', note='All fields are null for TCP connections. Only useful if your service has meaningful HTTP/3 traffic.'),
    "L": _g('Origin Metrics', 'Origin/shield fetch timing, bytes, IP, and retries on cache misses and passes. VCL hooks applied automatically. ottfb/ottlb/oconnect_ms/ost/obytes/oip/oretries are null on HITs; rid is always set; prid set only on shield log lines.', note='Enabling this group deploys additional VCL timing snippets to your service automatically.', recommended_with=['C']),
    "M": _g('Image Optimizer Metrics', 'Per-request IO input/output sizes and formats from Fastly-Io-Transform-Stats. Enables actual compression ratio and format conversion analysis.', note='Enabling this group deploys additional VCL snippets to your service. All fields are null for non-IO requests.', recommended_with=['A']),
    "METRICS": _g("Aggregate Metrics", "Computed aggregate metrics used for charts and dashboards.", locked=True),
    "VIRTUAL": _g('Virtual Fields', 'Derived or enriched fields that are not present in the raw logs but computed during analysis.', locked=True),
}

GROUP_DEPENDENCIES = {g: info["requires"] for g, info in GROUP_INFO.items() if info.get("requires")}

PRESETS: dict[str, dict[str, Any]] = {
    "minimal": _p("Minimal", "Always-on fields only. Error rates, latency, hit rates, throughput.", []),
    "standard": _p('Standard', 'Recommended for most sites. Request details, infrastructure, basic geo, proxy detection.', ['A', 'C', 'D', 'I']),
    "security": _p('Security', 'Standard + TLS fingerprinting and WAF. For security monitoring.', ['A', 'C', 'D', 'H', 'I', 'J'], field_overrides={'tls_ciphers_sha': True}),
    "performance": _p('Performance', 'Standard + cache deep-dive, network quality core, and origin metrics. For delivery optimization.', ['A', 'B', 'C', 'D', 'F', 'L']),
    "streaming": _p('Streaming', 'Standard + precision geo and full network telemetry. For streaming video analysis.', ['A', 'C', 'D', 'E', 'F', 'G']),
    "full": _p('Full', 'All groups enabled. Maximum data collection.', ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']),
}

INSIGHT_DEFINITIONS: list[dict[str, Any]] = [
    _i('error_spikes', 'origin', 'Error Spikes', 'URLs with abnormally elevated 5xx error rates in the window vs. baseline', ['status', 'url'], ['A']),
    _i('botnet_grouping', 'security', 'Botnet Grouping', 'TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline — attackers rotate IPs but rarely change TLS stacks', ['ja3', 'ja4'], ['H']),
    _i('low_and_slow', 'security', 'Low and Slow Scans', 'IPs probing many sensitive / vulnerability paths at a deliberately low request rate spread over a long span — designed to evade rate limits', ['ip', 'url'], ['A']),
    _i('credential_enumeration', 'security', 'Credential Enumeration / Brute Force', 'IPs generating a spike of 401/403 responses on authentication paths (login, auth, OAuth, password reset) — credential stuffing or brute force', ['ip', 'url', 'status'], ['A']),
    _i('content_discovery', 'security', 'Content-Discovery Scanning', 'IPs generating a burst of 404s across many distinct URLs — directory / endpoint enumeration probing for hidden or vulnerable paths', ['ip', 'url', 'status'], ['A']),
    _i('city_surges', 'traffic', 'City Traffic Surges', 'Cities with traffic volumes significantly higher than their historical baseline', ['city', 'country'], ['D']),
    _i('city_error_spikes', 'origin', 'City Error Spikes', 'Cities experiencing abnormally high error rates compared to their own baseline', ['city', 'status'], ['D']),
    _i('city_latency_regressions', 'edge', 'City Latency Regressions', 'Cities where response times (P95) have significantly slowed down compared to their baseline', ['city', 'elapsed'], ['D']),
    _i('new_city_traffic', 'traffic', 'New City Traffic', 'Cities with zero baseline presence now sending traffic', ['city'], ['D']),
    _i('new_country_traffic', 'traffic', 'New Country Traffic', 'Countries with zero baseline presence now sending traffic', ['country'], ['D']),
    _i('latency_regression', 'edge', 'URL Latency Regressions', 'URLs where response times (P95) have significantly slowed down compared to their baseline', ['url', 'elapsed'], ['A']),
    _i('asn_concentration', 'network', 'ASN Concentration', 'ISPs (ASNs) with a disproportionately large share of total traffic compared to the baseline', ['asn'], ['F']),
    _i('proxy_surge', 'security', 'Proxy Traffic Surge', 'Significant increase in traffic from known anonymizing proxies (VPN, Tor, etc.)', ['p_type'], ['I']),
    _i('ua_monoculture', 'traffic', 'User-Agent Monoculture', 'A single User-Agent string responsible for a massive percentage of traffic — typical for scraping or DDoS bots', ['ua'], ['A']),
    _i('request_size_anomaly', 'security', 'Request Size Anomalies', 'Drastic increase in average request body or header size — signal for data exfiltration or buffer overflow attempts', ['req_bytes', 'req_header_bytes'], ['A']),
    _i('cache_ttl_mismatch', 'edge', 'Cache TTL Mismatches', 'Objects being served from cache with very low hits and low TTLs — indicates inefficient caching strategy', ['cache', 'ttl', 'age', 'hits'], ['B']),
    _i('waf_signal_spikes', 'security', 'WAF Signal Spikes', 'Abnormal increase in specific NGWAF signals (e.g. SQLi, XSS) across multiple IPs', ['waf', 'waf_sig', 'waf_resp', 'status'], ['J']),
    _i('network_asn_health', 'network', 'Network Path (ASN) Health', 'ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline', ['asn', 'ploss', 'rtt_var', 'retrans'], ['F', 'G']),
    _i('region_latency', 'network', 'Billing Region Latency', 'Fastly regions showing elevated edge latency or TTFB spikes', ['server_region', 'elapsed', 'ttfb', 'asn', 'ottfb'], ['C', 'F', 'L']),
    _i('repeated_patterns', 'security', 'Scripted Traffic Patterns', 'IPs demonstrating highly regular request intervals — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits', ['ip']),
    _i('repeated_patterns_fp', 'security', 'Scripted Traffic Patterns (by TLS Fingerprint)', 'TLS fingerprints sending requests on a highly regular cadence — catches IP-rotating scrapers sharing a stable TLS stack', ['ip']),
    _i('new_probe_urls', 'security', 'New Probe URLs', 'Common attack patterns and sensitive paths appearing for the first time', ['url', 'status'], ['A']),
    _i('cipher_spread', 'security', 'Cipher Fingerprint Clustering', 'TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs', ['tls_ciphers_sha'], ['H']),
    _i('impossible_distance', 'security', 'Impossible Distance / Spoofing', 'Traffic where the network latency (RTT) is physically too low for the reported client distance', ['pop', 'lat', 'lon', 'tcp_rtt'], ['C', 'E', 'F']),
    _i('connection_abuse', 'security', 'Connection Reuse Anomaly', 'IPs making an unusually high number of requests per single TCP connection', ['conn_requests'], ['C']),
    _i('origin_latency_spike', 'origin', 'Origin Latency Spike', 'Sudden and significant increase in P95 response time from the origin server', ['ottfb'], ['L']),
    _i('origin_error_rate', 'origin', 'Origin Error Rate', 'Significant increase in 5xx errors returned by the origin server', ['ost'], ['L']),
    _i('origin_retries', 'origin', 'Origin Retries Elevated', 'URLs experiencing frequent retries when fetching from origin, indicating backend instability', ['oretries'], ['L']),
    _i('origin_ip_failure', 'origin', 'Specific Origin IP Failing', 'One or more origin IP addresses are returning significantly more errors than their peers', ['oip', 'ost'], ['L']),
    _i('shield_path_degradation', 'origin', 'Shield Path Degradation', 'Increased latency on the network path between edge POPs and shield POPs', ['rid', 'prid', 'edge', 'pop', 'ottfb'], ['C', 'L']),
    _i('cache_collapse', 'edge', 'Cache Efficiency Collapse', 'URLs whose cacheable hit ratio (HIT/(HIT+MISS)) dropped sharply vs. their baseline', ['url', 'cache'], ['A']),
    _i('cacheability_regression', 'edge', 'Cacheability Regression', 'URLs that flipped from cacheable to mostly PASS (uncacheable) vs. their baseline', ['url', 'cache'], ['A']),
    _i('tail_latency', 'edge', 'Tail Latency Anomaly', 'Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers', ['url', 'elapsed'], ['A']),
    _i('image_optimization_opportunities', 'edge', 'Image Optimization Opportunities', 'Large images being served without modern compression (WebP/AVIF), especially to mobile users', ['url', 'ua', 'resp_bytes'], ['A']),
    _i('asn_metro_performance', 'network', 'ASN/Metro Performance Regressions', 'Specific ISP/Metro combinations showing significantly higher network latency than baseline', ['asn', 'metro', 'tcp_rtt', 'country'], ['D', 'E', 'F']),
    _i('referer_monoculture', 'traffic', 'Referer Monoculture', 'A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood', ['referer'], ['A']),
    _i('method_drift', 'traffic', 'HTTP Method Drift', 'A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods', ['method'], ['A']),
    _i('new_asn_traffic', 'network', 'New ASN Traffic', 'An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range', ['asn'], ['F']),
    _i('asn_hosting_shift', 'network', 'ASN Hosting Shift', "An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure", ['asn', 'p_type'], ['F', 'I']),
    _i('metro_delivery_degradation', 'network', 'Metro Delivery-Rate Degradation', 'US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation', ['metro', 'delivery_rate'], ['E', 'G']),
    _i('connection_type_mix', 'network', 'Connection-Type Mix Shift', 'A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift', ['c_type', 'c_speed'], ['G']),
    _i('pop_latency_regression', 'edge', 'PoP Latency Regression', 'Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency', ['pop', 'elapsed'], ['C']),
    _i('http3_fallback', 'network', 'HTTP/3 → TCP Fallback Spike', 'A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)', ['transport'], ['F']),
    _i('cache_hit_cliff', 'edge', 'Cache HIT-Ratio Cliff', "The whole service's edge cache HIT ratio (HIT/(HIT+MISS)) fell off a cliff vs. baseline — a purge storm, TTL change, or origin Cache-Control regression", ['cache']),
    _i('payload_compression_regression', 'edge', 'Payload Compression Regression', 'Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB', ['url', 'resp_header_content_encoding', 'resp_bytes', 'status'], ['A']),
    _i('session_harvesting', 'security', 'Session-ID Harvesting', 'A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt', ['ip', 'cookie_session'], ['H']),
    _i('timeout_split', 'origin', 'Origin Connect vs Read Timeout Split', 'Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing', ['oconnect_ms', 'ottfb'], ['L']),
    _i('crawler_disparity', 'security', 'Bot & Crawler Referral & Bandwidth Disparity', 'Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth while driving negligible human referral traffic back to the platform', ['ua', 'referer', 'ip', 'resp_bytes', 'timestamp'], ['A']),
    _i('stale_browser_version', 'security', 'Stale Headless Browser', 'Detects anomalous clusters of request traffic using stale Chrome major versions, a standard fingerprint for un-updated Puppeteer/Playwright headless bots', ['ua', 'ip', 'timestamp'], ['A']),
    _i('orphaned_deep_crawl', 'security', 'Orphaned Deep Crawl', 'Flags client IPs performing direct, deep scans of nested catalog/database resource routes with empty HTTP referer headers', ['ip', 'url', 'referer', 'timestamp'], ['A']),
    _i('residential_fingerprint_dispersion', 'security', 'Residential Fingerprint Dispersion', 'Identifies identical TLS/TCP client handshakes distributed across multiple residential ASNs, signaling rotated botnet scans evading ASN blocks', ['ja4', 'ip', 'asn', 'p_type', 'timestamp'], ['H', 'I']),
]

# fmt: on

# Opt-in field IDs: fields flagged ``default_off`` in the catalog are excluded
# from ``resolve_enabled_fields`` even when their group is on, and emit only
# when explicitly opted in via ``field_overrides``. Frozen at import time; the
# catalog is a module-level literal so this never needs re-computing.
DEFAULT_OFF_FIELD_IDS: frozenset[str] = frozenset(f["id"] for f in LOG_FIELD_CATALOG if f.get("default_off"))


def resolve_enabled_fields(cfg: dict) -> set:
    """Expand group selections and per-field overrides into a flat set of enabled field IDs.

    Opt-in fields (``default_off``): a field flagged ``default_off`` in the
    catalog is NOT auto-included by its group. It is emitted only when the
    config explicitly opts it in via ``field_overrides[id] = True``. This is
    the deliberate-opt-in gate for sensitive captures (e.g. ``cookie_session``,
    a per-session client identifier) so that enabling a group / preset does
    not silently start collecting them. Everything else is unchanged: normal
    group fields are auto-enabled, and ``field_overrides`` can force any field
    on (True) or off (False) regardless of its group.
    """
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
        # ``default_off`` fields are opt-in: skip them during group expansion.
        # An explicit ``field_overrides[id] = True`` below still turns them on.
        if field["group"] in enabled_groups and field["id"] not in DEFAULT_OFF_FIELD_IDS:
            enabled.add(field["id"])

    # Apply per-field overrides. This is the sole path that can enable a
    # ``default_off`` field — an explicit opt-in wins over the skip above.
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

    Aggregate-budget guard (F013): individual per-field substr() limits used
    to be applied independently, so a config that summed url(2000) + ua(1000)
    + referer(1000) + 7 custom fields * 2000 = ~18 KB could blow past
    Fastly's 16 KiB silent-truncation cap, dropping the entire log line.
    Track ``budget`` as we lay down VCL: subtract structural overhead and
    fixed-typical-bytes up front, then clamp each variable-length field
    (url/ua/referer + custom fields) to the remaining budget before emitting
    its substr() limit. Greedy allocation in field-catalog order; custom
    fields share whatever's left.
    """
    enabled = resolve_enabled_fields(log_fields_config)
    limits = log_fields_config.get("field_limits") or {}
    custom_fields = [cf for cf in log_fields_config.get("custom_fields", []) if cf.get("enabled", True)]

    # Mirror estimate_log_line_bytes's structural model: braces (2) +
    # key/quote/colon/comma overhead (~5) per enabled built-in and custom field.
    budget = FASTLY_LOG_LINE_DELIVER_MAX
    budget -= 2 + len(enabled) * 5
    budget -= len(custom_fields) * 5
    # Pre-subtract fixed-length built-ins (everything except the variable
    # url/ua/referer trio). typical_bytes is the calibrated avg from
    # estimate_log_line_bytes and represents what those fields actually
    # write at run time.
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled and field["id"] not in ("url", "ua", "referer"):
            budget -= int(field.get("typical_bytes", 0) or 0)

    parts = []
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            vcl = field["vcl"]
            if vcl is None:
                continue
            # Inject dynamic limits
            if field["id"] == "url":
                limit = int(limits.get("url", 2000))
                limit = max(0, min(limit, budget))
                budget -= limit
                # Overwrite the static substr limit in the built-in VCL
                vcl = vcl.replace("substr(req.url, 0, 2000)", f"substr(req.url, 0, {limit // 6})")
            elif field["id"] == "ua":
                # Security: keep the substr cap even when generating the
                # alternative VCL variant. The edge-side substr (in vcl_recv)
                # is a *first* truncation — but we never want a 100 KB header
                # to slip through if the edge snippet is missing or fails to
                # run (e.g., on a request that bypasses our snippet stack).
                # An unbounded UA can truncate the entire JSON log line at
                # the 16 KB Fastly limit, dropping the request from the audit
                # trail entirely (repudiation attack).
                ua_limit = int(limits.get("ua", 1000))
                ua_limit = max(0, min(ua_limit, budget))
                budget -= ua_limit
                vcl = (
                    f'"ua":"%{{json.escape(substr(if(req.http.x-fos-edge-data:ua != "",'
                    f' req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, {ua_limit // 6}))}}V"'
                )
            elif field["id"] == "referer":
                # Same reasoning as above — keep the substr cap.
                ref_limit = int(limits.get("referer", 1000))
                ref_limit = max(0, min(ref_limit, budget))
                budget -= ref_limit
                vcl = (
                    f'"referer":"%{{json.escape(substr(if(req.http.x-fos-edge-data:referer != "",'
                    f' req.http.x-fos-edge-data:referer, req.http.Referer), 0, {ref_limit // 6}))}}V"'
                )

            parts.append(vcl)

    # Append enabled custom fields in alphabetical order for determinism
    for cf in sorted(custom_fields, key=lambda x: x["name"]):
        name = cf["name"]
        stage = cf.get("collection_stage", "edge")
        value_type = cf.get("value_type", "string")

        # Cap per-field limit at remaining budget so the aggregate line
        # cannot exceed FASTLY_LOG_LINE_DELIVER_MAX.
        estimate = cf.get("bytes_estimate")
        default_limit = min(max((estimate or 0) * 4, 40), 2000) if estimate else 2000
        cf_limit = int(cf.get("byte_limit") or limits.get(name) or default_limit)
        # Numeric/boolean values are short (a u64 is ≤20 chars). Never let
        # them reserve the 2000-char string default: greedy allocation runs
        # in alphabetical order, so a handful of numeric fields each grabbing
        # 2000 chars exhausts the line budget before the alphabetically-last
        # fields, which then get substr(..., 0, 0) and silently log null.
        # (Surfaced when edge_score_rtt_us + edge_sid both went null after
        # the latency fields were added.)
        if value_type in ("numeric", "boolean"):
            cf_limit = min(cf_limit, _NUMERIC_FIELD_MAX_LEN)
        cf_limit = max(0, min(cf_limit, budget))
        budget -= cf_limit

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
                    f'substr({raw_expr}, 0, {cf_limit}) ~ "^-?[0-9]+(\\\\.[0-9]+)?$", substr({raw_expr}, 0, {cf_limit}), "null")'
                )
                entry = f'"{name}":%{{{vcl_macro}}}V'
            else:
                # 016: clamp the string-field value to a sane length
                # (default 2000) BEFORE json.escape so a multi-megabyte
                # attacker-controlled custom field cannot push the log
                # line past Fastly's 16 KB limit and silently drop the
                # whole entry. The substr is INSIDE json.escape so the
                # encoded length stays bounded.
                vcl_macro = (
                    f'json.escape(if(fastly.ff.visits_this_service == 0, substr({raw_expr}, 0, {cf_limit // 6}), ""))'
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
            vcl_macro = (
                f'if(substr({expr}, 0, {cf_limit}) ~ "^-?[0-9]+(\\\\.[0-9]+)?$", substr({expr}, 0, {cf_limit}), "null")'
            )
            entry = f'"{name}":%{{{vcl_macro}}}V'
        else:
            # 016: substr-clamp the value before json.escape so an
            # oversized custom string field cannot push the line past
            # Fastly's 16 KB log-line limit.
            vcl_macro = f"json.escape(substr({expr}, 0, {cf_limit // 6}))"
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
        if f.get("default_off"):
            # Opt-in field: the admin UI renders it OFF even when its group is
            # enabled, requiring an explicit toggle to start collecting it.
            entry["default_off"] = True
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
    ordered_groups = [None, "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "METRICS"]
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
                "recommended_with": info.get("recommended_with"),
                "total_bytes": total_bytes,
                "fields": [f["id"] for f in fields],
            }
        )
    return result


# ---------------------------------------------------------------------------
# Custom field support

_BUILTIN_FIELD_NAMES = frozenset(f["id"] for f in LOG_FIELD_CATALOG)


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
    if not VALID_NAME_RE.fullmatch(name):
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


# ── Canonical SQL fragment builders (formerly backend.core.metric_expressions) ──

# Cache states that count as an edge hit (SQL ``IN`` tuple literal).
HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def coalesce(expr: str, default: str) -> str:
    """Render ``COALESCE(<expr>, <default>)`` byte-for-byte."""
    return f"COALESCE({expr}, {default})"


def sum_or_zero(expr: str, *, zero: str = "0", where: str | None = None) -> str:
    """Render a NULL-safe sum: ``COALESCE(SUM(<expr>)[ FILTER (WHERE <where>)], <zero>)``."""
    agg = f"SUM({expr})" if where is None else f"SUM({expr}) FILTER (WHERE {where})"
    return coalesce(agg, zero)


def column_or_default(
    actual_cols: Collection[str],
    col: str,
    default: str,
    *,
    cast: str | None = None,
) -> str:
    """Return a quoted-column expression with a NULL default, or the bare default."""
    if col not in actual_cols:
        return default
    ref = f'"{col}"' if cast is None else f'CAST("{col}" AS {cast})'
    return coalesce(ref, default)


def origin_latency_us(actual_cols: Collection[str], *, absent: str | None = "NULL") -> str | None:
    """Return the origin-latency-in-microseconds SQL expression for a schema."""
    has_ottfb = "ottfb" in actual_cols
    has_ttfb = "ttfb" in actual_cols
    if has_ottfb and has_ttfb:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if has_ottfb:
        return '"ottfb"'
    if has_ttfb:
        return '"ttfb" * 1000000.0'
    return absent


_this_module = sys.modules[__name__]
sys.modules["backend.core.metric_expressions"] = _this_module
_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "metric_expressions", _this_module)

# ---------------------------------------------------------------------------
# Enums (typed counterparts for the stringly-typed columns in the catalog)
# ---------------------------------------------------------------------------


class DuckType(StrEnum):
    """DuckDB column types in use across the field catalog.

    Values are kept identical to the strings that appear in the catalog's
    `duckdb_type` key so cross-referencing diffs is trivial.
    """

    TIMESTAMP = "TIMESTAMP"
    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    UTINYINT = "UTINYINT"
    USMALLINT = "USMALLINT"
    UINTEGER = "UINTEGER"
    UBIGINT = "UBIGINT"
    BIGINT = "BIGINT"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"


class Group(StrEnum):
    """Field grouping. Values match the legacy `group` column.

    `CORE` replaces the legacy `None` sentinel for always-on fields so the
    type can stay `Group` end-to-end instead of `Group | None`. The
    `from_legacy` classmethod handles the conversion at boundaries.
    """

    CORE = "CORE"  # legacy None
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    I = "I"  # noqa: E741 — single-letter group code is the public contract
    J = "J"
    K = "K"
    L = "L"
    M = "M"
    METRICS = "METRICS"
    VIRTUAL = "VIRTUAL"
    INTERNAL = "INTERNAL"

    @classmethod
    def from_legacy(cls, raw: str | None) -> Group:
        """Translate the legacy `None | "A".."M" | "METRICS" | "VIRTUAL" | "INTERNAL"`."""
        if raw is None:
            return cls.CORE
        return cls(raw)


# ---------------------------------------------------------------------------
# LogField dataclass — the row type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogField:
    """One row of the field catalog.

    Frozen + slotted: mutation throws at runtime, instances are hashable
    (usable as dict keys), per-instance memory is minimised. Constructed
    via the catalog adapter at module init time (`_field_from_dict`); the
    dict-authored catalog is the maintained source, this is the typed read
    view over it.

    Notes on field semantics that the dataclass shape captures:

    - `vcl=None` means the field is derived (computed by DuckDB SQL) or
      virtual (synthesised during analysis). `is_derived` is the public
      accessor.
    - `substr_cap` is the byte cap currently baked into the VCL literal for
      url/ua/referer (and any custom fields routed through this registry).
      `render_vcl(limits)` injects a runtime override without forcing every
      call site to know which fields have caps.
    - `required_by` is a tuple of insight IDs that name this field as a
      hard dependency. The legacy list is mutable; this is intentionally
      not.
    """

    code: str
    label: str
    group: Group
    duck_type: DuckType

    # All optional; defaulted for legacy compat with mostly-bare entries.
    description: str = ""
    vcl: str | None = None
    typical_bytes: int = 0
    required_by: tuple[str, ...] = ()
    substr_cap: int | None = None
    individually_toggleable: bool = False
    # Opt-in field: excluded from ``resolve_enabled_fields`` even when its
    # group is enabled; emits only when explicitly turned on via
    # ``field_overrides``. See ``log_fields.DEFAULT_OFF_FIELD_IDS``.
    default_off: bool = False
    formatter: str | None = None
    unit: str | None = None
    precision: int | None = None
    note: str | None = None

    # ---- Derived properties (zero LOC at call sites) -----------------------

    @property
    def is_derived(self) -> bool:
        """True for metrics/virtual/internal fields (`vcl is None`)."""
        return self.vcl is None


# ---------------------------------------------------------------------------
# Legacy-dict → LogField construction
# ---------------------------------------------------------------------------


_SUBSTR_RE = re.compile(r"substr\([^,]+,\s*0,\s*(\d+)\)")


def _detect_substr_cap(vcl: str | None) -> int | None:
    """Find the byte cap baked into a `substr(expr, 0, N)` call in VCL.

    Mirrors the legacy code's hard-coded knowledge that url/ua/referer (and
    any custom field VCL the catalog ships) carry a cap in the literal.
    Returns None when no substr call is present or the cap can't be parsed.
    """
    if vcl is None:
        return None
    m = _SUBSTR_RE.search(vcl)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:  # pragma: no cover — regex guarantees digits
        return None


def _field_from_dict(d: Mapping[str, Any]) -> LogField:
    """Build a `LogField` from a `LOG_FIELD_CATALOG` dict entry.

    Stable adapter — runs once per field at module init time. The dict-
    literal authoring format and the LogField read view both stay; this
    function is the single bridge that derives the latter from the former.

    ``d`` is typed ``Mapping[str, Any]`` (not ``Mapping[str, object]``) to
    match the catalog literal's annotation in _log_fields_data.py — the
    heterogeneous value types (str / int / bool / list / None) are
    discriminated per-key here rather than at the type-system level.
    """
    code = str(d["id"])
    raw_group = d.get("group")
    group = Group.from_legacy(raw_group if raw_group is None else str(raw_group))
    duck_type = DuckType(str(d["duckdb_type"]))
    vcl_raw = d.get("vcl")
    vcl: str | None = None if vcl_raw is None else str(vcl_raw)
    required_by_raw = d.get("required_by") or ()
    required_by = tuple(str(x) for x in required_by_raw)
    return LogField(
        code=code,
        label=str(d.get("label", code)),
        group=group,
        duck_type=duck_type,
        description=str(d.get("description", "")),
        vcl=vcl,
        typical_bytes=int(d.get("typical_bytes", 0) or 0),
        required_by=required_by,
        substr_cap=_detect_substr_cap(vcl),
        individually_toggleable=bool(d.get("individually_toggleable", False)),
        default_off=bool(d.get("default_off", False)),
        formatter=_opt_str(d.get("formatter")),
        unit=_opt_str(d.get("unit")),
        precision=_opt_int(d.get("precision")),
        note=_opt_str(d.get("note")),
    )


def _opt_str(v: object) -> str | None:
    return None if v is None else str(v)


def _opt_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):  # pragma: no cover — catalog uses ints
        return None


# ---------------------------------------------------------------------------
# Registry construction
# ---------------------------------------------------------------------------

# Import the catalog at module init. The dict literals are the authoring
# format and the LogField tuple is the read view. Both must agree byte-
# for-byte; the cheapest way to guarantee that is to derive one from the
# other, with a boot-time test asserting the equivalence.
_CATALOG = LOG_FIELD_CATALOG

REGISTRY: tuple[LogField, ...] = tuple(_field_from_dict(entry) for entry in _CATALOG)
"""Tuple of every known log field, in wire order (matches Rust scorer)."""


BY_CODE: Mapping[str, LogField] = MappingProxyType({f.code: f for f in REGISTRY})
"""Code → field lookup, O(1). Read-only view."""

if len(BY_CODE) != len(REGISTRY):  # pragma: no cover — guards a programmer mistake
    raise RuntimeError("duplicate field codes in REGISTRY — check log_fields.py for collisions")


def _group_index() -> Mapping[Group, tuple[LogField, ...]]:
    """Group → ordered tuple of fields belonging to it."""
    bucket: dict[Group, list[LogField]] = {g: [] for g in Group}
    for f in REGISTRY:
        bucket[f.group].append(f)
    return MappingProxyType({g: tuple(items) for g, items in bucket.items()})


BY_GROUP: Mapping[Group, tuple[LogField, ...]] = _group_index()
"""Group → ordered tuple of fields. Read-only view."""


# ---------------------------------------------------------------------------
# Lookup helpers (the public API the migration will route callers through)
# ---------------------------------------------------------------------------


def get(code: str) -> LogField:
    """Return the field with the given code. Raises `KeyError` on miss.

    Use this in router code paths where an unknown code is a programmer
    bug, not a user-input failure. For user-input validation use `try_get`.
    """
    return BY_CODE[code]


def try_get(code: str) -> LogField | None:
    """Return the field with the given code, or None when not present."""
    return BY_CODE.get(code)


def in_group(group: Group) -> tuple[LogField, ...]:
    """Return all fields belonging to a specific group, in catalog order."""
    return BY_GROUP[group]


def derived() -> tuple[LogField, ...]:
    """Return all derived fields (`vcl is None` — metrics, virtual, internal)."""
    return tuple(f for f in REGISTRY if f.is_derived)


# ---------------------------------------------------------------------------
# System-managed field filtering (CMCD, Scoring)
# ---------------------------------------------------------------------------
#
# CMCD and edge-scoring fields are generated on-demand from feature toggles
# (backend/provision/orchestrator.py's FeatureState) and are never persisted
# as user-authored custom_fields entries. Both provisioning (writing config)
# and the custom-fields router (reading config) need to strip them from a
# raw custom_fields list. The predicate lives here — a layer both callers
# can import downward from — instead of in either caller's own package.
# `backend.routers.services.core` re-exports both names for backward
# compatibility (same-identity re-export; see AGENTS.md's shim convention).


def _is_system_field(field_name: str) -> bool:
    """Return True if field_name is a system-managed field (CMCD or Scoring).

    System fields are generated on-demand from feature toggles and should
    not be displayed as user-editable custom fields.
    """
    return field_name.startswith("cmcd_") or field_name.startswith("edge_")


def _filter_user_custom_fields(custom_fields: list[dict]) -> list[dict]:
    """Filter out system-managed fields from custom_fields list.

    Returns only user-defined custom fields, excluding CMCD and Scoring fields.
    """
    return [f for f in custom_fields if not _is_system_field(f.get("name", ""))]


# ---------------------------------------------------------------------------
# Caller-facing API shape helpers + legacy re-exports
# ---------------------------------------------------------------------------
#
# Every helper and constant the migration plan ships on the registry is a
# direct re-export of the legacy `backend.core.log_fields` symbol — same
# function/object identity, zero behavior drift. Callers can flip
# ``from backend.core.log_fields import X`` to
# ``from backend.core.log_fields import X`` and observe identical
# behavior (parity guard: ``tests/core/test_field_registry.py``).
#
# When a downstream symbol gets re-implemented on top of REGISTRY
# primitives, replace the re-export with the new function — the parity
# test will fail loudly until both sides agree.


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


from backend.models._base import MetricType  # noqa: E402

METRIC_DEFINITIONS = {
    MetricType.REQUESTS.value: {"sql": "count(*)", "label": "Requests"},
    MetricType.ERRORS_5XX.value: {"sql": "sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END)", "label": "5xx Errors"},
    "5xx_rate": {
        "sql": "sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "5xx Error Rate",
    },
    MetricType.ERRORS_4XX.value: {
        "sql": "sum(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END)",
        "label": "4xx Errors",
    },
    "4xx_rate": {
        "sql": "sum(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "4xx Error Rate",
    },
    MetricType.LATENCY_P95.value: {
        "sql": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(elapsed AS DOUBLE)) / 1000.0",
        "label": "p95 Latency (s)",
    },
    MetricType.HIT_RATE.value: {
        "sql": "SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "Cache Hit Rate",
    },
    MetricType.THROUGHPUT.value: {"sql": "sum(resp_bytes)", "label": "Bandwidth"},
    MetricType.TTFB.value: {
        "sql": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(ottfb AS DOUBLE)) / 1000.0",
        "label": "p95 TTFB (s)",
    },
    MetricType.REQ_SIZE.value: {"sql": "avg(req_bytes + req_header_bytes)", "label": "Request Size (Bytes)"},
}


def get_metric_sql(metric: str, status_codes: list[int] | None = None, table_name: str | None = None) -> str:
    """Return the SQL aggregation expression or a full SELECT fragment for a metric."""
    status_clause = ""
    if status_codes and isinstance(status_codes, list) and len(status_codes) > 0:
        codes_str = ", ".join(str(c) for c in status_codes if isinstance(c, int))
        if codes_str:
            status_clause = f" AND status IN ({codes_str})"

    if metric == MetricType.SPECIFIC_STATUS.value:
        agg_sql = f"sum(CASE WHEN 1=1 {status_clause} THEN 1 ELSE 0 END)"
    elif metric == "specific_status_rate":
        agg_sql = f"sum(CASE WHEN 1=1 {status_clause} THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)"
    else:
        agg_sql = METRIC_DEFINITIONS.get(metric, {}).get("sql", "count(*)")

    if table_name:
        if metric in (MetricType.ERRORS_5XX.value, MetricType.ERRORS_4XX.value, MetricType.SPECIFIC_STATUS.value):
            # Optimization: use WHERE clause instead of sum(CASE WHEN...) for simple counts
            if metric == MetricType.ERRORS_5XX.value:
                return f"SELECT count(*) FROM {table_name} WHERE status >= 500"
            elif metric == MetricType.ERRORS_4XX.value:
                return f"SELECT count(*) FROM {table_name} WHERE status BETWEEN 400 AND 499"
            elif metric == MetricType.SPECIFIC_STATUS.value:
                return f"SELECT count(*) FROM {table_name} WHERE 1=1 {status_clause}"
        elif metric == MetricType.TTFB.value:
            return f"SELECT {agg_sql} FROM {table_name} WHERE ottfb IS NOT NULL"

        return f"SELECT {agg_sql} FROM {table_name}"

    return agg_sql


# ---------------------------------------------------------------------------
# Unified VCL safety validation (folded from backend.utils.vcl_safety)
# ---------------------------------------------------------------------------


class VclSafetyError(ValueError):
    """Raised when a value can't be safely embedded in generated VCL.

    Subclasses :class:`ValueError` so existing ``except ValueError``
    handlers at legacy call sites keep catching it.
    """


_FORBIDDEN_STRICT = re.compile(r'["\\\x00-\x1f\x7f]')
_FORBIDDEN_ALLOW_BACKSLASH = re.compile(r'["\x00-\x1f\x7f]')


def validate_quoted_literal(value: str, *, allow_backslash: bool = False, field: str = "value") -> str:
    """Validate a value destined for a VCL double-quoted string literal."""
    pattern = _FORBIDDEN_ALLOW_BACKSLASH if allow_backslash else _FORBIDDEN_STRICT
    m = pattern.search(value)
    if m is not None:
        raise VclSafetyError(
            f"{field} contains a character unsafe to embed in a VCL string literal at offset "
            f"{m.start()} ({value[m.start()]!r}); refusing to generate VCL "
            "(values are validated upstream + Fastly /validate-gated, so this "
            "indicates a caller that bypassed those checks)."
        )
    return value


def validate_log_format_macro(macro: str) -> str:
    """Validate the inline-VCL payload of a Fastly ``%{...}V`` log macro."""
    if ";" in macro or "/*" in macro or "//" in macro or re.search(r"(?<!\\)[{}]", macro):
        raise VclSafetyError("VCL macro contains invalid characters (;, /*, //, unescaped {, unescaped })")
    return macro


@dataclass
class LintResult:
    """Outcome of running falco on a VCL document."""

    ok: bool
    errors: list[str]
    warnings: list[str]
    skipped: bool = False
    skipped_reason: str = ""


def _parse_falco_output(returncode: int, out: str, err: str) -> tuple[list[str], list[str], int | None]:
    """Parse falco ``-v`` output into (errors, warnings, summary_error_count)."""
    out = out.strip()
    err = err.strip()
    combined = "\n".join(filter(None, [out, err]))

    errors: list[str] = []
    warnings: list[str] = []

    summary_re = re.compile(r"(\d+)\s+errors?\s*,\s*(\d+)\s+warnings?")
    summary_match = None
    for line in combined.splitlines():
        if "errors" in line and "warnings" in line and summary_re.search(line):
            summary_match = summary_re.search(line)
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if "[ERROR]" in stripped or "🔥 [ERROR]" in stripped:
            errors.append(stripped)
        elif "[WARNING]" in stripped or "❗ [WARNING]" in stripped:
            warnings.append(stripped)
        elif "[INFO]" in stripped or "🔈 [INFO]" in stripped:
            warnings.append(stripped)

    summary_errors = int(summary_match.group(1)) if summary_match else None
    return errors, warnings, summary_errors


def lint_document(
    vcl_text: str,
    *,
    falco_bin: str | None = None,
    snippet_name: str = "vcl_document",
    timeout: int = 10,
    verbose: bool = True,
) -> LintResult:
    """Run ``falco lint`` over a COMPLETE VCL document."""
    if falco_bin is None:
        falco_bin = shutil.which("falco")
    if falco_bin is None:
        logger.warning(
            "[vcl_safety] falco binary not on PATH; static analysis skipped for %s "
            "(production should install falco via the backend Dockerfile)",
            snippet_name,
        )
        return LintResult(ok=True, errors=[], warnings=[], skipped=True, skipped_reason="falco binary not found")

    try:
        returncode, out, err = _run_falco_lint(falco_bin, vcl_text, timeout=timeout, verbose=verbose)
    except subprocess.TimeoutExpired:
        return LintResult(
            ok=False,
            errors=[f"falco lint timed out after {timeout}s for {snippet_name!r}"],
            warnings=[],
        )

    errors, warnings, summary_errors = _parse_falco_output(returncode, out, err)
    ok = summary_errors == 0 if summary_errors is not None else (returncode == 0 and not errors)

    if summary_errors and summary_errors > 0 and not errors:
        errors.append(
            f"falco reported {summary_errors} error(s) but no parseable diagnostics; stdout={out.strip()[:200]!r}"
        )

    return LintResult(ok=ok, errors=errors, warnings=warnings)


def _run_falco_lint(
    falco_bin: str,
    vcl_text: str,
    *,
    timeout: int,
    verbose: bool,
    redact_path_to: str | None = None,
) -> tuple[int, str, str]:
    """Write ``vcl_text`` to a tempfile and run ``falco [-v] lint <file>``.

    Shared subprocess plumbing for the two falco callers in this tree —
    :func:`lint_log_format` (log-format JSON+VCL check) and
    :func:`backend.utils.vcl_validator.lint_vcl` (scoring-snippet VCL
    check). Each caller resolves ``falco_bin`` via its own
    ``shutil.which("falco")`` (so test patches on the caller's module
    namespace continue to work) and parses output its own way; this
    helper only handles tempfile lifecycle + subprocess invocation +
    optional path redaction.

    ``redact_path_to``: when set, every occurrence of the tempfile path
    in stdout/stderr is replaced with the given string so error messages
    don't leak a ``/tmp/<random>.vcl`` filename to the operator.

    Returns ``(returncode, stdout, stderr)``. Propagates
    :class:`subprocess.TimeoutExpired` for the caller to translate into
    its own failure shape; the tempfile is removed in either case.
    """
    argv = [falco_bin]
    if verbose:
        argv.append("-v")
    # Path appended after tempfile is written.
    argv.extend(["lint", ""])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".vcl", delete=False, encoding="utf-8") as tmp:
        tmp.write(vcl_text)
        tmp_path = tmp.name

    try:
        argv[-1] = tmp_path
        res = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        stdout = res.stdout or ""
        stderr = res.stderr or ""
        if redact_path_to is not None:
            stdout = stdout.replace(tmp_path, redact_path_to)
            stderr = stderr.replace(tmp_path, redact_path_to)
        return res.returncode, stdout, stderr
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def log_format_to_vcl_log(raw: str) -> str:
    """Convert a Fastly log format template string to a VCL log concatenation.

    Fastly format templates use %{vcl_expr}V for inline VCL expressions; everything
    else is literal text. This produces the equivalent VCL log statement body:
        {"literal"} + vcl_expr + {"literal"} ...
    which is what Fastly generates internally when it compiles the logging endpoint.
    """
    # 1. Split FIRST on the raw template (with \{ / \} escapes intact). We
    # cannot pre-unescape: macro-content validation below relies on being
    # able to tell raw `{` / `}` (suspicious; injection vector) from
    # `\{` / `\}` (legitimate Fastly literal-brace escape, used in patterns
    # like `strftime(\{"format"\}, time.start)`). See audit finding 008.
    parts = re.split(r"%\{(.*?)\}V", raw, flags=re.DOTALL)
    vcl_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            # Literal text — unescape Fastly's \{ / \} into real braces,
            # then wrap as a VCL heredoc string literal {"..."}.
            part = part.replace("\\{", "{").replace("\\}", "}")
            if part:
                # We only need to worry if the literal text itself contains the
                # heredoc closing delimiter "}. This is extremely rare in JSON
                # (which uses " } or "} followed by more).
                # For safety, if it exists, we split it into two heredocs.
                if '"}' in part:
                    part = part.replace('"}', '"} + {"}')
                vcl_parts.append(f'{{"{part}"}}')
        else:
            # VCL expression — reject macro content containing a raw `;`
            # OR an unescaped `{` / `}` (a brace not preceded by `\`).
            # Those are the building blocks of the VCL-injection attack
            # from audit finding 008: `;` terminates the surrounding log
            # statement, then `}` closes the vcl_log block, then `{`
            # opens a new attacker-controlled subroutine. Legitimate
            # heredoc patterns like `strftime(\{"format"\}, ...)` use
            # `\{` / `\}` escapes and pass cleanly.
            # Reject macro content that could inject VCL structure (;, /*,
            # //, unescaped { / }). Delegated to the unified safety module
            # (see audit finding 008); VclSafetyError subclasses ValueError
            # so existing handlers keep catching it.
            validate_log_format_macro(part)
            var = part.replace("\\{", "{").replace("\\}", "}").strip().replace('\\"', '"')
            vcl_parts.append(var)

    # Use + for concatenation to satisfy Falco/modern VCL
    return " + ".join(vcl_parts)


def lint_log_format(format_str: str, snippets: dict[str, str] | None = None) -> tuple[bool, str]:
    """Validate a log format string for JSON structure and VCL syntax.

    If snippets are provided, they are also linted in their respective subroutines.
    Returns (is_valid, message).
    """
    if not format_str or not format_str.strip():
        return False, "Format is empty"

    # Check for internal newlines (DuckDB requirement)
    if "\n" in format_str or "\r" in format_str:
        return False, "Format contains newlines. It must be a single-line string."

    # 1. Validate JSON structure by masking macros
    # 0. Strip Fastly VCL escapes like \{ and \} for JSON validation
    unescaped_str = format_str.replace("\\{", "{").replace("\\}", "}")

    # Mask macros: %{var}V -> "vcl_macro"
    test_str = re.sub(r'"%\{.*?\}V"', '"vcl_macro"', unescaped_str)
    test_str = re.sub(r'"%\{.*?\}t"', '"time_macro"', test_str)
    test_str = re.sub(r"%\{.*?\}V", '"vcl_macro"', test_str)
    test_str = re.sub(r"%\{.*?\}t", '"time_macro"', test_str)

    try:
        data = json.loads(test_str)
        if not isinstance(data, dict):
            return False, "Format must be a JSON object"
        if "timestamp" not in data:
            return False, "Missing required field: 'timestamp'"
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON structure: {str(e)}"

    # 2. Deeper VCL validation using falco
    falco_bin = shutil.which("falco")
    if not falco_bin:
        return True, "Valid JSON (falco linter not found in PATH for VCL validation)"

    try:
        log_body = log_format_to_vcl_log(format_str)
    except ValueError as e:
        return False, f"VCL Error: {str(e)}"

    # Minimal Fastly VCL file that exercises the log statement and any snippets.
    # Includes the subroutines falco expects so it does not complain about missing hooks.
    snip = snippets or {}

    vcl_src = (
        f"sub vcl_recv    {{\n  #FASTLY recv\n{snip.get('recv_reset', '')}\n{snip.get('recv', '')}\n}}\n"
        f"sub vcl_hash    {{\n  #FASTLY hash\n{snip.get('hash', '')}\n}}\n"
        f"sub vcl_hit     {{\n  #FASTLY hit\n{snip.get('hit', '')}\n}}\n"
        f"sub vcl_miss    {{\n  #FASTLY miss\n{snip.get('miss', '')}\n}}\n"
        f"sub vcl_pass    {{\n  #FASTLY pass\n{snip.get('pass', '')}\n}}\n"
        f"sub vcl_fetch   {{\n  #FASTLY fetch\n{snip.get('fetch', '')}\n}}\n"
        f"sub vcl_error   {{\n  #FASTLY error\n{snip.get('error', '')}\n}}\n"
        f"sub vcl_deliver {{\n  #FASTLY deliver\n{snip.get('deliver', '')}\n}}\n"
        f"sub vcl_log     {{\n  #FASTLY log\n  log {log_body};\n}}\n"
    )

    try:
        returncode, stdout, stderr = _run_falco_lint(
            falco_bin, vcl_src, timeout=15, verbose=False, redact_path_to="vcl-config"
        )
    except subprocess.TimeoutExpired:
        return True, "Valid JSON (falco validation timed out)"
    except Exception as e:
        return True, f"Valid JSON (VCL validation skipped: {str(e)})"

    if returncode != 0:
        # Fail-closed parse: scan BOTH streams. The prior ``msg = stdout or
        # stderr`` short-circuited the moment falco emitted any non-error
        # banner to stdout (version line, "linting…", etc.), letting an
        # exit_code != 0 with errors on stderr silently fall through to the
        # ``return True`` below — a fail-open VCL validator that accepted
        # invalid log formats.
        msg = (stdout or "") + "\n" + (stderr or "")
        errors = []
        for raw_line in msg.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "[ERROR]" in line or "ERROR:" in line or "💥" in line:
                errors.append(line)

        if errors:
            return False, "VCL Error: " + errors[0]

        # falco exited non-zero but we couldn't pin a specific error line.
        # Fail closed anyway — a non-zero exit means falco rejected the VCL,
        # and falling through to ``return True`` here was a fail-open hole
        # that accepted invalid VCL whenever falco's output format drifted.
        return False, "VCL Error: falco rejected the VCL (exit status nonzero)"

    return True, "Valid VCL configuration"


# ---------------------------------------------------------------------------
# VCL static analysis + user-input validation for scoring snippets
# (folded from backend.utils.vcl_validator)
# ---------------------------------------------------------------------------

MAX_REGEX_BYTES = 2048


class RegexValidationError(ValueError):
    """Raised when an operator-supplied URL exclusion regex fails policy."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


def validate_url_exclusion_regex(value: str) -> str:
    """Cheap input policing on a user-typed regex before VCL interpolation."""
    if not isinstance(value, str):
        raise RegexValidationError("type", f"regex must be a string, got {type(value).__name__}")
    cleaned = value.strip()
    if not cleaned:
        return ""
    if len(cleaned.encode("utf-8")) > MAX_REGEX_BYTES:
        raise RegexValidationError(
            "too_long",
            f"regex exceeds {MAX_REGEX_BYTES}-byte limit (got {len(cleaned.encode('utf-8'))})",
        )
    try:
        validate_quoted_literal(cleaned, allow_backslash=True, field="regex")
    except VclSafetyError:
        m = _FORBIDDEN_ALLOW_BACKSLASH.search(cleaned)
        ch = m.group(0) if m else "?"
        raise RegexValidationError(
            "disallowed_char",
            f"regex contains disallowed character (codepoint U+{ord(ch):04X}): "
            "double-quotes and control characters are not permitted because they "
            "would break the surrounding VCL string literal",
        )
    try:
        re.compile(cleaned)
    except re.error as exc:
        raise RegexValidationError(
            "invalid_regex", f"regex failed to compile: {exc.msg} at position {exc.pos}"
        ) from exc
    return cleaned


def _falco_binary() -> str | None:
    """Return the falco binary path, or None if unavailable."""
    return shutil.which("falco")


def lint_vcl(
    snippet: str,
    *,
    snippet_name: str = "scoring_snippet",
    wrap_subroutine: str | None = "vcl_recv",
) -> LintResult:
    """Run ``falco lint`` over a VCL snippet body."""
    falco_bin = _falco_binary()
    if falco_bin is None:
        logger.warning(
            "[vcl_validator] falco binary not on PATH; static analysis skipped for %s "
            "(production should install falco via the backend Dockerfile)",
            snippet_name,
        )
        return LintResult(ok=True, errors=[], warnings=[], skipped=True, skipped_reason="falco binary not found")

    if wrap_subroutine:
        stage = wrap_subroutine.removeprefix("vcl_") if wrap_subroutine.startswith("vcl_") else wrap_subroutine
        full_vcl = (
            "backend F_origin_0 {\n"
            '  .host = "example.com";\n'
            '  .port = "80";\n'
            "}\n"
            "backend F_session_scorer {\n"
            '  .host = "scorer.edgecompute.app";\n'
            '  .port = "443";\n'
            "}\n\n"
            f"sub {wrap_subroutine} {{\n"
            f"  #FASTLY {stage}\n"
            "  declare local var.fastly_req_do_shield BOOL;\n"
            f"{snippet}\n"
            '  if (req.http.X-Lint-Sentinel == "lint-only-never-fires") {\n'
            "    set req.backend = F_origin_0;\n"
            "    set req.backend = F_session_scorer;\n"
            "    set var.fastly_req_do_shield = false;\n"
            "  }\n"
            "}\n"
        )
    else:
        full_vcl = snippet

    return lint_document(full_vcl, falco_bin=falco_bin, snippet_name=snippet_name, timeout=10, verbose=True)


def validate_recv_exclusion_regex_with_lint(
    user_regex: str,
    *,
    build_full_snippet: Callable[[str], str],
    require_falco: bool = True,
) -> tuple[str, LintResult]:
    """One-call validation: input policy → assemble snippet → falco lint."""
    cleaned = validate_url_exclusion_regex(user_regex)
    full_snippet = build_full_snippet(cleaned)
    lint = lint_vcl(full_snippet, snippet_name="scoring_recv")
    if lint.skipped and require_falco:
        raise RegexValidationError(
            "falco_unavailable",
            f"VCL static-analysis tool unavailable: {lint.skipped_reason}. Refusing to ship unchecked VCL.",
        )
    if not lint.ok:
        joined = "\n".join(lint.errors[:5])
        raise RegexValidationError("vcl_lint", f"falco lint failed:\n{joined}")
    return cleaned, lint


# ---------------------------------------------------------------------------
# Caller-supplied Fastly token validation for destructive operations
# (folded from backend.utils.fastly_auth)
# ---------------------------------------------------------------------------

_DESTRUCTIVE_SCOPE = "global"


def _parse_scopes(raw: Any) -> list[str]:
    """Normalize the ``scope`` field from /tokens/self into a list of scopes."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if s]
    if isinstance(raw, str):
        return [s for s in raw.split() if s]
    return []


def validate_destructive_token(token: str, *, service_id: str) -> dict[str, Any]:
    """Validate that ``token`` is allowed to perform destructive ops on ``service_id``."""
    token = (token or "").strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "token_required",
                "message": (
                    "A Fastly API token with the 'global' scope is required "
                    "for destructive operations. Server-stored credentials are "
                    "not accepted here."
                ),
            },
        )

    try:
        token_data = fastly("GET", "/tokens/self", token=token)
    except Exception as e:
        logger.warning("[fastly-auth] /tokens/self call failed: %s", e)
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Could not validate token with Fastly."},
        )

    if not isinstance(token_data, dict):
        logger.warning("[fastly-auth] /tokens/self returned non-dict: %r", type(token_data))
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Unexpected token response shape."},
        )

    scopes = _parse_scopes(token_data.get("scope"))
    if _DESTRUCTIVE_SCOPE not in scopes:
        logger.warning(
            "[fastly-auth] token (id=%s, user=%s) missing 'global' scope; got=%r",
            token_data.get("id"),
            token_data.get("user_id") or "(automation)",
            scopes,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "insufficient_scope",
                "message": (
                    "Token does not have the 'global' scope required for destructive operations. "
                    "Use a Fastly token with full 'global' permissions, not 'global:read', "
                    "'purge_select', or 'purge_all'."
                ),
            },
        )

    bound_services = token_data.get("services")
    if isinstance(bound_services, list) and bound_services:
        if service_id not in bound_services:
            logger.warning(
                "[fastly-auth] token (id=%s) bound to %d services but not target service_id=%s",
                token_data.get("id"),
                len(bound_services),
                service_id,
            )
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "service_not_authorized",
                    "message": (
                        "Token is bound to a service list that does not include the target service. "
                        "Use a token with global access or with this service in its allow-list."
                    ),
                },
            )

    token_customer = (token_data.get("customer_id") or "").strip()
    try:
        service_data = fastly("GET", f"/service/{service_id}", token=token)
    except Exception as e:
        logger.warning(
            "[fastly-auth] /service/%s call failed during tenant verification: %s",
            service_id,
            e,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_verification_failed",
                "message": "Could not verify token tenant against target service.",
            },
        )

    service_customer = ""
    if isinstance(service_data, dict):
        service_customer = (service_data.get("customer_id") or "").strip()

    if not token_customer or not service_customer or token_customer != service_customer:
        logger.warning(
            "[fastly-auth] tenant mismatch: token customer=%r vs service customer=%r (token id=%s, service=%s)",
            token_customer or "(missing)",
            service_customer or "(missing)",
            token_data.get("id"),
            service_id,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_mismatch",
                "message": (
                    "The supplied token is not authorized for the target service's tenant. "
                    "Use a token issued under the same Fastly account that owns this service."
                ),
            },
        )

    logger.info(
        "[fastly-auth] destructive op authorized: token id=%s user=%s service=%s",
        token_data.get("id"),
        token_data.get("user_id") or "(automation)",
        service_id,
    )
    return token_data


_self_mod = _sys.modules[__name__]
for _alias_mod in (
    "backend.core._log_fields_data",
    "backend.core.field_registry",
    "backend.core.metric_expressions",
    "backend.utils.vcl_utils",
    "backend.utils.vcl_validator",
    "backend.utils.fastly_auth",
    "backend.utils.vcl_safety",
):
    _sys.modules[_alias_mod] = _self_mod
    if "backend" in _sys.modules and hasattr(_sys.modules["backend"], "_MODULE_ALIASES"):
        _sys.modules["backend"]._MODULE_ALIASES[_alias_mod] = __name__
if "backend.core" in _sys.modules:
    for _attr in ("_log_fields_data", "field_registry", "metric_expressions"):
        setattr(_sys.modules["backend.core"], _attr, _self_mod)
if "backend.utils" in _sys.modules:
    for _attr in ("vcl_utils", "vcl_validator", "fastly_auth", "vcl_safety"):
        setattr(_sys.modules["backend.utils"], _attr, _self_mod)
