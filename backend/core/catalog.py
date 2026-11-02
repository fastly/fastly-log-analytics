"""Log field catalog, typed registry, field resolution, and custom-field validation.

Every loggable field is defined in ``LOG_FIELD_CATALOG`` below; ``REGISTRY``,
``BY_CODE``, and ``BY_GROUP`` are derived at import time and stay byte-for-byte
equivalent (guarded by ``tests/core/test_field_registry.py``).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from backend.core.presets import PRESETS

FASTLY_LOG_LINE_DELIVER_MAX = 16 * 1024  # 16 KiB hard cap, silent truncation
FASTLY_LOG_LINE_SAFE_MAX = int(FASTLY_LOG_LINE_DELIVER_MAX * 0.60)  # 9830 bytes
_NUMERIC_FIELD_MAX_LEN = 20

VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")

_DUCKDB_RESERVED = frozenset(
    "select from where table column index view join on as is in not null true false "
    "and or case when then else end order group by having limit offset union all "
    "distinct with insert update delete create drop alter add set values into exists "
    "between like ilike cast try_cast extract interval timestamp date time integer "
    "varchar boolean double bigint float struct list map dt timestamp_hour".split()
)

_DUCKDB_TYPE_VALUE_TYPE_COMPAT: dict[str, set[str]] = {
    "VARCHAR": {"string", "ip", "url"},
    "INTEGER": {"numeric"},
    "BIGINT": {"numeric"},
    "DOUBLE": {"numeric"},
    "BOOLEAN": {"boolean"},
}

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


LOG_FIELD_CATALOG: list[dict[str, Any]] = [
    _f('timestamp', None, 'Timestamp', 'UTC timestamp of the request start time (ISO 8601 with timezone).', '"timestamp":"%{strftime(\\{"%Y-%m-%dT%H:%M:%S%z"\\},time.start)}V"', 'TIMESTAMP', 40),
    _f('ip', None, 'Client IP', 'Client IP address. Captured at the real edge via x-fos-edge-data header; falls back to client.ip for requests answered before capture ran (e.g. redirects).', '"ip":"%{json.escape(if(req.http.x-fos-edge-data:ip != "", req.http.x-fos-edge-data:ip, if(req.http.Fastly-Client-IP != "", req.http.Fastly-Client-IP, client.ip)))}V"', 'VARCHAR', 22, ['low_and_slow', 'credential_enumeration', 'content_discovery', 'botnet_grouping']),
    _f('status', None, 'Response Status', 'HTTP response status code (e.g. 200, 404, 503).', '"status":%{if(resp.status > 0, "" + resp.status, "null")}V', 'USMALLINT', 17, ['error_spikes', 'city_error_spikes', 'credential_enumeration', 'content_discovery', 'waf_signal_spikes', 'image_optimization_opportunities'], formatter='status'),
    _f('elapsed', None, 'Elapsed Time (µs)', 'Total request processing time in microseconds.', '"elapsed":%{if(time.elapsed.usec != "", time.elapsed.usec, "null")}V', 'UBIGINT', 18, ['latency_regression', 'city_latency_regressions', 'network_asn_health', 'tail_latency', 'region_latency'], formatter='number', unit='µs'),
    _f('cache', None, 'Cache State', 'Fastly cache state: HIT, MISS, PASS, SYNTH, etc.', '"cache":"%{json.escape(fastly_info.state)}V"', 'VARCHAR', 18, ['cache_collapse', 'cache_hit_cliff', 'cache_pressure']),
    _f('resp_bytes', None, 'Response Bytes', 'Bytes delivered to the client in the response body.', '"resp_bytes":%{if(resp.bytes_written > 0, "" + resp.bytes_written, "0")}V', 'UBIGINT', 18, ['cache_pressure', 'network_asn_health', 'image_optimization_opportunities'], formatter='bytes'),
    _f('host', 'A', 'Host', 'HTTP Host header (domain name) captured at the true client edge before any rewrites.', '"host":"%{json.escape(substr(if(req.http.x-fos-edge-data:host != "", req.http.x-fos-edge-data:host, req.http.Host), 0, 512))}V"', 'VARCHAR', 22, ['new_probe_urls']),
    _f('url', 'A', 'URL', 'Request URL path and query string. Average ~30 bytes; varies widely.', '"url":"%{json.escape(substr(req.url, 0, 2000))}V"', 'VARCHAR', 37, ['error_spikes', 'latency_regression', 'new_probe_urls', 'low_and_slow', 'credential_enumeration', 'content_discovery', 'tail_latency', 'image_optimization_opportunities']),
    _f('method', 'A', 'HTTP Method', 'Request method: GET, POST, HEAD, PUT, DELETE, etc.', '"method":"%{json.escape(substr(req.method, 0, 128))}V"', 'VARCHAR', 19, ['method_drift']),
    _f('proto', 'A', 'HTTP Version', 'HTTP protocol version: 1.0, 1.1, 2.0, or 3.0.', '"proto":"%{if(req.proto != "", regsub(req.proto, "^HTTP/", ""), "")}V"', 'VARCHAR', 15, formatter='number', precision=1),
    _f('ua', 'A', 'User-Agent', 'Client browser or bot identifier. Largest single field — bots inflate this significantly.', '"ua":"%{json.escape(substr(if(req.http.x-fos-edge-data:ua != "", req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, 1000))}V"', 'VARCHAR', 90, ['ua_monoculture', 'botnet_grouping', 'image_optimization_opportunities'], note='Largest single field — bots tend to have verbose user-agents.', individually_toggleable=True),
    _f('referer', 'A', 'Referer', 'Referring URL. Often empty; useful for traffic source analysis.', '"referer":"%{json.escape(substr(if(req.http.x-fos-edge-data:referer != "", req.http.x-fos-edge-data:referer, req.http.Referer), 0, 1000))}V"', 'VARCHAR', 44, ['referer_monoculture'], individually_toggleable=True),
    _f('req_bytes', 'A', 'Request Body Size', 'Request body size in bytes from Content-Length header. Zero for GET/HEAD or any request without Content-Length.', '"req_bytes":%{if(req.http.Content-Length ~ "^[0-9]+$", req.http.Content-Length, "0")}V', 'UBIGINT', 13, ['request_size_anomaly'], formatter='bytes'),
    _f('req_header_bytes', 'A', 'Request Header Size', 'Total bytes in the request headers. Large values are an injection or WAF bypass signal.', '"req_header_bytes":%{if(req.header_bytes_read > 0, "" + req.header_bytes_read, "0")}V', 'UINTEGER', 20, ['request_size_anomaly'], formatter='bytes'),
    _f('resp_header_content_encoding', 'A', 'Response Content-Encoding', 'Content-Encoding of the response served to the client (gzip / br / zstd, or empty when the response was sent uncompressed). Low-cardinality. Read directly from the final response at log time — no edge capture needed.', '"resp_header_content_encoding":"%{json.escape(resp.http.Content-Encoding)}V"', 'VARCHAR', 38, ['payload_compression_regression'], individually_toggleable=True),
    _f('ttl', 'B', 'Object TTL', 'Time-to-live assigned by origin headers. Null when object is not cacheable.', '"ttl":%{if(obj.ttl > 0s, regsub("" + obj.ttl, "(\\.[0-9]+)?s$", ""), "null")}V', 'FLOAT', 18, ['cache_pressure', 'cache_ttl_mismatch'], formatter='number', precision=0, unit='s'),
    _f('age', 'B', 'Object Age', 'How long the object has been in the Fastly cache (seconds).', '"age":%{if(obj.age > 0s, regsub("" + obj.age, "(\\.[0-9]+)?s$", ""), "null")}V', 'FLOAT', 17, ['cache_pressure', 'cache_ttl_mismatch'], formatter='number', precision=0, unit='s'),
    _f('hits', 'B', 'Object Hit Count', 'Number of times this cached object has been served.', '"hits":%{if(obj.hits > 0, "" + obj.hits, "null")}V', 'UINTEGER', 14, ['cache_ttl_mismatch']),
    _f('digest', 'B', 'Content Digest', 'Content hash for exact object identity. Required for Cache Pressure Analysis.', '"digest":"%{req.digest}V"', 'VARCHAR', 47, ['cache_pressure', 'pop_latency_regression'], note='Required for Cache Pressure Analysis (eviction detection).'),
    _f('pop', 'C', 'Edge PoP', 'Fastly Point of Presence code (e.g. JFK, LHR, SYD).', '"pop":"%{server.datacenter}V"', 'VARCHAR', 18, ['cache_pressure'], formatter='pop'),
    _f('backend', 'C', 'Backend', 'Origin backend name as configured in Fastly.', '"backend":"%{json.escape(req.backend)}V"', 'VARCHAR', 21),
    _f('edge', 'C', 'Edge Hit', 'True when the request hit the real edge (not a shield or restart).', '"edge":%{if(fastly.ff.visits_this_service == 0, "1", "0")}V', 'BOOLEAN', 9),
    _f('ttfb', 'C', 'Time to First Byte (s)', 'Seconds from request receipt to first byte of response from origin. Subtract from elapsed to isolate Fastly processing time.', '"ttfb":%{if(time.to_first_byte > 0s, regsub("" + time.to_first_byte, "s$", ""), "null")}V', 'FLOAT', 14, ['region_latency'], formatter='number', precision=3, unit='s'),
    _f('server_region', 'C', 'Server Region', 'Fastly billing region of the serving PoP (e.g. NA, EU, APAC). Captured at edge for accurate attribution through shields.', '"server_region":"%{json.escape(if(req.http.x-fos-edge-data:srv_region != "", req.http.x-fos-edge-data:srv_region, server.region))}V"', 'VARCHAR', 20, ['region_latency']),
    _f('is_ipv6', 'C', 'IPv6', 'True when the client connected over IPv6. IPv6 clients can have different routing and latency profiles.', '"is_ipv6":%{if(req.http.x-fos-edge-data:is_ipv6 ~ "^[0-9]+$", req.http.x-fos-edge-data:is_ipv6, if(req.is_ipv6, "1", "0"))}V', 'BOOLEAN', 12),
    _f('conn_requests', 'C', 'Conn. Request Count', 'Number of requests made on this TCP/QUIC connection. High values indicate HTTP/2 keep-alive multiplexing.', '"conn_requests":%{if(req.http.x-fos-edge-data:conn_reqs ~ "^[0-9]+$", req.http.x-fos-edge-data:conn_reqs, if(client.requests > 0, "" + client.requests, "null"))}V', 'USMALLINT', 20, ['connection_abuse']),
    _f('tls', 'C', 'TLS Version', 'TLS protocol version as a float: 1.2 or 1.3.', '"tls":"%{json.escape(if(req.http.x-fos-edge-data:tls != "", req.http.x-fos-edge-data:tls, if(tls.client.protocol != "", regsub(tls.client.protocol, "^TLSv", ""), "")))}V"', 'VARCHAR', 10, formatter='number', precision=1),
    _f('country', 'D', 'Country', 'ISO 3166-1 alpha-2 country code (e.g. US, DE, JP). Enables world map.', '"country":"%{json.escape(if(req.http.x-fos-edge-data:country != "", req.http.x-fos-edge-data:country, client.geo.country_code))}V"', 'VARCHAR', 15, ['new_country_traffic', 'city_surges', 'city_error_spikes', 'city_latency_regressions', 'new_city_traffic'], formatter='country', individually_toggleable=True),
    _f('city', 'D', 'City', 'City name from Fastly geo-IP. Variable length.', '"city":"%{json.escape(if(req.http.x-fos-edge-data:city != "", req.http.x-fos-edge-data:city, client.geo.city))}V"', 'VARCHAR', 18, ['city_surges', 'city_error_spikes', 'city_latency_regressions', 'new_city_traffic'], formatter='city', individually_toggleable=True),
    _f('region', 'D', 'Region', 'ISO 3166-2 region/state/province code.', '"region":"%{json.escape(if(req.http.x-fos-edge-data:region != "", req.http.x-fos-edge-data:region, if(client.geo.region == "?", "", client.geo.region)))}V"', 'VARCHAR', 14, formatter='region', individually_toggleable=True),
    _f('lat', 'E', 'Latitude', 'Client latitude (-90 to 90). Null for unresolvable IPs.', '"lat":%{if(req.http.x-fos-edge-data:lat ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lat, if(client.geo.country_code != "?", "" + client.geo.latitude, "null"))}V', 'FLOAT', 12, ['network_asn_health'], formatter='number', precision=4),
    _f('lon', 'E', 'Longitude', 'Client longitude (-180 to 180). Null for unresolvable IPs.', '"lon":%{if(req.http.x-fos-edge-data:lon ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:lon, if(client.geo.country_code != "?", "" + client.geo.longitude, "null"))}V', 'FLOAT', 13, ['network_asn_health'], formatter='number', precision=4),
    _f('metro', 'E', 'Metro Code', 'US DMA metro area code (e.g. 501 = New York City). Empty for non-US.', '"metro":%{if(req.http.x-fos-edge-data:metro ~ "^[0-9]+$", req.http.x-fos-edge-data:metro, if(client.geo.metro_code > 0, "" + client.geo.metro_code, "null"))}V', 'USMALLINT', 14, ['metro_delivery_degradation']),
    _f('asn', 'F', 'ASN', 'Client Autonomous System Number (ISP identity). Enables ASN-level analysis.', '"asn":%{if(req.http.x-fos-edge-data:asn ~ "^[0-9]+$", req.http.x-fos-edge-data:asn, if(client.as.number > 0, "" + client.as.number, "null"))}V', 'UINTEGER', 11, ['asn_concentration', 'asn_hosting_shift', 'new_asn_traffic', 'network_asn_health', 'region_latency']),
    _f('tcp_rtt', 'F', 'TCP RTT (µs)', 'TCP round-trip time in microseconds at the Fastly edge.', '"tcp_rtt":%{if(req.http.x-fos-edge-data:rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt, if(client.socket.tcpi_rtt > 0, "" + client.socket.tcpi_rtt, "null"))}V', 'UINTEGER', 19, ['network_asn_health'], formatter='number', unit='µs'),
    _f('transport', 'F', 'Transport Protocol', "Transport protocol: 'tcp' or 'quic'. Low-cardinality; essentially free in Parquet.", '"transport":"%{json.escape(if(req.http.x-fos-edge-data:transport != "", req.http.x-fos-edge-data:transport, transport.type))}V"', 'VARCHAR', 18, ['network_asn_health', 'http3_fallback']),
    _f('ploss', 'G', 'Packet Loss', 'Packet loss fraction (0.0–1.0). Direct indicator of network congestion.', '"ploss":%{if(req.http.x-fos-edge-data:ploss ~ "^-?[0-9]+(\\.[0-9]+)?$", req.http.x-fos-edge-data:ploss, if(client.socket.ploss > 0, "" + client.socket.ploss, "null"))}V', 'FLOAT', 18, ['network_asn_health'], formatter='percent', precision=4),
    _f('rtt_min', 'G', 'Minimum RTT (µs)', 'Minimum RTT seen on this TCP connection (geography baseline). Delta from tcp_rtt isolates congestion.', '"rtt_min":%{if(req.http.x-fos-edge-data:rtt_min ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_min, if(client.socket.tcpi_min_rtt > 0, "" + client.socket.tcpi_min_rtt, "null"))}V', 'UINTEGER', 19, ['network_asn_health'], formatter='number', unit='µs'),
    _f('rtt_var', 'G', 'RTT Variance / Jitter (µs)', 'RTT variance in microseconds. Jitter causes streaming buffer stalls more than raw latency.', '"rtt_var":%{if(req.http.x-fos-edge-data:rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:rtt_var, if(client.socket.tcpi_rttvar > 0, "" + client.socket.tcpi_rttvar, "null"))}V', 'UINTEGER', 18, ['network_asn_health'], formatter='number', unit='µs'),
    _f('retrans', 'G', 'TCP Retransmissions', 'TCP retransmission delta since previous sample. Direct congestion signal.', '"retrans":%{if(req.http.x-fos-edge-data:retrans ~ "^[0-9]+$", req.http.x-fos-edge-data:retrans, if(client.socket.tcpi_delta_retrans > 0, "" + client.socket.tcpi_delta_retrans, "null"))}V', 'UTINYINT', 15, ['network_asn_health'], formatter='number'),
    _f('bw', 'K', 'Bandwidth Estimate', "Fastly's estimated bandwidth for this connection (bytes/sec or bits/sec — see note). Only applicable for QUIC; TCP connections should use delivery_rate instead.", '"bw":%{if(req.http.x-fos-edge-data:bw ~ "^[0-9]+$", req.http.x-fos-edge-data:bw, if(transport.bw_estimate > 0, "" + transport.bw_estimate, "null"))}V', 'UBIGINT', 17, formatter='bytes'),
    _f('c_speed', 'G', 'Connection Speed Class', 'Geo-IP speed classification: broadband, cable, dsl, mobile, satellite, dialup. Low-cardinality.', '"c_speed":"%{json.escape(if(req.http.x-fos-edge-data:c_speed != "", req.http.x-fos-edge-data:c_speed, if(client.geo.conn_speed == "?", "", client.geo.conn_speed)))}V"', 'VARCHAR', 14, ['network_asn_health', 'connection_type_mix']),
    _f('c_type', 'G', 'Connection Type', 'Geo-IP connection type: residential, commercial, cellular, corporate. Low-cardinality.', '"c_type":"%{json.escape(if(req.http.x-fos-edge-data:c_type != "", req.http.x-fos-edge-data:c_type, if(client.geo.conn_type == "?", "", client.geo.conn_type)))}V"', 'VARCHAR', 27, ['network_asn_health', 'connection_type_mix']),
    _f('delivery_rate', 'G', 'TCP Delivery Rate', 'Actual TCP delivery rate in bytes/sec measured by the kernel. More reliable than bandwidth estimate for TCP connections.', '"delivery_rate":%{if(req.http.x-fos-edge-data:del_rate ~ "^[0-9]+$", req.http.x-fos-edge-data:del_rate, if(client.socket.tcpi_delivery_rate > 0, "" + client.socket.tcpi_delivery_rate, "null"))}V', 'UBIGINT', 22, ['network_asn_health', 'metro_delivery_degradation'], formatter='bytes'),
    _f('data_segs_out', 'G', 'TCP Data Segments Out', 'Total TCP data segments sent on this connection. Enables retransmit ratio: retrans / data_segs_out.', '"data_segs_out":%{if(req.http.x-fos-edge-data:data_segs ~ "^[0-9]+$", req.http.x-fos-edge-data:data_segs, if(client.socket.tcpi_data_segs_out > 0, "" + client.socket.tcpi_data_segs_out, "null"))}V', 'UINTEGER', 21, ['network_asn_health'], formatter='number'),
    _f('ja3', 'H', 'JA3 Fingerprint', 'MD5 TLS client fingerprint. Older standard; widely supported. 41 bytes avg.', '"ja3":"%{json.escape(if(req.http.x-fos-edge-data:ja3 != "", req.http.x-fos-edge-data:ja3, tls.client.ja3_md5))}V"', 'VARCHAR', 41, ['botnet_grouping'], individually_toggleable=True),
    _f('ja4', 'H', 'JA4 Fingerprint', 'Newer, richer TLS fingerprint standard. 43 bytes avg.', '"ja4":"%{json.escape(if(req.http.x-fos-edge-data:ja4 != "", req.http.x-fos-edge-data:ja4, tls.client.ja4))}V"', 'VARCHAR', 43, ['botnet_grouping'], individually_toggleable=True),
    _f('tls_ciphers_sha', 'H', 'TLS Cipher Suite SHA', "SHA fingerprint of the client's offered cipher suite list. Evasion-resistant complement to JA3/JA4 for bot farm detection.", '"tls_ciphers_sha":"%{json.escape(if(req.http.x-fos-edge-data:tls_csha != "", req.http.x-fos-edge-data:tls_csha, tls.client.ciphers_list_sha))}V"', 'VARCHAR', 48, ['cipher_spread'], individually_toggleable=True),
    _f('cookie_session', 'H', 'Session Cookie (hashed)', "SHA-256 hash of the client's session cookie — the FIRST present among a common framework allowlist (sessionid, PHPSESSID, JSESSIONID, ASP.NET_SessionId, connect.sid, session, sid). Hashed AT THE EDGE via digest.hash_sha256, so the raw cookie value is never logged or forwarded past Fastly. Empty when no session cookie is present. PRIVACY: this is a stable per-session client identifier — it MUST be masked for analysts exactly like the client IP (see mask_ips policy).", '"cookie_session":"%{json.escape(req.http.x-fos-edge-data:cookie_session)}V"', 'VARCHAR', 40, ['session_harvesting'], individually_toggleable=True, pii=True, default_off=True),
    _f('p_type', 'I', 'Proxy Type', 'Anonymizing proxy type: VPN, Tor, DCH (data center), etc.', '"p_type":"%{json.escape(if(req.http.x-fos-edge-data:p_type != "", req.http.x-fos-edge-data:p_type, if(client.geo.proxy_type == "?", "", client.geo.proxy_type)))}V"', 'VARCHAR', 10, ['asn_hosting_shift', 'proxy_surge']),
    _f('p_desc', 'I', 'Proxy Description', 'Anonymizing proxy provider name.', '"p_desc":"%{json.escape(if(req.http.x-fos-edge-data:p_desc != "", req.http.x-fos-edge-data:p_desc, if(client.geo.proxy_description == "?", "", client.geo.proxy_description)))}V"', 'VARCHAR', 10, ['proxy_surge']),
    _f('waf', 'J', 'WAF Executed', 'Whether NGWAF (Signal Sciences) processed this request.', '"waf":%{if(waf.executed, "1", "0")}V', 'BOOLEAN', 8, ['waf_signal_spikes']),
    _f('waf_resp', 'J', 'WAF Agent Response', 'NGWAF agent decision code (HTTP status equivalent).', '"waf_resp":%{if(waf.executed, if(req.http.x-sigsci-agentresponse ~ "^[0-9]+$", req.http.x-sigsci-agentresponse, "null"), "null")}V', 'USMALLINT', 16, ['waf_signal_spikes'], formatter='status'),
    _f('waf_ms', 'J', 'WAF Latency (ms)', 'Milliseconds the NGWAF inspection added to the request.', '"waf_ms":%{if(waf.executed, if(req.http.x-sigsci-decision-ms ~ "^[0-9]+$", req.http.x-sigsci-decision-ms, "null"), "null")}V', 'UINTEGER', 13, formatter='number', unit='ms'),
    _f('waf_sig', 'J', 'WAF Signal Tags', 'NGWAF signal tags (e.g. SQLI, XSS, CMDEXE).', '"waf_sig":"%{if(waf.executed, if(req.http.x-sigsci-tags != "", json.escape(req.http.x-sigsci-tags), ""), "")}V"', 'VARCHAR', 13, ['waf_signal_spikes']),
    _f('waf_req_id', 'J', 'WAF Request ID', 'NGWAF request correlation ID for cross-referencing with Signal Sciences.', '"waf_req_id":"%{if(waf.executed, if(req.http.x-fastly-ngwaf:requestid != "", json.escape(req.http.x-fastly-ngwaf:requestid), if(req.http.x-sigsci-requestid != "", json.escape(req.http.x-sigsci-requestid), "")), "")}V"', 'VARCHAR', 16),
    _f('q_rtt', 'K', 'QUIC Smoothed RTT (µs)', 'QUIC smoothed RTT in microseconds. Null for TCP connections.', '"q_rtt":%{if(req.http.x-fos-edge-data:q_rtt ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt, if(transport.type == "quic", "" + quic.rtt.smoothed, "null"))}V', 'UINTEGER', 19, formatter='number', unit='µs'),
    _f('q_rtt_var', 'K', 'QUIC RTT Variance (µs)', 'QUIC RTT variance in microseconds. Null for TCP connections.', '"q_rtt_var":%{if(req.http.x-fos-edge-data:q_rtt_var ~ "^[0-9]+$", req.http.x-fos-edge-data:q_rtt_var, if(transport.type == "quic", "" + quic.rtt.variance, "null"))}V', 'UINTEGER', 19, formatter='number', unit='µs'),
    _f('q_lost', 'K', 'QUIC Packets Lost', 'QUIC packets lost counter. Null for TCP connections.', '"q_lost":%{if(req.http.x-fos-edge-data:q_lost ~ "^[0-9]+$", req.http.x-fos-edge-data:q_lost, if(transport.type == "quic", "" + quic.num_packets.lost, "null"))}V', 'UINTEGER', 17, formatter='number'),
    _f('q_cwnd', 'K', 'QUIC Congestion Window', 'QUIC congestion window size. Null for TCP connections.', '"q_cwnd":%{if(req.http.x-fos-edge-data:q_cwnd ~ "^[0-9]+$", req.http.x-fos-edge-data:q_cwnd, if(transport.type == "quic", "" + quic.cc.cwnd, "null"))}V', 'UINTEGER', 16, formatter='number'),
    _f('ottfb', 'L', 'Origin TTFB (µs)', 'µs from fetch start to first byte of origin/shield response headers. Null on HITs.', '"ottfb":%{if(req.http.x-of-ttfb ~ "^[0-9]+$", req.http.x-of-ttfb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike', 'region_latency'], formatter='number', unit='µs'),
    _f('ottlb', 'L', 'Origin TTLB (µs)', 'µs from fetch start to full response body received. Null on HITs.', '"ottlb":%{if(req.http.x-of-ttlb ~ "^[0-9]+$", req.http.x-of-ttlb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike'], formatter='number', unit='µs'),
    _f('oconnect_ms', 'L', 'Origin Connect (ms)', 'Milliseconds to establish the connection (TCP + TLS handshake) to origin/shield, from beresp.handshake_time_to_origin_ms. Distinct from ottfb (first byte). Null on HITs and on failed fetches that never connected — connection timeouts land in vcl_error, where beresp (and therefore the handshake timing) is unavailable.', '"oconnect_ms":%{if(req.http.x-of-connect ~ "^[0-9]+$", req.http.x-of-connect, "null")}V', 'UINTEGER', 16, ['timeout_split'], formatter='number', unit='ms'),
    _f('ost', 'L', 'Origin Status', 'HTTP status returned by origin or shield. Null on HITs.', '"ost":%{if(req.http.x-of-status ~ "^[0-9]+$", req.http.x-of-status, "null")}V', 'USMALLINT', 10, ['origin_error_rate', 'origin_ip_failure'], formatter='status'),
    _f('obytes', 'L', 'Origin Bytes', "Bytes written in the response (resp.bytes_written). Null on HITs. Same variable as resp_bytes but null-on-HIT makes it queryable as 'total bytes fetched from origin'.", '"obytes":%{if(req.http.x-of-start ~ "^[0-9]+$", "" + resp.bytes_written, "null")}V', 'UBIGINT', 15),
    _f('oip', 'L', 'Origin IP', 'IP address of the backend server that handled the fetch. Null on HITs.', '"oip":"%{json.escape(if(req.http.x-of-oip, req.http.x-of-oip, ""))}V"', 'VARCHAR', 15, ['origin_ip_failure']),
    _f('oretries', 'L', 'Origin Retries', 'Backend connection retry count before success or failure. Null on HITs.', '"oretries":%{if(req.http.x-of-oretries ~ "^[0-9]+$", req.http.x-of-oretries, "null")}V', 'UTINYINT', 13, ['origin_retries'], formatter='number'),
    _f('rid', 'L', 'Request ID', '8-char random ID generated at this POP. Always set. Use with prid to correlate edge + shield log lines.', '"rid":"%{req.http.x-req-id}V"', 'VARCHAR', 16),
    _f('prid', 'L', 'Parent Request ID', "Edge POP's rid forwarded to the shield. Non-null only on shield log lines (edge=0, cache=MISS).", '"prid":"%{json.escape(req.http.x-edge-req-id)}V"', 'VARCHAR', 16),
    _f('io_input_bytes', 'M', 'IO Input Bytes', 'Original image size in bytes before IO processing, from Fastly-Io-Transform-Stats ifsz field.', '"io_input_bytes":%{if(req.http.x-fos-io-ifsz ~ "^[0-9]+$", req.http.x-fos-io-ifsz, "null")}V', 'UBIGINT', 20, formatter='bytes'),
    _f('io_output_bytes', 'M', 'IO Output Bytes', 'Optimized image size in bytes after IO processing, from Fastly-Io-Transform-Stats ofsz field.', '"io_output_bytes":%{if(req.http.x-fos-io-ofsz ~ "^[0-9]+$", req.http.x-fos-io-ofsz, "null")}V', 'UBIGINT', 20, formatter='bytes'),
    _f('io_input_format', 'M', 'IO Input Format', 'Original image format before IO processing, from Fastly-Io-Transform-Stats ifmt field.', '"io_input_format":"%{json.escape(if(req.http.x-fos-io-ifmt != "", req.http.x-fos-io-ifmt, ""))}V"', 'VARCHAR', 20),
    _f('io_output_format', 'M', 'IO Output Format', 'Output image format after IO processing, from Fastly-Io-Transform-Stats ofmt field.', '"io_output_format":"%{json.escape(if(req.http.x-fos-io-ofmt != "", req.http.x-fos-io-ofmt, ""))}V"', 'VARCHAR', 20),
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
    _v("_bot_name", "Fastly Bots", "Virtual field derived from User-Agent and IP to identify known bots."),
    _v("_ngwaf_bot_name", "NGWAF Verified Bots", "Virtual field enriched with NGWAF bot signal data."),
    _v("waf_sig_ind", "NGWAF Signals", "Individual NGWAF signals extracted from the waf_sig list."),
    _v('edge_score_reason_ind', 'Score Reasons', "Individual scoring reasons extracted from the comma-separated edge_score_reason field (e.g. 'cookie-missing', 'impossibly-fast', 'robotic-consistency', 'rare-transition'). Lets the dashboard show top-N reason breakdowns and filter by a single reason even when one request triggers multiple."),
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

# fmt: on

# Opt-in field IDs: excluded from resolve_enabled_fields unless explicitly opted in via field_overrides.
DEFAULT_OFF_FIELD_IDS: frozenset[str] = frozenset(f["id"] for f in LOG_FIELD_CATALOG if f.get("default_off"))


def resolve_enabled_fields(cfg: dict) -> set:
    """Expand group selections and per-field overrides into a flat set of enabled field IDs."""
    if cfg is None:
        cfg = {"groups": PRESETS["standard"]["groups"], "field_overrides": {}}
    enabled = {f["id"] for f in LOG_FIELD_CATALOG if f["group"] is None}
    enabled_groups = set(cfg.get("groups", []))
    changed = True
    while changed:
        changed = False
        for grp, required in GROUP_DEPENDENCIES.items():
            if grp in enabled_groups and required not in enabled_groups:
                enabled_groups.add(required)
                changed = True
    for field in LOG_FIELD_CATALOG:
        if field["group"] in enabled_groups and field["id"] not in DEFAULT_OFF_FIELD_IDS:
            enabled.add(field["id"])
    for field_id, on in cfg.get("field_overrides", {}).items():
        if on:
            enabled.add(field_id)
        else:
            enabled.discard(field_id)
    return enabled


def get_required_edge_headers(log_fields_config: dict) -> set:
    """Return the set of x-fos-edge-data keys required by the enabled log fields."""
    enabled = resolve_enabled_fields(log_fields_config)
    pattern = re.compile(r"req\.http\.x-fos-edge-data:([a-z0-9_]+)")
    required: set[str] = set()
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            required.update(pattern.findall(field["vcl"]))
    return required


def estimate_log_line_bytes(log_fields_config: dict) -> int:
    """Return the estimated average uncompressed log line size in bytes."""
    enabled = resolve_enabled_fields(log_fields_config)
    field_bytes = sum(f["typical_bytes"] for f in LOG_FIELD_CATALOG if f["id"] in enabled)
    custom_enabled = [cf for cf in log_fields_config.get("custom_fields", []) if cf.get("enabled", True)]
    field_bytes += sum(cf.get("bytes_estimate", 0) for cf in custom_enabled)
    return 2 + (len(enabled) + len(custom_enabled)) * 5 + field_bytes


def check_log_line_budget(log_fields_config: dict) -> dict | None:
    """Return a warning dict when estimated log line approaches the Fastly 16 KiB cap, else None."""
    estimate = estimate_log_line_bytes(log_fields_config)
    base = {
        "estimate_bytes": estimate,
        "deliver_max_bytes": FASTLY_LOG_LINE_DELIVER_MAX,
        "safe_max_bytes": FASTLY_LOG_LINE_SAFE_MAX,
    }
    if estimate >= FASTLY_LOG_LINE_DELIVER_MAX:
        return {
            "code": "LOG_LINE_TOO_LARGE",
            "severity": "error",
            **base,
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
            **base,
            "message": (
                f"Estimated log line is {estimate} bytes; Fastly silently truncates "
                f"at {FASTLY_LOG_LINE_DELIVER_MAX} bytes (16 KiB). Per-request "
                "values (long URLs, fat headers) can push real lines past the cap. "
                "Consider trimming optional fields."
            ),
        }
    return None


_VARIABLE_FIELD_DEFAULTS = {"url": 2000, "ua": 1000, "referer": 1000}


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
        for opt_flag in ("individually_toggleable", "default_off"):
            if f.get(opt_flag):
                entry[opt_flag] = True
        if f.get("note"):
            entry["note"] = f["note"]
        if f["id"] in _VARIABLE_FIELD_DEFAULTS:
            entry["limit"] = limits.get(f["id"], _VARIABLE_FIELD_DEFAULTS[f["id"]])
            entry["has_limit"] = True
        result.append(entry)
    return result


def get_groups_for_api() -> list:
    """Return group metadata suitable for the /api/log-fields/catalog endpoint."""
    ordered_groups = [None, "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "METRICS"]
    result = []
    for gid in ordered_groups:
        info = GROUP_INFO[gid]
        fields = [f for f in LOG_FIELD_CATALOG if f["group"] == gid]
        result.append(
            {
                "id": gid,
                "label": info["label"],
                "description": info["description"],
                "locked": info.get("locked", False),
                "requires": info.get("requires"),
                "note": info.get("note"),
                "recommended_with": info.get("recommended_with"),
                "total_bytes": sum(f["typical_bytes"] for f in fields),
                "fields": [f["id"] for f in fields],
            }
        )
    return result


_BUILTIN_FIELD_NAMES = frozenset(f["id"] for f in LOG_FIELD_CATALOG)


def validate_custom_field(field: dict, existing_names: list[str]) -> list[str]:
    """Validate a custom field definition dict; returns list of error/WARN strings."""
    errors: list[str] = []
    name = field.get("name", "")
    for key in ("name", "label", "vcl_log_expression", "duckdb_type", "value_type", "bytes_estimate"):
        if key not in field or field[key] is None:
            errors.append(f"Missing required field: '{key}'")
    if errors:
        return errors

    if not VALID_NAME_RE.fullmatch(name):
        errors.append("Field name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars")
    if name in _DUCKDB_RESERVED:
        errors.append(f"'{name}' is a DuckDB/SQL reserved word and cannot be used as a field name")
    if name in _BUILTIN_FIELD_NAMES:
        errors.append(f"'{name}' is already a built-in field name")
    if name in existing_names:
        errors.append(f"A custom field named '{name}' already exists on this service")
    if not (1 <= len(field.get("label", "")) <= 80):
        errors.append("'label' must be 1–80 characters")
    if len(field.get("description", "")) > 500:
        errors.append("'description' must not exceed 500 characters")

    expr = field.get("vcl_log_expression", "")
    if not expr.strip():
        errors.append("'vcl_log_expression' must not be empty")
    elif len(expr) > 512:
        errors.append(f"'vcl_log_expression' must be ≤ 512 characters (got {len(expr)})")
    elif "\n" in expr:
        errors.append("'vcl_log_expression' must not contain raw newlines")
    else:
        if ";" in expr:
            errors.append("'vcl_log_expression' must not contain semicolons (;)")
        if "//" in expr or "/*" in expr or "#" in expr:
            errors.append("'vcl_log_expression' must not contain VCL comments (//, /*, or #)")

    valid_types = set(_DUCKDB_TYPE_VALUE_TYPE_COMPAT)
    duckdb_type = field.get("duckdb_type", "")
    if duckdb_type not in valid_types:
        errors.append(f"'duckdb_type' must be one of: {', '.join(sorted(valid_types))}")

    all_value_types = {"string", "numeric", "boolean", "ip", "url"}
    value_type = field.get("value_type", "")
    if value_type not in all_value_types:
        errors.append(f"'value_type' must be one of: {', '.join(sorted(all_value_types))}")

    if duckdb_type in _DUCKDB_TYPE_VALUE_TYPE_COMPAT and value_type in all_value_types:
        if value_type not in _DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]:
            compat = ", ".join(sorted(_DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]))
            errors.append(
                f"'value_type' '{value_type}' is not compatible with 'duckdb_type' '{duckdb_type}'. "
                f"Compatible value_types: {compat}"
            )

    bytes_est = field.get("bytes_estimate", 0)
    try:
        bytes_est = int(bytes_est)
        if not (1 <= bytes_est <= 1024):
            errors.append("'bytes_estimate' must be between 1 and 1024")
    except (TypeError, ValueError):
        errors.append("'bytes_estimate' must be an integer")

    stage = field.get("collection_stage", "edge")
    if stage not in ("edge", "origin"):
        errors.append(f"'collection_stage' must be 'edge' or 'origin' (got '{stage}')")

    if isinstance(bytes_est, int) and isinstance(name, str):
        min_bytes = len(name) + 5
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
    """Return the log_fields config from a service config dict, with a safe default."""
    return cfg.get("log_fields") or _DEFAULT_LF_CONFIG.copy()


class DuckType(StrEnum):
    """DuckDB column types in use across the field catalog."""

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
    """Field grouping (`CORE` replaces legacy `None` sentinel)."""

    CORE = "CORE"
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    I = "I"  # noqa: E741
    J = "J"
    K = "K"
    L = "L"
    M = "M"
    METRICS = "METRICS"
    VIRTUAL = "VIRTUAL"
    INTERNAL = "INTERNAL"

    @classmethod
    def from_legacy(cls, raw: str | None) -> Group:
        return cls.CORE if raw is None else cls(raw)


@dataclass(frozen=True, slots=True)
class LogField:
    """One row of the field catalog (frozen + slotted typed read view)."""

    code: str
    label: str
    group: Group
    duck_type: DuckType
    description: str = ""
    vcl: str | None = None
    typical_bytes: int = 0
    required_by: tuple[str, ...] = ()
    substr_cap: int | None = None
    individually_toggleable: bool = False
    default_off: bool = False
    formatter: str | None = None
    unit: str | None = None
    precision: int | None = None
    note: str | None = None

    @property
    def is_derived(self) -> bool:
        return self.vcl is None


_SUBSTR_RE = re.compile(r"substr\([^,]+,\s*0,\s*(\d+)\)")


def _detect_substr_cap(vcl: str | None) -> int | None:
    if vcl is None or not (m := _SUBSTR_RE.search(vcl)):
        return None
    try:
        return int(m.group(1))
    except ValueError:  # pragma: no cover
        return None


def _opt_str(v: object) -> str | None:
    return None if v is None else str(v)


def _opt_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):  # pragma: no cover
        return None


def _field_from_dict(d: Mapping[str, Any]) -> LogField:
    code = str(d["id"])
    raw_group = d.get("group")
    vcl_raw = d.get("vcl")
    vcl: str | None = None if vcl_raw is None else str(vcl_raw)
    return LogField(
        code=code,
        label=str(d.get("label", code)),
        group=Group.from_legacy(raw_group if raw_group is None else str(raw_group)),
        duck_type=DuckType(str(d["duckdb_type"])),
        description=str(d.get("description", "")),
        vcl=vcl,
        typical_bytes=int(d.get("typical_bytes", 0) or 0),
        required_by=tuple(str(x) for x in (d.get("required_by") or ())),
        substr_cap=_detect_substr_cap(vcl),
        individually_toggleable=bool(d.get("individually_toggleable", False)),
        default_off=bool(d.get("default_off", False)),
        formatter=_opt_str(d.get("formatter")),
        unit=_opt_str(d.get("unit")),
        precision=_opt_int(d.get("precision")),
        note=_opt_str(d.get("note")),
    )


_CATALOG = LOG_FIELD_CATALOG
REGISTRY: tuple[LogField, ...] = tuple(_field_from_dict(entry) for entry in _CATALOG)
BY_CODE: Mapping[str, LogField] = MappingProxyType({f.code: f for f in REGISTRY})
if len(BY_CODE) != len(REGISTRY):  # pragma: no cover
    raise RuntimeError("duplicate field codes in REGISTRY — check log_fields.py for collisions")


def _group_index() -> Mapping[Group, tuple[LogField, ...]]:
    bucket: dict[Group, list[LogField]] = {g: [] for g in Group}
    for f in REGISTRY:
        bucket[f.group].append(f)
    return MappingProxyType({g: tuple(items) for g, items in bucket.items()})


BY_GROUP: Mapping[Group, tuple[LogField, ...]] = _group_index()


def get(code: str) -> LogField:
    return BY_CODE[code]


def try_get(code: str) -> LogField | None:
    return BY_CODE.get(code)


def in_group(group: Group) -> tuple[LogField, ...]:
    return BY_GROUP[group]


def derived() -> tuple[LogField, ...]:
    return tuple(f for f in REGISTRY if f.is_derived)


def _is_system_field(field_name: str) -> bool:
    return field_name.startswith("cmcd_") or field_name.startswith("edge_")


def _filter_user_custom_fields(custom_fields: list[dict]) -> list[dict]:
    return [f for f in custom_fields if not _is_system_field(f.get("name", ""))]


# ── Encode tables (human-readable → single-char code) ─────────────────────────
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
    """Return a VCL if/else expression that encodes known values to single chars."""
    expr = vcl_var
    for value, code in reversed(list(encode.items())):
        expr = f'if({vcl_var} == "{value}", "{code}", {expr})'
    expr = f'if({vcl_var} == "?", "", {expr})'
    return expr


def duckdb_decode_case(col: str, encode: dict[str, str]) -> str:
    """Return a SQL CASE expression that maps single-char codes back to full values."""
    decode = {code: value for value, code in encode.items()}
    clauses = []
    for code, value in decode.items():
        clauses.append(f"WHEN \"{col}\" = '{code}' THEN '{value}'")
    for value in encode:
        clauses.append(f"WHEN \"{col}\" = '{value}' THEN '{value}'")
    return f'CASE {" ".join(clauses)} ELSE "{col}" END'
