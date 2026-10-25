"""Module-private catalog/preset/insight data for log_fields.py.

Holds only DATA — no behaviour. Callers import these names through
``backend.core.log_fields``, which re-exports everything here.

Contents:
- LOG_FIELD_CATALOG: list of every Fastly log field with VCL emission
  template, DuckDB type, typical bytes, and group/dependency metadata.
  The dict-literal form is the chosen authoring format; the typed
  read view (``LogField`` tuple) lives in ``field_registry.py`` and
  is derived from this list at import time.
- GROUP_INFO: per-group labels, descriptions, and required-by-fields
  graph.
- GROUP_DEPENDENCIES: derived from GROUP_INFO above.
- PRESETS: bundle definitions surfaced in the log-fields admin UI.
- INSIGHT_DEFINITIONS: dashboard insight schemas keyed on field/group
  availability.
"""

from __future__ import annotations

from typing import Any

# fmt: off

# Heterogeneous-value dicts (str / bool / list / None / nested dict) — annotate
# as ``dict[str, Any]`` so downstream readers (field_registry._field_from_dict,
# group-dependency map at the bottom of this file) typecheck cleanly instead
# of mypy widening to ``object``.


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
    # Reads the edge-captured, already-hashed value out of the
    # x-fos-edge-data header (see fastly_api.EDGE_DATA_MAPPING /
    # _session_cookie_hash_vcl). json.escape is belt-and-suspenders — the
    # value is hex or "" — and marks the field as security-hooked.
    # Advisory: this is a client identifier. Downstream masking (insight
    # processors + the analyst log/query middleware) must treat it like ip.
    # Opt-in only. ``default_off`` fields are EXCLUDED from
    # ``resolve_enabled_fields`` even when their group is enabled — they
    # emit ONLY when explicitly turned on via ``field_overrides`` (admin
    # per-field toggle / provisioning ``--enable-field``). Capturing a
    # stable per-session client identifier must be a deliberate operator
    # choice, not a side effect of selecting the security/full preset.
    # (Distinct from ``pii``, which is purely a masking advisory; a field
    # can be one, both, or neither.)
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
    _f('ottfb', 'L', 'Origin TTFB (µs)', 'µs from fetch start to first byte of origin/shield response headers. Null on HITs.', '"ottfb":%{if(req.http.x-of-ttfb ~ "^[0-9]+$", req.http.x-of-ttfb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike', 'region_latency'], formatter='number', unit='µs'),
    _f('ottlb', 'L', 'Origin TTLB (µs)', 'µs from fetch start to full response body received. Null on HITs.', '"ottlb":%{if(req.http.x-of-ttlb ~ "^[0-9]+$", req.http.x-of-ttlb, "null")}V', 'UBIGINT', 16, ['origin_latency_spike'], formatter='number', unit='µs'),
    # Captured into req.http.x-of-connect in vcl_fetch (see
    # fastly_api.generate_capture_vcl). Digit-regex guard mirrors the other
    # group-L origin metrics: the header is scrubbed on inbound req, and the
    # guard keeps a spoofed non-numeric value out of the JSON log line.
    _f('oconnect_ms', 'L', 'Origin Connect (ms)', 'Milliseconds to establish the connection (TCP + TLS handshake) to origin/shield, from beresp.handshake_time_to_origin_ms. Distinct from ottfb (first byte). Null on HITs and on failed fetches that never connected — connection timeouts land in vcl_error, where beresp (and therefore the handshake timing) is unavailable.', '"oconnect_ms":%{if(req.http.x-of-connect ~ "^[0-9]+$", req.http.x-of-connect, "null")}V', 'UINTEGER', 16, ['timeout_split'], formatter='number', unit='ms'),
    _f('ost', 'L', 'Origin Status', 'HTTP status returned by origin or shield. Null on HITs.', '"ost":%{if(req.http.x-of-status ~ "^[0-9]+$", req.http.x-of-status, "null")}V', 'USMALLINT', 10, ['origin_error_rate', 'origin_ip_failure'], formatter='status'),
    # resp.bytes_written is a Fastly-internal counter (not from a header),
    # so no JSON-injection risk; the x-of-start guard is preserved as-is.
    _f('obytes', 'L', 'Origin Bytes', "Bytes written in the response (resp.bytes_written). Null on HITs. Same variable as resp_bytes but null-on-HIT makes it queryable as 'total bytes fetched from origin'.", '"obytes":%{if(req.http.x-of-start ~ "^[0-9]+$", "" + resp.bytes_written, "null")}V', 'UBIGINT', 15),
    # json.escape converts the value to JSON-string-safe form so
    # quotes / backslashes / control bytes get their \\uXXXX escapes
    # instead of terminating the literal early.
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

# ---------------------------------------------------------------------------
# Group metadata
# ---------------------------------------------------------------------------

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

# Group dependency rules: group → required group
GROUP_DEPENDENCIES = {g: info["requires"] for g, info in GROUP_INFO.items() if info.get("requires")}

# ---------------------------------------------------------------------------
# Preset bundles
# ---------------------------------------------------------------------------

PRESETS: dict[str, dict[str, Any]] = {
    "minimal": _p("Minimal", "Always-on fields only. Error rates, latency, hit rates, throughput.", []),
    "standard": _p('Standard', 'Recommended for most sites. Request details, infrastructure, basic geo, proxy detection.', ['A', 'C', 'D', 'I']),
    "security": _p('Security', 'Standard + TLS fingerprinting and WAF. For security monitoring.', ['A', 'C', 'D', 'H', 'I', 'J'], field_overrides={'tls_ciphers_sha': True}),
    "performance": _p('Performance', 'Standard + cache deep-dive, network quality core, and origin metrics. For delivery optimization.', ['A', 'B', 'C', 'D', 'F', 'L']),
    "streaming": _p('Streaming', 'Standard + precision geo and full network telemetry. For streaming video analysis.', ['A', 'C', 'D', 'E', 'F', 'G']),
    "full": _p('Full', 'All groups enabled. Maximum data collection.', ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']),
}

# ---------------------------------------------------------------------------
# Insight definitions
# ---------------------------------------------------------------------------

INSIGHT_DEFINITIONS: list[dict[str, Any]] = [
    _i('error_spikes', 'origin', 'Error Spikes', 'URLs with abnormally elevated 5xx error rates in the window vs. baseline', ['status', 'url'], ['A']),
    _i('botnet_grouping', 'security', 'Botnet Grouping', 'TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline — attackers rotate IPs but rarely change TLS stacks', ['ja3', 'ja4'], ['H']),
    # Phase-3: promoted from stub to a computed insight (definitions.py).
    # Keep category + required_fields in lockstep with the registry entry
    # (drift-guard test).
    _i('low_and_slow', 'security', 'Low and Slow Scans', 'IPs probing many sensitive / vulnerability paths at a deliberately low request rate spread over a long span — designed to evade rate limits', ['ip', 'url'], ['A']),
    # Phase-3: 401/403 spike per IP on authentication paths.
    _i('credential_enumeration', 'security', 'Credential Enumeration / Brute Force', 'IPs generating a spike of 401/403 responses on authentication paths (login, auth, OAuth, password reset) — credential stuffing or brute force', ['ip', 'url', 'status'], ['A']),
    # Phase-4 (Track A): per-IP 404 enumeration. Keep category +
    # required_fields in lockstep with the registry entry (drift-guard test).
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
    # Phase-3: promoted from stub to a computed insight (definitions.py) and
    # filed under the dedicated Network Path section. required_fields are
    # narrowed to exactly what the computed SQL scans (asn + the Group-G
    # quality metrics it aggregates) so the availability gate matches the
    # registry eligibility check — an over-broad list would gray the card
    # out on services that CAN compute it. Keep in lockstep with the
    # registry entry (drift-guard test).
    _i('network_asn_health', 'network', 'Network Path (ASN) Health', 'ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline', ['asn', 'ploss', 'rtt_var', 'retrans'], ['F', 'G']),
    _i('region_latency', 'network', 'Billing Region Latency', 'Fastly regions showing elevated edge latency or TTFB spikes', ['server_region', 'elapsed', 'ttfb', 'asn', 'ottfb'], ['C', 'F', 'L']),
    # Only hard-requires the always-on core fields (ip, timestamp), so the
    # card runs on every service. ``required_groups`` is empty to match: the
    # UA-allowlist bot-suppression (Group A's ``ua``) is a SOFT enhancement
    # with a NULL fallback, not a gate — declaring ["A"] would gray the card
    # out in the LogSettings/ProvisionWizard previews even though it runs.
    _i('repeated_patterns', 'security', 'Scripted Traffic Patterns', 'IPs demonstrating highly regular request intervals — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits', ['ip']),
    _i('repeated_patterns_fp', 'security', 'Scripted Traffic Patterns (by TLS Fingerprint)', 'TLS fingerprints sending requests on a highly regular cadence — catches IP-rotating scrapers sharing a stable TLS stack', ['ip']),
    # ── 18-vs-30 catalog reconciliation (design plan §5.1 step 5) ───────────
    # The registry (definitions.py) computes 30 insights; this legacy catalog
    # historically listed only 16 of them (+ 2 stubs). The entries below add
    # the missing computed insights so /api/insight-availability + the loading
    # skeletons cover every card and group under the same `category` as the
    # registry. `category` here MUST match the registry (drift-guard test).
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
    # ── Phase-4 Track B: coalesced traffic dims (referer/method/asn) ─────────
    # Keep category + required_fields in lockstep with the registry (drift-guard).
    _i('referer_monoculture', 'traffic', 'Referer Monoculture', 'A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood', ['referer'], ['A']),
    _i('method_drift', 'traffic', 'HTTP Method Drift', 'A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods', ['method'], ['A']),
    _i('new_asn_traffic', 'network', 'New ASN Traffic', 'An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range', ['asn'], ['F']),
    _i('asn_hosting_shift', 'network', 'ASN Hosting Shift', "An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure", ['asn', 'p_type'], ['F', 'I']),
    # ── Phase-4 Track B2: standalone network + edge cards ───────────────────
    # Keep category + required_fields in lockstep with the registry (drift-guard).
    _i('metro_delivery_degradation', 'network', 'Metro Delivery-Rate Degradation', 'US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation', ['metro', 'delivery_rate'], ['E', 'G']),
    _i('connection_type_mix', 'network', 'Connection-Type Mix Shift', 'A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift', ['c_type', 'c_speed'], ['G']),
    _i('pop_latency_regression', 'edge', 'PoP Latency Regression', 'Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency', ['pop', 'elapsed'], ['C']),
    _i('http3_fallback', 'network', 'HTTP/3 → TCP Fallback Spike', 'A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)', ['transport'], ['F']),
    _i('cache_hit_cliff', 'edge', 'Cache HIT-Ratio Cliff', "The whole service's edge cache HIT ratio (HIT/(HIT+MISS)) fell off a cliff vs. baseline — a purge storm, TTL change, or origin Cache-Control regression", ['cache']),
    # ── Phase-4 Track C: field-gated insights (new edge fields required) ─────
    # Keep category + required_fields in lockstep with the registry (drift-guard).
    _i('payload_compression_regression', 'edge', 'Payload Compression Regression', 'Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB', ['url', 'resp_header_content_encoding', 'resp_bytes', 'status'], ['A']),
    _i('session_harvesting', 'security', 'Session-ID Harvesting', 'A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt', ['ip', 'cookie_session'], ['H']),
    _i('timeout_split', 'origin', 'Origin Connect vs Read Timeout Split', 'Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing', ['oconnect_ms', 'ottfb'], ['L']),
    _i('crawler_disparity', 'security', 'Bot & Crawler Referral & Bandwidth Disparity', 'Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth while driving negligible human referral traffic back to the platform', ['ua', 'referer', 'ip', 'resp_bytes', 'timestamp'], ['A']),
    _i('stale_browser_version', 'security', 'Stale Headless Browser', 'Detects anomalous clusters of request traffic using stale Chrome major versions, a standard fingerprint for un-updated Puppeteer/Playwright headless bots', ['ua', 'ip', 'timestamp'], ['A']),
    _i('orphaned_deep_crawl', 'security', 'Orphaned Deep Crawl', 'Flags client IPs performing direct, deep scans of nested catalog/database resource routes with empty HTTP referer headers', ['ip', 'url', 'referer', 'timestamp'], ['A']),
    _i('residential_fingerprint_dispersion', 'security', 'Residential Fingerprint Dispersion', 'Identifies identical TLS/TCP client handshakes distributed across multiple residential ASNs, signaling rotated botnet scans evading ASN blocks', ['ja4', 'ip', 'asn', 'p_type', 'timestamp'], ['H', 'I']),
]

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------
