"""Insight definitions and metadata catalog for log analytics."""

from __future__ import annotations

from typing import Any


def _i(
    id_: str,
    cat: str,
    title: str,
    desc: str,
    fields: list[str],
    groups: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": id_,
        "category": cat,
        "title": title,
        "description": desc,
        "required_fields": fields,
        "required_groups": groups or [],
    }


# fmt: off
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
