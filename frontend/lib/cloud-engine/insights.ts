/**
 * Cloud Engine handlers for Page 15 Automated Anomaly Insights (`/insights`):
 * - `POST /api/insights`
 * - `GET /api/insight-availability` (`/api/insight-availability/{service_id}`)
 * - `POST /api/insights/cache-collapse-detail`
 *
 * Implements the 51 detector catalog (`INSIGHT_DEFINITIONS` / `INSIGHT_CARDS`),
 * the 8 coalesced single-pass DuckDB scan builders (`_coalesced_*_aggregates`),
 * the 80% (`0.8`) hourly rollup fastpath coverage threshold, and cache-collapse
 * drill-down shaping with `mask_ips` (`0.0.0.0`) PII protection.
 */

import { escapeSqlLiteral, requireSafeIdent } from "./sql-validator";

export const INSIGHTS_ROLLUP_MIN_COVERAGE = 0.8; // 80% hourly rollup coverage threshold
export const min_Baseline = 24;

export const COALESCED_SCAN_NAMES = [
  "_coalesced_city_aggregates",
  "_coalesced_url_aggregates",
  "_coalesced_ip_security_aggregates",
  "_coalesced_fp_aggregates",
  "_coalesced_dim_aggregates",
  "_coalesced_geo_latency_aggregates",
  "_coalesced_asn_aggregates",
  "_coalesced_scalar_aggregates",
] as const;

export type CoalescedScanName = (typeof COALESCED_SCAN_NAMES)[number];

export type InsightCategory =
  | "origin"
  | "security"
  | "traffic"
  | "edge"
  | "network";

export interface InsightDefinitionMeta {
  id: string;
  category: InsightCategory;
  title: string;
  description: string;
  required_fields: string[];
  required_groups: string[];
}

function def(
  id: string,
  category: InsightCategory,
  title: string,
  description: string,
  required_fields: string[],
  required_groups: string[] = [],
): InsightDefinitionMeta {
  return {
    id,
    category,
    title,
    description,
    required_fields,
    required_groups,
  };
}

/**
 * Complete catalog of all 51 anomaly insight detectors matching
 * `backend/core/insight_definitions.py:INSIGHT_DEFINITIONS`.
 */
export const INSIGHT_DEFINITIONS: readonly InsightDefinitionMeta[] = [
  def("error_spikes", "origin", "Error Spikes", "URLs with abnormally elevated 5xx error rates in the window vs. baseline", ["status", "url"], ["A"]),
  def("botnet_grouping", "security", "Botnet Grouping", "TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline — attackers rotate IPs but rarely change TLS stacks", ["ja3", "ja4"], ["H"]),
  def("low_and_slow", "security", "Low and Slow Scans", "IPs probing many sensitive / vulnerability paths at a deliberately low request rate spread over a long span — designed to evade rate limits", ["ip", "url"], ["A"]),
  def("credential_enumeration", "security", "Credential Enumeration / Brute Force", "IPs generating a spike of 401/403 responses on authentication paths (login, auth, OAuth, password reset) — credential stuffing or brute force", ["ip", "url", "status"], ["A"]),
  def("content_discovery", "security", "Content-Discovery Scanning", "IPs generating a burst of 404s across many distinct URLs — directory / endpoint enumeration probing for hidden or vulnerable paths", ["ip", "url", "status"], ["A"]),
  def("city_surges", "traffic", "City Traffic Surges", "Cities with traffic volumes significantly higher than their historical baseline", ["city", "country"], ["D"]),
  def("city_error_spikes", "origin", "City Error Spikes", "Cities experiencing abnormally high error rates compared to their own baseline", ["city", "status"], ["D"]),
  def("city_latency_regressions", "edge", "City Latency Regressions", "Cities where response times (P95) have significantly slowed down compared to their baseline", ["city", "elapsed"], ["D"]),
  def("new_city_traffic", "traffic", "New City Traffic", "Cities with zero baseline presence now sending traffic", ["city"], ["D"]),
  def("new_country_traffic", "traffic", "New Country Traffic", "Countries with zero baseline presence now sending traffic", ["country"], ["D"]),
  def("latency_regression", "edge", "URL Latency Regressions", "URLs where response times (P95) have significantly slowed down compared to their baseline", ["url", "elapsed"], ["A"]),
  def("asn_concentration", "network", "ASN Concentration", "ISPs (ASNs) with a disproportionately large share of total traffic compared to the baseline", ["asn"], ["F"]),
  def("proxy_surge", "security", "Proxy Traffic Surge", "Significant increase in traffic from known anonymizing proxies (VPN, Tor, etc.)", ["p_type"], ["I"]),
  def("ua_monoculture", "traffic", "User-Agent Monoculture", "A single User-Agent string responsible for a massive percentage of traffic — typical for scraping or DDoS bots", ["ua"], ["A"]),
  def("request_size_anomaly", "security", "Request Size Anomalies", "Drastic increase in average request body or header size — signal for data exfiltration or buffer overflow attempts", ["req_bytes", "req_header_bytes"], ["A"]),
  def("cache_ttl_mismatch", "edge", "Cache TTL Mismatches", "Objects being served from cache with very low hits and low TTLs — indicates inefficient caching strategy", ["cache", "ttl", "age", "hits"], ["B"]),
  def("waf_signal_spikes", "security", "WAF Signal Spikes", "Abnormal increase in specific NGWAF signals (e.g. SQLi, XSS) across multiple IPs", ["waf", "waf_sig", "waf_resp", "status"], ["J"]),
  def("network_asn_health", "network", "Network Path (ASN) Health", "ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline", ["asn", "ploss", "rtt_var", "retrans"], ["F", "G"]),
  def("region_latency", "network", "Billing Region Latency", "Fastly regions showing elevated edge latency or TTFB spikes", ["server_region", "elapsed", "ttfb", "asn", "ottfb"], ["C", "F", "L"]),
  def("repeated_patterns", "security", "Scripted Traffic Patterns", "IPs demonstrating highly regular request intervals — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits", ["ip"]),
  def("repeated_patterns_fp", "security", "Scripted Traffic Patterns (by TLS Fingerprint)", "TLS fingerprints sending requests on a highly regular cadence — catches IP-rotating scrapers sharing a stable TLS stack", ["ip"]),
  def("new_probe_urls", "security", "New Probe URLs", "Common attack patterns and sensitive paths appearing for the first time", ["url", "status"], ["A"]),
  def("cipher_spread", "security", "Cipher Fingerprint Clustering", "TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs", ["tls_ciphers_sha"], ["H"]),
  def("impossible_distance", "security", "Impossible Distance / Spoofing", "Traffic where the network latency (RTT) is physically too low for the reported client distance", ["pop", "lat", "lon", "tcp_rtt"], ["C", "E", "F"]),
  def("connection_abuse", "security", "Connection Reuse Anomaly", "IPs making an unusually high number of requests per single TCP connection", ["conn_requests"], ["C"]),
  def("origin_latency_spike", "origin", "Origin Latency Spike", "Sudden and significant increase in P95 response time from the origin server", ["ottfb"], ["L"]),
  def("origin_error_rate", "origin", "Origin Error Rate", "Significant increase in 5xx errors returned by the origin server", ["ost"], ["L"]),
  def("origin_retries", "origin", "Origin Retries Elevated", "URLs experiencing frequent retries when fetching from origin, indicating backend instability", ["oretries"], ["L"]),
  def("origin_ip_failure", "origin", "Specific Origin IP Failing", "One or more origin IP addresses are returning significantly more errors than their peers", ["oip", "ost"], ["L"]),
  def("shield_path_degradation", "origin", "Shield Path Degradation", "Increased latency on the network path between edge POPs and shield POPs", ["rid", "prid", "edge", "pop", "ottfb"], ["C", "L"]),
  def("cache_collapse", "edge", "Cache Efficiency Collapse", "URLs whose cacheable hit ratio (HIT/(HIT+MISS)) dropped sharply vs. their baseline", ["url", "cache"], ["A"]),
  def("cacheability_regression", "edge", "Cacheability Regression", "URLs that flipped from cacheable to mostly PASS (uncacheable) vs. their baseline", ["url", "cache"], ["A"]),
  def("tail_latency", "edge", "Tail Latency Anomaly", "Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers", ["url", "elapsed"], ["A"]),
  def("image_optimization_opportunities", "edge", "Image Optimization Opportunities", "Large images being served without modern compression (WebP/AVIF), especially to mobile users", ["url", "ua", "resp_bytes"], ["A"]),
  def("asn_metro_performance", "network", "ASN/Metro Performance Regressions", "Specific ISP/Metro combinations showing significantly higher network latency than baseline", ["asn", "metro", "tcp_rtt", "country"], ["D", "E", "F"]),
  def("referer_monoculture", "traffic", "Referer Monoculture", "A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood", ["referer"], ["A"]),
  def("method_drift", "traffic", "HTTP Method Drift", "A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods", ["method"], ["A"]),
  def("new_asn_traffic", "network", "New ASN Traffic", "An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range", ["asn"], ["F"]),
  def("asn_hosting_shift", "network", "ASN Hosting Shift", "An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure", ["asn", "p_type"], ["F", "I"]),
  def("metro_delivery_degradation", "network", "Metro Delivery-Rate Degradation", "US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation", ["metro", "delivery_rate"], ["E", "G"]),
  def("connection_type_mix", "network", "Connection-Type Mix Shift", "A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift", ["c_type", "c_speed"], ["G"]),
  def("pop_latency_regression", "edge", "PoP Latency Regression", "Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency", ["pop", "elapsed"], ["C"]),
  def("http3_fallback", "network", "HTTP/3 → TCP Fallback Spike", "A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)", ["transport"], ["F"]),
  def("cache_hit_cliff", "edge", "Cache HIT-Ratio Cliff", "The whole service's edge cache HIT ratio (HIT/(HIT+MISS)) fell off a cliff vs. baseline — a purge storm, TTL change, or origin Cache-Control regression", ["cache"]),
  def("payload_compression_regression", "edge", "Payload Compression Regression", "Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB", ["url", "resp_header_content_encoding", "resp_bytes", "status"], ["A"]),
  def("session_harvesting", "security", "Session-ID Harvesting", "A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt", ["ip", "cookie_session"], ["H"]),
  def("timeout_split", "origin", "Origin Connect vs Read Timeout Split", "Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing", ["oconnect_ms", "ottfb"], ["L"]),
  def("crawler_disparity", "security", "Bot & Crawler Referral & Bandwidth Disparity", "Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth while driving negligible human referral traffic back to the platform", ["ua", "referer", "ip", "resp_bytes", "timestamp"], ["A"]),
  def("stale_browser_version", "security", "Stale Headless Browser", "Detects anomalous clusters of request traffic using stale Chrome major versions, a standard fingerprint for un-updated Puppeteer/Playwright headless bots", ["ua", "ip", "timestamp"], ["A"]),
  def("orphaned_deep_crawl", "security", "Orphaned Deep Crawl", "Flags client IPs performing direct, deep scans of nested catalog/database resource routes with empty HTTP referer headers", ["ip", "url", "referer", "timestamp"], ["A"]),
  def("residential_fingerprint_dispersion", "security", "Residential Fingerprint Dispersion", "Identifies identical TLS/TCP client handshakes distributed across multiple residential ASNs, signaling rotated botnet scans evading ASN blocks", ["ja4", "ip", "asn", "p_type", "timestamp"], ["H", "I"]),
];

export const INSIGHT_CARDS = INSIGHT_DEFINITIONS;

// ── 8 Coalesced Single-Pass Scan SQL Builders ────────────────────────────────

export function _coalesced_city_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "city", "region", "country", status, elapsed,
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
      WHERE "city" IS NOT NULL AND "city" != ''
    )
    SELECT "city", "region", "country",
      COUNT(*) FILTER (WHERE is_w) AS w_cnt,
      COUNT(*) FILTER (WHERE is_b) AS b_cnt,
      SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors_4xx,
      SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_errors_4xx,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95
    FROM base
    GROUP BY "city", "region", "country"
    HAVING COUNT(*) FILTER (WHERE is_w) >= 5
  `;
}

export function _coalesced_url_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "url", status, cache, elapsed, resp_bytes,
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
      WHERE "url" IS NOT NULL
    )
    SELECT "url",
      COUNT(*) FILTER (WHERE is_w) AS w_total,
      COUNT(*) FILTER (WHERE is_b) AS b_total,
      SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_5xx,
      SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_5xx,
      SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
      SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
      SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
      SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
      SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
      SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
      ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p99,
      ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50
    FROM base
    GROUP BY "url"
    HAVING COUNT(*) FILTER (WHERE is_w) >= 3
  `;
}

export function _coalesced_ip_security_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "ip", "url", status,
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
      WHERE "ip" IS NOT NULL AND "ip" != ''
    )
    SELECT "ip",
      COUNT(*) FILTER (WHERE is_w AND status IN (401, 403)) AS ce_w_denied,
      COUNT(*) FILTER (WHERE is_b AND status IN (401, 403)) AS ce_b_denied,
      COUNT(*) FILTER (WHERE is_w AND status = 404) AS cd_w_404,
      COUNT(DISTINCT "url") FILTER (WHERE is_w AND status = 404) AS cd_distinct_404
    FROM base
    GROUP BY "ip"
    HAVING COUNT(*) FILTER (WHERE is_w) >= 5
  `;
}

export function _coalesced_fp_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "ja4" AS fp, "tls_ciphers_sha" AS cipher, "ip", "asn",
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
    )
    SELECT fp, cipher,
      COUNT(DISTINCT "ip") FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '') AS bg_w_ips,
      COUNT(DISTINCT "ip") FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '') AS bg_b_ips
    FROM base
    GROUP BY GROUPING SETS ((fp), (cipher))
  `;
}

export function _coalesced_dim_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "country", "p_type", "method", "referer", "ua",
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
    )
    SELECT "country", "p_type", "method", "referer", "ua",
      COUNT(*) FILTER (WHERE is_w) AS w_cnt,
      COUNT(*) FILTER (WHERE is_b) AS b_cnt
    FROM base
    GROUP BY GROUPING SETS (("country"), ("p_type"), ("method"), ("referer"), ("ua"))
  `;
}

export function _coalesced_geo_latency_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "pop", "server_region", "metro", elapsed,
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
    )
    SELECT "pop", "server_region", "metro",
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
      COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
      COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total
    FROM base
    GROUP BY GROUPING SETS (("pop"), ("server_region"), ("metro"))
  `;
}

export function _coalesced_asn_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "asn", "p_type",
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
    )
    SELECT "asn",
      COUNT(*) FILTER (WHERE is_w) AS w_cnt,
      COUNT(*) FILTER (WHERE is_b) AS b_cnt,
      COUNT(*) FILTER (WHERE is_w AND "p_type" = 'hosting') AS w_hosting,
      COUNT(*) FILTER (WHERE is_b AND "p_type" = 'hosting') AS b_hosting
    FROM base
    GROUP BY "asn"
  `;
}

export function _coalesced_scalar_aggregates(
  tableName = "requests",
  windowStartIso = "1970-01-01T00:00:00Z",
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  const wEsc = escapeSqlLiteral(windowStartIso);
  return `
    WITH base AS (
      SELECT "transport", "cache",
        (timestamp < CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_b,
        (timestamp >= CAST('${wEsc}' AS TIMESTAMPTZ)) AS is_w
      FROM ${tbl}
    )
    SELECT
      COUNT(*) FILTER (WHERE is_w AND lower("transport") = 'quic') AS w_quic,
      COUNT(*) FILTER (WHERE is_w AND "transport" IS NOT NULL AND "transport" != '') AS w_tr_total,
      COUNT(*) FILTER (WHERE is_b AND lower("transport") = 'quic') AS b_quic,
      COUNT(*) FILTER (WHERE is_b AND "transport" IS NOT NULL AND "transport" != '') AS b_tr_total,
      SUM(CASE WHEN starts_with("cache", 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
      SUM(CASE WHEN starts_with("cache", 'HIT') OR starts_with("cache", 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_cacheable,
      SUM(CASE WHEN starts_with("cache", 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
      SUM(CASE WHEN starts_with("cache", 'HIT') OR starts_with("cache", 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_cacheable
    FROM base
  `;
}

export function zscore(value: number, mean: number, stddev: number): number {
  if (!Number.isFinite(stddev) || stddev <= 0) return 0;
  return (value - mean) / stddev;
}

export function threshold_checks(params: {
  windowValue: number;
  baselineValue: number;
  minWindowCount?: number;
  windowCount?: number;
  multiplier?: number;
  minDelta?: number;
}): boolean {
  const {
    windowValue,
    baselineValue,
    minWindowCount = 10,
    windowCount = 10,
    multiplier = 2.0,
    minDelta = 0.05,
  } = params;
  if (windowCount < minWindowCount) return false;
  return (
    windowValue >= baselineValue * multiplier &&
    windowValue - baselineValue >= minDelta
  );
}

export interface InsightItem {
  label: string;
  current_val: number;
  baseline_val: number;
  unit: string;
  meta: Record<string, unknown>;
  severity: "critical" | "warning" | "info";
  investigate_url?: string;
}

export interface InsightCard {
  id: string;
  title: string;
  description: string;
  category: string;
  severity: "critical" | "warning" | "info" | "clean" | "error";
  summary: string;
  items: InsightItem[];
}

export interface InsightsEnvelope {
  insights: InsightCard[];
  window_start: string;
  window_end: string;
  baseline_start: string;
  baseline_end: string;
  computed_at: string;
  window_hours: number;
  baseline_hours: number;
  window_total_requests: number;
}

export function build_insights_envelope(
  insights: InsightCard[] = [],
  window_start = "",
  window_end = "",
  baseline_start = "",
  baseline_end = "",
  window_hours = 24,
  baseline_hours = 168,
  window_total_requests = 0,
): InsightsEnvelope {
  return {
    insights,
    window_start,
    window_end,
    baseline_start,
    baseline_end,
    computed_at: window_end || new Date().toISOString(),
    window_hours,
    baseline_hours,
    window_total_requests,
  };
}

export const buildInsightsEnvelope = build_insights_envelope;

export interface InsightAvailabilityEntry extends InsightDefinitionMeta {
  available: boolean;
  missing_fields: string[] | null;
}

export interface InsightAvailabilityResponse {
  insights: InsightAvailabilityEntry[];
  unavailable: InsightAvailabilityEntry[];
  available: boolean;
}

/**
 * Evaluate which of the 51 insight detectors are supported by the service's
 * enabled Parquet schema columns (`GET /api/insight-availability`).
 */
export function handleInsightAvailability(
  _serviceId?: string,
  actualColumns?: Iterable<string>,
): InsightAvailabilityResponse {
  const colSet = actualColumns
    ? new Set(Array.from(actualColumns))
    : new Set(
        INSIGHT_DEFINITIONS.flatMap((d) => d.required_fields),
      );

  const insights: InsightAvailabilityEntry[] = INSIGHT_DEFINITIONS.map((d) => {
    const missing = d.required_fields.filter((f) => !colSet.has(f));
    const available = missing.length === 0;
    return {
      ...d,
      available,
      missing_fields: available ? null : missing,
    };
  });

  return {
    insights,
    unavailable: insights.filter((i) => !i.available),
    available: true,
  };
}

export interface InsightsRequest {
  service_id?: string;
  window_size_hrs?: number;
  baseline_hours?: number;
  clamp_start?: string | null;
  clamp_end?: string | null;
  filters?: Record<string, unknown>;
}

export interface CloudInsightsContext {
  serviceId?: string;
  tableName?: string;
  maskIps?: boolean;
  role?: "admin" | "analyst" | "analyst-no-pii";
  availableColumns?: ReadonlySet<string>;
  availableHistoryHours?: number;
  hourlyRollupCoverage?: number;
  now?: Date;
  urlAggregateRows?: Record<string, unknown>[];
  ipSecurityRows?: Record<string, unknown>[];
  windowTotalRequests?: number;
}

export function canUseInsightsRollupFastpath(
  filters: Record<string, unknown> | undefined,
  hourlyRollupCoverage: number,
): boolean {
  const hasActiveFilters =
    filters != null && Object.keys(filters).length > 0;
  if (hasActiveFilters) return false;
  return hourlyRollupCoverage >= INSIGHTS_ROLLUP_MIN_COVERAGE;
}

export async function handleInsights(
  body: InsightsRequest = {},
  ctx: CloudInsightsContext = {},
): Promise<InsightsEnvelope> {
  const windowHours = Math.max(1, Number(body.window_size_hrs ?? 24));
  const baselineHours = Math.max(1, Number(body.baseline_hours ?? 168));
  const nowDate = ctx.now ?? new Date();
  const endDate =
    body.clamp_end && !Number.isNaN(Date.parse(body.clamp_end))
      ? new Date(Math.min(nowDate.getTime(), Date.parse(body.clamp_end)))
      : nowDate;
  const windowStartDate = new Date(
    endDate.getTime() - windowHours * 3_600_000,
  );
  const baselineStartDate = new Date(
    endDate.getTime() - (windowHours + baselineHours) * 3_600_000,
  );

  const windowEnd = endDate.toISOString();
  const windowStart = windowStartDate.toISOString();
  const baselineStart = baselineStartDate.toISOString();
  const baselineEnd = windowStart;
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  const serviceId = body.service_id ?? ctx.serviceId ?? "default";

  const availability = handleInsightAvailability(
    serviceId,
    ctx.availableColumns,
  );
  const availableDefs = availability.insights.filter((i) => i.available);
  const historyHours = ctx.availableHistoryHours ?? baselineHours;

  const urlRows = ctx.urlAggregateRows ?? [];
  const ipRows = ctx.ipSecurityRows ?? [];

  const cards: InsightCard[] = availableDefs.map((d) => {
    if (historyHours < min_Baseline) {
      return {
        id: d.id,
        title: d.title,
        description: d.description,
        category: d.category,
        severity: "info",
        summary: `Requires ${baselineHours}h of historical data (only ${Math.max(0.1, Math.round(historyHours * 10) / 10)}h available)`,
        items: [],
      };
    }

    if (d.id === "cache_collapse" && urlRows.length > 0) {
      const items: InsightItem[] = [];
      for (const r of urlRows) {
        const wHits = Number(r.w_hits ?? 0);
        const wMiss = Number(r.w_miss ?? 0);
        const bHits = Number(r.b_hits ?? 0);
        const bMiss = Number(r.b_miss ?? 0);
        const wCacheable = wHits + wMiss;
        const bCacheable = bHits + bMiss;
        if (wCacheable >= 20 && bCacheable >= 20) {
          const wRate = (wHits / wCacheable) * 100;
          const bRate = (bHits / bCacheable) * 100;
          if (bRate - wRate >= 20) {
            const url = String(r.url ?? "(empty)");
            items.push({
              label: url,
              current_val: Math.round(wRate * 10) / 10,
              baseline_val: Math.round(bRate * 10) / 10,
              unit: "% hit rate",
              meta: {
                window_requests: wCacheable,
                baseline_requests: bCacheable,
                filters: { url },
              },
              severity: wRate < 20 ? "critical" : "warning",
              investigate_url: `/dashboard?service=${encodeURIComponent(serviceId)}&start=${encodeURIComponent(windowStart)}&end=${encodeURIComponent(windowEnd)}&filter_url=${encodeURIComponent(url)}`,
            });
          }
        }
      }
      if (items.length > 0) {
        return {
          id: d.id,
          title: d.title,
          description: d.description,
          category: d.category,
          severity: items.some((i) => i.severity === "critical")
            ? "critical"
            : "warning",
          summary: `${items.length} anomalies detected`,
          items,
        };
      }
    }

    if (d.id === "credential_enumeration" && ipRows.length > 0) {
      const items: InsightItem[] = [];
      for (const r of ipRows) {
        const wDenied = Number(r.ce_w_denied ?? 0);
        const bDenied = Number(r.ce_b_denied ?? 0);
        if (wDenied >= 20 && wDenied > bDenied * 2) {
          const ipLabel = maskIps ? "0.0.0.0" : String(r.ip ?? "(unknown)");
          items.push({
            label: ipLabel,
            current_val: wDenied,
            baseline_val: bDenied,
            unit: "denied requests",
            meta: {
              requests: wDenied,
              filters: { ip: ipLabel },
            },
            severity: wDenied >= 100 ? "critical" : "warning",
          });
        }
      }
      if (items.length > 0) {
        return {
          id: d.id,
          title: d.title,
          description: d.description,
          category: d.category,
          severity: items.some((i) => i.severity === "critical")
            ? "critical"
            : "warning",
          summary: `${items.length} anomalies detected`,
          items,
        };
      }
    }

    return {
      id: d.id,
      title: d.title,
      description: d.description,
      category: d.category,
      severity: "clean",
      summary: `No ${d.title.toLowerCase()} detected`,
      items: [],
    };
  });

  return build_insights_envelope(
    cards,
    windowStart,
    windowEnd,
    baselineStart,
    baselineEnd,
    windowHours,
    baselineHours,
    ctx.windowTotalRequests ?? 0,
  );
}

// ── Cache-Collapse Drill-Down (`POST /api/insights/cache-collapse-detail`) ───

export interface CacheCollapseDetailRequest {
  service_id?: string;
  url: string;
  window_size_hrs?: number;
  baseline_hours?: number;
}

export interface CacheCollapseTimelinePoint {
  bucket: string;
  expected_hits: number;
  real_hits: number;
  misses: number;
  total_requests: number;
  hit_rate: number;
}

export interface CacheCollapseRecentMiss {
  timestamp: string;
  cache: string;
  pop: string | null;
  ip: string | null;
  status: number | null;
}

export interface CacheCollapseDetailResponse {
  url: string;
  window_hit_rate: number;
  baseline_hit_rate: number;
  window_pass_rate: number;
  baseline_pass_rate: number;
  breakdown: {
    hits: number;
    misses: number;
    passes: number;
    other: number;
  };
  timeline: CacheCollapseTimelinePoint[];
  recent_misses: CacheCollapseRecentMiss[];
  has_data: boolean;
}

export async function handleCacheCollapseDetail(
  body: CacheCollapseDetailRequest,
  ctx: {
    maskIps?: boolean;
    role?: "admin" | "analyst" | "analyst-no-pii";
    rates?: {
      w_hits: number;
      w_miss: number;
      w_pass: number;
      w_total: number;
      b_hits: number;
      b_miss: number;
      b_pass: number;
      b_total: number;
    };
    timelineRows?: {
      bucket: string;
      total_requests: number;
      real_hits: number;
      misses: number;
    }[];
    missRows?: {
      timestamp: string;
      cache?: string | null;
      pop?: string | null;
      ip?: string | null;
      status?: number | null;
    }[];
  } = {},
): Promise<CacheCollapseDetailResponse> {
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  const r = ctx.rates ?? {
    w_hits: 0,
    w_miss: 0,
    w_pass: 0,
    w_total: 0,
    b_hits: 0,
    b_miss: 0,
    b_pass: 0,
    b_total: 0,
  };

  const wCacheable = r.w_hits + r.w_miss;
  const bCacheable = r.b_hits + r.b_miss;
  const wHitRateRatio = wCacheable > 0 ? r.w_hits / wCacheable : 0;
  const bHitRateRatio = bCacheable > 0 ? r.b_hits / bCacheable : 0;
  const wPassRateRatio = r.w_total > 0 ? r.w_pass / r.w_total : 0;
  const bPassRateRatio = r.b_total > 0 ? r.b_pass / r.b_total : 0;

  const timeline: CacheCollapseTimelinePoint[] = (ctx.timelineRows ?? []).map(
    (pt) => {
      const cacheable = pt.real_hits + pt.misses;
      return {
        bucket: pt.bucket,
        total_requests: pt.total_requests,
        real_hits: pt.real_hits,
        misses: pt.misses,
        expected_hits: Math.round(cacheable * bHitRateRatio),
        hit_rate: cacheable > 0 ? (pt.real_hits / cacheable) * 100 : 0,
      };
    },
  );

  const recentMisses: CacheCollapseRecentMiss[] = (ctx.missRows ?? []).map(
    (m) => ({
      timestamp: m.timestamp,
      cache: m.cache || "UNKNOWN",
      pop: m.pop ?? null,
      ip: m.ip ? (maskIps ? "0.0.0.0" : m.ip) : null,
      status: m.status != null ? Number(m.status) : null,
    }),
  );

  return {
    url: body.url,
    window_hit_rate: wHitRateRatio * 100,
    baseline_hit_rate: bHitRateRatio * 100,
    window_pass_rate: wPassRateRatio * 100,
    baseline_pass_rate: bPassRateRatio * 100,
    breakdown: {
      hits: r.w_hits,
      misses: r.w_miss,
      passes: r.w_pass,
      other: Math.max(0, r.w_total - r.w_hits - r.w_miss - r.w_pass),
    },
    timeline,
    recent_misses: recentMisses,
    has_data: r.w_total + r.b_total > 0,
  };
}
