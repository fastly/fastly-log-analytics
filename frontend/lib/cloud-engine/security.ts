/**
 * Feature F20 — Page 10 `/security` (Security, Bots, Proxy Watchdog & Threat Intel) Cloud-Engine Module.
 *
 * Parity with `backend/routers/security.py`, `backend/high_scale/security.py`,
 * `backend/repositories/security/watchdog.py`, and `backend/repositories/utils/response_shaping/domains.py` + `asn.py`:
 * - POST `/api/security/aggregates` and POST/GET `/api/security/bundle`
 * - POST `/api/security/top-bots`
 * - POST `/api/security/proxies`
 * - GET `/api/security/threat-intel`
 * - GET `/api/security/proxies/export` (`format=fastly-acl` with `"ip,comment"` and `format=plain`)
 */

import { sqlEscape } from "@/lib/sqlBuilder";
import { compileFiltersToSql, getDuckDBEngine, type DuckDBReadContext, type DuckDBColumnType } from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";
import { POP_COORDINATES, haversineKm } from "./origin";
import { formatAsnLabel, type FilterSpec } from "./performance";

export type SecuritySectionName =
  | "tls_fingerprints"
  | "fingerprint_coverage"
  | "req_size_dist"
  | "top_ips_header"
  | "ipv6_adoption"
  | "proxy_dist"
  | "conn_reuse_dist"
  | "verified_bots_ts"
  | "ngwaf_verified_bots"
  | "ngwaf_verified_bots_ts"
  | "wellknown_bots"
  | "bot_efficiency"
  | "top_bots"
  | "proxies";

export const ALL_SECURITY_SECTIONS: readonly SecuritySectionName[] = [
  "tls_fingerprints",
  "fingerprint_coverage",
  "req_size_dist",
  "top_ips_header",
  "ipv6_adoption",
  "proxy_dist",
  "conn_reuse_dist",
  "verified_bots_ts",
  "ngwaf_verified_bots",
  "ngwaf_verified_bots_ts",
  "wellknown_bots",
  "bot_efficiency",
  "top_bots",
  "proxies",
] as const;

export const SECURITY_REQ_HEADER_BUCKETS: ReadonlyArray<readonly [number, string]> = [
  [256, "0-256B"],
  [1024, "256B-1KB"],
  [4096, "1KB-4KB"],
  [8192, "4KB-8KB"],
  [32768, "8KB-32KB"],
] as const;
export const SECURITY_REQ_HEADER_LAST = ">32KB";

export const SECURITY_CONN_REUSE_BUCKETS: ReadonlyArray<readonly [number, string]> = [
  [1, "1 (New)"],
  [5, "2-5"],
  [20, "6-20"],
  [100, "21-100"],
] as const;
export const SECURITY_CONN_REUSE_LAST = ">100";

export const ACTIVE_CLIENTS_LIMIT = 100;
export const IMPOSSIBLE_DIST_SPEED_KM_PER_MS = 100.0;
export const IMPOSSIBLE_DIST_BUFFER_KM = 150.0;

export const SECURITY_ROLLUPS = {
  sec_tls_fingerprints: "sec_tls_fingerprints",
  sec_req_size_dist: "sec_req_size_dist",
  sec_conn_reuse_dist: "sec_conn_reuse_dist",
  sec_proxy_dist: "sec_proxy_dist",
  sec_ipv6_adoption: "sec_ipv6_adoption",
  sec_verified_bots_ts: "sec_verified_bots_ts",
} as const;

/**
 * Embedded `well_known_bots` (`well-known-bots`) directory for client-side bot verification
 * and efficiency classification.
 */
export interface WellKnownBotEntry {
  id: string;
  category: string;
  patterns: readonly string[];
  rdns_suffixes?: readonly string[];
}

export const well_known_bots: readonly WellKnownBotEntry[] = [
  { id: "Googlebot", category: "Search Engine Crawler", patterns: ["googlebot", "google-inspectiontool"] },
  { id: "Bingbot", category: "Search Engine Crawler", patterns: ["bingbot", "msnbot"] },
  { id: "GPTBot", category: "AI Crawler", patterns: ["gptbot", "chatgpt-user", "oai-searchbot"] },
  { id: "ClaudeBot", category: "AI Crawler", patterns: ["claudebot", "claude-web", "anthropic-ai"] },
  { id: "PerplexityBot", category: "AI Crawler", patterns: ["perplexitybot", "perplexity-user"] },
  { id: "Applebot", category: "Search Engine Crawler", patterns: ["applebot"] },
  { id: "DuckDuckBot", category: "Search Engine Crawler", patterns: ["duckduckbot"] },
  { id: "AhrefsBot", category: "SEO Crawler", patterns: ["ahrefsbot"] },
  { id: "SemrushBot", category: "SEO Crawler", patterns: ["semrushbot"] },
  { id: "Bytespider", category: "AI Crawler", patterns: ["bytespider"] },
] as const;

export interface SecurityAggregatesRequest {
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>;
  start_time?: string | null;
  end_time?: string | null;
  range_token?: string | null;
  anchor?: string | null;
  service_id?: string | null;
  bucket_seconds?: number | null;
  filters?: FilterSpec | null;
  sections?: SecuritySectionName[] | null;
  mask_ips?: boolean;
}

export interface SecurityFingerprintItem {
  fingerprint: string;
  ip_count: number;
  request_count: number;
}

export interface SecurityBucketItem {
  bucket: string;
  count: number;
}

export interface SecurityTopIpHeaderItem {
  ip: string;
  max_header: number;
}

export interface SecurityIpv6Point {
  time: string;
  pct: number;
}

export interface SecurityProxyTypeItem {
  type: string;
  proxy_type?: string;
  count: number;
}

export interface SecurityVerifiedBotTsPoint {
  time: string;
  bot_type: string;
  count: number;
}

export interface SecurityNgwafBotItem {
  bot_name: string;
  wellknown_bot_name: string | null;
  bot_category: string | null;
  request_count: number;
}

export interface SecurityNgwafBotTsPoint {
  time: string;
  bot_name: string;
  count: number;
}

export interface WellKnownBotSummaryItem {
  id: string;
  category: string;
  requests: number;
  unique_ips: number;
  verified_ips: number;
  spoofed_ips: number;
  unverified_ips: number;
  status: "Verified" | "Spoofed" | "Unverified";
  sample_ips: Array<{ ip: string; status: string; rdns: string }>;
}

export interface BotEfficiencyItem {
  bot_name: string;
  company: string;
  category: string;
  crawl_requests: number;
  crawl_bytes: number;
  referral_visits: number;
  crawl_to_referral_ratio: number | null;
  req_pct: number;
  bytes_pct: number;
}

export interface SecurityTopBotItem {
  name: string;
  category: string;
  requests: number;
}

export interface SecurityActiveClientItem {
  ip: string;
  asn: number | null;
  isp: string;
  rtt_min_ms: number;
  tcp_rtt_ms: number | null;
  distance_km: number | null;
  pop: string | null;
  client_lat: number | null;
  client_lon: number | null;
  pop_lat: number;
  pop_lon: number;
  impossible_distance: boolean;
  impossible_dist?: boolean;
  risk_level: "High" | "Medium" | "Low";
  country: string | null;
  city: string | null;
}

export interface SecurityProxiesData {
  active_proxies_count: number;
  total_requests_count: number;
  distance_mismatches_count: number;
  traffic_quality: Array<{ type: string; count: number }>;
  suspicious_isps: Array<{ asn: number | null; isp: string; count: number }>;
  active_clients: SecurityActiveClientItem[];
}

export interface SecurityAggregatesResponse extends Partial<SecurityProxiesData> {
  tls_fingerprints?: SecurityFingerprintItem[];
  fingerprint_coverage?: { tls_ciphers_sha: number };
  req_size_dist?: SecurityBucketItem[];
  top_ips_header?: SecurityTopIpHeaderItem[];
  ipv6_adoption?: SecurityIpv6Point[];
  proxy_dist?: SecurityProxyTypeItem[];
  conn_reuse_dist?: SecurityBucketItem[];
  verified_bots_ts?: SecurityVerifiedBotTsPoint[];
  ngwaf_configured?: boolean;
  ngwaf_verified_bots?: SecurityNgwafBotItem[];
  ngwaf_verified_bots_ts?: SecurityNgwafBotTsPoint[];
  wellknown_bots?: WellKnownBotSummaryItem[];
  bot_efficiency?: BotEfficiencyItem[];
  bots?: SecurityTopBotItem[];
  ngwaf_bots?: Array<{ bot_name: string; bot_category: string | null; requests: number }>;
}

export interface ThreatIntelRow {
  tls_fingerprint: string;
  requests: number;
  top_country: string;
  matched_waf_rules_count: number;
  bot_percentage: number;
  is_anonymized_proxy: boolean;
}

export interface ThreatIntelListResponse {
  data: ThreatIntelRow[];
}

export interface SecurityEngineClient {
  getAvailableColumns: () => Promise<ReadonlySet<string>>;
  tryRollup?: (
    name: string,
    opts: { startTime: string; endTime: string; serviceId: string },
  ) => Promise<Record<string, unknown>[] | null>;
  query: (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;
}

let customSecurityClient: SecurityEngineClient | null = null;

export function setSecurityEngineClient(client: SecurityEngineClient | null): void {
  customSecurityClient = client;
}

export async function getSecurityEngineClient(context?: DuckDBReadContext): Promise<SecurityEngineClient> {
  if (customSecurityClient) return customSecurityClient;
  const engine = (context ?? getDuckDBEngine().readContext()) as unknown as {
    getPresentColumns: () => Promise<ReadonlySet<string>>;
    querySql?: (sql: string) => Promise<Record<string, unknown>[]>;
    executeSql?: (sql: string) => Promise<Record<string, unknown>[]>;
    queryRollup?: (
      name: string,
      opts: { startTime: string; endTime: string; serviceId: string },
    ) => Promise<Record<string, unknown>[] | null>;
  };
  return {
    async getAvailableColumns(): Promise<ReadonlySet<string>> {
      return engine.getPresentColumns();
    },
    tryRollup: engine.queryRollup ? (name, opts) => engine.queryRollup!(name, opts) : undefined,
    async query(sql: string): Promise<Record<string, unknown>[]> {
      const rewritten = sql.replace(/\bFROM\s+logs\b/gi, "FROM requests");
      if (typeof engine.querySql === "function") return engine.querySql(rewritten);
      if (typeof engine.executeSql === "function") return engine.executeSql(rewritten);
      return [];
    },
  };
}

function toNum(v: unknown, fallback = 0): number {
  if (typeof v === "number") return Number.isFinite(v) ? v : fallback;
  if (typeof v === "bigint") return Number(v);
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }
  return fallback;
}

function roundTo(v: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(v * factor) / factor;
}

function toIsoTimestamp(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number") return new Date(value).toISOString();
  return String(value ?? "");
}

export function maskIpValue(ip: string, maskIps = false): string {
  if (!maskIps || !ip) return ip;
  if (ip.includes(":")) {
    const parts = ip.split(":");
    return `${parts.slice(0, 2).join(":")}::xxxx`;
  }
  const octets = ip.split(".");
  if (octets.length === 4) {
    return `${octets[0]}.${octets[1]}.${octets[2]}.xxx`;
  }
  return "0.0.0.0";
}

/**
 * Mirrors `_expand_security_sections` in `backend/routers/security.py`:
 * - `{"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}` union group
 * - `tls_fingerprints` implies `fingerprint_coverage`
 */
export function expandSecuritySections(
  sections?: readonly string[] | null,
): Set<SecuritySectionName> {
  if (!sections || sections.length === 0) {
    return new Set(ALL_SECURITY_SECTIONS);
  }
  const out = new Set<SecuritySectionName>();
  for (const s of sections) {
    if ((ALL_SECURITY_SECTIONS as readonly string[]).includes(s)) {
      out.add(s as SecuritySectionName);
    }
  }
  if (out.has("ngwaf_verified_bots") || out.has("ngwaf_verified_bots_ts")) {
    out.add("ngwaf_verified_bots");
    out.add("ngwaf_verified_bots_ts");
  }
  if (out.has("tls_fingerprints")) {
    out.add("fingerprint_coverage");
  }
  return out.size > 0 ? out : new Set(ALL_SECURITY_SECTIONS);
}

function buildSecurityBaseWhere(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { whereSql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (req.start_time) {
    const ms = Date.parse(req.start_time);
    if (Number.isFinite(ms)) {
      if (availableColumns?.has("ts_ms")) {
        clauses.push(`"ts_ms" >= ${Math.floor(ms)}`);
      } else {
        clauses.push(`timestamp >= CAST('${sqlEscape(new Date(ms).toISOString())}' AS TIMESTAMPTZ)`);
      }
    }
  }
  if (req.end_time) {
    const ms = Date.parse(req.end_time);
    if (Number.isFinite(ms)) {
      if (availableColumns?.has("ts_ms")) {
        clauses.push(`"ts_ms" < ${Math.ceil(ms)}`);
      } else {
        clauses.push(`timestamp < CAST('${sqlEscape(new Date(ms).toISOString())}' AS TIMESTAMPTZ)`);
      }
    }
  }
  if (req.filters && typeof req.filters === "object") {
    const dFilters: DashboardFilter[] = [];
    for (const [rawKey, entry] of Object.entries(req.filters)) {
      if (rawKey === "filter_mode" || !entry) continue;
      const field = rawKey.replace(/_\d+$/, "");
      if (Array.isArray(entry)) {
        for (const v of entry) {
          if (v) dFilters.push({ field, value: String(v), mode: "include" });
        }
      } else if (typeof entry === "object" && Array.isArray(entry.values)) {
        const mode = entry.mode === "exclude" ? "exclude" : "include";
        for (const v of entry.values) {
          if (v) dFilters.push({ field, value: String(v), mode });
        }
      }
    }
    if (dFilters.length > 0) {
      clauses.push(`(${compileFiltersToSql(dFilters, false, [], req.customColumnTypes)})`);
    }
  }

  return {
    whereSql: clauses.length > 0 ? clauses.join(" AND ") : "1=1",
    params,
  };
}

export function buildReqHeaderBucketCaseSql(colExpr = "h"): string {
  return `CASE
    WHEN ${colExpr} <= 256 THEN '0-256B'
    WHEN ${colExpr} <= 1024 THEN '256B-1KB'
    WHEN ${colExpr} <= 4096 THEN '1KB-4KB'
    WHEN ${colExpr} <= 8192 THEN '4KB-8KB'
    WHEN ${colExpr} <= 32768 THEN '8KB-32KB'
    ELSE '>32KB'
  END`;
}

export function buildConnReuseBucketCaseSql(colExpr = "c"): string {
  return `CASE
    WHEN ${colExpr} <= 1 THEN '1 (New)'
    WHEN ${colExpr} <= 5 THEN '2-5'
    WHEN ${colExpr} <= 20 THEN '6-20'
    WHEN ${colExpr} <= 100 THEN '21-100'
    ELSE '>100'
  END`;
}

function securityIpExpr(availableColumns?: ReadonlySet<string>): string {
  if (availableColumns?.has("client_ip")) {
    return availableColumns.has("ip")
      ? "COALESCE(NULLIF(CAST(client_ip AS VARCHAR), ''), ip)"
      : "client_ip";
  }
  return "ip";
}

export function buildSecurityTlsFingerprintsSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const ipCol = securityIpExpr(availableColumns);
  const sql = `
    SELECT
      CAST(tls_ciphers_sha AS VARCHAR) AS fingerprint,
      COUNT(DISTINCT ${ipCol}) AS ip_count,
      COUNT(*) AS request_count
    FROM logs
    WHERE (${whereSql})
      AND tls_ciphers_sha IS NOT NULL
      AND CAST(tls_ciphers_sha AS VARCHAR) != ''
    GROUP BY fingerprint
    ORDER BY request_count DESC
    LIMIT 20
  `.trim();
  return { sql, params };
}

export function buildSecurityFingerprintCoverageSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const sql = `
    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) FILTER (WHERE tls_ciphers_sha IS NOT NULL AND CAST(tls_ciphers_sha AS VARCHAR) != '') AS populated
    FROM logs
    WHERE (${whereSql})
  `.trim();
  return { sql, params };
}

export function buildSecurityReqSizeDistSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const bucketCase = buildReqHeaderBucketCaseSql("h");
  const sql = `
    SELECT
      ${bucketCase} AS bucket,
      COUNT(*) AS count,
      MIN(h) AS min_val
    FROM (
      SELECT TRY_CAST(req_header_bytes AS BIGINT) AS h
      FROM logs
      WHERE (${whereSql})
    )
    WHERE h IS NOT NULL
    GROUP BY bucket
    ORDER BY min_val ASC
  `.trim();
  return { sql, params };
}

export function buildSecurityTopIpsHeaderSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const ipCol = securityIpExpr(availableColumns);
  const sql = `
    SELECT
      CAST(${ipCol} AS VARCHAR) AS ip,
      MAX(TRY_CAST(req_header_bytes AS BIGINT)) AS max_header
    FROM logs
    WHERE (${whereSql})
      AND ${ipCol} IS NOT NULL
      AND CAST(${ipCol} AS VARCHAR) != ''
      AND TRY_CAST(req_header_bytes AS BIGINT) IS NOT NULL
    GROUP BY 1
    ORDER BY max_header DESC
    LIMIT 10
  `.trim();
  return { sql, params };
}

export function buildSecurityConnReuseDistSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const bucketCase = buildConnReuseBucketCaseSql("c");
  const sql = `
    SELECT
      ${bucketCase} AS bucket,
      COUNT(*) AS count,
      MIN(c) AS min_val
    FROM (
      SELECT TRY_CAST(conn_requests AS BIGINT) AS c
      FROM logs
      WHERE (${whereSql})
    )
    WHERE c IS NOT NULL AND c > 0
    GROUP BY bucket
    ORDER BY min_val ASC
  `.trim();
  return { sql, params };
}

export function buildSecurityIpv6AdoptionSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const ipv6Expr = availableColumns?.has("is_ipv6")
    ? "LOWER(CAST(is_ipv6 AS VARCHAR)) IN ('1', 'true', 't', 'yes')"
    : "FALSE";
  const sql = `
    SELECT
      date_trunc('hour', EPOCH_MS(CAST(ts_ms AS BIGINT))) AS bucket_ts,
      COUNT(*) FILTER (WHERE ${ipv6Expr}) * 100.0 / NULLIF(COUNT(*), 0) AS pct
    FROM logs
    WHERE (${whereSql})
    GROUP BY bucket_ts
    ORDER BY bucket_ts ASC
  `.trim();
  return { sql, params };
}

export function buildSecurityProxyDistSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const col = availableColumns?.has("proxy_type") ? "proxy_type" : "p_type";
  const sql = `
    SELECT
      CAST(${col} AS VARCHAR) AS type,
      COUNT(*) AS count
    FROM logs
    WHERE (${whereSql})
      AND ${col} IS NOT NULL
      AND CAST(${col} AS VARCHAR) != ''
    GROUP BY type
    ORDER BY count DESC
  `.trim();
  return { sql, params };
}

export function buildSecurityVerifiedBotsTsSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const bucketSeconds = Math.max(60, Math.trunc(toNum(req.bucket_seconds, 300)));
  const sql = `
    SELECT
      time_bucket(INTERVAL '${bucketSeconds} seconds', EPOCH_MS(CAST(ts_ms AS BIGINT))) AS bucket_ts,
      REPLACE(CAST(waf_sig AS VARCHAR), 'VERIFIED-BOT.', '') AS bot_type,
      COUNT(*) AS count
    FROM logs
    WHERE (${whereSql})
      AND waf_sig IS NOT NULL
      AND POSITION('VERIFIED-BOT.' IN UPPER(CAST(waf_sig AS VARCHAR))) > 0
    GROUP BY bucket_ts, bot_type
    ORDER BY bucket_ts ASC, bot_type ASC
  `.trim();
  return { sql, params };
}

export function buildSecurityProxiesSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const ipCol = securityIpExpr(availableColumns);
  const rttMinCol = availableColumns?.has("rtt_min")
    ? "COALESCE(rtt_min, tcpi_min_rtt)" : "tcpi_min_rtt";
  const tcpRttCol = availableColumns?.has("tcp_rtt")
    ? "COALESCE(tcp_rtt, tcpi_rtt)" : "tcpi_rtt";
  const sql = `
    SELECT
      CAST(${ipCol} AS VARCHAR) AS ip,
      TRY_CAST(asn AS BIGINT) AS asn,
      TRY_CAST(${rttMinCol} AS DOUBLE) AS rtt_min,
      TRY_CAST(${tcpRttCol} AS DOUBLE) AS tcp_rtt,
      TRY_CAST(lat AS DOUBLE) AS lat,
      TRY_CAST(lon AS DOUBLE) AS lon,
      CAST(pop AS VARCHAR) AS pop,
      CAST(country AS VARCHAR) AS country,
      CAST(city AS VARCHAR) AS city
    FROM logs
    WHERE (${whereSql})
      AND ${rttMinCol} IS NOT NULL
    LIMIT 5000
  `.trim();
  return { sql, params };
}

export function buildSecurityThreatIntelSql(
  req: SecurityAggregatesRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
  const hasJa3 = !availableColumns || availableColumns.has("ja3");
  const hasJa4 = !availableColumns || availableColumns.has("ja4");
  const fpExpr =
    hasJa3 && hasJa4
      ? "COALESCE(NULLIF(CAST(ja3 AS VARCHAR), ''), NULLIF(CAST(ja4 AS VARCHAR), ''))"
      : hasJa3
        ? "NULLIF(CAST(ja3 AS VARCHAR), '')"
        : hasJa4
          ? "NULLIF(CAST(ja4 AS VARCHAR), '')"
          : "NULL";
  const isBotExpr = !availableColumns || availableColumns.has("is_bot")
    ? "LOWER(CAST(is_bot AS VARCHAR)) IN ('1', 'true', 't', 'yes')"
    : "FALSE";
  const pTypeExpr = !availableColumns || availableColumns.has("p_type")
    ? "MAX(CASE WHEN p_type IS NOT NULL AND CAST(p_type AS VARCHAR) != '' THEN 1 ELSE 0 END)"
    : "0";
  const wafExpr =
    !availableColumns || availableColumns.has("waf_logged_rules") || availableColumns.has("waf_blocked_rules")
      ? "COUNT(*) FILTER (WHERE (waf_logged_rules IS NOT NULL AND CAST(waf_logged_rules AS VARCHAR) != '') OR (waf_blocked_rules IS NOT NULL AND CAST(waf_blocked_rules AS VARCHAR) != ''))"
      : "0";

  const sql = `
    SELECT
      ${fpExpr} AS tls_fingerprint,
      COUNT(*) AS requests,
      MODE(CAST(country AS VARCHAR)) AS top_country,
      ${wafExpr} AS matched_waf_rules_count,
      ROUND(COUNT(*) FILTER (WHERE ${isBotExpr}) * 100.0 / NULLIF(COUNT(*), 0), 2) AS bot_percentage,
      ${pTypeExpr} AS is_anonymized_proxy
    FROM logs
    WHERE (${whereSql})
      AND ${fpExpr} IS NOT NULL
    GROUP BY tls_fingerprint
    ORDER BY requests DESC
    LIMIT 50
  `.trim();
  return { sql, params };
}

export function matchWellKnownBot(ua: string): WellKnownBotEntry | null {
  const lower = (ua || "").toLowerCase();
  if (!lower) return null;
  for (const bot of well_known_bots) {
    if (bot.patterns.some((p) => lower.includes(p))) {
      return bot;
    }
  }
  return null;
}

export function summarizeWellKnownBotRows(
  rows: ReadonlyArray<Record<string, unknown>>,
): WellKnownBotSummaryItem[] {
  const grouped = new Map<
    string,
    {
      entry: WellKnownBotEntry;
      requests: number;
      ips: Set<string>;
      sampleIps: Array<{ ip: string; status: string; rdns: string }>;
    }
  >();

  for (const row of rows) {
    const ua = String(row.ua ?? "");
    const ip = String(row.ip ?? row.client_ip ?? "");
    const cnt = Math.max(0, Math.trunc(toNum(row.cnt ?? row.count ?? row.requests, 0)));
    if (!ua || cnt <= 0) continue;
    const match = matchWellKnownBot(ua);
    if (!match) continue;

    let bucket = grouped.get(match.id);
    if (!bucket) {
      bucket = { entry: match, requests: 0, ips: new Set(), sampleIps: [] };
      grouped.set(match.id, bucket);
    }
    bucket.requests += cnt;
    if (ip && !bucket.ips.has(ip)) {
      bucket.ips.add(ip);
      if (bucket.sampleIps.length < 5) {
        bucket.sampleIps.push({ ip, status: "Unverified", rdns: "N/A" });
      }
    }
  }

  return [...grouped.values()]
    .map((b) => ({
      id: b.entry.id,
      category: b.entry.category,
      requests: b.requests,
      unique_ips: b.ips.size,
      verified_ips: 0,
      spoofed_ips: 0,
      unverified_ips: b.ips.size,
      status: "Unverified" as const,
      sample_ips: b.sampleIps,
    }))
    .sort((a, b) => b.requests - a.requests);
}

export function evaluateImpossibleDistance(params: {
  rttMinUs: number | null;
  tcpRttUs: number | null;
  clientLat: number | null;
  clientLon: number | null;
  pop: string | null;
}): {
  distanceKm: number | null;
  popLat: number;
  popLon: number;
  impossibleDist: boolean;
  isTunnel: boolean;
  isMed: boolean;
  riskLevel: "High" | "Medium" | "Low";
} {
  const cleanPop = (params.pop ?? "").trim().toUpperCase().split("-")[0] || "";
  const popCoord = POP_COORDINATES[cleanPop] ?? [38.0, -97.0];
  const popLat = popCoord[0];
  const popLon = popCoord[1];

  let distanceKm: number | null = null;
  if (
    params.clientLat !== null &&
    params.clientLon !== null &&
    Number.isFinite(params.clientLat) &&
    Number.isFinite(params.clientLon)
  ) {
    distanceKm = roundTo(haversineKm(params.clientLat, params.clientLon, popLat, popLon), 1);
  }

  const rttMin = params.rttMinUs;
  const tcpRtt = params.tcpRttUs;
  const impossibleDist =
    rttMin !== null &&
    rttMin > 0 &&
    rttMin < 200_000 &&
    distanceKm !== null &&
    distanceKm > (rttMin / 1000.0) * IMPOSSIBLE_DIST_SPEED_KM_PER_MS + IMPOSSIBLE_DIST_BUFFER_KM;

  const isTunnel =
    rttMin !== null && tcpRtt !== null && tcpRtt > 0 && rttMin / tcpRtt < 0.1;
  const isMed =
    rttMin !== null && tcpRtt !== null && tcpRtt > 0 && rttMin / tcpRtt < 0.2;

  const riskLevel: "High" | "Medium" | "Low" =
    impossibleDist || isTunnel ? "High" : isMed ? "Medium" : "Low";

  return {
    distanceKm,
    popLat,
    popLon,
    impossibleDist,
    isTunnel,
    isMed,
    riskLevel,
  };
}

export function shapeSecurityProxiesFromRawRows(
  rows: ReadonlyArray<Record<string, unknown>>,
  maskIps = false,
): SecurityProxiesData {
  const activeProxyIps = new Set<string>();
  const mismatchIps = new Set<string>();
  let totalProxyRequests = 0;
  let tunnelCount = 0;
  let wifiCount = 0;
  let directCount = 0;
  const ispCounts = new Map<number | null, number>();
  const clients: SecurityActiveClientItem[] = [];

  for (const row of rows) {
    const rawIp = String(row.ip ?? row.client_ip ?? "").trim();
    const rttMinUs = row.rtt_min != null ? toNum(row.rtt_min, NaN) : null;
    const tcpRttUs = row.tcp_rtt != null ? toNum(row.tcp_rtt, NaN) : null;
    const clientLat = row.lat != null ? toNum(row.lat, NaN) : null;
    const clientLon = row.lon != null ? toNum(row.lon, NaN) : null;
    const asn = row.asn != null && Number.isFinite(toNum(row.asn, NaN)) ? Math.trunc(toNum(row.asn)) : null;
    const pop = row.pop ? String(row.pop) : null;

    const evalRes = evaluateImpossibleDistance({
      rttMinUs: rttMinUs !== null && Number.isFinite(rttMinUs) ? rttMinUs : null,
      tcpRttUs: tcpRttUs !== null && Number.isFinite(tcpRttUs) ? tcpRttUs : null,
      clientLat: clientLat !== null && Number.isFinite(clientLat) ? clientLat : null,
      clientLon: clientLon !== null && Number.isFinite(clientLon) ? clientLon : null,
      pop,
    });

    if (evalRes.isTunnel) {
      tunnelCount++;
      ispCounts.set(asn, (ispCounts.get(asn) ?? 0) + 1);
    } else if (
      rttMinUs !== null &&
      tcpRttUs !== null &&
      tcpRttUs > 0 &&
      rttMinUs / tcpRttUs < 0.7
    ) {
      wifiCount++;
    } else {
      directCount++;
    }

    if (evalRes.isTunnel || evalRes.impossibleDist) {
      totalProxyRequests++;
      if (rawIp) activeProxyIps.add(rawIp);
    }
    if (evalRes.impossibleDist && rawIp) {
      mismatchIps.add(rawIp);
    }

    if (rttMinUs !== null && Number.isFinite(rttMinUs) && (evalRes.impossibleDist || evalRes.isMed)) {
      clients.push({
        ip: maskIpValue(rawIp, maskIps),
        asn,
        isp: asn !== null ? formatAsnLabel(asn) : "Unknown ISP",
        rtt_min_ms: roundTo(rttMinUs / 1000.0, 2),
        tcp_rtt_ms: tcpRttUs !== null && Number.isFinite(tcpRttUs) ? roundTo(tcpRttUs / 1000.0, 2) : null,
        distance_km: evalRes.distanceKm,
        pop,
        client_lat: clientLat !== null && Number.isFinite(clientLat) ? roundTo(clientLat, 4) : null,
        client_lon: clientLon !== null && Number.isFinite(clientLon) ? roundTo(clientLon, 4) : null,
        pop_lat: evalRes.popLat,
        pop_lon: evalRes.popLon,
        impossible_distance: evalRes.impossibleDist,
        impossible_dist: evalRes.impossibleDist,
        risk_level: evalRes.riskLevel,
        country: row.country ? String(row.country) : null,
        city: row.city ? String(row.city) : null,
      });
    }
  }

  clients.sort((a, b) => {
    const rank = (r: "High" | "Medium" | "Low") => (r === "High" ? 0 : r === "Medium" ? 1 : 2);
    const diff = rank(a.risk_level) - rank(b.risk_level);
    if (diff !== 0) return diff;
    return (b.distance_km ?? -1) - (a.distance_km ?? -1);
  });

  const trafficQuality = [
    { type: "Direct Connection", count: directCount },
    { type: "WiFi / Mobile", count: wifiCount },
    { type: "Active Tunnel / Proxy", count: tunnelCount },
  ]
    .filter((q) => q.count > 0)
    .sort((a, b) => b.count - a.count);

  const suspiciousIsps = [...ispCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([asn, count]) => ({
      asn,
      isp: asn !== null ? formatAsnLabel(asn) : "Unknown ISP",
      count,
    }));

  return {
    active_proxies_count: activeProxyIps.size,
    total_requests_count: totalProxyRequests,
    distance_mismatches_count: mismatchIps.size,
    traffic_quality: trafficQuality,
    suspicious_isps: suspiciousIsps,
    active_clients: clients.slice(0, ACTIVE_CLIENTS_LIMIT),
  };
}

export function shapeSecurityThreatIntelRows(
  rows: ReadonlyArray<Record<string, unknown>>,
): ThreatIntelListResponse {
  return {
    data: rows.map((r) => ({
      tls_fingerprint: String(r.tls_fingerprint ?? ""),
      requests: Math.trunc(toNum(r.requests)),
      top_country: String(r.top_country || "Unknown"),
      matched_waf_rules_count: Math.trunc(toNum(r.matched_waf_rules_count)),
      bot_percentage: roundTo(toNum(r.bot_percentage), 2),
      is_anonymized_proxy: Boolean(r.is_anonymized_proxy),
    })),
  };
}

/**
 * Formats active proxy clients for `/api/security/proxies/export`:
 * - `format="fastly-acl"` -> CSV with `"ip,comment"` header
 * - `format="plain"` -> newline-delimited IP list
 */
export function formatProxiesExportPayload(
  clients: ReadonlyArray<Partial<SecurityActiveClientItem>>,
  options: { threshold?: string; format?: string } = {},
): { body: string; mediaType: string; filename: string } {
  const threshold = options.threshold ?? "High";
  const format = options.format ?? "fastly-acl";

  const filtered = clients.filter((c) => {
    if (threshold === "High") return c.risk_level === "High";
    return c.risk_level === "High" || c.risk_level === "Medium";
  });

  const seen = new Set<string>();
  const unique: Array<{ ip: string; impossible_distance: boolean }> = [];
  for (const c of filtered) {
    const ip = String(c.ip ?? "").trim();
    if (!ip || seen.has(ip)) continue;
    seen.add(ip);
    unique.push({
      ip,
      impossible_distance: Boolean(c.impossible_distance ?? c.impossible_dist),
    });
  }

  if (format === "fastly-acl") {
    const lines = ["ip,comment"];
    for (const item of unique) {
      const reason = item.impossible_distance
        ? "Impossible distance mismatch"
        : "Behavioral VPN/Proxy tunnel";
      lines.push(`${item.ip},Watchdog block: ${reason}`);
    }
    return {
      body: `${lines.join("\n")}\n`,
      mediaType: "text/csv",
      filename: `watchdog_blocklist_${threshold.toLowerCase()}_${format}.csv`,
    };
  }

  const lines = unique.map((item) => item.ip);
  return {
    body: lines.length > 0 ? `${lines.join("\n")}\n` : "",
    mediaType: "text/plain",
    filename: `blocklist_${threshold.toLowerCase()}.txt`,
  };
}

/** Every request column a /api/security/aggregates section scan reads. */
export const SECURITY_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "ip", "client_ip", "tls_ciphers_sha", "req_header_bytes", "conn_requests", "is_ipv6",
  "p_type", "waf_sig", "ua", "asn", "rtt_min", "tcpi_min_rtt", "tcp_rtt", "tcpi_rtt", "lat", "lon", "pop",
  "country", "city",
] as const;

/** The columns a projected /api/security/aggregates load needs, or null when the filters can't name theirs. */
export function securityScanColumns(filters: unknown): string[] | null {
  if (filters == null || filters === "") return [...SECURITY_SCAN_COLUMNS];
  if (typeof filters !== "object" || Array.isArray(filters)) return null;
  return [...new Set([...SECURITY_SCAN_COLUMNS, ...Object.keys(filters)
    .filter((key) => key !== "filter_mode").map((key) => key.replace(/_\d+$/, ""))])];
}

export const THREAT_INTEL_SCAN_COLUMNS = [
  "timestamp", "ja3", "ja4", "is_bot", "p_type", "waf_logged_rules", "waf_blocked_rules", "country",
] as const;

/** The columns a projected /api/security/threat-intel load needs, or null when the filters can't name theirs. */
export function threatIntelScanColumns(filters: unknown): string[] | null {
  if (filters == null || filters === "") return [...THREAT_INTEL_SCAN_COLUMNS];
  if (typeof filters !== "object" || Array.isArray(filters)) return null;
  return [...new Set([...THREAT_INTEL_SCAN_COLUMNS, ...Object.keys(filters)
    .filter((key) => key !== "filter_mode").map((key) => key.replace(/_\d+$/, ""))])];
}

export async function executeSecurityAggregates(
  req: SecurityAggregatesRequest,
  context?: DuckDBReadContext,
): Promise<SecurityAggregatesResponse> {
  const read = context ?? getDuckDBEngine().readContext();
  req = { ...req, customColumnTypes: read.getCustomColumnTypes() };
  const client = await getSecurityEngineClient(read);
  const availableColumns = await client.getAvailableColumns();
  const sections = expandSecuritySections(req.sections);
  const maskIps = Boolean(req.mask_ips);

  const response: SecurityAggregatesResponse = {};
  const tasks: Promise<void>[] = [];

  if (sections.has("tls_fingerprints")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityTlsFingerprintsSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.tls_fingerprints = rows.map((r) => ({
          fingerprint: String(r.fingerprint ?? ""),
          ip_count: Math.trunc(toNum(r.ip_count)),
          request_count: Math.trunc(toNum(r.request_count)),
        }));
      })(),
    );
  }

  if (sections.has("fingerprint_coverage")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityFingerprintCoverageSql(req, availableColumns);
        const rows = await client.query(sql, params);
        const total = toNum(rows[0]?.total_rows, 0);
        const pop = toNum(rows[0]?.populated, 0);
        response.fingerprint_coverage = {
          tls_ciphers_sha: total > 0 ? pop / total : 0.0,
        };
      })(),
    );
  }

  if (sections.has("req_size_dist")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityReqSizeDistSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.req_size_dist = rows.map((r) => ({
          bucket: String(r.bucket ?? ""),
          count: Math.trunc(toNum(r.count)),
        }));
      })(),
    );
  }

  if (sections.has("top_ips_header")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityTopIpsHeaderSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.top_ips_header = rows.map((r) => ({
          ip: maskIpValue(String(r.ip ?? ""), maskIps),
          max_header: Math.trunc(toNum(r.max_header)),
        }));
      })(),
    );
  }

  if (sections.has("conn_reuse_dist")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityConnReuseDistSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.conn_reuse_dist = rows.map((r) => ({
          bucket: String(r.bucket ?? ""),
          count: Math.trunc(toNum(r.count)),
        }));
      })(),
    );
  }

  if (sections.has("ipv6_adoption")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityIpv6AdoptionSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.ipv6_adoption = rows.map((r) => ({
          time: toIsoTimestamp(r.bucket_ts ?? r.time),
          pct: roundTo(toNum(r.pct), 2),
        }));
      })(),
    );
  }

  if (sections.has("proxy_dist")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityProxyDistSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.proxy_dist = rows.map((r) => ({
          type: String(r.type ?? r.proxy_type ?? ""),
          count: Math.trunc(toNum(r.count)),
        }));
      })(),
    );
  }

  if (sections.has("verified_bots_ts")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityVerifiedBotsTsSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.verified_bots_ts = rows.map((r) => ({
          time: toIsoTimestamp(r.bucket_ts ?? r.time),
          bot_type: String(r.bot_type ?? ""),
          count: Math.trunc(toNum(r.count)),
        }));
      })(),
    );
  }

  if (sections.has("ngwaf_verified_bots") || sections.has("ngwaf_verified_bots_ts")) {
    response.ngwaf_configured = false;
    response.ngwaf_verified_bots = [];
    response.ngwaf_verified_bots_ts = [];
  }

  if (sections.has("wellknown_bots")) {
    tasks.push(
      (async () => {
        const { whereSql, params } = buildSecurityBaseWhere(req, availableColumns);
        const ipCol = securityIpExpr(availableColumns);
        const rows = await client.query(
          `SELECT CAST(ua AS VARCHAR) AS ua, CAST(${ipCol} AS VARCHAR) AS ip, COUNT(*) AS cnt FROM logs WHERE (${whereSql}) AND ua IS NOT NULL GROUP BY 1, 2 LIMIT 5000`,
          params,
        );
        response.wellknown_bots = summarizeWellKnownBotRows(rows);
      })(),
    );
  }

  if (sections.has("bot_efficiency")) {
    response.bot_efficiency = [];
  }

  if (sections.has("top_bots")) {
    response.bots = [];
    response.ngwaf_bots = [];
  }

  if (sections.has("proxies")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildSecurityProxiesSql(req, availableColumns);
        const rows = await client.query(sql, params);
        Object.assign(response, shapeSecurityProxiesFromRawRows(rows, maskIps));
      })(),
    );
  }

  await Promise.all(tasks);
  return response;
}

export async function executeSecurityThreatIntel(
  req: SecurityAggregatesRequest,
  context?: DuckDBReadContext,
): Promise<ThreatIntelListResponse> {
  const read = context ?? getDuckDBEngine().readContext();
  req = { ...req, customColumnTypes: read.getCustomColumnTypes() };
  const client = await getSecurityEngineClient(read);
  const availableColumns = await client.getAvailableColumns();
  const { sql, params } = buildSecurityThreatIntelSql(req, availableColumns);
  const rows = await client.query(sql, params);
  return shapeSecurityThreatIntelRows(rows);
}

async function parseSecurityRequest(req: Request, url: URL): Promise<SecurityAggregatesRequest> {
  let body: Record<string, unknown> = {};
  if (req.method === "POST") {
    try {
      const parsed = await req.clone().json();
      if (parsed && typeof parsed === "object") {
        body = parsed as Record<string, unknown>;
      }
    } catch {
      body = {};
    }
  }
  return {
    start_time: (body.start_time as string | undefined) ?? url.searchParams.get("start_time"),
    end_time: (body.end_time as string | undefined) ?? url.searchParams.get("end_time"),
    range_token: (body.range_token as string | undefined) ?? url.searchParams.get("range_token"),
    anchor: (body.anchor as string | undefined) ?? url.searchParams.get("anchor"),
    service_id: (body.service_id as string | undefined) ?? url.searchParams.get("service_id"),
    bucket_seconds:
      body.bucket_seconds != null
        ? Number(body.bucket_seconds)
        : url.searchParams.get("bucket_seconds")
          ? Number(url.searchParams.get("bucket_seconds"))
          : 300,
    filters: (body.filters as FilterSpec | undefined) ?? null,
    sections: Array.isArray(body.sections) ? (body.sections as SecuritySectionName[]) : null,
    mask_ips: Boolean(
      body.mask_ips ||
        url.searchParams.get("mask_ips") === "1" ||
        req.headers.get("x-mask-ips") === "1" ||
        req.headers.get("x-session-role") === "analyst-no-pii",
    ),
  };
}

/**
 * Route dispatcher for `/api/security/*` endpoints.
 */
export async function handleSecurityRequest(
  req: Request,
  path?: string,
  url?: URL,
  context?: DuckDBReadContext,
): Promise<Response | null> {
  const parsedUrl = url ?? new URL(req.url, "http://localhost");
  const pathname = (path ?? parsedUrl.pathname).replace(/\/+$/, "");
  if (/^\/api\/security\/(?:aggregates|bundle|top-bots|proxies(?:\/export)?|threat-intel)$/.test(pathname)) {
    context ??= getDuckDBEngine().readContext();
  }

  if (pathname === "/api/security/aggregates" || pathname === "/api/security/bundle") {
    const secReq = await parseSecurityRequest(req, parsedUrl);
    const data = await executeSecurityAggregates(secReq, context);
    return Response.json(data, { status: 200 });
  }

  if (pathname === "/api/security/top-bots") {
    const secReq = await parseSecurityRequest(req, parsedUrl);
    secReq.sections = ["top_bots"];
    const data = await executeSecurityAggregates(secReq, context);
    return Response.json(
      { bots: data.bots ?? [], ngwaf_bots: data.ngwaf_bots ?? [] },
      { status: 200 },
    );
  }

  if (pathname === "/api/security/proxies") {
    const secReq = await parseSecurityRequest(req, parsedUrl);
    secReq.sections = ["proxies"];
    const data = await executeSecurityAggregates(secReq, context);
    return Response.json(data, { status: 200 });
  }

  if (pathname === "/api/security/threat-intel") {
    const secReq = await parseSecurityRequest(req, parsedUrl);
    const data = await executeSecurityThreatIntel(secReq, context);
    return Response.json(data, { status: 200 });
  }

  if (pathname === "/api/security/proxies/export") {
    const secReq = await parseSecurityRequest(req, parsedUrl);
    secReq.sections = ["proxies"];
    const data = await executeSecurityAggregates(secReq, context);
    const threshold = parsedUrl.searchParams.get("threshold") ?? "High";
    const format = parsedUrl.searchParams.get("format") ?? "fastly-acl";
    const exported = formatProxiesExportPayload(data.active_clients ?? [], {
      threshold,
      format,
    });
    return new Response(exported.body, {
      status: 200,
      headers: {
        "Content-Type": exported.mediaType,
        "Content-Disposition": `attachment; filename=${exported.filename}`,
      },
    });
  }

  return null;
}
