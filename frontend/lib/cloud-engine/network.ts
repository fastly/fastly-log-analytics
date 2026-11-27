import {
  duckSqlString,
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
  type DuckDBReadContext,
} from "./dashboard/duckdb";
import type { FiltersPayload } from "@/types/api";
import { cloudActiveLogFieldIds } from "./active-log-fields";
import { buildOriginShieldingAnalysisSql, enrichShieldingRouteWithPopDistance } from "./origin";
import { parseEpochMs, roundTo } from "./engine-utils";
import { analyticsFiltersToSql, rangeServiceSql } from "./query-params";
import { formatAsnDisplayLabel, formatCityCountryLabel, loadAsnNames } from "./asn-names";
import { resolveCountryName } from "@/lib/format";
export { parseEpochMs, roundTo };

export const TCP_FIELDS = [
  "client_rtt_us",
  "client_ploss",
  "client_cwnd",
  "client_tcpi_rtt",
  "client_tcpi_rttvar",
  "client_tcpi_rcv_space",
  "client_tcpi_snd_mss",
  "client_tcpi_delta_retrans",
] as const;

export const TCP_FIELD_CATALOG_IDS: Record<string, readonly string[]> = {
  client_rtt_us: ["client_rtt_us", "tcp_rtt", "tcpi_rtt"],
  client_tcpi_rtt: ["client_tcpi_rtt", "tcp_rtt", "tcpi_rtt"],
  client_ploss: ["client_ploss", "ploss"],
  client_cwnd: ["client_cwnd", "cwnd", "q_cwnd", "tcpi_snd_cwnd"],
  client_tcpi_rttvar: ["client_tcpi_rttvar", "rtt_var", "q_rtt_var", "tcpi_rtt_var"],
  client_tcpi_delta_retrans: ["client_tcpi_delta_retrans", "retrans", "tcpi_total_retrans"],
  client_tcpi_rcv_space: ["client_tcpi_rcv_space", "tcpi_rcv_space"],
  client_tcpi_snd_mss: ["client_tcpi_snd_mss", "tcpi_snd_mss"],
};

export function missingTcpFieldsFromActiveIds(enabled: ReadonlySet<string>): string[] {
  return TCP_FIELDS.filter((f) => !(TCP_FIELD_CATALOG_IDS[f] ?? [f]).some((id) => enabled.has(id)));
}

export function hasTcpRttField(enabled: ReadonlySet<string>): boolean {
  return (TCP_FIELD_CATALOG_IDS.client_rtt_us ?? ["client_rtt_us"]).some((id) => enabled.has(id));
}

export const PROTOCOL_BUCKETS = [
  "HTTP/3",
  "HTTP/2",
  "HTTP/1.1",
  "HTTP/1.0",
  "Other",
] as const;

export interface MetroRow {
  city?: string | null;
  metro?: string | null;
  country?: string | null;
  reqs?: number;
  total_reqs?: number;
  health_score?: number | null;
  rtt_med_us?: number;
  rtt_med_ms?: number;
  avg_ploss?: number;
  avg_ploss_pct?: number;
  error_pct?: number;
  [key: string]: unknown;
}

export interface NetworkQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  filter_country?: string;
  filter_asn?: string | number;
  filter_pop?: string;
  filters?: FiltersPayload;
  bucket_seconds?: number;
  top_n?: number;
  map_asn?: string;
  region_country?: string;
  sections?: readonly string[];
  /** Only these /network-quality parts, as `{ quality_parts }`: what summaries lacked (see `networkFromRollup`). */
  quality_parts?: readonly QualityPart[];
  /** The ASNs whose client-only loss and RTT variance the `asn_client` part reads. */
  quality_asns?: readonly number[];
  /** Set on the raw read for a summary answer's missing sections, which summaries must not answer again. */
  network_residual?: boolean;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns"> & Partial<Pick<DuckDBReadContext, "getCustomColumnTypes">>;
  customColumnTypes?: ReturnType<DuckDBReadContext["getCustomColumnTypes"]>;
  active_log_field_ids?: readonly string[];
  signal?: AbortSignal;
}

function clamp(val: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, val));
}

/**
 * Computes composite 0-100 network health score.
 * Supports both:
 *  - 4-arg: (rtt_med_us, rtt_baseline_us, avg_ploss, error_pct)
 *  - 5-arg: (throughput_bps, rtt_congestion_us, avg_ploss, rtt_jitter_us, error_pct)
 */
export function computeNetworkHealthScore(
  arg1: number | null | undefined,
  arg2: number | null | undefined,
  avg_ploss: number | null | undefined,
  arg4: number | null | undefined,
  arg5?: number | null | undefined,
): number {
  if (arg5 === undefined) {
    const rtt_med_us = Math.max(0, Number(arg1 ?? 0));
    const rtt_baseline_us = Math.max(1, Number(arg2 ?? 1));
    const ploss = Math.max(0, Number(avg_ploss ?? 0));
    const error_pct = Math.max(0, Number(arg4 ?? 0));

    const rtt_ratio = rtt_med_us / rtt_baseline_us;
    const rtt_score = clamp(100 - Math.max(0, rtt_ratio - 1) * 50, 0, 100);
    const ploss_pct = ploss <= 1 ? ploss * 100 : ploss;
    const ploss_score = clamp(100 - ploss_pct * 20, 0, 100);
    const error_score = clamp(100 - error_pct * 10, 0, 100);
    const composite = 0.4 * rtt_score + 0.35 * ploss_score + 0.25 * error_score;
    return roundTo(clamp(composite, 0, 100), 1);
  }

  const throughput_bps = Math.max(0, Number(arg1 ?? 0));
  const rtt_congestion_us = Math.max(0, Number(arg2 ?? 0));
  const ploss = Math.max(0, Number(avg_ploss ?? 0));
  const rtt_jitter_us = Math.max(0, Number(arg4 ?? 0));
  const error_pct = Math.max(0, Number(arg5 ?? 0));

  const throughput_mbps = throughput_bps / 1_000_000;
  const throughput_score = clamp((throughput_mbps / 50) * 100, 0, 100);
  const congestion_ms = rtt_congestion_us / 1000;
  const congestion_score = clamp(100 - (congestion_ms / 100) * 100, 0, 100);
  const ploss_pct = ploss <= 1 ? ploss * 100 : ploss;
  const ploss_score = clamp(100 - (ploss_pct / 5) * 100, 0, 100);
  const jitter_ms = rtt_jitter_us / 1000;
  const jitter_score = clamp(100 - (jitter_ms / 50) * 100, 0, 100);
  const error_score = clamp(100 - (error_pct / 10) * 100, 0, 100);
  const composite =
    0.25 * throughput_score +
    0.25 * congestion_score +
    0.25 * ploss_score +
    0.15 * jitter_score +
    0.1 * error_score;
  return roundTo(clamp(composite, 0, 100), 1);
}


const UNKNOWN_CITY_VALUES = new Set(["", "?", "-", "(none)", "unknown", "n/a", "null"]);

export function isUnknownCity(city: string | null | undefined): boolean {
  return UNKNOWN_CITY_VALUES.has(String(city ?? "").trim().toLowerCase());
}

/**
 * Collapses duplicate (city, country) rows in the metro leaderboard by
 * summing request counts and computing request-weighted averages.
 */
export function deduplicateMetroLeaderboard(
  rows: readonly MetroRow[],
): MetroRow[] {
  const byKey = new Map<string, MetroRow & { city: string; country: string; reqs: number; total_reqs: number }>();
  const weightedFields = [
    "health_score",
    "rtt_med_us",
    "rtt_med_ms",
    "avg_ploss",
    "avg_ploss_pct",
    "error_pct",
  ] as const;

  for (const row of rows) {
    const city = String(row.city ?? row.metro ?? "").trim();
    const country = String(row.country ?? "").trim();
    if (isUnknownCity(city)) continue;
    const key = `${city.toLowerCase()}::${country.toLowerCase()}`;
    const reqs = Math.max(0, Number(row.reqs ?? row.total_reqs ?? 0));
    const existing = byKey.get(key);
    if (!existing) {
      const copy: MetroRow & {
        city: string;
        country: string;
        reqs: number;
        total_reqs: number;
      } = {
        ...row,
        city,
        country,
        reqs,
        total_reqs: reqs,
      };
      if ("metro" in row) {
        copy.metro = city;
      }
      byKey.set(key, copy);
    } else {
      const prevReqs = existing.reqs;
      const totalReqs = prevReqs + reqs;
      for (const field of weightedFields) {
        if (field in row || field in existing) {
          const prevVal = Number(existing[field] ?? 0);
          const curVal = Number(row[field] ?? 0);
          existing[field] =
            totalReqs > 0
              ? roundTo((prevVal * prevReqs + curVal * reqs) / totalReqs, 2)
              : 0;
        }
      }
      existing.reqs = totalReqs;
      existing.total_reqs = totalReqs;
    }
  }

  return [...byKey.values()]
    .sort((a, b) => b.reqs - a.reqs)
    .slice(0, 200);
}


export function normalizeProtocolBucket(raw: unknown): string {
  const val = String(raw ?? "").trim().toUpperCase();
  if (val === "HTTP/3" || val === "H3" || val === "3" || val === "3.0" || val.startsWith("HTTP/3")) {
    return "HTTP/3";
  }
  if (val === "HTTP/2" || val === "H2" || val === "2" || val === "2.0" || val === "HTTP/2.0") {
    return "HTTP/2";
  }
  if (val === "HTTP/1.1" || val === "1.1") {
    return "HTTP/1.1";
  }
  if (val === "HTTP/1.0" || val === "1.0") {
    return "HTTP/1.0";
  }
  return "Other";
}

export function normalizeProtocolDistribution(
  entries: readonly { protocol: unknown; requests: unknown }[],
  total: number,
): { protocol: string; requests: number; share_pct: number }[] {
  const counts = new Map<string, number>();
  for (const entry of entries) {
    const reqs = Number(entry.requests ?? 0);
    if (reqs <= 0) continue;
    const norm = normalizeProtocolBucket(entry.protocol);
    counts.set(norm, (counts.get(norm) ?? 0) + reqs);
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
    .map(([protocol, requests]) => ({
      protocol,
      requests,
      share_pct: total > 0 ? (requests * 100) / total : 0,
    }));
}

export function pickWorstSignificant<T extends { health_score: number | null; reqs: number }>(
  rows: readonly T[],
  totalReqs: number,
): T | undefined {
  const scored = rows.filter((row) => row.health_score != null);
  const sig = scored.filter((row) => row.reqs > totalReqs * 0.01);
  const pool = sig.length > 0 ? sig : scored;
  return [...pool].sort((a, b) => a.health_score! - b.health_score!)[0];
}

export function asnTrendFromBuckets(
  asnBuckets: readonly Record<string, unknown>[] | undefined,
  totalBucketCount: number,
  bucketSeconds: number,
  fallbackScore: number | null,
): { health_score_1h_ago: number | null; health_score_1w_ago: number | null; trend: "improving" | "degrading" | "stable" } {
  const sorted = [...(asnBuckets ?? [])].sort((a, b) =>
    String(a.bucket ?? "").localeCompare(String(b.bucket ?? "")),
  );
  const avg = (items: typeof sorted): number | null => {
    const scores = items
      .map((b) => b.health_score)
      .filter((s): s is number => typeof s === "number" && Number.isFinite(s));
    return scores.length ? roundTo(scores.reduce((sum, s) => sum + s, 0) / scores.length, 1) : null;
  };
  const lastScore = sorted.length ? sorted[sorted.length - 1].health_score : null;
  const latest = typeof lastScore === "number" && Number.isFinite(lastScore) ? lastScore : fallbackScore;
  const current = typeof fallbackScore === "number" && Number.isFinite(fallbackScore) ? fallbackScore : latest;
  let hs1h = latest;
  let hs1w = latest;
  if (totalBucketCount > 1 && sorted.length > 0) {
    const n1h = Math.max(1, Math.floor(3600 / Math.max(1, bucketSeconds)));
    hs1w = avg(sorted.slice(0, Math.max(1, Math.floor(sorted.length / 4))));
    hs1h = avg(sorted.slice(Math.max(0, sorted.length - n1h)));
  }
  let trend: "improving" | "degrading" | "stable" = "stable";
  if (current != null && hs1h != null) {
    const delta = current - hs1h;
    if (delta < -5) trend = "degrading";
    else if (delta > 5) trend = "improving";
  }
  return { health_score_1h_ago: hs1h, health_score_1w_ago: hs1w, trend };
}

export function computeGlobalHealthScoreFromLeaderboard(
  leaderboard: ReadonlyArray<{
    total_reqs?: number;
    reqs?: number;
    health_score_now?: number | null;
    health_score?: number | null;
  }>,
  fallbackScore: number | null,
): number {
  let weightedSum = 0;
  let totalWeight = 0;
  const scores: number[] = [];
  for (const row of leaderboard) {
    const score = row.health_score_now ?? row.health_score;
    if (typeof score !== "number" || !Number.isFinite(score)) continue;
    scores.push(score);
    const weight = Math.max(0, Number(row.total_reqs ?? row.reqs ?? 0));
    if (weight > 0) {
      weightedSum += score * weight;
      totalWeight += weight;
    }
  }
  if (totalWeight > 0) return roundTo(weightedSum / totalWeight, 1);
  if (scores.length > 0) return roundTo(scores.reduce((sum, s) => sum + s, 0) / scores.length, 1);
  return fallbackScore ?? 0;
}

export function emptyNetworkHealth(missingTcpFields: string[] = []) {
  return {
    available: true,
    has_data: false,
    leaderboard: [] as Record<string, unknown>[],
    buckets: [] as string[],
    heatmap: [] as Record<string, unknown>[],
    cities: [] as { name: string; lat: number | null; lon: number | null }[],
    map_buckets: [] as Record<string, unknown>[],
    has_metro: false,
    summary: {
      total_requests: 0,
      total_reqs: 0,
      avg_rtt_ms: 0,
      median_rtt_ms: 0,
      p95_rtt_ms: 0,
      avg_ploss_pct: 0,
      health_score: null as number | null,
      global_health_score: 0,
    },
    timeseries: [] as Record<string, unknown>[],
    by_country: [] as Record<string, unknown>[],
    by_asn: [] as Record<string, unknown>[],
    by_pop: [] as Record<string, unknown>[],
    metro_leaderboard: [] as MetroRow[],
    missing_tcp_fields: missingTcpFields,
  };
}

export function emptyNetworkQuality(missingTcpFields: string[] = [], regionCountry = "US") {
  return {
    available: false,
    by_country: [] as Record<string, unknown>[],
    by_pop: [] as Record<string, unknown>[],
    countries: [] as string[],
    region_country: regionCountry,
    summary: {
      total_requests: 0,
      avg_throughput_mbps: 0,
      avg_cwnd: 0,
      avg_rtt_ms: 0,
      avg_rttvar_ms: 0,
      retrans_rate_pct: 0,
    },
    scatter: [] as Record<string, unknown>[],
    by_region: [] as Record<string, unknown>[],
    by_asn: [] as Record<string, unknown>[],
    protocol_distribution: PROTOCOL_BUCKETS.map((protocol) => ({
      protocol,
      requests: 0,
      pct: 0,
      avg_rtt_ms: 0,
    })),
    missing_tcp_fields: missingTcpFields,
  };
}

export function emptyPopHealth() {
  return {
    data: [] as Record<string, unknown>[],
    pops: [] as Record<string, unknown>[],
    by_pop: [] as Record<string, unknown>[],
    elevated_count: 0,
    total_pops: 0,
  };
}

export function emptyNetworkTransport(missingFields: string[] = []) {
  return {
    has_transport_data: false,
    tcp_rtt_p50_ms: null as number | null,
    tcp_rtt_p90_ms: null as number | null,
    tcp_rtt_p95_ms: null as number | null,
    tcp_rtt_p99_ms: null as number | null,
    retrans_rate_pct: null as number | null,
    packet_loss_pct: null as number | null,
    delivery_rate_mbps: null as number | null,
    bandwidth_bytes: 0,
    total_requests: 0,
    protocol_distribution: normalizeProtocolDistribution([], 0),
    missing_fields: missingFields,
  };
}

export function emptyNetworkBundle(missingTcpFields: string[] = []) {
  const health = emptyNetworkHealth(missingTcpFields);
  const quality = emptyNetworkQuality(missingTcpFields);
  const transport = emptyNetworkTransport(missingTcpFields);
  return {
    available: false,
    health,
    quality,
    transport,
    network_health: health,
    network_quality: quality,
    pop_health: [] as Record<string, unknown>[],
    missing_tcp_fields: missingTcpFields,
  };
}

export function parseMetroCode(raw: unknown): number | null {
  if (raw == null) return null;
  const n = typeof raw === "bigint" ? Number(raw) : typeof raw === "number" ? raw : Number(String(raw).trim());
  if (!Number.isFinite(n)) return null;
  const code = Math.trunc(n);
  return code > 0 && code === n ? code : null;
}

/** Every request column the /network scans read, physical Cloud names and their aliases alike. */
export const NETWORK_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "service_id", "status", "cache", "cache_status", "resp_bytes", "resp_body_size",
  "elapsed", "time_elapsed_usec", "proto", "protocol", "pop", "fastly_pop", "edge", "rid", "prid",
  "ttfb", "ttfb_ms", "ottfb", "country", "geo_country", "city", "geo_city", "region", "geo_region",
  "lat", "lon", "metro", "asn", "client_asn", "as_name", "tcp_rtt", "tcpi_rtt", "client_rtt_us", "client_tcpi_rtt",
  "ploss", "client_ploss", "client_cwnd", "rtt_var", "tcpi_rtt_var", "client_tcpi_rttvar",
  "retrans", "client_tcpi_delta_retrans", "delivery_rate", "tcpi_delivery_rate",
] as const;

export type NetworkScan = "health" | "map" | "shielding" | "quality" | "pop_health" | "transport";

/**
 * The /network-quality figures v3 summaries written before the `quality` rows
 * lack: the average throughput, the region table and the client-only ASN loss
 * and RTT variance. A raw read of only these returns `{ quality_parts }`.
 */
export const QUALITY_PARTS = ["throughput", "region", "asn_client"] as const;
export type QualityPart = typeof QUALITY_PARTS[number];

function qualityParts(value: unknown): QualityPart[] | null {
  return Array.isArray(value) && value.length > 0 &&
    value.every((part) => (QUALITY_PARTS as readonly unknown[]).includes(part)) ? value as QualityPart[] : null;
}

export type NetworkQualityParts = {
  throughput?: { avg_throughput_mbps: number | null };
  region?: { value: string; rtt_ms: number; reqs: number; label: string }[];
  asn_client?: { asn: number; avg_rttvar_us: number | null; avg_ploss: number | null }[];
};

const MAP_SECTIONS = new Set(["buckets", "cities", "map_buckets"]);

/** Sections that need only the map's buckets and cities (and maybe shielding): no core grouping sets. */
function mapOnly(sections?: readonly string[]): boolean {
  return Boolean(sections?.length) &&
    sections!.every((section) => MAP_SECTIONS.has(section) || section === "shielding_analysis") &&
    sections!.some((section) => section === "cities" || section === "map_buckets");
}

/**
 * The raw scans a /network read runs (see `handleNetworkRequest`), or null when
 * its section list isn't one: then it may run any of them.
 */
export function networkScans(path: string, sections: unknown): NetworkScan[] | null {
  if (sections != null && !(Array.isArray(sections) && sections.every((section) => typeof section === "string"))) return null;
  const list = sections as readonly string[] | undefined;
  const health = (bundle: boolean): NetworkScan[] => !list?.length ? ["health", "shielding", "pop_health"]
    : wantCore(list) ? [mapOnly(list) ? "map" : "health", ...wantShielding(list) ? ["shielding" as const] : []]
    : !bundle || wantShielding(list) ? ["shielding"] : [];
  if (path === "/api/network-health") return health(false);
  if (path === "/api/network-quality") return ["quality"];
  if (path === "/api/network/pop-health") return ["pop_health"];
  if (path !== "/api/network/bundle" || !list?.length) return ["health", "shielding", "quality", "pop_health", "transport"];
  return [...health(true), ...(["quality", "pop_health", "transport"] as const).filter((scan) => list.includes(scan))];
}

/**
 * The columns a projected /network load needs: the canonical NETWORK_SCAN_COLUMNS
 * superset plus any dynamic filter columns, so concurrent /network requests share
 * one `file.scopedKey` and DuckDB registration. Null when the filters can't name theirs.
 */
export function networkScanColumns(_path: string, params: Record<string, unknown>): string[] | null {
  let filters: unknown = params.filters;
  if (typeof filters === "string") {
    const trimmed = filters.trim();
    if (!trimmed) {
      filters = null;
    } else {
      try {
        filters = JSON.parse(trimmed);
      } catch {
        return null;
      }
    }
  }
  if (filters != null && (typeof filters !== "object" || Array.isArray(filters))) return null;
  return [...new Set([
    ...NETWORK_SCAN_COLUMNS,
    ...filters && typeof filters === "object" ? Object.keys(filters) : [],
  ])];
}

function buildWhereClause(params: NetworkQueryParams): string {
  const clauses: string[] = [rangeServiceSql(params)];
  if (params.filter_country) {
    clauses.push(`COALESCE(geo_country, country) = ${duckSqlString(String(params.filter_country))}`);
  }
  if (params.filter_asn !== undefined && params.filter_asn !== "") {
    const asnNum = Number(params.filter_asn);
    if (Number.isFinite(asnNum)) {
      clauses.push(`COALESCE(client_asn, asn) = ${Math.floor(asnNum)}`);
    }
  }
  if (params.filter_pop) {
    clauses.push(`COALESCE(fastly_pop, pop) = ${duckSqlString(String(params.filter_pop))}`);
  }
  clauses.push(analyticsFiltersToSql(params.filters, params.customColumnTypes));
  return clauses.join(" AND ");
}

async function queryNetworkBuckets(
  params: NetworkQueryParams,
  where: string,
  engine: Pick<DuckDBAnalyticalEngine, "executeSql">,
  /** False groups buckets without ASNs, for the map alone (and its `map` projection). */
  heatmap = true,
) {
  const bucketMs = Math.max(1, Math.min(14400, Math.floor(params.bucket_seconds ?? 300))) * 1000;
  const topN = Math.max(1, Math.min(100, Math.floor(params.top_n ?? 30)));
  const bucket = `(floor(ts_ms / ${bucketMs}) * ${bucketMs})::BIGINT`;
  const mapAsn = params.map_asn && params.map_asn !== "all" ? Number(params.map_asn) : null;
  const mapAsnFilter = mapAsn !== null && Number.isFinite(mapAsn) ? Math.floor(mapAsn) : null;
  // The ASN heatmap and city map share one pass; with a map ASN chosen, only its rows make city groups.
  const rows = await engine.executeSql<Record<string, unknown>>(`WITH scoped AS (
      SELECT ${bucket} AS bucket_ms,
        ${heatmap ? "COALESCE(client_asn, asn, 0)" : "0"} AS asn_key,
        COALESCE(geo_city, city) AS city_key,
        COALESCE(geo_country, country) AS country_key,
        ${mapAsnFilter === null ? "1" : `CASE WHEN COALESCE(client_asn, asn, 0) = ${mapAsnFilter} THEN 1 END`} AS map_key,
        lat, lon,
        CASE WHEN TRY_CAST(metro AS BIGINT) > 0 THEN TRY_CAST(metro AS BIGINT) END AS metro_code,
        COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt) AS rtt_us,
        COALESCE(client_ploss, ploss) AS ploss_value,
        status
      FROM requests WHERE ${where}
    ), grouped AS (
      SELECT GROUPING(asn_key) = 0 AS is_asn,
        bucket_ms, asn_key AS asn, city_key AS city, country_key AS country, map_key,
        AVG(lat) AS lat, AVG(lon) AS lon,
        MIN(metro_code) AS metro_code,
        COUNT(*)::BIGINT AS reqs,
        QUANTILE_CONT(rtt_us, [0.5, 0.1]) AS rtt_quantiles,
        AVG(ploss_value) AS avg_ploss,
        COUNT(*) FILTER (WHERE status >= 500) * 100.0 / COUNT(*) AS error_pct
      FROM scoped
      GROUP BY GROUPING SETS ((bucket_ms, asn_key), (bucket_ms, city_key, country_key, map_key))
    )
    SELECT is_asn, bucket_ms, asn, city, country, map_key, lat, lon, metro_code, reqs,
      rtt_quantiles[1] AS rtt_med_us, rtt_quantiles[2] AS baseline, avg_ploss, error_pct
    FROM grouped ORDER BY bucket_ms, reqs DESC`, [], params.signal);
  const asnRows = rows.filter((row) => row.is_asn && Number(row.asn ?? 0) > 0);
  const cityRows = rows.filter(
    (row) => !row.is_asn && row.city != null && row.map_key != null && !isUnknownCity(String(row.city)),
  );
  const buckets = [...new Set([...asnRows, ...cityRows].map((row) => new Date(Number(row.bucket_ms)).toISOString()))].sort();
  const bucketIndexes = new Map(buckets.map((value, index) => [value, index]));
  const cell = (row: Record<string, unknown>) => ({
    reqs: Number(row.reqs),
    rtt_med_us: row.rtt_med_us == null ? null : Number(row.rtt_med_us),
    avg_ploss: row.avg_ploss == null ? null : Number(row.avg_ploss),
    error_pct: Number(row.error_pct),
    health_score: row.rtt_med_us == null ? null : computeNetworkHealthScore(
      Number(row.rtt_med_us), Math.max(1, Number(row.baseline)), Number(row.avg_ploss ?? 0), Number(row.error_pct),
    ),
  });
  const byAsn = new Map<number, { asn: number; label: string; total_reqs: number; buckets: Record<string, unknown>[] }>();
  for (const row of asnRows) {
    const asn = Number(row.asn);
    const entry = byAsn.get(asn) ?? { asn, label: formatAsnDisplayLabel(asn), total_reqs: 0, buckets: [] };
    const bucket = new Date(Number(row.bucket_ms)).toISOString();
    entry.total_reqs += Number(row.reqs);
    entry.buckets.push({ ...cell(row), bucket, bucket_idx: bucketIndexes.get(bucket) });
    byAsn.set(asn, entry);
  }

  const cities: { name: string; lat: number | null; lon: number | null }[] = [];
  const cityIndexes = new Map<string, number>();
  const mapBuckets = new Map<string, { bucket: string; bucket_idx: number; cities: Record<string, unknown>[] }>();
  for (const row of cityRows) {
    const key = JSON.stringify([row.country, row.city]);
    if (!cityIndexes.has(key)) {
      cityIndexes.set(key, cities.length);
      cities.push({ name: String(row.city), lat: row.lat == null ? null : Number(row.lat), lon: row.lon == null ? null : Number(row.lon) });
    }
    const bucket = new Date(Number(row.bucket_ms)).toISOString();
    const entry = mapBuckets.get(bucket) ?? { bucket, bucket_idx: bucketIndexes.get(bucket)!, cities: [] };
    entry.cities.push({
      ...cell(row),
      city_idx: cityIndexes.get(key),
      country: row.country,
      metro_code: parseMetroCode(row.metro_code),
    });
    mapBuckets.set(bucket, entry);
  }
  return {
    buckets, cities, map_buckets: [...mapBuckets.values()],
    heatmap: heatmap ? [...byAsn.values()].sort((a, b) => b.total_reqs - a.total_reqs).slice(0, topN) : [],
  };
}

async function queryNetworkShielding(
  params: NetworkQueryParams,
  where: string,
  engine: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">,
) {
  const enabled = await cloudActiveLogFieldIds(params.service_id, params.signal ?? new AbortController().signal);
  const missing = ["edge", "rid", "prid", "pop"].filter((field) => !enabled.has(field));
  if (!enabled.has("ottfb") && !enabled.has("ttfb")) missing.push("ttfb");
  if (missing.length) {
    return { has_data: false, requires_fields: missing, rows: [], total_routes: 0, truncated: false };
  }
  const timeWhere = buildWhereClause({
    start_time: params.start_time, end_time: params.end_time, service_id: params.service_id,
  });
  const rows = await engine.executeSql<Record<string, unknown>>(
    buildOriginShieldingAnalysisSql("requests", where, timeWhere, params.top_n ?? 30),
    [], params.signal,
  );
  const shaped = rows.map((row) => enrichShieldingRouteWithPopDistance(row));
  const total = Number(rows[0]?.total_routes ?? 0);
  return {
    has_data: shaped.length > 0, rows: shaped, total_routes: total, truncated: total > shaped.length,
  };
}

async function detectMissingTcpFields(
  params: Pick<NetworkQueryParams, "service_id" | "signal">,
): Promise<{ missingTcpFields: string[]; hasRtt: boolean }> {
  const enabled = await cloudActiveLogFieldIds(
    params.service_id,
    params.signal ?? new AbortController().signal,
  );
  return {
    missingTcpFields: missingTcpFieldsFromActiveIds(enabled),
    hasRtt: hasTcpRttField(enabled),
  };
}

// Same section gates as the backend, so a page's split requests don't repeat each other's scans.
const CORE_SECTIONS = new Set(["summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"]);

function wantCore(sections?: readonly string[]): boolean {
  return !sections?.length || sections.includes("health") || sections.some((section) => CORE_SECTIONS.has(section));
}

function wantShielding(sections?: readonly string[]): boolean {
  return !sections?.length || sections.includes("health") || sections.includes("shielding_analysis");
}

async function queryNetworkHealthSections(params: NetworkQueryParams) {
  if (wantCore(params.sections)) return queryNetworkHealth(params);
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  return { available: true, shielding_analysis: await queryNetworkShielding(params, buildWhereClause(params), engine) };
}

export async function queryNetworkHealth(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const { missingTcpFields } = await detectMissingTcpFields(params);
  const where = buildWhereClause(params);
  if (mapOnly(params.sections)) {
    // The core tables' keys come back empty, as when there are no rows.
    const bucketData = await queryNetworkBuckets(params, where, engine, false);
    return {
      ...emptyNetworkHealth(missingTcpFields),
      has_data: bucketData.buckets.length > 0,
      ...bucketData,
      shielding_analysis: wantShielding(params.sections)
        ? await queryNetworkShielding(params, where, engine) : undefined,
    };
  }

  // One pass for the summary and the country, ASN and metro tables; quantiles stay exact per grouping set.
  const asnNamesReady = loadAsnNames();
  const coreRows = await engine.executeSql<Record<string, unknown>>(
    `WITH scoped AS (
      SELECT
        COALESCE(geo_country, country, 'Unknown') AS country_key,
        COALESCE(client_asn, asn, 0)::BIGINT AS asn_key,
        COALESCE(as_name, 'AS' || CAST(COALESCE(client_asn, asn, 0) AS VARCHAR)) AS as_name_key,
        COALESCE(geo_city, city, 'Unknown') AS city_key,
        COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt) AS rtt_us,
        COALESCE(client_ploss, ploss) AS ploss_value,
        status
      FROM requests
      WHERE ${where}
    ), grouped AS (
      SELECT
        CASE
          WHEN GROUPING(country_key) = 1 AND GROUPING(asn_key) = 1 AND GROUPING(city_key) = 1 THEN 'summary'
          WHEN GROUPING(asn_key) = 0 THEN 'asn'
          WHEN GROUPING(city_key) = 0 THEN 'metro'
          ELSE 'country'
        END AS grouping_set,
        country_key, asn_key, as_name_key, city_key,
        COUNT(*)::BIGINT AS reqs,
        AVG(rtt_us)::DOUBLE AS avg_rtt_us,
        QUANTILE_CONT(rtt_us, [0.5, 0.95, 0.99, 0.1]) AS rtt_quantiles,
        AVG(ploss_value)::DOUBLE AS avg_ploss,
        (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
      FROM scoped
      GROUP BY GROUPING SETS ((), (country_key), (asn_key, as_name_key), (city_key, country_key))
    )
    SELECT
      grouping_set, country_key, asn_key, as_name_key, city_key, reqs, avg_rtt_us,
      rtt_quantiles[1]::DOUBLE AS med_rtt_us,
      rtt_quantiles[2]::DOUBLE AS p95_rtt_us,
      rtt_quantiles[3]::DOUBLE AS p99_rtt_us,
      rtt_quantiles[4]::DOUBLE AS p10_rtt_us,
      avg_ploss, error_pct
    FROM grouped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY grouping_set ORDER BY reqs DESC)
      <= CASE grouping_set WHEN 'metro' THEN 400 WHEN 'summary' THEN 1 ELSE 100 END
    ORDER BY grouping_set, reqs DESC`,
    [],
    params.signal,
  );
  const coreSet = (name: string) => coreRows.filter((row) => row.grouping_set === name);
  const s = coreSet("summary")[0];

  const totalRequests = Number(s?.reqs ?? 0);
  if (totalRequests === 0) {
    return emptyNetworkHealth(missingTcpFields);
  }

  const medRttUs = Number(s.med_rtt_us ?? 0);
  const baseRttUs = Math.max(1, Number(s.p10_rtt_us ?? medRttUs ?? 1));
  const avgPloss = Number(s.avg_ploss ?? 0);
  const errorPct = Number(s.error_pct ?? 0);
  const healthScore = s.med_rtt_us == null ? null : computeNetworkHealthScore(
    medRttUs,
    baseRttUs,
    avgPloss,
    errorPct,
  );

  const byCountryRows = coreSet("country").map((row): Record<string, unknown> => ({
    ...row, country: row.country_key, total_reqs: row.reqs, rtt_med_us: row.med_rtt_us, rtt_base_us: row.p10_rtt_us,
  }));

  const by_country = byCountryRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(1, Number(row.rtt_base_us ?? rttMedUs ?? 1));
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    return {
      country: String(row.country ?? "Unknown"),
      reqs: Number(row.reqs ?? 0),
      total_reqs: Number(row.total_reqs ?? 0),
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      avg_ploss: roundTo(ploss, 4),
      avg_ploss_pct: roundTo(ploss <= 1 ? ploss * 100 : ploss, 2),
      error_pct: roundTo(errPct, 2),
      health_score: row.rtt_med_us == null ? null : computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const byAsnRows = coreSet("asn")
    .filter((row) => Number(row.asn_key ?? 0) > 0)
    .map((row): Record<string, unknown> => ({
      ...row, asn: row.asn_key, as_name: row.as_name_key, rtt_med_us: row.med_rtt_us, rtt_base_us: row.p10_rtt_us,
    }));

  await asnNamesReady;

  const by_asn = byAsnRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(1, Number(row.rtt_base_us ?? rttMedUs ?? 1));
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    const asnNum = Number(row.asn ?? 0);
    return {
      asn: asnNum,
      as_name: formatAsnDisplayLabel(asnNum, row.as_name == null ? null : String(row.as_name)),
      reqs: Number(row.reqs ?? 0),
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      p95_rtt_us: row.p95_rtt_us == null ? null : roundTo(Number(row.p95_rtt_us), 1),
      p99_rtt_us: row.p99_rtt_us == null ? null : roundTo(Number(row.p99_rtt_us), 1),
      avg_ploss: roundTo(ploss, 4),
      error_pct: roundTo(errPct, 2),
      health_score: row.rtt_med_us == null ? null : computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const metroRawRows = coreSet("metro").map((row) => ({
    ...row, city: row.city_key, country: row.country_key, rtt_med_us: row.med_rtt_us, rtt_base_us: row.p10_rtt_us,
  })) as unknown as MetroRow[];

  const scoredMetros: MetroRow[] = metroRawRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(
      1,
      Number((row as Record<string, unknown>).rtt_base_us ?? rttMedUs ?? 1),
    );
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    const reqs = Number(row.reqs ?? 0);
    return {
      city: String(row.city ?? "Unknown"),
      country: String(row.country ?? "Unknown"),
      raw_city: String(row.city ?? "Unknown"),
      region: "",
      reqs,
      total_reqs: reqs,
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      avg_ploss: roundTo(ploss, 4),
      avg_ploss_pct: roundTo(ploss <= 1 ? ploss * 100 : ploss, 2),
      error_pct: roundTo(errPct, 2),
      health_score: row.rtt_med_us == null ? null : computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const metro_leaderboard = deduplicateMetroLeaderboard(scoredMetros).map((row) => ({
    ...row,
    city: formatCityCountryLabel(String(row.raw_city ?? row.city ?? ""), String(row.country ?? "")),
  }));
  const popHealth = params.sections?.length ? null : await queryPopHealth(params);
  const bucketData = await queryNetworkBuckets(params, where, engine);
  const shielding = wantShielding(params.sections)
    ? await queryNetworkShielding(params, where, engine) : undefined;
  const bucketSeconds = Math.max(1, Math.min(14400, Math.floor(params.bucket_seconds ?? 300)));
  const heatmapByAsn = new Map<number, readonly Record<string, unknown>[]>(
    bucketData.heatmap.map((entry) => [entry.asn, entry.buckets]),
  );
  const worstAsn = pickWorstSignificant(by_asn, totalRequests);
  const worstCountry = pickWorstSignificant(by_country, totalRequests);

  const leaderboard = by_asn.slice(0, Math.max(1, Math.min(100, params.top_n ?? 30))).map((row) => ({
    ...row, label: row.as_name, total_reqs: row.reqs,
    health_score_now: row.health_score,
    ...asnTrendFromBuckets(heatmapByAsn.get(row.asn), bucketData.buckets.length, bucketSeconds, row.health_score),
  }));
  const globalHealthScore = computeGlobalHealthScoreFromLeaderboard(leaderboard, healthScore);

  return {
    available: true,
    has_data: true,
    ...bucketData,
    shielding_analysis: shielding,
    has_metro: metro_leaderboard.length > 0,
    leaderboard,
    summary: {
      total_requests: totalRequests,
      total_reqs: totalRequests,
      global_health_score: globalHealthScore,
      worst_asn: worstAsn ? { label: worstAsn.as_name, score: worstAsn.health_score } : null,
      worst_country: worstCountry ? { label: resolveCountryName(worstCountry.country), score: worstCountry.health_score } : null,
      avg_rtt_ms: roundTo(Number(s.avg_rtt_us ?? 0) / 1000, 2),
      median_rtt_ms: roundTo(medRttUs / 1000, 2),
      p95_rtt_ms: roundTo(Number(s.p95_rtt_us ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo(avgPloss <= 1 ? avgPloss * 100 : avgPloss, 2),
      health_score: healthScore,
    },
    timeseries: [] as Record<string, unknown>[],
    by_country,
    by_asn,
    ...popHealth ? { by_pop: popHealth.by_pop } : {},
    metro_leaderboard,
    missing_tcp_fields: missingTcpFields,
  };
}

/**
 * Only the named /network-quality parts, computed as `queryNetworkQuality` does
 * and over only their own columns (`QUALITY_PART_COLUMNS`).
 */
async function queryNetworkQualityParts(
  params: NetworkQueryParams,
  parts: readonly QualityPart[],
): Promise<NetworkQualityParts> {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildWhereClause(params);
  const out: NetworkQualityParts = {};
  if (parts.includes("throughput")) {
    const [row] = await engine.executeSql<Record<string, unknown>>(`SELECT AVG(CASE WHEN COALESCE(time_elapsed_usec, elapsed) > 0
        THEN (COALESCE(resp_body_size, resp_bytes, 0) * 8.0) / COALESCE(time_elapsed_usec, elapsed) END)::DOUBLE AS tp
      FROM requests WHERE ${where}`, [], params.signal);
    out.throughput = { avg_throughput_mbps: row?.tp == null ? null : Number(row.tp) };
  }
  if (parts.includes("region")) {
    const regionCountrySql = duckSqlString(String(params.region_country ?? "US"));
    const rows = await engine.executeSql<Record<string, unknown>>(`SELECT
        COALESCE(geo_region, region, 'Unknown') AS value,
        AVG(COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt)) / 1000.0 AS rtt_ms,
        COUNT(*)::BIGINT AS reqs
      FROM requests WHERE ${where} AND COALESCE(geo_country, country) = ${regionCountrySql}
      GROUP BY 1 ORDER BY reqs DESC`, [], params.signal);
    out.region = rows
      .filter((row) => row.rtt_ms != null)
      .map((row) => ({
        value: String(row.value),
        rtt_ms: roundTo(Number(row.rtt_ms), 2),
        reqs: Number(row.reqs),
        label: String(row.value),
      }));
  }
  if (parts.includes("asn_client")) {
    const asns = (params.quality_asns ?? []).map(Number).filter(Number.isFinite).map(Math.floor);
    const rows = asns.length ? await engine.executeSql<Record<string, unknown>>(`SELECT
        COALESCE(client_asn, asn, 0)::BIGINT AS asn,
        AVG(client_tcpi_rttvar)::DOUBLE AS avg_rttvar_us,
        AVG(COALESCE(client_ploss, ploss))::DOUBLE AS avg_ploss
      FROM requests WHERE ${where} AND COALESCE(client_asn, asn, 0) IN (${asns.join(", ")})
        AND (client_tcpi_rttvar IS NOT NULL OR client_ploss IS NOT NULL OR ploss IS NOT NULL)
      GROUP BY 1`, [], params.signal) : [];
    out.asn_client = rows.map((row) => ({
      asn: Number(row.asn),
      avg_rttvar_us: row.avg_rttvar_us == null ? null : Number(row.avg_rttvar_us),
      avg_ploss: row.avg_ploss == null ? null : Number(row.avg_ploss),
    }));
  }
  return out;
}

export async function queryNetworkQuality(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const { missingTcpFields, hasRtt } = await detectMissingTcpFields(params);
  const where = buildWhereClause(params);

  const regionCountrySql = duckSqlString(String(params.region_country ?? "US"));
  const asnNamesReady = loadAsnNames();
  // One pass for the summary and every grouped table except the scatter sample.
  const qualityRows = await engine.executeSql<Record<string, unknown>>(
    `WITH scoped AS (
      SELECT
        COALESCE(geo_country, country, 'Unknown') AS country_key,
        COALESCE(client_asn, asn, 0)::BIGINT AS asn_key,
        COALESCE(as_name, 'AS' || CAST(COALESCE(client_asn, asn, 0) AS VARCHAR)) AS as_name_key,
        COALESCE(protocol, proto, 'Other') AS proto_key,
        COALESCE(fastly_pop, pop) AS pop_key,
        CASE WHEN COALESCE(geo_country, country) = ${regionCountrySql}
          THEN COALESCE(geo_region, region, 'Unknown') END AS region_key,
        COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt) AS rtt_us,
        client_cwnd, client_tcpi_rttvar, COALESCE(client_ploss, ploss) AS ploss_value,
        COALESCE(client_tcpi_rttvar, rtt_var, tcpi_rtt_var) AS rttvar_us,
        COALESCE(client_tcpi_delta_retrans, retrans, 0) AS retrans_value,
        CASE WHEN COALESCE(time_elapsed_usec, elapsed) > 0 THEN (COALESCE(resp_body_size, resp_bytes, 0) * 8.0) / COALESCE(time_elapsed_usec, elapsed) ELSE NULL END AS throughput_mbps
      FROM requests
      WHERE ${where}
    ), grouped AS (
      SELECT
        CASE
          WHEN GROUPING(country_key) = 0 THEN 'region'
          WHEN GROUPING(asn_key) = 0 THEN 'asn'
          WHEN GROUPING(proto_key) = 0 THEN 'proto'
          WHEN GROUPING(pop_key) = 0 THEN 'pop'
          WHEN GROUPING(region_key) = 0 THEN 'region_country'
          ELSE 'summary'
        END AS grouping_set,
        country_key, asn_key, as_name_key, proto_key, pop_key, region_key,
        COUNT(*)::BIGINT AS requests,
        COUNT(*) AS reqs,
        AVG(client_cwnd)::DOUBLE AS avg_cwnd,
        AVG(rtt_us)::DOUBLE AS avg_rtt_us,
        AVG(rtt_us) / 1000.0 AS rtt_ms,
        AVG(rttvar_us)::DOUBLE AS avg_any_rttvar_us,
        AVG(client_tcpi_rttvar)::DOUBLE AS avg_rttvar_us,
        AVG(ploss_value)::DOUBLE AS avg_ploss,
        (COUNT(*) FILTER (WHERE retrans_value > 0) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS retrans_rate_pct,
        AVG(throughput_mbps)::DOUBLE AS avg_throughput_mbps
      FROM scoped
      GROUP BY GROUPING SETS ((), (country_key), (asn_key, as_name_key), (proto_key), (pop_key), (region_key))
    )
    SELECT * FROM grouped
    WHERE grouping_set <> 'region_country' OR region_key IS NOT NULL
    QUALIFY grouping_set NOT IN ('region', 'asn') OR ROW_NUMBER() OVER (PARTITION BY grouping_set ORDER BY requests DESC) <= 100
    ORDER BY grouping_set, requests DESC`,
    [],
    params.signal,
  );
  const qualitySet = (name: string) => qualityRows.filter((row) => row.grouping_set === name);
  const summaryRow = qualitySet("summary")[0];

  const totalRequests = Number(summaryRow?.requests ?? 0);
  if (totalRequests === 0) {
    return { ...emptyNetworkQuality(missingTcpFields, String(params.region_country ?? "US")), available: hasRtt };
  }

  await asnNamesReady;

  const byRegionRows = qualitySet("region").map((row): Record<string, unknown> => ({ ...row, region: row.country_key }));

  const by_region = byRegionRows.map((r) => ({
    region: String(r.region ?? "Unknown"),
    requests: Number(r.requests ?? 0),
    avg_rtt_ms: roundTo(Number(r.avg_rtt_us ?? 0) / 1000, 2),
    avg_rttvar_ms: roundTo(Number(r.avg_rttvar_us ?? 0) / 1000, 2),
    avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
    avg_cwnd: roundTo(Number(r.avg_cwnd ?? 0), 1),
  }));

  const byAsnRows = qualitySet("asn")
    .filter((row) => Number(row.asn_key ?? 0) > 0)
    .map((row): Record<string, unknown> => ({ ...row, asn: row.asn_key, as_name: row.as_name_key }));

  const by_asn = byAsnRows.map((r) => {
    const asnNum = Number(r.asn ?? 0);
    return {
      asn: asnNum,
      as_name: formatAsnDisplayLabel(asnNum, r.as_name == null ? null : String(r.as_name)),
      requests: Number(r.requests ?? 0),
      avg_rtt_ms: roundTo(Number(r.avg_rtt_us ?? 0) / 1000, 2),
      avg_rttvar_ms: roundTo(Number(r.avg_rttvar_us ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
      avg_cwnd: roundTo(Number(r.avg_cwnd ?? 0), 1),
    };
  });

  const scatter = await engine.executeSql<Record<string, unknown>>(`SELECT
    ROUND(COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt) / 1000.0, 2) AS rtt_ms,
    ROUND(COALESCE(ttfb_ms, ttfb * 1000.0), 2) AS ttfb_ms,
    COALESCE(NULLIF(TRIM(COALESCE(cache_status, cache, '')), ''), 'UNKNOWN') AS cache FROM requests
    WHERE ${where} AND COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt) IS NOT NULL
      AND COALESCE(ttfb_ms, ttfb * 1000.0) IS NOT NULL LIMIT 2000`, [], params.signal);

  const protoRows = qualitySet("proto").map((row) => ({
    proto: row.proto_key as string | null, requests: Number(row.requests), avg_rtt_us: row.avg_rtt_us as number | null,
  }));

  const protoMap = new Map<
    string,
    { requests: number; rttWeightedUs: number }
  >();
  for (const b of PROTOCOL_BUCKETS) {
    protoMap.set(b, { requests: 0, rttWeightedUs: 0 });
  }
  for (const row of protoRows) {
    const bucket = normalizeProtocolBucket(row.proto);
    const cur = protoMap.get(bucket)!;
    const reqs = Number(row.requests ?? 0);
    const rtt = Number(row.avg_rtt_us ?? 0);
    cur.requests += reqs;
    cur.rttWeightedUs += rtt * reqs;
  }

  const protocol_distribution = PROTOCOL_BUCKETS.map((protocol) => {
    const entry = protoMap.get(protocol)!;
    return {
      protocol,
      requests: entry.requests,
      pct:
        totalRequests > 0
          ? roundTo((entry.requests * 100) / totalRequests, 2)
          : 0,
      avg_rtt_ms:
        entry.requests > 0
          ? roundTo(entry.rttWeightedUs / entry.requests / 1000, 2)
          : 0,
    };
  });

  const s = { ...summaryRow, avg_rttvar_us: summaryRow.avg_any_rttvar_us } as Record<string, unknown>;
  const bar = (value: string, label: string, row: { avg_rtt_ms: number; requests: number }) => ({
    value, label, rtt_ms: row.avg_rtt_ms, reqs: row.requests,
  });
  const byPopRows = qualitySet("pop")
    .filter((row) => row.rtt_ms != null)
    .map((row) => ({ pop: row.pop_key, rtt_ms: roundTo(Number(row.rtt_ms), 2), reqs: row.reqs }));
  const regionRows = qualitySet("region_country")
    .filter((row) => row.rtt_ms != null)
    .map((row) => ({ value: row.region_key, rtt_ms: roundTo(Number(row.rtt_ms), 2), reqs: row.reqs }));
  return {
    available: hasRtt,
    by_country: by_region.map((row) => bar(row.region, resolveCountryName(row.region), row)),
    by_pop: byPopRows.map((row) => ({ ...row, value: String(row.pop ?? "Unknown"), label: String(row.pop ?? "Unknown") })),
    countries: by_region.map((row) => row.region),
    region_country: params.region_country ?? "US",
    summary: {
      total_requests: totalRequests,
      avg_throughput_mbps: roundTo(Number(s.avg_throughput_mbps ?? 0), 2),
      avg_cwnd: roundTo(Number(s.avg_cwnd ?? 0), 1),
      avg_rtt_ms: roundTo(Number(s.avg_rtt_us ?? 0) / 1000, 2),
      avg_rttvar_ms: roundTo(Number(s.avg_rttvar_us ?? 0) / 1000, 2),
      retrans_rate_pct: roundTo(Number(s.retrans_rate_pct ?? 0), 2),
    },
    scatter,
    by_region: regionRows.map((row) => ({ ...row, label: String(row.value) })),
    by_asn: by_asn.map((row) => ({ ...row, ...bar(String(row.asn), row.as_name, row) })),
    protocol_distribution,
    missing_tcp_fields: missingTcpFields,
  };
}

export async function queryPopHealth(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildWhereClause(params);

  const rows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(fastly_pop, pop, 'Unknown') AS pop,
      COUNT(*)::BIGINT AS requests,
      COUNT(*) FILTER (WHERE status >= 500) AS errors,
      COUNT(*) FILTER (WHERE starts_with(UPPER(COALESCE(cache_status, cache)), 'HIT')) * 100.0 / COUNT(*) AS cache_hit_rate,
      SUM(COALESCE(resp_body_size, resp_bytes, 0)) AS bandwidth_bytes,
      QUANTILE_CONT(COALESCE(ttfb_ms, ttfb * 1000.0), 0.95) AS p95_ttfb_ms,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt), 0.5)::DOUBLE AS p50_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt), 0.95)::DOUBLE AS p95_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, tcp_rtt, tcpi_rtt), 0.1)::DOUBLE AS baseline_p50_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}
    GROUP BY 1
    ORDER BY requests DESC
    LIMIT 200`,
    [],
    params.signal,
  );

  if (rows.length === 0) {
    return emptyPopHealth();
  }

  let elevatedCount = 0;
  const pops = rows.map((r) => {
    const p50Ms = Number(r.p50_us ?? 0) / 1000;
    const p95Ms = Number(r.p95_us ?? 0) / 1000;
    const baselineMs = Math.max(0.1, Number(r.baseline_p50_us ?? 0) / 1000);
    const isElevated = baselineMs > 0 && p50Ms > baselineMs * 1.5;
    if (isElevated) elevatedCount++;
    return {
      pop: String(r.pop ?? "Unknown"),
      requests: Number(r.requests ?? 0),
      errors: Number(r.errors ?? 0),
      error_rate: Number(r.error_pct ?? 0),
      bandwidth_bytes: Number(r.bandwidth_bytes ?? 0),
      cache_hit_rate: Number(r.cache_hit_rate ?? 0),
      p50_rtt_us: r.p50_us == null ? null : Number(r.p50_us),
      p95_ttfb_ms: r.p95_ttfb_ms == null ? null : Number(r.p95_ttfb_ms),
      p50_ms: roundTo(p50Ms, 2),
      p95_ms: roundTo(p95Ms, 2),
      baseline_p50_ms: roundTo(baselineMs, 2),
      avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
      error_pct: roundTo(Number(r.error_pct ?? 0), 2),
      is_elevated: isElevated,
    };
  });

  return {
    data: pops,
    pops,
    by_pop: pops,
    elevated_count: elevatedCount,
    total_pops: pops.length,
  };
}

async function queryNetworkTransport(params: NetworkQueryParams) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildWhereClause(params);
  // The summary and the protocol split share one pass.
  const rows = await engine.executeSql<Record<string, unknown>>(`WITH scoped AS (
      SELECT COALESCE(proto, protocol, 'Unknown') AS protocol_key,
        COALESCE(resp_body_size, resp_bytes, 0) AS bytes,
        COALESCE(tcp_rtt, client_rtt_us, client_tcpi_rtt, tcpi_rtt) AS rtt_us,
        COALESCE(client_ploss, ploss) AS ploss_value,
        COALESCE(retrans, client_tcpi_delta_retrans) AS retrans_value,
        COALESCE(delivery_rate, tcpi_delivery_rate) AS delivery_rate_value
      FROM requests WHERE ${where}
    ), grouped AS (
      SELECT GROUPING(protocol_key) = 0 AS is_protocol, protocol_key,
        COUNT(*) AS total_requests,
        SUM(bytes) AS bandwidth_bytes,
        QUANTILE_CONT(rtt_us, [0.5, 0.9, 0.95, 0.99]) AS rtt_quantiles,
        COUNT(*) FILTER (WHERE retrans_value > 0) * 100.0 / NULLIF(COUNT(*), 0) AS retrans_rate_pct,
        AVG(ploss_value) * 100.0 AS packet_loss_pct,
        AVG(delivery_rate_value) * 8.0 / 1000000.0 AS delivery_rate_mbps
      FROM scoped GROUP BY GROUPING SETS ((), (protocol_key))
    )
    SELECT is_protocol, protocol_key, total_requests, bandwidth_bytes,
      rtt_quantiles[1] / 1000.0 AS tcp_rtt_p50_ms,
      rtt_quantiles[2] / 1000.0 AS tcp_rtt_p90_ms,
      rtt_quantiles[3] / 1000.0 AS tcp_rtt_p95_ms,
      rtt_quantiles[4] / 1000.0 AS tcp_rtt_p99_ms,
      retrans_rate_pct, packet_loss_pct, delivery_rate_mbps
    FROM grouped ORDER BY is_protocol, total_requests DESC`, [], params.signal);
  const summary = rows.find((entry) => !entry.is_protocol);
  const row: Record<string, unknown> = summary ? {
    total_requests: Number(summary.total_requests ?? 0),
    bandwidth_bytes: summary.bandwidth_bytes == null ? null : Number(summary.bandwidth_bytes),
    tcp_rtt_p50_ms: summary.tcp_rtt_p50_ms,
    tcp_rtt_p90_ms: summary.tcp_rtt_p90_ms,
    tcp_rtt_p95_ms: summary.tcp_rtt_p95_ms,
    tcp_rtt_p99_ms: summary.tcp_rtt_p99_ms,
    retrans_rate_pct: summary.retrans_rate_pct == null ? null : roundTo(Number(summary.retrans_rate_pct), 2),
    packet_loss_pct: summary.packet_loss_pct,
    delivery_rate_mbps: summary.delivery_rate_mbps,
  } : {};
  const protocols = rows.filter((entry) => entry.is_protocol)
    .map((entry) => ({ protocol: entry.protocol_key, requests: entry.total_requests }));
  const total = Number(row.total_requests ?? 0);
  const transportFields = ["tcp_rtt", "retrans", "ploss", "delivery_rate"] as const;
  const missingFields = Array.isArray(params.active_log_field_ids)
    ? (() => {
        const enabled = new Set(params.active_log_field_ids);
        return transportFields.filter((f) => !enabled.has(f));
      })()
    : (row.tcp_rtt_p50_ms == null ? ["tcp_rtt"] : []);
  return {
    ...row,
    has_transport_data: Array.isArray(params.active_log_field_ids)
      ? missingFields.length < transportFields.length
      : total > 0,
    missing_fields: missingFields,
    protocol_distribution: normalizeProtocolDistribution(protocols, total),
  };
}

export function networkBundle<H extends object | null, Q, P, T>(
  network_health: H, network_quality: Q, pop_health: P, transport: T,
) {
  const popRows = Array.isArray(pop_health)
    ? pop_health
    : pop_health && typeof pop_health === "object" && "data" in pop_health && Array.isArray((pop_health as { data?: unknown }).data)
      ? (pop_health as { data: unknown[] }).data
      : null;
  return {
    available: true,
    health: network_health,
    quality: network_quality,
    transport,
    network_health,
    network_quality,
    pop_health: popRows,
    missing_tcp_fields: network_health && "missing_tcp_fields" in network_health
      ? network_health.missing_tcp_fields as string[] : undefined,
  };
}

export async function queryNetworkBundle(params: NetworkQueryParams = {}) {
  const [network_health, network_quality, pop_health, transport] = await Promise.all([
    queryNetworkHealth(params),
    queryNetworkQuality(params),
    queryPopHealth(params),
    queryNetworkTransport(params),
  ]);
  return networkBundle(network_health, network_quality, pop_health, transport);
}

// Same gates as the backend bundle; the page asks for one section at a time and must not pay for the rest.
async function queryNetworkBundleSections(params: NetworkQueryParams & { sections: readonly string[] }) {
  const { sections } = params;
  const [network_health, network_quality, pop_health, transport] = await Promise.all([
    wantCore(sections) || wantShielding(sections) ? queryNetworkHealthSections(params) : null,
    sections.includes("quality") ? queryNetworkQuality(params) : null,
    sections.includes("pop_health") ? queryPopHealth(params) : null,
    sections.includes("transport") ? queryNetworkTransport(params) : null,
  ]);
  return networkBundle(network_health, network_quality, pop_health, transport);
}

export async function handleNetworkRequest(
  path: string,
  params: NetworkQueryParams = {},
) {
  if (path.includes("/api/network/bundle")) {
    const { sections } = params;
    return sections?.length ? queryNetworkBundleSections({ ...params, sections }) : queryNetworkBundle(params);
  }
  if (path.includes("/api/network-health")) {
    return queryNetworkHealthSections(params);
  }
  if (path.includes("/api/network-quality")) {
    const parts = qualityParts(params.quality_parts);
    return parts ? { quality_parts: await queryNetworkQualityParts(params, parts) } : queryNetworkQuality(params);
  }
  if (path.includes("/api/network/pop-health")) {
    return queryPopHealth(params);
  }
  return queryNetworkBundle(params);
}
