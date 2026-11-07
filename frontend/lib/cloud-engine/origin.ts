import { quoteIdent, sqlEscape } from "@/lib/sqlBuilder";
import { getDuckDBEngine, type DuckDBColumnType, type DuckDBReadContext } from "./dashboard/duckdb";
import { compileFiltersToSql } from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";

export type OriginSectionName =
  | "summary"
  | "breakdown"
  | "retries"
  | "timeseries"
  | "slow_urls"
  | "status_codes"
  | "path_breakdown"
  | "pop_latency"
  | "ip_health"
  | "shielding_routes";

export type OriginDrilldownDimension =
  | "host"
  | "shield_pop"
  | "datacenter"
  | "protocol"
  | "pop"
  | "oip";

export interface OriginSummarySection {
  has_data: boolean;
  total_misses: number | null;
  total_passes: number | null;
  ottfb_p50_ms: number | null;
  ottfb_p75_ms: number | null;
  ottfb_p95_ms: number | null;
  ottfb_p99_ms: number | null;
  ottlb_p50_ms: number | null;
  ottlb_p95_ms: number | null;
  cdn_overhead_p50_ms: number | null;
  origin_error_rate: number | null;
  obytes_p50: number | null;
  _approx?: boolean;
  _approx_note?: string;
}

export interface OriginTimeseriesPoint {
  time: string;
  miss_count: number;
  value: number | null;
  edge?: boolean | null;
}

export interface OriginTimeseriesSection {
  has_data: boolean;
  series: OriginTimeseriesPoint[];
  _approx?: boolean;
}

export interface OriginSlowUrlRow {
  url: string;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
}

export interface OriginSlowUrlsSection {
  has_data: boolean;
  rows: OriginSlowUrlRow[];
  _approx?: boolean;
}

export interface OriginStatusCodeRow {
  status: number;
  count: number;
  pct: number | null;
}

export interface OriginStatusCodesSection {
  has_data: boolean;
  rows: OriginStatusCodeRow[];
  _approx?: boolean;
}

export interface OriginPathRow {
  edge: boolean | null;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
}

export interface OriginPathBreakdownSection {
  has_data: boolean;
  shielding_detected: boolean;
  rows: OriginPathRow[];
  _approx?: boolean;
}

export interface OriginPopLatencyRow {
  pop: string;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
  elevated: boolean;
}

export interface OriginPopLatencySection {
  has_data: boolean;
  requires_group_c: boolean;
  median_p95_ms?: number | null;
  rows: OriginPopLatencyRow[];
  _approx?: boolean;
}

export interface OriginIpHealthRow {
  oip: string;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
  error_pct: number | null;
}

export interface OriginIpHealthSection {
  has_data: boolean;
  rows: OriginIpHealthRow[];
  _approx?: boolean;
}

export interface OriginAggregatesResponse {
  has_data: boolean;
  summary: OriginSummarySection;
  timeseries: OriginTimeseriesSection;
  slow_urls: OriginSlowUrlsSection;
  status_codes: OriginStatusCodesSection;
  path_breakdown: OriginPathBreakdownSection;
  pop_latency: OriginPopLatencySection;
  ip_health: OriginIpHealthSection;
  section_timings?: Array<Record<string, unknown>>;
}

export interface ShieldingRouteRow {
  edge_pop: string;
  shield_pop: string;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  distance_km: number | null;
  light_speed_rtt_ms: number | null;
  efficiency_ratio: number | null;
  low_sample: boolean;
  anomaly_eligible: boolean;
  anomaly_static: boolean;
  edge_lat: number | null;
  edge_lon: number | null;
  shield_lat: number | null;
  shield_lon: number | null;
}

export interface OriginShieldingAnalysisResponse {
  has_data: boolean;
  requires_fields: string[];
  edge_only: boolean;
  rows: ShieldingRouteRow[];
  total_routes: number | null;
  truncated: boolean | null;
  error: boolean;
}

export interface OriginDrilldownRow {
  key: string;
  requests: number;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  error_rate: number;
  bytes: number | null;
}

export interface OriginDrilldownResponse {
  has_data: boolean;
  dimension: string;
  rows: OriginDrilldownRow[];
}

export type OriginSqlRow = Record<string, unknown>;

export interface OriginSqlExecutor {
  query(sql: string, signal?: AbortSignal): Promise<OriginSqlRow[]>;
  getColumns?(tableName?: string, signal?: AbortSignal): Promise<Set<string>>;
  getCustomColumnTypes?(): ReadonlyMap<string, DuckDBColumnType>;
  tableName?: string;
}

export const ORIGIN_SUMMARY_KEYS = [
  "total_misses",
  "total_passes",
  "ottfb_p50_ms",
  "ottfb_p75_ms",
  "ottfb_p95_ms",
  "ottfb_p99_ms",
  "ottlb_p50_ms",
  "ottlb_p95_ms",
  "cdn_overhead_p50_ms",
  "origin_error_rate",
  "obytes_p50",
] as const;

export const ORIGIN_ROLLUP_TABLES = {
  origin_summary: "origin_summary.parquet",
  origin_breakdown: "origin_breakdown.parquet",
  origin_retries: "origin_restarts.parquet",
  origin_latency_ts: "origin_latency_ts.parquet",
  slow_urls: "slow_urls.parquet",
  origin_path: "origin_path.parquet",
  origin_pop: "origin_pop.parquet",
  origin_ip: "origin_ip.parquet",
  shielding_routes: "shielding_routes.parquet",
} as const;

export const POP_COORDINATES: Readonly<Record<string, readonly [number, number]>> = {
  AMS: [52.3086, 4.7639],
  ATL: [33.6407, -84.4277],
  BFI: [47.53, -122.302],
  BOS: [42.3656, -71.0096],
  CDG: [49.0097, 2.5479],
  CHI: [41.9742, -87.9073],
  DEN: [39.8561, -104.6737],
  DFW: [32.8998, -97.0403],
  EWR: [40.6895, -74.1745],
  FRA: [50.0379, 8.5622],
  GRU: [-23.4356, -46.4731],
  HKG: [22.308, 113.9185],
  IAD: [38.9531, -77.4565],
  ICN: [37.4602, 126.4407],
  JFK: [40.6413, -73.7781],
  LAX: [33.9416, -118.4085],
  LCY: [51.5053, 0.0553],
  LHR: [51.47, -0.4543],
  MAD: [40.4983, -3.5676],
  MIA: [25.7959, -80.287],
  NRT: [35.772, 140.3929],
  ORD: [41.9742, -87.9073],
  SEA: [47.4502, -122.3088],
  SFO: [37.6213, -122.379],
  SIN: [1.3644, 103.9915],
  SJC: [37.3382, -121.8863],
  SYD: [-33.9399, 151.1753],
  TYO: [35.5494, 139.7798],
  YYZ: [43.6777, -79.6248],
};

function optFloat(val: unknown, ndigits?: number): number | null {
  if (val === null || val === undefined || val === "") return null;
  const n = typeof val === "bigint" ? Number(val) : Number(val);
  if (!Number.isFinite(n)) return null;
  if (ndigits === undefined) return n;
  const factor = 10 ** ndigits;
  return Math.round(n * factor) / factor;
}

function intOrZero(val: unknown): number {
  if (val === null || val === undefined) return 0;
  const n = typeof val === "bigint" ? Number(val) : Number(val);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

export function haversineKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(a)));
}

export function isPopLatencyElevated(
  p95Ms: number | null | undefined,
  medianP95Ms: number | null | undefined,
  factor = 1.5,
): boolean {
  return (
    p95Ms !== null &&
    p95Ms !== undefined &&
    medianP95Ms !== null &&
    medianP95Ms !== undefined &&
    p95Ms > medianP95Ms * factor
  );
}

export function originLatencyUsExpr(
  actualCols: ReadonlySet<string>,
  absent: string | null = "NULL",
): string | null {
  const hasOttfb = actualCols.has("ottfb");
  const hasTtfb = actualCols.has("ttfb");
  if (hasOttfb && hasTtfb) {
    return 'COALESCE("ottfb", "ttfb" * 1000000.0)';
  }
  if (hasOttfb) return '"ottfb"';
  if (hasTtfb) return '"ttfb" * 1000000.0';
  return absent;
}

export function emptyOriginSummary(): OriginSummarySection {
  return {
    has_data: false,
    total_misses: 0,
    total_passes: 0,
    ottfb_p50_ms: null,
    ottfb_p75_ms: null,
    ottfb_p95_ms: null,
    ottfb_p99_ms: null,
    ottlb_p50_ms: null,
    ottlb_p95_ms: null,
    cdn_overhead_p50_ms: null,
    origin_error_rate: null,
    obytes_p50: null,
  };
}

export function emptyOriginAggregates(): OriginAggregatesResponse {
  return {
    has_data: false,
    summary: emptyOriginSummary(),
    timeseries: { has_data: false, series: [] },
    slow_urls: { has_data: false, rows: [] },
    status_codes: { has_data: false, rows: [] },
    path_breakdown: { has_data: false, shielding_detected: false, rows: [] },
    pop_latency: { has_data: false, requires_group_c: false, rows: [] },
    ip_health: { has_data: false, rows: [] },
  };
}

export function buildOriginSummarySql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
): string {
  const latVal = originLatencyUsExpr(actualCols) ?? "NULL";
  const hasCache = actualCols.has("cache");
  const hasOttlb = actualCols.has("ottlb");
  const hasElapsed = actualCols.has("elapsed");
  const hasOst = actualCols.has("ost");
  const hasObytes = actualCols.has("obytes");

  const totalMisses = hasCache
    ? `COUNT(*) FILTER (WHERE starts_with("cache", 'MISS'))`
    : "0";
  const totalPasses = hasCache
    ? `COUNT(*) FILTER (WHERE starts_with("cache", 'PASS'))`
    : "0";
  const ottlbP50 = hasOttlb ? `QUANTILE_CONT("ottlb", [0.5, 0.95])[1] / 1000.0` : "NULL";
  const ottlbP95 = hasOttlb ? `QUANTILE_CONT("ottlb", [0.5, 0.95])[2] / 1000.0` : "NULL";
  const cdnOvh =
    hasElapsed && hasOttlb ? `MEDIAN("elapsed" - "ottlb") / 1000.0` : "NULL";
  const ost5xx = hasOst
    ? `COUNT(*) FILTER (WHERE "ost" >= 500 AND "ost" < 600) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)`
    : "NULL";
  const obytesP50 = hasObytes ? `MEDIAN("obytes")` : "NULL";

  return `
    SELECT
      COUNT(*) AS requests,
      ${totalMisses} AS total_misses,
      ${totalPasses} AS total_passes,
      QUANTILE_CONT(${latVal}, [0.5, 0.75, 0.95, 0.99])[1] / 1000.0 AS ottfb_p50_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.75, 0.95, 0.99])[2] / 1000.0 AS ottfb_p75_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.75, 0.95, 0.99])[3] / 1000.0 AS ottfb_p95_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.75, 0.95, 0.99])[4] / 1000.0 AS ottfb_p99_ms,
      ${ottlbP50} AS ottlb_p50_ms,
      ${ottlbP95} AS ottlb_p95_ms,
      ${cdnOvh} AS cdn_overhead_p50_ms,
      ${ost5xx} AS origin_error_rate,
      ${obytesP50} AS obytes_p50
    FROM ${table}
    WHERE ${where} AND (${latVal} IS NOT NULL)
  `;
}

export function shapeOriginSummaryRow(
  row: OriginSqlRow | null | undefined,
): OriginSummarySection {
  if (!row || optFloat(row.ottfb_p50_ms) === null) {
    return emptyOriginSummary();
  }
  return {
    has_data: true,
    total_misses: row.total_misses != null ? intOrZero(row.total_misses) : 0,
    total_passes: row.total_passes != null ? intOrZero(row.total_passes) : 0,
    ottfb_p50_ms: optFloat(row.ottfb_p50_ms, 2),
    ottfb_p75_ms: optFloat(row.ottfb_p75_ms, 2),
    ottfb_p95_ms: optFloat(row.ottfb_p95_ms, 2),
    ottfb_p99_ms: optFloat(row.ottfb_p99_ms, 2),
    ottlb_p50_ms: optFloat(row.ottlb_p50_ms, 2),
    ottlb_p95_ms: optFloat(row.ottlb_p95_ms, 2),
    cdn_overhead_p50_ms: optFloat(row.cdn_overhead_p50_ms, 2),
    origin_error_rate: optFloat(row.origin_error_rate, 4),
    obytes_p50: optFloat(row.obytes_p50, 1),
  };
}

export function buildOriginTimeseriesSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  options: {
    metric?: "ttfb" | "ttlb";
    percentile?: "p50" | "p95" | "p99";
    bucketMinutes?: number;
    splitByLeg?: boolean;
  } = {},
): string | null {
  const metric = options.metric ?? "ttfb";
  const percentile = options.percentile ?? "p95";
  const bucketMinutes = Math.max(1, Math.trunc(options.bucketMinutes ?? 5));
  const splitByLeg = Boolean(options.splitByLeg && actualCols.has("edge"));

  const latExpr =
    metric === "ttlb"
      ? actualCols.has("ottlb")
        ? '"ottlb"'
        : null
      : originLatencyUsExpr(actualCols, null);
  if (!latExpr) return null;

  const pctVal = percentile === "p50" ? 0.5 : percentile === "p99" ? 0.99 : 0.95;
  const aggExpr =
    percentile === "p50"
      ? `MEDIAN(${latExpr})`
      : `QUANTILE_CONT(${latExpr}, ${pctVal})`;
  const tsExpr = 'EPOCH_MS(CAST("ts_ms" AS BIGINT))';
  const edgeCol = splitByLeg ? `, "edge"` : "";

  return `
    SELECT
      time_bucket(INTERVAL '${bucketMinutes} minutes', ${tsExpr}) AS ts,
      COUNT(*) AS miss_count,
      ${aggExpr} / 1000.0 AS value
      ${edgeCol}
    FROM ${table}
    WHERE ${where} AND (${latExpr} IS NOT NULL)
    GROUP BY ts ${edgeCol}
    ORDER BY ts
  `;
}

export function shapeOriginTimeseriesRows(
  rows: readonly OriginSqlRow[],
  splitByLeg = false,
): OriginTimeseriesSection {
  if (!rows.length) {
    return { has_data: false, series: [] };
  }
  const series: OriginTimeseriesPoint[] = rows.map((r) => {
    const rawTs = r.ts ?? r.time;
    const iso =
      rawTs instanceof Date
        ? rawTs.toISOString()
        : typeof rawTs === "number"
          ? new Date(rawTs).toISOString()
          : String(rawTs ?? "");
    const pt: OriginTimeseriesPoint = {
      time: iso,
      miss_count: intOrZero(r.miss_count),
      value: optFloat(r.value, 2),
    };
    if (splitByLeg || r.edge !== undefined) {
      pt.edge = r.edge === null || r.edge === undefined ? null : Boolean(r.edge);
    }
    return pt;
  });
  return { has_data: series.length > 0, series };
}

// Exact percentiles hold each group's values (8 bytes a row); APPROX_QUANTILE's per-group digest took ~50 KB
// of WASM heap outside DuckDB's memory limit, so a day of distinct URLs or origin IPs exhausted the page.
// Identical QUANTILE_CONT calls in one SELECT share a single aggregate state.
export function buildOriginSlowUrlsSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  minRequests = 10,
  limit = 20,
): string | null {
  if (!actualCols.has("url")) return null;
  const latVal = originLatencyUsExpr(actualCols, null);
  if (!latVal) return null;
  const safeMin = Math.max(1, Math.trunc(minRequests));
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  return `
    WITH base AS (
      SELECT "url", ${latVal} AS _lat_val
      FROM ${table}
      WHERE ${where} AND (${latVal} IS NOT NULL) AND "url" IS NOT NULL
    ),
    qualified AS (
      SELECT "url"
      FROM base
      GROUP BY "url"
      HAVING COUNT(*) >= ${safeMin}
    )
    SELECT
      b."url" AS url,
      COUNT(*) AS requests,
      QUANTILE_CONT(b._lat_val, [0.5, 0.95, 0.99])[1] / 1000.0 AS p50_ms,
      QUANTILE_CONT(b._lat_val, [0.5, 0.95, 0.99])[2] / 1000.0 AS p95_ms,
      QUANTILE_CONT(b._lat_val, [0.5, 0.95, 0.99])[3] / 1000.0 AS p99_ms
    FROM base b
    SEMI JOIN qualified q USING ("url")
    GROUP BY b."url"
    ORDER BY p95_ms DESC
    LIMIT ${safeLimit}
  `;
}

export function shapeOriginSlowUrlsRows(
  rows: readonly OriginSqlRow[],
): OriginSlowUrlsSection {
  if (!rows.length) return { has_data: false, rows: [] };
  return {
    has_data: true,
    rows: rows.map((r) => ({
      url: String(r.url ?? ""),
      requests: intOrZero(r.requests),
      p50_ms: optFloat(r.p50_ms, 2),
      p95_ms: optFloat(r.p95_ms, 2),
      p99_ms: optFloat(r.p99_ms, 2),
    })),
  };
}

export function buildOriginStatusCodesSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
): string | null {
  if (!actualCols.has("ost")) return null;
  return `
    SELECT
      CASE
        WHEN "ost" BETWEEN 100 AND 599 THEN "ost"
        ELSE -1
      END AS status,
      COUNT(*) AS count,
      COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
    FROM ${table}
    WHERE ${where} AND "ost" IS NOT NULL
    GROUP BY 1
    ORDER BY count DESC
  `;
}

export function shapeOriginStatusCodesRows(
  rows: readonly OriginSqlRow[],
): OriginStatusCodesSection {
  if (!rows.length) return { has_data: false, rows: [] };
  const total = rows.reduce((acc, r) => acc + intOrZero(r.count), 0);
  return {
    has_data: true,
    rows: rows.map((r) => {
      const count = intOrZero(r.count);
      const pct =
        r.pct !== undefined && r.pct !== null
          ? optFloat(r.pct, 2)
          : total > 0
            ? optFloat((count * 100.0) / total, 2)
            : 0;
      return {
        status: intOrZero(r.status),
        count,
        pct,
      };
    }),
  };
}

export function buildOriginPathBreakdownSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
): string | null {
  if (!actualCols.has("edge")) return null;
  const latVal = originLatencyUsExpr(actualCols, null);
  if (!latVal) return null;
  return `
    SELECT
      "edge",
      COUNT(*) AS requests,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[1] / 1000.0 AS p50_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[2] / 1000.0 AS p95_ms
    FROM ${table}
    WHERE ${where} AND (${latVal} IS NOT NULL)
    GROUP BY "edge"
  `;
}

export function shapeOriginPathBreakdownRows(
  rows: readonly OriginSqlRow[],
): OriginPathBreakdownSection {
  if (!rows.length) {
    return { has_data: false, shielding_detected: false, rows: [] };
  }
  const shaped: OriginPathRow[] = rows.map((r) => ({
    edge: r.edge === null || r.edge === undefined ? null : Boolean(r.edge),
    requests: intOrZero(r.requests),
    p50_ms: optFloat(r.p50_ms, 2),
    p95_ms: optFloat(r.p95_ms, 2),
  }));
  return {
    has_data: true,
    shielding_detected: shaped.some((r) => r.edge === false),
    rows: shaped,
  };
}

export function buildOriginPopLatencySql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  limit = 30,
): string | null {
  if (!actualCols.has("pop")) return null;
  const latVal = originLatencyUsExpr(actualCols, null);
  if (!latVal) return null;
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  return `
    SELECT
      "pop",
      COUNT(*) AS requests,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[1] / 1000.0 AS p50_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[2] / 1000.0 AS p95_ms
    FROM ${table}
    WHERE ${where} AND (${latVal} IS NOT NULL) AND "pop" IS NOT NULL AND "pop" != ''
    GROUP BY "pop"
    ORDER BY p95_ms DESC
    LIMIT ${safeLimit}
  `;
}

export function shapeOriginPopLatencyRows(
  rows: readonly OriginSqlRow[],
  options: { requiresGroupC?: boolean; factor?: number } = {},
): OriginPopLatencySection {
  const requiresGroupC = Boolean(options.requiresGroupC);
  const factor = options.factor ?? 1.5;
  if (!rows.length) {
    return {
      has_data: false,
      requires_group_c: requiresGroupC,
      rows: [],
    };
  }
  const parsed = rows.map((r) => ({
    pop: String(r.pop ?? ""),
    requests: intOrZero(r.requests),
    p50_ms: optFloat(r.p50_ms, 2),
    p95_ms: optFloat(r.p95_ms, 2),
  }));
  const validP95s = parsed
    .map((r) => r.p95_ms)
    .filter((v): v is number => v !== null)
    .sort((a, b) => a - b);
  const medianP95 =
    validP95s.length > 0 ? validP95s[Math.floor(validP95s.length / 2)] : 0;

  return {
    has_data: true,
    requires_group_c: requiresGroupC,
    median_p95_ms: medianP95,
    rows: parsed.map((r) => ({
      ...r,
      elevated: isPopLatencyElevated(r.p95_ms, medianP95, factor),
    })),
  };
}

export function buildOriginIpHealthSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  limit = 30,
  minRequests = 10,
): string | null {
  if (!actualCols.has("oip") || !actualCols.has("ost")) return null;
  const latVal = originLatencyUsExpr(actualCols) ?? "NULL";
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  const safeMin = Math.max(1, Math.trunc(minRequests));
  return `
    SELECT
      "oip",
      COUNT(*) AS requests,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[1] / 1000.0 AS p50_ms,
      QUANTILE_CONT(${latVal}, [0.5, 0.95])[2] / 1000.0 AS p95_ms,
      ROUND(COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0 / NULLIF(COUNT(*), 0), 1) AS error_pct
    FROM ${table}
    WHERE ${where} AND "oip" IS NOT NULL AND "oip" != '' AND "oip" != '-' AND "ost" IS NOT NULL
    GROUP BY "oip"
    HAVING COUNT(*) >= ${safeMin}
    ORDER BY error_pct DESC
    LIMIT ${safeLimit}
  `;
}

export function shapeOriginIpHealthRows(
  rows: readonly OriginSqlRow[],
): OriginIpHealthSection {
  if (!rows.length) return { has_data: false, rows: [] };
  return {
    has_data: true,
    rows: rows.map((r) => ({
      oip: String(r.oip ?? ""),
      requests: intOrZero(r.requests),
      p50_ms: optFloat(r.p50_ms, 2),
      p95_ms: optFloat(r.p95_ms, 2),
      error_pct: optFloat(r.error_pct, 1),
    })),
  };
}

export function enrichShieldingRouteWithPopDistance(
  row: OriginSqlRow,
  minRequests = 30,
): ShieldingRouteRow {
  const edgePop = String(row.edge_pop ?? "").toUpperCase();
  const shieldPop = String(row.shield_pop ?? "").toUpperCase();
  const requests = intOrZero(row.requests);
  const p50Ms = optFloat(row.p50_ms, 2);
  const p95Ms = optFloat(row.p95_ms, 2);
  const p99Ms = optFloat(row.p99_ms, 2);

  const eCoords = POP_COORDINATES[edgePop];
  const sCoords = POP_COORDINATES[shieldPop];
  const lowSample = requests < minRequests;

  let distanceKm: number | null = null;
  let lightSpeedRttMs: number | null = null;
  let efficiencyRatio: number | null = null;
  let anomalyEligible = false;

  if (eCoords && sCoords) {
    const dist = haversineKm(eCoords[0], eCoords[1], sCoords[0], sCoords[1]);
    distanceKm = optFloat(dist, 1);
    lightSpeedRttMs = optFloat((2.0 * dist * 1000.0) / 200_000.0, 2);
    if (lightSpeedRttMs !== null && lightSpeedRttMs > 0.5 && p50Ms !== null) {
      efficiencyRatio = optFloat(p50Ms / lightSpeedRttMs, 2);
    }
    anomalyEligible = Boolean(
      efficiencyRatio !== null &&
        efficiencyRatio > 3.0 &&
        p50Ms !== null &&
        lightSpeedRttMs !== null &&
        p50Ms - lightSpeedRttMs >= 20.0,
    );
  }

  return {
    edge_pop: edgePop,
    shield_pop: shieldPop,
    requests,
    p50_ms: p50Ms,
    p95_ms: p95Ms,
    p99_ms: p99Ms,
    distance_km: distanceKm,
    light_speed_rtt_ms: lightSpeedRttMs,
    efficiency_ratio: efficiencyRatio,
    low_sample: lowSample,
    anomaly_eligible: anomalyEligible,
    anomaly_static: anomalyEligible && !lowSample,
    edge_lat: eCoords ? eCoords[0] : null,
    edge_lon: eCoords ? eCoords[1] : null,
    shield_lat: sCoords ? sCoords[0] : null,
    shield_lon: sCoords ? sCoords[1] : null,
  };
}

export function buildOriginShieldingAnalysisSql(
  table: string,
  where: string,
  timeWhere: string,
  limit = 50,
): string {
  const safeLimit = Math.max(1, Math.min(200, Math.trunc(limit)));
  const edgeMinusShieldOttfb = `(COALESCE(e."ottfb", e."ttfb" * 1000000.0) - COALESCE(s."ottfb", s."ttfb" * 1000000.0))`;
  return `
    WITH edge_logs AS (
      SELECT "rid", "pop", "ottfb", "ttfb"
      FROM ${table}
      WHERE ${where} AND "edge" = true
    ),
    shield_logs AS MATERIALIZED (
      SELECT "prid", "pop", "ottfb", "ttfb"
      FROM ${table}
      WHERE ${timeWhere} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != ''
    ),
    pairs AS (
      SELECT
        e."pop" AS edge_pop,
        s."pop" AS shield_pop,
        COUNT(*) AS requests,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ${edgeMinusShieldOttfb}) / 1000.0 AS p50_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${edgeMinusShieldOttfb}) / 1000.0 AS p95_ms,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ${edgeMinusShieldOttfb}) / 1000.0 AS p99_ms
      FROM edge_logs e
      INNER JOIN shield_logs s ON s."prid" = e."rid"
      GROUP BY 1, 2
    ),
    ranked AS (
      SELECT
        *,
        COUNT(*) OVER () AS total_routes,
        ROW_NUMBER() OVER (ORDER BY requests DESC) AS rn_requests,
        ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST) AS rn_overhead
      FROM pairs
    )
    SELECT edge_pop, shield_pop, requests, p50_ms, p95_ms, p99_ms, total_routes
    FROM ranked
    WHERE rn_requests <= ${safeLimit} OR rn_overhead <= ${safeLimit}
    ORDER BY requests DESC
  `;
}

export function buildOriginDrilldownSql(
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  dimension: OriginDrilldownDimension = "host",
  limit = 30,
): string | null {
  const dimColMap: Record<OriginDrilldownDimension, string[]> = {
    host: ["host"],
    shield_pop: ["pop"],
    pop: ["pop"],
    datacenter: ["server_region", "pop"],
    protocol: ["proto", "protocol"],
    oip: ["oip"],
  };
  const candidates = dimColMap[dimension] ?? ["host"];
  const dimCol = candidates.find((c) => actualCols.has(c));
  if (!dimCol) return null;

  const latVal = originLatencyUsExpr(actualCols, null);
  if (!latVal) return null;

  const errorExpr = actualCols.has("ost")
    ? `"ost" >= 500`
    : actualCols.has("status")
      ? `"status" >= 500`
      : "FALSE";
  const bytesExpr = actualCols.has("obytes")
    ? `SUM("obytes")`
    : actualCols.has("resp_bytes")
      ? `SUM("resp_bytes")`
      : "NULL";
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  const qCol = quoteIdent(dimCol);

  return `
    SELECT
      COALESCE(CAST(${qCol} AS VARCHAR), 'unknown') AS dim_key,
      COUNT(*) AS requests,
      ROUND(QUANTILE_CONT(${latVal}, [0.5, 0.95, 0.99])[1] / 1000.0, 2) AS p50_ms,
      ROUND(QUANTILE_CONT(${latVal}, [0.5, 0.95, 0.99])[2] / 1000.0, 2) AS p95_ms,
      ROUND(QUANTILE_CONT(${latVal}, [0.5, 0.95, 0.99])[3] / 1000.0, 2) AS p99_ms,
      ROUND(COUNT(*) FILTER (WHERE ${errorExpr}) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate,
      ${bytesExpr} AS bytes
    FROM ${table}
    WHERE ${where} AND (${latVal} IS NOT NULL) AND ${qCol} IS NOT NULL AND CAST(${qCol} AS VARCHAR) != ''
    GROUP BY 1
    ORDER BY requests DESC
    LIMIT ${safeLimit}
  `;
}

export function shapeOriginDrilldownRows(
  rows: readonly OriginSqlRow[],
  dimension: OriginDrilldownDimension = "host",
): OriginDrilldownResponse {
  if (!rows.length) {
    return { has_data: false, dimension, rows: [] };
  }
  return {
    has_data: true,
    dimension,
    rows: rows.map((r) => ({
      key: String(r.dim_key ?? r.key ?? "unknown"),
      requests: intOrZero(r.requests),
      p50_ms: optFloat(r.p50_ms, 2),
      p95_ms: optFloat(r.p95_ms, 2),
      p99_ms: optFloat(r.p99_ms, 2),
      error_rate: optFloat(r.error_rate, 2) ?? 0,
      bytes: r.bytes !== null && r.bytes !== undefined ? intOrZero(r.bytes) : null,
    })),
  };
}

let customOriginExecutor: OriginSqlExecutor | null = null;

export function setOriginSqlExecutor(executor: OriginSqlExecutor | null): void {
  customOriginExecutor = executor;
}

function createDefaultOriginExecutor(context?: DuckDBReadContext): OriginSqlExecutor {
  const read = context ?? getDuckDBEngine().readContext();
  return {
    tableName: "requests",
    getCustomColumnTypes: () => read.getCustomColumnTypes(),
    async query(sql: string, signal?: AbortSignal): Promise<OriginSqlRow[]> {
      const engine = read as unknown as {
        querySql?: (sql: string, params: readonly unknown[], signal?: AbortSignal) => Promise<OriginSqlRow[]>;
        executeSql?: (sql: string, params: readonly unknown[], signal?: AbortSignal) => Promise<OriginSqlRow[]>;
        ensureBackend?: () => Promise<{
          query: (
            sql: string,
            signal?: AbortSignal,
          ) => Promise<{
            numRows: number;
            schema?: { fields?: Array<{ name: string }> };
            getChild: (name: string) => { toJSON: () => unknown[] } | null;
          }>;
        }>;
      };
      if (typeof engine.querySql === "function") {
        return engine.querySql(sql, [], signal);
      }
      if (typeof engine.executeSql === "function") {
        return engine.executeSql(sql, [], signal);
      }
      if (typeof engine.ensureBackend === "function") {
        const backend = await engine.ensureBackend();
        const table = await backend.query(sql, signal);
        const describeTable = await backend
          .query(`DESCRIBE (${sql})`, signal)
          .catch(() => null);
        const colNames =
          (describeTable?.getChild("column_name")?.toJSON() as string[]) ??
          table.schema?.fields?.map((f) => f.name) ??
          [];
        const colArrays = colNames.map((name) => [
          name,
          table.getChild(name)?.toJSON() ?? [],
        ] as const);
        const rows: OriginSqlRow[] = [];
        for (let i = 0; i < table.numRows; i++) {
          const obj: OriginSqlRow = {};
          for (const [col, arr] of colArrays) {
            const v = arr[i];
            obj[col] = typeof v === "bigint" ? Number(v) : v;
          }
          rows.push(obj);
        }
        return rows;
      }
      return [];
    },
    async getColumns(tableName = "requests", signal?: AbortSignal): Promise<Set<string>> {
      const rows = await this.query(`DESCRIBE SELECT * FROM ${tableName}`, signal).catch(() => {
        signal?.throwIfAborted();
        read.assertCurrent();
        return [];
      });
      return new Set(rows.map((r) => String(r.column_name ?? "")).filter(Boolean));
    },
  };
}

const ORIGIN_LATENCY_COLUMNS = ["timestamp", "ts_ms", "ottfb", "ttfb"] as const;

/** Every request column each /api/origin scan reads, by route. */
export const ORIGIN_SCAN_COLUMNS = {
  aggregates: [...ORIGIN_LATENCY_COLUMNS, "cache", "ottlb", "elapsed", "ost", "obytes", "edge", "url", "pop", "oip"],
  "shielding-analysis": [...ORIGIN_LATENCY_COLUMNS, "edge", "rid", "prid", "pop"],
  drilldown: [...ORIGIN_LATENCY_COLUMNS, "ost", "obytes", "host", "pop", "server_region", "proto", "oip"],
} as const;

const ORIGIN_AGGREGATE_ROUTES = new Set([
  "aggregates", "summary", "timeseries", "slow-urls", "status-codes", "path-breakdown", "pop-latency", "ip-health",
]);

function filterFields(filters: unknown): string[] | null {
  if (filters == null || filters === "") return [];
  if (typeof filters !== "object" || Array.isArray(filters)) return null;
  return Object.keys(filters).map((key) => key.replace(/_\d+$/, ""));
}

/** The columns a projected /api/origin load needs, or null when the route or filters can't name theirs. */
export function originScanColumns(pathname: string, body: Record<string, unknown>): string[] | null {
  const route = pathname.replace(/^\/api\/origin\//, "");
  const base = ORIGIN_AGGREGATE_ROUTES.has(route)
    ? ORIGIN_SCAN_COLUMNS.aggregates
    : ORIGIN_SCAN_COLUMNS[route as keyof typeof ORIGIN_SCAN_COLUMNS];
  const filters = filterFields(body.filters);
  const custom = filterFields(body.custom_filters);
  if (!base || !filters || !custom) return null;
  return [...new Set([...base, ...filters, ...custom])];
}

function parseFilters(
  rawFilters?: Record<string, { mode?: string; values?: string[] }>,
  customFilters?: Record<string, { mode?: string; values?: string[] }>,
): DashboardFilter[] {
  const out: DashboardFilter[] = [];
  const push = (src?: Record<string, { mode?: string; values?: string[] }>) => {
    if (!src || typeof src !== "object") return;
    for (const [rawKey, entry] of Object.entries(src)) {
      const field = rawKey.replace(/_\d+$/, "");
      const vals = Array.isArray(entry?.values) ? entry.values.map(String) : [];
      const mode = entry?.mode === "exclude" ? "exclude" : "include";
      for (const value of vals) {
        if (value) out.push({ field, value, mode });
      }
    }
  };
  push(rawFilters);
  push(customFilters);
  return out;
}

function buildWhereClause(
  body: {
    start_time?: string;
    end_time?: string;
    filters?: Record<string, { mode?: string; values?: string[] }>;
    custom_filters?: Record<string, { mode?: string; values?: string[] }>;
  },
  actualCols: ReadonlySet<string>,
  customTypes: ReadonlyMap<string, DuckDBColumnType>,
): { where: string; timeWhere: string } {
  const clauses: string[] = [];
  const startMs = body.start_time ? Date.parse(body.start_time) : NaN;
  const endMs = body.end_time ? Date.parse(body.end_time) : NaN;
  if (Number.isFinite(startMs)) {
    if (actualCols.has("ts_ms")) {
      clauses.push(`"ts_ms" >= ${Math.floor(startMs)}`);
    } else if (actualCols.has("timestamp")) {
      clauses.push(
        `"timestamp" >= CAST('${sqlEscape(new Date(startMs).toISOString())}' AS TIMESTAMPTZ)`,
      );
    }
  }
  if (Number.isFinite(endMs)) {
    if (actualCols.has("ts_ms")) {
      clauses.push(`"ts_ms" < ${Math.ceil(endMs)}`);
    } else if (actualCols.has("timestamp")) {
      clauses.push(
        `"timestamp" < CAST('${sqlEscape(new Date(endMs).toISOString())}' AS TIMESTAMPTZ)`,
      );
    }
  }
  const timeWhere = clauses.length > 0 ? clauses.join(" AND ") : "TRUE";
  const filters = parseFilters(body.filters, body.custom_filters);
  const filterSql = filters.length > 0 ? compileFiltersToSql(filters, false, [], customTypes) : "TRUE";
  const where = `${timeWhere} AND (${filterSql})`;
  return { where, timeWhere };
}

export async function queryOriginSlowUrlsWithFallback(
  executor: OriginSqlExecutor,
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  minRequests = 10,
  limit = 20,
  signal?: AbortSignal,
): Promise<OriginSlowUrlsSection> {
  const primarySql = buildOriginSlowUrlsSql(
    table,
    where,
    actualCols,
    minRequests,
    limit,
  );
  if (!primarySql) return { has_data: false, rows: [] };
  let rows = await executor.query(primarySql, signal);
  if (rows.length === 0 && minRequests > 1) {
    const fallbackSql = buildOriginSlowUrlsSql(table, where, actualCols, 1, limit);
    if (fallbackSql) {
      rows = await executor.query(fallbackSql, signal);
    }
  }
  return shapeOriginSlowUrlsRows(rows);
}

export async function queryOriginIpHealthWithFallback(
  executor: OriginSqlExecutor,
  table: string,
  where: string,
  actualCols: ReadonlySet<string>,
  limit = 30,
  minRequests = 10,
  signal?: AbortSignal,
): Promise<OriginIpHealthSection> {
  const primarySql = buildOriginIpHealthSql(
    table,
    where,
    actualCols,
    limit,
    minRequests,
  );
  if (!primarySql) return { has_data: false, rows: [] };
  let rows = await executor.query(primarySql, signal);
  if (rows.length === 0 && minRequests > 1) {
    const fallbackSql = buildOriginIpHealthSql(table, where, actualCols, limit, 1);
    if (fallbackSql) {
      rows = await executor.query(fallbackSql, signal);
    }
  }
  return shapeOriginIpHealthRows(rows);
}

export async function queryOriginAggregates(
  body: {
    start_time?: string;
    end_time?: string;
    filters?: Record<string, { mode?: string; values?: string[] }>;
    custom_filters?: Record<string, { mode?: string; values?: string[] }>;
    bucket_minutes?: number;
    split_by_leg?: boolean;
    timeseries_metric?: "ttfb" | "ttlb";
    timeseries_percentile?: "p50" | "p95" | "p99";
    slow_urls_limit?: number;
    slow_urls_min_requests?: number;
    ip_health_limit?: number;
    pop_latency_limit?: number;
    sections?: OriginSectionName[] | null;
  } = {},
  signal?: AbortSignal,
  executor: OriginSqlExecutor = customOriginExecutor ?? createDefaultOriginExecutor(),
): Promise<OriginAggregatesResponse> {
  const table = executor.tableName ?? "requests";
  const actualCols = executor.getColumns
    ? await executor.getColumns(table, signal)
    : new Set<string>();
  if (actualCols.size === 0 || !originLatencyUsExpr(actualCols, null)) {
    return emptyOriginAggregates();
  }

  const wanted = body.sections ? new Set(body.sections) : null;
  const want = (s: OriginSectionName) => !wanted || wanted.has(s);
  const { where } = buildWhereClause(body, actualCols, executor.getCustomColumnTypes?.() ?? new Map());
  const out = emptyOriginAggregates();

  if (want("summary")) {
    const sql = buildOriginSummarySql(table, where, actualCols);
    const rows = await executor.query(sql, signal);
    out.summary = shapeOriginSummaryRow(rows[0]);
  }

  if (want("timeseries")) {
    const sql = buildOriginTimeseriesSql(table, where, actualCols, {
      metric: body.timeseries_metric ?? "ttfb",
      percentile: body.timeseries_percentile ?? "p95",
      bucketMinutes: body.bucket_minutes ?? 5,
      splitByLeg: body.split_by_leg ?? false,
    });
    if (sql) {
      const rows = await executor.query(sql, signal);
      out.timeseries = shapeOriginTimeseriesRows(rows, body.split_by_leg ?? false);
    }
  }

  if (want("slow_urls")) {
    out.slow_urls = await queryOriginSlowUrlsWithFallback(
      executor,
      table,
      where,
      actualCols,
      body.slow_urls_min_requests ?? 10,
      body.slow_urls_limit ?? 20,
      signal,
    );
  }

  if (want("status_codes")) {
    const sql = buildOriginStatusCodesSql(table, where, actualCols);
    if (sql) {
      const rows = await executor.query(sql, signal);
      out.status_codes = shapeOriginStatusCodesRows(rows);
    }
  }

  if (want("path_breakdown")) {
    const sql = buildOriginPathBreakdownSql(table, where, actualCols);
    if (sql) {
      const rows = await executor.query(sql, signal);
      out.path_breakdown = shapeOriginPathBreakdownRows(rows);
    }
  }

  if (want("pop_latency")) {
    const hasEdge = actualCols.has("edge");
    const sql = buildOriginPopLatencySql(
      table,
      where,
      actualCols,
      body.pop_latency_limit ?? 30,
    );
    if (sql) {
      const rows = await executor.query(sql, signal);
      out.pop_latency = shapeOriginPopLatencyRows(rows, {
        requiresGroupC: !hasEdge,
      });
    } else {
      out.pop_latency = {
        has_data: false,
        requires_group_c: !hasEdge,
        rows: [],
      };
    }
  }

  if (want("ip_health")) {
    out.ip_health = await queryOriginIpHealthWithFallback(
      executor,
      table,
      where,
      actualCols,
      body.ip_health_limit ?? 30,
      10,
      signal,
    );
  }

  out.has_data = Boolean(
    out.summary.has_data ||
      out.timeseries.has_data ||
      out.slow_urls.has_data ||
      out.status_codes.has_data ||
      out.path_breakdown.has_data ||
      out.pop_latency.has_data ||
      out.ip_health.has_data,
  );
  return out;
}

export async function queryOriginShieldingAnalysis(
  body: {
    start_time?: string;
    end_time?: string;
    filters?: Record<string, { mode?: string; values?: string[] }>;
    custom_filters?: Record<string, { mode?: string; values?: string[] }>;
    limit?: number;
  } = {},
  signal?: AbortSignal,
  executor: OriginSqlExecutor = customOriginExecutor ?? createDefaultOriginExecutor(),
): Promise<OriginShieldingAnalysisResponse> {
  const table = executor.tableName ?? "requests";
  const actualCols = executor.getColumns
    ? await executor.getColumns(table, signal)
    : new Set<string>();
  const required = ["edge", "rid", "prid", "pop"];
  const missing = required.filter((col) => !actualCols.has(col));
  if (missing.length > 0 || !originLatencyUsExpr(actualCols, null)) {
    return {
      has_data: false,
      requires_fields: missing,
      edge_only: false,
      rows: [],
      total_routes: 0,
      truncated: false,
      error: false,
    };
  }

  const { where, timeWhere } = buildWhereClause(body, actualCols, executor.getCustomColumnTypes?.() ?? new Map());
  const limit = body.limit ?? 50;
  const sql = buildOriginShieldingAnalysisSql(table, where, timeWhere, limit);
  const rows = await executor.query(sql, signal);
  if (!rows.length) {
    return {
      has_data: false,
      requires_fields: [],
      edge_only: true,
      rows: [],
      total_routes: 0,
      truncated: false,
      error: false,
    };
  }
  const totalRoutes = intOrZero(rows[0].total_routes ?? rows.length);
  const shaped = rows.map((r) => enrichShieldingRouteWithPopDistance(r));
  return {
    has_data: true,
    requires_fields: [],
    edge_only: false,
    rows: shaped,
    total_routes: totalRoutes,
    truncated: totalRoutes > shaped.length,
    error: false,
  };
}

export async function queryOriginDrilldown(
  body: {
    start_time?: string;
    end_time?: string;
    filters?: Record<string, { mode?: string; values?: string[] }>;
    custom_filters?: Record<string, { mode?: string; values?: string[] }>;
    dimension?: OriginDrilldownDimension;
    limit?: number;
  } = {},
  signal?: AbortSignal,
  executor: OriginSqlExecutor = customOriginExecutor ?? createDefaultOriginExecutor(),
): Promise<OriginDrilldownResponse> {
  const dimension = body.dimension ?? "host";
  const table = executor.tableName ?? "requests";
  const actualCols = executor.getColumns
    ? await executor.getColumns(table, signal)
    : new Set<string>();
  const { where } = buildWhereClause(body, actualCols, executor.getCustomColumnTypes?.() ?? new Map());
  const sql = buildOriginDrilldownSql(
    table,
    where,
    actualCols,
    dimension,
    body.limit ?? 30,
  );
  if (!sql) {
    return { has_data: false, dimension, rows: [] };
  }
  const rows = await executor.query(sql, signal);
  return shapeOriginDrilldownRows(rows, dimension);
}

function jsonResp(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export async function handleOriginRequest(
  request: Request,
  pathArg?: string,
  urlArg?: URL,
  context?: DuckDBReadContext,
): Promise<Response | null> {
  const url = urlArg ?? new URL(request.url, "http://localhost");
  const pathname = pathArg ?? url.pathname;
  const method = (request.method || "GET").toUpperCase();

  if (!pathname.startsWith("/api/origin/")) {
    return null;
  }
  if (method !== "POST" && method !== "GET") {
    return null;
  }

  const executor = customOriginExecutor ?? createDefaultOriginExecutor(context);
  const body =
    method === "POST"
      ? ((await request.clone().json().catch(() => ({}))) as Record<string, unknown>)
      : {};

  if (pathname === "/api/origin/aggregates") {
    const res = await queryOriginAggregates(body, request.signal, executor);
    return jsonResp(res);
  }
  if (pathname === "/api/origin/summary") {
    const res = await queryOriginAggregates(
      { ...body, sections: ["summary"] },
      request.signal,
      executor,
    );
    return jsonResp(res.summary);
  }
  if (pathname === "/api/origin/timeseries") {
    const res = await queryOriginAggregates(
      {
        ...body,
        timeseries_metric:
          (body.metric as "ttfb" | "ttlb" | undefined) ??
          (body.timeseries_metric as "ttfb" | "ttlb" | undefined) ??
          "ttfb",
        timeseries_percentile:
          (body.percentile as "p50" | "p95" | "p99" | undefined) ??
          (body.timeseries_percentile as "p50" | "p95" | "p99" | undefined) ??
          "p95",
        sections: ["timeseries"],
      },
      request.signal,
      executor,
    );
    return jsonResp(res.timeseries);
  }
  if (pathname === "/api/origin/slow-urls") {
    const res = await queryOriginAggregates(
      {
        ...body,
        slow_urls_limit: Number(body.limit ?? body.slow_urls_limit ?? 20),
        slow_urls_min_requests: Number(
          body.min_requests ?? body.slow_urls_min_requests ?? 10,
        ),
        sections: ["slow_urls"],
      },
      request.signal,
      executor,
    );
    return jsonResp(res.slow_urls);
  }
  if (pathname === "/api/origin/status-codes") {
    const res = await queryOriginAggregates(
      { ...body, sections: ["status_codes"] },
      request.signal,
      executor,
    );
    return jsonResp(res.status_codes);
  }
  if (pathname === "/api/origin/path-breakdown") {
    const res = await queryOriginAggregates(
      { ...body, sections: ["path_breakdown"] },
      request.signal,
      executor,
    );
    return jsonResp(res.path_breakdown);
  }
  if (pathname === "/api/origin/pop-latency") {
    const res = await queryOriginAggregates(
      {
        ...body,
        pop_latency_limit: Number(body.limit ?? body.pop_latency_limit ?? 30),
        sections: ["pop_latency"],
      },
      request.signal,
      executor,
    );
    return jsonResp(res.pop_latency);
  }
  if (pathname === "/api/origin/ip-health") {
    const res = await queryOriginAggregates(
      {
        ...body,
        ip_health_limit: Number(body.limit ?? body.ip_health_limit ?? 30),
        sections: ["ip_health"],
      },
      request.signal,
      executor,
    );
    return jsonResp(res.ip_health);
  }
  if (pathname === "/api/origin/shielding-analysis") {
    const res = await queryOriginShieldingAnalysis(body, request.signal, executor);
    return jsonResp(res);
  }
  if (pathname === "/api/origin/drilldown") {
    const res = await queryOriginDrilldown(
      {
        ...body,
        dimension: (body.dimension as OriginDrilldownDimension) ?? "host",
        limit: Number(body.limit ?? 30),
      },
      request.signal,
      executor,
    );
    return jsonResp(res);
  }

  return null;
}
