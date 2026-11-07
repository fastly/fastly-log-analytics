/**
 * Feature F19 — Page 9 `/performance` (Performance & Latency Waterfalls) Cloud-Engine Module.
 *
 * Parity with `backend/routers/performance.py`, `backend/high_scale/performance.py`,
 * and `backend/repositories/utils/response_shaping/domains.py` + `asn.py`:
 * - POST `/api/performance/aggregates` (and `/api/performance`)
 *   - `top_urls`: latency percentiles (avg, p50, p95, p99) per URL (`req_count > 5`, top 20)
 *   - `top_asns`: latency percentiles (avg, p50, p95, p99) per ASN (`req_count > 10`, top 20) with ASN label resolution
 *   - `ttl_dist`: TTL distribution across `< 60s`, `1m - 1h`, `1h - 1d`, `> 1d`
 *   - `waterfall`: 4-stage timing decomposition (`edge_processing`, `origin_wait`, `origin_download`, `client_download`)
 *   - `scatter`: deterministic sample up to `300` points (`origin_ms`, `edge_ms`, `cache`)
 */

import { sqlEscape } from "@/lib/sqlBuilder";
import { compileFiltersToSql, getDuckDBEngine, type DuckDBReadContext, type DuckDBColumnType } from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";

export type PerformanceSectionName =
  | "top_urls"
  | "top_asns"
  | "ttl_dist"
  | "waterfall"
  | "scatter";

export type PerformanceSortBy = "avg" | "p50" | "p95" | "p99";

export const ALL_PERFORMANCE_SECTIONS: readonly PerformanceSectionName[] = [
  "top_urls",
  "top_asns",
  "ttl_dist",
  "waterfall",
  "scatter",
] as const;

export const SCATTER_SAMPLE_SIZE = 300;
export const MIN_URL_REQUESTS = 5;
export const MIN_ASN_REQUESTS = 10;
export const PERF_TOP_N = 20;

export const PERF_TTL_BUCKETS: ReadonlyArray<readonly [number, string]> = [
  [60, "< 60s"],
  [3600, "1m - 1h"],
  [86400, "1h - 1d"],
] as const;

export const PERF_TTL_LAST_BUCKET = "> 1d";

export const SORT_COLUMNS: Record<PerformanceSortBy, string> = {
  avg: "avg_ms",
  p50: "p50_ms",
  p95: "p95_ms",
  p99: "p99_ms",
};

export const PERFORMANCE_ROLLUPS = {
  perf_top_urls: "perf_top_urls",
  perf_top_asns: "perf_top_asns",
  perf_ttl_dist: "perf_ttl_dist",
} as const;

const KNOWN_ASN_LABELS: Readonly<Record<string, string>> = {
  "15169": "AS15169 (Google LLC)",
  "13335": "AS13335 (Cloudflare, Inc.)",
  "16509": "AS16509 (Amazon.com, Inc.)",
  "14618": "AS14618 (Amazon.com, Inc.)",
  "8075": "AS8075 (Microsoft Corporation)",
  "7922": "AS7922 (Comcast Cable Communications)",
  "7018": "AS7018 (AT&T Services, Inc.)",
  "701": "AS701 (Verizon Business)",
  "20940": "AS20940 (Akamai International B.V.)",
  "54113": "AS54113 (Fastly, Inc.)",
};

export function formatAsnLabel(asn: string | number): string {
  const clean = String(asn ?? "").replace(/^AS/i, "").trim();
  if (!clean) return "Unknown ASN";
  return KNOWN_ASN_LABELS[clean] ?? `AS${clean}`;
}

export type FilterSpec = Record<
  string,
  string[] | { mode?: string; values?: string[] } | string | undefined
>;

export interface PerformanceRequest {
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>;
  start_time?: string | null;
  end_time?: string | null;
  range_token?: string | null;
  anchor?: string | null;
  service_id?: string | null;
  filters?: FilterSpec | null;
  sort_by?: PerformanceSortBy | string | null;
  sections?: PerformanceSectionName[] | null;
}

export interface PerfTopUrlItem {
  url: string;
  requests: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
}

export interface PerfTopAsnItem {
  asn: string;
  label: string;
  requests: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
}

export interface PerfTtlDistItem {
  bucket: string;
  count: number;
}

export interface PerfWaterfallAvg {
  edge_processing: number;
  origin_wait: number;
  origin_download: number;
  client_download: number;
}

export interface PerfScatterItem {
  origin: number;
  edge: number;
  cache: string;
}

export interface PerformanceAggregatesResponse {
  top_urls?: PerfTopUrlItem[];
  top_asns?: PerfTopAsnItem[];
  ttl_dist?: PerfTtlDistItem[];
  waterfall?: { avg: PerfWaterfallAvg | null };
  scatter?: PerfScatterItem[];
  _approx?: boolean;
}

export interface PerformanceEngineClient {
  getAvailableColumns: () => Promise<ReadonlySet<string>>;
  tryRollup?: (
    name: string,
    opts: { startTime: string; endTime: string; serviceId: string },
  ) => Promise<Record<string, unknown>[] | null>;
  query: (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;
}

let customPerformanceClient: PerformanceEngineClient | null = null;

export function setPerformanceEngineClient(client: PerformanceEngineClient | null): void {
  customPerformanceClient = client;
}

export async function getPerformanceEngineClient(context?: DuckDBReadContext): Promise<PerformanceEngineClient> {
  if (customPerformanceClient) return customPerformanceClient;
  const read = context ?? getDuckDBEngine().readContext();
  const engine = read as unknown as {
    querySql?: (sql: string) => Promise<Record<string, unknown>[]>;
    executeSql?: (sql: string) => Promise<Record<string, unknown>[]>;
    queryRollup?: (
      name: string,
      opts: { startTime: string; endTime: string; serviceId: string },
    ) => Promise<Record<string, unknown>[] | null>;
  };
  return {
    async getAvailableColumns(): Promise<ReadonlySet<string>> {
      if (typeof engine.querySql === "function") {
        const rows = await engine.querySql("DESCRIBE SELECT * FROM requests").catch(() => {
          read.assertCurrent();
          return [];
        });
        return new Set(rows.map((r) => String(r.column_name ?? "")).filter(Boolean));
      }
      return new Set(["timestamp", "elapsed", "ttfb", "ottfb", "ottlb", "cache", "ttl", "url", "asn"]);
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

function hasActiveFilters(filters: FilterSpec | null | undefined): boolean {
  if (!filters || typeof filters !== "object") return false;
  for (const [key, val] of Object.entries(filters)) {
    if (key === "filter_mode" || val == null) continue;
    if (Array.isArray(val) && val.length > 0) return true;
    if (typeof val === "object" && "values" in val && Array.isArray(val.values) && val.values.length > 0) {
      return true;
    }
  }
  return false;
}

export function expandPerformanceSections(
  sections?: readonly string[] | null,
): Set<PerformanceSectionName> {
  if (!sections || sections.length === 0) {
    return new Set(ALL_PERFORMANCE_SECTIONS);
  }
  const out = new Set<PerformanceSectionName>();
  for (const s of sections) {
    if ((ALL_PERFORMANCE_SECTIONS as readonly string[]).includes(s)) {
      out.add(s as PerformanceSectionName);
    }
  }
  if (out.has("top_urls") || out.has("top_asns")) {
    out.add("top_urls");
    out.add("top_asns");
  }
  if (out.has("waterfall") || out.has("scatter")) {
    out.add("waterfall");
    out.add("scatter");
  }
  return out.size > 0 ? out : new Set(ALL_PERFORMANCE_SECTIONS);
}

export function resolveSortColumn(sortBy?: string | null): string {
  const key = (sortBy ?? "p99") as PerformanceSortBy;
  return SORT_COLUMNS[key] ?? SORT_COLUMNS.p99;
}

function buildPerformanceBaseWhere(
  req: PerformanceRequest,
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

export function buildTtlBucketCaseSql(colExpr = "ttl_val"): string {
  return `CASE
    WHEN ${colExpr} < 60 THEN '< 60s'
    WHEN ${colExpr} < 3600 THEN '1m - 1h'
    WHEN ${colExpr} < 86400 THEN '1h - 1d'
    ELSE '> 1d'
  END`;
}

export function buildPerfTopUrlsSql(
  req: PerformanceRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildPerformanceBaseWhere(req, availableColumns);
  const sortColumn = resolveSortColumn(req.sort_by);
  const sql = `
    SELECT
      CAST(url AS VARCHAR) AS url,
      COUNT(*) AS req_count,
      AVG(TRY_CAST(elapsed AS DOUBLE) / 1000.0) AS avg_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.5) AS p50_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.95) AS p95_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.99) AS p99_ms
    FROM logs
    WHERE (${whereSql})
      AND url IS NOT NULL
      AND CAST(url AS VARCHAR) != ''
      AND TRY_CAST(elapsed AS DOUBLE) IS NOT NULL
    GROUP BY url
    HAVING COUNT(*) > ${MIN_URL_REQUESTS}
    ORDER BY ${sortColumn} DESC NULLS LAST
    LIMIT ${PERF_TOP_N}
  `.trim();
  return { sql, params };
}

export function buildPerfTopAsnsSql(
  req: PerformanceRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildPerformanceBaseWhere(req, availableColumns);
  const sortColumn = resolveSortColumn(req.sort_by);
  const sql = `
    SELECT
      CAST(asn AS VARCHAR) AS asn,
      COUNT(*) AS req_count,
      AVG(TRY_CAST(elapsed AS DOUBLE) / 1000.0) AS avg_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.5) AS p50_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.95) AS p95_ms,
      approx_quantile(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 0.99) AS p99_ms
    FROM logs
    WHERE (${whereSql})
      AND asn IS NOT NULL
      AND CAST(asn AS VARCHAR) != ''
      AND TRY_CAST(elapsed AS DOUBLE) IS NOT NULL
    GROUP BY asn
    HAVING COUNT(*) > ${MIN_ASN_REQUESTS}
    ORDER BY ${sortColumn} DESC NULLS LAST
    LIMIT ${PERF_TOP_N}
  `.trim();
  return { sql, params };
}

export function buildPerfTtlDistSql(
  req: PerformanceRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildPerformanceBaseWhere(req, availableColumns);
  const bucketCase = buildTtlBucketCaseSql("ttl_val");
  const sql = `
    SELECT
      ${bucketCase} AS bucket,
      COUNT(*) AS count,
      MIN(ttl_val) AS min_ttl
    FROM (
      SELECT TRY_CAST(ttl AS DOUBLE) AS ttl_val
      FROM logs
      WHERE (${whereSql})
    )
    WHERE ttl_val IS NOT NULL
    GROUP BY bucket
    ORDER BY min_ttl ASC
  `.trim();
  return { sql, params };
}

export function buildPerfWaterfallSql(
  req: PerformanceRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildPerformanceBaseWhere(req, availableColumns);
  const hasOttfb = !availableColumns || availableColumns.has("ottfb");
  const hasOttlb = !availableColumns || availableColumns.has("ottlb");
  const ottfbExpr = hasOttfb ? "COALESCE(TRY_CAST(ottfb AS DOUBLE), 0.0) / 1000.0" : "0.0";
  const ottlbExpr = hasOttlb ? "COALESCE(TRY_CAST(ottlb AS DOUBLE), 0.0) / 1000.0" : "0.0";

  const sql = `
    SELECT
      COUNT(*) AS component_count,
      SUM(GREATEST(0.0, ttfb_ms - ottfb_ms)) / NULLIF(COUNT(*), 0) AS edge_processing,
      SUM(ottfb_ms) / NULLIF(COUNT(*), 0) AS origin_wait,
      SUM(GREATEST(0.0, ottlb_ms - ottfb_ms)) / NULLIF(COUNT(*), 0) AS origin_download,
      SUM(GREATEST(0.0, elapsed_ms - GREATEST(ottlb_ms, ttfb_ms))) / NULLIF(COUNT(*), 0) AS client_download
    FROM (
      SELECT
        TRY_CAST(elapsed AS DOUBLE) / 1000.0 AS elapsed_ms,
        TRY_CAST(ttfb AS DOUBLE) * 1000.0 AS ttfb_ms,
        ${ottfbExpr} AS ottfb_ms,
        ${ottlbExpr} AS ottlb_ms
      FROM logs
      WHERE (${whereSql})
    )
    WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL
  `.trim();
  return { sql, params };
}

export function buildPerfScatterSql(
  req: PerformanceRequest,
  availableColumns?: ReadonlySet<string>,
): { sql: string; params: unknown[] } {
  const { whereSql, params } = buildPerformanceBaseWhere(req, availableColumns);
  const cacheExpr = !availableColumns || availableColumns.has("cache") ? "COALESCE(CAST(cache AS VARCHAR), '')" : "''";
  const sql = `
    SELECT
      ttfb_ms AS origin_ms,
      elapsed_ms - ttfb_ms AS edge_ms,
      cache
    FROM (
      SELECT
        TRY_CAST(elapsed AS DOUBLE) / 1000.0 AS elapsed_ms,
        TRY_CAST(ttfb AS DOUBLE) * 1000.0 AS ttfb_ms,
        ${cacheExpr} AS cache,
        hash(COALESCE(CAST(timestamp AS VARCHAR), '') || '|' || COALESCE(CAST(url AS VARCHAR), '')) AS sample_key
      FROM logs
      WHERE (${whereSql})
    )
    WHERE elapsed_ms IS NOT NULL
      AND ttfb_ms IS NOT NULL
      AND (elapsed_ms - ttfb_ms) >= 0
    ORDER BY sample_key ASC
    LIMIT ${SCATTER_SAMPLE_SIZE}
  `.trim();
  return { sql, params };
}

export function shapePerfTopUrlRow(row: Record<string, unknown>): PerfTopUrlItem {
  const url = String(row.url ?? row.value ?? "");
  return {
    url,
    requests: Math.trunc(toNum(row.req_count ?? row.requests)),
    avg: roundTo(toNum(row.avg_ms ?? row.avg), 2),
    p50: roundTo(toNum(row.p50_ms ?? row.p50), 2),
    p95: roundTo(toNum(row.p95_ms ?? row.p95), 2),
    p99: roundTo(toNum(row.p99_ms ?? row.p99), 2),
  };
}

export function shapePerfTopUrlRows(rows: Record<string, unknown>[]): PerfTopUrlItem[] {
  return rows.map(shapePerfTopUrlRow);
}

export function shapePerformanceAsnRows(rows: Record<string, unknown>[]): PerfTopAsnItem[] {
  return rows.map((row) => {
    const rawAsn = row.asn ?? row.value ?? "";
    const asnStr = typeof rawAsn === "number" ? String(Math.trunc(rawAsn)) : String(rawAsn ?? "").trim();
    const existingLabel = typeof row.label === "string" && row.label.trim() !== "" ? row.label : null;
    return {
      asn: asnStr,
      label: existingLabel ?? formatAsnLabel(asnStr),
      requests: Math.trunc(toNum(row.req_count ?? row.requests)),
      avg: roundTo(toNum(row.avg_ms ?? row.avg), 2),
      p50: roundTo(toNum(row.p50_ms ?? row.p50), 2),
      p95: roundTo(toNum(row.p95_ms ?? row.p95), 2),
      p99: roundTo(toNum(row.p99_ms ?? row.p99), 2),
    };
  });
}

export function shapePerfTtlDistRows(rows: Record<string, unknown>[]): PerfTtlDistItem[] {
  return rows.map((row) => ({
    bucket: String(row.bucket ?? ""),
    count: Math.trunc(toNum(row.count)),
  }));
}

export function shapeWaterfallAvg(row: Record<string, unknown> | null | undefined): PerfWaterfallAvg {
  if (!row) {
    return {
      edge_processing: 0.0,
      origin_wait: 0.0,
      origin_download: 0.0,
      client_download: 0.0,
    };
  }
  return {
    edge_processing: Math.max(0.0, roundTo(toNum(row.edge_processing), 2)),
    origin_wait: Math.max(0.0, roundTo(toNum(row.origin_wait), 2)),
    origin_download: Math.max(0.0, roundTo(toNum(row.origin_download), 2)),
    client_download: Math.max(0.0, roundTo(toNum(row.client_download), 2)),
  };
}

export function shapePerfScatterRows(rows: Record<string, unknown>[]): PerfScatterItem[] {
  return rows.slice(0, SCATTER_SAMPLE_SIZE).map((row) => ({
    origin: roundTo(toNum(row.origin_ms ?? row.origin), 2),
    edge: roundTo(toNum(row.edge_ms ?? row.edge), 2),
    cache: String(row.cache ?? ""),
  }));
}

export async function executePerformanceAggregates(
  req: PerformanceRequest,
  context?: DuckDBReadContext,
): Promise<PerformanceAggregatesResponse> {
  const read = context ?? getDuckDBEngine().readContext();
  req = { ...req, customColumnTypes: read.getCustomColumnTypes() };
  const client = await getPerformanceEngineClient(read);
  const availableColumns = await client.getAvailableColumns();
  const sections = expandPerformanceSections(req.sections);
  const filtered = hasActiveFilters(req.filters);
  const window = {
    startTime: req.start_time ?? "1970-01-01T00:00:00Z",
    endTime: req.end_time ?? new Date().toISOString(),
    serviceId: req.service_id ?? "default",
  };

  const response: PerformanceAggregatesResponse = {
    _approx: !filtered,
  };

  const tasks: Promise<void>[] = [];

  if (sections.has("top_urls")) {
    tasks.push(
      (async () => {
        if (!filtered && typeof client.tryRollup === "function") {
          const rollupRows = await client.tryRollup(PERFORMANCE_ROLLUPS.perf_top_urls, window);
          if (rollupRows) {
            response.top_urls = shapePerfTopUrlRows(rollupRows);
            return;
          }
        }
        const { sql, params } = buildPerfTopUrlsSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.top_urls = shapePerfTopUrlRows(rows);
      })(),
    );
  }

  if (sections.has("top_asns")) {
    tasks.push(
      (async () => {
        if (!filtered && typeof client.tryRollup === "function") {
          const rollupRows = await client.tryRollup(PERFORMANCE_ROLLUPS.perf_top_asns, window);
          if (rollupRows) {
            response.top_asns = shapePerformanceAsnRows(rollupRows);
            return;
          }
        }
        const { sql, params } = buildPerfTopAsnsSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.top_asns = shapePerformanceAsnRows(rows);
      })(),
    );
  }

  if (sections.has("ttl_dist")) {
    tasks.push(
      (async () => {
        if (!filtered && typeof client.tryRollup === "function") {
          const rollupRows = await client.tryRollup(PERFORMANCE_ROLLUPS.perf_ttl_dist, window);
          if (rollupRows) {
            response.ttl_dist = shapePerfTtlDistRows(rollupRows);
            return;
          }
        }
        const { sql, params } = buildPerfTtlDistSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.ttl_dist = shapePerfTtlDistRows(rows);
      })(),
    );
  }

  if (sections.has("waterfall")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildPerfWaterfallSql(req, availableColumns);
        const rows = await client.query(sql, params);
        const row = rows[0];
        response.waterfall = {
          avg: row && row.component_count !== 0 ? shapeWaterfallAvg(row) : null,
        };
      })(),
    );
  }

  if (sections.has("scatter")) {
    tasks.push(
      (async () => {
        const { sql, params } = buildPerfScatterSql(req, availableColumns);
        const rows = await client.query(sql, params);
        response.scatter = shapePerfScatterRows(rows);
      })(),
    );
  }

  await Promise.all(tasks);
  return response;
}

async function parsePerformanceRequest(req: Request, url: URL): Promise<PerformanceRequest> {
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
    filters: (body.filters as FilterSpec | undefined) ?? null,
    sort_by: (body.sort_by as string | undefined) ?? url.searchParams.get("sort_by") ?? "p99",
    sections: Array.isArray(body.sections) ? (body.sections as PerformanceSectionName[]) : null,
  };
}

/**
 * Route dispatcher for `/api/performance/aggregates` and `/api/performance`.
 */
export async function handlePerformanceRequest(
  req: Request,
  path?: string,
  url?: URL,
  context?: DuckDBReadContext,
): Promise<Response | null> {
  const parsedUrl = url ?? new URL(req.url, "http://localhost");
  const pathname = (path ?? parsedUrl.pathname).replace(/\/+$/, "");

  if (pathname === "/api/performance/aggregates" || pathname === "/api/performance") {
    const read = context ?? getDuckDBEngine().readContext();
    const perfReq = await parsePerformanceRequest(req, parsedUrl);
    const data = await executePerformanceAggregates(perfReq, read);
    return Response.json(data, { status: 200 });
  }

  return null;
}
