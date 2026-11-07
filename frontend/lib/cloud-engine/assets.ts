import {
  getDuckDBEngine,
  compileFiltersToSql,
  type DuckDBAnalyticalEngine,
  type DuckDBReadContext,
} from "./dashboard/duckdb";
import type { FiltersPayload } from "@/types/api";

export const LARGE_UNCOMPRESSED_THRESHOLD_BYTES = 10240; // 10 KB
export const LOW_TTL_THRESHOLD_SECONDS = 3600; // 1 hour

export interface AssetFilterClause {
  column: string;
  operator: string;
  value: string;
}

export interface AssetsQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  filters?: readonly AssetFilterClause[] | FiltersPayload;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns"> & Partial<Pick<DuckDBReadContext, "getCustomColumnTypes">>;
  customColumnTypes?: ReturnType<DuckDBReadContext["getCustomColumnTypes"]>;
  signal?: AbortSignal;
}

export interface DimRowInput {
  asset_type?: string | null;
  cache_status?: string | null;
  content_encoding?: string | null;
  requests?: number;
  total_bytes?: number;
  bytes?: number;
  sum_ttfb?: number;
  sum_origin_ms?: number;
  origin_Count?: number;
  origin_count?: number;
}

function roundTo(val: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

export function emptyAssetsAggregates() {
  return {
    summary: {
      total_requests: 0,
      total_bytes: 0,
      overall_hit_ratio: 0,
      overall_hit_ratio_pct: 0,
      avg_ttfb_ms: 0,
      avg_origin_ms: 0,
    },
    asset_type_breakdown: [] as Record<string, unknown>[],
    cache_performance: [] as Record<string, unknown>[],
    compression_performance: [] as Record<string, unknown>[],
    large_uncompressed_assets: [] as Record<string, unknown>[],
    low_ttl_assets: [] as Record<string, unknown>[],
  };
}

export const empty_assets_aggregates = emptyAssetsAggregates;

/**
 * Computes summary metrics and breakdowns (asset_type_breakdown,
 * cache_performance, compression_performance) from dimension rows.
 * Supports both object rows and 7-element tuples.
 */
export function computeSummaryFromDims(
  dimRows: readonly (DimRowInput | readonly unknown[])[],
) {
  if (!dimRows || dimRows.length === 0) {
    return {
      total_requests: 0,
      total_bytes: 0,
      overall_hit_ratio: 0,
      overall_hit_ratio_pct: 0,
      avg_ttfb_ms: 0,
      avg_origin_ms: 0,
      asset_type_breakdown: [] as Record<string, unknown>[],
      cache_performance: [] as Record<string, unknown>[],
      compression_performance: [] as Record<string, unknown>[],
    };
  }

  let totalRequests = 0;
  let totalBytes = 0;
  let hitRequests = 0;
  let totalTtfbWeighted = 0;
  let totalOriginWeighted = 0;
  let totalOriginCount = 0;

  const byAsset = new Map<
    string,
    { requests: number; bytes: number; hits: number; compressed: number; ttfbSum: number }
  >();
  const byCache = new Map<
    string,
    { asset_type: string; cache_status: string; requests: number; bytes: number; ttfbSum: number }
  >();
  const byCompression = new Map<
    string,
    { asset_type: string; content_encoding: string; requests: number; bytes: number; ttfbSum: number }
  >();

  for (const raw of dimRows) {
    let assetType: string;
    let cacheStatus: string;
    let encoding: string;
    let reqs: number;
    let bytesVal: number;
    let sumTtfb: number;
    let sumOriginMs: number;
    let origCount: number;

    if (Array.isArray(raw)) {
      assetType = String(raw[0] ?? "Other");
      cacheStatus = String(raw[1] ?? "OTHER").toUpperCase();
      reqs = Number(raw[2] ?? 0);
      bytesVal = Number(raw[3] ?? 0);
      sumTtfb = Number(raw[4] ?? 0);
      sumOriginMs = Number(raw[5] ?? 0);
      origCount = Number(raw[6] ?? 0);
      encoding = String(raw[7] ?? "none").toLowerCase();
    } else {
      const obj = raw as DimRowInput;
      assetType = String(obj.asset_type ?? "Other");
      cacheStatus = String(obj.cache_status ?? "OTHER").toUpperCase();
      encoding = String(obj.content_encoding ?? "none").toLowerCase();
      reqs = Number(obj.requests ?? 0);
      bytesVal = Number(obj.total_bytes ?? obj.bytes ?? 0);
      sumTtfb = Number(obj.sum_ttfb ?? 0);
      sumOriginMs = Number(obj.sum_origin_ms ?? 0);
      origCount = Number(obj.origin_Count ?? obj.origin_count ?? 0);
    }

    totalRequests += reqs;
    totalBytes += bytesVal;
    const isHit = cacheStatus === "HIT" || cacheStatus.startsWith("HIT");
    if (isHit) hitRequests += reqs;
    totalTtfbWeighted += sumTtfb;
    totalOriginWeighted += sumOriginMs;
    totalOriginCount += origCount;

    const aBucket = byAsset.get(assetType) ?? {
      requests: 0,
      bytes: 0,
      hits: 0,
      compressed: 0,
      ttfbSum: 0,
    };
    aBucket.requests += reqs;
    aBucket.bytes += bytesVal;
    if (isHit) aBucket.hits += reqs;
    if (["gzip", "br", "zstd", "deflate"].includes(encoding)) aBucket.compressed += reqs;
    aBucket.ttfbSum += sumTtfb;
    byAsset.set(assetType, aBucket);

    const cacheKey = JSON.stringify([assetType, cacheStatus]);
    const cBucket = byCache.get(cacheKey) ?? {
      asset_type: assetType,
      cache_status: cacheStatus,
      requests: 0,
      bytes: 0,
      ttfbSum: 0,
    };
    cBucket.requests += reqs;
    cBucket.bytes += bytesVal;
    cBucket.ttfbSum += sumTtfb;
    byCache.set(cacheKey, cBucket);

    const encodingKey = JSON.stringify([assetType, encoding]);
    const encBucket = byCompression.get(encodingKey) ?? {
      asset_type: assetType,
      content_encoding: encoding,
      requests: 0,
      bytes: 0,
      ttfbSum: 0,
    };
    encBucket.requests += reqs;
    encBucket.bytes += bytesVal;
    encBucket.ttfbSum += sumTtfb;
    byCompression.set(encodingKey, encBucket);
  }

  const hitRatioPct =
    totalRequests > 0 ? roundTo((hitRequests * 100) / totalRequests, 2) : 0;
  const avgTtfbMs =
    totalRequests > 0 ? roundTo(totalTtfbWeighted / totalRequests, 2) : 0;
  const avgOriginMs =
    totalOriginCount > 0 ? roundTo(totalOriginWeighted / totalOriginCount, 2) : 0;

  const asset_type_breakdown = [...byAsset.entries()]
    .sort((a, b) => b[1].requests - a[1].requests)
    .map(([asset_type, b]) => ({
      asset_type,
      requests: b.requests,
      total_bytes: b.bytes,
      egress_bytes: b.bytes,
      cache_hit_ratio: b.requests > 0 ? b.hits / b.requests : 0,
      compression_rate: b.requests > 0 ? b.compressed / b.requests : 0,
      bytes: b.bytes,
      pct_requests:
        totalRequests > 0 ? roundTo((b.requests * 100) / totalRequests, 2) : 0,
      hit_ratio_pct:
        b.requests > 0 ? roundTo((b.hits * 100) / b.requests, 2) : 0,
      avg_ttfb_ms:
        b.requests > 0 ? roundTo(b.ttfbSum / b.requests, 2) : 0,
    }));

  const cache_performance = [...byCache.entries()]
    .sort((a, b) => b[1].requests - a[1].requests)
    .map(([, b]) => ({
      asset_type: b.asset_type,
      cache_status: b.cache_status,
      requests: b.requests,
      total_bytes: b.bytes,
      bytes: b.bytes,
      pct_requests:
        totalRequests > 0 ? roundTo((b.requests * 100) / totalRequests, 2) : 0,
      avg_ttfb_ms:
        b.requests > 0 ? roundTo(b.ttfbSum / b.requests, 2) : 0,
    }));

  const compression_performance = [...byCompression.entries()]
    .sort((a, b) => b[1].requests - a[1].requests)
    .map(([, b]) => ({
      asset_type: b.asset_type,
      content_encoding: b.content_encoding,
      requests: b.requests,
      total_bytes: b.bytes,
      bytes: b.bytes,
      pct_requests:
        totalRequests > 0 ? roundTo((b.requests * 100) / totalRequests, 2) : 0,
      avg_size_bytes:
        b.requests > 0 ? roundTo(b.bytes / b.requests, 1) : 0,
    }));

  return {
    total_requests: totalRequests,
    total_bytes: totalBytes,
    overall_hit_ratio: hitRatioPct,
    overall_hit_ratio_pct: hitRatioPct,
    avg_ttfb_ms: avgTtfbMs,
    avg_origin_ms: avgOriginMs,
    asset_type_breakdown,
    cache_performance,
    compression_performance,
  };
}

export const compute_summary_from_dims = computeSummaryFromDims;

function parseEpochMs(val: string | number | undefined): number | null {
  if (val === undefined || val === null || val === "") return null;
  if (typeof val === "number") {
    return val < 1e12 ? Math.floor(val * 1000) : Math.floor(val);
  }
  const num = Number(val);
  if (Number.isFinite(num)) {
    return num < 1e12 ? Math.floor(num * 1000) : Math.floor(num);
  }
  const parsed = Date.parse(val);
  return Number.isFinite(parsed) ? parsed : null;
}

const ALLOWED_FILTER_COLS = new Set([
  "asset_type",
  "cache_status",
  "content_encoding",
  "content_type",
  "host",
  "url",
  "path",
  "status",
  "fastly_pop",
  "geo_country",
]);

function buildAssetsWhere(params: AssetsQueryParams): string {
  const clauses: string[] = ["1=1"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms < ${endMs}`);
  if (params.service_id) {
    const escaped = String(params.service_id).replaceAll("'", "''");
    clauses.push(`(service_id = '${escaped}' OR service_id IS NULL)`);
  }
  if (params.filters && !Array.isArray(params.filters)) {
    clauses.push(compileFiltersToSql(Object.entries(params.filters as FiltersPayload).flatMap(([field, spec]) =>
      spec.values.map((value) => ({ field, value: String(value), mode: spec.mode })),
    ), false, [], params.customColumnTypes));
  }
  for (const f of Array.isArray(params.filters) ? params.filters : []) {
    if (!ALLOWED_FILTER_COLS.has(f.column)) continue;
    const escapedVal = String(f.value ?? "").replaceAll("'", "''");
    const colExpr =
      f.column === "cache_status"
        ? "UPPER(COALESCE(cache_status, cache, 'OTHER'))"
        : f.column === "asset_type"
          ? "derived_asset_type"
          : f.column;
    const op = String(f.operator ?? "=").toUpperCase();
    if (op === "=" || op === "EQ") {
      clauses.push(`${colExpr} = '${escapedVal}'`);
    } else if (op === "!=" || op === "NEQ") {
      clauses.push(`${colExpr} != '${escapedVal}'`);
    } else if (op === "CONTAINS" || op === "LIKE") {
      clauses.push(`${colExpr} ILIKE '%${escapedVal}%'`);
    }
  }
  return clauses.join(" AND ");
}

const DERIVED_ASSET_CTE = `
  WITH enriched AS (
    SELECT
      *,
      COALESCE(
        NULLIF(asset_type, ''),
        CASE
          WHEN LOWER(COALESCE(content_type, '')) LIKE '%javascript%' OR LOWER(COALESCE(url, path, '')) LIKE '%.js' THEN 'JavaScript'
          WHEN LOWER(COALESCE(content_type, '')) LIKE '%text/css%' OR LOWER(COALESCE(url, path, '')) LIKE '%.css' THEN 'CSS'
          WHEN LOWER(COALESCE(content_type, '')) LIKE 'image/%' THEN 'Image'
          WHEN LOWER(COALESCE(content_type, '')) LIKE 'font/%' OR LOWER(COALESCE(url, path, '')) LIKE '%.woff%' THEN 'Font'
          WHEN LOWER(COALESCE(content_type, '')) LIKE 'video/%' OR LOWER(COALESCE(content_type, '')) LIKE 'audio/%' THEN 'Media'
          WHEN LOWER(COALESCE(content_type, '')) LIKE '%application/json%' THEN 'JSON'
          WHEN LOWER(COALESCE(content_type, '')) LIKE '%text/html%' THEN 'HTML'
          ELSE 'Other'
        END
      ) AS derived_asset_type,
      UPPER(COALESCE(cache_status, cache, 'OTHER')) AS norm_cache_status,
      LOWER(COALESCE(NULLIF(content_encoding, ''), NULLIF(resp_header_content_encoding, ''), 'unknown')) AS norm_encoding,
      COALESCE(resp_body_size, resp_bytes, 0)::BIGINT AS body_bytes,
      COALESCE(ttfb_ms, ttfb * 1000.0, time_to_first_byte_ns / 1000000.0, time_elapsed_usec / 1000.0, elapsed / 1000.0, 0.0)::DOUBLE AS norm_ttfb_ms,
      COALESCE(origin_Whole_ms, origin_time_ms, 0.0)::DOUBLE AS norm_origin_ms,
      COALESCE(url, path, '/') AS norm_url
    FROM requests
  )
`;

export async function queryAssetsAggregates(params: AssetsQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildAssetsWhere(params);

  const dimRows = await engine.executeSql<DimRowInput>(
    `${DERIVED_ASSET_CTE}
    SELECT
      derived_asset_type AS asset_type,
      norm_cache_status AS cache_status,
      norm_encoding AS content_encoding,
      COUNT(*)::BIGINT AS requests,
      COALESCE(SUM(body_bytes), 0)::BIGINT AS total_bytes,
      COALESCE(SUM(norm_ttfb_ms), 0)::DOUBLE AS sum_ttfb,
      COALESCE(SUM(norm_origin_ms), 0)::DOUBLE AS sum_origin_ms,
      (COUNT(*) FILTER (WHERE norm_origin_ms > 0))::BIGINT AS origin_Count
    FROM enriched
    WHERE ${where}
    GROUP BY 1, 2, 3`,
    [],
    params.signal,
  );

  if (dimRows.length === 0) {
    return emptyAssetsAggregates();
  }

  const computed = computeSummaryFromDims(dimRows);

  // Large uncompressed text assets (JavaScript/CSS/HTML/JSON with avg size > 10240 bytes and identity/none encoding)
  const largeUncompressedRows = await engine.executeSql<Record<string, unknown>>(
    `${DERIVED_ASSET_CTE}
    SELECT
      norm_url AS url,
      derived_asset_type AS asset_type,
      norm_encoding AS content_encoding,
      COUNT(*)::BIGINT AS requests,
      AVG(body_bytes)::DOUBLE AS avg_size_bytes,
      MIN(status)::INTEGER AS status,
      SUM(body_bytes)::BIGINT AS total_bytes
    FROM enriched
    WHERE ${where}
      AND derived_asset_type IN ('JavaScript', 'CSS', 'HTML', 'JSON')
      AND norm_encoding IN ('none', 'identity', '')
    GROUP BY 1, 2, 3
    HAVING AVG(body_bytes) > 10240
    ORDER BY total_bytes DESC
    LIMIT 50`,
    [],
    params.signal,
  );

  // Low TTL static assets (ttl < 3600s and not cache hit)
  const lowTtlRows = await engine.executeSql<Record<string, unknown>>(
    `${DERIVED_ASSET_CTE}
    SELECT
      norm_url AS url,
      derived_asset_type AS asset_type,
      norm_cache_status AS cache_status,
      COUNT(*)::BIGINT AS requests,
      AVG(ttl)::DOUBLE AS avg_ttl_seconds,
      (COUNT(*) FILTER (WHERE starts_with(norm_cache_status, 'HIT')) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS hit_ratio_pct
    FROM enriched
    WHERE ${where}
      AND ttl < 3600
      AND NOT starts_with(norm_cache_status, 'HIT')
    GROUP BY 1, 2, 3
    ORDER BY requests DESC
    LIMIT 50`,
    [],
    params.signal,
  );

  const present = await engine.getPresentColumns();
  return {
    missing_fields: [
      ...(!present.has("ttl") ? ["ttl"] : []),
      ...(!present.has("content_encoding") && !present.has("resp_header_content_encoding") ? ["resp_header_content_encoding"] : []),
    ],
    summary: {
      total_requests: computed.total_requests,
      total_bytes: computed.total_bytes,
      overall_hit_ratio: computed.overall_hit_ratio,
      overall_hit_ratio_pct: computed.overall_hit_ratio_pct,
      avg_ttfb_ms: computed.avg_ttfb_ms,
      avg_origin_ms: computed.avg_origin_ms,
    },
    asset_type_breakdown: computed.asset_type_breakdown,
    cache_performance: computed.cache_performance,
    compression_performance: computed.compression_performance,
    large_uncompressed_assets: largeUncompressedRows.map((r) => ({
      url: String(r.url ?? "/"),
      asset_type: String(r.asset_type ?? "Other"),
      content_encoding: String(r.content_encoding ?? "none"),
      requests: Number(r.requests ?? 0),
      avg_size_bytes: roundTo(Number(r.avg_size_bytes ?? 0), 1),
      avg_bytes: roundTo(Number(r.avg_size_bytes ?? 0), 1),
      status: Number(r.status ?? 0),
      total_bytes: Number(r.total_bytes ?? 0),
    })),
    low_ttl_assets: lowTtlRows.map((r) => ({
      url: String(r.url ?? "/"),
      asset_type: String(r.asset_type ?? "Other"),
      cache_status: String(r.cache_status ?? "MISS"),
      requests: Number(r.requests ?? 0),
      avg_ttl_seconds: roundTo(Number(r.avg_ttl_seconds ?? 0), 1),
      avg_ttl: roundTo(Number(r.avg_ttl_seconds ?? 0), 1),
      hit_ratio_pct: roundTo(Number(r.hit_ratio_pct ?? 0), 2),
    })),
  };
}

export async function handleAssetsRequest(
  path: string,
  params: AssetsQueryParams = {},
) {
  if (path.includes("/api/assets/aggregates")) {
    return queryAssetsAggregates(params);
  }
  return queryAssetsAggregates(params);
}
