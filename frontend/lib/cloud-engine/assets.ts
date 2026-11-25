import {
  duckSqlString,
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
  type DuckDBReadContext,
} from "./dashboard/duckdb";
import type { FiltersPayload } from "@/types/api";
import type { AssetsSection } from "./rollup-sections";
import { roundTo } from "./engine-utils";
import { analyticsFiltersToSql, rangeServiceSql } from "./query-params";

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
  /** Cloud only: answer just these sections (all when absent or empty). */
  sections?: readonly AssetsSection[] | null;
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
  /** High-Scale's flags: the encoding is exactly gzip/br/deflate; the URL is compressible text. */
  is_compressed?: number | boolean;
  is_compressible?: number | boolean;
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


const SECTION_KEYS: Record<AssetsSection, readonly string[]> = {
  summary: ["missing_fields", "summary", "asset_type_breakdown", "cache_performance", "compression_performance"],
  large_uncompressed_assets: ["large_uncompressed_assets"],
  low_ttl_assets: ["low_ttl_assets"],
};

/** The response keys of the requested sections, every key when none are named; typed whole, as a caller naming sections reads only those. */
export function selectAssetsSections<T extends object>(out: T, sections?: readonly AssetsSection[] | null): T {
  if (!sections?.length) return out;
  const keys = new Set(sections.flatMap((section) => SECTION_KEYS[section] ?? []));
  return Object.fromEntries(Object.entries(out).filter(([key]) => keys.has(key))) as T;
}

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
  let cacheDenomRequests = 0;
  let totalTtfbWeighted = 0;
  let totalOriginWeighted = 0;
  let totalOriginCount = 0;

  const byAsset = new Map<
    string,
    {
      requests: number; bytes: number; hits: number; cacheDenom: number;
      compressed: number; compressible: number; ttfbSum: number;
    }
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
    let compressed = false;
    let compressible = false;

    if (Array.isArray(raw)) {
      assetType = String(raw[0] ?? "API/Dynamic");
      cacheStatus = String(raw[1] ?? "MISS").toUpperCase();
      reqs = Number(raw[2] ?? 0);
      bytesVal = Number(raw[3] ?? 0);
      sumTtfb = Number(raw[4] ?? 0);
      sumOriginMs = Number(raw[5] ?? 0);
      origCount = Number(raw[6] ?? 0);
      encoding = String(raw[7] ?? "uncompressed");
      compressed = Boolean(Number(raw[8] ?? 0));
      compressible = Boolean(Number(raw[9] ?? 0));
    } else {
      const obj = raw as DimRowInput;
      assetType = String(obj.asset_type ?? "API/Dynamic");
      cacheStatus = String(obj.cache_status ?? "MISS").toUpperCase();
      encoding = String(obj.content_encoding ?? "uncompressed");
      compressed = Boolean(Number(obj.is_compressed ?? 0));
      compressible = Boolean(Number(obj.is_compressible ?? 0));
      reqs = Number(obj.requests ?? 0);
      bytesVal = Number(obj.total_bytes ?? obj.bytes ?? 0);
      sumTtfb = Number(obj.sum_ttfb ?? 0);
      sumOriginMs = Number(obj.sum_origin_ms ?? 0);
      origCount = Number(obj.origin_Count ?? obj.origin_count ?? 0);
    }

    totalRequests += reqs;
    totalBytes += bytesVal;
    // Cache hit ratio includes HIT*/MISS*/PASS* variants (HIT-CLUSTER, MISS-CLUSTER, etc.), BYPASS, and ERROR.
    const isHit = cacheStatus.startsWith("HIT");
    const inCacheDenom =
      isHit ||
      cacheStatus.startsWith("MISS") ||
      cacheStatus.startsWith("PASS") ||
      cacheStatus === "BYPASS" ||
      cacheStatus === "ERROR";
    if (isHit) hitRequests += reqs;
    if (inCacheDenom) cacheDenomRequests += reqs;
    totalTtfbWeighted += sumTtfb;
    totalOriginWeighted += sumOriginMs;
    totalOriginCount += origCount;

    const aBucket = byAsset.get(assetType) ?? {
      requests: 0,
      bytes: 0,
      hits: 0,
      cacheDenom: 0,
      compressed: 0,
      compressible: 0,
      ttfbSum: 0,
    };
    aBucket.requests += reqs;
    aBucket.bytes += bytesVal;
    if (isHit) aBucket.hits += reqs;
    if (inCacheDenom) aBucket.cacheDenom += reqs;
    if (compressible) {
      aBucket.compressible += reqs;
      if (compressed) aBucket.compressed += reqs;
    }
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
    cacheDenomRequests > 0 ? roundTo((hitRequests * 100) / cacheDenomRequests, 2) : 0;
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
      cache_hit_ratio: b.cacheDenom > 0 ? b.hits / b.cacheDenom : 0,
      compression_rate: b.compressible > 0 ? b.compressed / b.compressible : 0,
      bytes: b.bytes,
      pct_requests:
        totalRequests > 0 ? roundTo((b.requests * 100) / totalRequests, 2) : 0,
      hit_ratio_pct:
        b.cacheDenom > 0 ? roundTo((b.hits * 100) / b.cacheDenom, 2) : 0,
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
  const clauses: string[] = [rangeServiceSql(params)];
  if (params.filters && !Array.isArray(params.filters)) {
    clauses.push(analyticsFiltersToSql(params.filters as FiltersPayload, params.customColumnTypes));
  }
  for (const f of Array.isArray(params.filters) ? params.filters : []) {
    if (!ALLOWED_FILTER_COLS.has(f.column)) continue;
    const rawVal = String(f.value ?? "");
    const colExpr =
      f.column === "cache_status"
        ? "UPPER(COALESCE(cache_status, cache, 'OTHER'))"
        : f.column === "asset_type"
          ? "derived_asset_type"
          : f.column;
    const op = String(f.operator ?? "=").toUpperCase();
    if (op === "=" || op === "EQ") {
      clauses.push(`${colExpr} = ${duckSqlString(rawVal)}`);
    } else if (op === "!=" || op === "NEQ") {
      clauses.push(`${colExpr} != ${duckSqlString(rawVal)}`);
    } else if (op === "CONTAINS" || op === "LIKE") {
      clauses.push(`${colExpr} ILIKE ${duckSqlString(`%${rawVal}%`)}`);
    }
  }
  return clauses.join(" AND ");
}

const LIKE_ANY = (column: string, exts: readonly string[]) =>
  exts.map((ext) => `${column} LIKE '%${ext}'`).join(" OR ");

/** High-Scale's `_ASSET_TYPE_CASE`: the asset type from the URL's extension. */
const ASSET_TYPE_SQL = `CASE
          WHEN ${LIKE_ANY("url_lower", [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"])} THEN 'Images'
          WHEN ${LIKE_ANY("url_lower", [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".csv", ".zip", ".tar", ".gz", ".tgz", ".rar", ".7z"])} THEN 'Documents'
          WHEN ${LIKE_ANY("url_lower", [".js", ".mjs", ".css"])} THEN 'JavaScript/CSS'
          WHEN ${LIKE_ANY("url_lower", [".woff", ".woff2", ".ttf", ".otf", ".eot"])} THEN 'Fonts'
          WHEN ${LIKE_ANY("url_lower", [".m3u8", ".ts", ".mp4", ".m4s", ".webm"])} THEN 'Video'
          ELSE 'API/Dynamic'
        END`;

/**
 * Assets & Shield over raw rows by High-Scale's rules (`backend/high_scale/ingest_controller/service.py`):
 * URL-extension asset types, an empty cache counted as MISS, `content_encoding_norm`, and compression
 * judged over compressible URLs only. The summary writer (`compute/cloud/src/ingest/summary.rs`) keys
 * its `assets` rows the same way.
 */
const DERIVED_ASSET_CTE = `
  WITH asset_base AS (
    SELECT
      *,
      LOWER(COALESCE(url, path, '')) AS url_lower,
      COALESCE(NULLIF(content_encoding, ''), NULLIF(resp_header_content_encoding, ''), '') AS raw_encoding
    FROM requests
  ),
  enriched AS (
    SELECT
      *,
      ${ASSET_TYPE_SQL} AS derived_asset_type,
      UPPER(COALESCE(NULLIF(cache_status, ''), NULLIF(cache, ''), 'MISS')) AS norm_cache_status,
      CASE
        WHEN LOWER(raw_encoding) LIKE '%br%' THEN 'br'
        WHEN LOWER(raw_encoding) LIKE '%gzip%' THEN 'gzip'
        WHEN raw_encoding != '' THEN raw_encoding
        ELSE 'uncompressed'
      END AS norm_encoding,
      (LOWER(raw_encoding) IN ('gzip', 'br', 'deflate'))::INTEGER AS is_compressed,
      (${LIKE_ANY("split_part(split_part(url_lower, '?', 1), '#', 1)", [".js", ".mjs", ".css", ".svg", ".html", ".json"])})::INTEGER AS is_compressible,
      COALESCE(resp_body_size, resp_bytes, 0)::BIGINT AS body_bytes,
      COALESCE(ttfb_ms, ttfb * 1000.0, time_to_first_byte_ns / 1000000.0, time_elapsed_usec / 1000.0, elapsed / 1000.0, 0.0)::DOUBLE AS norm_ttfb_ms,
      COALESCE(origin_Whole_ms, origin_time_ms, 0.0)::DOUBLE AS norm_origin_ms,
      COALESCE(url, path, '/') AS norm_url
    FROM asset_base
  )
`;

export async function queryAssetsAggregates(params: AssetsQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildAssetsWhere(params);

  const sections = Array.isArray(params.sections) && params.sections.length ? params.sections : null;
  const want = (section: AssetsSection) => !sections || sections.includes(section);
  const dimRows = !want("summary") ? [] : await engine.executeSql<DimRowInput>(
    `${DERIVED_ASSET_CTE}
    SELECT
      derived_asset_type AS asset_type,
      norm_cache_status AS cache_status,
      norm_encoding AS content_encoding,
      is_compressed,
      is_compressible,
      COUNT(*)::BIGINT AS requests,
      COALESCE(SUM(body_bytes), 0)::BIGINT AS total_bytes,
      COALESCE(SUM(norm_ttfb_ms), 0)::DOUBLE AS sum_ttfb,
      COALESCE(SUM(norm_origin_ms), 0)::DOUBLE AS sum_origin_ms,
      (COUNT(*) FILTER (WHERE norm_origin_ms > 0))::BIGINT AS origin_Count
    FROM enriched
    WHERE ${where}
    GROUP BY 1, 2, 3, 4, 5`,
    [],
    params.signal,
  );

  if (want("summary") && dimRows.length === 0) {
    return selectAssetsSections(emptyAssetsAggregates(), sections);
  }

  const computed = computeSummaryFromDims(dimRows);

  // High-Scale's large uncompressed assets: compressible URLs served uncompressed, by bytes.
  const largeUncompressedRows = !want("large_uncompressed_assets") ? [] : await engine.executeSql<Record<string, unknown>>(
    `${DERIVED_ASSET_CTE}
    SELECT
      norm_url AS url,
      ANY_VALUE(derived_asset_type) AS asset_type,
      ANY_VALUE(norm_encoding) AS content_encoding,
      COUNT(*)::BIGINT AS requests,
      AVG(body_bytes)::DOUBLE AS avg_size_bytes,
      COALESCE(status, 200)::INTEGER AS status,
      SUM(body_bytes)::BIGINT AS total_bytes
    FROM enriched
    WHERE ${where}
      AND is_compressed = 0
      AND is_compressible = 1
      AND (${LIKE_ANY("split_part(split_part(url_lower, '?', 1), '#', 1)", [".js", ".mjs", ".css", ".svg", ".html", ".json"])})
      AND COALESCE(status, 200) NOT IN (304, 204)
    GROUP BY norm_url, COALESCE(status, 200)
    HAVING AVG(body_bytes) >= 10240
    ORDER BY total_bytes DESC
    LIMIT 20`,
    [],
    params.signal,
  );

  // High-Scale's low-TTL hotspots: static (non-API) URLs answered with a cache MISS, by requests.
  const lowTtlRows = !want("low_ttl_assets") ? [] : await engine.executeSql<Record<string, unknown>>(
    `${DERIVED_ASSET_CTE}
    SELECT
      norm_url AS url,
      derived_asset_type AS asset_type,
      'MISS' AS cache_status,
      COUNT(*)::BIGINT AS requests,
      AVG(ttl)::DOUBLE AS avg_ttl_seconds,
      0.0::DOUBLE AS hit_ratio_pct
    FROM enriched
    WHERE ${where}
      AND norm_cache_status = 'MISS'
      AND derived_asset_type != 'API/Dynamic'
    GROUP BY 1, 2
    HAVING AVG(ttl) < 3600
    ORDER BY requests DESC
    LIMIT 20`,
    [],
    params.signal,
  );

  const present = want("summary") ? await engine.getPresentColumns() : new Set<string>();
  return selectAssetsSections({
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
      asset_type: String(r.asset_type ?? "API/Dynamic"),
      content_encoding: String(r.content_encoding ?? "uncompressed"),
      requests: Number(r.requests ?? 0),
      avg_size_bytes: roundTo(Number(r.avg_size_bytes ?? 0), 1),
      avg_bytes: roundTo(Number(r.avg_size_bytes ?? 0), 1),
      status: Number(r.status ?? 0),
      total_bytes: Number(r.total_bytes ?? 0),
    })),
    low_ttl_assets: lowTtlRows.map((r) => ({
      url: String(r.url ?? "/"),
      asset_type: String(r.asset_type ?? "API/Dynamic"),
      cache_status: String(r.cache_status ?? "MISS"),
      requests: Number(r.requests ?? 0),
      avg_ttl_seconds: roundTo(Number(r.avg_ttl_seconds ?? 0), 1),
      avg_ttl: roundTo(Number(r.avg_ttl_seconds ?? 0), 1),
      hit_ratio_pct: roundTo(Number(r.hit_ratio_pct ?? 0), 2),
    })),
  }, sections);
}

/** Every request column the /api/assets/aggregates scans read, physical Cloud names and their aliases alike. */
export const ASSETS_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "service_id", "status", "ttl", "url", "path", "cache_status",
  "cache", "content_encoding", "resp_header_content_encoding", "resp_body_size", "resp_bytes", "ttfb_ms", "ttfb",
  "time_to_first_byte_ns", "time_elapsed_usec", "elapsed", "origin_Whole_ms", "origin_time_ms",
] as const;

// The window, service and `derived_asset_type`/`norm_url` columns every section reads, then each section's own.
const ASSETS_BASE_COLUMNS = ["timestamp", "ts_ms", "service_id", "url", "path"] as const;
const ENCODING_BYTES_COLUMNS = ["content_encoding", "resp_header_content_encoding", "resp_body_size", "resp_bytes"] as const;
const ASSETS_SECTION_COLUMNS: Record<AssetsSection, readonly string[]> = {
  summary: ASSETS_SCAN_COLUMNS.filter((column) => column !== "status" && column !== "ttl"),
  large_uncompressed_assets: [...ENCODING_BYTES_COLUMNS, "status"],
  low_ttl_assets: ["ttl", "cache_status", "cache"],
};

/**
 * The columns a projected /api/assets/aggregates load needs, or null when the filters can't name theirs.
 * Named `sections` read only their own columns; the Cloud view lists the rest as NULL.
 */
export function assetsScanColumns(filters: unknown, sections?: unknown): string[] | null {
  const named = Array.isArray(sections) && sections.length &&
    sections.every((section) => Object.hasOwn(ASSETS_SECTION_COLUMNS, section)) ? sections as AssetsSection[] : null;
  const columns: string[] = named
    ? [...ASSETS_BASE_COLUMNS, ...named.flatMap((section) => ASSETS_SECTION_COLUMNS[section])]
    : [...ASSETS_SCAN_COLUMNS];
  if (filters == null || filters === "") return [...new Set(columns)];
  if (typeof filters !== "object") return null;
  if (!Array.isArray(filters)) return [...new Set([...columns, ...Object.keys(filters)])];
  const fields: string[] = [];
  for (const clause of filters as unknown[]) {
    if (!clause || typeof clause !== "object") return null;
    const column = (clause as { column?: unknown }).column;
    // `buildAssetsWhere` reads a `cache_status` filter through `COALESCE(cache_status, cache, ...)`.
    // An `asset_type` filter matches the URL-derived type, so it reads no column of its own.
    if (typeof column === "string" && ALLOWED_FILTER_COLS.has(column) && column !== "asset_type") {
      fields.push(column, ...column === "cache_status" ? ["cache"] : []);
    }
  }
  return [...new Set([...columns, ...fields])];
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
