import {
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
} from "./dashboard/duckdb";

export const IO_LARGE_IMAGE_THRESHOLD_BYTES = 51200; // 50 * 1024

export interface ValueQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">;
  signal?: AbortSignal;
}

function roundTo(val: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

export function emptyValueSummary() {
  return {
    overview: {
      total_requests: 0,
      total_bytes: 0,
      unique_countries: 0,
      unique_pops: 0,
      unique_asns: 0,
      error_rate_pct: 0,
      availability_pct: 100,
    },
    caching: {
      total_requests: 0,
      cache_hits: 0,
      cache_misses: 0,
      cache_passes: 0,
      hit_ratio_pct: 0,
      origin_offload_pct: 0,
      bandwidth_saved_bytes: 0,
      hit_ttfb_p50_ms: 0,
      miss_ttfb_p50_ms: 0,
      cache_acceleration_factor: 1.0,
      cache_accel_factor: 1.0,
      shield_requests: 0,
      shield_hit_ratio_pct: 0,
    },
    security: {
      tls_13_requests: 0,
      tls_13_pct: 0,
      waf_evaluated_requests: 0,
      waf_blocked_requests: 0,
      ddos_mitigated_requests: 0,
    },
    bots: {
      bot_requests: 0,
      bot_share_pct: 0,
      verified_bot_requests: 0,
      suspected_bot_requests: 0,
      bot_cache_hit_ratio_pct: 0,
      origin_requests_saved_from_bots: 0,
    },
    performance: {
      ttfb_p50_ms: 0,
      ttfb_p95_ms: 0,
      ttfb_p99_ms: 0,
      http3_requests: 0,
      http3_pct: 0,
      http2_requests: 0,
      http2_pct: 0,
      compressed_requests: 0,
      compression_ratio_pct: 0,
    },
    network: {
      unique_pops: 0,
      unique_countries: 0,
      unique_asns: 0,
      avg_rtt_ms: 0,
      median_rtt_ms: 0,
      avg_ploss_pct: 0,
    },
    io: {
      total_images: 0,
      modern_format_images: 0,
      modern_format_pct: 0,
      legacy_format_images: 0,
      io_transformed_images: 0,
      unoptimized_large_images: 0,
      estimated_savings_bytes: 0,
    },
  };
}

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

function buildWhereClause(params: ValueQueryParams): string {
  const clauses: string[] = ["1=1"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms <= ${endMs}`);
  if (params.service_id) {
    const escaped = String(params.service_id).replaceAll("'", "''");
    clauses.push(`(service_id = '${escaped}' OR service_id IS NULL)`);
  }
  return clauses.join(" AND ");
}

export async function queryValueSummary(params: ValueQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const where = buildWhereClause(params);

  const rows = await engine.executeSql<Record<string, unknown>>(
    `WITH normalized AS (
      SELECT
        status,
        UPPER(COALESCE(cache_status, cache, '')) AS c_status,
        COALESCE(resp_body_size, resp_bytes, 0)::BIGINT AS body_bytes,
        COALESCE(ttfb_ms, time_to_first_byte_ns / 1000000.0, time_elapsed_usec / 1000.0) AS ttfb,
        COALESCE(geo_country, country) AS country_code,
        COALESCE(fastly_pop, pop) AS pop_code,
        COALESCE(client_asn, asn) AS asn_val,
        COALESCE(tls_version, tls) AS tls_ver,
        UPPER(COALESCE(protocol, '')) AS proto,
        LOWER(COALESCE(content_type, '')) AS c_type,
        LOWER(COALESCE(content_encoding, '')) AS c_enc,
        LOWER(COALESCE(url, path, '')) AS req_url,
        COALESCE( fastly_is_shield, is_shield, FALSE ) AS shield_flag,
        waf_action,
        ngwaf_decision,
        bot_category,
        bot_verified,
        COALESCE(client_rtt_us, client_tcpi_rtt) AS rtt_us,
        client_ploss
      FROM requests
      WHERE ${where}
    )
    SELECT
      COUNT(*)::BIGINT AS total_requests,
      COALESCE(SUM(body_bytes), 0)::BIGINT AS total_bytes,
      COUNT(DISTINCT country_code)::BIGINT AS unique_countries,
      COUNT(DISTINCT pop_code)::BIGINT AS unique_pops,
      COUNT(DISTINCT asn_val)::BIGINT AS unique_asns,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600))::BIGINT AS s5xx,
      (COUNT(*) FILTER (WHERE starts_with(c_status, 'HIT')))::BIGINT AS cache_hits,
      (COUNT(*) FILTER (WHERE starts_with(c_status, 'MISS')))::BIGINT AS cache_misses,
      (COUNT(*) FILTER (WHERE starts_with(c_status, 'PASS')))::BIGINT AS cache_passes,
      COALESCE(SUM(body_bytes) FILTER (WHERE starts_with(c_status, 'HIT')), 0)::BIGINT AS bandwidth_saved_bytes,
      QUANTILE_CONT(ttfb, 0.5) FILTER (WHERE starts_with(c_status, 'HIT'))::DOUBLE AS hit_ttfb_p50_ms,
      QUANTILE_CONT(ttfb, 0.5) FILTER (WHERE starts_with(c_status, 'MISS'))::DOUBLE AS miss_ttfb_p50_ms,
      (COUNT(*) FILTER (WHERE shield_flag IS TRUE))::BIGINT AS shield_requests,
      (COUNT(*) FILTER (WHERE shield_flag IS TRUE AND starts_with(c_status, 'HIT')))::BIGINT AS shield_hits,
      (COUNT(*) FILTER (WHERE tls_ver ILIKE '%1.3%'))::BIGINT AS tls_13_requests,
      (COUNT(*) FILTER (WHERE waf_action IS NOT NULL OR ngwaf_decision IS NOT NULL))::BIGINT AS waf_evaluated,
      (COUNT(*) FILTER (WHERE UPPER(COALESCE(waf_action, ngwaf_decision, '')) IN ('BLOCK', 'BLOCKED', 'DENY')))::BIGINT AS waf_blocked,
      (COUNT(*) FILTER (WHERE bot_category IS NOT NULL AND bot_category != ''))::BIGINT AS bot_requests,
      (COUNT(*) FILTER (WHERE bot_verified IS TRUE))::BIGINT AS verified_bot_requests,
      (COUNT(*) FILTER (WHERE bot_category IS NOT NULL AND bot_category != '' AND starts_with(c_status, 'HIT')))::BIGINT AS bot_cache_hits,
      QUANTILE_CONT(ttfb, 0.5)::DOUBLE AS ttfb_p50_ms,
      QUANTILE_CONT(ttfb, 0.95)::DOUBLE AS ttfb_p95_ms,
      QUANTILE_CONT(ttfb, 0.99)::DOUBLE AS ttfb_p99_ms,
      (COUNT(*) FILTER (WHERE proto LIKE 'HTTP/3%' OR proto = 'H3'))::BIGINT AS http3_requests,
      (COUNT(*) FILTER (WHERE proto LIKE 'HTTP/2%' OR proto = 'H2'))::BIGINT AS http2_requests,
      (COUNT(*) FILTER (WHERE c_enc IN ('br', 'gzip', 'zstd', 'deflate')))::BIGINT AS compressed_requests,
      AVG(rtt_us)::DOUBLE AS avg_rtt_us,
      QUANTILE_CONT(rtt_us, 0.5)::DOUBLE AS med_rtt_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE starts_with(c_type, 'image/')))::BIGINT AS total_images,
      (COUNT(*) FILTER (WHERE c_type LIKE 'image/webp%' OR c_type LIKE 'image/avif%' OR req_url LIKE '%ifmt=webp%' OR req_url LIKE '%ifmt=avif%' OR req_url LIKE '%format=webp%' OR req_url LIKE '%format=avif%'))::BIGINT AS modern_format_images,
      (COUNT(*) FILTER (WHERE c_type LIKE 'image/jpeg%' OR c_type LIKE 'image/jpg%' OR c_type LIKE 'image/png%' OR c_type LIKE 'image/gif%'))::BIGINT AS legacy_format_images,
      (COUNT(*) FILTER (WHERE req_url LIKE '%ifmt=%' OR req_url LIKE '%io_transform%' OR req_url LIKE '%width=%' OR req_url LIKE '%quality=%' OR req_url LIKE '%auto=webp%'))::BIGINT AS io_transformed_images,
      (COUNT(*) FILTER (WHERE (c_type LIKE 'image/jpeg%' OR c_type LIKE 'image/jpg%' OR c_type LIKE 'image/png%' OR c_type LIKE 'image/gif%') AND body_bytes > 51200))::BIGINT AS unoptimized_large_images,
      COALESCE(SUM(body_bytes) FILTER (WHERE (c_type LIKE 'image/jpeg%' OR c_type LIKE 'image/jpg%' OR c_type LIKE 'image/png%' OR c_type LIKE 'image/gif%') AND body_bytes > 51200), 0)::BIGINT AS unoptimized_large_image_bytes
    FROM normalized`,
    [],
    params.signal,
  );

  const r = rows[0];
  const totalRequests = Number(r?.total_requests ?? 0);
  if (totalRequests === 0) {
    return emptyValueSummary();
  }

  const totalBytes = Number(r.total_bytes ?? 0);
  const s5xx = Number(r.s5xx ?? 0);
  const errorRatePct = roundTo((s5xx * 100) / totalRequests, 2);
  const availabilityPct = roundTo(Math.max(0, 100 - errorRatePct), 2);

  const cacheHits = Number(r.cache_hits ?? 0);
  const cacheMisses = Number(r.cache_misses ?? 0);
  const cachePasses = Number(r.cache_passes ?? 0);
  const cacheDenom = cacheHits + cacheMisses + cachePasses;
  const originOffloadPct =
    cacheDenom > 0
      ? roundTo((cacheHits * 100) / cacheDenom, 2)
      : roundTo((cacheHits * 100) / totalRequests, 2);
  const hitTtfb = Number(r.hit_ttfb_p50_ms ?? 0);
  const missTtfb = Number(r.miss_ttfb_p50_ms ?? 0);
  const cacheAccelFactor =
    hitTtfb > 0 && missTtfb > 0 ? roundTo(missTtfb / hitTtfb, 2) : 1.0;
  const shieldReqs = Number(r.shield_requests ?? 0);
  const shieldHits = Number(r.shield_hits ?? 0);

  const tls13Reqs = Number(r.tls_13_requests ?? 0);
  const botReqs = Number(r.bot_requests ?? 0);
  const verifiedBotReqs = Number(r.verified_bot_requests ?? 0);
  const botCacheHits = Number(r.bot_cache_hits ?? 0);

  const http3Reqs = Number(r.http3_requests ?? 0);
  const http2Reqs = Number(r.http2_requests ?? 0);
  const compressedReqs = Number(r.compressed_requests ?? 0);

  const totalImages = Number(r.total_images ?? 0);
  const modernImages = Number(r.modern_format_images ?? 0);
  const legacyImages = Number(r.legacy_format_images ?? 0);
  const ioTransformedImages = Number(r.io_transformed_images ?? 0);
  const unoptimizedLargeImages = Number(r.unoptimized_large_images ?? 0);
  const unoptimizedLargeBytes = Number(r.unoptimized_large_image_bytes ?? 0);

  return {
    overview: {
      total_requests: totalRequests,
      total_bytes: totalBytes,
      unique_countries: Number(r.unique_countries ?? 0),
      unique_pops: Number(r.unique_pops ?? 0),
      unique_asns: Number(r.unique_asns ?? 0),
      error_rate_pct: errorRatePct,
      availability_pct: availabilityPct,
    },
    caching: {
      total_requests: totalRequests,
      cache_hits: cacheHits,
      cache_misses: cacheMisses,
      cache_passes: cachePasses,
      hit_ratio_pct: originOffloadPct,
      origin_offload_pct: originOffloadPct,
      bandwidth_saved_bytes: Number(r.bandwidth_saved_bytes ?? 0),
      hit_ttfb_p50_ms: roundTo(hitTtfb, 2),
      miss_ttfb_p50_ms: roundTo(missTtfb, 2),
      cache_acceleration_factor: cacheAccelFactor,
      cache_accel_factor: cacheAccelFactor,
      shield_requests: shieldReqs,
      shield_hit_ratio_pct:
        shieldReqs > 0 ? roundTo((shieldHits * 100) / shieldReqs, 2) : 0,
    },
    security: {
      tls_13_requests: tls13Reqs,
      tls_13_pct: roundTo((tls13Reqs * 100) / totalRequests, 2),
      waf_evaluated_requests: Number(r.waf_evaluated ?? 0),
      waf_blocked_requests: Number(r.waf_blocked ?? 0),
      ddos_mitigated_requests: Number(r.waf_blocked ?? 0),
    },
    bots: {
      bot_requests: botReqs,
      bot_share_pct: roundTo((botReqs * 100) / totalRequests, 2),
      verified_bot_requests: verifiedBotReqs,
      suspected_bot_requests: Math.max(0, botReqs - verifiedBotReqs),
      bot_cache_hit_ratio_pct:
        botReqs > 0 ? roundTo((botCacheHits * 100) / botReqs, 2) : 0,
      origin_requests_saved_from_bots: botCacheHits,
    },
    performance: {
      ttfb_p50_ms: roundTo(Number(r.ttfb_p50_ms ?? 0), 2),
      ttfb_p95_ms: roundTo(Number(r.ttfb_p95_ms ?? 0), 2),
      ttfb_p99_ms: roundTo(Number(r.ttfb_p99_ms ?? 0), 2),
      http3_requests: http3Reqs,
      http3_pct: roundTo((http3Reqs * 100) / totalRequests, 2),
      http2_requests: http2Reqs,
      http2_pct: roundTo((http2Reqs * 100) / totalRequests, 2),
      compressed_requests: compressedReqs,
      compression_ratio_pct: roundTo((compressedReqs * 100) / totalRequests, 2),
    },
    network: {
      unique_pops: Number(r.unique_pops ?? 0),
      unique_countries: Number(r.unique_countries ?? 0),
      unique_asns: Number(r.unique_asns ?? 0),
      avg_rtt_ms: roundTo(Number(r.avg_rtt_us ?? 0) / 1000, 2),
      median_rtt_ms: roundTo(Number(r.med_rtt_us ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
    },
    io: {
      total_images: totalImages,
      modern_format_images: modernImages,
      modern_format_pct:
        totalImages > 0 ? roundTo((modernImages * 100) / totalImages, 2) : 0,
      legacy_format_images: legacyImages,
      io_transformed_images: ioTransformedImages,
      unoptimized_large_images: unoptimizedLargeImages,
      estimated_savings_bytes: Math.round(unoptimizedLargeBytes * 0.4),
    },
  };
}

export async function handleValueRequest(
  path: string,
  params: ValueQueryParams = {},
) {
  if (path.includes("/api/value/summary")) {
    return queryValueSummary(params);
  }
  return queryValueSummary(params);
}
