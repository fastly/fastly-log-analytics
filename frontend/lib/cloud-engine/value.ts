import { sqlEscape } from "@/lib/sqlBuilder";
import type { components } from "@/types/api";
import {
  compileFiltersToSql, getDuckDBEngine, type DuckDBAnalyticalEngine, type DuckDBColumnType,
} from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";

export const IO_LARGE_IMAGE_THRESHOLD_BYTES = 51200;

type ValueSection = NonNullable<components["schemas"]["ValueSummaryRequest"]["sections"]>[number];
type ValueSections = {
  [Section in ValueSection]: NonNullable<components["schemas"]["ValueSummaryResponse"][Section]>;
};
type ValueSummary = Partial<ValueSections>;
type ValuePoint = components["schemas"]["ValueTimeSeriesPoint"];

export interface ValueQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  filters?: Record<string, { mode?: string; values?: string[] }>;
  custom_filters?: Record<string, { mode?: string; values?: string[] }>;
  sections?: readonly ValueSection[] | null;
  chart_interval?: string;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">;
  signal?: AbortSignal;
}

function roundTo(val: number, decimals = 2): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

function nullableNumber(value: unknown): number | null {
  return value == null ? null : roundTo(Number(value));
}

export function emptyValueSummary(): ValueSections {
  return {
    overview: {
      total_requests: 0, total_bandwidth_bytes: null, origin_offload_pct: null,
      threats_blocked: null, cache_acceleration_factor: null, edge_hit_requests: 0, origin_requests: 0,
    },
    caching: {
      total_requests: 0, hit_requests: 0, miss_requests: 0, pass_requests: 0, synth_requests: 0,
      origin_offload_pct: null, bandwidth_saved_bytes: null, shield_effectiveness_pct: null,
      offload_time_series: [], cache_state_time_series: [],
    },
    security: {
      total_requests: 0, waf_blocked: null, waf_logged: null, waf_passed: null,
      threat_time_series: [], top_waf_signals: [],
    },
    bots: { total_requests: 0, bot_requests: null, verified_bots: null, top_bots: [] },
    performance: {
      total_requests: 0, cache_accel_factor: null, avg_hit_latency_ms: null,
      avg_miss_latency_ms: null, p99_latency_ms: null, latency_time_series: [],
    },
    network: {
      total_requests: 0, http3_pct: null, h2_pct: null, tls_pct: null, ipv6_pct: null,
      protocol_time_series: [],
    },
    io: {
      total_requests: 0, io_transforms: null, io_bandwidth_bytes: null,
      io_shield_bandwidth_bytes: null, io_pct_of_traffic: null, io_estimated_cost_usd: null,
      modern_format_pct: null, image_request_count: null, image_bandwidth_bytes: null,
      io_actual_bandwidth_saved_bytes: null, io_actual_compression_ratio: null,
      estimated_savings_bytes: null, io_time_series: [], io_bandwidth_time_series: [],
      io_compression_time_series: [], format_distribution: [], optimization_opportunities: [],
      io_format_conversion_pairs: [],
    },
  };
}

function parseEpochMs(val: string | number | undefined): number | null {
  if (val === undefined || val === null || val === "") return null;
  const num = Number(val);
  if (Number.isFinite(num)) return num < 1e12 ? Math.floor(num * 1000) : Math.floor(num);
  const parsed = Date.parse(String(val));
  return Number.isFinite(parsed) ? parsed : null;
}

function buildWhereClause(params: ValueQueryParams, types: Map<string, DuckDBColumnType>): string {
  const clauses = ["TRUE"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms < ${endMs}`);
  if (params.service_id) {
    clauses.push(`(service_id = '${sqlEscape(params.service_id)}' OR service_id IS NULL)`);
  }
  const filters: DashboardFilter[] = [];
  for (const source of [params.filters, params.custom_filters]) {
    for (const [key, spec] of Object.entries(source ?? {})) {
      for (const value of spec?.values ?? []) {
        filters.push({
          field: key.replace(/_\d+$/, ""), value: String(value),
          mode: spec.mode === "exclude" ? "exclude" : "include",
        });
      }
    }
  }
  if (filters.length) clauses.push(`(${compileFiltersToSql(filters, false, [], types)})`);
  return clauses.join(" AND ");
}

function selectSections(summary: ValueSummary, sections?: readonly ValueSection[] | null): ValueSummary {
  if (!sections?.length) return summary;
  return Object.fromEntries(sections.map((section) => [section, summary[section]]));
}

const CHART_INTERVAL_SECONDS: Record<string, number> = {
  "1 second": 1, "1 minute": 60, "5 minutes": 300, "1 hour": 3600, "1 day": 86400,
};

export async function queryValueSummary(params: ValueQueryParams = {}): Promise<ValueSummary> {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  const types = new Map<string, DuckDBColumnType>();
  if (Object.keys(params.filters ?? {}).length || Object.keys(params.custom_filters ?? {}).length) {
    const schema = await engine.executeSql<{ column_name: string; column_type: string }>(
      "DESCRIBE requests", [], params.signal,
    );
    for (const column of schema) {
      if (["VARCHAR", "BIGINT", "DOUBLE", "BOOLEAN"].includes(column.column_type)) {
        types.set(column.column_name, column.column_type as DuckDBColumnType);
      }
    }
  }
  const normalized = `WITH normalized AS (
    SELECT EPOCH_MS(CAST(ts_ms AS BIGINT)) AS "timestamp", url,
      UPPER(COALESCE(NULLIF(cache_status, ''), cache, '')) AS cache_state,
      COALESCE(resp_body_size, resp_body_bytes, resp_bytes) AS body_bytes,
      COALESCE(time_elapsed_usec, elapsed) / 1000.0 AS latency_ms,
      UPPER(COALESCE(NULLIF(protocol, ''), proto, '')) AS protocol_name,
      COALESCE(is_ssl, CASE WHEN COALESCE(tls_version, tls) IS NOT NULL
        THEN NULLIF(COALESCE(tls_version, tls), '') IS NOT NULL END) AS encrypted,
      COALESCE(is_ipv6, ipv6) AS ipv6_flag,
      COALESCE(fastly_is_shield, is_shield, NOT edge) AS shield_flag,
      LOWER(COALESCE(NULLIF(content_type, ''), resp_header_content_type, '')) AS content_type,
      COALESCE(NULLIF(waf_action, ''), NULLIF(ngwaf_decision, '')) AS waf_decision,
      NULLIF(waf_sig, '') AS waf_signals,
      COALESCE(bot_verified, verified_bot) AS bot_verified,
      COALESCE(NULLIF(_bot_name, ''), NULLIF(ngwaf_bot_name, ''), NULLIF(bot_category, '')) AS bot_name,
      is_bot,
      NULLIF(COALESCE(NULLIF(io_output_format, ''), ofmt), '') AS output_format,
      NULLIF(COALESCE(NULLIF(io_input_format, ''), ifmt), '') AS input_format,
      io_input_bytes, io_output_bytes
    FROM requests WHERE ${buildWhereClause(params, types)}
  )`;
  const query = (sql: string) =>
    engine.executeSql<Record<string, unknown>>(`${normalized} ${sql}`, [], params.signal);
  const count = (row: Record<string, unknown>, key: string) => Number(row[key] ?? 0);
  const rows = await query(`SELECT
    COUNT(*) AS total,
    COUNT(NULLIF(cache_state, '')) AS cache_samples,
    COUNT(body_bytes) AS byte_samples,
    SUM(body_bytes) AS total_bytes,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'HIT')) AS hits,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'MISS')) AS misses,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'PASS')) AS passes,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'SYNTH')) AS synths,
    SUM(body_bytes) FILTER (WHERE starts_with(cache_state, 'HIT')) AS saved_bytes,
    AVG(latency_ms) FILTER (WHERE starts_with(cache_state, 'HIT')) AS avg_hit,
    AVG(latency_ms) FILTER (WHERE starts_with(cache_state, 'MISS')) AS avg_miss,
    QUANTILE_CONT(latency_ms, 0.99) AS p99,
    COUNT(*) FILTER (WHERE shield_flag) AS shield_requests,
    COUNT(*) FILTER (WHERE shield_flag AND starts_with(cache_state, 'HIT')) AS shield_hits,
    COUNT(waf_decision) AS waf_samples,
    COUNT(*) FILTER (WHERE UPPER(waf_decision) IN ('BLOCK', 'BLOCKED', 'DENY')) AS blocked,
    COUNT(*) FILTER (WHERE UPPER(waf_decision) IN ('LOG', 'LOGGED')) AS logged,
    COUNT(*) FILTER (WHERE UPPER(waf_decision) IN ('ALLOW', 'ALLOWED', 'PASS', 'PASSED')) AS passed,
    COUNT(*) FILTER (WHERE bot_name IS NOT NULL OR is_bot OR bot_verified) AS bots,
    COUNT(*) FILTER (WHERE bot_verified) AS verified,
    COUNT(bot_verified) AS verified_samples,
    COUNT(bot_name) + COUNT(is_bot) + COUNT(bot_verified) AS bot_samples,
    COUNT(NULLIF(protocol_name, '')) AS protocol_samples,
    COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/3%' OR protocol_name IN ('H3', '3')) AS h3,
    COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/2%' OR protocol_name IN ('H2', '2')) AS h2,
    COUNT(*) FILTER (WHERE encrypted) AS tls_count,
    COUNT(encrypted) AS tls_samples,
    COUNT(ipv6_flag) AS ipv6_samples,
    COUNT(*) FILTER (WHERE ipv6_flag) AS ipv6_count,
    COUNT(*) FILTER (WHERE starts_with(content_type, 'image/')) AS images,
    SUM(body_bytes) FILTER (WHERE starts_with(content_type, 'image/')) AS image_bytes,
    COUNT(*) FILTER (WHERE output_format IS NOT NULL) AS io_count,
    SUM(body_bytes) FILTER (WHERE output_format IS NOT NULL) AS io_bytes,
    SUM(body_bytes) FILTER (WHERE output_format IS NOT NULL AND shield_flag) AS io_shield_bytes,
    SUM(io_input_bytes) FILTER (WHERE io_output_bytes IS NOT NULL) AS input_bytes,
    SUM(io_output_bytes) FILTER (WHERE io_input_bytes IS NOT NULL) AS output_bytes,
    SUM(io_input_bytes - io_output_bytes) AS actual_saved
    FROM normalized`);
  const r = rows[0] ?? {};
  const total = count(r, "total");
  const result = emptyValueSummary();
  if (!total) return selectSections(result, params.sections);
  const hits = count(r, "hits");
  const hasCache = count(r, "cache_samples") > 0;
  const offload = hasCache ? roundTo(hits * 100 / total) : null;
  const avgHit = nullableNumber(r.avg_hit);
  const avgMiss = nullableNumber(r.avg_miss);
  const acceleration = avgHit !== null && avgHit > 0 && avgMiss !== null
    ? roundTo(avgMiss / avgHit) : null;
  const hasWaf = count(r, "waf_samples") > 0;
  result.overview = {
    total_requests: total, total_bandwidth_bytes: nullableNumber(r.total_bytes),
    origin_offload_pct: offload, cache_acceleration_factor: acceleration,
    threats_blocked: hasWaf ? count(r, "blocked") : null,
    edge_hit_requests: hasCache ? hits : null,
    origin_requests: hasCache ? count(r, "misses") + count(r, "passes") : null,
  };
  result.caching = {
    total_requests: total, hit_requests: hasCache ? hits : null,
    miss_requests: hasCache ? count(r, "misses") : null,
    pass_requests: hasCache ? count(r, "passes") : null,
    synth_requests: hasCache ? count(r, "synths") : null,
    origin_offload_pct: offload,
    bandwidth_saved_bytes: count(r, "byte_samples") ? count(r, "saved_bytes") : null,
    shield_effectiveness_pct: count(r, "shield_requests")
      ? roundTo(count(r, "shield_hits") * 100 / count(r, "shield_requests")) : null,
    offload_time_series: [], cache_state_time_series: [],
  };
  result.performance = {
    total_requests: total, avg_hit_latency_ms: avgHit, avg_miss_latency_ms: avgMiss,
    p99_latency_ms: nullableNumber(r.p99), cache_accel_factor: acceleration, latency_time_series: [],
  };
  result.security = {
    total_requests: total, waf_blocked: hasWaf ? count(r, "blocked") : null,
    waf_logged: hasWaf ? count(r, "logged") : null, waf_passed: hasWaf ? count(r, "passed") : null,
    threat_time_series: [], top_waf_signals: [],
  };
  result.bots = {
    total_requests: total, bot_requests: count(r, "bot_samples") ? count(r, "bots") : null,
    verified_bots: count(r, "verified_samples") ? count(r, "verified") : null, top_bots: [],
  };
  result.network = {
    total_requests: total,
    http3_pct: count(r, "protocol_samples") ? roundTo(count(r, "h3") * 100 / total) : null,
    h2_pct: count(r, "protocol_samples") ? roundTo(count(r, "h2") * 100 / total) : null,
    tls_pct: count(r, "tls_samples") ? roundTo(count(r, "tls_count") * 100 / total) : null,
    ipv6_pct: count(r, "ipv6_samples") ? roundTo(count(r, "ipv6_count") * 100 / total) : null,
    protocol_time_series: [],
  };
  result.io = {
    ...result.io, total_requests: total, image_request_count: count(r, "images"),
    image_bandwidth_bytes: nullableNumber(r.image_bytes),
    io_transforms: count(r, "io_count") || null, io_bandwidth_bytes: nullableNumber(r.io_bytes),
    io_shield_bandwidth_bytes: nullableNumber(r.io_shield_bytes),
    io_pct_of_traffic: count(r, "io_count") ? roundTo(count(r, "io_count") * 100 / total) : null,
    io_actual_bandwidth_saved_bytes: nullableNumber(r.actual_saved),
    io_actual_compression_ratio: Number(r.output_bytes) > 0 && r.input_bytes != null
      ? roundTo(Number(r.input_bytes) / Number(r.output_bytes)) : null,
  };

  const interval = CHART_INTERVAL_SECONDS[params.chart_interval ?? "1 day"] ?? 86400;
  const buckets = await query(`SELECT
    time_bucket(INTERVAL '${interval} seconds', "timestamp") AS bucket,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'HIT')) AS hits,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'MISS')) AS misses,
    COUNT(*) FILTER (WHERE starts_with(cache_state, 'PASS')) AS passes,
    AVG(latency_ms) FILTER (WHERE starts_with(cache_state, 'HIT')) AS avg_hit,
    AVG(latency_ms) FILTER (WHERE starts_with(cache_state, 'MISS')) AS avg_miss,
    COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/3%' OR protocol_name IN ('H3', '3')) AS h3,
    COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/2%' OR protocol_name IN ('H2', '2')) AS h2,
    COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/1.1%' OR protocol_name = '1.1') AS h11,
    COUNT(*) FILTER (WHERE UPPER(waf_decision) IN ('BLOCK', 'BLOCKED', 'DENY')) AS blocked,
    COUNT(*) FILTER (WHERE UPPER(waf_decision) IN ('LOG', 'LOGGED')) AS logged,
    COUNT(*) FILTER (WHERE output_format IS NOT NULL) AS io_count,
    SUM(body_bytes) FILTER (WHERE output_format IS NOT NULL) AS io_bytes,
    SUM(io_input_bytes - io_output_bytes) AS actual_saved
    FROM normalized GROUP BY bucket ORDER BY bucket`);
  for (const b of buckets) {
    const raw = b.bucket;
    const time = new Date(raw instanceof Date ? raw : typeof raw === "number" ? raw : String(raw)).toISOString();
    const point = (value: number, category?: string): ValuePoint => ({ time, value, ...(category ? { category } : {}) });
    const bucketTotal = count(b, "total");
    if (count(r, "cache_samples")) {
      result.caching.offload_time_series!.push(point(roundTo(count(b, "hits") * 100 / bucketTotal)));
      for (const [key, category] of [["hits", "HIT"], ["misses", "MISS"], ["passes", "PASS"]] as const) {
        result.caching.cache_state_time_series!.push(point(count(b, key), category));
      }
      const other = bucketTotal - count(b, "hits") - count(b, "misses") - count(b, "passes");
      if (other) result.caching.cache_state_time_series!.push(point(other, "OTHER"));
    }
    if (b.avg_hit != null) result.performance.latency_time_series!.push(point(Number(b.avg_hit), "hit"));
    if (b.avg_miss != null) result.performance.latency_time_series!.push(point(Number(b.avg_miss), "miss"));
    if (count(r, "protocol_samples")) {
      for (const [key, category] of [["h3", "HTTP/3"], ["h2", "HTTP/2"], ["h11", "HTTP/1.1"]] as const) {
        if (count(b, key)) result.network.protocol_time_series!.push(point(count(b, key), category));
      }
      const other = bucketTotal - count(b, "h3") - count(b, "h2") - count(b, "h11");
      if (other) result.network.protocol_time_series!.push(point(other, "Other"));
    }
    if (hasWaf) {
      result.security.threat_time_series!.push(point(count(b, "blocked"), "blocked"), point(count(b, "logged"), "logged"));
    }
    if (count(r, "io_count")) result.io.io_time_series!.push(point(count(b, "io_count")));
    if (b.io_bytes != null) result.io.io_bandwidth_time_series!.push(point(Number(b.io_bytes)));
    if (b.actual_saved != null) result.io.io_compression_time_series!.push(point(Number(b.actual_saved)));
  }

  const want = (section: ValueSection) => !params.sections?.length || params.sections.includes(section);
  if (want("bots")) {
    result.bots.top_bots = (await query(`SELECT bot_name, COUNT(*) AS count FROM normalized
      WHERE bot_name IS NOT NULL GROUP BY bot_name ORDER BY count DESC LIMIT 20`))
      .map((row) => ({ name: String(row.bot_name), count: count(row, "count") }));
  }
  if (want("security")) {
    result.security.top_waf_signals = (await query(`SELECT signal, COUNT(*) AS count
      FROM (SELECT UNNEST(string_split(waf_signals, ',')) AS signal FROM normalized)
      WHERE signal IS NOT NULL AND signal != '' GROUP BY signal ORDER BY count DESC LIMIT 20`))
      .map((row) => ({ signal: String(row.signal), count: count(row, "count") }));
  }
  if (want("io")) {
    const formats = await query(`SELECT COALESCE(output_format, NULLIF(split_part(content_type, '/', 2), '')) AS format,
      COUNT(*) AS count FROM normalized WHERE output_format IS NOT NULL OR starts_with(content_type, 'image/')
      GROUP BY 1 ORDER BY count DESC`);
    const formatTotal = formats.reduce((sum, row) => sum + count(row, "count"), 0);
    result.io.format_distribution = formats.map((row) => ({
      format: String(row.format), count: count(row, "count"), pct: roundTo(count(row, "count") * 100 / formatTotal),
    }));
    result.io.modern_format_pct = formatTotal ? roundTo(formats
      .filter((row) => ["webp", "avif", "jpegxl", "jxl"].includes(String(row.format).toLowerCase()))
      .reduce((sum, row) => sum + count(row, "count"), 0) * 100 / formatTotal) : null;
    result.io.optimization_opportunities = (await query(`SELECT url, COUNT(*) AS request_count,
      SUM(body_bytes) AS total_bytes, AVG(body_bytes) / 1024.0 AS avg_kb FROM normalized
      WHERE content_type IN ('image/jpeg', 'image/jpg', 'image/png', 'image/gif')
        AND output_format IS NULL AND body_bytes > ${IO_LARGE_IMAGE_THRESHOLD_BYTES}
      GROUP BY url ORDER BY total_bytes DESC LIMIT 20`))
      .map((row) => ({ url: String(row.url ?? ""), request_count: count(row, "request_count"),
        total_bytes: count(row, "total_bytes"), avg_kb: nullableNumber(row.avg_kb) }));
    result.io.io_format_conversion_pairs = (await query(`SELECT input_format, output_format,
      COUNT(*) AS count, AVG(io_input_bytes / NULLIF(io_output_bytes, 0)) AS avg_ratio FROM normalized
      WHERE input_format IS NOT NULL AND output_format IS NOT NULL
      GROUP BY input_format, output_format ORDER BY count DESC LIMIT 20`))
      .map((row) => ({ input_format: String(row.input_format), output_format: String(row.output_format),
        count: count(row, "count"), avg_ratio: nullableNumber(row.avg_ratio) }));
  }
  return selectSections(result, params.sections);
}

export async function handleValueRequest(_path: string, params: ValueQueryParams = {}): Promise<ValueSummary> {
  return queryValueSummary(params);
}
