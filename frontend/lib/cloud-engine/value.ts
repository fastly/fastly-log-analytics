import type { components } from "@/types/api";
import { BOT_UA_CLASSIFY_LIMIT, classifyUserAgentCountsSync, loadWellKnownBots } from "./bots/classify";
import {
  compileFiltersToSql, duckSqlString, getDuckDBEngine, type DuckDBAnalyticalEngine, type DuckDBColumnType,
} from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";
import { roundTo } from "./engine-utils";

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

// Not the engine-utils parseEpochMs: a non-finite number here falls through to Date.parse and yields null, not NaN.
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
    clauses.push(`(service_id = ${duckSqlString(String(params.service_id))} OR service_id IS NULL)`);
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
  const want = (section: ValueSection) => !params.sections?.length || params.sections.includes(section);
  const needCache = want("overview") || want("caching") || want("performance");
  const needBytes = want("overview") || want("caching") || want("io");
  const needLatency = want("overview") || want("performance");
  const needP99 = want("performance");
  const needShield = want("caching") || want("io");
  const needWaf = want("overview") || want("security");
  const needWafDetail = want("security");
  const needBots = want("bots");
  const needNet = want("network");
  const needIo = want("io");
  const needBuckets = want("caching") || want("performance") || want("network") || want("security") || want("io");

  const normCols: string[] = [`EPOCH_MS(CAST(ts_ms AS BIGINT)) AS "timestamp"`];
  if (needIo) normCols.push("url", "LOWER(COALESCE(url, '')) AS url_lc");
  if (needCache) normCols.push(`UPPER(COALESCE(NULLIF(cache_status, ''), cache, '')) AS cache_state`);
  if (needBytes) normCols.push(`COALESCE(resp_body_size, resp_body_bytes, resp_bytes) AS body_bytes`);
  if (needLatency) normCols.push(`COALESCE(time_elapsed_usec, elapsed) / 1000.0 AS latency_ms`);
  if (needNet) {
    normCols.push(
      `UPPER(COALESCE(NULLIF(protocol, ''), proto, '')) AS protocol_name`,
      `COALESCE(is_ssl, CASE WHEN COALESCE(tls_version, tls) IS NOT NULL THEN NULLIF(COALESCE(tls_version, tls), '') IS NOT NULL END) AS encrypted`,
      `COALESCE(is_ipv6, ipv6) AS ipv6_flag`,
    );
  }
  if (needShield) normCols.push(`COALESCE(fastly_is_shield, is_shield, NOT edge) AS shield_flag`);
  if (needIo) normCols.push(`LOWER(COALESCE(NULLIF(content_type, ''), resp_header_content_type, '')) AS content_type`);
  if (needWaf) {
    normCols.push(
      `COALESCE(NULLIF(waf_action, ''), NULLIF(ngwaf_decision, '')) AS waf_decision`,
      `waf AS waf_flag`,
      `waf_resp AS waf_resp_code`,
    );
    if (needWafDetail) {
      normCols.push(
        `NULLIF(waf_sig, '') AS waf_signals`,
        `CASE WHEN NULLIF(waf_sig, '') IS NOT NULL THEN
        len(list_filter(
          regexp_split_to_array(LOWER(TRIM(CAST(waf_sig AS VARCHAR))), '[,[:space:]]+'),
          t -> t != '' AND NOT regexp_matches(
            t,
            '^(bot-analysis|datacenter|site-flagged-ip|tor-exit|corporate-proxy|residential-proxy|public-proxy|vpn|verified-bot([.][a-z0-9_.-]+)?|suspected-(bad-)?bot([.][a-z0-9_.-]+)?|site[.][a-z0-9_.-]+)$'
          )
        )) > 0
      ELSE FALSE END AS has_attack_waf_sig`,
      );
    }
  }
  if (needBots) {
    normCols.push(
      `COALESCE(bot_verified, verified_bot, CASE WHEN waf_sig IS NOT NULL THEN regexp_matches(waf_sig, '(?i)verified-bot\\.') END) AS bot_verified`,
      `COALESCE(bot_verified, verified_bot) AS bot_verified_col`,
      `COALESCE(NULLIF(_bot_name, ''), NULLIF(ngwaf_bot_name, ''), NULLIF(bot_category, '')) AS bot_name`,
      `is_bot`,
      `NULLIF(TRIM(CAST(ua AS VARCHAR)), '') AS ua_norm`,
    );
  }
  if (needIo) {
    normCols.push(
      `NULLIF(COALESCE(NULLIF(io_output_format, ''), ofmt), '') AS output_format`,
      `NULLIF(COALESCE(NULLIF(io_input_format, ''), ifmt), '') AS input_format`,
      `io_input_bytes`,
      `io_output_bytes`,
    );
  }
  const normalized = `WITH normalized AS (
    SELECT ${normCols.join(", ")}
    FROM requests WHERE ${buildWhereClause(params, types)}
  )`;
  const query = (sql: string) =>
    engine.executeSql<Record<string, unknown>>(`${normalized} ${sql}`, [], params.signal);
  const count = (row: Record<string, unknown>, key: string) => Number(row[key] ?? 0);

  const aggCols: string[] = ["COUNT(*) AS total"];
  if (needCache) {
    aggCols.push(
      `COUNT(NULLIF(cache_state, '')) AS cache_samples`,
      `COUNT(*) FILTER (WHERE starts_with(cache_state, 'HIT')) AS hits`,
      `COUNT(*) FILTER (WHERE starts_with(cache_state, 'MISS')) AS misses`,
      `COUNT(*) FILTER (WHERE starts_with(cache_state, 'PASS')) AS passes`,
      `COUNT(*) FILTER (WHERE starts_with(cache_state, 'SYNTH')) AS synths`,
    );
  }
  if (needBytes) {
    aggCols.push(`COUNT(body_bytes) AS byte_samples`, `SUM(body_bytes) AS total_bytes`);
    if (needCache) aggCols.push(`SUM(body_bytes) FILTER (WHERE starts_with(cache_state, 'HIT')) AS saved_bytes`);
  }
  if (needLatency) {
    aggCols.push(
      `SUM(latency_ms) FILTER (WHERE starts_with(cache_state, 'HIT')) AS sum_hit_latency`,
      `COUNT(latency_ms) FILTER (WHERE starts_with(cache_state, 'HIT')) AS cnt_hit_latency`,
      `SUM(latency_ms) FILTER (WHERE starts_with(cache_state, 'MISS')) AS sum_miss_latency`,
      `COUNT(latency_ms) FILTER (WHERE starts_with(cache_state, 'MISS')) AS cnt_miss_latency`,
    );
  }
  if (needShield && needCache) {
    aggCols.push(
      `COUNT(*) FILTER (WHERE shield_flag) AS shield_requests`,
      `COUNT(*) FILTER (WHERE shield_flag AND starts_with(cache_state, 'HIT')) AS shield_hits`,
    );
  }
  if (needWaf) {
    aggCols.push(
      `COUNT(*) FILTER (WHERE waf_decision IS NOT NULL OR waf_flag IS NOT NULL) AS waf_samples`,
      `COUNT(*) FILTER (WHERE CASE
        WHEN waf_decision IS NOT NULL THEN UPPER(waf_decision) IN ('BLOCK', 'BLOCKED', 'DENY')
        ELSE waf_flag IS TRUE AND waf_resp_code = 406
      END) AS blocked`,
    );
    if (needWafDetail) {
      aggCols.push(
        `COUNT(*) FILTER (WHERE CASE
        WHEN waf_decision IS NOT NULL THEN UPPER(waf_decision) IN ('LOG', 'LOGGED')
        ELSE waf_flag IS TRUE AND COALESCE(waf_resp_code, 0) != 406 AND has_attack_waf_sig
      END) AS logged`,
        `COUNT(*) FILTER (WHERE CASE
        WHEN waf_decision IS NOT NULL THEN UPPER(waf_decision) IN ('ALLOW', 'ALLOWED', 'PASS', 'PASSED')
        ELSE waf_flag IS TRUE AND waf_resp_code = 200 AND NOT has_attack_waf_sig
      END) AS passed`,
      );
    }
  }
  if (needBots) {
    aggCols.push(
      `COUNT(*) FILTER (WHERE bot_name IS NOT NULL OR is_bot OR bot_verified_col) AS bots`,
      `COUNT(*) FILTER (WHERE bot_verified) AS verified`,
      `COUNT(bot_verified) AS verified_samples`,
      `COUNT(bot_name) + COUNT(is_bot) + COUNT(bot_verified_col) AS bot_samples`,
      `COUNT(ua_norm) AS ua_samples`,
    );
  }
  if (needNet) {
    aggCols.push(
      `COUNT(NULLIF(protocol_name, '')) AS protocol_samples`,
      `COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/3%' OR protocol_name IN ('H3', '3')) AS h3`,
      `COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/2%' OR protocol_name IN ('H2', '2')) AS h2`,
      `COUNT(*) FILTER (WHERE protocol_name LIKE 'HTTP/1.1%' OR protocol_name = '1.1') AS h11`,
      `COUNT(*) FILTER (WHERE encrypted) AS tls_count`,
      `COUNT(encrypted) AS tls_samples`,
      `COUNT(ipv6_flag) AS ipv6_samples`,
      `COUNT(*) FILTER (WHERE ipv6_flag) AS ipv6_count`,
    );
  }
  const urlImgExt = `(${[".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"].map((ext) => `ends_with(url_lc, '${ext}')`).join(" OR ")})`;
  const isImageExpr = `(starts_with(content_type, 'image/') OR (content_type = '' AND ${urlImgExt}))`;
  if (needIo) {
    aggCols.push(
      `COUNT(*) FILTER (WHERE ${isImageExpr}) AS images`,
      `COUNT(body_bytes) FILTER (WHERE ${isImageExpr}) AS image_byte_samples`,
      `SUM(body_bytes) FILTER (WHERE ${isImageExpr}) AS image_bytes`,
      `COUNT(*) FILTER (WHERE output_format IS NOT NULL) AS io_count`,
      `COUNT(body_bytes) FILTER (WHERE output_format IS NOT NULL) AS io_byte_samples`,
      `SUM(body_bytes) FILTER (WHERE output_format IS NOT NULL) AS io_bytes`,
      `COUNT(body_bytes) FILTER (WHERE output_format IS NOT NULL AND shield_flag) AS io_shield_byte_samples`,
      `SUM(body_bytes) FILTER (WHERE output_format IS NOT NULL AND shield_flag) AS io_shield_bytes`,
      `COUNT(io_input_bytes) FILTER (WHERE io_output_bytes IS NOT NULL) AS input_byte_samples`,
      `SUM(io_input_bytes) FILTER (WHERE io_output_bytes IS NOT NULL) AS input_bytes`,
      `COUNT(io_output_bytes) FILTER (WHERE io_input_bytes IS NOT NULL) AS output_byte_samples`,
      `SUM(io_output_bytes) FILTER (WHERE io_input_bytes IS NOT NULL) AS output_bytes`,
      `COUNT(io_input_bytes - io_output_bytes) AS actual_saved_samples`,
      `SUM(io_input_bytes - io_output_bytes) AS actual_saved`,
    );
  }

  const interval = CHART_INTERVAL_SECONDS[params.chart_interval ?? "1 day"] ?? 86400;
  const mainSql = needBuckets
    ? `SELECT time_bucket(INTERVAL '${interval} seconds', "timestamp") AS bucket, ${aggCols.join(", ")} FROM normalized GROUP BY bucket ORDER BY bucket`
    : `SELECT ${aggCols.join(", ")} FROM normalized`;
  const [buckets, p99Rows] = await Promise.all([
    query(mainSql),
    needP99 ? query(`SELECT QUANTILE_CONT(latency_ms, 0.99) AS p99 FROM normalized`) : Promise.resolve([]),
  ]);

  const sumKey = (key: string) => buckets.reduce((acc, row) => acc + count(row, key), 0);
  const total = sumKey("total");
  const result = emptyValueSummary();
  if (!total) return selectSections(result, params.sections);
  const hits = sumKey("hits");
  const cacheSamples = sumKey("cache_samples");
  const hasCache = cacheSamples > 0;
  const offload = hasCache ? roundTo(hits * 100 / total, 1) : null;
  const cntHitLat = sumKey("cnt_hit_latency");
  const cntMissLat = sumKey("cnt_miss_latency");
  const avgHit = cntHitLat > 0 ? roundTo(sumKey("sum_hit_latency") / cntHitLat) : null;
  const avgMiss = cntMissLat > 0 ? roundTo(sumKey("sum_miss_latency") / cntMissLat) : null;
  const acceleration = avgHit !== null && avgHit > 0 && avgMiss !== null
    ? roundTo(avgMiss / avgHit, 1) : null;
  const hasWaf = sumKey("waf_samples") > 0;
  const byteSamples = sumKey("byte_samples");
  result.overview = {
    total_requests: total, total_bandwidth_bytes: byteSamples > 0 ? sumKey("total_bytes") : null,
    origin_offload_pct: offload, cache_acceleration_factor: acceleration,
    threats_blocked: hasWaf ? sumKey("blocked") : null,
    edge_hit_requests: hasCache ? hits : null,
    origin_requests: hasCache ? sumKey("misses") + sumKey("passes") : null,
  };
  const shieldRequests = sumKey("shield_requests");
  result.caching = {
    total_requests: total, hit_requests: hasCache ? hits : null,
    miss_requests: hasCache ? sumKey("misses") : null,
    pass_requests: hasCache ? sumKey("passes") : null,
    synth_requests: hasCache ? sumKey("synths") : null,
    origin_offload_pct: offload,
    bandwidth_saved_bytes: byteSamples ? sumKey("saved_bytes") : null,
    shield_effectiveness_pct: shieldRequests
      ? roundTo(sumKey("shield_hits") * 100 / shieldRequests, 1) : null,
    offload_time_series: [], cache_state_time_series: [],
  };
  result.performance = {
    total_requests: total, avg_hit_latency_ms: avgHit, avg_miss_latency_ms: avgMiss,
    p99_latency_ms: nullableNumber(p99Rows[0]?.p99), cache_accel_factor: acceleration, latency_time_series: [],
  };
  result.security = {
    total_requests: total, waf_blocked: hasWaf ? sumKey("blocked") : null,
    waf_logged: hasWaf ? sumKey("logged") : null, waf_passed: hasWaf ? sumKey("passed") : null,
    threat_time_series: [], top_waf_signals: [],
  };
  const hasBotCol = sumKey("bot_samples") > 0;
  const uaSamples = sumKey("ua_samples");
  result.bots = {
    total_requests: total,
    bot_requests: hasBotCol ? sumKey("bots") : (uaSamples > 0 ? 0 : null),
    verified_bots: sumKey("verified_samples") ? sumKey("verified") : null,
    top_bots: [],
  };
  const protocolSamples = sumKey("protocol_samples");
  result.network = {
    total_requests: total,
    http3_pct: protocolSamples ? roundTo(sumKey("h3") * 100 / total, 1) : null,
    h2_pct: protocolSamples ? roundTo(sumKey("h2") * 100 / total, 1) : null,
    tls_pct: sumKey("tls_samples") ? roundTo(sumKey("tls_count") * 100 / total, 1) : null,
    ipv6_pct: sumKey("ipv6_samples") ? roundTo(sumKey("ipv6_count") * 100 / total, 1) : null,
    protocol_time_series: [],
  };
  const ioCount = sumKey("io_count");
  const outputBytes = sumKey("output_byte_samples") > 0 ? sumKey("output_bytes") : null;
  const inputBytes = sumKey("input_byte_samples") > 0 ? sumKey("input_bytes") : null;
  result.io = {
    ...result.io, total_requests: total, image_request_count: sumKey("images"),
    image_bandwidth_bytes: sumKey("image_byte_samples") > 0 ? sumKey("image_bytes") : null,
    io_transforms: ioCount || null,
    io_bandwidth_bytes: sumKey("io_byte_samples") > 0 ? sumKey("io_bytes") : null,
    io_shield_bandwidth_bytes: sumKey("io_shield_byte_samples") > 0 ? sumKey("io_shield_bytes") : null,
    io_pct_of_traffic: ioCount ? roundTo(ioCount * 100 / total) : null,
    io_actual_bandwidth_saved_bytes: sumKey("actual_saved_samples") > 0 ? sumKey("actual_saved") : null,
    io_actual_compression_ratio: outputBytes != null && outputBytes > 0 && inputBytes != null
      ? roundTo(inputBytes / outputBytes) : null,
  };

  if (needBuckets) {
    for (const b of buckets) {
      const raw = b.bucket;
      const time = new Date(raw instanceof Date ? raw : typeof raw === "number" ? raw : String(raw)).toISOString();
      const point = (value: number, category?: string): ValuePoint => ({ time, value, ...(category ? { category } : {}) });
      const bucketTotal = count(b, "total");
      if (needCache && hasCache) {
        result.caching.offload_time_series!.push(point(roundTo(count(b, "hits") * 100 / bucketTotal, 1)));
        for (const [key, category] of [["hits", "HIT"], ["misses", "MISS"], ["passes", "PASS"]] as const) {
          result.caching.cache_state_time_series!.push(point(count(b, key), category));
        }
        const other = bucketTotal - count(b, "hits") - count(b, "misses") - count(b, "passes");
        if (other) result.caching.cache_state_time_series!.push(point(other, "OTHER"));
      }
      if (needLatency) {
        const bHitCnt = count(b, "cnt_hit_latency");
        const bMissCnt = count(b, "cnt_miss_latency");
        if (bHitCnt > 0) result.performance.latency_time_series!.push(point(count(b, "sum_hit_latency") / bHitCnt, "hit"));
        if (bMissCnt > 0) result.performance.latency_time_series!.push(point(count(b, "sum_miss_latency") / bMissCnt, "miss"));
      }
      if (needNet && protocolSamples) {
        for (const [key, category] of [["h3", "HTTP/3"], ["h2", "HTTP/2"], ["h11", "HTTP/1.1"]] as const) {
          if (count(b, key)) result.network.protocol_time_series!.push(point(count(b, key), category));
        }
        const other = bucketTotal - count(b, "h3") - count(b, "h2") - count(b, "h11");
        if (other) result.network.protocol_time_series!.push(point(other, "Other"));
      }
      if (needWaf && hasWaf) {
        result.security.threat_time_series!.push(point(count(b, "blocked"), "blocked"), point(count(b, "logged"), "logged"));
      }
      if (needIo) {
        if (ioCount) result.io.io_time_series!.push(point(count(b, "io_count")));
        if (b.io_bytes != null) result.io.io_bandwidth_time_series!.push(point(Number(b.io_bytes)));
        if (b.actual_saved != null) result.io.io_compression_time_series!.push(point(Number(b.actual_saved)));
      }
    }
  }
  if (want("bots")) {
    const namedBots = (await query(`SELECT bot_name, COUNT(*) AS count FROM normalized
      WHERE bot_name IS NOT NULL GROUP BY bot_name ORDER BY count DESC LIMIT 20`))
      .map((row) => ({ name: String(row.bot_name), count: count(row, "count") }));
    if (namedBots.length > 0) {
      result.bots.top_bots = namedBots;
    } else if (uaSamples > 0) {
      const uaRows = (await query(`SELECT ua_norm AS ua, COUNT(*) AS count FROM normalized
        WHERE ua_norm IS NOT NULL GROUP BY ua_norm ORDER BY count DESC, ua_norm ASC LIMIT ${BOT_UA_CLASSIFY_LIMIT}`))
        .map((row) => [String(row.ua), count(row, "count")] as const);
      const allBots = await loadWellKnownBots();
      const classified = classifyUserAgentCountsSync(uaRows, 20, allBots);
      result.bots.bot_requests = classified.matchedRequests;
      result.bots.top_bots = classified.top.map((b) => ({ name: b.name, count: b.request_count }));
    }
  }
  if (want("security")) {
    result.security.top_waf_signals = (await query(`SELECT signal, COUNT(*) AS count
      FROM (SELECT UNNEST(string_split(waf_signals, ',')) AS signal FROM normalized)
      WHERE signal IS NOT NULL AND signal != '' GROUP BY signal ORDER BY count DESC LIMIT 20`))
      .map((row) => ({ signal: String(row.signal), count: count(row, "count") }));
  }
  if (want("io")) {
    const urlImgFmt = `CASE
      WHEN ends_with(url_lc, '.png') THEN 'png'
      WHEN ends_with(url_lc, '.jpg') OR ends_with(url_lc, '.jpeg') THEN 'jpeg'
      WHEN ends_with(url_lc, '.gif') THEN 'gif'
      WHEN ends_with(url_lc, '.svg') THEN 'svg'
      WHEN ends_with(url_lc, '.webp') THEN 'webp'
      WHEN ends_with(url_lc, '.ico') THEN 'ico'
      ELSE NULL END`;
    const formats = await query(`SELECT COALESCE(
      output_format,
      CASE WHEN starts_with(content_type, 'image/') THEN NULLIF(split_part(content_type, '/', 2), '') END,
      CASE WHEN content_type = '' THEN ${urlImgFmt} END
    ) AS format,
      COUNT(*) AS count FROM normalized WHERE output_format IS NOT NULL OR ${isImageExpr}
      GROUP BY 1 HAVING format IS NOT NULL AND format != '' ORDER BY count DESC`);
    const formatTotal = formats.reduce((sum, row) => sum + count(row, "count"), 0);
    result.io.format_distribution = formats.map((row) => ({
      format: String(row.format), count: count(row, "count"), pct: roundTo(count(row, "count") * 100 / formatTotal),
    }));
    result.io.modern_format_pct = formatTotal ? roundTo(formats
      .filter((row) => ["webp", "avif", "jpegxl", "jxl"].includes(String(row.format).toLowerCase()))
      .reduce((sum, row) => sum + count(row, "count"), 0) * 100 / formatTotal) : null;
    const urlRasterExt = `(${[".jpg", ".jpeg", ".png", ".gif"].map((ext) => `ends_with(url_lc, '${ext}')`).join(" OR ")})`;
    result.io.optimization_opportunities = (await query(`SELECT url, COUNT(*) AS request_count,
      SUM(body_bytes) AS total_bytes, AVG(body_bytes) / 1024.0 AS avg_kb FROM normalized
      WHERE (content_type IN ('image/jpeg', 'image/jpg', 'image/png', 'image/gif')
          OR (content_type = '' AND ${urlRasterExt}))
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

/** Every request column the /api/value/summary scan reads, physical Cloud names and their aliases alike. */
export const VALUE_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "service_id", "url", "cache_status", "cache", "resp_body_size", "resp_body_bytes", "resp_bytes",
  "time_elapsed_usec", "elapsed", "protocol", "proto", "is_ssl", "tls_version", "tls", "is_ipv6", "ipv6",
  "fastly_is_shield", "is_shield", "edge", "content_type", "resp_header_content_type", "waf_action", "ngwaf_decision",
  "waf", "waf_resp", "waf_sig", "bot_verified", "verified_bot", "_bot_name", "ngwaf_bot_name", "bot_category", "is_bot", "ua",
  "io_output_format", "ofmt", "io_input_format", "ifmt", "io_input_bytes", "io_output_bytes",
] as const;

/** The columns each section's figures read, beyond the time and service ones every scan needs. */
const VALUE_SECTION_COLUMNS: Record<ValueSection, readonly (typeof VALUE_SCAN_COLUMNS)[number][]> = {
  overview: [
    "cache_status", "cache", "resp_body_size", "resp_body_bytes", "resp_bytes", "time_elapsed_usec", "elapsed",
    "waf_action", "ngwaf_decision", "waf", "waf_resp",
  ],
  caching: [
    "cache_status", "cache", "resp_body_size", "resp_body_bytes", "resp_bytes", "fastly_is_shield", "is_shield", "edge",
  ],
  performance: ["cache_status", "cache", "time_elapsed_usec", "elapsed"],
  network: ["protocol", "proto", "is_ssl", "tls_version", "tls", "is_ipv6", "ipv6"],
  security: ["waf_action", "ngwaf_decision", "waf", "waf_resp", "waf_sig"],
  bots: ["bot_verified", "verified_bot", "_bot_name", "ngwaf_bot_name", "bot_category", "is_bot", "waf_sig", "ua"],
  io: [
    "url", "content_type", "resp_header_content_type", "resp_body_size", "resp_body_bytes", "resp_bytes",
    "fastly_is_shield", "is_shield", "edge", "io_output_format", "ofmt", "io_input_format", "ifmt", "io_input_bytes",
    "io_output_bytes",
  ],
};

/**
 * The columns a projected /api/value/summary load needs, or null when the
 * filters can't name theirs: the requested sections' columns (every one when
 * none are named), as the figures of other sections are dropped from the answer.
 */
export function valueScanColumns(
  params: { filters?: unknown; custom_filters?: unknown; sections?: unknown },
): string[] | null {
  const sections = Array.isArray(params.sections) && params.sections.length &&
    params.sections.every((section) => Object.hasOwn(VALUE_SECTION_COLUMNS, section))
    ? params.sections as ValueSection[] : null;
  const fields: string[] = sections
    ? ["timestamp", "ts_ms", "service_id", ...sections.flatMap((section) => VALUE_SECTION_COLUMNS[section])]
    : [...VALUE_SCAN_COLUMNS];
  for (const filters of [params.filters, params.custom_filters]) {
    if (filters == null || filters === "") continue;
    if (typeof filters !== "object" || Array.isArray(filters)) return null;
    fields.push(...Object.keys(filters).filter((key) => key !== "filter_mode").map((key) => key.replace(/_\d+$/, "")));
  }
  return [...new Set(fields)];
}

export async function handleValueRequest(_path: string, params: ValueQueryParams = {}): Promise<ValueSummary> {
  return queryValueSummary(params);
}
