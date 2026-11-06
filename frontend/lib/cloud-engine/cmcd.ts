import {
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
} from "./dashboard/duckdb";

export const CMCD_COLUMNS = [
  "cmcd_sid",
  "cmcd_cid",
  "cmcd_br",
  "cmcd_bl",
  "cmcd_bs",
  "cmcd_mtp",
  "cmcd_ot",
  "cmcd_sf",
  "cmcd_st",
  "cmcd_tb",
  "cmcd_d",
  "cmcd_pr",
  "cmcd_rtp",
  "cmcd_su",
  "cmcd_dl",
] as const;

export const CMCD_SESSION_DURATION_BUCKETS = [
  "< 30s",
  "30s - 1m",
  "1-5 min",
  "5-15 min",
  "15-30 min",
  "30-60 min",
  "> 1h",
] as const;

export const CMCD_TS_DEFAULTS = {
  sessions_ts: {
    active_sessions: 0,
    rebuffer_events: 0,
    rebuffer_rate: 0.0,
  },
  buffer_health_ts: {
    avg_buffer_ms: 0.0,
    p10_buffer_ms: 0.0,
    p50_buffer_ms: 0.0,
    rebuffer_events: 0,
  },
  bitrate_ts: {
    avg_encoded_bitrate: 0.0,
    avg_top_bitrate: 0.0,
    utilization_ratio: 0.0,
  },
  throughput_ts: {
    avg_throughput_kbps: 0.0,
    avg_bitrate_kbps: 0.0,
    headroom_ratio: 0.0,
  },
  startup_ts: {
    startup_requests: 0,
    avg_startup_buffer_ms: 0.0,
    avg_startup_ttfb_ms: 0.0,
  },
} as const;

export interface CmcdQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  filter_sf?: string;
  filter_ot?: string;
  filter_st?: string;
  filter_cid?: string;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">;
  signal?: AbortSignal;
}

function roundTo(val: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

export function classifySessionDurationBucket(durationSeconds: number): string {
  if (durationSeconds < 30) return "< 30s";
  if (durationSeconds < 60) return "30s - 1m";
  if (durationSeconds < 300) return "1-5 min";
  if (durationSeconds < 900) return "5-15 min";
  if (durationSeconds < 1800) return "15-30 min";
  if (durationSeconds < 3600) return "30-60 min";
  return "> 1h";
}

export function emptyCmcdAggregates() {
  return {
    summary: {
      active_sessions: 0,
      total_requests: 0,
      rebuffer_events: 0,
      rebuffer_rate: 0,
      avg_bitrate_kbps: 0,
      avg_buffer_ms: 0,
      avg_throughput_kbps: 0,
      peak_viewers: 0,
      avg_session_duration: 0,
      utilization_ratio: 0,
    },
    sessions_ts: [] as Record<string, unknown>[],
    buffer_health_ts: [] as Record<string, unknown>[],
    bitrate_ts: [] as Record<string, unknown>[],
    throughput_ts: [] as Record<string, unknown>[],
    startup_ts: [] as Record<string, unknown>[],
    top_content: [] as Record<string, unknown>[],
    rebuffer_by_country: [] as Record<string, unknown>[],
    rebuffer_by_asn: [] as Record<string, unknown>[],
    object_type_dist: [] as Record<string, unknown>[],
    streaming_format_dist: [] as Record<string, unknown>[],
    session_duration_dist: CMCD_SESSION_DURATION_BUCKETS.map((bucket) => ({
      bucket,
      sessions: 0,
      pct: 0,
    })),
  };
}

export const empty_cmcd_aggregates = emptyCmcdAggregates;

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

function buildCmcdWhere(params: CmcdQueryParams): string {
  const clauses: string[] = [
    "(cmcd_sid IS NOT NULL OR cmcd_br IS NOT NULL OR cmcd_bl IS NOT NULL OR cmcd_ot IS NOT NULL)",
  ];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms <= ${endMs}`);
  if (params.service_id) {
    const escaped = String(params.service_id).replaceAll("'", "''");
    clauses.push(`(service_id = '${escaped}' OR service_id IS NULL)`);
  }
  if (params.filter_sf) {
    const escaped = String(params.filter_sf).replaceAll("'", "''");
    clauses.push(`cmcd_sf = '${escaped}'`);
  }
  if (params.filter_ot) {
    const escaped = String(params.filter_ot).replaceAll("'", "''");
    clauses.push(`cmcd_ot = '${escaped}'`);
  }
  if (params.filter_st) {
    const escaped = String(params.filter_st).replaceAll("'", "''");
    clauses.push(`cmcd_st = '${escaped}'`);
  }
  if (params.filter_cid) {
    const escaped = String(params.filter_cid).replaceAll("'", "''");
    clauses.push(`cmcd_cid = '${escaped}'`);
  }
  return clauses.join(" AND ");
}

export async function queryCmcdAggregates(params: CmcdQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const where = buildCmcdWhere(params);

  const summaryRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COUNT(*)::BIGINT AS total_requests,
      COUNT(DISTINCT cmcd_sid)::BIGINT AS active_sessions,
      (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
      AVG(cmcd_br)::DOUBLE AS avg_bitrate_kbps,
      AVG(cmcd_tb)::DOUBLE AS avg_top_bitrate_kbps,
      AVG(cmcd_bl)::DOUBLE AS avg_buffer_ms,
      AVG(cmcd_mtp)::DOUBLE AS avg_throughput_kbps,
      MIN(ts_ms)::BIGINT AS min_ts,
      MAX(ts_ms)::BIGINT AS max_ts
    FROM requests
    WHERE ${where}`,
    [],
    params.signal,
  );

  const s = summaryRows[0];
  const totalRequests = Number(s?.total_requests ?? 0);
  if (totalRequests === 0) {
    return emptyCmcdAggregates();
  }

  const activeSessions = Number(s.active_sessions ?? 0);
  const rebufferEvents = Number(s.rebuffer_events ?? 0);
  const rebufferRate =
    totalRequests > 0 ? roundTo((rebufferEvents * 100) / totalRequests, 2) : 0;
  const avgBitrate = Number(s.avg_bitrate_kbps ?? 0);
  const avgTopBitrate = Number(s.avg_top_bitrate_kbps ?? 0);
  const utilizationRatio =
    avgTopBitrate > 0 ? roundTo((avgBitrate * 100) / avgTopBitrate, 2) : 0;

  // Time series (1-minute buckets)
  const bucketRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      (floor(ts_ms / 60000) * 60000)::BIGINT AS bucket_ms,
      COUNT(*)::BIGINT AS reqs,
      COUNT(DISTINCT cmcd_sid)::BIGINT AS active_sessions,
      (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
      AVG(cmcd_bl)::DOUBLE AS avg_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.1)::DOUBLE AS p10_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.5)::DOUBLE AS p50_buffer_ms,
      AVG(cmcd_br)::DOUBLE AS avg_encoded_bitrate,
      AVG(cmcd_tb)::DOUBLE AS avg_top_bitrate,
      AVG(cmcd_mtp)::DOUBLE AS avg_throughput_kbps,
      (COUNT(*) FILTER (WHERE cmcd_su IS TRUE))::BIGINT AS startup_requests,
      AVG(cmcd_bl) FILTER (WHERE cmcd_su IS TRUE)::DOUBLE AS avg_startup_buffer_ms,
      AVG(COALESCE(ttfb_ms, time_elapsed_usec / 1000.0)) FILTER (WHERE cmcd_su IS TRUE)::DOUBLE AS avg_startup_ttfb_ms
    FROM requests
    WHERE ${where}
    GROUP BY 1
    ORDER BY 1`,
    [],
    params.signal,
  );

  let peakViewers = 0;
  const sessions_ts: Record<string, unknown>[] = [];
  const buffer_health_ts: Record<string, unknown>[] = [];
  const bitrate_ts: Record<string, unknown>[] = [];
  const throughput_ts: Record<string, unknown>[] = [];
  const startup_ts: Record<string, unknown>[] = [];

  for (const b of bucketRows) {
    const tsIso = new Date(Number(b.bucket_ms ?? 0)).toISOString();
    const sess = Number(b.active_sessions ?? 0);
    if (sess > peakViewers) peakViewers = sess;
    const reqs = Number(b.reqs ?? 0);
    const rebuf = Number(b.rebuffer_events ?? 0);
    const encBr = Number(b.avg_encoded_bitrate ?? 0);
    const topBr = Number(b.avg_top_bitrate ?? 0);
    const mtp = Number(b.avg_throughput_kbps ?? 0);

    sessions_ts.push({
      timestamp: tsIso,
      active_sessions: sess,
      rebuffer_events: rebuf,
      rebuffer_rate: reqs > 0 ? roundTo((rebuf * 100) / reqs, 2) : 0,
    });
    buffer_health_ts.push({
      timestamp: tsIso,
      avg_buffer_ms: roundTo(Number(b.avg_buffer_ms ?? 0), 1),
      p10_buffer_ms: roundTo(Number(b.p10_buffer_ms ?? 0), 1),
      p50_buffer_ms: roundTo(Number(b.p50_buffer_ms ?? 0), 1),
      rebuffer_events: rebuf,
    });
    bitrate_ts.push({
      timestamp: tsIso,
      avg_encoded_bitrate: roundTo(encBr, 1),
      avg_top_bitrate: roundTo(topBr, 1),
      utilization_ratio:
        topBr > 0 ? roundTo((encBr * 100) / topBr, 2) : 0,
    });
    throughput_ts.push({
      timestamp: tsIso,
      avg_throughput_kbps: roundTo(mtp, 1),
      avg_bitrate_kbps: roundTo(encBr, 1),
      headroom_ratio: encBr > 0 ? roundTo(mtp / encBr, 2) : 0,
    });
    startup_ts.push({
      timestamp: tsIso,
      startup_requests: Number(b.startup_requests ?? 0),
      avg_startup_buffer_ms: roundTo(Number(b.avg_startup_buffer_ms ?? 0), 1),
      avg_startup_ttfb_ms: roundTo(Number(b.avg_startup_ttfb_ms ?? 0), 2),
    });
  }

  // Session durations & session_duration_dist
  const sessionRows = await engine.executeSql<{
    sid: string;
    dur_sec: number;
  }>(
    `SELECT
      cmcd_sid AS sid,
      GREATEST(0.0, (MAX(ts_ms) - MIN(ts_ms)) / 1000.0 + COALESCE(MAX(cmcd_d) / 1000.0, 0.0))::DOUBLE AS dur_sec
    FROM requests
    WHERE ${where} AND cmcd_sid IS NOT NULL AND cmcd_sid != ''
    GROUP BY 1`,
    [],
    params.signal,
  );

  const durationBucketCounts = new Map<string, number>();
  for (const label of CMCD_SESSION_DURATION_BUCKETS) {
    durationBucketCounts.set(label, 0);
  }
  let totalDurationSec = 0;
  for (const sr of sessionRows) {
    const dur = Number(sr.dur_sec ?? 0);
    totalDurationSec += dur;
    const label = classifySessionDurationBucket(dur);
    durationBucketCounts.set(label, (durationBucketCounts.get(label) ?? 0) + 1);
  }
  const avgSessionDuration =
    sessionRows.length > 0
      ? roundTo(totalDurationSec / sessionRows.length, 1)
      : 0;
  const session_duration_dist = CMCD_SESSION_DURATION_BUCKETS.map((bucket) => {
    const count = durationBucketCounts.get(bucket) ?? 0;
    return {
      bucket,
      sessions: count,
      pct:
        sessionRows.length > 0
          ? roundTo((count * 100) / sessionRows.length, 2)
          : 0,
    };
  });

  const [
    topContentRows,
    countryRows,
    asnRows,
    otRows,
    sfRows,
  ] = await Promise.all([
    engine.executeSql<Record<string, unknown>>(
      `SELECT
        COALESCE(cmcd_cid, url, 'Unknown') AS content_id,
        COUNT(DISTINCT cmcd_sid)::BIGINT AS sessions,
        COUNT(*)::BIGINT AS requests,
        AVG(cmcd_br)::DOUBLE AS avg_bitrate_kbps,
        (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS rebuffer_rate
      FROM requests
      WHERE ${where}
      GROUP BY 1
      ORDER BY sessions DESC, requests DESC
      LIMIT 25`,
      [],
      params.signal,
    ),
    engine.executeSql<Record<string, unknown>>(
      `SELECT
        COALESCE(geo_country, country, 'Unknown') AS country,
        COUNT(DISTINCT cmcd_sid)::BIGINT AS sessions,
        COUNT(*)::BIGINT AS requests,
        (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
        (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS rebuffer_rate
      FROM requests
      WHERE ${where}
      GROUP BY 1
      ORDER BY rebuffer_events DESC, requests DESC
      LIMIT 50`,
      [],
      params.signal,
    ),
    engine.executeSql<Record<string, unknown>>(
      `SELECT
        COALESCE(client_asn, asn, 0)::BIGINT AS asn,
        COALESCE(as_name, 'AS' || CAST(COALESCE(client_asn, asn, 0) AS VARCHAR)) AS as_name,
        COUNT(DISTINCT cmcd_sid)::BIGINT AS sessions,
        COUNT(*)::BIGINT AS requests,
        (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
        (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS rebuffer_rate,
        AVG(cmcd_mtp)::DOUBLE AS avg_throughput_kbps
      FROM requests
      WHERE ${where}
      GROUP BY 1, 2
      ORDER BY rebuffer_events DESC, requests DESC
      LIMIT 50`,
      [],
      params.signal,
    ),
    engine.executeSql<{ object_type: string; requests: number }>(
      `SELECT COALESCE(cmcd_ot, 'unknown') AS object_type, COUNT(*)::BIGINT AS requests FROM requests WHERE ${where} GROUP BY 1 ORDER BY requests DESC`,
      [],
      params.signal,
    ),
    engine.executeSql<{ format: string; requests: number }>(
      `SELECT COALESCE(cmcd_sf, 'unknown') AS format, COUNT(*)::BIGINT AS requests FROM requests WHERE ${where} GROUP BY 1 ORDER BY requests DESC`,
      [],
      params.signal,
    ),
  ]);

  return {
    summary: {
      active_sessions: activeSessions,
      total_requests: totalRequests,
      rebuffer_events: rebufferEvents,
      rebuffer_rate: rebufferRate,
      avg_bitrate_kbps: roundTo(avgBitrate, 1),
      avg_buffer_ms: roundTo(Number(s.avg_buffer_ms ?? 0), 1),
      avg_throughput_kbps: roundTo(Number(s.avg_throughput_kbps ?? 0), 1),
      peak_viewers: peakViewers,
      avg_session_duration: avgSessionDuration,
      utilization_ratio: utilizationRatio,
    },
    sessions_ts,
    buffer_health_ts,
    bitrate_ts,
    throughput_ts,
    startup_ts,
    top_content: topContentRows.map((r) => ({
      content_id: String(r.content_id ?? "Unknown"),
      sessions: Number(r.sessions ?? 0),
      requests: Number(r.requests ?? 0),
      avg_bitrate_kbps: roundTo(Number(r.avg_bitrate_kbps ?? 0), 1),
      rebuffer_rate: roundTo(Number(r.rebuffer_rate ?? 0), 2),
    })),
    rebuffer_by_country: countryRows.map((r) => ({
      country: String(r.country ?? "Unknown"),
      sessions: Number(r.sessions ?? 0),
      requests: Number(r.requests ?? 0),
      rebuffer_events: Number(r.rebuffer_events ?? 0),
      rebuffer_rate: roundTo(Number(r.rebuffer_rate ?? 0), 2),
    })),
    rebuffer_by_asn: asnRows.map((r) => ({
      asn: Number(r.asn ?? 0),
      as_name: String(r.as_name ?? ""),
      sessions: Number(r.sessions ?? 0),
      requests: Number(r.requests ?? 0),
      rebuffer_events: Number(r.rebuffer_events ?? 0),
      rebuffer_rate: roundTo(Number(r.rebuffer_rate ?? 0), 2),
      avg_throughput_kbps: roundTo(Number(r.avg_throughput_kbps ?? 0), 1),
    })),
    object_type_dist: otRows.map((r) => ({
      object_type: String(r.object_type ?? "unknown"),
      requests: Number(r.requests ?? 0),
      pct:
        totalRequests > 0
          ? roundTo((Number(r.requests ?? 0) * 100) / totalRequests, 2)
          : 0,
    })),
    streaming_format_dist: sfRows.map((r) => ({
      format: String(r.format ?? "unknown"),
      requests: Number(r.requests ?? 0),
      pct:
        totalRequests > 0
          ? roundTo((Number(r.requests ?? 0) * 100) / totalRequests, 2)
          : 0,
    })),
    session_duration_dist,
  };
}

export async function queryCmcdStatus(serviceId = "default") {
  return {
    service_id: serviceId,
    enabled: true,
    fields: [...CMCD_COLUMNS],
  };
}

export async function handleCmcdRequest(
  path: string,
  params: CmcdQueryParams = {},
) {
  if (path.includes("/api/cmcd/aggregates")) {
    return queryCmcdAggregates(params);
  }
  if (path.includes("/cmcd/status")) {
    return queryCmcdStatus(params.service_id ?? "default");
  }
  if (path.includes("/cmcd/enable") || path.includes("/cmcd/disable")) {
    return { status: "complete", service_id: params.service_id ?? "default" };
  }
  return queryCmcdAggregates(params);
}
