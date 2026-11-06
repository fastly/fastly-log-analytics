import {
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
} from "./dashboard/duckdb";

export const CWV_THRESHOLDS = {
  LCP: { good: 2500, poor: 4000 },
  CLS: { good: 0.1, poor: 0.25 },
  INP: { good: 200, poor: 500 },
  FCP: { good: 1800, poor: 3000 },
  TTFB: { good: 800, poor: 1800 },
} as const;

export type CwvMetricName = keyof typeof CWV_THRESHOLDS;

export interface RumQueryParams {
  service_id?: string;
  start_time?: string | number;
  end_time?: string | number;
  device_type?: string;
  country?: string;
  page_url?: string;
  since?: string | number;
  limit?: number;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">;
  signal?: AbortSignal;
}

function roundTo(val: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

export function classifyCwvRating(
  metricName: string,
  value: number,
): "good" | "needs-improvement" | "poor" {
  const upper = metricName.toUpperCase() as CwvMetricName;
  const bounds = CWV_THRESHOLDS[upper] ?? CWV_THRESHOLDS.LCP;
  if (value <= bounds.good) return "good";
  if (value > bounds.poor) return "poor";
  return "needs-improvement";
}

function emptyMetricSummary() {
  return {
    p50: 0,
    p75: 0,
    p95: 0,
    good_pct: 0,
    needs_improvement_pct: 0,
    poor_pct: 0,
    count: 0,
  };
}

export function emptyRumAnalytics() {
  return {
    no_data: true,
    summary: {
      total_pageviews: 0,
      unique_sessions: 0,
      total_vitals_events: 0,
      total_errors: 0,
      error_rate_pct: 0,
      overall_cwv_good_pct: 0,
    },
    metrics: {
      LCP: emptyMetricSummary(),
      CLS: emptyMetricSummary(),
      INP: emptyMetricSummary(),
      FCP: emptyMetricSummary(),
      TTFB: emptyMetricSummary(),
    },
    timeseries: [] as Record<string, unknown>[],
    worst_pages: [] as Record<string, unknown>[],
    environments: {
      browsers: [] as { name: string; count: number; pct: number }[],
      os: [] as { name: string; count: number; pct: number }[],
      devices: [] as { name: string; count: number; pct: number }[],
      connections: [] as { name: string; count: number; pct: number }[],
    },
    error_summary: {
      total_errors: 0,
      affected_sessions: 0,
      unique_error_messages: 0,
    },
    top_errors: [] as Record<string, unknown>[],
    geo_breakdown: [] as Record<string, unknown>[],
    navigation_types: [] as Record<string, unknown>[],
    attributions: [] as Record<string, unknown>[],
  };
}

export const empty_rum_analytics = emptyRumAnalytics;

export function shapeRumEnvironments(
  envRows:
    | Record<
        string,
        readonly ({ name?: string; value?: string; count?: number; cnt?: number } | readonly [string, number])[]
      >
    | null
    | undefined,
) {
  const result: {
    browsers: { name: string; count: number; pct: number }[];
    os: { name: string; count: number; pct: number }[];
    devices: { name: string; count: number; pct: number }[];
    connections: { name: string; count: number; pct: number }[];
  } = {
    browsers: [],
    os: [],
    devices: [],
    connections: [],
  };
  if (!envRows) return result;

  for (const key of ["browsers", "os", "devices", "connections"] as const) {
    const rawList = envRows[key] ?? [];
    let total = 0;
    const normalized: { name: string; count: number }[] = [];
    for (const item of rawList) {
      if (Array.isArray(item)) {
        const name = String(item[0] ?? "Unknown");
        const count = Number(item[1] ?? 0);
        total += count;
        normalized.push({ name, count });
      } else if (item && typeof item === "object") {
        const obj = item as {
          name?: string;
          value?: string;
          count?: number;
          cnt?: number;
        };
        const name = String(obj.name ?? obj.value ?? "Unknown");
        const count = Number(obj.count ?? obj.cnt ?? 0);
        total += count;
        normalized.push({ name, count });
      }
    }
    result[key] = normalized.map((entry) => ({
      name: entry.name,
      count: entry.count,
      pct: total > 0 ? roundTo((entry.count * 100) / total, 2) : 0,
    }));
  }

  return result;
}

export const shape_rum_environments = shapeRumEnvironments;

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

export function buildRumWhere(params: RumQueryParams): string {
  const clauses: string[] = ["1=1"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms <= ${endMs}`);
  if (params.service_id) {
    const escaped = String(params.service_id).replaceAll("'", "''");
    clauses.push(`(service_id = '${escaped}' OR service_id IS NULL)`);
  }
  if (params.device_type) {
    const escaped = String(params.device_type).replaceAll("'", "''");
    clauses.push(`device_type = '${escaped}'`);
  }
  if (params.country) {
    const escaped = String(params.country).replaceAll("'", "''");
    clauses.push(`country = '${escaped}'`);
  }
  if (params.page_url) {
    const escaped = String(params.page_url).replaceAll("'", "''");
    clauses.push(`COALESCE(page_url, url, path) = '${escaped}'`);
  }
  return clauses.join(" AND ");
}

export async function queryRumAnalytics(params: RumQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const where = buildRumWhere(params);

  const countRows = await engine.executeSql<{
    vitals_cnt: number;
    sessions_cnt: number;
  }>(
    `SELECT
      COUNT(*)::BIGINT AS vitals_cnt,
      COUNT(DISTINCT session_id)::BIGINT AS sessions_cnt
    FROM client_vitals
    WHERE ${where}`,
    [],
    params.signal,
  );
  const errorCountRows = await engine.executeSql<{
    errors_cnt: number;
    affected_sessions: number;
    unique_msgs: number;
  }>(
    `SELECT
      COUNT(*)::BIGINT AS errors_cnt,
      COUNT(DISTINCT session_id)::BIGINT AS affected_sessions,
      COUNT(DISTINCT COALESCE(error_message, message))::BIGINT AS unique_msgs
    FROM client_errors
    WHERE ${where}`,
    [],
    params.signal,
  );

  const totalVitals = Number(countRows[0]?.vitals_cnt ?? 0);
  const totalErrors = Number(errorCountRows[0]?.errors_cnt ?? 0);
  if (totalVitals === 0 && totalErrors === 0) {
    return emptyRumAnalytics();
  }

  const metricRows = await engine.executeSql<{
    metric: string;
    cnt: number;
    p50: number | null;
    p75: number | null;
    p95: number | null;
    good_cnt: number;
    poor_cnt: number;
  }>(
    `SELECT
      UPPER(COALESCE(metric_name, '')) AS metric,
      COUNT(*)::BIGINT AS cnt,
      QUANTILE_CONT(metric_value, 0.5)::DOUBLE AS p50,
      QUANTILE_CONT(metric_value, 0.75)::DOUBLE AS p75,
      QUANTILE_CONT(metric_value, 0.95)::DOUBLE AS p95,
      (COUNT(*) FILTER (WHERE
        CASE UPPER(COALESCE(metric_name, ''))
          WHEN 'LCP' THEN metric_value <= 2500
          WHEN 'CLS' THEN metric_value <= 0.1
          WHEN 'INP' THEN metric_value <= 200
          WHEN 'FCP' THEN metric_value <= 1800
          WHEN 'TTFB' THEN metric_value <= 800
          ELSE metric_value <= 2500
        END
      ))::BIGINT AS good_cnt,
      (COUNT(*) FILTER (WHERE
        CASE UPPER(COALESCE(metric_name, ''))
          WHEN 'LCP' THEN metric_value > 4000
          WHEN 'CLS' THEN metric_value > 0.25
          WHEN 'INP' THEN metric_value > 500
          WHEN 'FCP' THEN metric_value > 3000
          WHEN 'TTFB' THEN metric_value > 1800
          ELSE metric_value > 4000
        END
      ))::BIGINT AS poor_cnt
    FROM client_vitals
    WHERE ${where}
    GROUP BY 1`,
    [],
    params.signal,
  );

  const metrics = {
    LCP: emptyMetricSummary(),
    CLS: emptyMetricSummary(),
    INP: emptyMetricSummary(),
    FCP: emptyMetricSummary(),
    TTFB: emptyMetricSummary(),
  };

  let totalGoodEvents = 0;
  for (const row of metricRows) {
    const mName = row.metric as CwvMetricName;
    if (!(mName in metrics)) continue;
    const cnt = Number(row.cnt ?? 0);
    const goodCnt = Number(row.good_cnt ?? 0);
    const poorCnt = Number(row.poor_cnt ?? 0);
    const niCnt = Math.max(0, cnt - goodCnt - poorCnt);
    totalGoodEvents += goodCnt;
    metrics[mName] = {
      p50: roundTo(Number(row.p50 ?? 0), mName === "CLS" ? 3 : 1),
      p75: roundTo(Number(row.p75 ?? 0), mName === "CLS" ? 3 : 1),
      p95: roundTo(Number(row.p95 ?? 0), mName === "CLS" ? 3 : 1),
      good_pct: cnt > 0 ? roundTo((goodCnt * 100) / cnt, 2) : 0,
      needs_improvement_pct: cnt > 0 ? roundTo((niCnt * 100) / cnt, 2) : 0,
      poor_pct: cnt > 0 ? roundTo((poorCnt * 100) / cnt, 2) : 0,
      count: cnt,
    };
  }

  // Worst pages (minimum sample threshold HAVING COUNT(*) >= 3, ordered by LCP p75 DESC)
  const worstPageRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(page_url, url, path, '/') AS page_url,
      COUNT(*)::BIGINT AS sample_count,
      QUANTILE_CONT(metric_value, 0.75) FILTER (WHERE UPPER(metric_name) = 'LCP')::DOUBLE AS lcp_p75,
      QUANTILE_CONT(metric_value, 0.75) FILTER (WHERE UPPER(metric_name) = 'CLS')::DOUBLE AS cls_p75,
      QUANTILE_CONT(metric_value, 0.75) FILTER (WHERE UPPER(metric_name) = 'INP')::DOUBLE AS inp_p75
    FROM client_vitals
    WHERE ${where}
    GROUP BY 1
    HAVING COUNT(*) >= 3
    ORDER BY lcp_p75 DESC NULLS LAST, sample_count DESC
    LIMIT 20`,
    [],
    params.signal,
  );

  const worst_pages = worstPageRows.map((r) => ({
    page_url: String(r.page_url ?? "/"),
    sample_count: Number(r.sample_count ?? 0),
    lcp_p75: roundTo(Number(r.lcp_p75 ?? 0), 1),
    cls_p75: roundTo(Number(r.cls_p75 ?? 0), 3),
    inp_p75: roundTo(Number(r.inp_p75 ?? 0), 1),
  }));

  const [browserRows, osRows, deviceRows, connRows] = await Promise.all([
    engine.executeSql<{ name: string; count: number }>(
      `SELECT COALESCE(browser_name, 'Unknown') AS name, COUNT(*)::BIGINT AS count FROM client_vitals WHERE ${where} GROUP BY 1 ORDER BY count DESC LIMIT 20`,
      [],
      params.signal,
    ),
    engine.executeSql<{ name: string; count: number }>(
      `SELECT COALESCE(os_name, 'Unknown') AS name, COUNT(*)::BIGINT AS count FROM client_vitals WHERE ${where} GROUP BY 1 ORDER BY count DESC LIMIT 20`,
      [],
      params.signal,
    ),
    engine.executeSql<{ name: string; count: number }>(
      `SELECT COALESCE(device_type, 'Unknown') AS name, COUNT(*)::BIGINT AS count FROM client_vitals WHERE ${where} GROUP BY 1 ORDER BY count DESC LIMIT 20`,
      [],
      params.signal,
    ),
    engine.executeSql<{ name: string; count: number }>(
      `SELECT COALESCE(connection_type, 'Unknown') AS name, COUNT(*)::BIGINT AS count FROM client_vitals WHERE ${where} GROUP BY 1 ORDER BY count DESC LIMIT 20`,
      [],
      params.signal,
    ),
  ]);

  const environments = shapeRumEnvironments({
    browsers: browserRows,
    os: osRows,
    devices: deviceRows,
    connections: connRows,
  });

  const topErrorRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(error_message, message, 'Unknown Error') AS message,
      COALESCE(filename, source, '') AS source,
      COUNT(*)::BIGINT AS count,
      MIN(lineno)::BIGINT AS lineno,
      MIN(colno)::BIGINT AS colno,
      COUNT(DISTINCT session_id)::BIGINT AS affected_sessions,
      MAX(ts_ms)::BIGINT AS last_seen_ms
    FROM client_errors
    WHERE ${where}
    GROUP BY 1, 2
    ORDER BY count DESC
    LIMIT 25`,
    [],
    params.signal,
  );

  const uniqueSessions = Math.max(
    1,
    Number(countRows[0]?.sessions_cnt ?? 0),
  );

  return {
    no_data: false,
    summary: {
      total_pageviews: Math.max(uniqueSessions, metrics.LCP.count),
      unique_sessions: Number(countRows[0]?.sessions_cnt ?? 0),
      total_vitals_events: totalVitals,
      total_errors: totalErrors,
      error_rate_pct:
        totalVitals > 0 ? roundTo((totalErrors * 100) / totalVitals, 2) : 0,
      overall_cwv_good_pct:
        totalVitals > 0
          ? roundTo((totalGoodEvents * 100) / totalVitals, 2)
          : 0,
    },
    metrics,
    timeseries: [] as Record<string, unknown>[],
    worst_pages,
    environments,
    error_summary: {
      total_errors: totalErrors,
      affected_sessions: Number(errorCountRows[0]?.affected_sessions ?? 0),
      unique_error_messages: Number(errorCountRows[0]?.unique_msgs ?? 0),
    },
    top_errors: topErrorRows.map((r) => ({
      message: String(r.message ?? "Unknown Error"),
      source: String(r.source ?? ""),
      lineno: r.lineno == null ? null : Number(r.lineno),
      colno: r.colno == null ? null : Number(r.colno),
      count: Number(r.count ?? 0),
      affected_sessions: Number(r.affected_sessions ?? 0),
      last_seen: r.last_seen_ms
        ? new Date(Number(r.last_seen_ms)).toISOString()
        : null,
    })),
    geo_breakdown: [] as Record<string, unknown>[],
    navigation_types: [] as Record<string, unknown>[],
    attributions: [] as Record<string, unknown>[],
  };
}

export async function queryRumLiveEvents(params: RumQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const sinceMs = parseEpochMs(params.since) ?? 0;
  const limit = Math.min(200, Math.max(1, Number(params.limit ?? 100)));

  const vitals = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      'vital' AS event_type,
      ts_ms,
      metric_name,
      metric_value,
      COALESCE(page_url, url, path, '/') AS page_url,
      browser_name,
      os_name,
      device_type,
      country,
      session_id
    FROM client_vitals
    WHERE ts_ms > ${sinceMs} AND ${buildRumWhere(params)}
    ORDER BY ts_ms DESC
    LIMIT ${limit}`,
    [],
    params.signal,
  );

  const errors = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      'error' AS event_type,
      ts_ms,
      COALESCE(error_message, message) AS message,
      COALESCE(page_url, url, path, '/') AS page_url,
      browser_name,
      os_name,
      device_type,
      country,
      session_id
    FROM client_errors
    WHERE ts_ms > ${sinceMs} AND ${buildRumWhere(params)}
    ORDER BY ts_ms DESC
    LIMIT ${limit}`,
    [],
    params.signal,
  );

  const combined = [...vitals, ...errors]
    .sort((a, b) => Number(b.ts_ms ?? 0) - Number(a.ts_ms ?? 0))
    .slice(0, limit);

  return {
    events: combined,
    next_since:
      combined.length > 0 ? Number(combined[0].ts_ms ?? sinceMs) : sinceMs,
  };
}

export async function queryRumBeaconHealth(params: RumQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const rows = await engine.executeSql<{
    total_beacons: number;
    s2xx: number;
    s4xx: number;
    s5xx: number;
    avg_latency_ms: number | null;
  }>(
    `SELECT
      COUNT(*)::BIGINT AS total_beacons,
      (COUNT(*) FILTER (WHERE status >= 200 AND status < 300))::BIGINT AS s2xx,
      (COUNT(*) FILTER (WHERE status >= 400 AND status < 500))::BIGINT AS s4xx,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600))::BIGINT AS s5xx,
      AVG(COALESCE(ttfb_ms, time_elapsed_usec / 1000.0, 0.0))::DOUBLE AS avg_latency_ms
    FROM requests
    WHERE COALESCE(url, path, '') LIKE '/__fla_rum%'`,
    [],
    params.signal,
  );

  const r = rows[0];
  const total = Number(r?.total_beacons ?? 0);
  const s2xx = Number(r?.s2xx ?? 0);
  return {
    total_beacons: total,
    status_2xx: s2xx,
    status_4xx: Number(r?.s4xx ?? 0),
    status_5xx: Number(r?.s5xx ?? 0),
    success_rate_pct: total > 0 ? roundTo((s2xx * 100) / total, 2) : 100,
    avg_latency_ms: roundTo(Number(r?.avg_latency_ms ?? 0), 2),
    recent_errors: [] as Record<string, unknown>[],
  };
}

export async function queryRumStatus(serviceId = "default") {
  return {
    service_id: serviceId,
    enabled: true,
    version: "1.0.0",
    sample_rate: 1.0,
    endpoint: "/__fla_rum",
  };
}

export async function queryRumVersions() {
  return {
    current_version: "1.0.0",
    latest_version: "1.0.0",
    up_to_date: true,
  };
}

export async function updateRumSettings(
  serviceId: string,
  settings: Record<string, unknown> = {},
) {
  return {
    service_id: serviceId,
    updated: true,
    settings,
  };
}

export async function handleRumRequest(
  path: string,
  params: RumQueryParams = {},
) {
  if (path.includes("rum/analytics")) {
    return queryRumAnalytics(params);
  }
  if (path.includes("rum/live-events")) {
    return queryRumLiveEvents(params);
  }
  if (path.includes("rum/beacon-health")) {
    return queryRumBeaconHealth(params);
  }
  if (path.includes("rum/versions")) {
    return queryRumVersions();
  }
  if (path.includes("rum/settings")) {
    return updateRumSettings(params.service_id ?? "default");
  }
  if (
    path.includes("rum/enable") ||
    path.includes("rum/disable") ||
    path.includes("rum/upgrade")
  ) {
    return { status: "complete", service_id: params.service_id ?? "default" };
  }
  if (path.includes("rum/status")) {
    return queryRumStatus(params.service_id ?? "default");
  }
  return queryRumAnalytics(params);
}
