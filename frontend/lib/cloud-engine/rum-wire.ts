import { getDuckDBEngine } from "./dashboard/duckdb";
import { buildRumWhere, queryRumAnalytics, queryRumLiveEvents, type RumQueryParams } from "./rum";

const BEACON_IDENTITY = `coalesce(nullif(request_event_id, ''), nullif(client_id, '') || ':' || round(ts_ms / 1000.0)::VARCHAR,
  scoped_key || ':' || ts_ms::VARCHAR)`;

export async function cloudRumUsage(params: RumQueryParams, classARate: number) {
  const where = buildRumWhere(params);
  const rows = await getDuckDBEngine().executeSql<Record<string, unknown>>(`WITH events AS (
    SELECT ts_ms, ${BEACON_IDENTITY} AS beacon_id FROM client_vitals WHERE ${where}
    UNION ALL SELECT ts_ms, ${BEACON_IDENTITY} FROM client_errors WHERE ${where}
  ) SELECT strftime(epoch_ms(ts_ms), '%Y-%m-%d') AS date,
    count(DISTINCT beacon_id) AS beacons FROM events GROUP BY 1 ORDER BY 1`, [], params.signal);
  const data = rows.map((row) => ({
    date: String(row.date), beacon_count: Number(row.beacons),
    estimated_class_a_ops: Number(row.beacons),
    estimated_cost_usd: Number(row.beacons) / 1000 * classARate,
  }));
  const total = data.reduce((sum, row) => sum + row.beacon_count, 0);
  const days = (Date.parse(String(params.end_time)) - Date.parse(String(params.start_time))) / 86400000;
  return {
    data, total_beacons: total, total_estimated_class_a: total,
    total_estimated_cost_usd: total / 1000 * classARate, class_a_rate_per_1k: classARate,
    average_beacons_per_day: days > 0 ? total / days : 0,
    note: "Estimated from processed RUM beacon identities, not raw delivery receipts. One FOS Class A operation is estimated per beacon; these operations are included in total FOS usage.",
  };
}

export async function cloudRumAnalytics(params: RumQueryParams) {
  const data = await queryRumAnalytics(params);
  const engine = getDuckDBEngine();
  const where = buildRumWhere(params);
  const identity = BEACON_IDENTITY;
  const events = `WITH events AS (
    SELECT ts_ms, ${identity} AS beacon_id, page_url, browser_name, os_name, device_type,
      metric_name, metric_value, FALSE AS is_error FROM client_vitals WHERE ${where}
    UNION ALL SELECT ts_ms, ${identity}, page_url, browser_name, os_name, device_type,
      NULL, NULL, TRUE FROM client_errors WHERE ${where}
  )`;
  const counts = (await engine.executeSql<Record<string, unknown>>(`${events} SELECT
    count(DISTINCT beacon_id) AS beacons,
    count(DISTINCT beacon_id) FILTER (WHERE NOT is_error AND NOT starts_with(metric_name, 'event_')) AS pageviews,
    count(DISTINCT beacon_id) FILTER (WHERE starts_with(metric_name, 'event_')) AS interactions,
    count(DISTINCT beacon_id) FILTER (WHERE is_error) AS errors FROM events`, [], params.signal))[0];
  const pages = await engine.executeSql<Record<string, unknown>>(`${events} SELECT page_url,
    count(DISTINCT beacon_id) FILTER (WHERE NOT is_error) AS views,
    avg(metric_value) FILTER (WHERE lower(metric_name) IN ('duration', 'pageloadtime', 'load_time', 'pageload')) AS avg_load_time,
    quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'LCP') AS lcp_p75,
    quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'CLS') AS cls_p75,
    count(DISTINCT beacon_id) FILTER (WHERE is_error) AS errors
    FROM events GROUP BY 1 HAVING views > 0 ORDER BY errors DESC, avg_load_time DESC NULLS LAST LIMIT 5`, [], params.signal);
  const environments = await engine.executeSql<Record<string, unknown>>(`${events}
    SELECT kind, name, count(DISTINCT beacon_id) AS count FROM events,
      LATERAL (VALUES ('browsers', browser_name), ('os', os_name), ('devices', device_type)) AS env(kind, name)
    WHERE NOT is_error GROUP BY 1, 2`, [], params.signal);
  const trends = await engine.executeSql<Record<string, unknown>>(`${events} SELECT (ts_ms // 3600000) * 3600000 AS bucket,
    quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'LCP') AS lcp,
    quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'CLS') AS cls,
    count(DISTINCT beacon_id) FILTER (WHERE is_error) AS errors,
    count(DISTINCT beacon_id) FILTER (WHERE NOT is_error) AS views,
    count(DISTINCT beacon_id) FILTER (WHERE NOT is_error AND NOT starts_with(metric_name, 'event_')) AS pageviews,
    count(DISTINCT beacon_id) FILTER (WHERE starts_with(metric_name, 'event_')) AS interactions
    FROM events GROUP BY 1 ORDER BY 1`, [], params.signal);
  const seconds = (value: unknown) => value == null ? null : Number(value) / 1000;
  const vitals = (name: "LCP" | "CLS" | "INP") => {
    const metric = data.metrics[name];
    return {
      p75: metric.count ? name === "LCP" ? seconds(metric.p75) : metric.p75 : null,
      distribution: {
        good: metric.good_pct,
        needs_improvement: metric.needs_improvement_pct,
        poor: metric.poor_pct,
      },
    };
  };
  return {
    ...data,
    beacon_count: Number(counts?.beacons ?? 0),
    pageview_count: Number(counts?.pageviews ?? 0),
    interaction_count: Number(counts?.interactions ?? 0),
    error_count: Number(counts?.errors ?? 0),
    vitals: { lcp: vitals("LCP"), cls: vitals("CLS"), inp: vitals("INP"),
      fid: { p75: null, fcp: data.metrics.FCP.count ? seconds(data.metrics.FCP.p75) : null,
        ttfb: data.metrics.TTFB.count ? seconds(data.metrics.TTFB.p75) : null } },
    worst_pages: pages.map((row) => ({
      path: String(row.page_url ?? "/"),
      views: Number(row.views),
      avg_load_time: seconds(row.avg_load_time),
      lcp_p75: seconds(row.lcp_p75),
      cls_p75: row.cls_p75,
      error_rate: Number(row.errors) / (Number(row.views) + Number(row.errors)) * 100,
    })),
    errors: data.top_errors.map((row) => ({
      message: String(row.message ?? ""),
      file: String(row.source ?? ""),
      line: Number(row.lineno ?? 0), col: Number(row.colno ?? 0), count: Number(row.count ?? 0),
    })),
    trends: {
      timestamps: trends.map((row) => new Date(Number(row.bucket)).toISOString()),
      lcp: trends.map((row) => seconds(row.lcp)),
      cls: trends.map((row) => row.cls),
      error_rate: trends.map((row) => Number(row.errors) / (Number(row.views) + Number(row.errors)) * 100),
      pageviews: trends.map((row) => Number(row.pageviews)),
      interactions: trends.map((row) => Number(row.interactions)),
      errors: trends.map((row) => Number(row.errors)),
    },
    environments: Object.fromEntries(["browsers", "os", "devices"].map((kind) =>
      [kind, Object.fromEntries(environments.filter((row) => row.kind === kind).map((row) => [String(row.name ?? "Unknown"), Number(row.count)]))],
    )),
  };
}

export async function cloudRumLiveEvents(params: RumQueryParams) {
  const data = await queryRumLiveEvents(params);
  return data.events.map((row) => ({
    time: new Date(Number(row.ts_ms)).toISOString(),
    type: String(row.event_type),
    path: String(row.page_url ?? "/"),
    desc: row.event_type === "error" ? String(row.message ?? "") : `${row.metric_name}: ${row.metric_value}`,
    browser: String(row.browser_name ?? ""),
    os: String(row.os_name ?? ""),
    raw_log: row,
  }));
}
