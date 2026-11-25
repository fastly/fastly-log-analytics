import { getDuckDBEngine } from "./dashboard/duckdb";
import { parseEpochMs, roundTo } from "./engine-utils";
import { buildRumWhere, queryRumAnalytics, queryRumLiveEvents, type RumQueryParams } from "./rum";
import { queryRumWireFromRollup, type RumRollupSource } from "./rum-rollup";

const BEACON_IDENTITY = `coalesce(nullif(request_event_id, ''), nullif(client_id, '') || '_' || round(ts_ms / 1000.0)::BIGINT::VARCHAR,
  scoped_key || '_' || ts_ms::VARCHAR)`;
const ROW_WEIGHT = `CASE WHEN coalesce(sample_rate, 1.0) > 0 AND coalesce(sample_rate, 1.0) < 1.0 THEN 1.0 / sample_rate ELSE 1.0 END`;
const PAGE_VITALS = `coalesce(metric_name, '') <> 'ttlb'`;
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
const MAX_PADDED_TREND_BUCKETS = 2000;

function decodePct(value: string): string {
  if (!value.includes("%")) return value;
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export async function cloudRumUsage(params: RumQueryParams, classARate: number) {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  const where = buildRumWhere(params);
  const errorWhere = buildRumWhere(params, "client_errors");
  const rows = await engine.executeSql<Record<string, unknown>>(`WITH events AS (
    SELECT ts_ms, ${BEACON_IDENTITY} AS beacon_id, ${ROW_WEIGHT} AS w FROM client_vitals WHERE ${where} AND ${PAGE_VITALS}
    UNION ALL SELECT ts_ms, ${BEACON_IDENTITY}, ${ROW_WEIGHT} AS w FROM client_errors WHERE ${errorWhere}
  ), per_beacon AS (
    SELECT strftime(epoch_ms(ts_ms), '%Y-%m-%d') AS date, beacon_id, max(w) AS w
    FROM events GROUP BY 1, 2
  ) SELECT date, round(sum(w))::BIGINT AS beacons FROM per_beacon GROUP BY 1 ORDER BY 1`, [], params.signal);
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

export async function cloudRumAnalytics(
  params: RumQueryParams & { rollupSource?: RumRollupSource },
) {
  if (params.rollupSource) {
    const rolled = await queryRumWireFromRollup(params.rollupSource, params);
    if (rolled) return rolled;
  }
  const engine = params.engine ?? getDuckDBEngine().readContext();
  const data = await queryRumAnalytics({ ...params, engine });
  const where = buildRumWhere(params);
  const errorWhere = buildRumWhere(params, "client_errors");
  const identity = BEACON_IDENTITY;
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  const hourly = startMs == null || endMs == null || (endMs - startMs) <= 48 * HOUR_MS;
  const stepMs = hourly ? HOUR_MS : DAY_MS;

  // MATERIALIZED: the vitals/errors views are one UNION ALL branch per parquet
  // file, and DuckDB keeps a hash table per branch for every GROUP BY / DISTINCT
  // below; a day of per-period files blew the memory limit without it.
  const events = `WITH events AS MATERIALIZED (
    SELECT ts_ms, ${identity} AS beacon_id, page_url, browser_name, os_name, device_type,
      metric_name, metric_value, FALSE AS is_error, ${ROW_WEIGHT} AS w
    FROM client_vitals WHERE ${where} AND ${PAGE_VITALS}
    UNION ALL SELECT ts_ms, ${identity}, page_url, browser_name, os_name, device_type,
      NULL, NULL, TRUE, ${ROW_WEIGHT} AS w
    FROM client_errors WHERE ${errorWhere}
  )`;
  const counts = (await engine.executeSql<Record<string, unknown>>(`${events}, per_beacon AS (
    SELECT beacon_id,
      max(w) AS w_all,
      max(w) FILTER (WHERE NOT is_error AND NOT starts_with(coalesce(metric_name, ''), 'event_')) AS w_pv,
      max(w) FILTER (WHERE starts_with(coalesce(metric_name, ''), 'event_')) AS w_int,
      max(w) FILTER (WHERE is_error) AS w_err,
      bool_or(w > 1.0) AS sampled
    FROM events GROUP BY 1
  ) SELECT
    coalesce(round(sum(w_all)), 0)::BIGINT AS beacons,
    coalesce(round(sum(w_pv)), 0)::BIGINT AS pageviews,
    coalesce(round(sum(w_int)), 0)::BIGINT AS interactions,
    coalesce(round(sum(w_err)), 0)::BIGINT AS errors,
    coalesce(bool_or(sampled), FALSE) AS sampled
  FROM per_beacon`, [], params.signal))[0];

  const vitalRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT upper(coalesce(metric_name, '')) AS metric_name,
      quantile_cont(metric_value, 0.75)::DOUBLE AS p75,
      sum(${ROW_WEIGHT})::DOUBLE AS total_w,
      sum(${ROW_WEIGHT}) FILTER (WHERE coalesce(nullif(lower(rating), ''), '') = 'good' OR (coalesce(nullif(rating, ''), '') = '' AND CASE upper(coalesce(metric_name, ''))
        WHEN 'LCP' THEN metric_value <= 2500
        WHEN 'CLS' THEN metric_value <= 0.1
        WHEN 'INP' THEN metric_value <= 200
        WHEN 'FCP' THEN metric_value <= 1800
        WHEN 'TTFB' THEN metric_value <= 800
        ELSE metric_value <= 2500 END))::DOUBLE AS good_w,
      sum(${ROW_WEIGHT}) FILTER (WHERE coalesce(nullif(lower(rating), ''), '') = 'poor' OR (coalesce(nullif(rating, ''), '') = '' AND CASE upper(coalesce(metric_name, ''))
        WHEN 'LCP' THEN metric_value > 4000
        WHEN 'CLS' THEN metric_value > 0.25
        WHEN 'INP' THEN metric_value > 500
        WHEN 'FCP' THEN metric_value > 3000
        WHEN 'TTFB' THEN metric_value > 1800
        ELSE metric_value > 4000 END))::DOUBLE AS poor_w
    FROM client_vitals
    WHERE ${where} AND ${PAGE_VITALS}
    GROUP BY 1`,
    [],
    params.signal,
  );

  const pages = await engine.executeSql<Record<string, unknown>>(`${events}, page_beacons AS (
    SELECT page_url, beacon_id,
      max(w) FILTER (WHERE NOT is_error) AS w_view,
      max(w) FILTER (WHERE is_error) AS w_err
    FROM events GROUP BY 1, 2
  ), page_w AS (
    SELECT page_url,
      coalesce(round(sum(w_view)), 0)::BIGINT AS views,
      coalesce(round(sum(w_err)), 0)::BIGINT AS errors
    FROM page_beacons GROUP BY 1 HAVING views > 0
  ), page_stats AS (
    SELECT page_url,
      avg(metric_value) FILTER (WHERE metric_name = 'page_load') AS avg_load_time,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'LCP') AS lcp_p75,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'CLS') AS cls_p75
    FROM events WHERE NOT is_error GROUP BY 1
  )
  SELECT pw.page_url, pw.views, pw.errors, ps.avg_load_time, ps.lcp_p75, ps.cls_p75,
    pw.errors * 100.0 / nullif(pw.views + pw.errors, 0) AS error_rate
  FROM page_w pw LEFT JOIN page_stats ps USING (page_url)
  ORDER BY error_rate DESC NULLS LAST, ps.avg_load_time DESC NULLS LAST, pw.page_url LIMIT 5`, [], params.signal);

  const environments = await engine.executeSql<Record<string, unknown>>(`${events}, env_beacons AS (
    SELECT kind, coalesce(nullif(name, ''), 'Unknown') AS name, beacon_id, max(w) AS w
    FROM events,
      LATERAL (VALUES ('browsers', browser_name), ('os', os_name), ('devices', device_type)) AS env(kind, name)
    WHERE NOT is_error
    GROUP BY 1, 2, 3
  ) SELECT kind, name, coalesce(round(sum(w)), 0)::BIGINT AS count FROM env_beacons GROUP BY 1, 2`, [], params.signal);

  const errorRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      coalesce(nullif(error_message, ''), nullif(message, ''), '') AS message,
      coalesce(filename, source, '') AS file,
      coalesce(lineno, 0)::BIGINT AS line,
      coalesce(colno, 0)::BIGINT AS col,
      coalesce(round(sum(${ROW_WEIGHT})), 0)::BIGINT AS count
    FROM client_errors WHERE ${errorWhere}
    GROUP BY 1, 2, 3, 4
    ORDER BY count DESC, message, file, line, col
    LIMIT 3`,
    [],
    params.signal,
  );

  const trendRows = await engine.executeSql<Record<string, unknown>>(`${events}, bucket_beacons AS (
    SELECT (ts_ms // ${stepMs}) * ${stepMs} AS bucket, beacon_id,
      max(w) FILTER (WHERE NOT is_error) AS w_view,
      max(w) FILTER (WHERE NOT is_error AND NOT starts_with(coalesce(metric_name, ''), 'event_')) AS w_pv,
      max(w) FILTER (WHERE starts_with(coalesce(metric_name, ''), 'event_')) AS w_int,
      max(w) FILTER (WHERE is_error) AS w_err
    FROM events GROUP BY 1, 2
  ), bucket_counts AS (
    SELECT bucket,
      coalesce(round(sum(w_view)), 0)::BIGINT AS views,
      coalesce(round(sum(w_pv)), 0)::BIGINT AS pageviews,
      coalesce(round(sum(w_int)), 0)::BIGINT AS interactions,
      coalesce(round(sum(w_err)), 0)::BIGINT AS errors
    FROM bucket_beacons GROUP BY 1
  ), bucket_metrics AS (
    SELECT (ts_ms // ${stepMs}) * ${stepMs} AS bucket,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'LCP') AS lcp,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'CLS') AS cls,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'FCP') AS fcp,
      quantile_cont(metric_value, 0.75) FILTER (WHERE upper(metric_name) = 'TTFB') AS ttfb
    FROM events WHERE NOT is_error GROUP BY 1
  )
  SELECT bc.bucket, bm.lcp, bm.cls, bm.fcp, bm.ttfb, bc.views, bc.pageviews, bc.interactions, bc.errors
  FROM bucket_counts bc LEFT JOIN bucket_metrics bm USING (bucket)
  ORDER BY bc.bucket`, [], params.signal);

  const seconds = (value: unknown): number | null => {
    if (value == null) return null;
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return roundTo(n > 20 ? n / 1000 : n, 2);
  };

  const vitalsMap = new Map<string, Record<string, unknown>>();
  for (const row of vitalRows) {
    vitalsMap.set(String(row.metric_name ?? "").toUpperCase(), row);
  }
  const shapeCwv = (name: "LCP" | "CLS" | "INP" | "FCP" | "TTFB") => {
    const row = vitalsMap.get(name);
    if (!row || !Number(row.total_w ?? 0)) {
      return { p75: null, distribution: { good: 0, needs_improvement: 0, poor: 0 } };
    }
    const rawP75 = row.p75 == null ? null : Number(row.p75);
    const p75 = rawP75 == null ? null
      : name === "CLS" ? roundTo(rawP75, 3)
      : name === "INP" ? Math.round(rawP75)
      : seconds(rawP75);
    const total = Number(row.total_w);
    const good = roundTo((Number(row.good_w ?? 0) * 100) / total, 2);
    const poor = roundTo((Number(row.poor_w ?? 0) * 100) / total, 2);
    return {
      p75,
      distribution: {
        good,
        needs_improvement: roundTo(Math.max(0, 100 - good - poor), 2),
        poor,
      },
    };
  };
  const fidRow = vitalsMap.get("FID");
  const fcpVital = shapeCwv("FCP");
  const ttfbVital = shapeCwv("TTFB");

  const trendByBucket = new Map<number, Record<string, unknown>>();
  for (const row of trendRows) {
    trendByBucket.set(Number(row.bucket), row);
  }
  const paddedBuckets: number[] = [];
  if (!data.no_data && startMs != null && endMs != null && endMs > startMs) {
    const firstBucket = Math.floor(startMs / stepMs) * stepMs;
    const lastBucket = Math.floor((endMs - 1) / stepMs) * stepMs;
    const totalBuckets = Math.floor((lastBucket - firstBucket) / stepMs) + 1;
    if (totalBuckets > 0 && totalBuckets <= MAX_PADDED_TREND_BUCKETS) {
      for (let b = firstBucket; b <= lastBucket; b += stepMs) paddedBuckets.push(b);
    }
  }
  const bucketKeys = paddedBuckets.length > 0 ? paddedBuckets : trendRows.map((row) => Number(row.bucket));

  const timestamps: string[] = [];
  const lcpSeries: Array<number | null> = [];
  const clsSeries: Array<number | null> = [];
  const fcpSeries: Array<number | null> = [];
  const ttfbSeries: Array<number | null> = [];
  const errorRateSeries: Array<number | null> = [];
  const pageviewSeries: number[] = [];
  const interactionSeries: number[] = [];
  const errorSeries: number[] = [];
  for (const bucket of bucketKeys) {
    const row = trendByBucket.get(bucket);
    timestamps.push(new Date(bucket).toISOString());
    lcpSeries.push(row ? seconds(row.lcp) : null);
    clsSeries.push(row && row.cls != null ? roundTo(Number(row.cls), 3) : null);
    fcpSeries.push(row ? seconds(row.fcp) : null);
    ttfbSeries.push(row ? seconds(row.ttfb) : null);
    const views = row ? Number(row.views ?? 0) : 0;
    const errs = row ? Number(row.errors ?? 0) : 0;
    const denom = views + errs;
    errorRateSeries.push(row && denom > 0 ? roundTo((errs * 100) / denom, 2) : null);
    pageviewSeries.push(row ? Number(row.pageviews ?? 0) : 0);
    interactionSeries.push(row ? Number(row.interactions ?? 0) : 0);
    errorSeries.push(errs);
  }

  return {
    ...data,
    is_mock: false,
    estimated: Boolean(counts?.sampled),
    beacon_count: Number(counts?.beacons ?? 0),
    pageview_count: Number(counts?.pageviews ?? 0),
    interaction_count: Number(counts?.interactions ?? 0),
    error_count: Number(counts?.errors ?? 0),
    vitals: {
      lcp: shapeCwv("LCP"),
      cls: shapeCwv("CLS"),
      inp: shapeCwv("INP"),
      fcp: fcpVital,
      ttfb: ttfbVital,
      fid: {
        p75: fidRow?.p75 == null ? null : roundTo(Number(fidRow.p75), 1),
        fcp: fcpVital.p75,
        ttfb: ttfbVital.p75,
      },
    },
    worst_pages: pages.map((row) => ({
      path: decodePct(String(row.page_url ?? "/")),
      views: Number(row.views ?? 0),
      avg_load_time: seconds(row.avg_load_time),
      lcp_p75: seconds(row.lcp_p75),
      cls_p75: row.cls_p75 == null ? null : roundTo(Number(row.cls_p75), 3),
      error_rate: row.error_rate == null ? 0 : roundTo(Number(row.error_rate), 2),
    })),
    errors: errorRows.map((row) => ({
      message: String(row.message ?? ""),
      file: String(row.file ?? ""),
      line: Number(row.line ?? 0),
      col: Number(row.col ?? 0),
      count: Number(row.count ?? 0),
    })),
    trends: {
      timestamps,
      lcp: lcpSeries,
      cls: clsSeries,
      fcp: fcpSeries,
      ttfb: ttfbSeries,
      error_rate: errorRateSeries,
      pageviews: pageviewSeries,
      interactions: interactionSeries,
      errors: errorSeries,
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
