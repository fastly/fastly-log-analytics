import { loadAsnNames } from "./asn-names";
import { getDuckDBEngine, duckSqlString } from "./dashboard/duckdb";
import { parseEpochMs, roundTo } from "./engine-utils";
import { formatAsnLabel } from "./performance";
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

export interface RumResourceLatencyItem {
  requests: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  matched_requests: number;
  edge_avg: number | null;
  edge_p50: number | null;
  edge_p95: number | null;
  edge_p99: number | null;
}

export interface RumSlowestUrlRow extends RumResourceLatencyItem {
  url: string;
}

export interface RumSlowestNetworkRow extends RumResourceLatencyItem {
  asn: string;
  label: string;
}

function shapeResourceLatency(row: Record<string, unknown>): RumResourceLatencyItem {
  const opt = (key: string) => {
    const raw = row[key];
    if (raw == null) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? roundTo(n, 2) : null;
  };
  return {
    requests: Number(row.requests ?? 0),
    avg: opt("avg_ms") ?? 0,
    p50: opt("p50_ms") ?? 0,
    p95: opt("p95_ms") ?? 0,
    p99: opt("p99_ms") ?? 0,
    matched_requests: Number(row.matched_requests ?? 0),
    edge_avg: opt("edge_avg_ms"),
    edge_p50: opt("edge_p50_ms"),
    edge_p95: opt("edge_p95_ms"),
    edge_p99: opt("edge_p99_ms"),
  };
}

export async function queryRumResourceTimings(params: RumQueryParams): Promise<{
  slowest_urls: RumSlowestUrlRow[];
  slowest_networks: RumSlowestNetworkRow[];
}> {
  const engine = params.engine ?? getDuckDBEngine().readContext();
  const ttlbWhere = `${buildRumWhere(params, "client_vitals", { includeTtlb: true })} AND metric_name = 'ttlb'`;
  const reqServiceClause = params.service_id
    ? `(service_id = ${duckSqlString(String(params.service_id))} OR service_id IS NULL)`
    : "1=1";
  const reqWindowClauses = [reqServiceClause, "rid IS NOT NULL", "rid != ''"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs != null) reqWindowClauses.push(`ts_ms >= ${Math.floor(startMs) - 600_000}`);
  if (endMs != null) reqWindowClauses.push(`ts_ms < ${Math.ceil(endMs) + 600_000}`);

  const runTtlbQueries = async (withReqJoin: boolean) => {
    const reqCte = withReqJoin
      ? `req_edge AS (
          SELECT rid, AVG(TRY_CAST(elapsed AS DOUBLE) / 1000.0) AS edge_ms, ANY_VALUE(asn) AS req_asn
          FROM requests
          WHERE ${reqWindowClauses.join(" AND ")}
          GROUP BY rid
        )`
      : `req_edge AS (SELECT NULL::VARCHAR AS rid, NULL::DOUBLE AS edge_ms, NULL::BIGINT AS req_asn WHERE FALSE)`;
    const aggCols = `
      coalesce(round(sum(v.w)), 0)::BIGINT AS requests,
      avg(v.metric_value)::DOUBLE AS avg_ms,
      quantile_cont(v.metric_value, 0.5)::DOUBLE AS p50_ms,
      quantile_cont(v.metric_value, 0.95)::DOUBLE AS p95_ms,
      quantile_cont(v.metric_value, 0.99)::DOUBLE AS p99_ms,
      coalesce(round(sum(v.w) FILTER (WHERE r.edge_ms IS NOT NULL)), 0)::BIGINT AS matched_requests,
      (avg(r.edge_ms) FILTER (WHERE r.edge_ms IS NOT NULL))::DOUBLE AS edge_avg_ms,
      (quantile_cont(r.edge_ms, 0.5) FILTER (WHERE r.edge_ms IS NOT NULL))::DOUBLE AS edge_p50_ms,
      (quantile_cont(r.edge_ms, 0.95) FILTER (WHERE r.edge_ms IS NOT NULL))::DOUBLE AS edge_p95_ms,
      (quantile_cont(r.edge_ms, 0.99) FILTER (WHERE r.edge_ms IS NOT NULL))::DOUBLE AS edge_p99_ms
    `;
    const urlSql = `WITH ttlb_rows AS (
      SELECT coalesce(nullif(req_id, ''), request_event_id) AS rid, resource_url, metric_value, ${ROW_WEIGHT} AS w
      FROM client_vitals
      WHERE ${ttlbWhere} AND resource_url IS NOT NULL AND resource_url != ''
    ), ${reqCte}
    SELECT v.resource_url AS url, ${aggCols}
    FROM ttlb_rows v LEFT JOIN req_edge r ON v.rid IS NOT NULL AND v.rid != '' AND r.rid = v.rid
    GROUP BY 1
    ORDER BY p99_ms DESC NULLS LAST, p95_ms DESC NULLS LAST, requests DESC
    LIMIT 20`;
    const netSql = `WITH ttlb_rows AS (
      SELECT coalesce(nullif(req_id, ''), request_event_id) AS rid, asn, metric_value, ${ROW_WEIGHT} AS w
      FROM client_vitals
      WHERE ${ttlbWhere}
    ), ${reqCte}, joined AS (
      SELECT coalesce(v.asn, r.req_asn) AS asn_val, v.metric_value, v.w, r.edge_ms
      FROM ttlb_rows v LEFT JOIN req_edge r ON v.rid IS NOT NULL AND v.rid != '' AND r.rid = v.rid
    )
    SELECT asn_val AS asn_key,
      coalesce(round(sum(w)), 0)::BIGINT AS requests,
      avg(metric_value)::DOUBLE AS avg_ms,
      quantile_cont(metric_value, 0.5)::DOUBLE AS p50_ms,
      quantile_cont(metric_value, 0.95)::DOUBLE AS p95_ms,
      quantile_cont(metric_value, 0.99)::DOUBLE AS p99_ms,
      coalesce(round(sum(w) FILTER (WHERE edge_ms IS NOT NULL)), 0)::BIGINT AS matched_requests,
      (avg(edge_ms) FILTER (WHERE edge_ms IS NOT NULL))::DOUBLE AS edge_avg_ms,
      (quantile_cont(edge_ms, 0.5) FILTER (WHERE edge_ms IS NOT NULL))::DOUBLE AS edge_p50_ms,
      (quantile_cont(edge_ms, 0.95) FILTER (WHERE edge_ms IS NOT NULL))::DOUBLE AS edge_p95_ms,
      (quantile_cont(edge_ms, 0.99) FILTER (WHERE edge_ms IS NOT NULL))::DOUBLE AS edge_p99_ms
    FROM joined
    WHERE asn_val IS NOT NULL AND asn_val > 0
    GROUP BY 1
    ORDER BY p99_ms DESC NULLS LAST, p95_ms DESC NULLS LAST, requests DESC
    LIMIT 20`;
    return Promise.all([
      engine.executeSql<Record<string, unknown>>(urlSql, [], params.signal),
      engine.executeSql<Record<string, unknown>>(netSql, [], params.signal),
    ]);
  };

  let urlRows: Record<string, unknown>[] = [];
  let netRows: Record<string, unknown>[] = [];
  try {
    [urlRows, netRows] = await runTtlbQueries(true);
  } catch (err) {
    if (params.signal?.aborted) throw err;
    [urlRows, netRows] = await runTtlbQueries(false);
  }

  await loadAsnNames().catch(() => {});
  const slowest_urls: RumSlowestUrlRow[] = urlRows
    .filter((r) => typeof r.url === "string" && r.url.length > 0)
    .map((row) => ({
      url: decodePct(String(row.url)),
      ...shapeResourceLatency(row),
    }));
  const slowest_networks: RumSlowestNetworkRow[] = netRows
    .map((row) => {
      const asnNum = row.asn_key != null ? String(Math.trunc(Number(row.asn_key))) : "";
      if (!asnNum || asnNum === "0") return null;
      return {
        asn: `AS${asnNum}`,
        label: formatAsnLabel(asnNum),
        ...shapeResourceLatency(row),
      };
    })
    .filter((row): row is RumSlowestNetworkRow => row !== null);

  return { slowest_urls, slowest_networks };
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

  const { slowest_urls, slowest_networks } = await queryRumResourceTimings({ ...params, engine });

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
    slowest_urls,
    slowest_networks,
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
  return data.events.map((row) => {
    const etype = String(row.event_type);
    const reqId = typeof row.req_id === "string" && row.req_id ? row.req_id : null;
    const resourceUrl = typeof row.resource_url === "string" && row.resource_url ? decodePct(row.resource_url) : null;
    const metricName = typeof row.metric_name === "string" && row.metric_name ? row.metric_name : null;
    const metricValue = row.metric_value != null && Number.isFinite(Number(row.metric_value)) ? Number(row.metric_value) : null;
    const desc = etype === "error"
      ? String(row.message ?? "")
      : etype === "resource"
        ? `Resource TTLB: ${metricValue != null ? `${Math.round(metricValue)}ms` : "—"}`
        : `${row.metric_name}: ${row.metric_value}`;
    return {
      time: new Date(Number(row.ts_ms)).toISOString(),
      type: etype,
      path: decodePct(String(row.page_url ?? "/")),
      desc,
      browser: String(row.browser_name ?? ""),
      os: String(row.os_name ?? ""),
      req_id: reqId,
      resource_url: resourceUrl,
      metric_name: metricName,
      metric_value: metricValue,
      raw_log: row,
    };
  });
}

export async function cloudRumRequestLog(
  params: RumQueryParams & { req_id?: string; timestamp?: string },
) {
  const reqId = String(params.req_id ?? "").trim();
  if (!reqId || reqId.length > 128 || /\s/.test(reqId)) {
    return { found: false, req_id: reqId, request: null };
  }
  const engine = params.engine ?? getDuckDBEngine().readContext();
  const clauses = [`rid = ${duckSqlString(reqId)}`];
  if (params.service_id && params.service_id !== "default") {
    clauses.push(`(service_id = ${duckSqlString(String(params.service_id))} OR service_id IS NULL)`);
  }
  const anchorMs = parseEpochMs(params.timestamp);
  if (anchorMs != null) {
    clauses.push(`ts_ms >= ${Math.floor(anchorMs) - 7_200_000} AND ts_ms < ${Math.ceil(anchorMs) + 7_200_000}`);
  }
  const rows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      ts_ms,
      rid,
      method,
      url,
      host,
      status,
      cache,
      pop,
      country,
      city,
      region,
      asn,
      round(TRY_CAST(elapsed AS DOUBLE) / 1000.0, 2) AS elapsed_ms,
      round(TRY_CAST(ttfb AS DOUBLE) * 1000.0, 2) AS ttfb_ms,
      resp_bytes,
      ip AS client_ip,
      ua,
      proto,
      tls
    FROM requests
    WHERE ${clauses.join(" AND ")}
    ORDER BY ts_ms DESC
    LIMIT 1`,
    [],
    params.signal,
  );
  if (!rows.length) return { found: false, req_id: reqId, request: null };
  const row = { ...rows[0] };
  row.timestamp = row.ts_ms != null ? new Date(Number(row.ts_ms)).toISOString() : null;
  delete row.ts_ms;
  return { found: true, req_id: reqId, request: row };
}
