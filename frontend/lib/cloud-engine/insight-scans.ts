import { compileFiltersToSql, duckSqlString, getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";
import {
  _coalesced_url_aggregates,
  _coalesced_ip_security_aggregates,
  _coalesced_city_aggregates,
  _coalesced_dim_aggregates,
  _coalesced_asn_aggregates,
  _coalesced_geo_latency_aggregates,
  _coalesced_scalar_aggregates,
  handleInsights,
  handleInsightAvailability,
  handleCacheCollapseDetail,
  CURRENT_WINDOW_INSIGHT_DETECTORS,
  INSIGHT_DEFINITIONS,
  build_insights_envelope,
  min_Baseline,
  resolveInsightWindow,
  type InsightCard,
  type InsightsRequest,
  type CacheCollapseDetailRequest,
} from "./insights";
import { cloudActiveLogFieldIds } from "./active-log-fields";
import { formatBudgetAmount, type CloudBudgetDetail } from "./range-loader";
import type { RollupResult } from "./dashboard/rollup-query";
import { INSIGHTS_DEFERRED, insightsDeferredDetectors, rollupInsightsBaseline } from "./insights-rollup";

export { CLOUD_INSIGHT_DETECTORS } from "./insights";

/** Every request column the Insights and cache-collapse scans read. */
export const INSIGHT_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "url", "status", "cache", "elapsed", "resp_bytes", "ip", "city", "region",
  "country", "p_type", "method", "referer", "ua", "pop", "server_region", "metro", "asn", "transport",
] as const;

/**
 * What an availability read loads: it needs only the files' schemas, which a
 * column-projected load keeps whole, so no column beyond the timestamps.
 */
export const INSIGHT_AVAILABILITY_COLUMNS: readonly string[] = ["timestamp", "ts_ms"];

/** Each coalesced scan, the columns its SQL reads, and the detectors it serves. */
const INSIGHT_SCANS = {
  url: { build: _coalesced_url_aggregates, columns: ["url", "status", "cache", "elapsed", "resp_bytes"],
    ids: ["error_spikes", "cache_collapse", "cacheability_regression", "latency_regression", "tail_latency"] },
  ip: { build: _coalesced_ip_security_aggregates, columns: ["ip", "url", "status"],
    ids: ["credential_enumeration", "content_discovery"] },
  city: { build: _coalesced_city_aggregates, columns: ["city", "region", "country", "status", "elapsed"],
    ids: ["city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"] },
  dimension: { build: _coalesced_dim_aggregates, columns: ["country", "p_type", "method", "referer", "ua"],
    ids: ["new_country_traffic", "ua_monoculture", "referer_monoculture", "method_drift", "proxy_surge"] },
  asn: { build: _coalesced_asn_aggregates, columns: ["asn", "p_type"],
    ids: ["asn_concentration", "new_asn_traffic", "asn_hosting_shift"] },
  geo: { build: _coalesced_geo_latency_aggregates, columns: ["pop", "server_region", "metro", "elapsed"],
    ids: ["pop_latency_regression"] },
  scalar: { build: _coalesced_scalar_aggregates, columns: ["transport", "cache"],
    ids: ["cache_hit_cliff", "http3_fallback"] },
} as const;

/**
 * The columns a column-projected Insights load needs, including filtered
 * fields; with `detectors`, only those the scans serving them read.
 */
export function insightScanColumns(filters: readonly DashboardFilter[], detectors?: readonly string[]): string[] {
  const columns = detectors
    ? ["timestamp", "ts_ms", ...Object.values(INSIGHT_SCANS)
      .filter((scan) => scan.ids.some((id) => detectors.includes(id))).flatMap((scan) => scan.columns)]
    : INSIGHT_SCAN_COLUMNS;
  return [...new Set([...columns, ...filters.map((filter) => filter.field)])];
}

/**
 * Evaluates the Cloud detectors over local Parquet. With `baseline` (summaries
 * of the unfiltered baseline), only the window is scanned raw and the baseline
 * columns come from the summaries; detectors the summaries can't answer come
 * back as placeholders listed in `deferred_detectors`, for a follow-up request
 * with `detectors` to evaluate raw.
 */
export async function queryCloudInsights(
  body: InsightsRequest,
  options: {
    signal: AbortSignal;
    maskIps: boolean;
    filters: DashboardFilter[];
    engine?: DuckDBReadContext;
    baseline?: RollupResult;
    earliestLogAtMs?: number | null;
  },
) {
  const engine = options.engine ?? getDuckDBEngine().readContext();
  const { end, start, baselineHours, windowStart: windowStartMs } = resolveInsightWindow(body);
  const windowStart = new Date(windowStartMs).toISOString();
  const filters = compileFiltersToSql(options.filters, false, [], engine.getCustomColumnTypes());
  const scanFrom = options.baseline ? windowStartMs : start;
  const scope = `WITH cloud_insight_scope AS (
    SELECT * FROM requests WHERE ts_ms >= ${scanFrom} AND ts_ms < ${end} AND (${filters || "TRUE"})
  )`;
  // The log field settings decide which detectors run; every built-in column exists in the files.
  const [enabledFields, totals] = await Promise.all([
    cloudActiveLogFieldIds(body.service_id, options.signal ?? new AbortController().signal),
    engine.executeSql(`${scope} SELECT COUNT(*) FILTER (WHERE ts_ms >= ${windowStartMs}) AS requests,
      COUNT(*) FILTER (WHERE ts_ms < ${windowStartMs}) AS baseline_requests,
      MIN(ts_ms) FILTER (WHERE ts_ms < ${windowStartMs}) AS first_ms
      FROM cloud_insight_scope`, [], options.signal),
  ]);
  const availability = handleInsightAvailability(body.service_id, enabledFields);
  const availableIds = new Set(availability.insights.filter((d) => d.available).map((d) => d.id));
  const baselineReqs = options.baseline ? null : Number(totals[0]?.baseline_requests ?? 0);
  const firstMs = options.baseline ? options.baseline.dataFromMs : totals[0]?.first_ms;
  const hasBaselineEvidence = options.baseline ? firstMs != null : (baselineReqs ?? 0) > 0 && firstMs != null;
  const effectiveFirstMs = !hasBaselineEvidence
    ? null
    : options.earliestLogAtMs != null && Number.isFinite(options.earliestLogAtMs)
      ? Math.min(Number(firstMs), options.earliestLogAtMs)
      : Number(firstMs);
  const historyHours = effectiveFirstMs == null ? 0 : Math.max(0, (end - effectiveFirstMs) / 3_600_000);
  const deferred = options.baseline ? insightsDeferredDetectors(options.baseline.rollup) : INSIGHTS_DEFERRED;
  const wanted = (id: string) => body.detectors ? body.detectors.includes(id)
    : !(options.baseline && deferred.has(id));
  const scan = ({ build, ids }: (typeof INSIGHT_SCANS)[keyof typeof INSIGHT_SCANS]) =>
    Number(totals[0]?.requests ?? 0) > 0 && ids.some((id) => wanted(id) && availableIds.has(id) &&
      (historyHours >= Math.min(min_Baseline, baselineHours) || CURRENT_WINDOW_INSIGHT_DETECTORS.has(id)))
      ? engine.executeSql(`${scope} SELECT * FROM (${build("cloud_insight_scope", windowStart)})`, [], options.signal)
      : Promise.resolve([]);
  const [urlRows, ipRows, cityRows, dimensionRows, asnRows, geoRows, scalarRows] = await Promise.all([
    scan(INSIGHT_SCANS.url), scan(INSIGHT_SCANS.ip), scan(INSIGHT_SCANS.city), scan(INSIGHT_SCANS.dimension),
    scan(INSIGHT_SCANS.asn), scan(INSIGHT_SCANS.geo), scan(INSIGHT_SCANS.scalar),
  ]);
  const summarized = options.baseline && rollupInsightsBaseline(options.baseline, {
    url: urlRows, ip: ipRows, city: cityRows, dimension: dimensionRows, asn: asnRows, geo: geoRows, scalar: scalarRows,
  });
  const envelope = await handleInsights(body, {
    now: new Date(end),
    maskIps: options.maskIps,
    availableColumns: enabledFields,
    availableHistoryHours: historyHours,
    urlAggregateRows: summarized?.rows.url ?? urlRows,
    ipSecurityRows: summarized?.rows.ip ?? ipRows,
    cityAggregateRows: summarized?.rows.city ?? cityRows,
    dimensionAggregateRows: summarized?.rows.dimension ?? dimensionRows,
    asnAggregateRows: summarized?.rows.asn ?? asnRows,
    geoLatencyRows: summarized?.rows.geo ?? geoRows,
    scalarAggregateRows: summarized?.rows.scalar ?? scalarRows,
    windowTotalRequests: Number(totals[0]?.requests ?? 0),
    baselineTotalRequests: summarized?.baselineRequests ?? Number(totals[0]?.baseline_requests ?? 0),
    baselineSource: summarized?.source,
  });
  // A follow-up's projection hides the columns other detectors need; only its own count.
  const unavailable = availability.unavailable.filter((d) => !body.detectors || body.detectors.includes(d.id));
  return {
    ...envelope,
    ...(summarized ? {
      rollup: summarized.meta,
      deferred_detectors: envelope.insights.filter((card) => card.placeholder).map((card) => card.id),
    } : {}),
    unsupported_detectors: unavailable.filter((d) => d.reason === "cloud_detector_unsupported")
      .map(({ id, reason }) => ({ id, reason })),
    unavailable_detectors: unavailable,
  };
}

/**
 * The envelope of a detector follow-up whose raw scan, even shortened, exceeds
 * the browser budget: each requested detector comes back as a finished card
 * saying it was not evaluated and why, so the rest of the page still renders.
 */
export function insightsOverBudget(body: InsightsRequest & { end_time?: string }, detail: CloudBudgetDetail) {
  const { windowHours, baselineHours, end, windowStart, start } = resolveInsightWindow({
    ...body, clamp_end: body.clamp_end ?? body.end_time,
  });
  const ids = body.detectors ?? [];
  const summary = `Not evaluated: this range needs ${formatBudgetAmount(detail.unit, detail.needed)
  } of raw logs but the browser scan budget is ${formatBudgetAmount(detail.unit, detail.limit)
  }; choose a shorter window or baseline.`;
  const notEvaluated = { reason: "scan_budget" as const, ...detail };
  const cards: InsightCard[] = INSIGHT_DEFINITIONS.filter((d) => ids.includes(d.id)).map((d) => ({
    id: d.id, title: d.title, description: d.description, category: d.category,
    severity: "info", summary, items: [], placeholder: false, not_evaluated: notEvaluated,
  }));
  return {
    ...build_insights_envelope(cards, new Date(windowStart).toISOString(), new Date(end).toISOString(),
      new Date(start).toISOString(), new Date(windowStart).toISOString(), windowHours, baselineHours),
    unevaluated_detectors: cards.map(({ id }) => ({ id, ...notEvaluated })),
  };
}

export async function queryCloudCacheCollapse(
  body: CacheCollapseDetailRequest,
  options: { signal: AbortSignal; maskIps: boolean; engine?: DuckDBReadContext },
) {
  const engine = options.engine ?? getDuckDBEngine().readContext();
  const { end, start, windowStart } = resolveInsightWindow(body);
  const where = `url = ${duckSqlString(body.url)} AND ts_ms >= ${start} AND ts_ms < ${end}`;
  const [rates, timelineRows, missRows] = await Promise.all([
    engine.executeSql(`SELECT
      COUNT(*) FILTER (WHERE ts_ms >= ${windowStart}) AS w_total,
      COUNT(*) FILTER (WHERE ts_ms < ${windowStart}) AS b_total,
      ${["w", "b"].flatMap((side) => ["HIT", "MISS", "PASS"].map((state) =>
        `COUNT(*) FILTER (WHERE ts_ms ${side === "w" ? ">=" : "<"} ${windowStart} AND starts_with(cache, '${state}')) AS ${side}_${state.toLowerCase() === "hit" ? "hits" : state.toLowerCase()}`,
      )).join(", ")} FROM requests WHERE ${where}`, [], options.signal),
    engine.executeSql(`SELECT epoch_ms((ts_ms // 3600000) * 3600000)::VARCHAR AS bucket,
      COUNT(*) AS total_requests, COUNT(*) FILTER (WHERE starts_with(cache, 'HIT')) AS real_hits,
      COUNT(*) FILTER (WHERE starts_with(cache, 'MISS')) AS misses
      FROM requests WHERE ${where} AND ts_ms >= ${windowStart} GROUP BY 1 ORDER BY 1`, [], options.signal),
    engine.executeSql(`SELECT timestamp::VARCHAR AS timestamp, cache, pop, ip, status FROM requests
      WHERE ${where} AND ts_ms >= ${windowStart} AND starts_with(cache, 'MISS')
      ORDER BY ts_ms DESC LIMIT 50`, [], options.signal),
  ]);
  type Context = NonNullable<Parameters<typeof handleCacheCollapseDetail>[1]>;
  return handleCacheCollapseDetail(body, {
    maskIps: options.maskIps,
    rates: rates[0] as Context["rates"],
    timelineRows: timelineRows as Context["timelineRows"],
    missRows: missRows as Context["missRows"],
  });
}
