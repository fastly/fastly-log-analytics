import { getDuckDBEngine, compileFiltersToSql, type DuckDBReadContext } from "./dashboard/duckdb";
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
  min_Baseline,
  resolveInsightWindow,
  type InsightsRequest,
  type CacheCollapseDetailRequest,
} from "./insights";
import { escapeSqlLiteral } from "./sql-validator";

export { CLOUD_INSIGHT_DETECTORS } from "./insights";

/** Every request column the Insights and cache-collapse scans read. */
export const INSIGHT_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "url", "status", "cache", "elapsed", "resp_bytes", "ip", "city", "region",
  "country", "p_type", "method", "referer", "ua", "pop", "server_region", "metro", "asn", "transport",
] as const;

/** The columns a column-projected Insights load needs, including filtered fields. */
export function insightScanColumns(filters: readonly DashboardFilter[]): string[] {
  return [...new Set([...INSIGHT_SCAN_COLUMNS, ...filters.map((filter) => filter.field)])];
}

export async function queryCloudInsights(
  body: InsightsRequest,
  options: { signal: AbortSignal; maskIps: boolean; filters: DashboardFilter[]; engine?: DuckDBReadContext },
) {
  const engine = options.engine ?? getDuckDBEngine().readContext();
  const { end, start, baselineHours, windowStart: windowStartMs } = resolveInsightWindow(body);
  const windowStart = new Date(windowStartMs).toISOString();
  const filters = compileFiltersToSql(options.filters, false, [], engine.getCustomColumnTypes());
  const scope = `WITH cloud_insight_scope AS (
    SELECT * FROM requests WHERE ts_ms >= ${start} AND ts_ms < ${end} AND (${filters || "TRUE"})
  )`;
  const [availableColumns, totals] = await Promise.all([
    engine.getPresentColumns(),
    engine.executeSql(`${scope} SELECT COUNT(*) FILTER (WHERE ts_ms >= ${windowStartMs}) AS requests,
      COUNT(*) FILTER (WHERE ts_ms < ${windowStartMs}) AS baseline_requests,
      MIN(ts_ms) FILTER (WHERE ts_ms < ${windowStartMs}) AS first_ms
      FROM cloud_insight_scope`, [], options.signal),
  ]);
  const availability = handleInsightAvailability(body.service_id, availableColumns);
  const availableIds = new Set(availability.insights.filter((d) => d.available).map((d) => d.id));
  const historyHours = totals[0]?.first_ms == null ? 0
    : Math.max(0, (windowStartMs - Number(totals[0].first_ms)) / 3_600_000);
  const scan = (builder: (table: string, start: string) => string, ids: string[]) =>
    Number(totals[0]?.requests ?? 0) > 0 && ids.some((id) => availableIds.has(id) &&
      (historyHours >= Math.min(min_Baseline, baselineHours) || CURRENT_WINDOW_INSIGHT_DETECTORS.has(id)))
      ? engine.executeSql(`${scope} SELECT * FROM (${builder("cloud_insight_scope", windowStart)})`, [], options.signal)
      : Promise.resolve([]);
  const [urlRows, ipRows, cityRows, dimensionRows, asnRows, geoRows, scalarRows] = await Promise.all([
    scan(_coalesced_url_aggregates, ["error_spikes", "cache_collapse", "cacheability_regression", "latency_regression", "tail_latency"]),
    scan(_coalesced_ip_security_aggregates, ["credential_enumeration", "content_discovery"]),
    scan(_coalesced_city_aggregates, ["city_surges", "city_error_spikes", "city_latency_regressions", "new_city_traffic"]),
    scan(_coalesced_dim_aggregates, ["new_country_traffic", "ua_monoculture", "referer_monoculture", "method_drift", "proxy_surge"]),
    scan(_coalesced_asn_aggregates, ["asn_concentration", "new_asn_traffic", "asn_hosting_shift"]),
    scan(_coalesced_geo_latency_aggregates, ["pop_latency_regression"]),
    scan(_coalesced_scalar_aggregates, ["cache_hit_cliff", "http3_fallback"]),
  ]);
  const envelope = await handleInsights(body, {
    now: new Date(end),
    maskIps: options.maskIps,
    availableColumns,
    availableHistoryHours: historyHours,
    urlAggregateRows: urlRows,
    ipSecurityRows: ipRows,
    cityAggregateRows: cityRows,
    dimensionAggregateRows: dimensionRows,
    asnAggregateRows: asnRows,
    geoLatencyRows: geoRows,
    scalarAggregateRows: scalarRows,
    windowTotalRequests: Number(totals[0]?.requests ?? 0),
    baselineTotalRequests: Number(totals[0]?.baseline_requests ?? 0),
  });
  return {
    ...envelope,
    unsupported_detectors: availability.unavailable.filter((d) => d.reason === "cloud_detector_unsupported")
      .map(({ id, reason }) => ({ id, reason })),
    unavailable_detectors: availability.unavailable,
  };
}

export async function queryCloudCacheCollapse(
  body: CacheCollapseDetailRequest,
  options: { signal: AbortSignal; maskIps: boolean; engine?: DuckDBReadContext },
) {
  const engine = options.engine ?? getDuckDBEngine().readContext();
  const { end, start, windowStart } = resolveInsightWindow(body);
  const where = `url = '${escapeSqlLiteral(body.url)}' AND ts_ms >= ${start} AND ts_ms < ${end}`;
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
