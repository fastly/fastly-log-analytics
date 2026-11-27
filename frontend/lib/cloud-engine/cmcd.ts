import {
  duckSqlString,
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
  type DuckDBReadContext,
} from "./dashboard/duckdb";
import type { FiltersPayload } from "@/types/api";
import { parseEpochMs, roundTo } from "./engine-utils";
import { cloudActiveLogFieldIds } from "./active-log-fields";
import { fetchCloudMetadata } from "./metadata-cache";
import { cmcdFromRollup, cmcdRollupEligible, type CmcdRollupSource } from "./cmcd-rollup";
import { analyticsFiltersToSql, rangeServiceSql } from "./query-params";
import { formatAsnDisplayLabel, loadAsnNames } from "./asn-names";

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

export const CMCD_SESSION_DURATION_BUCKETS = ["<30s", "30s–2m", "2–5m", "5–10m", "10–30m", "30m–1h", ">1h"] as const;

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
  filters?: FiltersPayload;
  bucket_seconds?: number;
  top_n?: number;
  sections?: readonly string[] | null;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns"> & Partial<Pick<DuckDBReadContext, "getCustomColumnTypes">>;
  rollupSource?: CmcdRollupSource;
  customColumnTypes?: ReturnType<DuckDBReadContext["getCustomColumnTypes"]>;
  available?: boolean;
  signal?: AbortSignal;
}

export function classifySessionDurationBucket(durationSeconds: number): string {
  if (durationSeconds < 30) return "<30s";
  if (durationSeconds < 120) return "30s–2m";
  if (durationSeconds < 300) return "2–5m";
  if (durationSeconds < 600) return "5–10m";
  if (durationSeconds < 1800) return "10–30m";
  if (durationSeconds < 3600) return "30m–1h";
  return ">1h";
}

const MAX_PADDED_BUCKETS = 2000;

function padSeries(
  rows: Record<string, unknown>[],
  fromMs: number | null,
  toMs: number | null,
  bucketMs: number,
  emptyRow: (tsIso: string) => Record<string, unknown>,
): Record<string, unknown>[] {
  if (rows.length === 0 || bucketMs <= 0 || fromMs == null || toMs == null || toMs <= fromMs) {
    return rows;
  }
  const stAligned = Math.floor(fromMs / bucketMs) * bucketMs;
  const etAligned = Math.floor((toMs - 1) / bucketMs) * bucketMs;
  if (etAligned < stAligned || Math.floor((etAligned - stAligned) / bucketMs) + 1 > MAX_PADDED_BUCKETS) {
    return rows;
  }
  const byBucket = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    if (typeof row.bucket === "string") byBucket.set(row.bucket, row);
  }
  const out: Record<string, unknown>[] = [];
  for (let ts = stAligned; ts <= etAligned; ts += bucketMs) {
    const tsIso = new Date(ts).toISOString();
    out.push(byBucket.get(tsIso) ?? emptyRow(tsIso));
  }
  return out;
}

export function padCmcdTimeseries(
  series: {
    sessions_ts: Record<string, unknown>[];
    buffer_health_ts: Record<string, unknown>[];
    bitrate_ts: Record<string, unknown>[];
    throughput_ts: Record<string, unknown>[];
    startup_ts: Record<string, unknown>[];
  },
  startTime: string | number | undefined,
  endTime: string | number | undefined,
  bucketMs: number,
) {
  const fromMs = parseEpochMs(startTime);
  const toMs = parseEpochMs(endTime);
  return {
    sessions_ts: padSeries(series.sessions_ts, fromMs, toMs, bucketMs, (tsIso) => ({
      bucket: tsIso,
      concurrent_sessions: 0,
      new_sessions: 0,
      rebuffer_session_pct: null,
      timestamp: tsIso,
      active_sessions: 0,
      rebuffer_events: 0,
      rebuffer_rate: 0,
    })),
    buffer_health_ts: padSeries(series.buffer_health_ts, fromMs, toMs, bucketMs, (tsIso) => ({
      bucket: tsIso,
      p50_buffer: null,
      p95_buffer: null,
      starvation_rate: null,
      timestamp: tsIso,
      avg_buffer_ms: 0,
      p10_buffer_ms: 0,
      p50_buffer_ms: 0,
      rebuffer_events: 0,
    })),
    bitrate_ts: padSeries(series.bitrate_ts, fromMs, toMs, bucketMs, (tsIso) => ({
      bucket: tsIso,
      avg_bitrate: null,
      timestamp: tsIso,
      avg_encoded_bitrate: 0,
      avg_top_bitrate: 0,
      utilization_ratio: null,
    })),
    throughput_ts: padSeries(series.throughput_ts, fromMs, toMs, bucketMs, (tsIso) => ({
      bucket: tsIso,
      p50: null,
      p95: null,
      p99: null,
      timestamp: tsIso,
      avg_throughput_kbps: 0,
      avg_bitrate_kbps: 0,
      headroom_ratio: 0,
    })),
    startup_ts: padSeries(series.startup_ts, fromMs, toMs, bucketMs, (tsIso) => ({
      bucket: tsIso,
      startup_ratio: null,
      timestamp: tsIso,
      startup_requests: 0,
      avg_startup_buffer_ms: 0,
      avg_startup_ttfb_ms: 0,
    })),
  };
}

export function emptyCmcdAggregates(available = true) {
  return {
    available,
    has_data: false,
    overview: {
      active_sessions: 0, peak_viewers: 0, avg_session_duration: null as number | null,
      rebuffer_rate: null as number | null, avg_bitrate: null as number | null,
      avg_buffer_length: null as number | null, median_throughput: null as number | null,
    },
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
    session_duration_dist: [] as Record<string, unknown>[],
    bitrate_ladder: [] as Record<string, unknown>[],
    stream_type_dist: [] as Record<string, unknown>[],
  };
}

const BITRATE_LADDER_TIERS: Record<number, string> = {
  1: "< 1 Mbps (SD)",
  2: "1–2.5 Mbps (720p)",
  3: "2.5–5 Mbps (1080p)",
  4: "5–10 Mbps (1440p)",
  5: "10+ Mbps (4K)",
};


function buildCmcdWhere(params: CmcdQueryParams): string {
  const clauses: string[] = [
    rangeServiceSql(params, [
      "(NULLIF(cmcd_sid, '') IS NOT NULL OR cmcd_br IS NOT NULL OR cmcd_bl IS NOT NULL OR NULLIF(cmcd_ot, '') IS NOT NULL)",
    ]),
  ];
  if (params.filter_sf) {
    clauses.push(`cmcd_sf = ${duckSqlString(String(params.filter_sf))}`);
  }
  if (params.filter_ot) {
    clauses.push(`cmcd_ot = ${duckSqlString(String(params.filter_ot))}`);
  }
  if (params.filter_st) {
    clauses.push(`cmcd_st = ${duckSqlString(String(params.filter_st))}`);
  }
  if (params.filter_cid) {
    clauses.push(`cmcd_cid = ${duckSqlString(String(params.filter_cid))}`);
  }
  clauses.push(analyticsFiltersToSql(params.filters, params.customColumnTypes));
  return clauses.join(" AND ");
}

/** Every request column the /api/cmcd/aggregates scans read. */
export const CMCD_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "service_id", "cmcd_sid", "cmcd_cid", "cmcd_br", "cmcd_bl", "cmcd_bs", "cmcd_mtp", "cmcd_ot",
  "cmcd_sf", "cmcd_st", "cmcd_tb", "cmcd_su", "geo_country", "country", "client_asn", "asn",
  "as_name", "ttfb_ms", "ttfb", "time_elapsed_usec", "elapsed",
] as const;

/** A Parquet file must contain at least one of these columns to satisfy `buildCmcdWhere`. */
export const CMCD_REQUIRED_ANY_COLUMNS = ["cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot"] as const;

const CMCD_WHERE_BASE_COLUMNS = [
  "timestamp", "ts_ms", "service_id", "cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot",
] as const;

const CMCD_BREAKDOWN_SECTION_COLUMNS: Readonly<Record<string, readonly string[]>> = {
  bitrate_ladder: ["cmcd_bs"],
  stream_type_dist: ["cmcd_st", "cmcd_bs"],
  top_content: ["cmcd_cid", "cmcd_bs"],
  rebuffer_by_country: ["geo_country", "country", "cmcd_bs", "cmcd_mtp"],
  rebuffer_by_asn: ["client_asn", "asn", "as_name", "cmcd_bs", "cmcd_mtp"],
};

function isBreakdownOnlySections(sections: readonly string[] | null | undefined): boolean {
  return Boolean(
    sections && sections.length > 0 && sections.every((s) => Object.prototype.hasOwnProperty.call(CMCD_BREAKDOWN_SECTION_COLUMNS, s)),
  );
}

/** The columns a projected /api/cmcd/aggregates load needs, or null when the filters can't name theirs. */
export function cmcdScanColumns(
  filters: unknown,
  sections?: unknown,
  cmcdFilters?: Pick<CmcdQueryParams, "filter_sf" | "filter_ot" | "filter_st" | "filter_cid">,
): string[] | null {
  const sectionList = Array.isArray(sections)
    ? sections.filter((s): s is string => typeof s === "string")
    : undefined;
  let base: string[];
  if (isBreakdownOnlySections(sectionList)) {
    const cols = new Set<string>(CMCD_WHERE_BASE_COLUMNS);
    for (const s of sectionList!) {
      for (const col of CMCD_BREAKDOWN_SECTION_COLUMNS[s]) cols.add(col);
    }
    if (cmcdFilters?.filter_sf) cols.add("cmcd_sf");
    if (cmcdFilters?.filter_ot) cols.add("cmcd_ot");
    if (cmcdFilters?.filter_st) cols.add("cmcd_st");
    if (cmcdFilters?.filter_cid) cols.add("cmcd_cid");
    base = [...cols];
  } else {
    base = [...CMCD_SCAN_COLUMNS];
  }
  if (filters == null || filters === "") return base;
  if (typeof filters !== "object" || Array.isArray(filters)) return null;
  return [...new Set([...base, ...Object.keys(filters).filter((key) => key !== "filter_mode")])];
}

async function resolveCmcdServiceEnabled(
  serviceId: string | undefined,
  signal: AbortSignal,
): Promise<{ serviceId: string; enabled: boolean }> {
  const activeIds = await cloudActiveLogFieldIds(serviceId, signal);
  if (activeIds.has("cmcd_sid")) {
    return { serviceId: serviceId ?? "default", enabled: true };
  }
  if (serviceId && serviceId !== "default") {
    const response = await fetchCloudMetadata("/api/services", signal).catch(() => null);
    if (response?.ok) {
      const { services = [] } = (await response.json().catch(() => ({}))) as {
        services?: Array<{
          service_id?: string;
          cmcd?: { enabled?: boolean };
        }>;
      };
      const service = services.find((item) => item.service_id === serviceId);
      if (service) {
        return {
          serviceId: service.service_id ?? serviceId,
          enabled: Boolean(service.cmcd?.enabled),
        };
      }
    }
  }
  return { serviceId: serviceId ?? "default", enabled: false };
}

export async function queryCmcdAggregates(params: CmcdQueryParams = {}) {
  const available =
    typeof params.available === "boolean"
      ? params.available
      : (await resolveCmcdServiceEnabled(params.service_id, params.signal ?? new AbortController().signal)).enabled;
  if (params.rollupSource && cmcdRollupEligible(params)) {
    const rollup = await cmcdFromRollup({ ...params, available }, params.rollupSource, params.signal);
    if (rollup) return rollup;
  }
  const sectionSet = params.sections?.length ? new Set(params.sections) : null;
  const breakdownOnly = isBreakdownOnlySections(params.sections);
  const asnNamesReady = !sectionSet || sectionSet.has("rebuffer_by_asn") ? loadAsnNames() : Promise.resolve();
  const engine = params.engine ?? getDuckDBEngine().readContext();
  params = { ...params, customColumnTypes: engine.getCustomColumnTypes?.() };
  const where = buildCmcdWhere(params);
  const bucketMs = Math.max(1, Math.min(14400, Math.floor(params.bucket_seconds ?? 300))) * 1000;
  const topN = Math.max(1, Math.min(100, Math.floor(params.top_n ?? 30)));
  const nullable = (value: unknown) => {
    if (value == null) return null;
    const n = Number(value);
    return Number.isFinite(n) ? roundTo(n, 2) : null;
  };

  const coreRows = breakdownOnly
    ? await engine.executeSql<Record<string, unknown>>(
      `SELECT
        7::INTEGER AS grp,
        COUNT(*)::BIGINT AS total_requests,
        COUNT(cmcd_bs) AS has_bs,
        COUNT(NULLIF(cmcd_ot, '')) AS has_ot,
        COUNT(cmcd_bl) AS has_bl,
        COUNT(cmcd_br) AS has_br,
        COUNT(NULLIF(cmcd_st, '')) AS has_st,
        COUNT(NULLIF(cmcd_cid, '')) AS has_cid
      FROM requests
      WHERE ${where}`,
      [],
      params.signal,
    )
    : await engine.executeSql<Record<string, unknown>>(
    `SELECT
      GROUPING(bucket_ms, ot_key, sf_key)::INTEGER AS grp,
      bucket_ms,
      ot_key,
      sf_key,
      COUNT(*)::BIGINT AS total_requests,
      COUNT(DISTINCT NULLIF(cmcd_sid, ''))::BIGINT AS active_sessions,
      (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
      COUNT(cmcd_bs) AS has_bs,
      COUNT(DISTINCT NULLIF(cmcd_sid, '')) FILTER (WHERE cmcd_bs IS TRUE) AS rebuffer_sessions,
      AVG(cmcd_br)::DOUBLE AS avg_bitrate_kbps,
      AVG(cmcd_tb)::DOUBLE AS avg_top_bitrate_kbps,
      AVG(cmcd_bl)::DOUBLE AS avg_buffer_ms,
      AVG(cmcd_mtp)::DOUBLE AS avg_throughput_kbps,
      COUNT(NULLIF(cmcd_ot, '')) AS has_ot,
      COUNT(cmcd_bl) AS has_bl,
      COUNT(cmcd_br) AS has_br,
      COUNT(cmcd_mtp) AS has_mtp,
      COUNT(NULLIF(cmcd_st, '')) AS has_st,
      COUNT(cmcd_su) AS has_su,
      COUNT(NULLIF(cmcd_cid, '')) AS has_cid,
      AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_avg_bitrate,
      AVG(cmcd_tb) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_avg_top_bitrate,
      AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_avg_buffer,
      AVG(cmcd_br / cmcd_tb) FILTER (WHERE cmcd_tb > 0)::DOUBLE AS all_utilization_ratio,
      AVG(cmcd_br / cmcd_tb) FILTER (WHERE cmcd_ot = 'v' AND cmcd_tb > 0)::DOUBLE AS video_utilization_ratio,
      QUANTILE_CONT(cmcd_bl, 0.1)::DOUBLE AS all_p10_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.5)::DOUBLE AS all_p50_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.95)::DOUBLE AS all_p95_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.1) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p10_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.5) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p50_buffer_ms,
      QUANTILE_CONT(cmcd_bl, 0.95) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p95_buffer_ms,
      QUANTILE_CONT(cmcd_mtp, 0.5)::DOUBLE AS all_p50_throughput,
      QUANTILE_CONT(cmcd_mtp, 0.95)::DOUBLE AS all_p95_throughput,
      QUANTILE_CONT(cmcd_mtp, 0.99)::DOUBLE AS all_p99_throughput,
      QUANTILE_CONT(cmcd_mtp, 0.5) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p50_throughput,
      QUANTILE_CONT(cmcd_mtp, 0.95) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p95_throughput,
      QUANTILE_CONT(cmcd_mtp, 0.99) FILTER (WHERE cmcd_ot = 'v')::DOUBLE AS video_p99_throughput,
      (COUNT(*) FILTER (WHERE cmcd_su IS TRUE))::BIGINT AS startup_requests,
      AVG(cmcd_bl) FILTER (WHERE cmcd_su IS TRUE)::DOUBLE AS avg_startup_buffer_ms,
      AVG(COALESCE(ttfb_ms, ttfb * 1000.0, time_elapsed_usec / 1000.0, elapsed / 1000.0)) FILTER (WHERE cmcd_su IS TRUE)::DOUBLE AS avg_startup_ttfb_ms
    FROM (
      SELECT
        (floor(ts_ms / ${bucketMs}) * ${bucketMs})::BIGINT AS bucket_ms,
        NULLIF(cmcd_ot, '') AS ot_key,
        NULLIF(cmcd_sf, '') AS sf_key,
        cmcd_sid, cmcd_cid, cmcd_br, cmcd_bl, cmcd_bs, cmcd_mtp, cmcd_ot, cmcd_st, cmcd_tb, cmcd_su,
        ttfb_ms, ttfb, time_elapsed_usec, elapsed
      FROM requests
      WHERE ${where}
    ) base
    GROUP BY GROUPING SETS ((), (bucket_ms), (ot_key), (sf_key))`,
    [],
    params.signal,
  );

  const s = coreRows.find((r) => Number(r.grp) === 7);
  const totalRequests = Number(s?.total_requests ?? 0);
  if (!s || totalRequests === 0) {
    return emptyCmcdAggregates(available);
  }

  const activeSessions = Number(s.active_sessions ?? 0);
  // Like High-Scale, quality measurements cover video segments once the window reports object types.
  const hasOt = Number(s.has_ot) > 0;
  const video = hasOt ? "cmcd_ot = 'v'" : "TRUE";
  // High-Scale omits a section when the window never reported the CMCD keys it needs.
  const has = (key: string) => Number(s[`has_${key}`]) > 0;
  // cmcd_bs is a sparse flag (1 only on starvation, NULL otherwise), so buffer-length telemetry
  // proves starvation tracking is active even in healthy windows with 0 starvations.
  const hasBsEffective = has("bs") || has("bl");
  const hasSu = has("su");
  const rebufferEvents = Number(s.rebuffer_events ?? 0);
  const rebufferRate =
    totalRequests > 0 ? roundTo((rebufferEvents * 100) / totalRequests, 2) : 0;
  const avgBitrate = Number(s.avg_bitrate_kbps ?? 0);
  const avgTopBitrate = Number(s.avg_top_bitrate_kbps ?? 0);
  const utilizationRatio =
    avgTopBitrate > 0 ? roundTo((avgBitrate * 100) / avgTopBitrate, 2) : 0;

  const bucketRows = coreRows
    .filter((r) => Number(r.grp) === 3)
    .sort((a, b) => Number(a.bucket_ms ?? 0) - Number(b.bucket_ms ?? 0));
  const otRows = coreRows
    .filter((r) => Number(r.grp) === 5 && r.ot_key != null)
    .sort((a, b) => {
      const diff = Number(b.total_requests ?? 0) - Number(a.total_requests ?? 0);
      if (diff !== 0) return diff;
      const ak = String(a.ot_key);
      const bk = String(b.ot_key);
      return ak < bk ? -1 : ak > bk ? 1 : 0;
    });
  const sfRows = coreRows
    .filter((r) => Number(r.grp) === 6 && r.sf_key != null)
    .sort((a, b) => {
      const diff = Number(b.active_sessions ?? 0) - Number(a.active_sessions ?? 0);
      if (diff !== 0) return diff;
      const ak = String(a.sf_key);
      const bk = String(b.sf_key);
      return ak < bk ? -1 : ak > bk ? 1 : 0;
    });

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
    const reqs = Number(b.total_requests ?? 0);
    const rebuf = Number(b.rebuffer_events ?? 0);
    const rawEncBr = hasOt ? b.video_avg_bitrate : b.avg_bitrate_kbps;
    const rawTopBr = hasOt ? b.video_avg_top_bitrate : b.avg_top_bitrate_kbps;
    const rawUtil = hasOt ? b.video_utilization_ratio : b.all_utilization_ratio;
    const p10Buf = hasOt ? b.video_p10_buffer_ms : b.all_p10_buffer_ms;
    const p50Buf = hasOt ? b.video_p50_buffer_ms : b.all_p50_buffer_ms;
    const p95Buf = hasOt ? b.video_p95_buffer_ms : b.all_p95_buffer_ms;
    const p50Mtp = hasOt ? b.video_p50_throughput : b.all_p50_throughput;
    const p95Mtp = hasOt ? b.video_p95_throughput : b.all_p95_throughput;
    const p99Mtp = hasOt ? b.video_p99_throughput : b.all_p99_throughput;
    const encBr = Number(rawEncBr ?? 0);
    const topBr = Number(rawTopBr ?? 0);
    const mtp = Number(b.avg_throughput_kbps ?? 0);

    sessions_ts.push({
      bucket: tsIso,
      concurrent_sessions: sess,
      new_sessions: 0,
      rebuffer_session_pct: hasBsEffective && sess > 0 ? roundTo(Number(b.rebuffer_sessions) * 100 / sess, 2) : null,
      timestamp: tsIso,
      active_sessions: sess,
      rebuffer_events: rebuf,
      rebuffer_rate: reqs > 0 ? roundTo((rebuf * 100) / reqs, 2) : 0,
    });
    buffer_health_ts.push({
      bucket: tsIso,
      p50_buffer: nullable(p50Buf),
      p95_buffer: nullable(p95Buf),
      starvation_rate: hasBsEffective && reqs > 0 ? roundTo(rebuf * 100 / reqs, 2) : null,
      timestamp: tsIso,
      avg_buffer_ms: roundTo(Number(b.avg_buffer_ms ?? 0), 1),
      p10_buffer_ms: roundTo(Number(p10Buf ?? 0), 1),
      p50_buffer_ms: roundTo(Number(p50Buf ?? 0), 1),
      rebuffer_events: rebuf,
    });
    bitrate_ts.push({
      bucket: tsIso,
      avg_bitrate: nullable(rawEncBr),
      timestamp: tsIso,
      avg_encoded_bitrate: roundTo(encBr, 1),
      avg_top_bitrate: roundTo(topBr, 1),
      utilization_ratio: rawUtil == null ? null : roundTo(Number(rawUtil), 2),
    });
    throughput_ts.push({
      bucket: tsIso,
      p50: nullable(p50Mtp),
      p95: nullable(p95Mtp),
      p99: nullable(p99Mtp),
      timestamp: tsIso,
      avg_throughput_kbps: roundTo(mtp, 1),
      avg_bitrate_kbps: roundTo(encBr, 1),
      headroom_ratio: encBr > 0 ? roundTo(mtp / encBr, 2) : 0,
    });
    startup_ts.push({
      bucket: tsIso,
      startup_ratio: hasSu && reqs > 0 ? roundTo(Number(b.startup_requests) * 100 / reqs, 2) : null,
      timestamp: tsIso,
      startup_requests: Number(b.startup_requests ?? 0),
      avg_startup_buffer_ms: roundTo(Number(b.avg_startup_buffer_ms ?? 0), 1),
      avg_startup_ttfb_ms: roundTo(Number(b.avg_startup_ttfb_ms ?? 0), 2),
    });
  }

  const rebufferRateSql = hasBsEffective
    ? "COUNT(DISTINCT NULLIF(cmcd_sid, '')) FILTER (WHERE cmcd_bs IS TRUE) * 100.0 / NULLIF(COUNT(DISTINCT NULLIF(cmcd_sid, '')), 0)"
    : "NULL";

  const wantSection = (name: string) => !sectionSet || sectionSet.has(name);
  const wantCid = wantSection("top_content") && has("cid");
  const wantGeo = (wantSection("rebuffer_by_country") || wantSection("rebuffer_by_asn")) && hasBsEffective;
  const wantLadder = wantSection("bitrate_ladder") && has("br");
  const wantSt = wantSection("stream_type_dist") && has("st");
  const wantSessionMetrics =
    wantSection("overview") || wantSection("sessions_ts") || wantSection("session_duration_dist");

  const axes: Array<{ id: "cid" | "country" | "asn" | "ladder" | "st"; expr: string; groupCols: string; having: string }> = [];
  if (wantCid) {
    axes.push({ id: "cid", expr: "content_id", groupCols: "(content_id)", having: "content_id IS NOT NULL" });
  }
  if (wantGeo) {
    axes.push({ id: "country", expr: "country_key", groupCols: "(country_key)", having: "country_key IS NOT NULL" });
    axes.push({ id: "asn", expr: "asn_key", groupCols: "(asn_key, asn_name_key)", having: "asn_key IS NOT NULL" });
  }
  if (wantLadder) {
    axes.push({ id: "ladder", expr: "br_tier", groupCols: "(br_tier)", having: "br_tier IS NOT NULL" });
  }
  if (wantSt) {
    axes.push({ id: "st", expr: "st_key", groupCols: "(st_key)", having: "st_key IS NOT NULL" });
  }

  const grpById: Partial<Record<"cid" | "country" | "asn" | "ladder" | "st", number>> = {};
  const breakdownSql = (() => {
    if (axes.length === 0) return null;
    const n = axes.length;
    const allMask = (1 << n) - 1;
    const groupingCols = axes.map((a) => a.expr).join(", ");
    const grpExpr = n === 1 ? "0::INTEGER" : `GROUPING(${groupingCols})::INTEGER`;
    const havingParts: string[] = [];
    axes.forEach((a, idx) => {
      const grpVal = n === 1 ? 0 : allMask ^ (1 << (n - 1 - idx));
      grpById[a.id] = grpVal;
      havingParts.push(n === 1 ? a.having : `(GROUPING(${groupingCols}) = ${grpVal} AND ${a.having})`);
    });
    const segRebufferRateSql = hasBsEffective
      ? "COUNT(*) FILTER (WHERE cmcd_bs IS TRUE) * 100.0 / NULLIF(COUNT(*), 0)"
      : "NULL::DOUBLE";
    const tieBreakCols = [
      ...(wantLadder ? ["br_tier ASC NULLS LAST"] : []),
      ...(wantGeo ? ["asn_key ASC NULLS LAST"] : []),
      ...(wantCid ? ["content_id ASC NULLS LAST"] : []),
      ...(wantGeo ? ["country_key ASC NULLS LAST"] : []),
      ...(wantSt ? ["st_key ASC NULLS LAST"] : []),
    ];
    const ladderGrp = grpById.ladder;
    const stGrp = grpById.st;
    const unlimitedGrps = [ladderGrp, stGrp].filter((v): v is number => v !== undefined);
    const qualifyLimit = unlimitedGrps.length > 0
      ? `row_number() OVER (PARTITION BY ${grpExpr} ORDER BY COUNT(DISTINCT NULLIF(cmcd_sid, '')) DESC, ${tieBreakCols.join(", ")}) <= ${topN} OR grp IN (${unlimitedGrps.join(", ")})`
      : `row_number() OVER (PARTITION BY ${grpExpr} ORDER BY COUNT(DISTINCT NULLIF(cmcd_sid, '')) DESC, ${tieBreakCols.join(", ")}) <= ${topN}`;
    return `SELECT
      ${grpExpr} AS grp,
      ${wantCid ? "content_id" : "NULL::VARCHAR AS content_id"},
      ${wantGeo ? "country_key, asn_key, asn_name_key" : "NULL::VARCHAR AS country_key, NULL::BIGINT AS asn_key, NULL::VARCHAR AS asn_name_key"},
      ${wantLadder ? "br_tier" : "NULL::INTEGER AS br_tier"},
      ${wantSt ? "st_key" : "NULL::VARCHAR AS st_key"},
      COUNT(DISTINCT NULLIF(cmcd_sid, ''))::BIGINT AS sessions,
      COUNT(*)::BIGINT AS requests,
      (COUNT(*) FILTER (WHERE cmcd_bs IS TRUE))::BIGINT AS rebuffer_events,
      AVG(cmcd_br) FILTER (WHERE ${video})::DOUBLE AS avg_bitrate,
      AVG(cmcd_bl) FILTER (WHERE ${video})::DOUBLE AS avg_buffer_length,
      ${wantGeo ? `QUANTILE_CONT(cmcd_mtp, 0.5) FILTER (WHERE ${video})::DOUBLE` : "NULL::DOUBLE"} AS median_throughput,
      ${rebufferRateSql} AS rebuffer_rate,
      ${segRebufferRateSql} AS seg_rebuffer_rate,
      ${wantGeo ? "AVG(cmcd_mtp)::DOUBLE" : "NULL::DOUBLE"} AS avg_throughput_kbps
    FROM (
      SELECT
        ${wantCid ? "NULLIF(cmcd_cid, '')" : "NULL::VARCHAR"} AS content_id,
        ${wantGeo ? "COALESCE(NULLIF(geo_country, ''), NULLIF(country, ''))" : "NULL::VARCHAR"} AS country_key,
        ${wantGeo ? "COALESCE(client_asn, asn)::BIGINT" : "NULL::BIGINT"} AS asn_key,
        ${wantGeo ? "COALESCE(NULLIF(as_name, ''), 'AS' || CAST(COALESCE(client_asn, asn) AS VARCHAR))" : "NULL::VARCHAR"} AS asn_name_key,
        CASE
          WHEN (${video}) AND cmcd_br IS NOT NULL AND cmcd_br > 0 THEN
            CASE
              WHEN cmcd_br < 1000 THEN 1
              WHEN cmcd_br < 2500 THEN 2
              WHEN cmcd_br < 5000 THEN 3
              WHEN cmcd_br < 10000 THEN 4
              ELSE 5
            END
          ELSE NULL
        END AS br_tier,
        ${wantSt ? "NULLIF(cmcd_st, '')" : "NULL::VARCHAR"} AS st_key,
        cmcd_sid, cmcd_bs, cmcd_br, cmcd_bl, ${wantGeo ? "cmcd_mtp" : "NULL::DOUBLE AS cmcd_mtp"}, cmcd_ot
      FROM requests
      WHERE ${where}
    ) base
    GROUP BY GROUPING SETS (${axes.map((a) => a.groupCols).join(", ")})
    HAVING ${havingParts.join(" OR ")}
    QUALIFY ${qualifyLimit}
    ORDER BY grp, sessions DESC, ${tieBreakCols.join(", ")}`;
  })();

  const [sessionAggRows, breakdownRows] = await Promise.all([
    !wantSessionMetrics
      ? Promise.resolve([] as Record<string, unknown>[])
      : engine.executeSql<Record<string, unknown>>(
      `WITH sessions AS (
        SELECT
          cmcd_sid AS sid,
          (floor(MIN(ts_ms) / ${bucketMs}) * ${bucketMs})::BIGINT AS first_bucket_ms,
          (MAX(ts_ms) - MIN(ts_ms)) / 1000.0 AS duration
        FROM requests
        WHERE ${where} AND cmcd_sid IS NOT NULL AND cmcd_sid != ''
        GROUP BY 1
      ),
      classified AS (
        SELECT
          first_bucket_ms,
          duration,
          CASE
            WHEN duration < 30 THEN '<30s'
            WHEN duration < 120 THEN '30s–2m'
            WHEN duration < 300 THEN '2–5m'
            WHEN duration < 600 THEN '5–10m'
            WHEN duration < 1800 THEN '10–30m'
            WHEN duration < 3600 THEN '30m–1h'
            ELSE '>1h'
          END AS duration_bucket
        FROM sessions
      )
      SELECT
        GROUPING(first_bucket_ms, duration_bucket)::INTEGER AS grp,
        first_bucket_ms,
        duration_bucket,
        COUNT(*)::BIGINT AS session_count,
        SUM(duration)::DOUBLE AS total_duration_sec,
        (COUNT(*) FILTER (WHERE duration > 0))::BIGINT AS positive_count,
        SUM(duration) FILTER (WHERE duration > 0)::DOUBLE AS positive_duration_sec
      FROM classified
      GROUP BY GROUPING SETS ((), (first_bucket_ms), (duration_bucket))`,
      [],
      params.signal,
    ),
    breakdownSql == null
      ? Promise.resolve([] as Record<string, unknown>[])
      : engine.executeSql<Record<string, unknown>>(breakdownSql, [], params.signal),
  ]);
  await asnNamesReady;

  const sessionSummary = sessionAggRows.find((r) => Number(r.grp) === 3);
  const totalSessions = Number(sessionSummary?.session_count ?? 0);
  const totalDurationSec = Number(sessionSummary?.total_duration_sec ?? 0);
  const positiveCount = Number(sessionSummary?.positive_count ?? 0);
  const positiveDurationSec = Number(sessionSummary?.positive_duration_sec ?? 0);
  const avgSessionDuration =
    totalSessions > 0 ? roundTo(totalDurationSec / totalSessions, 1) : 0;

  const newSessionsByBucket = new Map<string, number>();
  const durationBucketCounts = new Map<string, number>();
  for (const row of sessionAggRows) {
    const grp = Number(row.grp);
    if (grp === 1 && row.first_bucket_ms != null) {
      newSessionsByBucket.set(
        new Date(Number(row.first_bucket_ms)).toISOString(),
        Number(row.session_count ?? 0),
      );
    } else if (grp === 2 && row.duration_bucket != null) {
      durationBucketCounts.set(String(row.duration_bucket), Number(row.session_count ?? 0));
    }
  }
  for (const point of sessions_ts) {
    point.new_sessions = newSessionsByBucket.get(String(point.bucket)) ?? 0;
  }
  const session_duration_dist = CMCD_SESSION_DURATION_BUCKETS.filter((bucket) => durationBucketCounts.get(bucket)).map((bucket) => {
    const count = durationBucketCounts.get(bucket) ?? 0;
    return {
      bucket,
      duration_bucket: bucket,
      session_count: count,
      sessions: count,
      pct:
        totalSessions > 0
          ? roundTo((count * 100) / totalSessions, 2)
          : 0,
    };
  });

  const topContentRows = grpById.cid === undefined ? [] : breakdownRows.filter((r) => Number(r.grp) === grpById.cid);
  const countryRows = grpById.country === undefined ? [] : breakdownRows.filter((r) => Number(r.grp) === grpById.country);
  const asnRows = grpById.asn === undefined ? [] : breakdownRows.filter((r) => Number(r.grp) === grpById.asn);
  const ladderRows = grpById.ladder === undefined
    ? []
    : breakdownRows
        .filter((r) => Number(r.grp) === grpById.ladder && r.br_tier != null)
        .sort((a, b) => Number(a.br_tier) - Number(b.br_tier));
  const stRows = grpById.st === undefined
    ? []
    : breakdownRows
        .filter((r) => Number(r.grp) === grpById.st && r.st_key != null)
        .sort((a, b) => {
          const diff = Number(b.sessions ?? 0) - Number(a.sessions ?? 0);
          if (diff !== 0) return diff;
          const ak = String(a.st_key);
          const bk = String(b.st_key);
          return ak < bk ? -1 : ak > bk ? 1 : 0;
        });

  return {
    available,
    has_data: true,
    overview: {
      active_sessions: activeSessions,
      peak_viewers: peakViewers,
      avg_session_duration: positiveCount > 0 ? Math.round(positiveDurationSec / positiveCount) : null,
      rebuffer_rate: hasBsEffective && activeSessions ? roundTo(Number(s.rebuffer_sessions) * 100 / activeSessions, 2) : null,
      avg_bitrate: nullable(hasOt ? s.video_avg_bitrate : s.avg_bitrate_kbps),
      avg_buffer_length: nullable(hasOt ? s.video_avg_buffer : s.avg_buffer_ms),
      median_throughput: nullable(hasOt ? s.video_p50_throughput : s.all_p50_throughput),
    },
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
    ...padCmcdTimeseries(
      {
        sessions_ts,
        buffer_health_ts: has("bl") ? buffer_health_ts : [],
        bitrate_ts: has("br") ? bitrate_ts : [],
        throughput_ts: has("mtp") ? throughput_ts : [],
        startup_ts: hasSu ? startup_ts : [],
      },
      params.start_time,
      params.end_time,
      bucketMs,
    ),
    top_content: !wantCid ? [] : topContentRows.map((r) => ({
      content_id: String(r.content_id ?? "Unknown"),
      sessions: Number(r.sessions ?? 0),
      session_count: Number(r.sessions ?? 0),
      requests: Number(r.requests ?? 0),
      avg_bitrate_kbps: roundTo(Number(r.avg_bitrate ?? 0), 1),
      avg_bitrate: nullable(r.avg_bitrate),
      avg_buffer_length: nullable(r.avg_buffer_length),
      rebuffer_rate: nullable(r.rebuffer_rate),
    })),
    rebuffer_by_country: !wantGeo ? [] : countryRows.map((r) => ({
      country: String(r.country_key ?? "Unknown"),
      session_count: Number(r.sessions ?? 0),
      median_throughput: nullable(r.median_throughput),
      sessions: Number(r.sessions ?? 0),
      requests: Number(r.requests ?? 0),
      rebuffer_events: Number(r.rebuffer_events ?? 0),
      rebuffer_rate: nullable(r.rebuffer_rate),
    })),
    rebuffer_by_asn: !wantGeo ? [] : asnRows.map((r) => {
      const asn = Number(r.asn_key ?? 0);
      const rawName = String(r.asn_name_key ?? "").trim();
      const label = formatAsnDisplayLabel(asn, rawName);
      return {
        asn,
        as_name: rawName && !/^AS\d+$/i.test(rawName) ? rawName : label,
        label,
        session_count: Number(r.sessions ?? 0),
        avg_bitrate: nullable(r.avg_bitrate),
        avg_buffer_length: nullable(r.avg_buffer_length),
        sessions: Number(r.sessions ?? 0),
        requests: Number(r.requests ?? 0),
        rebuffer_events: Number(r.rebuffer_events ?? 0),
        rebuffer_rate: nullable(r.rebuffer_rate),
        avg_throughput_kbps: roundTo(Number(r.avg_throughput_kbps ?? 0), 1),
      };
    }),
    object_type_dist: otRows.map((r) => ({
      object_type: String(r.ot_key ?? "unknown"),
      request_count: Number(r.total_requests ?? 0),
      requests: Number(r.total_requests ?? 0),
      pct:
        totalRequests > 0
          ? roundTo((Number(r.total_requests ?? 0) * 100) / totalRequests, 2)
          : 0,
    })),
    streaming_format_dist: sfRows.map((r) => ({
      format: String(r.sf_key ?? "unknown"),
      streaming_format: String(r.sf_key ?? "unknown"),
      session_count: Number(r.active_sessions ?? 0),
      requests: Number(r.total_requests ?? 0),
      pct:
        totalRequests > 0
          ? roundTo((Number(r.total_requests ?? 0) * 100) / totalRequests, 2)
          : 0,
    })),
    session_duration_dist,
    bitrate_ladder: !wantLadder
      ? []
      : ladderRows
          .filter((r) => Number(r.br_tier) in BITRATE_LADDER_TIERS)
          .map((r) => ({
            tier: BITRATE_LADDER_TIERS[Number(r.br_tier)],
            request_count: Number(r.requests ?? 0),
            session_count: Number(r.sessions ?? 0),
            avg_bitrate: r.avg_bitrate == null ? null : roundTo(Number(r.avg_bitrate), 2),
            rebuffer_rate: hasBsEffective && r.seg_rebuffer_rate != null ? roundTo(Number(r.seg_rebuffer_rate), 2) : null,
          })),
    stream_type_dist: !wantSt
      ? []
      : stRows.map((r) => ({
          stream_type: String(r.st_key ?? "unknown"),
          session_count: Number(r.sessions ?? 0),
          request_count: Number(r.requests ?? 0),
        })),
  };
}

export async function handleCmcdRequest(
  path: string,
  params: CmcdQueryParams = {},
) {
  if (path.includes("/api/cmcd/aggregates")) {
    return queryCmcdAggregates(params);
  }
  if (path.includes("/cmcd/enable") || path.includes("/cmcd/disable")) {
    return { status: "complete", service_id: params.service_id ?? "default" };
  }
  return queryCmcdAggregates(params);
}
