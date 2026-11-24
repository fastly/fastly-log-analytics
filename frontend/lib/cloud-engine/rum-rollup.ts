import { parquetMetadata } from "hyparquet";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";
import { fetchManifest, type ManifestFile } from "./dashboard/manifest";
import { readColumns } from "./dashboard/parquet";
import type { queryRollup } from "./dashboard/rollup-query";

export type RumRollupSource = Parameters<typeof queryRollup>[0];
import { parseEpochMs, roundTo } from "./engine-utils";
import { parseParquetPartitionWindow } from "./parquet-key";
import { mergeNullableHistogram, mergeNullableHll, mergeNumberMap } from "./rollup-merge";
import {
  emptyRumAnalytics,
  shapeRumEnvironments,
  type CwvMetricName,
  type RumQueryParams,
} from "./rum";

const RUM_SUMMARY_VERSION = "1";

const TEXT_COLUMNS = [
  "kind",
  "bucket",
  "key",
  "count",
  "good",
  "poor",
  "min_line",
  "min_col",
  "last_ts",
  "truncated",
] as const;

const BINARY_COLUMNS = [
  "value_hist",
  "lcp_hist",
  "cls_hist",
  "inp_hist",
  "cid_hll",
  "msg_hll",
] as const;

export type RumMetricAgg = {
  count: number;
  good: number;
  poor: number;
  hist: Histogram | null;
};

export type RumPageAgg = {
  count: number;
  lcp: Histogram | null;
  cls: Histogram | null;
  inp: Histogram | null;
};

export type RumErrorAgg = {
  message: string;
  source: string;
  count: number;
  minLine: number | null;
  minCol: number | null;
  cidHll: Hll | null;
  lastTsMs: number | null;
};

export type RumSummary = {
  vitalsCount: number;
  vitalsCidHll: Hll | null;
  errorsCount: number;
  errorsCidHll: Hll | null;
  errorsMsgHll: Hll | null;
  metrics: Map<string, RumMetricAgg>;
  pages: Map<string, RumPageAgg>;
  pagesTruncated: boolean;
  envs: {
    browser: Map<string, number>;
    os: Map<string, number>;
    device: Map<string, number>;
  };
  envsTruncated: boolean;
  errors: Map<string, RumErrorAgg>;
  errorsTruncated: boolean;
};

function num(value: unknown): number {
  return value == null ? 0 : Number(value);
}

function optNum(value: unknown): number | null {
  return value == null ? null : Number(value);
}

function tsMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

const mergeHist = mergeNullableHistogram;
const mergeHll = mergeNullableHll;

function readHist(col: ArrayLike<unknown> | undefined, i: number): Histogram | null {
  const blob = col?.[i];
  return blob instanceof Uint8Array ? Histogram.fromBytes(blob) : null;
}

function readHll(col: ArrayLike<unknown> | undefined, i: number): Hll | null {
  const blob = col?.[i];
  return blob instanceof Uint8Array ? Hll.fromBytes(blob) : null;
}

export function emptyRumSummary(): RumSummary {
  return {
    vitalsCount: 0,
    vitalsCidHll: null,
    errorsCount: 0,
    errorsCidHll: null,
    errorsMsgHll: null,
    metrics: new Map(),
    pages: new Map(),
    pagesTruncated: false,
    envs: {
      browser: new Map(),
      os: new Map(),
      device: new Map(),
    },
    envsTruncated: false,
    errors: new Map(),
    errorsTruncated: false,
  };
}

export async function decodeRumSummary(buffer: ArrayBuffer): Promise<RumSummary | null> {
  const metadata = parquetMetadata(buffer);
  const version = metadata.key_value_metadata?.find(
    (entry) => entry.key === "flp_rum_summary",
  )?.value;
  if (version !== RUM_SUMMARY_VERSION) return null;

  const [text, bin] = await Promise.all([
    readColumns(buffer, TEXT_COLUMNS, true),
    readColumns(buffer, BINARY_COLUMNS, false),
  ]);

  const out = emptyRumSummary();
  const kinds = text.columns.get("kind");
  const keys = text.columns.get("key");
  const counts = text.columns.get("count");
  const goods = text.columns.get("good");
  const poors = text.columns.get("poor");
  const minLines = text.columns.get("min_line");
  const minCols = text.columns.get("min_col");
  const lastTsCol = text.columns.get("last_ts");
  const truncatedCol = text.columns.get("truncated");

  const valueHists = bin.columns.get("value_hist");
  const lcpHists = bin.columns.get("lcp_hist");
  const clsHists = bin.columns.get("cls_hist");
  const inpHists = bin.columns.get("inp_hist");
  const cidHlls = bin.columns.get("cid_hll");
  const msgHlls = bin.columns.get("msg_hll");

  for (let i = 0; i < text.rows; i++) {
    const kind = String(kinds?.[i] ?? "");
    switch (kind) {
      case "metric": {
        const name = String(keys?.[i] ?? "");
        const prev = out.metrics.get(name);
        const hist = readHist(valueHists, i);
        if (prev) {
          prev.count += num(counts?.[i]);
          prev.good += num(goods?.[i]);
          prev.poor += num(poors?.[i]);
          prev.hist = mergeHist(prev.hist, hist);
        } else {
          out.metrics.set(name, {
            count: num(counts?.[i]),
            good: num(goods?.[i]),
            poor: num(poors?.[i]),
            hist,
          });
        }
        break;
      }
      case "vitals_total": {
        out.vitalsCount += num(counts?.[i]);
        out.vitalsCidHll = mergeHll(out.vitalsCidHll, readHll(cidHlls, i));
        break;
      }
      case "page": {
        const path = String(keys?.[i] ?? "/");
        if (truncatedCol?.[i] === true) out.pagesTruncated = true;
        const prev = out.pages.get(path);
        const lcp = readHist(lcpHists, i);
        const cls = readHist(clsHists, i);
        const inp = readHist(inpHists, i);
        if (prev) {
          prev.count += num(counts?.[i]);
          prev.lcp = mergeHist(prev.lcp, lcp);
          prev.cls = mergeHist(prev.cls, cls);
          prev.inp = mergeHist(prev.inp, inp);
        } else {
          out.pages.set(path, { count: num(counts?.[i]), lcp, cls, inp });
        }
        break;
      }
      case "env": {
        if (truncatedCol?.[i] === true) out.envsTruncated = true;
        const [dim, val] = JSON.parse(String(keys?.[i] ?? "[]")) as [string, string];
        if (dim === "browser" || dim === "os" || dim === "device") {
          const map = out.envs[dim];
          map.set(val, (map.get(val) ?? 0) + num(counts?.[i]));
        }
        break;
      }
      case "errors_total": {
        out.errorsCount += num(counts?.[i]);
        out.errorsCidHll = mergeHll(out.errorsCidHll, readHll(cidHlls, i));
        out.errorsMsgHll = mergeHll(out.errorsMsgHll, readHll(msgHlls, i));
        break;
      }
      case "error": {
        if (truncatedCol?.[i] === true) out.errorsTruncated = true;
        const rawKey = String(keys?.[i] ?? "[]");
        const [message, source] = JSON.parse(rawKey) as [string, string];
        const prev = out.errors.get(rawKey);
        const line = optNum(minLines?.[i]);
        const col = optNum(minCols?.[i]);
        const last = tsMs(lastTsCol?.[i]);
        const hll = readHll(cidHlls, i);
        if (prev) {
          prev.count += num(counts?.[i]);
          if (line !== null) prev.minLine = prev.minLine === null ? line : Math.min(prev.minLine, line);
          if (col !== null) prev.minCol = prev.minCol === null ? col : Math.min(prev.minCol, col);
          if (last !== null) prev.lastTsMs = prev.lastTsMs === null ? last : Math.max(prev.lastTsMs, last);
          prev.cidHll = mergeHll(prev.cidHll, hll);
        } else {
          out.errors.set(rawKey, {
            message,
            source,
            count: num(counts?.[i]),
            minLine: line,
            minCol: col,
            cidHll: hll,
            lastTsMs: last,
          });
        }
        break;
      }
    }
  }
  return out;
}

export function mergeRumSummaries(parts: readonly RumSummary[]): RumSummary {
  const out = emptyRumSummary();
  for (const part of parts) {
    out.vitalsCount += part.vitalsCount;
    out.vitalsCidHll = mergeHll(out.vitalsCidHll, part.vitalsCidHll);
    out.errorsCount += part.errorsCount;
    out.errorsCidHll = mergeHll(out.errorsCidHll, part.errorsCidHll);
    out.errorsMsgHll = mergeHll(out.errorsMsgHll, part.errorsMsgHll);
    out.pagesTruncated ||= part.pagesTruncated;
    out.envsTruncated ||= part.envsTruncated;
    out.errorsTruncated ||= part.errorsTruncated;

    for (const [name, agg] of part.metrics) {
      const prev = out.metrics.get(name);
      if (prev) {
        prev.count += agg.count;
        prev.good += agg.good;
        prev.poor += agg.poor;
        prev.hist = mergeHist(prev.hist, agg.hist);
      } else {
        out.metrics.set(name, {
          count: agg.count,
          good: agg.good,
          poor: agg.poor,
          hist: mergeHist(null, agg.hist),
        });
      }
    }

    for (const [path, page] of part.pages) {
      const prev = out.pages.get(path);
      if (prev) {
        prev.count += page.count;
        prev.lcp = mergeHist(prev.lcp, page.lcp);
        prev.cls = mergeHist(prev.cls, page.cls);
        prev.inp = mergeHist(prev.inp, page.inp);
      } else {
        out.pages.set(path, {
          count: page.count,
          lcp: mergeHist(null, page.lcp),
          cls: mergeHist(null, page.cls),
          inp: mergeHist(null, page.inp),
        });
      }
    }

    for (const dim of ["browser", "os", "device"] as const) {
      mergeNumberMap(out.envs[dim], part.envs[dim]);
    }

    for (const [key, err] of part.errors) {
      const prev = out.errors.get(key);
      if (prev) {
        prev.count += err.count;
        if (err.minLine !== null) {
          prev.minLine = prev.minLine === null ? err.minLine : Math.min(prev.minLine, err.minLine);
        }
        if (err.minCol !== null) {
          prev.minCol = prev.minCol === null ? err.minCol : Math.min(prev.minCol, err.minCol);
        }
        if (err.lastTsMs !== null) {
          prev.lastTsMs = prev.lastTsMs === null ? err.lastTsMs : Math.max(prev.lastTsMs, err.lastTsMs);
        }
        prev.cidHll = mergeHll(prev.cidHll, err.cidHll);
      } else {
        out.errors.set(key, {
          ...err,
          cidHll: mergeHll(null, err.cidHll),
        });
      }
    }
  }
  return out;
}

export function rumAnalyticsFromSummary(summary: RumSummary) {
  if (summary.vitalsCount === 0 && summary.errorsCount === 0) {
    return emptyRumAnalytics();
  }

  const base = emptyRumAnalytics();
  const metrics = base.metrics;
  let totalGoodEvents = 0;

  for (const mName of ["LCP", "CLS", "INP", "FCP", "TTFB"] as const satisfies readonly CwvMetricName[]) {
    const agg = summary.metrics.get(mName);
    if (!agg) continue;
    const cnt = agg.count;
    const goodCnt = agg.good;
    const poorCnt = agg.poor;
    const niCnt = Math.max(0, cnt - goodCnt - poorCnt);
    const scale = mName === "CLS" ? 1_000_000 : 1_000;
    const dp = mName === "CLS" ? 3 : 1;
    totalGoodEvents += goodCnt;
    metrics[mName] = {
      p50: roundTo((agg.hist?.quantile(0.5) ?? 0) / scale, dp),
      p75: roundTo((agg.hist?.quantile(0.75) ?? 0) / scale, dp),
      p95: roundTo((agg.hist?.quantile(0.95) ?? 0) / scale, dp),
      good_pct: cnt > 0 ? roundTo((goodCnt * 100) / cnt, 2) : 0,
      needs_improvement_pct: cnt > 0 ? roundTo((niCnt * 100) / cnt, 2) : 0,
      poor_pct: cnt > 0 ? roundTo((poorCnt * 100) / cnt, 2) : 0,
      count: cnt,
    };
  }

  const worst_pages = [...summary.pages.entries()]
    .filter(([, p]) => p.count >= 3)
    .map(([page_url, p]) => ({
      page_url,
      sample_count: p.count,
      lcpRaw: p.lcp?.quantile(0.75) ?? null,
      clsRaw: p.cls?.quantile(0.75) ?? null,
      inpRaw: p.inp?.quantile(0.75) ?? null,
    }))
    .sort((a, b) => {
      if (a.lcpRaw === null && b.lcpRaw !== null) return 1;
      if (a.lcpRaw !== null && b.lcpRaw === null) return -1;
      if (a.lcpRaw !== null && b.lcpRaw !== null && b.lcpRaw !== a.lcpRaw) {
        return b.lcpRaw - a.lcpRaw;
      }
      return b.sample_count - a.sample_count;
    })
    .slice(0, 20)
    .map((r) => ({
      page_url: r.page_url,
      sample_count: r.sample_count,
      lcp_p75: roundTo((r.lcpRaw ?? 0) / 1_000, 1),
      cls_p75: roundTo((r.clsRaw ?? 0) / 1_000_000, 3),
      inp_p75: roundTo((r.inpRaw ?? 0) / 1_000, 1),
    }));

  const topEnv = (map: Map<string, number>) =>
    [...map.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 20)
      .map(([name, count]) => ({ name, count }));

  const environments = shapeRumEnvironments({
    browsers: topEnv(summary.envs.browser),
    os: topEnv(summary.envs.os),
    devices: topEnv(summary.envs.device),
    connections: summary.vitalsCount > 0 ? [{ name: "Unknown", count: summary.vitalsCount }] : [],
  });

  const top_errors = [...summary.errors.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 25)
    .map((e) => ({
      message: e.message,
      source: e.source,
      lineno: e.minLine,
      colno: e.minCol,
      count: e.count,
      affected_sessions: e.cidHll?.count() ?? 0,
      last_seen: e.lastTsMs ? new Date(e.lastTsMs).toISOString() : null,
    }));

  const rawSessions = summary.vitalsCidHll?.count() ?? 0;
  const uniqueSessions = Math.max(1, rawSessions);

  return {
    no_data: false,
    summary: {
      total_pageviews: Math.max(uniqueSessions, metrics.LCP.count),
      unique_sessions: rawSessions,
      total_vitals_events: summary.vitalsCount,
      total_errors: summary.errorsCount,
      error_rate_pct:
        summary.vitalsCount > 0 ? roundTo((summary.errorsCount * 100) / summary.vitalsCount, 2) : 0,
      overall_cwv_good_pct:
        summary.vitalsCount > 0 ? roundTo((totalGoodEvents * 100) / summary.vitalsCount, 2) : 0,
    },
    metrics,
    timeseries: [] as Record<string, unknown>[],
    worst_pages,
    environments,
    error_summary: {
      total_errors: summary.errorsCount,
      affected_sessions: summary.errorsCidHll?.count() ?? 0,
      unique_error_messages: summary.errorsMsgHll?.count() ?? 0,
    },
    top_errors,
    geo_breakdown: [] as Record<string, unknown>[],
    navigation_types: [] as Record<string, unknown>[],
    attributions: [] as Record<string, unknown>[],
  };
}

export function rumRollupEligible(params: RumQueryParams): boolean {
  if (params.device_type || params.country || params.page_url) return false;
  if (params.filters === undefined || params.filters === null) return true;
  let filters: unknown;
  try {
    filters = typeof params.filters === "string" ? JSON.parse(params.filters) : params.filters;
  } catch {
    return false;
  }
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) return false;
  for (const spec of Object.values(filters as Record<string, { values?: unknown[] }>)) {
    if (spec && Array.isArray(spec.values) && spec.values.length > 0) return false;
  }
  return true;
}

function fileWindow(file: ManifestFile): [number, number] | null {
  const parsed = parseParquetPartitionWindow(file.key);
  if (parsed) return parsed;
  const start = Date.parse(file.start);
  const end = Date.parse(file.end);
  return Number.isFinite(start) && Number.isFinite(end) && end >= start ? [start, end] : null;
}

export async function queryRumFromRollup(
  source: RumRollupSource,
  params: RumQueryParams,
): Promise<ReturnType<typeof emptyRumAnalytics> | null> {
  if (!rumRollupEligible(params)) return null;
  const fromMs = parseEpochMs(params.start_time);
  const toMs = parseEpochMs(params.end_time);
  if (fromMs === null || toMs === null || !(toMs > fromMs)) return null;

  const from = new Date(fromMs).toISOString();
  const to = new Date(toMs).toISOString();
  const [raw, summaries] = await Promise.all([
    fetchManifest(source.fetchJson, from, to, params.signal, "rum", "raw"),
    fetchManifest(source.fetchJson, from, to, params.signal, "rum", "summary"),
  ]);
  if (!raw.complete || !summaries.complete) return null;

  const byWindow = new Map<string, ManifestFile>();
  for (const file of summaries.files) {
    const win = fileWindow(file);
    if (!win) return null;
    if (win[0] < fromMs || win[1] > toMs) return null;
    byWindow.set(`${win[0]}:${win[1]}`, file);
  }

  for (const file of raw.files) {
    const win = fileWindow(file);
    if (!win) return null;
    if (!byWindow.has(`${win[0]}:${win[1]}`)) return null;
  }

  if (byWindow.size === 0) return emptyRumAnalytics();

  let decoded: (RumSummary | null)[];
  try {
    decoded = await Promise.all(
      [...byWindow.values()].map(async (file) => {
        const { bytes } = await source.fetchFile(file.key, params.signal);
        return decodeRumSummary(bytes);
      }),
    );
  } catch (error) {
    params.signal?.throwIfAborted();
    // Each RUM summary is the only one for its window, so raw Parquet answers a lost one.
    if (!(error instanceof MissingCloudFileError)) throw error;
    console.warn(`flp: RUM summary ${error.key} is listed but missing; reading raw Parquet`);
    return null;
  }
  if (decoded.some((part) => part === null)) return null;

  return rumAnalyticsFromSummary(mergeRumSummaries(decoded as RumSummary[]));
}
