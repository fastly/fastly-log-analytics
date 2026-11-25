import { parquetMetadata } from "hyparquet";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";
import { fetchManifest, type Manifest, type ManifestFile } from "./dashboard/manifest";
import { readColumns } from "./dashboard/parquet";
import type { queryRollup } from "./dashboard/rollup-query";
import { mapLimit } from "./dashboard/sources";
import { fetchLanes } from "./fetch-lanes";

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
const RUM_FETCH_CONCURRENCY = 16;
const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
const MAX_PADDED_TREND_BUCKETS = 2000;

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

export type RumBucketAgg = {
  vitalsCount: number;
  vitalsCidHll: Hll | null;
  errorsCount: number;
  errorsCidHll: Hll | null;
  metrics: Map<string, RumMetricAgg>;
};

export type RumSummary = {
  vitalsCount: number;
  vitalsCidHll: Hll | null;
  errorsCount: number;
  errorsCidHll: Hll | null;
  errorsMsgHll: Hll | null;
  sampled: boolean;
  buckets: Map<number, RumBucketAgg>;
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

function emptyRumBucket(): RumBucketAgg {
  return {
    vitalsCount: 0,
    vitalsCidHll: null,
    errorsCount: 0,
    errorsCidHll: null,
    metrics: new Map(),
  };
}

function getOrCreateBucket(buckets: Map<number, RumBucketAgg>, bucketMs: number): RumBucketAgg {
  let bucket = buckets.get(bucketMs);
  if (!bucket) {
    bucket = emptyRumBucket();
    buckets.set(bucketMs, bucket);
  }
  return bucket;
}

function addMetricAgg(
  target: Map<string, RumMetricAgg>,
  name: string,
  count: number,
  good: number,
  poor: number,
  hist: Histogram | null,
): void {
  const prev = target.get(name);
  if (prev) {
    prev.count += count;
    prev.good += good;
    prev.poor += poor;
    prev.hist = mergeHist(prev.hist, hist);
  } else {
    target.set(name, {
      count,
      good,
      poor,
      hist: mergeHist(null, hist),
    });
  }
}

export function emptyRumSummary(): RumSummary {
  return {
    vitalsCount: 0,
    vitalsCidHll: null,
    errorsCount: 0,
    errorsCidHll: null,
    errorsMsgHll: null,
    sampled: false,
    buckets: new Map(),
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
  const bucketsCol = text.columns.get("bucket");
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
    const bucketMs = tsMs(bucketsCol?.[i]);
    switch (kind) {
      case "metric": {
        const name = String(keys?.[i] ?? "");
        const count = num(counts?.[i]);
        const good = num(goods?.[i]);
        const poor = num(poors?.[i]);
        const hist = readHist(valueHists, i);
        addMetricAgg(out.metrics, name, count, good, poor, hist);
        if (bucketMs !== null) {
          addMetricAgg(getOrCreateBucket(out.buckets, bucketMs).metrics, name, count, good, poor, hist);
        }
        break;
      }
      case "vitals_total": {
        const count = num(counts?.[i]);
        const hll = readHll(cidHlls, i);
        out.vitalsCount += count;
        out.vitalsCidHll = mergeHll(out.vitalsCidHll, hll);
        if (bucketMs !== null) {
          const b = getOrCreateBucket(out.buckets, bucketMs);
          b.vitalsCount += count;
          b.vitalsCidHll = mergeHll(b.vitalsCidHll, hll);
        }
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
        const count = num(counts?.[i]);
        const cHll = readHll(cidHlls, i);
        out.errorsCount += count;
        out.errorsCidHll = mergeHll(out.errorsCidHll, cHll);
        out.errorsMsgHll = mergeHll(out.errorsMsgHll, readHll(msgHlls, i));
        if (bucketMs !== null) {
          const b = getOrCreateBucket(out.buckets, bucketMs);
          b.errorsCount += count;
          b.errorsCidHll = mergeHll(b.errorsCidHll, cHll);
        }
        break;
      }
      case "sampled": {
        out.sampled = true;
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
    out.sampled ||= part.sampled;
    out.pagesTruncated ||= part.pagesTruncated;
    out.envsTruncated ||= part.envsTruncated;
    out.errorsTruncated ||= part.errorsTruncated;

    for (const [bucketMs, bucket] of part.buckets) {
      const dst = getOrCreateBucket(out.buckets, bucketMs);
      dst.vitalsCount += bucket.vitalsCount;
      dst.vitalsCidHll = mergeHll(dst.vitalsCidHll, bucket.vitalsCidHll);
      dst.errorsCount += bucket.errorsCount;
      dst.errorsCidHll = mergeHll(dst.errorsCidHll, bucket.errorsCidHll);
      for (const [name, agg] of bucket.metrics) {
        addMetricAgg(dst.metrics, name, agg.count, agg.good, agg.poor, agg.hist);
      }
    }

    for (const [name, agg] of part.metrics) {
      addMetricAgg(out.metrics, name, agg.count, agg.good, agg.poor, agg.hist);
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

type WindowedFile = {
  file: ManifestFile;
  start: number;
  end: number;
};

function isDayBundleKey(file: ManifestFile): boolean {
  return file.kind === "day" || file.key.includes("/day_bundle=");
}

function isOpenBundleKey(file: ManifestFile): boolean {
  return file.key.includes("/open_bundle=");
}

function isHourBundleKey(file: ManifestFile): boolean {
  return !isOpenBundleKey(file) && (file.kind === "hour" || file.key.includes("/hour_bundle="));
}

function containedInAny(win: WindowedFile, containers: readonly WindowedFile[]): boolean {
  return containers.some((c) => c.start <= win.start && c.end >= win.end);
}

function mergeIntervals(intervals: readonly [number, number][]): [number, number][] {
  if (intervals.length === 0) return [];
  const sorted = [...intervals].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const merged: [number, number][] = [[sorted[0][0], sorted[0][1]]];
  for (let i = 1; i < sorted.length; i++) {
    const [start, end] = sorted[i];
    const last = merged[merged.length - 1];
    if (start <= last[1]) {
      if (end > last[1]) last[1] = end;
    } else {
      merged.push([start, end]);
    }
  }
  return merged;
}

function intervalsCover(merged: readonly [number, number][], start: number, end: number): boolean {
  for (const [mStart, mEnd] of merged) {
    if (mStart > start) return false;
    if (mEnd >= end) return true;
  }
  return false;
}

/**
 * Deduplicates summary/bundle files inside `[fromMs, toMs]`: prefers complete
 * `day_bundle` over `hour_bundle`/`open_bundle`/`summary=`, `hour_bundle` over
 * `open_bundle`/`summary=`, and widest `open_bundle` over its covered prefix of
 * `summary=` files, while keeping trailing `summary=` files after `open_bundle`.
 */
export function selectRumSummaryFiles(
  files: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
): WindowedFile[] | null {
  const days = new Map<string, WindowedFile>();
  const hours = new Map<string, WindowedFile>();
  const opens = new Map<number, WindowedFile>();
  const windows = new Map<string, WindowedFile>();

  for (const file of files) {
    const win = fileWindow(file);
    if (!win) return null;
    const [start, end] = win;
    // Page, env, and error rows have bucket=null, so every selected file must fit inside [fromMs, toMs].
    if (start < fromMs || end > toMs) continue;
    const entry: WindowedFile = { file, start, end };
    if (isDayBundleKey(file)) {
      days.set(`${start}:${end}`, entry);
    } else if (isHourBundleKey(file)) {
      hours.set(`${start}:${end}`, entry);
    } else if (isOpenBundleKey(file)) {
      const prev = opens.get(start);
      if (!prev || end > prev.end) opens.set(start, entry);
    } else {
      windows.set(`${start}:${end}`, entry);
    }
  }

  const keptDays = [...days.values()];
  const keptHours = [...hours.values()].filter((h) => !containedInAny(h, keptDays));
  const coarse = [...keptDays, ...keptHours];
  const keptOpens = [...opens.values()].filter((o) => !containedInAny(o, coarse));
  const bundles = [...coarse, ...keptOpens];
  const keptWindows = [...windows.values()].filter((w) => !containedInAny(w, bundles));
  return [...bundles, ...keptWindows];
}

function decodePct(value: string): string {
  if (!value.includes("%")) return value;
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function secondsFromMs(value: number | null): number | null {
  if (value == null || !Number.isFinite(value)) return null;
  return roundTo(value > 20 ? value / 1000 : value, 2);
}

export function rumWireFromSummary(summary: RumSummary, params: RumQueryParams) {
  const data = rumAnalyticsFromSummary(summary);
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  const hourly = startMs == null || endMs == null || (endMs - startMs) <= 48 * HOUR_MS;
  const stepMs = hourly ? HOUR_MS : DAY_MS;

  const histQuantileScaled = (hist: Histogram | null | undefined, scale: number): number | null => {
    const q = hist?.quantile(0.75);
    return q == null ? null : q / scale;
  };

  const shapeCwv = (name: "LCP" | "CLS" | "INP" | "FCP" | "TTFB") => {
    const agg = summary.metrics.get(name);
    if (!agg || agg.count <= 0) {
      return { p75: null, distribution: { good: 0, needs_improvement: 0, poor: 0 } };
    }
    const scale = name === "CLS" ? 1_000_000 : 1_000;
    const rawP75 = histQuantileScaled(agg.hist, scale);
    const p75 = rawP75 == null ? null
      : name === "CLS" ? roundTo(rawP75, 3)
      : name === "INP" ? Math.round(rawP75)
      : secondsFromMs(rawP75);
    const good = roundTo((agg.good * 100) / agg.count, 2);
    const poor = roundTo((agg.poor * 100) / agg.count, 2);
    return {
      p75,
      distribution: {
        good,
        needs_improvement: roundTo(Math.max(0, 100 - good - poor), 2),
        poor,
      },
    };
  };

  const fidRawP75 = histQuantileScaled(summary.metrics.get("FID")?.hist, 1_000);
  const fidP75 = fidRawP75 == null ? null : roundTo(fidRawP75, 1);
  const fcpVital = shapeCwv("FCP");
  const ttfbVital = shapeCwv("TTFB");

  let interactionCount = 0;
  for (const [name, agg] of summary.metrics) {
    if (name.startsWith("EVENT_")) interactionCount += agg.count;
  }
  const pageviewCount = summary.vitalsCount === 0
    ? 0
    : Math.max(
        summary.vitalsCidHll?.count() ?? 0,
        summary.metrics.get("LCP")?.count ?? 0,
        summary.metrics.get("FCP")?.count ?? 0,
        summary.metrics.get("TTFB")?.count ?? 0,
      ) || Math.max(0, summary.vitalsCount - interactionCount);
  const errorCount = summary.errorsCount;
  const beaconCount = pageviewCount + interactionCount + errorCount;

  const rolledBuckets = new Map<number, RumBucketAgg>();
  for (const [bucket5Ms, b] of summary.buckets) {
    const aligned = Math.floor(bucket5Ms / stepMs) * stepMs;
    const dst = getOrCreateBucket(rolledBuckets, aligned);
    dst.vitalsCount += b.vitalsCount;
    dst.vitalsCidHll = mergeHll(dst.vitalsCidHll, b.vitalsCidHll);
    dst.errorsCount += b.errorsCount;
    dst.errorsCidHll = mergeHll(dst.errorsCidHll, b.errorsCidHll);
    for (const [name, agg] of b.metrics) {
      addMetricAgg(dst.metrics, name, agg.count, agg.good, agg.poor, agg.hist);
    }
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
  const bucketKeys = paddedBuckets.length > 0
    ? paddedBuckets
    : [...rolledBuckets.keys()].sort((a, b) => a - b);

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
    const row = rolledBuckets.get(bucket);
    timestamps.push(new Date(bucket).toISOString());
    if (!row) {
      lcpSeries.push(null);
      clsSeries.push(null);
      fcpSeries.push(null);
      ttfbSeries.push(null);
      errorRateSeries.push(null);
      pageviewSeries.push(0);
      interactionSeries.push(0);
      errorSeries.push(0);
      continue;
    }
    const bLcp = histQuantileScaled(row.metrics.get("LCP")?.hist, 1_000);
    const bCls = histQuantileScaled(row.metrics.get("CLS")?.hist, 1_000_000);
    const bFcp = histQuantileScaled(row.metrics.get("FCP")?.hist, 1_000);
    const bTtfb = histQuantileScaled(row.metrics.get("TTFB")?.hist, 1_000);
    lcpSeries.push(secondsFromMs(bLcp));
    clsSeries.push(bCls == null ? null : roundTo(bCls, 3));
    fcpSeries.push(secondsFromMs(bFcp));
    ttfbSeries.push(secondsFromMs(bTtfb));

    let bInteractions = 0;
    for (const [name, agg] of row.metrics) {
      if (name.startsWith("EVENT_")) bInteractions += agg.count;
    }
    const bPageviews = row.vitalsCount === 0
      ? 0
      : Math.max(
          row.vitalsCidHll?.count() ?? 0,
          row.metrics.get("LCP")?.count ?? 0,
          row.metrics.get("FCP")?.count ?? 0,
          row.metrics.get("TTFB")?.count ?? 0,
        ) || Math.max(0, row.vitalsCount - bInteractions);
    const bViews = bPageviews + bInteractions;
    const bErrs = row.errorsCount;
    const denom = bViews + bErrs;
    errorRateSeries.push(denom > 0 ? roundTo((bErrs * 100) / denom, 2) : null);
    pageviewSeries.push(bPageviews);
    interactionSeries.push(bInteractions);
    errorSeries.push(bErrs);
  }

  const worst_pages = [...summary.pages.entries()]
    .filter(([, p]) => p.count > 0)
    .map(([page_url, p]) => {
      const pCls = histQuantileScaled(p.cls, 1_000_000);
      return {
        path: decodePct(page_url),
        views: p.count,
        avg_load_time: null as number | null,
        lcp_p75: secondsFromMs(histQuantileScaled(p.lcp, 1_000)),
        cls_p75: pCls == null ? null : roundTo(pCls, 3),
        error_rate: 0,
      };
    })
    .sort((a, b) => {
      if (a.lcp_p75 === null && b.lcp_p75 !== null) return 1;
      if (a.lcp_p75 !== null && b.lcp_p75 === null) return -1;
      if (a.lcp_p75 !== null && b.lcp_p75 !== null && b.lcp_p75 !== a.lcp_p75) {
        return b.lcp_p75 - a.lcp_p75;
      }
      return b.views - a.views || a.path.localeCompare(b.path);
    })
    .slice(0, 5);

  const errors = [...summary.errors.values()]
    .sort(
      (a, b) =>
        b.count - a.count ||
        a.message.localeCompare(b.message) ||
        a.source.localeCompare(b.source) ||
        (a.minLine ?? 0) - (b.minLine ?? 0) ||
        (a.minCol ?? 0) - (b.minCol ?? 0),
    )
    .slice(0, 3)
    .map((e) => ({
      message: e.message,
      file: e.source,
      line: e.minLine ?? 0,
      col: e.minCol ?? 0,
      count: e.count,
    }));

  const envRecord = (map: Map<string, number>) => Object.fromEntries(map.entries());

  return {
    ...data,
    is_mock: false,
    estimated: false,
    beacon_count: beaconCount,
    pageview_count: pageviewCount,
    interaction_count: interactionCount,
    error_count: errorCount,
    vitals: {
      lcp: shapeCwv("LCP"),
      cls: shapeCwv("CLS"),
      inp: shapeCwv("INP"),
      fcp: fcpVital,
      ttfb: ttfbVital,
      fid: {
        p75: fidP75,
        fcp: fcpVital.p75,
        ttfb: ttfbVital.p75,
      },
    },
    worst_pages,
    slowest_urls: [] as Array<Record<string, unknown>>,
    slowest_networks: [] as Array<Record<string, unknown>>,
    errors,
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
    environments: {
      browsers: envRecord(summary.envs.browser),
      os: envRecord(summary.envs.os),
      devices: envRecord(summary.envs.device),
    },
  };
}

async function loadMergedRumSummary(
  source: RumRollupSource,
  params: RumQueryParams,
  options?: { rawManifest?: Manifest },
): Promise<RumSummary | null> {
  if (!rumRollupEligible(params)) return null;
  const fromMs = parseEpochMs(params.start_time);
  const toMs = parseEpochMs(params.end_time);
  if (fromMs === null || toMs === null || !(toMs > fromMs)) return null;

  const listFrom = new Date(Math.floor(fromMs / 1000) * 1000).toISOString();
  const listTo = new Date(Math.ceil(toMs / 1000) * 1000).toISOString();
  const rawPromise = options?.rawManifest?.complete
    ? Promise.resolve(options.rawManifest)
    : fetchManifest(source.fetchJson, listFrom, listTo, params.signal, "rum", "raw");
  const [raw, summaries] = await Promise.all([
    rawPromise,
    fetchManifest(source.fetchJson, listFrom, listTo, params.signal, "rum", "summary"),
  ]);
  if (!raw.complete || !summaries.complete) return null;

  const selected = selectRumSummaryFiles(summaries.files, fromMs, toMs);
  if (!selected) return null;
  const covered = mergeIntervals(selected.map((entry) => [entry.start, entry.end]));

  for (const file of raw.files) {
    const win = fileWindow(file);
    if (!win) return null;
    const clampedStart = Math.max(win[0], fromMs);
    const clampedEnd = Math.min(win[1], toMs);
    if (clampedEnd <= clampedStart) continue;
    if (!intervalsCover(covered, clampedStart, clampedEnd)) return null;
  }

  if (selected.length === 0) return emptyRumSummary();

  let decoded: (RumSummary | null)[];
  try {
    decoded = await mapLimit(selected, RUM_FETCH_CONCURRENCY, async ({ file }) => {
      params.signal?.throwIfAborted();
      const { bytes } = await fetchLanes.run("summary", () => {
        params.signal?.throwIfAborted();
        return source.fetchFile(file.key, params.signal);
      });
      return decodeRumSummary(bytes);
    });
  } catch (error) {
    params.signal?.throwIfAborted();
    if (!(error instanceof MissingCloudFileError)) throw error;
    console.warn(`flp: RUM summary ${error.key} is listed but missing; reading raw Parquet`);
    return null;
  }
  if (decoded.some((part) => part === null)) return null;

  return mergeRumSummaries(decoded as RumSummary[]);
}

export async function queryRumFromRollup(
  source: RumRollupSource,
  params: RumQueryParams,
): Promise<ReturnType<typeof emptyRumAnalytics> | null> {
  const merged = await loadMergedRumSummary(source, params);
  if (!merged) return null;
  return rumAnalyticsFromSummary(merged);
}

export async function queryRumWireFromRollup(
  source: RumRollupSource,
  params: RumQueryParams,
  options?: { rawManifest?: Manifest },
): Promise<ReturnType<typeof rumWireFromSummary> | null> {
  const merged = await loadMergedRumSummary(source, params, options);
  if (!merged || merged.sampled) return null;
  return rumWireFromSummary(merged, params);
}
