import type { CmcdQueryParams } from "./cmcd";
import { classifySessionDurationBucket, CMCD_SESSION_DURATION_BUCKETS, emptyCmcdAggregates, padCmcdTimeseries } from "./cmcd";
import type { FileSource } from "./dashboard/engine";
import { Histogram } from "./dashboard/histogram";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { Hll } from "./dashboard/hll";
import { mergeHistogramBytes, mergeHllBytes } from "./rollup-merge";
import { filesOverlapping, type ManifestFile } from "./dashboard/manifest";
import { readColumns } from "./dashboard/parquet";
import { mapLimit } from "./dashboard/sources";
import { parseEpochMs, roundTo } from "./engine-utils";
import { fetchLanes } from "./fetch-lanes";
import { hasFilterValues } from "./rollup-sections";
import { formatAsnDisplayLabel, loadAsnNames } from "./asn-names";

const CMCD_FETCH_CONCURRENCY = 16;
const CMCD_ROLLUP_BUCKET_MS = 300_000;
const MAX_CMCD_LIST_PAGES = 64;
const MAX_DECODED_CMCD_FILES = 256;
/**
 * Cap per-window cmcd_summary files before falling back to compacted raw Parquet. Hour and day
 * bundles do not count: each stands for a whole hour or day of windows, so 24h of bundles plus
 * the open hour's windows (~140) stays on summaries instead of ~150 MB of raw compacts.
 */
export const MAX_CMCD_ROLLUP_FILES = 250;

export type CmcdRollupSource = Pick<FileSource, "fetchJson" | "fetchFile">;

type DecodedCmcdFile = {
  rows: number;
  textNum: Map<string, ArrayLike<unknown>>;
  bin: Map<string, ArrayLike<unknown>>;
};

const decodedCmcdFileCache = new Map<string, DecodedCmcdFile>();
const cmcdSourceIds = new WeakMap<object, number>();
let nextCmcdSourceId = 1;

export function clearCmcdRollupCache(): void {
  decodedCmcdFileCache.clear();
}

function cmcdFileCacheKey(source: CmcdRollupSource, file: ManifestFile): string {
  const scopeFn = (source.fetchJson as { manifestCacheScope?: () => string }).manifestCacheScope;
  let scope: string;
  if (scopeFn) {
    scope = scopeFn();
  } else {
    let id = cmcdSourceIds.get(source.fetchFile);
    if (id === undefined) {
      id = nextCmcdSourceId++;
      cmcdSourceIds.set(source.fetchFile, id);
    }
    scope = `src:${id}`;
  }
  return `${scope}\0${file.key}\0${file.etag ?? file.size}\0${file.rows}`;
}

function getCachedCmcdFile(key: string): DecodedCmcdFile | undefined {
  const hit = decodedCmcdFileCache.get(key);
  if (!hit) return undefined;
  decodedCmcdFileCache.delete(key);
  decodedCmcdFileCache.set(key, hit);
  return hit;
}

function setCachedCmcdFile(key: string, value: DecodedCmcdFile): void {
  if (decodedCmcdFileCache.has(key)) {
    decodedCmcdFileCache.delete(key);
  } else if (decodedCmcdFileCache.size >= MAX_DECODED_CMCD_FILES) {
    const oldest = decodedCmcdFileCache.keys().next().value;
    if (oldest !== undefined) decodedCmcdFileCache.delete(oldest);
  }
  decodedCmcdFileCache.set(key, value);
}

const TEXT_AND_NUM_COLS = [
  "kind", "bucket", "key", "label", "truncated", "reqs",
  "rebuffer_events", "has_bs", "has_su", "has_ot", "has_bl", "has_br", "has_mtp", "has_cid",
  "br_sum", "br_n", "tb_sum", "tb_n", "bl_sum", "bl_n", "mtp_sum", "mtp_n",
  "v_br_sum", "v_br_n", "v_tb_sum", "v_tb_n", "v_bl_sum", "v_bl_n",
  "util_sum", "util_n", "util_all_sum", "util_all_n",
  "startup_requests", "su_bl_sum", "su_bl_n", "su_ttfb_sum", "su_ttfb_n",
  "min_ts", "max_ts", "first_ts", "last_ts",
] as const;

const BINARY_COLS = [
  "sid_hll", "rebuffer_sid_hll", "v_bl_hist", "bl_hist", "v_mtp_hist", "mtp_hist",
] as const;

type BucketAgg = {
  reqs: number;
  sidHll: Hll;
  rebufferEvents: number;
  rebufferSidHll: Hll;
  hasBs: number;
  hasSu: number;
  hasOt: number;
  hasBl: number;
  hasBr: number;
  hasMtp: number;
  hasCid: number;
  brSum: number;
  brN: number;
  tbSum: number;
  tbN: number;
  blSum: number;
  blN: number;
  mtpSum: number;
  mtpN: number;
  vBrSum: number;
  vBrN: number;
  vTbSum: number;
  vTbN: number;
  vBlSum: number;
  vBlN: number;
  utilSum: number;
  utilN: number;
  utilAllSum: number;
  utilAllN: number;
  startupRequests: number;
  suBlSum: number;
  suBlN: number;
  suTtfbSum: number;
  suTtfbN: number;
  vBlHist: Histogram;
  blHist: Histogram;
  vMtpHist: Histogram;
  mtpHist: Histogram;
  minTs: number;
  maxTs: number;
};

type ContentAgg = {
  reqs: number;
  sidHll: Hll;
  rebufferSidHll: Hll;
  hasBs: number;
  vBrSum: number;
  vBrN: number;
  brSum: number;
  brN: number;
  vBlSum: number;
  vBlN: number;
  blSum: number;
  blN: number;
};

type CountryAgg = {
  reqs: number;
  sidHll: Hll;
  rebufferEvents: number;
  rebufferSidHll: Hll;
  hasBs: number;
  vMtpHist: Histogram;
  mtpHist: Histogram;
};

type AsnAgg = {
  reqs: number;
  label: string | null;
  sidHll: Hll;
  rebufferEvents: number;
  rebufferSidHll: Hll;
  hasBs: number;
  vBrSum: number;
  vBrN: number;
  brSum: number;
  brN: number;
  vBlSum: number;
  vBlN: number;
  blSum: number;
  blN: number;
  mtpSum: number;
  mtpN: number;
};

function newBucketAgg(): BucketAgg {
  return {
    reqs: 0,
    sidHll: new Hll(),
    rebufferEvents: 0,
    rebufferSidHll: new Hll(),
    hasBs: 0,
    hasSu: 0,
    hasOt: 0,
    hasBl: 0,
    hasBr: 0,
    hasMtp: 0,
    hasCid: 0,
    brSum: 0,
    brN: 0,
    tbSum: 0,
    tbN: 0,
    blSum: 0,
    blN: 0,
    mtpSum: 0,
    mtpN: 0,
    vBrSum: 0,
    vBrN: 0,
    vTbSum: 0,
    vTbN: 0,
    vBlSum: 0,
    vBlN: 0,
    utilSum: 0,
    utilN: 0,
    utilAllSum: 0,
    utilAllN: 0,
    startupRequests: 0,
    suBlSum: 0,
    suBlN: 0,
    suTtfbSum: 0,
    suTtfbN: 0,
    vBlHist: new Histogram(),
    blHist: new Histogram(),
    vMtpHist: new Histogram(),
    mtpHist: new Histogram(),
    minTs: Infinity,
    maxTs: -Infinity,
  };
}

function mergeHll(target: Hll, blob: unknown) {
  mergeHllBytes(target, blob);
}

function mergeHist(target: Histogram, blob: unknown) {
  mergeHistogramBytes(target, blob);
}

function num(col: ArrayLike<unknown> | undefined, i: number): number {
  const v = col?.[i];
  return v == null ? 0 : Number(v);
}

function str(col: ArrayLike<unknown> | undefined, i: number): string | null {
  const v = col?.[i];
  return v == null ? null : String(v);
}

function histQuantile(hist: Histogram, q: number): number | null {
  const v = hist.quantile(q);
  return v == null ? null : v / 1000;
}

/** Whether `cmcd_summary` may answer this /api/cmcd/aggregates read. */
export function cmcdRollupEligible(params: CmcdQueryParams): boolean {
  if (
    params.filter_sf ||
    params.filter_ot ||
    params.filter_st ||
    params.filter_cid ||
    hasFilterValues(params.filters) ||
    params.sections?.some((s) => s === "bitrate_ladder" || s === "stream_type_dist")
  ) {
    return false;
  }
  return (params.bucket_seconds ?? 60) >= 300;
}

async function listCmcdSummaryFiles(
  fetchJson: CmcdRollupSource["fetchJson"],
  fromIso: string,
  toIso: string,
  fromMs: number,
  toMs: number,
  maxFiles: number,
  signal?: AbortSignal,
  view?: "summary",
): Promise<ManifestFile[] | null> {
  const byKey = new Map<string, ManifestFile>();
  let windows = 0;
  let cursor: string | null = null;
  for (let pageIndex = 0; pageIndex < MAX_CMCD_LIST_PAGES; pageIndex++) {
    signal?.throwIfAborted();
    const params = new URLSearchParams({
      from: fromIso,
      to: toIso,
      dataset: "cmcd_summary",
    });
    if (view) params.set("view", view);
    if (cursor) params.set("cursor", cursor);
    const page = (await fetchJson(`/api/files?${params}`, signal)) as {
      files?: ManifestFile[];
      next_cursor?: string | null;
      complete?: boolean;
    };
    const pageFiles = Array.isArray(page?.files) ? page.files : [];
    for (const file of filesOverlapping(pageFiles, fromMs, toMs)) {
      if (byKey.has(file.key)) continue;
      byKey.set(file.key, file);
      if (file.kind !== "hour" && file.kind !== "day" && ++windows > maxFiles) return null;
    }
    if (page?.complete) return [...byKey.values()];
    if (!page?.next_cursor) return null;
    cursor = page.next_cursor;
  }
  return null;
}

/**
 * The fewest CMCD summary files for the range: `view=summary` replaces each hour whose
 * `hour_bundle` covers exactly its listed windows (and whole days likewise) with that bundle,
 * and the open hour's settled windows with its `open_bundle` snapshot (also kind `hour`).
 * Bundles merge their windows losslessly, so either listing gives the same aggregates. A server
 * without CMCD bundles rejects the view, and the per-window listing answers instead.
 */
async function listCmcdRollupFiles(
  fetchJson: CmcdRollupSource["fetchJson"],
  fromIso: string,
  toIso: string,
  fromMs: number,
  toMs: number,
  signal?: AbortSignal,
): Promise<ManifestFile[] | null> {
  try {
    return await listCmcdSummaryFiles(
      fetchJson,
      fromIso,
      toIso,
      fromMs,
      toMs,
      MAX_CMCD_ROLLUP_FILES,
      signal,
      "summary",
    );
  } catch (error) {
    if (signal?.aborted) throw error;
  }
  return listCmcdSummaryFiles(
    fetchJson,
    fromIso,
    toIso,
    fromMs,
    toMs,
    MAX_CMCD_ROLLUP_FILES,
    signal,
  );
}

export async function cmcdFromRollup(
  params: CmcdQueryParams,
  source: CmcdRollupSource,
  signal?: AbortSignal,
) {
  if (!cmcdRollupEligible(params)) return null;
  const fromMs = parseEpochMs(params.start_time);
  const toMs = parseEpochMs(params.end_time);
  if (fromMs === null || toMs === null || toMs <= fromMs) return null;

  const fromIso = new Date(fromMs).toISOString();
  const toIso = new Date(toMs).toISOString();
  const files = await listCmcdRollupFiles(
    source.fetchJson,
    fromIso,
    toIso,
    fromMs,
    toMs,
    signal,
  );
  if (!files || files.length === 0) return null;
  try {
    return await cmcdFromFiles(files, params, fromMs, toMs, source, signal);
  } catch (error) {
    if (signal?.aborted || !(error instanceof MissingCloudFileError)) throw error;
    const missing = files.find((file) => file.key === error.key);
    if (missing?.kind !== "hour" && missing?.kind !== "day") throw error;
    // An open hour's snapshot is replaced as the hour grows: a fresh summary view names its
    // successor. A bundle storage lost stays listed, and its hours' windows answer exactly.
    const relisted = await listCmcdRollupFiles(source.fetchJson, fromIso, toIso, fromMs, toMs, signal);
    if (relisted && relisted.length > 0 && !relisted.some((file) => file.key === error.key)) {
      try {
        return await cmcdFromFiles(relisted, params, fromMs, toMs, source, signal);
      } catch (retryError) {
        if (signal?.aborted || !(retryError instanceof MissingCloudFileError)) throw retryError;
      }
    }
    console.warn(`flp: CMCD bundle ${error.key} is listed but missing; reading its windows`);
    const windows = await listCmcdSummaryFiles(
      source.fetchJson,
      fromIso,
      toIso,
      fromMs,
      toMs,
      MAX_CMCD_ROLLUP_FILES,
      signal,
    );
    if (!windows || windows.length === 0) return null;
    return cmcdFromFiles(windows, params, fromMs, toMs, source, signal);
  }
}

async function cmcdFromFiles(
  files: ManifestFile[],
  params: CmcdQueryParams,
  fromMs: number,
  toMs: number,
  source: CmcdRollupSource,
  signal?: AbortSignal,
) {
  const asnNamesReady = loadAsnNames();
  const buckets = new Map<number, BucketAgg>();
  const sessions = new Map<string, { firstTs: number; lastTs: number; reqs: number }>();
  const contents = new Map<string, ContentAgg>();
  const countries = new Map<string, CountryAgg>();
  const asns = new Map<number, AsnAgg>();
  const ots = new Map<string, number>();
  const sfs = new Map<string, { reqs: number; sidHll: Hll }>();
  const truncated = new Set<string>();

  await mapLimit(files, CMCD_FETCH_CONCURRENCY, async (file) => {
    signal?.throwIfAborted();
    const cacheKey = cmcdFileCacheKey(source, file);
    let decoded = getCachedCmcdFile(cacheKey);
    if (!decoded) {
      const { bytes } = await fetchLanes.run("summary", () => {
        signal?.throwIfAborted();
        return source.fetchFile(file.key, signal);
      });
      const [textNum, bin] = await Promise.all([
        readColumns(bytes, TEXT_AND_NUM_COLS, true),
        readColumns(bytes, BINARY_COLS, false),
      ]);
      decoded = { rows: textNum.rows, textNum: textNum.columns, bin: bin.columns };
      setCachedCmcdFile(cacheKey, decoded);
    }
    const c = decoded.textNum;
    const b = decoded.bin;
    const rowCount = decoded.rows;
    const kindCol = c.get("kind");
    const bucketCol = c.get("bucket");
    const keyCol = c.get("key");
    const labelCol = c.get("label");
    const truncCol = c.get("truncated");
    const reqsCol = c.get("reqs");
    const sidHllCol = b.get("sid_hll");
    const rebCol = c.get("rebuffer_events");
    const rebSidHllCol = b.get("rebuffer_sid_hll");
    const hasBsCol = c.get("has_bs");
    const hasSuCol = c.get("has_su");
    const hasOtCol = c.get("has_ot");
    const hasBlCol = c.get("has_bl");
    const hasBrCol = c.get("has_br");
    const hasMtpCol = c.get("has_mtp");
    const hasCidCol = c.get("has_cid");
    const brSumCol = c.get("br_sum");
    const brNCol = c.get("br_n");
    const tbSumCol = c.get("tb_sum");
    const tbNCol = c.get("tb_n");
    const blSumCol = c.get("bl_sum");
    const blNCol = c.get("bl_n");
    const mtpSumCol = c.get("mtp_sum");
    const mtpNCol = c.get("mtp_n");
    const vBrSumCol = c.get("v_br_sum");
    const vBrNCol = c.get("v_br_n");
    const vTbSumCol = c.get("v_tb_sum");
    const vTbNCol = c.get("v_tb_n");
    const vBlSumCol = c.get("v_bl_sum");
    const vBlNCol = c.get("v_bl_n");
    const utilSumCol = c.get("util_sum");
    const utilNCol = c.get("util_n");
    const utilAllSumCol = c.get("util_all_sum");
    const utilAllNCol = c.get("util_all_n");
    const suReqCol = c.get("startup_requests");
    const suBlSumCol = c.get("su_bl_sum");
    const suBlNCol = c.get("su_bl_n");
    const suTtfbSumCol = c.get("su_ttfb_sum");
    const suTtfbNCol = c.get("su_ttfb_n");
    const vBlHistCol = b.get("v_bl_hist");
    const blHistCol = b.get("bl_hist");
    const vMtpHistCol = b.get("v_mtp_hist");
    const mtpHistCol = b.get("mtp_hist");
    const minTsCol = c.get("min_ts");
    const maxTsCol = c.get("max_ts");
    const firstTsCol = c.get("first_ts");
    const lastTsCol = c.get("last_ts");

    for (let i = 0; i < rowCount; i++) {
      const kind = str(kindCol, i);
      const key = str(keyCol, i);
      if (kind === "totals") {
        if (key && truncCol?.[i] === true) truncated.add(key);
        continue;
      }
      if (kind === "bucket") {
        const bucketMs = num(bucketCol, i);
        if (bucketMs + CMCD_ROLLUP_BUCKET_MS <= fromMs || bucketMs >= toMs) {
          continue;
        }
        let agg = buckets.get(bucketMs);
        if (!agg) {
          agg = newBucketAgg();
          buckets.set(bucketMs, agg);
        }
        agg.reqs += num(reqsCol, i);
        mergeHll(agg.sidHll, sidHllCol?.[i]);
        agg.rebufferEvents += num(rebCol, i);
        mergeHll(agg.rebufferSidHll, rebSidHllCol?.[i]);
        agg.hasBs += num(hasBsCol, i);
        agg.hasSu += num(hasSuCol, i);
        agg.hasOt += num(hasOtCol, i);
        agg.hasBl += num(hasBlCol, i);
        agg.hasBr += num(hasBrCol, i);
        agg.hasMtp += num(hasMtpCol, i);
        agg.hasCid += num(hasCidCol, i);
        agg.brSum += num(brSumCol, i);
        agg.brN += num(brNCol, i);
        agg.tbSum += num(tbSumCol, i);
        agg.tbN += num(tbNCol, i);
        agg.blSum += num(blSumCol, i);
        agg.blN += num(blNCol, i);
        agg.mtpSum += num(mtpSumCol, i);
        agg.mtpN += num(mtpNCol, i);
        agg.vBrSum += num(vBrSumCol, i);
        agg.vBrN += num(vBrNCol, i);
        agg.vTbSum += num(vTbSumCol, i);
        agg.vTbN += num(vTbNCol, i);
        agg.vBlSum += num(vBlSumCol, i);
        agg.vBlN += num(vBlNCol, i);
        agg.utilSum += num(utilSumCol, i);
        agg.utilN += num(utilNCol, i);
        agg.utilAllSum += num(utilAllSumCol, i);
        agg.utilAllN += num(utilAllNCol, i);
        agg.startupRequests += num(suReqCol, i);
        agg.suBlSum += num(suBlSumCol, i);
        agg.suBlN += num(suBlNCol, i);
        agg.suTtfbSum += num(suTtfbSumCol, i);
        agg.suTtfbN += num(suTtfbNCol, i);
        mergeHist(agg.vBlHist, vBlHistCol?.[i]);
        mergeHist(agg.blHist, blHistCol?.[i]);
        mergeHist(agg.vMtpHist, vMtpHistCol?.[i]);
        mergeHist(agg.mtpHist, mtpHistCol?.[i]);
        if (minTsCol?.[i] != null) agg.minTs = Math.min(agg.minTs, Number(minTsCol[i]));
        if (maxTsCol?.[i] != null) agg.maxTs = Math.max(agg.maxTs, Number(maxTsCol[i]));
      } else if (kind === "session" && key) {
        const firstTs = num(firstTsCol, i);
        const lastTs = num(lastTsCol, i);
        if (lastTs < fromMs || firstTs >= toMs) continue;
        const reqs = num(reqsCol, i);
        const cur = sessions.get(key);
        if (cur) {
          cur.firstTs = Math.min(cur.firstTs, firstTs);
          cur.lastTs = Math.max(cur.lastTs, lastTs);
          cur.reqs += reqs;
        } else {
          sessions.set(key, { firstTs, lastTs, reqs });
        }
      } else if (kind === "content" && key) {
        let agg = contents.get(key);
        if (!agg) {
          agg = {
            reqs: 0, sidHll: new Hll(), rebufferSidHll: new Hll(), hasBs: 0,
            vBrSum: 0, vBrN: 0, brSum: 0, brN: 0, vBlSum: 0, vBlN: 0, blSum: 0, blN: 0,
          };
          contents.set(key, agg);
        }
        agg.reqs += num(reqsCol, i);
        mergeHll(agg.sidHll, sidHllCol?.[i]);
        mergeHll(agg.rebufferSidHll, rebSidHllCol?.[i]);
        agg.hasBs += num(hasBsCol, i);
        agg.vBrSum += num(vBrSumCol, i);
        agg.vBrN += num(vBrNCol, i);
        agg.brSum += num(brSumCol, i);
        agg.brN += num(brNCol, i);
        agg.vBlSum += num(vBlSumCol, i);
        agg.vBlN += num(vBlNCol, i);
        agg.blSum += num(blSumCol, i);
        agg.blN += num(blNCol, i);
      } else if (kind === "country" && key) {
        let agg = countries.get(key);
        if (!agg) {
          agg = {
            reqs: 0, sidHll: new Hll(), rebufferEvents: 0, rebufferSidHll: new Hll(),
            hasBs: 0, vMtpHist: new Histogram(), mtpHist: new Histogram(),
          };
          countries.set(key, agg);
        }
        agg.reqs += num(reqsCol, i);
        mergeHll(agg.sidHll, sidHllCol?.[i]);
        agg.rebufferEvents += num(rebCol, i);
        mergeHll(agg.rebufferSidHll, rebSidHllCol?.[i]);
        agg.hasBs += num(hasBsCol, i);
        mergeHist(agg.vMtpHist, vMtpHistCol?.[i]);
        mergeHist(agg.mtpHist, mtpHistCol?.[i]);
      } else if (kind === "asn" && key) {
        const asn = Number(key);
        let agg = asns.get(asn);
        if (!agg) {
          agg = {
            reqs: 0, label: null, sidHll: new Hll(), rebufferEvents: 0, rebufferSidHll: new Hll(),
            hasBs: 0, vBrSum: 0, vBrN: 0, brSum: 0, brN: 0, vBlSum: 0, vBlN: 0, blSum: 0, blN: 0,
            mtpSum: 0, mtpN: 0,
          };
          asns.set(asn, agg);
        }
        agg.reqs += num(reqsCol, i);
        const label = str(labelCol, i);
        if (label && (!agg.label || label < agg.label)) agg.label = label;
        mergeHll(agg.sidHll, sidHllCol?.[i]);
        agg.rebufferEvents += num(rebCol, i);
        mergeHll(agg.rebufferSidHll, rebSidHllCol?.[i]);
        agg.hasBs += num(hasBsCol, i);
        agg.vBrSum += num(vBrSumCol, i);
        agg.vBrN += num(vBrNCol, i);
        agg.brSum += num(brSumCol, i);
        agg.brN += num(brNCol, i);
        agg.vBlSum += num(vBlSumCol, i);
        agg.vBlN += num(vBlNCol, i);
        agg.blSum += num(blSumCol, i);
        agg.blN += num(blNCol, i);
        agg.mtpSum += num(mtpSumCol, i);
        agg.mtpN += num(mtpNCol, i);
      } else if (kind === "ot" && key) {
        ots.set(key, (ots.get(key) ?? 0) + num(reqsCol, i));
      } else if (kind === "sf" && key) {
        let agg = sfs.get(key);
        if (!agg) {
          agg = { reqs: 0, sidHll: new Hll() };
          sfs.set(key, agg);
        }
        agg.reqs += num(reqsCol, i);
        mergeHll(agg.sidHll, sidHllCol?.[i]);
      }
    }
  });

  const overall = newBucketAgg();
  for (const agg of buckets.values()) {
    overall.reqs += agg.reqs;
    overall.sidHll.merge(agg.sidHll);
    overall.rebufferEvents += agg.rebufferEvents;
    overall.rebufferSidHll.merge(agg.rebufferSidHll);
    overall.hasBs += agg.hasBs;
    overall.hasSu += agg.hasSu;
    overall.hasOt += agg.hasOt;
    overall.hasBl += agg.hasBl;
    overall.hasBr += agg.hasBr;
    overall.hasMtp += agg.hasMtp;
    overall.hasCid += agg.hasCid;
    overall.brSum += agg.brSum;
    overall.brN += agg.brN;
    overall.tbSum += agg.tbSum;
    overall.tbN += agg.tbN;
    overall.blSum += agg.blSum;
    overall.blN += agg.blN;
    overall.mtpSum += agg.mtpSum;
    overall.mtpN += agg.mtpN;
    overall.vBrSum += agg.vBrSum;
    overall.vBrN += agg.vBrN;
    overall.vTbSum += agg.vTbSum;
    overall.vTbN += agg.vTbN;
    overall.vBlSum += agg.vBlSum;
    overall.vBlN += agg.vBlN;
    overall.utilSum += agg.utilSum;
    overall.utilN += agg.utilN;
    overall.utilAllSum += agg.utilAllSum;
    overall.utilAllN += agg.utilAllN;
    overall.startupRequests += agg.startupRequests;
    overall.suBlSum += agg.suBlSum;
    overall.suBlN += agg.suBlN;
    overall.suTtfbSum += agg.suTtfbSum;
    overall.suTtfbN += agg.suTtfbN;
    overall.vBlHist.merge(agg.vBlHist);
    overall.blHist.merge(agg.blHist);
    overall.vMtpHist.merge(agg.vMtpHist);
    overall.mtpHist.merge(agg.mtpHist);
  }

  const totalRequests = overall.reqs;
  if (totalRequests === 0) return emptyCmcdAggregates();

  const activeSessions = Math.round(overall.sidHll.count());
  const rebufferSessions = Math.round(overall.rebufferSidHll.count());
  const hasOt = overall.hasOt > 0;
  const has = (key: keyof Pick<BucketAgg, "hasBs" | "hasSu" | "hasBl" | "hasBr" | "hasMtp" | "hasCid">) =>
    overall[key] > 0;
  const hasBsEffective = has("hasBs") || has("hasBl");
  const hasSu = has("hasSu");
  const rebufferEvents = overall.rebufferEvents;
  const rebufferRate = totalRequests > 0 ? roundTo((rebufferEvents * 100) / totalRequests, 2) : 0;
  const avgBitrate = overall.brN > 0 ? overall.brSum / overall.brN : 0;
  const avgTopBitrate = overall.tbN > 0 ? overall.tbSum / overall.tbN : 0;
  const utilizationRatio = avgTopBitrate > 0 ? roundTo((avgBitrate * 100) / avgTopBitrate, 2) : 0;

  const bucketMs = Math.max(300, Math.min(14400, Math.floor(params.bucket_seconds ?? 300))) * 1000;
  const topN = Math.max(1, Math.min(100, Math.floor(params.top_n ?? 30)));
  const rebucketed = new Map<number, BucketAgg>();
  for (const [bMs, agg] of buckets) {
    const targetMs = Math.floor(bMs / bucketMs) * bucketMs;
    let dst = rebucketed.get(targetMs);
    if (!dst) {
      dst = newBucketAgg();
      rebucketed.set(targetMs, dst);
    }
    dst.reqs += agg.reqs;
    dst.sidHll.merge(agg.sidHll);
    dst.rebufferEvents += agg.rebufferEvents;
    dst.rebufferSidHll.merge(agg.rebufferSidHll);
    dst.hasBs += agg.hasBs;
    dst.hasSu += agg.hasSu;
    dst.brSum += agg.brSum;
    dst.brN += agg.brN;
    dst.tbSum += agg.tbSum;
    dst.tbN += agg.tbN;
    dst.blSum += agg.blSum;
    dst.blN += agg.blN;
    dst.mtpSum += agg.mtpSum;
    dst.mtpN += agg.mtpN;
    dst.vBrSum += agg.vBrSum;
    dst.vBrN += agg.vBrN;
    dst.vTbSum += agg.vTbSum;
    dst.vTbN += agg.vTbN;
    dst.vBlSum += agg.vBlSum;
    dst.vBlN += agg.vBlN;
    dst.utilSum += agg.utilSum;
    dst.utilN += agg.utilN;
    dst.utilAllSum += agg.utilAllSum;
    dst.utilAllN += agg.utilAllN;
    dst.startupRequests += agg.startupRequests;
    dst.suBlSum += agg.suBlSum;
    dst.suBlN += agg.suBlN;
    dst.suTtfbSum += agg.suTtfbSum;
    dst.suTtfbN += agg.suTtfbN;
    dst.vBlHist.merge(agg.vBlHist);
    dst.blHist.merge(agg.blHist);
    dst.vMtpHist.merge(agg.vMtpHist);
    dst.mtpHist.merge(agg.mtpHist);
  }

  const sortedBuckets = [...rebucketed.entries()].sort((a, b) => a[0] - b[0]);
  const newSessionCounts = new Map<number, number>();
  for (const s of sessions.values()) {
    const bMs = Math.floor(s.firstTs / bucketMs) * bucketMs;
    newSessionCounts.set(bMs, (newSessionCounts.get(bMs) ?? 0) + 1);
  }

  let peakViewers = 0;
  const sessions_ts: Record<string, unknown>[] = [];
  const buffer_health_ts: Record<string, unknown>[] = [];
  const bitrate_ts: Record<string, unknown>[] = [];
  const throughput_ts: Record<string, unknown>[] = [];
  const startup_ts: Record<string, unknown>[] = [];

  for (const [bMs, b] of sortedBuckets) {
    const tsIso = new Date(bMs).toISOString();
    const sess = Math.round(b.sidHll.count());
    const rebSess = Math.round(b.rebufferSidHll.count());
    if (sess > peakViewers) peakViewers = sess;
    const reqs = b.reqs;
    const rebuf = b.rebufferEvents;
    const encBrVal = hasOt ? (b.vBrN > 0 ? b.vBrSum / b.vBrN : null) : (b.brN > 0 ? b.brSum / b.brN : null);
    const topBrVal = hasOt ? (b.vTbN > 0 ? b.vTbSum / b.vTbN : null) : (b.tbN > 0 ? b.tbSum / b.tbN : null);
    const encBr = encBrVal ?? 0;
    const topBr = topBrVal ?? 0;
    const mtp = b.mtpN > 0 ? b.mtpSum / b.mtpN : 0;
    const blHist = hasOt ? b.vBlHist : b.blHist;
    const mtpHist = hasOt ? b.vMtpHist : b.mtpHist;
    const p10Buf = histQuantile(blHist, 0.1);
    const p50Buf = histQuantile(blHist, 0.5);
    const p95Buf = histQuantile(blHist, 0.95);
    const utilN = hasOt ? b.utilN : b.utilAllN;
    const utilSum = hasOt ? b.utilSum : b.utilAllSum;

    sessions_ts.push({
      bucket: tsIso,
      concurrent_sessions: sess,
      new_sessions: newSessionCounts.get(bMs) ?? 0,
      rebuffer_session_pct: hasBsEffective && sess > 0 ? roundTo((rebSess * 100) / sess, 2) : null,
      timestamp: tsIso,
      active_sessions: sess,
      rebuffer_events: rebuf,
      rebuffer_rate: reqs > 0 ? roundTo((rebuf * 100) / reqs, 2) : 0,
    });
    buffer_health_ts.push({
      bucket: tsIso,
      p50_buffer: p50Buf,
      p95_buffer: p95Buf,
      starvation_rate: hasBsEffective && reqs > 0 ? roundTo((rebuf * 100) / reqs, 2) : null,
      timestamp: tsIso,
      avg_buffer_ms: roundTo(b.blN > 0 ? b.blSum / b.blN : 0, 1),
      p10_buffer_ms: roundTo(p10Buf ?? 0, 1),
      p50_buffer_ms: roundTo(p50Buf ?? 0, 1),
      rebuffer_events: rebuf,
    });
    bitrate_ts.push({
      bucket: tsIso,
      avg_bitrate: encBrVal,
      timestamp: tsIso,
      avg_encoded_bitrate: roundTo(encBr, 1),
      avg_top_bitrate: roundTo(topBr, 1),
      utilization_ratio: utilN > 0 ? roundTo(utilSum / utilN, 2) : null,
    });
    throughput_ts.push({
      bucket: tsIso,
      p50: histQuantile(mtpHist, 0.5),
      p95: histQuantile(mtpHist, 0.95),
      p99: histQuantile(mtpHist, 0.99),
      timestamp: tsIso,
      avg_throughput_kbps: roundTo(mtp, 1),
      avg_bitrate_kbps: roundTo(encBr, 1),
      headroom_ratio: encBr > 0 ? roundTo(mtp / encBr, 2) : 0,
    });
    startup_ts.push({
      bucket: tsIso,
      startup_ratio: hasSu && reqs > 0 ? roundTo((b.startupRequests * 100) / reqs, 2) : null,
      timestamp: tsIso,
      startup_requests: b.startupRequests,
      avg_startup_buffer_ms: roundTo(b.suBlN > 0 ? b.suBlSum / b.suBlN : 0, 1),
      avg_startup_ttfb_ms: roundTo(b.suTtfbN > 0 ? b.suTtfbSum / b.suTtfbN : 0, 2),
    });
  }

  const durationBucketCounts = new Map<string, number>();
  for (const label of CMCD_SESSION_DURATION_BUCKETS) durationBucketCounts.set(label, 0);
  let totalDurationSec = 0;
  const positiveDurations: number[] = [];
  for (const s of sessions.values()) {
    const dur = (s.lastTs - s.firstTs) / 1000;
    totalDurationSec += dur;
    if (dur > 0) positiveDurations.push(dur);
    const label = classifySessionDurationBucket(dur);
    durationBucketCounts.set(label, (durationBucketCounts.get(label) ?? 0) + 1);
  }
  const avgSessionDuration = sessions.size > 0 ? roundTo(totalDurationSec / sessions.size, 1) : 0;
  const session_duration_dist = CMCD_SESSION_DURATION_BUCKETS
    .filter((bucket) => durationBucketCounts.get(bucket))
    .map((bucket) => {
      const count = durationBucketCounts.get(bucket) ?? 0;
      return {
        bucket,
        duration_bucket: bucket,
        session_count: count,
        sessions: count,
        pct: sessions.size > 0 ? roundTo((count * 100) / sessions.size, 2) : 0,
      };
    });

  const top_content = !has("hasCid")
    ? []
    : [...contents.entries()]
        .map(([content_id, c]) => {
          const sess = Math.round(c.sidHll.count());
          const rebSess = Math.round(c.rebufferSidHll.count());
          const brAvg = hasOt ? (c.vBrN > 0 ? c.vBrSum / c.vBrN : null) : (c.brN > 0 ? c.brSum / c.brN : null);
          const blAvg = hasOt ? (c.vBlN > 0 ? c.vBlSum / c.vBlN : null) : (c.blN > 0 ? c.blSum / c.blN : null);
          return {
            content_id,
            sessions: sess,
            session_count: sess,
            requests: c.reqs,
            avg_bitrate_kbps: roundTo(brAvg ?? 0, 1),
            avg_bitrate: brAvg,
            avg_buffer_length: blAvg,
            rebuffer_rate: hasBsEffective && sess > 0 ? (rebSess * 100) / sess : null,
          };
        })
        .sort((a, b) => b.sessions - a.sessions || a.content_id.localeCompare(b.content_id))
        .slice(0, topN);

  const rebuffer_by_country = !hasBsEffective
    ? []
    : [...countries.entries()]
        .map(([country, c]) => {
          const sess = Math.round(c.sidHll.count());
          const rebSess = Math.round(c.rebufferSidHll.count());
          const mtpHist = hasOt ? c.vMtpHist : c.mtpHist;
          return {
            country,
            session_count: sess,
            median_throughput: histQuantile(mtpHist, 0.5),
            sessions: sess,
            requests: c.reqs,
            rebuffer_events: c.rebufferEvents,
            rebuffer_rate: hasBsEffective && sess > 0 ? (rebSess * 100) / sess : null,
          };
        })
        .filter((row) => row.sessions > 0)
        .sort((a, b) => b.sessions - a.sessions || a.country.localeCompare(b.country))
        .slice(0, topN);

  await asnNamesReady;
  const rebuffer_by_asn = !hasBsEffective
    ? []
    : [...asns.entries()]
        .map(([asn, a]) => {
          const sess = Math.round(a.sidHll.count());
          const rebSess = Math.round(a.rebufferSidHll.count());
          const rawName = (a.label ?? "").trim();
          const label = formatAsnDisplayLabel(asn, rawName);
          const as_name = rawName && !/^AS\d+$/i.test(rawName) ? rawName : label;
          const brAvg = hasOt ? (a.vBrN > 0 ? a.vBrSum / a.vBrN : null) : (a.brN > 0 ? a.brSum / a.brN : null);
          const blAvg = hasOt ? (a.vBlN > 0 ? a.vBlSum / a.vBlN : null) : (a.blN > 0 ? a.blSum / a.blN : null);
          return {
            asn,
            as_name,
            label,
            session_count: sess,
            avg_bitrate: brAvg,
            avg_buffer_length: blAvg,
            sessions: sess,
            requests: a.reqs,
            rebuffer_events: a.rebufferEvents,
            rebuffer_rate: hasBsEffective && sess > 0 ? (rebSess * 100) / sess : null,
            avg_throughput_kbps: roundTo(a.mtpN > 0 ? a.mtpSum / a.mtpN : 0, 1),
          };
        })
        .filter((row) => row.sessions > 0)
        .sort((a, b) => b.sessions - a.sessions || a.asn - b.asn)
        .slice(0, topN);

  const object_type_dist = [...ots.entries()]
    .map(([object_type, reqs]) => ({
      object_type,
      request_count: reqs,
      requests: reqs,
      pct: totalRequests > 0 ? roundTo((reqs * 100) / totalRequests, 2) : 0,
    }))
    .sort((a, b) => b.requests - a.requests || a.object_type.localeCompare(b.object_type));

  const streaming_format_dist = [...sfs.entries()]
    .map(([format, s]) => ({
      format,
      streaming_format: format,
      session_count: Math.round(s.sidHll.count()),
      requests: s.reqs,
      pct: totalRequests > 0 ? roundTo((s.reqs * 100) / totalRequests, 2) : 0,
    }))
    .sort((a, b) => b.session_count - a.session_count || a.format.localeCompare(b.format));

  return {
    available: true,
    has_data: true,
    overview: {
      active_sessions: activeSessions,
      peak_viewers: peakViewers,
      avg_session_duration: positiveDurations.length
        ? Math.round(positiveDurations.reduce((sum, d) => sum + d, 0) / positiveDurations.length)
        : null,
      rebuffer_rate: hasBsEffective && activeSessions ? roundTo((rebufferSessions * 100) / activeSessions, 2) : null,
      avg_bitrate: hasOt ? (overall.vBrN > 0 ? overall.vBrSum / overall.vBrN : null) : (overall.brN > 0 ? overall.brSum / overall.brN : null),
      avg_buffer_length: hasOt ? (overall.vBlN > 0 ? overall.vBlSum / overall.vBlN : null) : (overall.blN > 0 ? overall.blSum / overall.blN : null),
      median_throughput: histQuantile(hasOt ? overall.vMtpHist : overall.mtpHist, 0.5),
    },
    summary: {
      active_sessions: activeSessions,
      total_requests: totalRequests,
      rebuffer_events: rebufferEvents,
      rebuffer_rate: rebufferRate,
      avg_bitrate_kbps: roundTo(avgBitrate, 1),
      avg_buffer_ms: roundTo(overall.blN > 0 ? overall.blSum / overall.blN : 0, 1),
      avg_throughput_kbps: roundTo(overall.mtpN > 0 ? overall.mtpSum / overall.mtpN : 0, 1),
      peak_viewers: peakViewers,
      avg_session_duration: avgSessionDuration,
      utilization_ratio: utilizationRatio,
    },
    ...padCmcdTimeseries(
      {
        sessions_ts,
        buffer_health_ts: has("hasBl") ? buffer_health_ts : [],
        bitrate_ts: has("hasBr") ? bitrate_ts : [],
        throughput_ts: has("hasMtp") ? throughput_ts : [],
        startup_ts: hasSu ? startup_ts : [],
      },
      params.start_time,
      params.end_time,
      bucketMs,
    ),
    top_content,
    rebuffer_by_country,
    rebuffer_by_asn,
    object_type_dist,
    streaming_format_dist,
    session_duration_dist,
    bitrate_ladder: [],
    stream_type_dist: [],
  };
}
