import { parquetMetadata } from "hyparquet";
import type { FileSource } from "./engine";
import { Histogram } from "./histogram";
import { fetchManifest, filesOverlapping, type ManifestFile } from "./manifest";
import {
  SUMMARY_VERSION,
  planCoarsestFirst,
  type SummaryUse,
} from "./summaries";

const FIVE_MINUTES_MS = 300_000;
const HOUR_MS = 3_600_000;

/** Latency histograms per row, in microseconds (summary.rs `HIST_COLS`). */
export type RollupHists = {
  ttfb: Histogram | null;
  elapsed: Histogram | null;
  rtt: Histogram | null;
  ottfb: Histogram | null;
  ottlb: Histogram | null;
};

/**
 * Exact sums for one dimension value or series bucket, as summary.rs writes
 * them: `status5xx` is 500-599, `errors` is status >= 400 or 0, `cacheHits`
 * counts HIT and HIT-STALE, `rtt*` is the coalesced client RTT in µs and
 * `ploss*` the coalesced packet loss. Series rows carry no `errors`,
 * `cacheHits`, latency, origin or TCP sums beyond RTT and loss.
 */
export type RollupAgg = {
  requests: number;
  errors: number;
  status5xx: number;
  cacheHits: number;
  respBytes: number;
  latUsSum: number;
  latUsCount: number;
  ost5xx: number;
  ostTotal: number;
  rttSum: number;
  rttCount: number;
  plossSum: number;
  plossCount: number;
  cwndSum: number;
  cwndCount: number;
  rttvarSum: number;
  rttvarCount: number;
  retrans: number;
  latSum: number;
  lonSum: number;
  geoCount: number;
  /** AS name for `asn` values (the smallest seen). */
  label: string | null;
  hists: RollupHists;
};

export type Rollup = {
  /** `dims` rows: dimension (pop, oip, edge, backend, country, region, asn, city, proto) → value → sums. */
  dims: Map<string, Map<string, RollupAgg>>;
  /** `series5` rows: field (overall, proto, asn) → value → bucket start ms → sums. */
  series: Map<string, Map<string, Map<number, RollupAgg>>>;
  /** The coarsest series bucket merged: 5 minutes, or an hour once a day bundle is in. */
  seriesStepMs: number;
  /** `all_fields` rows: field → value → row count; null values are not counted, '' is. */
  fields: Map<string, Map<string, number>>;
  /**
   * `group/field` (e.g. `dims/asn`, `series5/asn`, `all_fields/ua`) where some source
   * cut values to its top K: kept counts there are lower bounds and cut values are absent.
   */
  truncated: Set<string>;
  /** `overview` waterfall sums in ms over rows with both TTFB and elapsed (absent before any overview row). */
  waterfall?: RollupWaterfall;
};

export type RollupWaterfall = {
  count: number;
  edgeProcessing: number;
  originWait: number;
  originDownload: number;
  clientDownload: number;
};

const WATERFALL_COLUMNS = {
  count: "waterfall_count",
  edgeProcessing: "edge_processing_sum",
  originWait: "origin_wait_sum",
  originDownload: "origin_download_sum",
  clientDownload: "client_download_sum",
} as const;
const WATERFALL_KEYS = Object.keys(WATERFALL_COLUMNS) as (keyof RollupWaterfall)[];

function addWaterfall(into: RollupWaterfall | undefined, from: RollupWaterfall): RollupWaterfall {
  const out = { ...(into ?? from) };
  if (into) for (const key of WATERFALL_KEYS) out[key] += from[key];
  return out;
}

function addFieldCounts(
  into: Map<string, Map<string, number>>,
  from: Map<string, Map<string, number>>,
) {
  for (const [field, values] of from) {
    const target = into.get(field) ?? new Map<string, number>();
    into.set(field, target);
    for (const [value, count] of values) target.set(value, (target.get(value) ?? 0) + count);
  }
}

const SUM_COLUMNS = {
  requests: "requests",
  errors: "errors",
  status5xx: "status_5xx",
  cacheHits: "cache_hits",
  respBytes: "resp_bytes_sum",
  latUsSum: "lat_us_sum",
  latUsCount: "lat_us_count",
  ost5xx: "ost_5xx_count",
  ostTotal: "ost_total_count",
  rttSum: "rtt_sum",
  rttCount: "rtt_count",
  plossSum: "ploss_sum",
  plossCount: "ploss_count",
  cwndSum: "cwnd_sum",
  cwndCount: "cwnd_count",
  rttvarSum: "rttvar_sum",
  rttvarCount: "rttvar_count",
  retrans: "retrans_count",
  latSum: "lat_sum",
  lonSum: "lon_sum",
  geoCount: "geo_count",
} as const;
type SumKey = keyof typeof SUM_COLUMNS;
const SUM_KEYS = Object.keys(SUM_COLUMNS) as SumKey[];

const HIST_COLUMNS = {
  ttfb: "ttfb_hist",
  elapsed: "elapsed_hist",
  rtt: "rtt_hist",
  ottfb: "ottfb_hist",
  ottlb: "ottlb_hist",
} as const;
const HIST_KEYS = Object.keys(HIST_COLUMNS) as (keyof RollupHists)[];

function emptyAgg(): RollupAgg {
  const agg = { label: null, hists: {} } as unknown as RollupAgg;
  for (const key of SUM_KEYS) agg[key] = 0;
  for (const key of HIST_KEYS) agg.hists[key] = null;
  return agg;
}

/** Adds `from` into `into`; histograms are copied, never shared. */
function addAgg(into: RollupAgg, from: RollupAgg) {
  for (const key of SUM_KEYS) into[key] += from[key];
  if (from.label !== null && (into.label === null || from.label < into.label)) {
    into.label = from.label;
  }
  for (const key of HIST_KEYS) {
    const h = from.hists[key];
    if (!h) continue;
    const target = (into.hists[key] ??= new Histogram());
    target.merge(h);
  }
}

const num = (value: unknown) =>
  typeof value === "bigint" ? Number(value) : typeof value === "number" ? value : 0;

function bucketMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  return null;
}

/**
 * Decodes the rollup groups of a v3 window summary or bundle, or returns null
 * for any other format version so the caller reads raw Parquet instead.
 * `seriesStepMs` is the file's `series5` bucket: an hour for day bundles.
 */
export async function decodeRollup(
  bytes: ArrayBuffer,
  seriesStepMs: number,
): Promise<Rollup | null> {
  const version = parquetMetadata(bytes).key_value_metadata?.find(
    (kv) => kv.key === "flp_summary",
  )?.value;
  if (version !== SUMMARY_VERSION) return null;
  const { readColumns } = await import("./parquet");
  const sumNames = Object.values(SUM_COLUMNS);
  const { rows, columns } = await readColumns(bytes, [
    "bundle",
    "bucket",
    "field",
    "value",
    "truncated",
    "label",
    "count",
    ...sumNames,
    ...Object.values(WATERFALL_COLUMNS),
  ]);
  const binary = await readColumns(bytes, Object.values(HIST_COLUMNS), false);
  const col = (name: string) => columns.get(name) ?? [];
  const [group, bucket, field, value, truncated, label, count] = [
    col("bundle"),
    col("bucket"),
    col("field"),
    col("value"),
    col("truncated"),
    col("label"),
    col("count"),
  ];
  const sums = SUM_KEYS.map((key) => [key, col(SUM_COLUMNS[key])] as const);
  const hists = HIST_KEYS.map(
    (key) => [key, binary.columns.get(HIST_COLUMNS[key]) ?? []] as const,
  );
  const rollup: Rollup = {
    dims: new Map(),
    series: new Map(),
    seriesStepMs,
    fields: new Map(),
    truncated: new Set(),
  };
  const read = (i: number) => {
    const agg = emptyAgg();
    for (const [key, values] of sums) agg[key] = num(values[i]);
    agg.label = typeof label[i] === "string" ? (label[i] as string) : null;
    for (const [key, values] of hists) {
      const blob = values[i];
      agg.hists[key] = blob instanceof Uint8Array ? Histogram.fromBytes(blob) : null;
    }
    return agg;
  };
  for (let i = 0; i < rows; i++) {
    const name = String(field[i] ?? "");
    switch (group[i]) {
      case "dims": {
        const values = rollup.dims.get(name) ?? new Map<string, RollupAgg>();
        rollup.dims.set(name, values);
        const key = String(value[i] ?? "");
        const agg = values.get(key);
        if (agg) addAgg(agg, read(i));
        else values.set(key, read(i));
        break;
      }
      case "series5": {
        const t = bucketMs(bucket[i]);
        if (t === null) return null;
        const values = rollup.series.get(name) ?? new Map();
        rollup.series.set(name, values);
        const key = String(value[i] ?? "");
        const buckets: Map<number, RollupAgg> = values.get(key) ?? new Map();
        values.set(key, buckets);
        const agg = buckets.get(t);
        if (agg) addAgg(agg, read(i));
        else buckets.set(t, read(i));
        break;
      }
      case "dim_totals":
        if (truncated[i] === true) rollup.truncated.add(`${String(value[i])}/${name}`);
        break;
      case "overview": {
        const sums = {} as RollupWaterfall;
        for (const key of WATERFALL_KEYS) sums[key] = num(col(WATERFALL_COLUMNS[key])[i]);
        rollup.waterfall = addWaterfall(rollup.waterfall, sums);
        break;
      }
      case "all_fields": {
        const values = rollup.fields.get(name) ?? new Map<string, number>();
        rollup.fields.set(name, values);
        const key = String(value[i] ?? "");
        values.set(key, (values.get(key) ?? 0) + num(count[i]));
        break;
      }
      case "field_totals":
        if (truncated[i] === true) rollup.truncated.add(`all_fields/${name}`);
        break;
    }
  }
  return rollup;
}

/** Sums rollups into new aggregates; the inputs (often cached) are not changed. */
export function mergeRollups(rollups: readonly Rollup[]): Rollup {
  const out: Rollup = {
    dims: new Map(),
    series: new Map(),
    seriesStepMs: Math.max(FIVE_MINUTES_MS, ...rollups.map((r) => r.seriesStepMs)),
    fields: new Map(),
    truncated: new Set(rollups.flatMap((r) => [...r.truncated])),
  };
  for (const rollup of rollups) {
    if (rollup.waterfall) out.waterfall = addWaterfall(out.waterfall, rollup.waterfall);
    addFieldCounts(out.fields, rollup.fields);
    for (const [dim, values] of rollup.dims) {
      const target = out.dims.get(dim) ?? new Map<string, RollupAgg>();
      out.dims.set(dim, target);
      for (const [value, agg] of values) {
        const into = target.get(value) ?? emptyAgg();
        target.set(value, into);
        addAgg(into, agg);
      }
    }
    for (const [field, values] of rollup.series) {
      const target = out.series.get(field) ?? new Map();
      out.series.set(field, target);
      for (const [value, buckets] of values) {
        const byBucket: Map<number, RollupAgg> = target.get(value) ?? new Map();
        target.set(value, byBucket);
        for (const [t, agg] of buckets) {
          const into = byBucket.get(t) ?? emptyAgg();
          byBucket.set(t, into);
          addAgg(into, agg);
        }
      }
    }
  }
  return out;
}

/**
 * Sums one `series5` field into `bucketMs` buckets aligned to the epoch, as the
 * raw queries' `floor(ts_ms / bucketMs)` does, or returns null when `bucketMs`
 * is not a whole number of the rollup's steps (raw rows must answer then).
 */
export function rebucketSeries(
  rollup: Rollup,
  field: string,
  bucketMs: number,
): Map<string, Map<number, RollupAgg>> | null {
  if (bucketMs % rollup.seriesStepMs !== 0) return null;
  const out = new Map<string, Map<number, RollupAgg>>();
  for (const [value, buckets] of rollup.series.get(field) ?? []) {
    const target = new Map<number, RollupAgg>();
    out.set(value, target);
    for (const [t, agg] of buckets) {
      const at = Math.floor(t / bucketMs) * bucketMs;
      const into = target.get(at) ?? emptyAgg();
      target.set(at, into);
      addAgg(into, agg);
    }
  }
  return out;
}

/** Sums aggregates into a new one (e.g. a whole series, or several dimension values). */
export function sumAggs(aggs: Iterable<RollupAgg>): RollupAgg {
  const out = emptyAgg();
  for (const agg of aggs) addAgg(out, agg);
  return out;
}

export type RollupPlan = {
  files: SummaryUse[];
  /** Start of the earliest raw window the summaries answer. */
  dataFromMs: number;
  /** End of the newest raw window the summaries answer; newer data is omitted. */
  dataThroughMs: number;
};

/**
 * Picks the summaries that answer [fromMs, toMs), or null when raw Parquet
 * must answer instead. A raw window that starts before the range is trimmed
 * (it is only partly inside); windows newer than every summary are an open
 * tail that is omitted when it spans at most `maxTailMs`. Any other window
 * without a summary is a gap, which a rollup would show as zero, so it is null.
 */
export function planRollup(
  rawFiles: readonly ManifestFile[],
  summaryFiles: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
  maxTailMs = HOUR_MS,
): RollupPlan | null {
  const raw = filesOverlapping([...rawFiles], fromMs, toMs);
  const plan = planCoarsestFirst(raw, summaryFiles, fromMs, toMs);
  if (!plan.summaries.length) return null;
  const covered = plan.summaries.flatMap((use) => use.covers);
  const dataFromMs = Math.min(...covered.map((f) => Date.parse(f.start)));
  const dataThroughMs = Math.max(...covered.map((f) => Date.parse(f.end)));
  let tailEndMs = dataThroughMs;
  for (const file of plan.raw) {
    const start = Date.parse(file.start);
    const end = Date.parse(file.end);
    if (start < fromMs) {
      if (end > dataFromMs) return null;
    } else if (start < dataThroughMs) {
      return null;
    } else {
      tailEndMs = Math.max(tailEndMs, Math.min(end, toMs));
    }
  }
  if (tailEndMs - dataThroughMs > maxTailMs) return null;
  return { files: plan.summaries, dataFromMs, dataThroughMs };
}

export type RollupResult = {
  rollup: Rollup;
  dataFromMs: number;
  dataThroughMs: number;
};

type RollupSource = Pick<FileSource, "fetchJson" | "fetchFile" | "cacheScope">;

const DECODED_LIMIT = 64;
const decoded = new Map<string, Promise<Rollup | null>>();
const inflight = new Map<string, Promise<RollupResult | null>>();

function decodeCached(
  source: RollupSource,
  file: ManifestFile,
): Promise<Rollup | null> {
  const key = `${source.cacheScope ?? "default"}|${file.key}`;
  const hit = decoded.get(key);
  if (hit) {
    decoded.delete(key);
    decoded.set(key, hit);
    return hit;
  }
  const step = file.key.includes("/day_bundle=") ? HOUR_MS : FIVE_MINUTES_MS;
  const pending = source
    .fetchFile(file.key)
    .then(({ bytes }) => decodeRollup(bytes, step));
  pending.catch(() => decoded.delete(key));
  decoded.set(key, pending);
  while (decoded.size > DECODED_LIMIT) {
    decoded.delete(decoded.keys().next().value!);
  }
  return pending;
}

/**
 * Merges the v3 summaries that answer [fromMs, toMs) for the unfiltered data,
 * or returns null when raw Parquet must answer (no listing, a gap, a long open
 * tail or a file that is not v3). Concurrent calls for one range share a read,
 * and decoded files are kept for later ranges.
 */
export function queryRollup(
  source: RollupSource,
  range: { fromMs: number; toMs: number; signal?: AbortSignal },
): Promise<RollupResult | null> {
  const key = `${source.cacheScope ?? "default"}|${range.fromMs}|${range.toMs}`;
  let shared = inflight.get(key);
  if (!shared) {
    // Unsignalled: one caller leaving must not fail the others sharing the read.
    shared = runRollup(source, range.fromMs, range.toMs).finally(() =>
      inflight.delete(key),
    );
    inflight.set(key, shared);
  }
  const { signal } = range;
  if (!signal) return shared;
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    shared.then(resolve, reject).finally(() =>
      signal.removeEventListener("abort", abort),
    );
  });
}

async function runRollup(
  source: RollupSource,
  fromMs: number,
  toMs: number,
): Promise<RollupResult | null> {
  const from = new Date(fromMs).toISOString();
  const to = new Date(toMs).toISOString();
  const [raw, summaries] = await Promise.all([
    fetchManifest(source.fetchJson, from, to),
    fetchManifest(source.fetchJson, from, to, undefined, undefined, "summary"),
  ]);
  if (!raw.complete || !summaries.complete) return null;
  const plan = planRollup(raw.files, summaries.files, fromMs, toMs);
  if (!plan) return null;
  const parts = await Promise.all(
    plan.files.map((use) => decodeCached(source, use.file)),
  );
  if (parts.some((part) => part === null)) return null;
  return {
    rollup: mergeRollups(parts as Rollup[]),
    dataFromMs: plan.dataFromMs,
    dataThroughMs: plan.dataThroughMs,
  };
}
