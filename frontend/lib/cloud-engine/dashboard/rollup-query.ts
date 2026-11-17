import { parquetMetadata } from "hyparquet";
import type { FileSource } from "./engine";
import { Histogram } from "./histogram";
import { fetchManifest, filesOverlapping, type ManifestFile } from "./manifest";
import {
  SUMMARY_VERSION,
  planCoarsestFirst,
  planCompactsFromWindows,
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
  /** Origin bytes (`origin` rows only), not microseconds. */
  obytes: Histogram | null;
  /** `elapsed - ottlb` (`origin` rows only); negative values are underflow. */
  overhead: Histogram | null;
};

/**
 * Exact sums for one dimension value or series bucket, as summary.rs writes
 * them: `status5xx` is 500-599, `errors` is status >= 400 or 0, `cacheHits`
 * counts HIT and HIT-STALE, `rtt*` is the coalesced client RTT in µs and
 * `ploss*` the coalesced packet loss. Series rows carry no `errors`,
 * `cacheHits`, latency, origin or TCP sums beyond RTT and loss (`city` rows add
 * coordinates). The `transport`, `origin`, `urls` and `quality` groups carry
 * their own subsets (see `Rollup.groups`).
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
  missRequests: number;
  passRequests: number;
  deliverySum: number;
  deliveryCount: number;
  elapsedSum: number;
  elapsedCount: number;
  /** `quality` rows: response bits per elapsed µs, over rows with an elapsed above 0. */
  throughputSum: number;
  throughputCount: number;
  /** AS name for `asn` values (the smallest seen). */
  label: string | null;
  hists: RollupHists;
};

export type Rollup = {
  /** `dims` rows: dimension (pop, oip, edge, backend, country, region, asn, city, proto) → value → sums. */
  dims: Map<string, Map<string, RollupAgg>>;
  /**
   * `series5` rows: field (overall, proto, asn, origin, city) → value → bucket start ms → sums.
   * `origin` and `city` are newer than some summaries: check `present` before reading them.
   */
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
  /** `overview` request, byte and exact HIT/MISS elapsed sums (absent before any overview row). */
  overview?: RollupOverview;
  /** `assets` rows: JSON `[asset_type, cache, encoding]` → sums (absent before any assets row). */
  assets?: Map<string, RollupAsset>;
  /** `transport`, `origin`, `urls` and `quality` rows: group → field → value → sums; read them through `rollupGroup`. */
  groups?: Map<string, Map<string, Map<string, RollupAgg>>>;
  /**
   * `group/field` with a `dim_totals` row in every merged file. Summaries written before a
   * group or field existed lack the row, so its data is unknown there, not zero.
   */
  present?: Set<string>;
  /** `sample` rows: up to `SAMPLE_K` rows with a TTFB per file, by `sample_key`. */
  samples?: RollupSample[];
};

/** One `sample` row: a raw row with a TTFB, picked by its hashed `key`. */
export type RollupSample = {
  key: bigint;
  tsMs: number;
  cache: string | null;
  ttfbS: number;
  rttUs: number | null;
};

/** Groups whose rows `decodeRollup` keeps in `Rollup.groups`. */
const KEYED_GROUPS = new Set(["transport", "origin", "urls", "quality"]);

/**
 * One field of a `transport`, `origin`, `urls` or `quality` group, or null when
 * some merged summary predates it (raw must answer then).
 */
export function rollupGroup(rollup: Rollup, group: string, field: string): Map<string, RollupAgg> | null {
  if (!rollup.present?.has(`${group}/${field}`)) return null;
  return rollup.groups?.get(group)?.get(field) ?? new Map();
}

/**
 * `overview` sums: hit elapsed covers cache HIT, HIT-STALE and HIT-CLUSTER exactly
 * and miss elapsed cache MISS exactly, each over rows with an elapsed (µs).
 */
export type RollupOverview = {
  requests: number;
  totalBandwidth: number;
  hitElapsedSum: number;
  hitElapsedCount: number;
  missElapsedSum: number;
  missElapsedCount: number;
};

const OVERVIEW_COLUMNS = {
  requests: "requests",
  totalBandwidth: "total_bandwidth_bytes",
  hitElapsedSum: "hit_elapsed_sum",
  hitElapsedCount: "hit_elapsed_count",
  missElapsedSum: "miss_elapsed_sum",
  missElapsedCount: "miss_elapsed_count",
} as const;
const OVERVIEW_KEYS = Object.keys(OVERVIEW_COLUMNS) as (keyof RollupOverview)[];

/** /assets sums for one asset key: body bytes, TTFB ms, and origin ms over rows with origin time. */
export type RollupAsset = {
  requests: number;
  respBytes: number;
  ttfbSum: number;
  originWaitSum: number;
  originRequests: number;
};

const ASSET_COLUMNS = {
  requests: "requests",
  respBytes: "resp_bytes_sum",
  ttfbSum: "ttfb_sum",
  originWaitSum: "origin_wait_sum",
  originRequests: "origin_requests",
} as const;
const ASSET_KEYS = Object.keys(ASSET_COLUMNS) as (keyof RollupAsset)[];

function addSums<T extends Record<string, number>>(into: T | undefined, from: T, keys: readonly (keyof T)[]): T {
  const out = { ...(into ?? from) };
  if (into) for (const key of keys) (out[key] as number) += from[key];
  return out;
}

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
  missRequests: "miss_requests",
  passRequests: "pass_requests",
  deliverySum: "delivery_rate_sum",
  deliveryCount: "delivery_rate_count",
  elapsedSum: "elapsed_sum",
  elapsedCount: "elapsed_count",
  throughputSum: "throughput_sum",
  throughputCount: "throughput_count",
} as const;
type SumKey = keyof typeof SUM_COLUMNS;
const SUM_KEYS = Object.keys(SUM_COLUMNS) as SumKey[];

const HIST_COLUMNS = {
  ttfb: "ttfb_hist",
  elapsed: "elapsed_hist",
  rtt: "rtt_hist",
  ottfb: "ottfb_hist",
  ottlb: "ottlb_hist",
  obytes: "obytes_hist",
  overhead: "overhead_hist",
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
  const { rows, columns } = await readColumns(bytes, [...new Set([
    "bundle",
    "bucket",
    "field",
    "value",
    "truncated",
    "label",
    "count",
    "sample_key",
    "ttfb_sum",
    ...sumNames,
    ...Object.values(WATERFALL_COLUMNS),
    ...Object.values(OVERVIEW_COLUMNS),
    ...Object.values(ASSET_COLUMNS),
  ])]);
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
    groups: new Map(),
    present: new Set(),
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
        rollup.present!.add(`${String(value[i])}/${name}`);
        if (truncated[i] === true) rollup.truncated.add(`${String(value[i])}/${name}`);
        break;
      case "overview": {
        const sums = {} as RollupWaterfall;
        for (const key of WATERFALL_KEYS) sums[key] = num(col(WATERFALL_COLUMNS[key])[i]);
        rollup.waterfall = addWaterfall(rollup.waterfall, sums);
        const overview = {} as RollupOverview;
        for (const key of OVERVIEW_KEYS) overview[key] = num(col(OVERVIEW_COLUMNS[key])[i]);
        rollup.overview = addSums(rollup.overview, overview, OVERVIEW_KEYS);
        break;
      }
      case "assets": {
        const asset = {} as RollupAsset;
        for (const key of ASSET_KEYS) asset[key] = num(col(ASSET_COLUMNS[key])[i]);
        const assets = (rollup.assets ??= new Map());
        const key = String(value[i] ?? "");
        assets.set(key, addSums(assets.get(key), asset, ASSET_KEYS));
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
      case "sample": {
        const t = bucketMs(bucket[i]);
        const ttfb = col("ttfb_sum")[i];
        const key = col("sample_key")[i];
        if (t === null || ttfb == null || key == null) break;
        const rtt = col("rtt_sum")[i];
        (rollup.samples ??= []).push({
          key: typeof key === "bigint" ? key : BigInt(Math.trunc(num(key))),
          tsMs: t,
          cache: typeof value[i] === "string" ? (value[i] as string) : null,
          ttfbS: num(ttfb),
          rttUs: rtt == null ? null : num(rtt),
        });
        break;
      }
      default:
        if (KEYED_GROUPS.has(String(group[i]))) {
          addKeyed(rollup.groups!, String(group[i]), name, String(value[i] ?? ""), read(i), false);
        }
    }
  }
  return rollup;
}

function addKeyed(
  into: Map<string, Map<string, Map<string, RollupAgg>>>,
  group: string,
  field: string,
  value: string,
  agg: RollupAgg,
  copy = true,
) {
  const fields = into.get(group) ?? new Map<string, Map<string, RollupAgg>>();
  into.set(group, fields);
  const values = fields.get(field) ?? new Map<string, RollupAgg>();
  fields.set(field, values);
  const target = values.get(value);
  if (target) addAgg(target, agg);
  else values.set(value, copy ? sumAggs([agg]) : agg);
}

/** Sums rollups into new aggregates; the inputs (often cached) are not changed. */
export function mergeRollups(rollups: readonly Rollup[]): Rollup {
  const out: Rollup = {
    dims: new Map(),
    series: new Map(),
    seriesStepMs: Math.max(FIVE_MINUTES_MS, ...rollups.map((r) => r.seriesStepMs)),
    fields: new Map(),
    truncated: new Set(rollups.flatMap((r) => [...r.truncated])),
    groups: new Map(),
    present: new Set(rollups.length ? [...rollups[0].present ?? []] : []),
  };
  for (const rollup of rollups) {
    for (const key of out.present!) if (!rollup.present?.has(key)) out.present!.delete(key);
    for (const [group, fields] of rollup.groups ?? []) {
      for (const [field, values] of fields) {
        for (const [value, agg] of values) addKeyed(out.groups!, group, field, value, agg);
      }
    }
    if (rollup.samples) (out.samples ??= []).push(...rollup.samples);
    if (rollup.waterfall) out.waterfall = addWaterfall(out.waterfall, rollup.waterfall);
    if (rollup.overview) out.overview = addSums(out.overview, rollup.overview, OVERVIEW_KEYS);
    if (rollup.assets) {
      const assets = (out.assets ??= new Map());
      for (const [key, asset] of rollup.assets) assets.set(key, addSums(assets.get(key), asset, ASSET_KEYS));
    }
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
 * (it is only partly inside). Compacts of the open hour are answered by their
 * windows' summaries. Plain windows newer than every summary are an open tail
 * that is omitted only when it spans at most one of those windows (one log
 * period, a window listed between the raw and summary listings). Any other
 * file without a summary is a gap, which a rollup would show as zero, so it is
 * null.
 */
export function planRollup(
  rawFiles: readonly ManifestFile[],
  summaryFiles: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
): RollupPlan | null {
  const raw = filesOverlapping([...rawFiles], fromMs, toMs);
  const coarse = planCoarsestFirst(raw, summaryFiles, fromMs, toMs);
  const used = new Set(coarse.summaries.map((use) => use.file.key));
  const compacted = planCompactsFromWindows(
    coarse.raw,
    summaryFiles.filter((file) => !used.has(file.key)),
    fromMs,
    toMs,
  );
  const plan = {
    summaries: [...coarse.summaries, ...compacted.summaries],
    raw: compacted.raw,
  };
  if (!plan.summaries.length) return null;
  const covered = plan.summaries.flatMap((use) => use.covers);
  const dataFromMs = Math.min(...covered.map((f) => Date.parse(f.start)));
  const dataThroughMs = Math.max(...covered.map((f) => Date.parse(f.end)));
  let tailEndMs = dataThroughMs;
  let windowMs = 0;
  for (const file of plan.raw) {
    const start = Date.parse(file.start);
    const end = Date.parse(file.end);
    if (start < fromMs) {
      if (end > dataFromMs) return null;
    } else if (start < dataThroughMs || !file.key.includes("/window=")) {
      return null;
    } else {
      tailEndMs = Math.max(tailEndMs, Math.min(end, toMs));
      windowMs = Math.max(windowMs, end - start);
    }
  }
  if (tailEndMs - dataThroughMs > windowMs) return null;
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
