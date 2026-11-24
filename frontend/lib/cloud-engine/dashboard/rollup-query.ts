import { parquetMetadata } from "hyparquet";
import type { FileSource } from "./engine";
import { MissingCloudFileError } from "./file-errors";
import { Histogram } from "./histogram";
import { fetchManifestWithSummaries, filesOverlapping, type ManifestFile } from "./manifest";
import { projectedReader } from "./projected-read";
import {
  readSummaryColumns, readSummaryProjection, type SummaryProjectionSource, type SummarySpec,
} from "./summary-projection";
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
  count: number;
  errors: number;
  status5xx: number;
  cacheHits: number;
  hitRequests: number;
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
  /** AS name for `asn` values (the smallest seen), or DMA metro code string for `series5/city`. */
  label: string | null;
  hists: RollupHists;
  /** Clone-safe serialized `hists.elapsed` used when `insightsBaselineRollup` crosses a worker boundary. */
  elapsedHist?: Uint8Array | null;
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
  /**
   * `series1` rows, like `series` per minute: `origin` only. Window summaries and hour bundles
   * carry them, day bundles don't: check `present` (`series1/origin`) before reading them.
   */
  series1?: Map<string, Map<string, Map<number, RollupAgg>>>;
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
  /**
   * The groups decoded, when a spec limited them (every group otherwise).
   * Outside production, reading any other group throws rather than reading
   * as "no rows" or "summary predates rows"; columns the spec left out read
   * as NaN sums, null labels and null histograms.
   */
  loaded?: ReadonlySet<string>;
  /** `value` rows: field → value → UTC day start ms → sums (absent before any value row). */
  value?: Map<string, Map<string, Map<number, RollupValue>>>;
  /** `value` `dim_totals` counts: field → requests over every value, kept or cut. */
  valueTotals?: Map<string, number>;
  /** `bins` `dim_totals` counts: field → requests (e.g. `verified_bot`). */
  binsTotals?: Map<string, number>;
};

/**
 * One /fastly-value `value` row: requests, plus per field the body bytes over rows with a body
 * (`cache`, `io`), elapsed µs over rows with one (`cache`), Image Optimizer bytes (`io_both`)
 * and input/output ratios over rows with output bytes (`io_pair`).
 */
export type RollupValue = {
  requests: number;
  respBytes: number;
  bytesCount: number;
  elapsedSum: number;
  elapsedCount: number;
  ioIn: number;
  ioOut: number;
  ratioSum: number;
  ratioCount: number;
};

const VALUE_COLUMNS = {
  requests: "requests",
  respBytes: "resp_bytes_sum",
  bytesCount: "count",
  elapsedSum: "elapsed_sum",
  elapsedCount: "elapsed_count",
  ioIn: "io_in_sum",
  ioOut: "io_out_sum",
  ratioSum: "ratio_sum",
  ratioCount: "ratio_count",
} as const;
const VALUE_KEYS = Object.keys(VALUE_COLUMNS) as (keyof RollupValue)[];

function addValue(
  into: Map<string, Map<string, Map<number, RollupValue>>>,
  field: string,
  value: string,
  day: number,
  sums: RollupValue,
) {
  const values = into.get(field) ?? new Map<string, Map<number, RollupValue>>();
  into.set(field, values);
  const days = values.get(value) ?? new Map<number, RollupValue>();
  values.set(value, days);
  days.set(day, addSums(days.get(day), sums, VALUE_KEYS));
}

/** One `sample` row: a raw row with a TTFB, picked by its hashed `key`. */
export type RollupSample = {
  key: bigint;
  tsMs: number;
  cache: string | null;
  ttfbS: number;
  elapsedUs: number | null;
  rttUs: number | null;
};

/** Groups whose rows `decodeRollup` keeps in `Rollup.groups`. */
const KEYED_GROUPS = new Set(["transport", "origin", "urls", "quality", "ins_url", "ins_ip", "ins_lat", "ins_asn", "shield_routes", "filtered_rollups"]);

/**
 * One field of a `transport`, `origin`, `urls` or `quality` group, or null when
 * some merged summary predates it (raw must answer then).
 */
export function rollupGroup(rollup: Rollup, group: string, field: string): Map<string, RollupAgg> | null {
  assertLoaded(rollup.loaded, group);
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
  threatsBlocked: number;
};

const OVERVIEW_COLUMNS = {
  requests: "requests",
  totalBandwidth: "total_bandwidth_bytes",
  hitElapsedSum: "hit_elapsed_sum",
  hitElapsedCount: "hit_elapsed_count",
  missElapsedSum: "miss_elapsed_sum",
  missElapsedCount: "miss_elapsed_count",
  threatsBlocked: "threats_blocked",
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
  count: "count",
  errors: "errors",
  status5xx: "status_5xx",
  cacheHits: "cache_hits",
  hitRequests: "hit_requests",
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

/** An aggregate of no rows: zero sums, no label, no histograms. */
export function emptyAgg(): RollupAgg {
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

/** Every row of a column a spec left out: NaN. */
const SKIPPED: ArrayLike<unknown> = new Proxy({ length: 0 }, {
  get: (_, property) => (property === "length" ? 0 : NaN),
});

const num = (value: unknown) =>
  typeof value === "bigint" ? Number(value) : typeof value === "number" ? value : 0;

function bucketMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  return null;
}

const GUARDED = process.env.NODE_ENV !== "production";

function assertLoaded(loaded: ReadonlySet<string> | undefined, group: string) {
  if (GUARDED && loaded && !loaded.has(group)) throw new Error(`Summary group ${group} was not loaded`);
}

/** A container of a group the spec did not load: any read throws. */
class UnloadedMap<K, V> extends Map<K, V> {
  constructor(private readonly group: string) {
    super();
  }

  private fail(): never {
    throw new Error(`Summary group ${this.group} was not loaded`);
  }

  override get(): V | undefined {
    return this.fail();
  }

  override has(): boolean {
    return this.fail();
  }

  override get size(): number {
    return this.fail();
  }

  override forEach(): void {
    this.fail();
  }

  override entries(): MapIterator<[K, V]> {
    return this.fail();
  }

  override keys(): MapIterator<K> {
    return this.fail();
  }

  override values(): MapIterator<V> {
    return this.fail();
  }

  override [Symbol.iterator](): MapIterator<[K, V]> {
    return this.fail();
  }
}

/** `group/field` keys: membership throws unless both the group and the group recording them were loaded. */
class GuardedKeys extends Set<string> {
  constructor(values: Iterable<string>, private readonly check: (key: string) => void) {
    super(values);
  }

  override has(key: string): boolean {
    this.check(key);
    return super.has(key);
  }
}

const groupOf = (key: string) => key.slice(0, key.indexOf("/"));

/**
 * Applies `loaded` to `rollup` (see `Rollup.loaded`). Guarded containers
 * structured-clone as plain ones, and the guarded properties are not
 * enumerable, so a posted rollup clones without throwing.
 */
function guard(rollup: Rollup, loaded: ReadonlySet<string> | undefined): Rollup {
  if (!loaded) return rollup;
  rollup.loaded = loaded;
  if (!GUARDED) return rollup;
  if (!loaded.has("dims")) rollup.dims = new UnloadedMap("dims");
  if (!loaded.has("series5")) rollup.series = new UnloadedMap("series5");
  if (!loaded.has("all_fields")) rollup.fields = new UnloadedMap("all_fields");
  rollup.present = new GuardedKeys(rollup.present ?? [], (key) => {
    assertLoaded(loaded, groupOf(key));
    assertLoaded(loaded, "dim_totals");
  });
  rollup.truncated = new GuardedKeys(rollup.truncated, (key) => {
    const group = groupOf(key);
    assertLoaded(loaded, group);
    assertLoaded(loaded, group === "all_fields" ? "field_totals" : "dim_totals");
  });
  const unloaded = (property: keyof Rollup, ...needs: string[]) => {
    if (needs.every((group) => loaded.has(group))) return;
    Object.defineProperty(rollup, property, {
      configurable: true,
      enumerable: false,
      get: () => needs.forEach((group) => assertLoaded(loaded, group)),
    });
  };
  unloaded("waterfall", "overview");
  unloaded("overview", "overview");
  unloaded("assets", "assets");
  unloaded("samples", "sample");
  unloaded("series1", "series1");
  unloaded("value", "value");
  unloaded("valueTotals", "value", "dim_totals");
  unloaded("binsTotals", "dim_totals");
  return rollup;
}

/** A guarded property's value without tripping its guard (undefined when its group was not loaded). */
const own = <K extends keyof Rollup>(rollup: Rollup, property: K): Rollup[K] | undefined => {
  const descriptor = Object.getOwnPropertyDescriptor(rollup, property);
  return descriptor && "value" in descriptor ? descriptor.value : undefined;
};

/**
 * Decodes the rollup groups of a v3 window summary or bundle, or returns null
 * for any other format version so the caller reads raw Parquet instead.
 * `seriesStepMs` is the file's `series5` bucket: an hour for day bundles.
 */
export async function decodeRollup(
  bytes: ArrayBuffer | readonly ArrayBuffer[],
  seriesStepMs: number,
  /** Decode only these groups and columns (`parts` from `readSummaryProjection` may hold more). */
  spec?: SummarySpec,
): Promise<Rollup | null> {
  const parts = Array.isArray(bytes) ? bytes : [bytes as ArrayBuffer];
  const version = parquetMetadata(parts[0]).key_value_metadata?.find(
    (kv) => kv.key === "flp_summary",
  )?.value;
  if (version !== SUMMARY_VERSION) return null;
  const sumNames = Object.values(SUM_COLUMNS);
  const read = (names: string[]) => (spec ? names.filter((name) => spec.columns.has(name)) : names);
  const { rows, columns } = await readSummaryColumns(parts, read([...new Set([
    "bundle",
    "bucket",
    "field",
    "value",
    "truncated",
    "label",
    "count",
    "sample_key",
    "ttfb_sum",
    "elapsed_us",
    ...sumNames,
    ...Object.values(WATERFALL_COLUMNS),
    ...Object.values(OVERVIEW_COLUMNS),
    ...Object.values(ASSET_COLUMNS),
    ...Object.values(VALUE_COLUMNS),
  ])]));
  const binary = await readSummaryColumns(parts, read(Object.values(HIST_COLUMNS)), false);
  // A column the spec left out reads as NaN; one the file lacks (an older summary) as 0.
  const skipped = (name: string) => spec !== undefined && !spec.columns.has(name);
  const col = (name: string): ArrayLike<unknown> => columns.get(name) ?? (skipped(name) ? SKIPPED : []);
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
  const groups = spec?.groups;
  const readAgg = (i: number) => {
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
    if (groups && (!groups.has(String(group[i]))
      || group[i] === "dim_totals" && !groups.has(String(value[i])))) continue;
    switch (group[i]) {
      case "dims": {
        const values = rollup.dims.get(name) ?? new Map<string, RollupAgg>();
        rollup.dims.set(name, values);
        const key = String(value[i] ?? "");
        const agg = values.get(key);
        if (agg) addAgg(agg, readAgg(i));
        else values.set(key, readAgg(i));
        break;
      }
      case "series5":
      case "series1": {
        const t = bucketMs(bucket[i]);
        if (t === null) return null;
        const series = group[i] === "series5" ? rollup.series : (rollup.series1 ??= new Map());
        const values = series.get(name) ?? new Map();
        series.set(name, values);
        const key = String(value[i] ?? "");
        const buckets: Map<number, RollupAgg> = values.get(key) ?? new Map();
        values.set(key, buckets);
        const agg = buckets.get(t);
        if (agg) addAgg(agg, readAgg(i));
        else buckets.set(t, readAgg(i));
        break;
      }
      case "dim_totals":
        rollup.present!.add(`${String(value[i])}/${name}`);
        if (truncated[i] === true) rollup.truncated.add(`${String(value[i])}/${name}`);
        if (value[i] === "value") {
          const totals = (rollup.valueTotals ??= new Map());
          totals.set(name, (totals.get(name) ?? 0) + num(col("requests")[i]));
        } else if (value[i] === "bins") {
          const totals = (rollup.binsTotals ??= new Map());
          totals.set(name, (totals.get(name) ?? 0) + num(col("requests")[i]));
        }
        break;
      case "value": {
        const t = bucketMs(bucket[i]);
        if (t === null) return null;
        const sums = {} as RollupValue;
        for (const key of VALUE_KEYS) sums[key] = num(col(VALUE_COLUMNS[key])[i]);
        addValue((rollup.value ??= new Map()), name, String(value[i] ?? ""), t, sums);
        break;
      }
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
        const elapsed = col("elapsed_us")[i];
        (rollup.samples ??= []).push({
          key: typeof key === "bigint" ? key : BigInt(Math.trunc(num(key))),
          tsMs: t,
          cache: typeof value[i] === "string" ? (value[i] as string) : null,
          ttfbS: num(ttfb),
          elapsedUs: elapsed == null ? null : num(elapsed),
          rttUs: rtt == null ? null : num(rtt),
        });
        break;
      }
      default:
        if (KEYED_GROUPS.has(String(group[i]))) {
          addKeyed(rollup.groups!, String(group[i]), name, String(value[i] ?? ""), readAgg(i), false);
        }
    }
  }
  return guard(rollup, groups);
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

/**
 * Sums rollups into new aggregates; the inputs (often cached) are not changed.
 * With `keys`, only those `group/field` keys (`dims/city`, `series5/overall`,
 * `all_fields/ua`, `ins_url/url`...) are kept, and no other rows.
 */
export function mergeRollups(rollups: readonly Rollup[], keys?: ReadonlySet<string>): Rollup {
  const keep = (key: string) => !keys || keys.has(key);
  const out: Rollup = {
    dims: new Map(),
    series: new Map(),
    seriesStepMs: Math.max(FIVE_MINUTES_MS, ...rollups.map((r) => r.seriesStepMs)),
    fields: new Map(),
    truncated: new Set(rollups.flatMap((r) => [...r.truncated]).filter(keep)),
    groups: new Map(),
    present: new Set(rollups.length ? [...rollups[0].present ?? []].filter(keep) : []),
  };
  const specced = rollups.flatMap((rollup) => (rollup.loaded ? [rollup.loaded] : []));
  const loaded = specced.length
    ? new Set([...specced[0]].filter((group) => specced.every((set) => set.has(group)))) : undefined;
  const has = (rollup: Rollup, group: string) => !rollup.loaded || rollup.loaded.has(group);
  for (const rollup of rollups) {
    for (const key of out.present!) {
      if (!rollup.present || !Set.prototype.has.call(rollup.present, key)) out.present!.delete(key);
    }
    for (const [group, fields] of rollup.groups ?? []) {
      for (const [field, values] of fields) {
        if (!keep(`${group}/${field}`)) continue;
        for (const [value, agg] of values) addKeyed(out.groups!, group, field, value, agg);
      }
    }
    if (keys) {
      mergeKept(out, rollup, keep, has);
      continue;
    }
    const samples = own(rollup, "samples");
    if (samples) (out.samples ??= []).push(...samples);
    for (const [field, values] of own(rollup, "value") ?? []) {
      for (const [value, days] of values) {
        for (const [day, sums] of days) addValue((out.value ??= new Map()), field, value, day, sums);
      }
    }
    for (const [field, n] of own(rollup, "valueTotals") ?? []) {
      const totals = (out.valueTotals ??= new Map());
      totals.set(field, (totals.get(field) ?? 0) + n);
    }
    for (const [field, n] of own(rollup, "binsTotals") ?? []) {
      const totals = (out.binsTotals ??= new Map());
      totals.set(field, (totals.get(field) ?? 0) + n);
    }
    const waterfall = own(rollup, "waterfall");
    if (waterfall) out.waterfall = addWaterfall(out.waterfall, waterfall);
    const overview = own(rollup, "overview");
    if (overview) out.overview = addSums(out.overview, overview, OVERVIEW_KEYS);
    const assets = own(rollup, "assets");
    if (assets) {
      const into = (out.assets ??= new Map());
      for (const [key, asset] of assets) into.set(key, addSums(into.get(key), asset, ASSET_KEYS));
    }
    mergeKept(out, rollup, keep, has);
  }
  return guard(out, loaded);
}

function mergeKept(
  out: Rollup,
  rollup: Rollup,
  keep: (key: string) => boolean,
  has: (rollup: Rollup, group: string) => boolean,
) {
  if (has(rollup, "all_fields")) {
    addFieldCounts(out.fields, new Map([...rollup.fields].filter(([field]) => keep(`all_fields/${field}`))));
  }
  for (const [dim, values] of has(rollup, "dims") ? rollup.dims : []) {
    if (!keep(`dims/${dim}`)) continue;
    const target = out.dims.get(dim) ?? new Map<string, RollupAgg>();
    out.dims.set(dim, target);
    for (const [value, agg] of values) {
      const into = target.get(value) ?? emptyAgg();
      target.set(value, into);
      addAgg(into, agg);
    }
  }
  if (has(rollup, "series5")) addSeries(out.series, rollup.series, (field) => keep(`series5/${field}`));
  const series1 = own(rollup, "series1");
  if (series1) addSeries((out.series1 ??= new Map()), series1, (field) => keep(`series1/${field}`));
}

function addSeries(
  out: Map<string, Map<string, Map<number, RollupAgg>>>,
  series: Map<string, Map<string, Map<number, RollupAgg>>>,
  keep: (field: string) => boolean,
) {
  for (const [field, values] of series) {
    if (!keep(field)) continue;
    const target = out.get(field) ?? new Map();
    out.set(field, target);
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

/**
 * Sums one `series5` (or per-minute `series1`) field into `bucketMs` buckets aligned to the
 * epoch, as the raw queries' `floor(ts_ms / bucketMs)` does, or returns null when `bucketMs`
 * is not a whole number of the rollup's steps (raw rows must answer then).
 */
export function rebucketSeries(
  rollup: Rollup,
  field: string,
  bucketMs: number,
  group: "series5" | "series1" = "series5",
): Map<string, Map<number, RollupAgg>> | null {
  const minutes = group === "series1";
  if (bucketMs % (minutes ? 60_000 : rollup.seriesStepMs) !== 0) return null;
  const out = new Map<string, Map<number, RollupAgg>>();
  for (const [value, buckets] of (minutes ? rollup.series1 : rollup.series)?.get(field) ?? []) {
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
  /** Raw files no summary answers, read whole and clipped to the range (with `residualBytes`). */
  residual?: ManifestFile[];
  /** Start of the earliest raw window the summaries (and residual) answer. */
  dataFromMs: number;
  /** End of the newest raw window the summaries (and residual) answer; newer data is omitted. */
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
 *
 * With `residualBytes`, every raw file no summary answers (one crossing either
 * edge of the range, an open tail or a gap) is planned as `residual` instead,
 * for the caller to roll up from raw rows clipped to the range, so the plan is
 * exact through `toMs`; it is null when their listed sizes sum past the budget.
 * Summaries only claim raw files they hold every row of (by identity and row
 * count), so no row is counted by both.
 */
export function planRollup(
  rawFiles: readonly ManifestFile[],
  summaryFiles: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
  options: { residualBytes?: number } = {},
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
  if (options.residualBytes !== undefined) {
    const residual = plan.raw;
    if (!plan.summaries.length && !residual.length) return null;
    const bytes = residual.reduce((sum, file) => sum + (file.size >= 0 ? file.size : Infinity), 0);
    if (bytes > options.residualBytes) return null;
    const spans = [
      ...plan.summaries.flatMap((use) => use.covers).map((f) => [Date.parse(f.start), Date.parse(f.end)]),
      ...residual.map((f) => [Math.max(fromMs, Date.parse(f.start)), Math.min(toMs, Date.parse(f.end))]),
    ];
    return {
      files: plan.summaries,
      residual,
      dataFromMs: Math.min(...spans.map(([start]) => start)),
      dataThroughMs: Math.max(...spans.map(([, end]) => end)),
    };
  }
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
  /** Earliest-start to latest-end ms across files that lacked a `group/field` present in other files. */
  missingRanges?: Map<string, { startMs: number; endMs: number }>;
};

type RollupSource = Pick<FileSource, "fetchJson"> & SummaryProjectionSource;

/**
 * Rolls up the raw rows no summary answers (see `planRollup`), so a range
 * whose edges cut through bundled hours stays exact. The merged result holds
 * only `keys`, the `group/field` keys the residual computes exactly.
 */
export type RollupResidual = {
  /** Distinguishes cached results. */
  id: string;
  /** The raw columns `accumulate` reads. */
  columns: readonly string[];
  /** Largest summed listed size of residual raw files worth reading; more answers null. */
  maxBytes: number;
  keys: ReadonlySet<string>;
  /** Adds decoded raw files' rows with a timestamp in [fromMs, toMs), one file at a time. */
  accumulate(fromMs: number, toMs: number): {
    add(file: ArrayBuffer): Promise<void>;
    finish(): Rollup;
  };
};

const DECODED_LIMIT = 64;
const RESIDUAL_READ_CONCURRENCY = 4;
let rollupEpoch = 0;
type SharedRollup = {
  controller: AbortController;
  waiters: number;
  settled: boolean;
  promise: Promise<RollupResult | null>;
};
const decoded = new Map<string, Promise<Rollup | null>>();
const inflight = new Map<string, SharedRollup>();
const activeRollupControllers = new Set<AbortController>();

export function clearRollupQueryCache(): void {
  rollupEpoch += 1;
  for (const controller of activeRollupControllers) {
    controller.abort(new DOMException("The operation was aborted.", "AbortError"));
  }
  activeRollupControllers.clear();
  for (const shared of inflight.values()) {
    shared.controller.abort(
      new DOMException("The operation was aborted.", "AbortError"),
    );
  }
  inflight.clear();
  decoded.clear();
}

function decodeCached(
  source: RollupSource,
  file: ManifestFile,
  spec: SummarySpec | undefined,
  signal: AbortSignal,
  callRollupEpoch: number,
): Promise<Rollup | null> {
  signal.throwIfAborted();
  if (callRollupEpoch !== rollupEpoch) {
    return Promise.reject(
      new DOMException("The operation was aborted.", "AbortError"),
    );
  }
  const key = `${source.cacheScope ?? "default"}|${file.key}|${spec?.id ?? "whole"}`;
  const hit = decoded.get(key);
  if (hit) {
    decoded.delete(key);
    decoded.set(key, hit);
    return hit;
  }
  const step = file.key.includes("/day_bundle=") ? HOUR_MS : FIVE_MINUTES_MS;
  const pending = (
    spec
      ? readSummaryProjection(source, file, spec, signal).then(({ parts }) => {
          signal.throwIfAborted();
          if (callRollupEpoch !== rollupEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          return decodeRollup(parts, step, spec);
        })
      : source.fetchFile(file.key, signal).then(({ bytes }) => {
          signal.throwIfAborted();
          if (callRollupEpoch !== rollupEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          return decodeRollup(bytes, step);
        })
  ).then((rollup) => {
    if (callRollupEpoch !== rollupEpoch || signal.aborted) {
      if (decoded.get(key) === pending) decoded.delete(key);
      throw (
        signal.reason ??
        new DOMException("The operation was aborted.", "AbortError")
      );
    }
    return rollup;
  });
  pending.catch(() => {
    if (decoded.get(key) === pending) decoded.delete(key);
  });
  if (callRollupEpoch === rollupEpoch && !signal.aborted) {
    decoded.set(key, pending);
    while (decoded.size > DECODED_LIMIT) {
      decoded.delete(decoded.keys().next().value!);
    }
  }
  return pending;
}

/**
 * Merges the v3 summaries that answer [fromMs, toMs) for the unfiltered data,
 * or returns null when raw Parquet must answer (no listing, a gap, a long open
 * tail or a file that is not v3). Concurrent calls for one range share a read,
 * and decoded files are kept for later ranges. With `spec`, only its groups
 * and columns are read and decoded (see `Rollup.loaded`).
 */
export function queryRollup(
  source: RollupSource,
  range: {
    fromMs: number;
    toMs: number;
    signal?: AbortSignal;
    spec?: SummarySpec;
    residual?: RollupResidual;
  },
): Promise<RollupResult | null> {
  const { signal } = range;
  signal?.throwIfAborted();
  const key = `${source.cacheScope ?? "default"}|${range.fromMs}|${range.toMs}|${range.spec?.id ?? "whole"}|${range.residual?.id ?? ""}`;
  let shared = inflight.get(key);
  if (!shared || shared.settled || shared.controller.signal.aborted) {
    const controller = new AbortController();
    const callRollupEpoch = rollupEpoch;
    activeRollupControllers.add(controller);
    const created: SharedRollup = {
      controller,
      waiters: 0,
      settled: false,
      promise: undefined as unknown as Promise<RollupResult | null>,
    };
    created.promise = runRollup(
      source,
      range.fromMs,
      range.toMs,
      range.spec,
      range.residual,
      controller.signal,
      callRollupEpoch,
    ).finally(() => {
      created.settled = true;
      activeRollupControllers.delete(controller);
      if (inflight.get(key) === created) inflight.delete(key);
    });
    inflight.set(key, created);
    shared = created;
  }
  const active = shared;
  active.waiters += 1;
  let left = false;
  const leave = (abortReason?: unknown) => {
    if (left) return;
    left = true;
    active.waiters -= 1;
    if (active.waiters === 0 && !active.settled) {
      active.controller.abort(
        abortReason ??
          new DOMException("The operation was aborted.", "AbortError"),
      );
    }
  };
  if (!signal) {
    return active.promise.finally(() => leave());
  }
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      leave(signal.reason);
      reject(signal.reason);
    };
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
    active.promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        leave();
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        leave();
        reject(error);
      },
    );
  });
}

async function runRollup(
  source: RollupSource,
  fromMs: number,
  toMs: number,
  spec: SummarySpec | undefined,
  residual: RollupResidual | undefined,
  signal: AbortSignal,
  callRollupEpoch: number,
): Promise<RollupResult | null> {
  const assertRollupActive = () => {
    signal.throwIfAborted();
    if (callRollupEpoch !== rollupEpoch) {
      throw new DOMException("The operation was aborted.", "AbortError");
    }
  };
  assertRollupActive();
  const from = new Date(fromMs).toISOString();
  const to = new Date(toMs).toISOString();
  const { raw, summaries } = await fetchManifestWithSummaries(
    source.fetchJson,
    from,
    to,
    signal,
  );
  assertRollupActive();
  if (!raw.complete || !summaries.complete) return null;
  // A listed summary whose object is gone is planned around like an unlisted
  // one: its raw files fall to other summaries or the residual, so it stays exact.
  const missing = new Set<string>();
  let plan: RollupPlan | null;
  let decodedParts: (Rollup | null)[];
  let residualPart: Rollup | null;
  for (;;) {
    plan = planRollup(
      raw.files,
      missing.size ? summaries.files.filter((file) => !missing.has(file.key)) : summaries.files,
      fromMs,
      toMs,
      residual ? { residualBytes: residual.maxBytes } : {},
    );
    if (!plan) return null;
    try {
      [decodedParts, residualPart] = await Promise.all([
        Promise.all(
          plan.files.map((use) =>
            decodeCached(source, use.file, spec, signal, callRollupEpoch),
          ),
        ),
        residual && plan.residual?.length
          ? readResidual(source, residual, plan.residual, fromMs, toMs, signal, assertRollupActive)
          : null,
      ]);
    } catch (error) {
      assertRollupActive();
      if (!(error instanceof MissingCloudFileError) || missing.has(error.key)
        || !plan.files.some((use) => use.file.key === error.key)) throw error;
      console.warn(`flp: summary ${error.key} is listed but missing; planning without it`);
      missing.add(error.key);
      continue;
    }
    break;
  }
  assertRollupActive();
  if (decodedParts.some((part) => part === null)) return null;
  const parts = [...(decodedParts as Rollup[]), ...(residualPart ? [residualPart] : [])];
  const spans = [
    ...plan.files.map((use) => (use.covers.length ? use.covers : [use.file])),
    ...(residualPart ? [plan.residual!] : []),
  ];
  const merged = mergeRollups(parts, residual?.keys);
  const seenKeys = new Set<string>();
  for (const part of parts) {
    for (const key of part.present ?? []) {
      if (!residual || residual.keys.has(key)) seenKeys.add(key);
    }
  }
  const missingRanges = new Map<string, { startMs: number; endMs: number }>();
  for (const key of seenKeys) {
    if (merged.present && Set.prototype.has.call(merged.present, key)) continue;
    let startMs = Infinity;
    let endMs = -Infinity;
    for (let i = 0; i < parts.length; i++) {
      const present = parts[i].present;
      if (present && Set.prototype.has.call(present, key)) continue;
      for (const f of spans[i]) {
        startMs = Math.min(startMs, Date.parse(f.start));
        endMs = Math.max(endMs, Date.parse(f.end));
      }
    }
    if (startMs < endMs) missingRanges.set(key, { startMs, endMs });
  }
  return {
    rollup: merged,
    dataFromMs: plan.dataFromMs,
    dataThroughMs: plan.dataThroughMs,
    missingRanges,
  };
}

/**
 * Reads residual raw files as projections of `residual.columns` (row groups
 * outside the range left out, the projection kept as a derived file), or
 * whole without range reads, and rolls up their rows in the range.
 */
async function readResidual(
  source: RollupSource,
  residual: RollupResidual,
  files: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
  signal: AbortSignal,
  assertActive: () => void,
): Promise<Rollup> {
  const project = source.fetchRange
    ? projectedReader(source, residual.columns, signal, { fromMs, toMs })
    : null;
  const into = residual.accumulate(fromMs, toMs);
  let added = Promise.resolve();
  let next = 0;
  const worker = async () => {
    while (next < files.length) {
      const file = files[next++];
      assertActive();
      const bytes = project && file.size > 0
        ? (await (await project(file)).read()).bytes
        : (await source.fetchFile(file.key, signal)).bytes;
      assertActive();
      // Decoding one file at a time bounds the decoded columns held at once.
      const add = added.then(() => {
        assertActive();
        return into.add(bytes);
      });
      added = add;
      await add;
      assertActive();
    }
  };
  await Promise.all(
    Array.from(
      { length: Math.min(RESIDUAL_READ_CONCURRENCY, files.length) },
      worker,
    ),
  );
  assertActive();
  return into.finish();
}
