import { isCdnHit } from "./cdn-cache";
import { getDuckDBEngine, type DuckDBLease } from "./duckdb";
import {
  fetchManifest,
  filesOverlapping,
  type FetchJson,
  type Manifest,
  type ManifestFile,
} from "./manifest";
import {
  compileFilters,
  createValueMatcher,
  isEdgeValue,
  type DashboardFilter,
  type Matcher,
  type RowFilter,
} from "./filters";
import { Hll } from "./hll";
import { readColumns } from "./parquet";
import {
  CARD_FIELDS,
  customFieldsKey,
  decodeSummary,
  planSummaries,
  type CardField,
  type CustomField,
  type DecodedSummary,
  type SummaryUse,
} from "./summaries";

export type Scalar = string | number | boolean;

export type MinuteBucket = {
  t: number;
  requests: number;
  bytes: number;
  s4xx: number;
  s5xx: number;
  hits: number;
  cacheRows: number;
};

export type CardResult = {
  /** Rows with a non-null value; 0 means the column is absent or all null. */
  total: number;
  distinct: number;
  top: { value: Scalar; count: number }[];
  /** Every value, kept only for the country card that feeds the map. */
  all?: { value: Scalar; count: number }[];
  /**
   * Some source summary kept only its top values for this field: `total` is
   * exact, but `top` counts are lower bounds and `distinct` is an estimate.
   */
  approximate?: boolean;
  /**
   * With `approximate`: non-null rows whose values fell outside every kept
   * list. No shown count is low by more than this.
   */
  unattributed?: number;
  /** With `approximate`: `distinct` is an HLL estimate, not a lower bound. */
  distinctEstimated?: boolean;
};

export type FetchStats = {
  listed: number;
  fetched: number;
  cached: number;
  cdnHits: number;
  rows: number;
  bytes: number;
  elapsedMs: number;
  /** Selective HTTP/OPFS scan rows and bytes are not measured; counters are partial. */
  selectiveIOUnmeasured?: boolean;
  /** Set when the run could use summaries: where their listing came from, or why it was not used. */
  summaryListing?: "inline" | "listed" | "incomplete" | "failed";
  /** Summary files planned in place of raw windows. */
  summaryFiles?: number;
};

export type DashboardResult = {
  from: string;
  to: string;
  totalRequests: number;
  hasBytes: boolean;
  buckets: MinuteBucket[];
  cards: Record<string, CardResult>;
  stats: FetchStats;
};

export type FileRollupData = {
  minTs: number;
  maxTs: number;
  totalRequests: number;
  hasBytes: boolean;
  buckets: MinuteBucket[];
  counts: Record<string, [Scalar, number][]>;
  nonNull: Record<string, number>;
};

export type FileRollup = FileRollupData & {
  version: 1;
  key: string;
  rawRows: number;
  byteLength: number;
  edgeRollup?: FileRollupData;
};

/** Aggregates computed elsewhere (DuckDB) for one field. */
export type FieldAggregate = {
  nonNull: number;
  distinct: number;
  entries: [Scalar, number][];
  /** False when `entries` holds only the top values plus ties. */
  complete: boolean;
  approximate?: boolean;
};

export type PartialAggregates = {
  totalRequests: number;
  minTs: number;
  maxTs: number;
  buckets: MinuteBucket[];
  fields: Record<string, FieldAggregate>;
};

export type FileSource = {
  fetchJson: FetchJson;
  openParquet?: (
    file: ManifestFile,
    signal?: AbortSignal,
    variant?: string,
  ) => Promise<import("./duckdb").ParquetAccess>;
  persistFile?: (
    file: ManifestFile,
    signal?: AbortSignal,
    variant?: string,
  ) => Promise<void | { downloaded: boolean }>;
  /** Presence hint only; openParquet must revalidate before pinning the file. */
  hasPersistentFile?: (
    file: ManifestFile,
    signal?: AbortSignal,
    variant?: string,
  ) => Promise<boolean>;
  /**
   * With `transfer`, the caller takes ownership of `bytes` (it may be
   * detached by a transfer), so the source must not keep it in memory.
   * `identity` distinguishes manifest revisions without changing the fetch URL.
   */
  fetchFile: (
    key: string,
    signal?: AbortSignal,
    mode?: { transfer?: boolean; identity?: string },
  ) => Promise<{ bytes: ArrayBuffer; cached: boolean; cdn?: string | null }>;
  /** Bytes `[start, end)` of an immutable file, uncached. */
  fetchRange?: (
    file: ManifestFile,
    start: number,
    end: number,
    signal?: AbortSignal,
  ) => Promise<ArrayBuffer>;
  /** Whether `fetchFile` can serve the bytes without a network request. */
  hasFile?: (key: string) => Promise<boolean>;
  /** `variant` names the custom fields the rollup also counts (`customFieldsKey`). */
  readRollup?: (key: string, variant?: string) => Promise<FileRollup | null>;
  writeRollup?: (
    key: string,
    rollup: FileRollup,
    variant?: string,
  ) => Promise<void>;
  clearMemoryCache?: () => void;
  pruneCache?: (
    manifest: Manifest,
    fromMs: number,
    toMs: number,
  ) => Promise<void>;
  beacon?: (body: Record<string, number>) => Promise<void>;
  cacheScope?: string;
};

/**
 * Work left in a run. `cached` files were answered from stored summaries;
 * `toFetch` must be downloaded and `toDecode` are decoded from local bytes.
 * `done` counts finished files out of `toFetch + toDecode`.
 */
export type RunProgress = {
  phase?: "cache" | "files" | "aggregate";
  cached: number;
  toFetch: number;
  toDecode: number;
  done: number;
  /** Bytes fetched over the network so far. */
  downloadedBytes: number;
  /** File bytes answered from the browser cache, cached rollups or memory. */
  cachedBytes: number;
  /** Files DuckDB read with HTTP range requests; their bytes are in neither total. */
  rangedFiles?: number;
};

export type EngineOptions = {
  from: string;
  to: string;
  filters?: DashboardFilter[];
  edgeOnly?: boolean;
  manifest?: Manifest;
  signal?: AbortSignal;
  onProgress?: (progress: RunProgress) => void;
  concurrency?: number;
  now?: () => number;
  wallNow?: () => number;
  /** Custom card fields counted next to `CARD_FIELDS`. */
  customFields?: readonly CustomField[];
};

// /api/file is latency-bound (~0.55 s per file whatever the width); live
// probes halved wall time from 8 to 16 lanes, and 32 added little.
const FETCH_CONCURRENCY = 16;

export type SearchFieldOptions = {
  from: string;
  to: string;
  column: CardField;
  query: string;
  filters?: DashboardFilter[];
  edgeOnly?: boolean;
  manifest?: Manifest;
  limit?: number;
  signal?: AbortSignal;
  concurrency?: number;
  now?: () => number;
  customFields?: readonly CustomField[];
  serviceId?: string;
};

export const MINUTE_MS = 60_000;
const TOP_N = 10;
/** The country card keeps every value for the map. */
const ALL_VALUES_FIELD = "country";
const NUMERIC_COLLATOR = new Intl.Collator(undefined, { numeric: true });
const BASE_COLUMNS = ["timestamp", "status", "cache", "resp_bytes"];
/** Shorter ranges read raw Parquet, whose rollups answer them exactly. */
const SUMMARY_MIN_RANGE_MS = 3_600_000;

function compareScalars(a: Scalar, b: Scalar): number {
  return NUMERIC_COLLATOR.compare(String(a), String(b));
}

function topEntries(
  counts: ReadonlyMap<Scalar, number>,
  limit: number,
): { value: Scalar; count: number }[] {
  if (counts.size === 0 || limit <= 0) return [];
  if (counts.size <= limit) {
    return [...counts]
      .sort((a, b) => b[1] - a[1] || compareScalars(a[0], b[0]))
      .map(([value, count]) => ({ value, count }));
  }
  if (limit > TOP_N) {
    const heap: { value: Scalar; count: number; order: number }[] = [];
    const compare = (a: (typeof heap)[number], b: (typeof heap)[number]) =>
      a.count - b.count ||
      compareScalars(b.value, a.value) ||
      b.order - a.order;
    let order = 0;
    for (const [value, count] of counts) {
      const entry = { value, count, order: order++ };
      if (heap.length < limit) {
        heap.push(entry);
        let child = heap.length - 1;
        while (child > 0) {
          const parent = (child - 1) >> 1;
          if (compare(heap[parent], heap[child]) <= 0) break;
          const swap = heap[parent];
          heap[parent] = heap[child];
          heap[child] = swap;
          child = parent;
        }
      } else {
        if (compare(entry, heap[0]) <= 0) continue;
        heap[0] = entry;
        let parent = 0;
        while (parent * 2 + 1 < heap.length) {
          let child = parent * 2 + 1;
          if (
            child + 1 < heap.length &&
            compare(heap[child + 1], heap[child]) < 0
          ) {
            child++;
          }
          if (compare(heap[parent], heap[child]) <= 0) break;
          const swap = heap[parent];
          heap[parent] = heap[child];
          heap[child] = swap;
          parent = child;
        }
      }
    }
    return heap
      .sort((a, b) => compare(b, a))
      .map(({ value, count }) => ({ value, count }));
  }
  const top: { value: Scalar; count: number }[] = [];
  for (const [value, count] of counts) {
    if (top.length === limit) {
      const worst = top[limit - 1];
      if (count < worst.count) continue;
      if (count === worst.count && compareScalars(value, worst.value) >= 0) {
        continue;
      }
    }
    let idx = top.length < limit ? top.length : limit - 1;
    while (idx > 0) {
      const prev = top[idx - 1];
      if (
        count > prev.count ||
        (count === prev.count && compareScalars(value, prev.value) < 0)
      ) {
        top[idx] = prev;
        idx--;
      } else {
        break;
      }
    }
    top[idx] = { value, count };
  }
  return top;
}

function toMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function toNumber(value: unknown): number | null {
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  return null;
}

function toScalar(value: unknown): Scalar | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isNaN(value) ? null : value;
  return String(value);
}

export class Aggregator {
  readonly buckets = new Map<number, MinuteBucket>();
  readonly counts = new Map<string, Map<Scalar, number>>();
  readonly nonNull = new Map<string, number>();
  /** Fields answered with top values only; see `mergeAggregates`. */
  private readonly topSummaries = new Map<string, FieldAggregate>();
  /** Fields a merged summary truncated to its top values. */
  private readonly truncated = new Set<string>();
  private ipSketch: Hll | null = null;
  totalRequests = 0;
  hasBytes = false;
  minTs = Infinity;
  maxTs = -Infinity;
  private readonly fromMs: number;
  private readonly toMs: number;
  private readonly rowFilter: RowFilter | null;
  private readonly edgeOnly: boolean;

  constructor(
    from: string | number,
    to: string | number,
    readonly fields: readonly string[] = CARD_FIELDS,
    filters: readonly DashboardFilter[] = [],
    edgeOnly = false,
  ) {
    this.rowFilter = compileFilters(filters);
    this.edgeOnly = edgeOnly;
    this.fromMs = typeof from === "number" ? from : Date.parse(from);
    this.toMs = typeof to === "number" ? to : Date.parse(to);
    for (const field of fields) {
      this.counts.set(field, new Map());
      this.nonNull.set(field, 0);
    }
  }

  add(
    rows: number,
    columns: Map<string, ArrayLike<unknown>>,
    alreadyFiltered = false,
  ): void {
    const timestamps = columns.get("timestamp");
    if (!timestamps) return;
    const status = columns.get("status");
    const cache = columns.get("cache");
    const bytes = columns.get("resp_bytes");
    const edgeCol =
      !alreadyFiltered && this.edgeOnly ? columns.get("edge") : undefined;
    if (bytes) this.hasBytes = true;
    const fieldColumns = this.fields.flatMap((field) => {
      const column = columns.get(field);
      return column
        ? [{ field, column, counts: this.counts.get(field)!, added: 0 }]
        : [];
    });
    const filterColumns =
      !alreadyFiltered && this.rowFilter
        ? new Map(this.rowFilter.fields.map((f) => [f, columns.get(f)]))
        : null;
    let i = 0;
    const read = (field: string) => toScalar(filterColumns!.get(field)?.[i]);
    for (i = 0; i < rows; i++) {
      const ts = toMs(timestamps[i]);
      if (ts === null) continue;
      if (!alreadyFiltered) {
        if (ts < this.fromMs || ts >= this.toMs) continue;
        if (edgeCol && !isEdgeValue(edgeCol[i])) continue;
        if (this.rowFilter && !this.rowFilter.accepts(read)) continue;
      }
      if (ts < this.minTs) this.minTs = ts;
      if (ts > this.maxTs) this.maxTs = ts;
      this.totalRequests++;
      const t = Math.floor(ts / MINUTE_MS) * MINUTE_MS;
      let bucket = this.buckets.get(t);
      if (!bucket) {
        bucket = {
          t,
          requests: 0,
          bytes: 0,
          s4xx: 0,
          s5xx: 0,
          hits: 0,
          cacheRows: 0,
        };
        this.buckets.set(t, bucket);
      }
      bucket.requests++;
      if (bytes) bucket.bytes += toNumber(bytes[i]) ?? 0;
      const code = status ? toNumber(status[i]) : null;
      if (code !== null) {
        if (code >= 500 && code < 600) bucket.s5xx++;
        else if (code >= 400) bucket.s4xx++;
      }
      const state = cache?.[i];
      if (typeof state === "string") {
        bucket.cacheRows++;
        if (isCdnHit(state)) bucket.hits++;
      }
      for (const entry of fieldColumns) {
        const value = toScalar(entry.column[i]);
        if (value === null) continue;
        entry.counts.set(value, (entry.counts.get(value) ?? 0) + 1);
        entry.added++;
      }
    }
    for (const entry of fieldColumns) {
      if (entry.added > 0) {
        this.nonNull.set(
          entry.field,
          this.nonNull.get(entry.field)! + entry.added,
        );
      }
    }
  }

  toRollupData(): FileRollupData {
    const counts: Record<string, [Scalar, number][]> = {};
    const nonNull: Record<string, number> = {};
    for (const field of this.fields) {
      const fieldCounts = this.counts.get(field);
      if (fieldCounts && fieldCounts.size > 0) {
        counts[field] = [...fieldCounts.entries()];
      }
      const nn = this.nonNull.get(field) ?? 0;
      if (nn > 0) {
        nonNull[field] = nn;
      }
    }
    return {
      minTs: this.minTs,
      maxTs: this.maxTs,
      totalRequests: this.totalRequests,
      hasBytes: this.hasBytes,
      buckets: [...this.buckets.values()].sort((a, b) => a.t - b.t),
      counts,
      nonNull,
    };
  }

  toRollup(
    key: string,
    rawRows: number,
    byteLength: number,
    edgeRollup?: FileRollupData,
  ): FileRollup {
    return {
      version: 1,
      key,
      rawRows,
      byteLength,
      ...this.toRollupData(),
      ...(edgeRollup ? { edgeRollup } : {}),
    };
  }

  mergeRollup(rollup: FileRollupData): void {
    this.totalRequests += rollup.totalRequests;
    if (rollup.hasBytes) this.hasBytes = true;
    if (rollup.minTs < this.minTs) this.minTs = rollup.minTs;
    if (rollup.maxTs > this.maxTs) this.maxTs = rollup.maxTs;
    for (const b of rollup.buckets) {
      const existing = this.buckets.get(b.t);
      if (!existing) {
        this.buckets.set(b.t, { ...b });
      } else {
        existing.requests += b.requests;
        existing.bytes += b.bytes;
        existing.s4xx += b.s4xx;
        existing.s5xx += b.s5xx;
        existing.hits += b.hits;
        existing.cacheRows += b.cacheRows;
      }
    }
    for (const [field, entries] of Object.entries(rollup.counts)) {
      const map = this.counts.get(field);
      if (!map) continue;
      for (const [value, count] of entries) {
        map.set(value, (map.get(value) ?? 0) + count);
      }
    }
    for (const [field, nn] of Object.entries(rollup.nonNull)) {
      if (this.nonNull.has(field)) {
        this.nonNull.set(field, this.nonNull.get(field)! + nn);
      }
    }
  }

  mergeSummary(summary: DecodedSummary): void {
    this.mergeRollup(summary.rollup);
    for (const field of summary.truncated) {
      if (this.counts.has(field)) this.truncated.add(field);
    }
    if (summary.ipSketch) {
      if (this.ipSketch) this.ipSketch.merge(summary.ipSketch);
      else this.ipSketch = summary.ipSketch.clone();
    }
  }

  /** Takes back a contribution merged earlier with `mergeRollup`. */
  subtractRollup(rollup: FileRollupData): void {
    this.totalRequests -= rollup.totalRequests;
    for (const b of rollup.buckets) {
      const existing = this.buckets.get(b.t);
      if (!existing) continue;
      existing.requests -= b.requests;
      existing.bytes -= b.bytes;
      existing.s4xx -= b.s4xx;
      existing.s5xx -= b.s5xx;
      existing.hits -= b.hits;
      existing.cacheRows -= b.cacheRows;
      if (
        existing.requests === 0 &&
        existing.bytes === 0 &&
        existing.cacheRows === 0
      ) {
        this.buckets.delete(b.t);
      }
    }
    for (const [field, entries] of Object.entries(rollup.counts)) {
      const map = this.counts.get(field);
      if (!map) continue;
      for (const [value, count] of entries) {
        const left = (map.get(value) ?? 0) - count;
        if (left > 0) map.set(value, left);
        else map.delete(value);
      }
    }
    for (const [field, nn] of Object.entries(rollup.nonNull)) {
      if (this.nonNull.has(field)) {
        this.nonNull.set(field, this.nonNull.get(field)! - nn);
      }
    }
  }

  /** Rebuilds the truncation marks and IP sketch from the summaries still merged. */
  resetSummaries(summaries: Iterable<DecodedSummary>): void {
    this.truncated.clear();
    this.ipSketch = null;
    for (const summary of summaries) {
      for (const field of summary.truncated) {
        if (this.counts.has(field)) this.truncated.add(field);
      }
      if (summary.ipSketch) {
        if (this.ipSketch) this.ipSketch.merge(summary.ipSketch);
        else this.ipSketch = summary.ipSketch.clone();
      }
    }
  }

  /** Distinct count of a truncated field: an HLL estimate for `ip`. */
  private approximateDistinct(field: string, counts: Map<Scalar, number>) {
    if (field !== "ip" || !this.ipSketch) return counts.size;
    const sketch = this.ipSketch.clone();
    for (const value of counts.keys()) sketch.add(String(value));
    return Math.max(sketch.count(), counts.size);
  }

  /** True until anything has been added or merged. */
  isPristine(): boolean {
    return (
      this.totalRequests === 0 &&
      this.buckets.size === 0 &&
      this.topSummaries.size === 0 &&
      [...this.counts.values()].every((counts) => counts.size === 0)
    );
  }

  /**
   * Merges SQL aggregates. Complete fields merge like rollups; top-only
   * fields are kept as the card's answer, so they are valid only on a
   * pristine aggregator.
   */
  mergeAggregates(aggregates: PartialAggregates): void {
    const counts: Record<string, [Scalar, number][]> = {};
    const nonNull: Record<string, number> = {};
    const pristine = this.isPristine();
    for (const [field, aggregate] of Object.entries(aggregates.fields)) {
      if (!this.counts.has(field)) continue;
      if (aggregate.complete || aggregate.approximate) {
        counts[field] = aggregate.entries;
        nonNull[field] = aggregate.nonNull;
        if (aggregate.approximate) this.truncated.add(field);
      } else {
        if (!pristine) {
          throw new Error(`Top-only ${field} counts cannot merge exactly`);
        }
        this.topSummaries.set(field, aggregate);
      }
    }
    this.mergeRollup({
      minTs: aggregates.minTs,
      maxTs: aggregates.maxTs,
      totalRequests: aggregates.totalRequests,
      hasBytes: false,
      buckets: aggregates.buckets,
      counts,
      nonNull,
    });
  }

  boundRankings(): void {
    const bytesPerField = Math.floor((32 * 1024 * 1024) / this.fields.length);
    for (const [field, counts] of this.counts) {
      let bytes = 0;
      for (const value of counts.keys()) {
        bytes += 48 + (typeof value === "string" ? value.length * 2 : 8);
      }
      if (counts.size <= 4096 && bytes <= bytesPerField) continue;
      const kept = new Map<Scalar, number>();
      bytes = 0;
      for (const { value, count } of topEntries(counts, 4096)) {
        const size = 48 + (typeof value === "string" ? value.length * 2 : 8);
        if (bytes + size > bytesPerField) continue;
        kept.set(value, count);
        bytes += size;
      }
      this.counts.set(field, kept);
      this.truncated.add(field);
    }
  }

  result(): Pick<
    DashboardResult,
    "totalRequests" | "hasBytes" | "buckets" | "cards"
  > {
    const cards: Record<string, CardResult> = {};
    for (const field of this.fields) {
      const summary = this.topSummaries.get(field);
      if (summary) {
        cards[field] = {
          total: summary.nonNull,
          distinct: summary.distinct,
          top: topEntries(new Map(summary.entries), TOP_N),
        };
        continue;
      }
      const counts = this.counts.get(field)!;
      const top = topEntries(counts, TOP_N);
      const approximate = this.truncated.has(field);
      const total = this.nonNull.get(field)!;
      let attributed = 0;
      if (approximate) for (const count of counts.values()) attributed += count;
      cards[field] = {
        total,
        distinct: approximate
          ? this.approximateDistinct(field, counts)
          : counts.size,
        top,
        ...(approximate
          ? {
              approximate,
              unattributed: Math.max(0, total - attributed),
              ...(field === "ip" && this.ipSketch
                ? { distinctEstimated: true }
                : {}),
            }
          : {}),
        ...(field === ALL_VALUES_FIELD
          ? {
              all: [...counts].map(([value, count]) => ({ value, count })),
            }
          : {}),
      };
    }
    return {
      totalRequests: this.totalRequests,
      hasBytes: this.hasBytes,
      buckets: [...this.buckets.values()].sort((a, b) => a.t - b.t),
      cards,
    };
  }

  isTruncated(field: string): boolean {
    return this.truncated.has(field);
  }

  searchValues(
    field: string,
    query: string,
    limit: number,
  ): { value: Scalar; count: number }[] {
    const counts = this.counts.get(field);
    if (!counts) return [];
    const needle = query.trim().toLowerCase();
    const filtered = new Map<Scalar, number>();
    for (const [value, count] of counts) {
      if (count <= 0) continue;
      if (needle === "" || String(value).toLowerCase().includes(needle)) {
        filtered.set(value, count);
      }
    }
    return topEntries(filtered, limit);
  }
}

function isInteriorRollup(
  rollup: Pick<FileRollupData, "totalRequests" | "minTs" | "maxTs">,
  fromMs: number,
  toMs: number,
): boolean {
  return (
    rollup.totalRequests === 0 ||
    (rollup.minTs >= fromMs && rollup.maxTs < toMs)
  );
}

function isRollupCompatibleWithEdgeOnly(
  rollup: FileRollupData,
  edgeOnly: boolean,
): boolean {
  if (!edgeOnly) return true;
  const edgeNonNull = rollup.nonNull.edge ?? 0;
  // Files without an edge column (or all-null edge column) are treated as edge.
  if (edgeNonNull === 0) return true;
  if (edgeNonNull !== rollup.totalRequests) return false;
  const edgeEntries = rollup.counts.edge ?? [];
  return edgeEntries.every(
    ([value, count]) => count === 0 || isEdgeValue(value),
  );
}

function getEffectiveRollup(
  rollup: FileRollup,
  edgeOnly: boolean,
): FileRollupData | null {
  if (!edgeOnly || isRollupCompatibleWithEdgeOnly(rollup, true)) {
    return rollup;
  }
  return rollup.edgeRollup ?? null;
}

export function classifyRollupAgainstFilters(
  rollup: FileRollupData,
  filters: readonly DashboardFilter[],
): "none" | "all" | "partial" {
  if (filters.length === 0) return "all";
  if (rollup.totalRequests === 0) return "none";

  const includes = new Map<string, Matcher[]>();
  const excludes = new Map<string, Matcher[]>();
  for (const { field, value, mode } of filters) {
    if (!field || value === "") continue;
    const target = mode === "include" ? includes : excludes;
    const list = target.get(field) ?? [];
    list.push(createValueMatcher(value));
    target.set(field, list);
  }

  let allMatch = true;

  for (const [field, matchers] of includes) {
    const entries = rollup.counts[field];
    if (!entries || entries.length === 0) {
      return "none";
    }
    let matchingRows = 0;
    for (const [value, count] of entries) {
      if (count > 0 && matchers.some((m) => m(value))) {
        matchingRows += count;
      }
    }
    if (matchingRows === 0) {
      return "none";
    }
    if (matchingRows < rollup.totalRequests) {
      allMatch = false;
    }
  }

  for (const [field, matchers] of excludes) {
    const entries = rollup.counts[field];
    if (!entries || entries.length === 0) {
      continue;
    }
    let excludedRows = 0;
    for (const [value, count] of entries) {
      if (count > 0 && matchers.some((m) => m(value))) {
        excludedRows += count;
      }
    }
    if (excludedRows >= rollup.totalRequests) {
      return "none";
    }
    if (excludedRows > 0) {
      allMatch = false;
    }
  }

  return allMatch ? "all" : "partial";
}

export function classifySummaryAgainstFilters(
  summary: DecodedSummary,
  filters: readonly DashboardFilter[],
  edgeOnly = false,
): "none" | "all" | "partial" {
  const { rollup } = summary;
  if (rollup.totalRequests === 0) return "none";

  const truncatedSet = new Set(summary.truncated);
  let allMatch = true;

  if (edgeOnly) {
    const edgeNonNull = rollup.nonNull.edge ?? 0;
    const edgeTruncated = truncatedSet.has("edge");
    if (edgeNonNull > 0 || edgeTruncated) {
      let edgeTrue = 0;
      for (const [value, count] of rollup.counts.edge ?? []) {
        if (count > 0 && isEdgeValue(value)) {
          edgeTrue += count;
        }
      }
      if (edgeTrue === 0 && !edgeTruncated) {
        return "none";
      }
      if (edgeTrue < rollup.totalRequests) {
        allMatch = false;
      }
    }
  }

  const includes = new Map<string, Matcher[]>();
  const excludes = new Map<string, Matcher[]>();
  for (const { field, value, mode } of filters) {
    if (!field || value === "") continue;
    const target = mode === "include" ? includes : excludes;
    const list = target.get(field) ?? [];
    list.push(createValueMatcher(value));
    target.set(field, list);
  }

  for (const [field, matchers] of includes) {
    const entries = rollup.counts[field] ?? [];
    const isTruncated = truncatedSet.has(field);
    let matchingRows = 0;
    for (const [value, count] of entries) {
      if (count > 0 && matchers.some((m) => m(value))) {
        matchingRows += count;
      }
    }
    if (matchingRows === 0) {
      if (!isTruncated) {
        return "none";
      }
      allMatch = false;
    } else if (matchingRows < rollup.totalRequests) {
      allMatch = false;
    }
  }

  for (const [field, matchers] of excludes) {
    const entries = rollup.counts[field] ?? [];
    const isTruncated = truncatedSet.has(field);
    let excludedRows = 0;
    for (const [value, count] of entries) {
      if (count > 0 && matchers.some((m) => m(value))) {
        excludedRows += count;
      }
    }
    if (excludedRows >= rollup.totalRequests) {
      return "none";
    }
    if (excludedRows > 0 || isTruncated) {
      allMatch = false;
    }
  }

  return allMatch ? "all" : "partial";
}

function planCoarsestFirst(
  rawFiles: readonly ManifestFile[],
  summaryFiles: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
): { summaries: SummaryUse[]; raw: ManifestFile[] } {
  const days = summaryFiles.filter((f) => f.key.includes("/day_bundle="));
  const hours = summaryFiles.filter((f) => f.key.includes("/hour_bundle="));
  const windows = summaryFiles.filter((f) => f.key.includes("/summary="));
  const p1 = planSummaries(rawFiles, days, fromMs, toMs);
  const p2 = planSummaries(p1.raw, hours, fromMs, toMs);
  const p3 = planSummaries(p2.raw, windows, fromMs, toMs);
  const p4 = planSummaries(p3.raw, summaryFiles, fromMs, toMs);
  return {
    summaries: [
      ...p1.summaries,
      ...p2.summaries,
      ...p3.summaries,
      ...p4.summaries,
    ],
    raw: p4.raw,
  };
}

type UnitMeta = {
  minTs: number;
  maxTs: number;
  totalRequests: number;
  hasBytes: boolean;
  rawRows: number;
  byteLength: number;
};

/**
 * The last unfiltered dashboard aggregate of a cache scope and the units it
 * sums: interior files (their rollups are re-read to subtract), boundary
 * slices and summaries. A refresh subtracts the units that left or changed and
 * adds only the new ones, so it equals a full recompute.
 */
type Memo = {
  aggregator: Aggregator;
  /** `customFieldsKey` of the custom fields the aggregate counts. */
  variant: string;
  fromMs: number;
  toMs: number;
  interior: Map<string, UnitMeta>;
  boundary: Map<
    string,
    { data: FileRollupData; rawRows: number; byteLength: number }
  >;
  summaries: Map<
    string,
    { decoded: DecodedSummary; rows: number; byteLength: number }
  >;
};

const MEMO_SCOPES = 2;
const memos = new Map<string, Memo>();
type CachedSummaryEntry = {
  variant: string;
  decoded: DecodedSummary;
  byteLength: number;
};
const summaryListings = new Map<string, Map<string, ManifestFile>>();
const decodedSummaries = new Map<string, Map<string, CachedSummaryEntry>>();

function setScopeEntry<V>(
  outer: Map<string, Map<string, V>>,
  scope: string,
  key: string,
  value: V,
): void {
  let inner = outer.get(scope);
  if (inner) {
    outer.delete(scope);
  } else {
    inner = new Map();
  }
  inner.set(key, value);
  outer.set(scope, inner);
  for (const k of outer.keys()) {
    if (outer.size <= MEMO_SCOPES) break;
    outer.delete(k);
  }
}

/** Forgets every kept dashboard aggregate. */
export function clearDashboardMemo(): void {
  memos.clear();
  summaryListings.clear();
  decodedSummaries.clear();
}

/**
 * Turns `prev` into the kept part of the next run over `[fromMs, toMs)`, or
 * returns null when recomputing is cheaper or a rollup to subtract is gone.
 */
async function reuseMemo(
  prev: Memo,
  source: FileSource,
  files: readonly ManifestFile[],
  summaryUses: readonly SummaryUse[],
  fromMs: number,
  toMs: number,
  signal: AbortSignal | undefined,
): Promise<Memo | null> {
  const next: Memo = {
    aggregator: prev.aggregator,
    variant: prev.variant,
    fromMs,
    toMs,
    interior: new Map(),
    boundary: new Map(),
    summaries: new Map(),
  };
  const sameRange = prev.fromMs === fromMs && prev.toMs === toMs;
  for (const file of files) {
    const meta = prev.interior.get(file.key);
    if (meta && isInteriorRollup(meta, fromMs, toMs)) {
      next.interior.set(file.key, meta);
    }
    const slice = sameRange ? prev.boundary.get(file.key) : undefined;
    if (slice) next.boundary.set(file.key, slice);
  }
  for (const use of summaryUses) {
    const kept = prev.summaries.get(use.file.key);
    if (
      kept &&
      kept.decoded.rollup.buckets.every(
        (b) => b.t >= fromMs && b.t + MINUTE_MS <= toMs,
      )
    ) {
      next.summaries.set(use.file.key, kept);
    }
  }
  const leaving = [...prev.interior.keys()].filter(
    (key) => !next.interior.has(key),
  );
  if (leaving.length > next.interior.size + next.summaries.size) return null;
  const rollups = await Promise.all(
    leaving.map((key) => source.readRollup!(key, prev.variant)),
  );
  signal?.throwIfAborted();
  if (rollups.some((rollup) => !rollup)) return null;
  const aggregator = next.aggregator;
  for (const rollup of rollups) aggregator.subtractRollup(rollup!);
  for (const [key, slice] of prev.boundary) {
    if (!next.boundary.has(key)) aggregator.subtractRollup(slice.data);
  }
  for (const [key, kept] of prev.summaries) {
    if (!next.summaries.has(key))
      aggregator.subtractRollup(kept.decoded.rollup);
  }
  aggregator.resetSummaries(
    [...next.summaries.values()].map((kept) => kept.decoded),
  );
  return next;
}

function unitMeta(
  rollup: FileRollupData,
  file: { rawRows: number; byteLength: number },
): UnitMeta {
  return {
    minTs: rollup.minTs,
    maxTs: rollup.maxTs,
    totalRequests: rollup.totalRequests,
    hasBytes: rollup.hasBytes,
    rawRows: file.rawRows,
    byteLength: file.byteLength,
  };
}

async function collectAggregator(
  source: FileSource,
  options: EngineOptions,
  fields: readonly string[],
  topK: number | null,
  lease: DuckDBLease,
  searchQuery?: string,
): Promise<{
  aggregator: Aggregator;
  stats: FetchStats;
  started: number;
  now: () => number;
}> {
  const now = options.now ?? (() => performance.now());
  const started = now();
  const { from, to, signal } = options;
  const edgeOnly = Boolean(options.edgeOnly);
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);
  const hasFilters = Boolean(options.filters && options.filters.length > 0);
  const customFields = options.customFields ?? [];
  const generation = lease.readContext();
  const cardFields = [...CARD_FIELDS, ...customFields.map((field) => field.id)];
  const variant = customFieldsKey(customFields);
  const manifest =
    options.manifest ??
    (await fetchManifest(source.fetchJson, from, to, signal));
  generation.assertCurrent();
  signal?.throwIfAborted();
  if (!manifest.complete) {
    throw new Error(
      "The file manifest for this range is incomplete; choose a shorter range.",
    );
  }
  void source.pruneCache?.(manifest, fromMs, toMs).catch(() => {});
  const scope = source.cacheScope ?? "default";
  if (manifest.summaryFiles) {
    for (const file of manifest.summaryFiles) {
      setScopeEntry(summaryListings, scope, file.key, file);
    }
  }
  let files: ManifestFile[] = filesOverlapping(manifest.files, fromMs, toMs);
  let summaryUses: SummaryUse[] = [];
  let summaryListing: FetchStats["summaryListing"];
  const canUseSummaries =
    topK !== null &&
    fields.length === cardFields.length &&
    toMs - fromMs >= SUMMARY_MIN_RANGE_MS &&
    files.some(
      (file) =>
        Date.parse(file.start) >= fromMs && Date.parse(file.end) <= toMs,
    );
  if (!hasFilters && !edgeOnly && canUseSummaries) {
    const inlineSummaries =
      manifest.summaryFiles &&
      (!manifest.summaryRange ||
        (manifest.summaryRange.from === from &&
          manifest.summaryRange.to === to))
        ? { files: manifest.summaryFiles, complete: true }
        : null;
    const listing =
      inlineSummaries ??
      (await fetchManifest(
        source.fetchJson,
        from,
        to,
        signal,
        undefined,
        "summary",
      ).catch((error: unknown) => {
        // Summaries only save work; without them every window reads raw.
        if (signal?.aborted) throw error;
        console.warn("flp: summary listing failed; reading raw Parquet", error);
        return null;
      }));
    summaryListing = !listing
      ? "failed"
      : !listing.complete
        ? "incomplete"
        : inlineSummaries
          ? "inline"
          : "listed";
    if (listing && !listing.complete) {
      console.warn("flp: summary listing incomplete; reading raw Parquet");
    }
    if (listing?.complete) {
      for (const file of listing.files) {
        setScopeEntry(summaryListings, scope, file.key, file);
      }
      const plan = planSummaries(files, listing.files, fromMs, toMs);
      files = plan.raw;
      summaryUses = plan.summaries;
    }
  }
  const incremental =
    topK === TOP_N &&
    !source.openParquet &&
    !hasFilters &&
    !edgeOnly &&
    fields.length === cardFields.length &&
    Boolean(source.readRollup && source.writeRollup);
  let listed = files.length + summaryUses.length;
  let memo: Memo | null = null;
  if (incremental) {
    // Taken, not shared: a concurrent or failed run recomputes in full.
    const prev = memos.get(scope);
    memos.delete(scope);
    memo =
      (prev &&
        prev.variant === variant &&
        (await reuseMemo(
          prev,
          source,
          files,
          summaryUses,
          fromMs,
          toMs,
          signal,
        ))) ||
      null;
  }
  const aggregator =
    memo?.aggregator ??
    new Aggregator(from, to, fields, options.filters, edgeOnly);
  memo ??= incremental
    ? {
        aggregator,
        variant,
        fromMs,
        toMs,
        interior: new Map(),
        boundary: new Map(),
        summaries: new Map(),
      }
    : null;
  const next = memo;
  const keptRows = { cached: 0, rows: 0, bytes: 0 };
  const scopeListings = summaryListings.get(scope);
  const scopeDecoded = decodedSummaries.get(scope);
  generation.assertCurrent();
  signal?.throwIfAborted();
  if (next) {
    for (const meta of [...next.interior.values(), ...next.boundary.values()]) {
      keptRows.cached++;
      keptRows.rows += meta.rawRows;
      keptRows.bytes += meta.byteLength;
    }
    for (const kept of next.summaries.values()) {
      keptRows.cached++;
      keptRows.rows += kept.rows;
      keptRows.bytes += kept.byteLength;
    }
    files = files.filter(
      (file) => !next.interior.has(file.key) && !next.boundary.has(file.key),
    );
    summaryUses = summaryUses.filter(
      (use) => !next.summaries.has(use.file.key),
    );
  } else if (
    (hasFilters || edgeOnly) &&
    canUseSummaries &&
    scopeListings &&
    scopeListings.size > 0
  ) {
    const cachedSummaryFiles: ManifestFile[] = [];
    for (const file of scopeListings.values()) {
      if (
        scopeDecoded?.get(file.key)?.variant === variant ||
        Boolean(await source.hasFile?.(file.key))
      ) {
        cachedSummaryFiles.push(file);
      }
    }
    signal?.throwIfAborted();
    if (cachedSummaryFiles.length > 0) {
      const initialPlan = planCoarsestFirst(
        files,
        cachedSummaryFiles,
        fromMs,
        toMs,
      );
      const remainingRaw = new Set(initialPlan.raw.map((f) => f.key));
      const triedSummaries = new Set<string>();
      const queue: SummaryUse[] = [...initialPlan.summaries];
      let resolvedSummaries = 0;
      const filterList = options.filters ?? [];

      while (queue.length > 0) {
        signal?.throwIfAborted();
        const use = queue.shift()!;
        triedSummaries.add(use.file.key);
        let decoded: DecodedSummary | null = null;
        let summaryBytes = 0;
        const mem = decodedSummaries.get(scope)?.get(use.file.key);
        if (
          mem &&
          mem.variant === variant &&
          mem.decoded.rollup.buckets.every(
            (b) => b.t >= fromMs && b.t + MINUTE_MS <= toMs,
          )
        ) {
          decoded = mem.decoded;
          summaryBytes = mem.byteLength;
        } else {
          try {
            const { bytes } = await source.fetchFile(use.file.key, signal);
            summaryBytes = bytes.byteLength;
            decoded = await decodeSummary(bytes, fromMs, toMs, customFields);
            if (decoded) {
              setScopeEntry(decodedSummaries, scope, use.file.key, {
                variant,
                decoded,
                byteLength: summaryBytes,
              });
            }
          } catch (error) {
            if (signal?.aborted) throw error;
          }
        }
        signal?.throwIfAborted();
        const matchClass = decoded
          ? classifySummaryAgainstFilters(decoded, filterList, edgeOnly)
          : "partial";
        const interiorBuckets =
          decoded !== null &&
          decoded.rollup.buckets.every(
            (b) => b.t >= fromMs && b.t + MINUTE_MS <= toMs,
          );
        if (decoded && matchClass === "none") {
          if (decoded.rollup.hasBytes) aggregator.hasBytes = true;
          let rows = 0;
          for (const covered of use.covers) rows += covered.rows ?? 0;
          keptRows.cached++;
          keptRows.rows += rows;
          keptRows.bytes += summaryBytes;
          resolvedSummaries++;
        } else if (decoded && matchClass === "all" && interiorBuckets) {
          aggregator.mergeSummary(decoded);
          let rows = 0;
          for (const covered of use.covers) rows += covered.rows ?? 0;
          keptRows.cached++;
          keptRows.rows += rows;
          keptRows.bytes += summaryBytes;
          resolvedSummaries++;
        } else {
          const finerAvailable = cachedSummaryFiles.filter(
            (f) => !triedSummaries.has(f.key),
          );
          const subPlan = planCoarsestFirst(
            use.covers,
            finerAvailable,
            fromMs,
            toMs,
          );
          queue.push(...subPlan.summaries);
          for (const rawFile of subPlan.raw) {
            remainingRaw.add(rawFile.key);
          }
        }
      }

      files = files.filter((file) => remainingRaw.has(file.key));
      listed = files.length + summaryUses.length + resolvedSummaries;
    }
  }
  const buildFullRollup =
    topK !== null &&
    (fields.length === cardFields.length || Boolean(source.writeRollup));
  const wanted = [...BASE_COLUMNS, ...(buildFullRollup ? cardFields : fields)];
  const duckdb = getDuckDBEngine();
  const scopedKeyOf = (key: string) => `${scope}:${key}`;
  const duckdbScopedKeys: string[] = [];
  const stats: FetchStats = {
    listed,
    fetched: 0,
    cached: keptRows.cached,
    cdnHits: 0,
    rows: keptRows.rows,
    bytes: keptRows.bytes,
    elapsedMs: 0,
    ...(summaryListing
      ? { summaryListing, summaryFiles: summaryUses.length }
      : {}),
  };
  const progress: RunProgress = {
    phase: "cache",
    cached: keptRows.cached,
    toFetch: 0,
    toDecode: 0,
    done: 0,
    downloadedBytes: 0,
    cachedBytes: keptRows.bytes,
  };
  let reportProgress = false;
  // Quick runs stay silent so the UI doesn't flash; downloads report at once.
  const maybeReportProgress = (force = false) => {
    if (!options.onProgress) return;
    if (force || now() - started >= 250) reportProgress = true;
    if (reportProgress) options.onProgress({ ...progress });
  };
  const runPool = async <T>(
    items: readonly T[],
    handle: (item: T) => Promise<void>,
  ) => {
    let next = 0;
    const worker = async () => {
      while (next < items.length) {
        signal?.throwIfAborted();
        await handle(items[next++]);
      }
    };
    await Promise.all(
      Array.from(
        {
          length: Math.min(
            options.concurrency ?? FETCH_CONCURRENCY,
            items.length,
          ),
        },
        worker,
      ),
    );
  };
  const countCached = (rawRows: number, byteLength: number) => {
    stats.cached++;
    stats.rows += rawRows;
    stats.bytes += byteLength;
    progress.cached++;
    progress.cachedBytes += byteLength;
    maybeReportProgress(false);
  };
  const countRead = (cached: boolean, byteLength: number) => {
    if (cached) progress.cachedBytes += byteLength;
    else progress.downloadedBytes += byteLength;
  };

  // Pass 1 merges everything cached summaries can answer and lists the rest, so
  // progress reports only the files that still need fetching or decoding.
  type PendingFile = {
    file: ManifestFile;
    scopedKey: string;
    local: boolean;
    hadRollup: boolean;
    rollupMeta?: { rawRows: number; minTs: number; maxTs: number };
    summary?: SummaryUse;
  };
  const pending: PendingFile[] = [];
  let toFetch = 0;
  let toDecode = 0;
  const addPending = async (entry: Omit<PendingFile, "local">) => {
    const local = Boolean(await source.hasFile?.(entry.file.key));
    signal?.throwIfAborted();
    if (local) toDecode++;
    else toFetch++;
    pending.push({ ...entry, local });
  };
  const classify = async (file: ManifestFile) => {
    const scopedKey = scopedKeyOf(file.key);
    const cachedRollup = (await source.readRollup?.(file.key, variant)) ?? null;
    signal?.throwIfAborted();
    const activeRollup = cachedRollup
      ? getEffectiveRollup(cachedRollup, edgeOnly)
      : null;

    if (hasFilters) {
      const filterList = options.filters ?? [];
      if (cachedRollup) {
        const matchClass = activeRollup
          ? classifyRollupAgainstFilters(activeRollup, filterList)
          : classifyRollupAgainstFilters(cachedRollup, filterList) === "none"
            ? "none"
            : "partial";
        if (matchClass === "none") {
          if (cachedRollup.hasBytes) aggregator.hasBytes = true;
          countCached(cachedRollup.rawRows, cachedRollup.byteLength);
          return;
        }
        if (
          matchClass === "all" &&
          activeRollup &&
          isInteriorRollup(activeRollup, fromMs, toMs)
        ) {
          aggregator.mergeRollup(activeRollup);
          countCached(cachedRollup.rawRows, cachedRollup.byteLength);
          return;
        }
      }
      const existing = lease.getFileMeta(scopedKey);
      if (existing) {
        duckdbScopedKeys.push(scopedKey);
        countCached(existing.rawRows, existing.byteLength);
        return;
      }
      await addPending({
        file,
        scopedKey,
        hadRollup: false,
        rollupMeta: cachedRollup
          ? {
              rawRows: cachedRollup.rawRows,
              minTs: cachedRollup.minTs,
              maxTs: cachedRollup.maxTs,
            }
          : undefined,
      });
      return;
    }

    if (cachedRollup) {
      if (activeRollup && isInteriorRollup(activeRollup, fromMs, toMs)) {
        aggregator.mergeRollup(activeRollup);
        next?.interior.set(file.key, unitMeta(activeRollup, cachedRollup));
        countCached(cachedRollup.rawRows, cachedRollup.byteLength);
        return;
      }
    }
    // DuckDB answers top values only, which cannot be subtracted later.
    if (!next) {
      const existing = lease.getFileMeta(scopedKey);
      if (existing) {
        duckdbScopedKeys.push(scopedKey);
        countCached(
          cachedRollup?.rawRows ?? existing.rawRows,
          cachedRollup?.byteLength ?? existing.byteLength,
        );
        return;
      }
    }
    await addPending({ file, scopedKey, hadRollup: Boolean(cachedRollup) });
  };
  await runPool(summaryUses, (summary) =>
    addPending({
      file: summary.file,
      scopedKey: scopedKeyOf(summary.file.key),
      hadRollup: false,
      summary,
    }),
  );
  await runPool(files, classify);

  progress.toFetch = toFetch;
  progress.toDecode = toDecode;
  progress.phase = "files";
  if (pending.length > 0) maybeReportProgress(toFetch > 0);
  const finishFile = () => {
    progress.done++;
    maybeReportProgress(false);
  };

  const microFiles = new Set<string>();
  let microBytes = 0;
  const wallNow = options.wallNow?.() ?? Date.now();
  const reserveMicroFiles = async (items: readonly ManifestFile[]) => {
    if (!source.persistFile) return;
    const candidates = items
      .filter(
        (file) =>
          Number.isSafeInteger(file.size) &&
          file.size > 0 &&
          file.size <= 8 * 1024 * 1024 &&
          Date.parse(file.start) < Date.parse(file.end) &&
          Date.parse(file.end) <= Math.min(toMs, wallNow),
      )
      .sort(
        (a, b) =>
          Date.parse(b.end) - Date.parse(a.end) || a.key.localeCompare(b.key),
      );
    // Lookups run a chunk at a time; reservations still apply in candidate order.
    for (
      let index = 0;
      index < candidates.length && microFiles.size < 128;
      index += FETCH_CONCURRENCY
    ) {
      const chunk = candidates.slice(index, index + FETCH_CONCURRENCY);
      // Hits need no prefetch reservation; openParquet revalidates them before use.
      const hits = await Promise.all(
        chunk.map((file) =>
          microFiles.has(file.key) || microBytes + file.size > 8 * 1024 * 1024
            ? false
            : source.hasPersistentFile?.(file, signal, variant),
        ),
      );
      chunk.forEach((file, i) => {
        if (microFiles.size >= 128 || hits[i]) return;
        if (
          microFiles.has(file.key) ||
          microBytes + file.size > 8 * 1024 * 1024
        )
          return;
        microFiles.add(file.key);
        microBytes += file.size;
      });
    }
  };

  const readSummary = async (summary: SummaryUse) => {
    let decoded: DecodedSummary | null = null;
    let summaryBytes = 0;
    try {
      const { bytes, cached, cdn } = await source.fetchFile(
        summary.file.key,
        signal,
      );
      stats.bytes += bytes.byteLength;
      summaryBytes = bytes.byteLength;
      countRead(cached, bytes.byteLength);
      if (cached) stats.cached++;
      else stats.fetched++;
      if (isCdnHit(cdn)) stats.cdnHits++;
      const mem = decodedSummaries.get(scope)?.get(summary.file.key);
      if (
        mem &&
        mem.variant === variant &&
        mem.byteLength === bytes.byteLength &&
        mem.decoded.rollup.buckets.every(
          (b) => b.t >= fromMs && b.t + MINUTE_MS <= toMs,
        )
      ) {
        decoded = mem.decoded;
      } else {
        decoded = await decodeSummary(bytes, fromMs, toMs, customFields);
      }
    } catch (error) {
      if (signal?.aborted) throw error;
    }
    signal?.throwIfAborted();
    if (decoded) {
      setScopeEntry(decodedSummaries, scope, summary.file.key, {
        variant,
        decoded,
        byteLength: summaryBytes,
      });
      aggregator.mergeSummary(decoded);
      let rows = 0;
      for (const file of summary.covers) rows += file.rows ?? 0;
      stats.rows += rows;
      next?.summaries.set(summary.file.key, {
        decoded,
        rows,
        byteLength: summaryBytes,
      });
    } else {
      // Unreadable, retired or another version: read its windows instead.
      stats.listed += summary.covers.length;
      await reserveMicroFiles(summary.covers);
      for (const file of summary.covers) await classify(file);
      progress.toFetch = toFetch;
      progress.toDecode = toDecode;
    }
    finishFile();
  };

  await reserveMicroFiles(
    pending.filter((entry) => !entry.summary).map((entry) => entry.file),
  );

  await runPool(pending, async (entry) => {
    const { file, scopedKey, hadRollup, rollupMeta } = entry;
    if (entry.summary) {
      await readSummary(entry.summary);
      return;
    }
    if (source.openParquet) {
      stats.selectiveIOUnmeasured = true;
      let downloaded = false;
      if (microFiles.has(file.key)) {
        try {
          const persisted = await source.persistFile?.(file, signal, variant);
          downloaded = Boolean(persisted?.downloaded);
          if (downloaded) {
            stats.bytes += file.size;
            progress.downloadedBytes += file.size;
          }
        } catch {
          signal?.throwIfAborted();
          // Persistence failures are reported by the source; native ranges remain usable.
        }
      }
      const access = await source.openParquet(file, signal, variant);
      let released = false;
      try {
        const completeRollup =
          topK !== null &&
          !hasFilters &&
          !edgeOnly &&
          Boolean(source.writeRollup) &&
          Date.parse(file.start) >= fromMs &&
          Date.parse(file.end) <= toMs;
        const target = completeRollup
          ? new Aggregator(fromMs, toMs, cardFields)
          : aggregator;
        await duckdb.queryParquetIntoAggregator(
          access,
          fromMs,
          toMs,
          options.filters ?? [],
          target,
          {
            edgeOnly,
            topK: null,
            allValuesFields: [ALL_VALUES_FIELD],
            signal,
            fieldSearch: searchQuery === undefined
              ? undefined : { field: fields[0], query: searchQuery },
            context: lease.readContext(),
          },
          customFields,
        );
        released = true;
        access.release?.();
        if (completeRollup) {
          const rollup = target.toRollup(file.key, file.rows, file.size);
          const truncated = Object.entries(target.result().cards)
            .filter(([, card]) => card.approximate)
            .map(([field]) => field);
          aggregator.mergeSummary({ rollup, truncated, ipSketch: null });
          aggregator.boundRankings();
          if (truncated.length === 0 && rollup.totalRequests === file.rows) {
            await source.writeRollup?.(file.key, rollup, variant);
          }
        }
        let aggregateEntries = aggregator.buckets.size;
        let aggregateBytes = aggregator.buckets.size * 96;
        for (const counts of aggregator.counts.values()) {
          aggregateEntries += counts.size;
          for (const value of counts.keys()) {
            aggregateBytes +=
              48 + (typeof value === "string" ? value.length * 2 : 8);
          }
        }
        if (aggregateEntries > 200_000 || aggregateBytes > 64 * 1024 * 1024) {
          throw new Error(
            "The selective query exceeds the browser aggregate budget; choose a shorter range or add a filter.",
          );
        }
        if (access.cached && !downloaded) {
          stats.cached++;
          progress.cachedBytes += file.size;
        } else {
          stats.fetched++;
          if (!downloaded) progress.rangedFiles = (progress.rangedFiles ?? 0) + 1;
        }
      } finally {
        if (!released) access.release?.();
      }
      finishFile();
      return;
    }
    if (hasFilters) {
      // DuckDB keeps the rows, so hand it the only copy of the bytes.
      const { bytes, cached, cdn } = await source.fetchFile(file.key, signal, {
        transfer: true,
      });
      const meta = await lease.ingestFile(
        scopedKey,
        file.key,
        bytes,
        rollupMeta,
        signal,
      );
      duckdbScopedKeys.push(scopedKey);
      stats.rows += meta.rawRows;
      stats.bytes += meta.byteLength;
      countRead(cached, meta.byteLength);
      if (cached) stats.cached++;
      else stats.fetched++;
      if (isCdnHit(cdn)) stats.cdnHits++;
      finishFile();
      return;
    }

    // Re-read rather than hold every boundary rollup through pass 1.
    const cachedRollup = hadRollup
      ? ((await source.readRollup?.(file.key, variant)) ?? null)
      : null;
    signal?.throwIfAborted();
    const { bytes, cached, cdn } = await source.fetchFile(file.key, signal);
    const decoded = await readColumns(bytes, wanted);
    signal?.throwIfAborted();
    if (!decoded.columns.has("timestamp")) {
      throw new Error(`${file.key} has no timestamp column`);
    }
    if (buildFullRollup) {
      let rollupToWrite: FileRollup | null = null;
      let rollupForFile = cachedRollup;
      if (!rollupForFile) {
        const fileAgg = new Aggregator(-Infinity, Infinity, cardFields);
        fileAgg.add(decoded.rows, decoded.columns, true);
        rollupForFile = fileAgg.toRollup(
          file.key,
          decoded.rows,
          bytes.byteLength,
        );
        if (!isRollupCompatibleWithEdgeOnly(rollupForFile, true)) {
          const edgeAgg = new Aggregator(
            -Infinity,
            Infinity,
            cardFields,
            [],
            true,
          );
          edgeAgg.add(decoded.rows, decoded.columns, false);
          rollupForFile.edgeRollup = edgeAgg.toRollupData();
        }
        rollupToWrite = rollupForFile;
      } else if (
        !isRollupCompatibleWithEdgeOnly(rollupForFile, true) &&
        !rollupForFile.edgeRollup
      ) {
        const edgeAgg = new Aggregator(
          -Infinity,
          Infinity,
          cardFields,
          [],
          true,
        );
        edgeAgg.add(decoded.rows, decoded.columns, false);
        rollupForFile = {
          ...rollupForFile,
          edgeRollup: edgeAgg.toRollupData(),
        };
        rollupToWrite = rollupForFile;
      }

      const effectiveRollup = getEffectiveRollup(rollupForFile, edgeOnly);
      if (effectiveRollup && isInteriorRollup(effectiveRollup, fromMs, toMs)) {
        aggregator.mergeRollup(effectiveRollup);
        next?.interior.set(file.key, unitMeta(effectiveRollup, rollupForFile));
      } else if (next) {
        const slice = new Aggregator(fromMs, toMs, fields);
        slice.add(decoded.rows, decoded.columns);
        const data = slice.toRollupData();
        aggregator.mergeRollup(data);
        next.boundary.set(file.key, {
          data,
          rawRows: decoded.rows,
          byteLength: bytes.byteLength,
        });
      } else {
        aggregator.add(decoded.rows, decoded.columns);
      }
      if (rollupToWrite && source.writeRollup) {
        void Promise.resolve(
          source.writeRollup(file.key, rollupToWrite, variant),
        ).catch(() => {});
      }
    } else {
      aggregator.add(decoded.rows, decoded.columns);
    }
    stats.rows += decoded.rows;
    stats.bytes += bytes.byteLength;
    countRead(cached, bytes.byteLength);
    if (cached) stats.cached++;
    else stats.fetched++;
    if (isCdnHit(cdn)) stats.cdnHits++;
    finishFile();
  });
  progress.phase = "aggregate";
  maybeReportProgress(false);
  if (duckdbScopedKeys.length > 0) {
    await duckdb.queryIntoAggregator(
      duckdbScopedKeys,
      fromMs,
      toMs,
      options.filters ?? [],
      aggregator,
      {
        edgeOnly,
        topK,
        allValuesFields: [ALL_VALUES_FIELD],
        signal,
        fieldSearch: searchQuery === undefined
          ? undefined : { field: fields[0], query: searchQuery },
        context: lease.readContext(),
      },
    );
  }
  generation.assertCurrent();
  signal?.throwIfAborted();
  if (next) {
    aggregator.hasBytes =
      [...next.interior.values()].some((meta) => meta.hasBytes) ||
      [...next.boundary.values()].some((slice) => slice.data.hasBytes) ||
      [...next.summaries.values()].some((kept) => kept.decoded.rollup.hasBytes);
    memos.set(scope, next);
    for (const key of memos.keys()) {
      if (memos.size <= MEMO_SCOPES) break;
      memos.delete(key);
    }
  }
  return { aggregator, stats, started, now };
}

export async function runDashboard(
  source: FileSource,
  options: EngineOptions,
): Promise<DashboardResult> {
  const duckdb = getDuckDBEngine();
  const generation = await duckdb.beginRequest();
  generation.assertCurrent();
  options.signal?.throwIfAborted();
  const lease = duckdb.lease(options.customFields);
  const { aggregator, stats, started, now } = await collectAggregator(
    source,
    options,
    [...CARD_FIELDS, ...(options.customFields ?? []).map((field) => field.id)],
    TOP_N,
    lease,
  ).finally(() => lease.release());
  generation.assertCurrent();
  options.signal?.throwIfAborted();
  const aggregated = aggregator.result();
  // The kept aggregate's buckets change on the next refresh.
  aggregated.buckets = aggregated.buckets.map((bucket) => ({ ...bucket }));
  stats.elapsedMs = Math.round(now() - started);
  sendBeacon(source, stats);
  return { from: options.from, to: options.to, ...aggregated, stats };
}

export async function searchFieldValues(
  source: FileSource,
  options: SearchFieldOptions,
): Promise<{ value: Scalar; count: number }[]> {
  const filters = (options.filters ?? []).filter(
    (f) => f.field !== options.column,
  );
  const duckdb = getDuckDBEngine();
  const generation = await duckdb.beginRequest();
  generation.assertCurrent();
  options.signal?.throwIfAborted();
  const lease = duckdb.lease(options.customFields);
  const { aggregator } = await collectAggregator(
    source,
    {
      from: options.from,
      to: options.to,
      filters,
      edgeOnly: options.edgeOnly,
      manifest: options.manifest,
      signal: options.signal,
      concurrency: options.concurrency,
      now: options.now,
      customFields: options.customFields,
    },
    [options.column],
    null,
    lease,
    options.query,
  ).finally(() => lease.release());
  generation.assertCurrent();
  options.signal?.throwIfAborted();
  return aggregator.searchValues(
    options.column,
    options.query,
    options.limit ?? 100,
  );
}

/**
 * Answers a field search from converter summaries and cached rollups alone,
 * never raw Parquet. `partial` means a summary cut the field to its top
 * values, a window has neither, or other filters apply, so counts are lower
 * bounds and `searchFieldValues` must follow for the exact list.
 */
export async function searchFieldSummaries(
  source: FileSource,
  options: SearchFieldOptions,
): Promise<{ entries: { value: Scalar; count: number }[]; partial: boolean }> {
  const { from, to, signal, column } = options;
  const otherFilters = (options.filters ?? []).filter(
    (f) => f.field !== column,
  );
  if (otherFilters.length > 0 || options.edgeOnly) {
    return { entries: [], partial: true };
  }
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);
  const customFields = options.customFields ?? [];
  const variant = customFieldsKey(customFields);
  const scope = source.cacheScope ?? "default";
  const manifest =
    options.manifest ??
    (await fetchManifest(source.fetchJson, from, to, signal));
  if (!manifest.complete) return { entries: [], partial: true };
  let files = filesOverlapping(manifest.files, fromMs, toMs);
  let summaryUses: SummaryUse[] = [];
  if (toMs - fromMs >= SUMMARY_MIN_RANGE_MS) {
    const listing =
      manifest.summaryFiles &&
      (!manifest.summaryRange ||
        (manifest.summaryRange.from === from &&
          manifest.summaryRange.to === to))
        ? { files: manifest.summaryFiles, complete: true }
        : await fetchManifest(
            source.fetchJson,
            from,
            to,
            signal,
            undefined,
            "summary",
          ).catch((error: unknown) => {
            if (signal?.aborted) throw error;
            return null;
          });
    if (listing?.complete) {
      const plan = planSummaries(files, listing.files, fromMs, toMs);
      files = plan.raw;
      summaryUses = plan.summaries;
    }
  }
  const aggregator = new Aggregator(from, to, [column]);
  let partial = false;
  for (const use of summaryUses) {
    signal?.throwIfAborted();
    const mem = decodedSummaries.get(scope)?.get(use.file.key);
    let decoded =
      mem &&
      mem.variant === variant &&
      mem.decoded.rollup.buckets.every(
        (b) => b.t >= fromMs && b.t + MINUTE_MS <= toMs,
      )
        ? mem.decoded
        : null;
    if (!decoded) {
      try {
        const { bytes } = await source.fetchFile(use.file.key, signal);
        decoded = await decodeSummary(bytes, fromMs, toMs, customFields);
        if (decoded) {
          setScopeEntry(decodedSummaries, scope, use.file.key, {
            variant,
            decoded,
            byteLength: bytes.byteLength,
          });
        }
      } catch (error) {
        if (signal?.aborted) throw error;
      }
    }
    if (decoded) aggregator.mergeSummary(decoded);
    else partial = true;
  }
  for (const file of files) {
    const rollup = (await source.readRollup?.(file.key, variant)) ?? null;
    signal?.throwIfAborted();
    if (rollup && isInteriorRollup(rollup, fromMs, toMs)) {
      aggregator.mergeRollup(rollup);
    } else {
      partial = true;
    }
  }
  return {
    entries: aggregator.searchValues(
      column,
      options.query,
      options.limit ?? 100,
    ),
    partial: partial || aggregator.isTruncated(column),
  };
}

function sendBeacon(source: FileSource, stats: FetchStats): void {
  try {
    void Promise.resolve(
      source.beacon?.({
        files_listed: stats.listed,
        files_fetched: stats.fetched,
        files_cached: stats.cached,
        cdn_hits: stats.cdnHits,
        ...(!stats.selectiveIOUnmeasured
          ? {
              rows_scanned: stats.rows,
              bytes_scanned: stats.bytes,
            }
          : {}),
        duration_ms: stats.elapsedMs,
      }),
    ).catch(() => {});
  } catch {
    // A beacon failure never fails the query.
  }
}

export const MAX_PAGES = 100;
export const queryFieldValues = searchFieldValues;
export const getFieldValues = searchFieldValues;
