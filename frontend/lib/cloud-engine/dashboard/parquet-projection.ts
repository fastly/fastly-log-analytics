import { parquetMetadata, type FileMetaData, type SchemaElement } from "hyparquet";
import { ByteWriter } from "hyparquet-writer";
import { writeMetadata } from "hyparquet-writer/src/metadata.js";
import { MAX_REGISTERED_BYTES } from "./budget";

/** Tail probe for multi-MiB 100-column compacts whose Parquet footers exceed 256 KiB. */
export const LARGE_FOOTER_PROBE_BYTES = 512 * 1024;
/** One tail read usually covers the whole footer, including 100-column multi-row-group compacts. */
export const FOOTER_PROBE_BYTES = 256 * 1024;
/** Smaller tail probe for sub-300 KiB files so footer probes don't download the entire file. */
export const SMALL_FOOTER_PROBE_BYTES = 64 * 1024;
export function defaultFooterProbeBytes(size: number): number {
  if (size >= 4 * 1024 * 1024) return LARGE_FOOTER_PROBE_BYTES;
  return size >= 300 * 1024 ? FOOTER_PROBE_BYTES : SMALL_FOOTER_PROBE_BYTES;
}
/**
 * Needed chunks closer than this always share one HTTP range. Compacted files
 * hold many row groups of ~100 columns, so most chunks are a few KiB and a
 * wider gap pulls in the unneeded columns between them.
 */
export const RANGE_MERGE_GAP = 8 * 1024;
/**
 * What one more range request costs, in bytes of transfer. Any gap up to this
 * size merges unconditionally, eliminating small intra-row-group splits without
 * pulling in wide unneeded columns.
 */
export const RANGE_REQUEST_COST_BYTES = 32 * 1024;
/**
 * Soft target for HTTP ranges per projected file. Above this count, gaps up to
 * [`SOFT_MERGE_MAX_GAP`] are merged greedily while planned bytes stay within
 * [`SOFT_MERGE_MAX_FILE_RATIO`] of the file size, reducing HTTP 206 counts
 * without swallowing wide unneeded columns.
 */
export const TARGET_RANGES_PER_FILE = 8;
/** Largest gap merged when reducing a file's ranges toward [`TARGET_RANGES_PER_FILE`]. */
export const SOFT_MERGE_MAX_GAP = 128 * 1024;
/** Maximum fraction of file size Pass 2 may reach while coalescing toward [`TARGET_RANGES_PER_FILE`]. */
export const SOFT_MERGE_MAX_FILE_RATIO = 0.40;
/**
 * Hard upper bound on HTTP ranges issued for a single projected Parquet file
 * (4 waves at RANGE_CONCURRENCY = 16). Preserves projected byte savings on
 * heavily compacted files with wide unneeded columns while capping worst-case
 * HTTP 206 fan-out.
 */
export const MAX_RANGES_PER_FILE = 64;

/**
 * Projected reads of files up to these sizes fetch the whole file in one request instead of two
 * footer reads and then up to [`MAX_RANGES_PER_FILE`] column ranges. Any file up to
 * `anyUpToBytes` costs little more than its footer probe. A closed hour's compacted files up to
 * `closedHourUpToBytes` held many row groups that needed 15–17 requests for about half their
 * bytes; larger compacts keep ranges, where a dashboard projection reads far fewer bytes.
 * Open-hour compacts above `anyUpToBytes` keep ranges too.
 */
export const wholeReadLimits = { anyUpToBytes: 256 * 1024, closedHourUpToBytes: 2 * 1024 * 1024 };

const HOUR_MS = 3_600_000;

/** Whether a projected read of `file` should fetch it whole, per [`wholeReadLimits`]. */
export function readsWholeFirst(file: { size: number; end: string; key?: string }, nowMs = Date.now()): boolean {
  if (!(Number.isSafeInteger(file.size) && file.size > 0)) return false;
  if (file.size <= wholeReadLimits.anyUpToBytes) return true;
  if (file.size <= wholeReadLimits.closedHourUpToBytes) {
    if (Date.parse(file.end) <= Math.floor(nowMs / HOUR_MS) * HOUR_MS) return true;
    if (file.key && /(?:^|\/)compact=/.test(file.key)) return true;
  }
  return false;
}

export const WHOLE_FALLBACK_RANGE_RATIO = 0.85;
export const WHOLE_FALLBACK_MANY_RANGES_MIN = 8;
export const WHOLE_FALLBACK_MANY_RANGES_RATIO = 0.60;
export const WHOLE_FALLBACK_HIGH_FANOUT_MIN = 16;
export const WHOLE_FALLBACK_HIGH_FANOUT_RATIO = 0.45;
export const WHOLE_FALLBACK_HIGH_FANOUT_MAX_BYTES = 8 * 1024 * 1024;
export const WHOLE_FALLBACK_EXTREME_FANOUT_MIN = 24;
export const WHOLE_FALLBACK_EXTREME_FANOUT_RATIO = 0.30;
export const WHOLE_FALLBACK_EXTREME_FANOUT_MAX_BYTES = 16 * 1024 * 1024;

/**
 * Whether a post-footer projection plan should fall back to a single shared whole-file fetch
 * instead of issuing many HTTP 206 range requests on a multi-row-group file.
 */
export function shouldFallbackToWhole(
  plan: { ranges: readonly { start: number; end: number }[] },
  fileSize: number,
  plannedRangeBytes: number,
  maxFileBytes = MAX_REGISTERED_BYTES,
): boolean {
  if (plan.ranges.length === 0 || fileSize <= 0 || fileSize > maxFileBytes) return false;
  if (plannedRangeBytes >= fileSize * WHOLE_FALLBACK_RANGE_RATIO) return true;
  if (plan.ranges.length >= WHOLE_FALLBACK_MANY_RANGES_MIN
    && plannedRangeBytes >= fileSize * WHOLE_FALLBACK_MANY_RANGES_RATIO) {
    return true;
  }
  if (plan.ranges.length >= WHOLE_FALLBACK_EXTREME_FANOUT_MIN
    && (plannedRangeBytes >= fileSize * WHOLE_FALLBACK_EXTREME_FANOUT_RATIO
      || fileSize <= WHOLE_FALLBACK_EXTREME_FANOUT_MAX_BYTES)) {
    return true;
  }
  return plan.ranges.length >= WHOLE_FALLBACK_HIGH_FANOUT_MIN
    && (plannedRangeBytes >= fileSize * WHOLE_FALLBACK_HIGH_FANOUT_RATIO
      || fileSize <= WHOLE_FALLBACK_HIGH_FANOUT_MAX_BYTES);
}

/** The footer of a Parquet file held whole. */
export function readWholeParquetFooter(bytes: ArrayBuffer): Promise<ParquetFooter> {
  return readParquetFooter(bytes.byteLength, async (start, end) => bytes.slice(start, end));
}

export type FetchRange = (start: number, end: number) => Promise<ArrayBuffer>;

export type ParquetFooter = {
  metadata: FileMetaData;
  footerLength: number;
  size: number;
  /** The file's last bytes as read for the footer, from `start` to its end. */
  tail?: { start: number; bytes: ArrayBuffer };
};

type Chunk = { rowGroup: number; column: number; start: number; length: number };

export type ParquetProjection = {
  footer: ParquetFooter;
  columns: ReadonlySet<string>;
  /** Indexes of the original row groups the rebuilt file keeps, in file order. */
  rowGroups: number[];
  /** Top-level column names of the original file. */
  presentColumns: string[];
  chunks: Chunk[];
  /** Coalesced byte ranges that cover every needed chunk (end exclusive). */
  ranges: { start: number; end: number }[];
  /** Compressed bytes of the needed column chunks, measured from the footer. */
  neededBytes: number;
  /** Upper bound of the rebuilt file: needed chunks plus the original footer and framing. */
  projectedBytes: number;
};

const MAGIC = 0x31524150; // "PAR1" little-endian

export async function readParquetFooter(
  size: number,
  fetchRange: FetchRange,
  probeBytes = defaultFooterProbeBytes(size),
): Promise<ParquetFooter> {
  if (!Number.isSafeInteger(size) || size < 12) throw new Error("Parquet file is too small");
  const probeStart = Math.max(0, size - probeBytes);
  let tail = await fetchRange(probeStart, size);
  const view = new DataView(tail);
  if (view.getUint32(tail.byteLength - 4, true) !== MAGIC) throw new Error("Parquet footer magic is missing");
  const footerLength = view.getUint32(tail.byteLength - 8, true);
  if (footerLength + 12 > size) throw new Error("Parquet footer length exceeds the file");
  let tailStart = probeStart;
  if (footerLength + 8 > tail.byteLength) {
    tailStart = size - footerLength - 8;
    const head = new Uint8Array(await fetchRange(tailStart, probeStart));
    const whole = new Uint8Array(size - tailStart);
    whole.set(head);
    whole.set(new Uint8Array(tail), head.byteLength);
    tail = whole.buffer;
  }
  const footer = tail.slice(tail.byteLength - footerLength - 8);
  return {
    metadata: parquetMetadata(footer, { geoparquet: false }), footerLength, size,
    tail: { start: tailStart, bytes: tail },
  };
}

function topLevel(schema: readonly SchemaElement[]): { name: string; leaves: number }[] {
  const out: { name: string; leaves: number }[] = [];
  let index = 1;
  const walk = (): number => {
    const element = schema[index++];
    if (!element) throw new Error("Parquet schema is truncated");
    if (!element.num_children) return 1;
    let leaves = 0;
    for (let i = 0; i < element.num_children; i++) leaves += walk();
    return leaves;
  };
  for (let i = 0; i < (schema[0]?.num_children ?? 0); i++) {
    const name = schema[index]?.name;
    out.push({ name, leaves: walk() });
  }
  return out;
}

const chunkStart = (meta: { data_page_offset: bigint; dictionary_page_offset?: bigint }) => {
  const data = Number(meta.data_page_offset);
  const dictionary = meta.dictionary_page_offset === undefined ? NaN : Number(meta.dictionary_page_offset);
  return dictionary >= 4 && dictionary < data ? dictionary : data;
};

/**
 * Whether a row group may hold a `timestamp` in [fromMs, toMs]: false only when
 * its footer statistics prove every value lies outside, so a group without
 * statistics is kept.
 */
export function timestampRowGroupFilter(
  metadata: FileMetaData,
  fromMs: number,
  toMs: number,
): (rowGroup: number) => boolean {
  return (rowGroup) => {
    const chunk = metadata.row_groups[rowGroup]?.columns.find(
      (column) => column.meta_data?.path_in_schema.length === 1 && column.meta_data.path_in_schema[0] === "timestamp");
    const stats = chunk?.meta_data?.statistics;
    const min = stats?.min_value;
    const max = stats?.max_value;
    // Microsecond values floor to their millisecond, so "max < fromMs" stays exact.
    if (!(min instanceof Date) || !(max instanceof Date)) return true;
    return !(max.getTime() < fromMs || min.getTime() > toMs);
  };
}

/** Exact-value include/exclude filter predicate used for Parquet row-group statistics pruning. */
export type RowGroupFilterPredicate = {
  field: string;
  mode: "include" | "exclude";
  values: readonly string[];
};

const utf8Encoder = new TextEncoder();
const NUMERIC_LITERAL = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const INTEGER_LITERAL = /^[+-]?\d+$/;

function compareUtf8(a: string, b: string): number {
  if (a === b) return 0;
  // Fast path for pure ASCII: JS UTF-16 code units equal UTF-8 bytes.
  let ascii = true;
  for (let i = 0; i < a.length; i++) if (a.charCodeAt(i) > 0x7f) { ascii = false; break; }
  if (ascii) {
    for (let i = 0; i < b.length; i++) if (b.charCodeAt(i) > 0x7f) { ascii = false; break; }
  }
  if (ascii) return a < b ? -1 : 1;
  const ab = utf8Encoder.encode(a);
  const bb = utf8Encoder.encode(b);
  const len = Math.min(ab.length, bb.length);
  for (let i = 0; i < len; i++) {
    if (ab[i] !== bb[i]) return ab[i] < bb[i] ? -1 : 1;
  }
  return ab.length < bb.length ? -1 : ab.length > bb.length ? 1 : 0;
}

function numericInRange(raw: string, min: number | bigint, max: number | bigint): boolean {
  const trimmed = raw.trim();
  if (!NUMERIC_LITERAL.test(trimmed)) return false;
  if (typeof min === "bigint" && typeof max === "bigint" && INTEGER_LITERAL.test(trimmed)) {
    try {
      const value = BigInt(trimmed);
      return value >= min && value <= max;
    } catch {
      return false;
    }
  }
  const value = Number(trimmed);
  return Number.isFinite(value) && value >= Number(min) && value <= Number(max);
}

function numericEquals(raw: string, target: number | bigint): boolean {
  const trimmed = raw.trim();
  if (!NUMERIC_LITERAL.test(trimmed)) return false;
  if (typeof target === "bigint" && INTEGER_LITERAL.test(trimmed)) {
    try {
      return BigInt(trimmed) === target;
    } catch {
      return false;
    }
  }
  const value = Number(trimmed);
  return Number.isFinite(value) && value === Number(target);
}

/**
 * Whether a row group may hold a row matching all exact-value `filters`: false
 * only when Parquet column chunk statistics prove zero rows can match. Groups
 * lacking statistics, missing the column, or using wildcards are conservatively kept.
 */
export function filterRowGroupFilter(
  metadata: FileMetaData,
  filters: readonly RowGroupFilterPredicate[],
): (rowGroup: number) => boolean {
  const active = filters.filter(
    (f) => f.values.length > 0 && f.field !== "conn_requests" && f.values.every((v) => v.length > 0 && !v.includes("*")),
  );
  if (active.length === 0) return () => true;
  return (rowGroup) => {
    const group = metadata.row_groups[rowGroup];
    if (!group) return true;
    for (const predicate of active) {
      const chunk = group.columns.find(
        (column) => column.meta_data?.path_in_schema.length === 1 && column.meta_data.path_in_schema[0] === predicate.field,
      );
      const meta = chunk?.meta_data;
      const stats = meta?.statistics;
      if (!meta || !stats) continue;
      const nullCount = stats.null_count;
      const numValues = meta.num_values;
      if (predicate.mode === "include" && nullCount !== undefined && numValues !== undefined && BigInt(nullCount) >= BigInt(numValues)) {
        return false;
      }
      const min = stats.min_value;
      const max = stats.max_value;
      if (min === undefined || max === undefined || min === null || max === null) continue;
      const zeroNulls = nullCount !== undefined && BigInt(nullCount) === BigInt(0);
      if (typeof min === "string" && typeof max === "string") {
        if (predicate.mode === "include") {
          if (!predicate.values.some((v) => compareUtf8(v, min) >= 0 && compareUtf8(v, max) <= 0)) return false;
        } else if (min === max && zeroNulls && predicate.values.includes(min)) {
          return false;
        }
      } else if ((typeof min === "number" || typeof min === "bigint") && (typeof max === "number" || typeof max === "bigint")) {
        if (predicate.mode === "include") {
          if (!predicate.values.some((v) => numericInRange(v, min, max))) return false;
        } else if (min === max && zeroNulls && predicate.values.some((v) => numericEquals(v, min))) {
          return false;
        }
      } else if (typeof min === "boolean" && typeof max === "boolean") {
        const parsed = predicate.values.flatMap((v) => v === "true" ? [true] : v === "false" ? [false] : []);
        if (predicate.mode === "include") {
          if (parsed.length === 0 || (min === max && !parsed.includes(min))) return false;
        } else if (min === max && zeroNulls && parsed.includes(min)) {
          return false;
        }
      }
    }
    return true;
  };
}

/**
 * The column chunks of `columns` to fetch, in the row groups `keepRowGroup`
 * accepts (every row group by default), coalesced into byte ranges.
 */
export function planParquetProjection(
  footer: ParquetFooter,
  columns: ReadonlySet<string>,
  keepRowGroup: (rowGroup: number) => boolean = () => true,
): ParquetProjection {
  const { metadata, footerLength, size } = footer;
  const dataEnd = size - footerLength - 8;
  const chunks: Chunk[] = [];
  const rowGroups = metadata.row_groups.map((_, rowGroup) => rowGroup).filter((rowGroup) => keepRowGroup(rowGroup));
  rowGroups.forEach((rowGroup) => metadata.row_groups[rowGroup].columns.forEach((chunk, column) => {
    const meta = chunk.meta_data;
    if (!meta || chunk.file_path) throw new Error("Parquet column chunk metadata is not inline");
    if (!columns.has(meta.path_in_schema[0])) return;
    const start = chunkStart(meta);
    const length = Number(meta.total_compressed_size);
    if (!(start >= 4 && length > 0 && start + length <= dataEnd)) throw new Error("Parquet column chunk is outside the file");
    chunks.push({ rowGroup, column, start, length });
  }));
  const ranges = coalesceRanges([...chunks].sort((a, b) => a.start - b.start), size);
  const neededBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  return {
    footer, columns, rowGroups, chunks, ranges, neededBytes,
    presentColumns: topLevel(metadata.schema).map((field) => field.name),
    projectedBytes: neededBytes + footerLength + 12,
  };
}

export type CoalesceOptions = {
  targetRanges?: number;
  softMergeMaxGap?: number;
  softMergeMaxFileRatio?: number;
};

/**
 * The cheapest coalescing of the chunks (sorted by start) by bytes plus request cost,
 * subject to at most [`MAX_RANGES_PER_FILE`] ranges.
 */
function coalesceRanges(
  chunks: readonly { start: number; length: number }[],
  fileSize: number,
  options?: CoalesceOptions,
): { start: number; end: number }[] {
  if (chunks.length === 0) return [];
  const targetRanges = options?.targetRanges ?? TARGET_RANGES_PER_FILE;
  const softMergeMaxGap = options?.softMergeMaxGap ?? SOFT_MERGE_MAX_GAP;
  const softMergeMaxFileRatio = options?.softMergeMaxFileRatio ?? SOFT_MERGE_MAX_FILE_RATIO;
  // Merging any gap <= max(RANGE_MERGE_GAP, RANGE_REQUEST_COST_BYTES) strictly lowers or
  // preserves `bytes + ranges * RANGE_REQUEST_COST_BYTES` without pulling in large gaps.
  const baseGap = Math.max(RANGE_MERGE_GAP, RANGE_REQUEST_COST_BYTES);
  const ranges: { start: number; end: number }[] = [];
  for (const chunk of chunks) {
    const last = ranges[ranges.length - 1];
    if (last && chunk.start - last.end <= baseGap) {
      last.end = Math.max(last.end, chunk.start + chunk.length);
    } else {
      ranges.push({ start: chunk.start, end: chunk.start + chunk.length });
    }
  }
  let totalBytes = ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
  const maxSoftBytes = Math.floor(fileSize * softMergeMaxFileRatio);
  // Pass 2 (soft target): while above targetRanges, merge the smallest
  // remaining gap as long as it stays <= softMergeMaxGap and planned bytes stay
  // within softMergeMaxFileRatio of the file size.
  while (ranges.length > targetRanges) {
    let minIndex = 0;
    let minGap = Infinity;
    for (let i = 0; i < ranges.length - 1; i++) {
      const gap = ranges[i + 1].start - ranges[i].end;
      if (gap < minGap) {
        minGap = gap;
        minIndex = i;
      }
    }
    if (minGap > softMergeMaxGap || totalBytes + minGap > maxSoftBytes) break;
    ranges[minIndex].end = ranges[minIndex + 1].end;
    ranges.splice(minIndex + 1, 1);
    totalBytes += minGap;
  }
  // Pass 3 (hard cap): if still above MAX_RANGES_PER_FILE, greedily merge only the
  // smallest remaining gap. This avoids the power-of-2 cliff that collapses uniform
  // row-group compacts into one whole-file range.
  while (ranges.length > MAX_RANGES_PER_FILE) {
    let minIndex = 0;
    let minGap = Infinity;
    for (let i = 0; i < ranges.length - 1; i++) {
      const gap = ranges[i + 1].start - ranges[i].end;
      if (gap < minGap) {
        minGap = gap;
        minIndex = i;
      }
    }
    ranges[minIndex].end = ranges[minIndex + 1].end;
    ranges.splice(minIndex + 1, 1);
  }
  return ranges;
}

function fetchRangeWithTail(
  tail: ParquetFooter["tail"],
  start: number,
  end: number,
  fetchRange: FetchRange,
): Promise<ArrayBuffer> {
  if (!tail || end <= tail.start) return fetchRange(start, end);
  if (start >= tail.start) return Promise.resolve(tail.bytes.slice(start - tail.start, end - tail.start));
  return fetchRange(start, tail.start).then((headBuf) => {
    const head = new Uint8Array(headBuf);
    const out = new Uint8Array(end - start);
    out.set(head);
    out.set(new Uint8Array(tail.bytes, 0, end - tail.start), head.byteLength);
    return out.buffer;
  });
}

/** The planned ranges' bytes, fetching only what the footer read didn't already cover. */
export async function fetchProjectedRanges(plan: ParquetProjection, fetchRange: FetchRange): Promise<ArrayBuffer[]> {
  return Promise.all(plan.ranges.map(({ start, end }) => fetchRangeWithTail(plan.footer.tail, start, end, fetchRange)));
}

/** Plans the coalesced HTTP ranges needed to satisfy multiple projections of the same Parquet file. */
export function planCoalescedProjectedRanges(
  plans: readonly ParquetProjection[],
  options?: CoalesceOptions,
): { start: number; end: number }[] {
  if (plans.length === 0) return [];
  const spans = plans
    .flatMap((plan) => plan.ranges.map((r) => ({ start: r.start, length: r.end - r.start })))
    .sort((a, b) => a.start - b.start || a.length - b.length);
  return coalesceRanges(spans, plans[0].footer.size, options);
}

/**
 * Fetches ranges for multiple projections of the same Parquet file in one coalesced pass,
 * pooling adjacent ranges across plans so each file issues minimal HTTP 206 requests.
 */
export async function fetchCoalescedProjectedRanges(
  plans: readonly ParquetProjection[],
  fetchRange: FetchRange,
  options?: CoalesceOptions,
): Promise<ArrayBuffer[][]> {
  if (plans.length === 0) return [];
  if (plans.length === 1 && !options) return [await fetchProjectedRanges(plans[0], fetchRange)];
  const unified = planCoalescedProjectedRanges(plans, options);
  const tail = plans[0].footer.tail;
  const fetched = await Promise.all(
    unified.map(({ start, end }) => fetchRangeWithTail(tail, start, end, fetchRange)),
  );
  return plans.map((plan) =>
    plan.ranges.map((r) => {
      const idx = unified.findIndex((u) => u.start <= r.start && r.end <= u.end);
      const u = unified[idx];
      return fetched[idx].slice(r.start - u.start, r.end - u.start);
    }),
  );
}

/** The schema of the file `assembleProjectedParquet` builds from this plan. */
export function projectedSchema(plan: ParquetProjection): SchemaElement[] {
  const { metadata } = plan.footer;
  const schema: SchemaElement[] = [{ ...metadata.schema[0], num_children: 0 }];
  let index = 1;
  for (const field of topLevel(metadata.schema)) {
    const subtree = [] as SchemaElement[];
    const take = (): void => {
      const element = metadata.schema[index++];
      subtree.push(element);
      for (let i = 0; i < (element.num_children ?? 0); i++) take();
    };
    take();
    if (!plan.columns.has(field.name)) continue;
    schema.push(...subtree);
    schema[0].num_children!++;
  }
  return schema;
}

/** What a dashboard query needs to know of a Parquet file's DuckDB schema. */
export type ParquetQuerySchema = {
  /** Top-level column names, in file order. */
  columns: string[];
  /** DuckDB's type for the `timestamp` column. */
  timestampType: string;
  /** Equal for files DuckDB reads with the same schema. */
  signature: string;
};

/**
 * The query schema DuckDB would DESCRIBE for a file with this footer schema, or
 * null when the footer alone doesn't settle `timestamp`'s DuckDB type: only a
 * millisecond or microsecond INT64 with a logical TIMESTAMP type does.
 */
export function parquetQuerySchema(schema: readonly SchemaElement[]): ParquetQuerySchema | null {
  const fields = topLevel(schema);
  let index = 1;
  let timestamp: SchemaElement | undefined;
  for (const field of fields) {
    if (field.name === "timestamp") timestamp = schema[index];
    index += countElements(schema, index);
  }
  const logical = timestamp?.logical_type;
  if (timestamp?.type !== "INT64" || logical?.type !== "TIMESTAMP" || timestamp.num_children
    || (logical.unit !== "MILLIS" && logical.unit !== "MICROS")) return null;
  const signature = JSON.stringify(schema.slice(1).map((element) => [
    element.name, element.type ?? null, element.type_length ?? null, element.repetition_type ?? null,
    element.converted_type ?? null, element.scale ?? null, element.precision ?? null,
    element.logical_type ?? null, element.num_children ?? null,
  ]));
  return {
    columns: fields.map((field) => field.name),
    timestampType: logical.isAdjustedToUTC ? "TIMESTAMP WITH TIME ZONE" : "TIMESTAMP",
    signature,
  };
}

function countElements(schema: readonly SchemaElement[], at: number): number {
  let count = 1;
  for (let i = 0; i < (schema[at]?.num_children ?? 0); i++) count += countElements(schema, at + count);
  return count;
}

/**
 * A standalone Parquet file holding only the planned column chunks, byte for
 * byte, under a footer rewritten for their new offsets, with the planned row
 * groups and the original key-value metadata. Statistics, page indexes and
 * bloom filters are dropped because they point into the original.
 */
export function assembleProjectedParquet(plan: ParquetProjection, rangeBytes: readonly ArrayBuffer[]): ArrayBuffer {
  if (rangeBytes.length !== plan.ranges.length) throw new Error("Projected Parquet ranges are incomplete");
  const { metadata } = plan.footer;
  const schema = projectedSchema(plan);
  const writer = new ByteWriter();
  writer.appendUint32(MAGIC);
  const byGroup = new Map<number, Chunk[]>();
  for (const chunk of plan.chunks) byGroup.set(chunk.rowGroup, [...byGroup.get(chunk.rowGroup) ?? [], chunk]);
  const rowGroups = plan.rowGroups.map((rowGroup) => {
    const group = metadata.row_groups[rowGroup];
    const kept = byGroup.get(rowGroup) ?? [];
    const groupOffset = writer.offset;
    const columns = kept.map((chunk) => {
      const range = plan.ranges.findIndex((r) => r.start <= chunk.start && chunk.start + chunk.length <= r.end);
      if (range < 0) throw new Error("Projected Parquet chunk has no fetched range");
      const source = rangeBytes[range];
      if (source.byteLength !== plan.ranges[range].end - plan.ranges[range].start)
        throw new Error("Projected Parquet range has the wrong length");
      const at = chunk.start - plan.ranges[range].start;
      const delta = BigInt(writer.offset - chunk.start);
      const original = group.columns[chunk.column];
      const meta = original.meta_data!;
      const shiftedDictionary = meta.dictionary_page_offset !== undefined && Number(meta.dictionary_page_offset) >= 4
        ? meta.dictionary_page_offset + delta : undefined;
      const out = {
        file_offset: BigInt(writer.offset),
        meta_data: {
          type: meta.type,
          encodings: meta.encodings,
          path_in_schema: meta.path_in_schema,
          codec: meta.codec,
          num_values: meta.num_values,
          total_uncompressed_size: meta.total_uncompressed_size,
          total_compressed_size: meta.total_compressed_size,
          data_page_offset: meta.data_page_offset + delta,
          ...(shiftedDictionary !== undefined ? { dictionary_page_offset: shiftedDictionary } : {}),
          ...(meta.encoding_stats ? { encoding_stats: meta.encoding_stats } : {}),
        },
      };
      writer.appendBytes(new Uint8Array(source, at, chunk.length));
      return out;
    });
    return {
      columns,
      num_rows: group.num_rows,
      total_byte_size: columns.reduce((sum, chunk) => sum + chunk.meta_data.total_uncompressed_size, BigInt(0)),
      total_compressed_size: columns.reduce((sum, chunk) => sum + chunk.meta_data.total_compressed_size, BigInt(0)),
      file_offset: BigInt(groupOffset),
    };
  });
  writeMetadata(writer, {
    version: metadata.version,
    schema,
    num_rows: rowGroups.reduce((sum, group) => sum + group.num_rows, BigInt(0)),
    row_groups: rowGroups,
    ...(metadata.key_value_metadata ? { key_value_metadata: metadata.key_value_metadata } : {}),
    ...(metadata.created_by ? { created_by: metadata.created_by } : {}),
    metadata_length: 0,
  });
  writer.appendUint32(MAGIC);
  writer.finish();
  return writer.getBuffer();
}
