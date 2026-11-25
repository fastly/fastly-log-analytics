import { parquetMetadata, type FileMetaData } from "hyparquet";
import { fetchLanes } from "../fetch-lanes";
import type { FileSource } from "./engine";
import { currentStoreMetaEpoch } from "./file-store";
import type { ManifestFile } from "./manifest";
import { readColumns, type DecodedColumns } from "./parquet";
import {
  assembleProjectedParquet, fetchCoalescedProjectedRanges, planCoalescedProjectedRanges,
  planParquetProjection, readParquetFooter, SOFT_MERGE_MAX_FILE_RATIO,
  type CoalesceOptions, type ParquetFooter,
} from "./parquet-projection";
import type { DerivedFile } from "./sources";

/** Columns every summary row is keyed by; a spec reads them whatever else it names. */
const BASE_COLUMNS = ["bundle", "bucket", "field", "value", "truncated"] as const;

/** The summary groups and columns one page reads. */
export type SummarySpec = {
  /** Canonical: two specs with the same groups and columns have the same id. */
  readonly id: string;
  readonly groups: ReadonlySet<string>;
  /** Includes the base columns. */
  readonly columns: ReadonlySet<string>;
  /** Columns read only in the row groups holding one of their groups. */
  readonly only: ReadonlyMap<string, ReadonlySet<string>>;
};

export function defineSummarySpec({ groups, columns, only = {} }: {
  groups: readonly string[];
  columns: readonly string[];
  only?: Record<string, readonly string[]>;
}): SummarySpec {
  const sortedOnly = Object.entries(only).map(([column, of]) => [column, [...new Set(of)].sort()] as const)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  for (const [column] of sortedOnly) {
    if (!columns.includes(column)) throw new Error(`Summary spec limits ${column}, which it does not read`);
  }
  const groupSet = [...new Set(groups)].sort();
  const columnSet = [...new Set([...BASE_COLUMNS, ...columns])].sort();
  return {
    id: JSON.stringify([groupSet, columnSet, sortedOnly]),
    groups: new Set(groupSet),
    columns: new Set(columnSet),
    only: new Map(sortedOnly.map(([column, of]) => [column, new Set(of)])),
  };
}

/**
 * When a projection is not worth its extra requests: files at most
 * `wholeBelowBytes` are read whole, as are files a projection would fetch
 * more than `wholeAboveShare` of. Mutable for tests.
 */
export const summaryProjectionLimits = { wholeBelowBytes: 2 * 1024 * 1024, wholeAboveShare: 0.6 };

const SUMMARY_TARGET_RANGES = 4;
const SUMMARY_SOFT_MERGE_MAX_GAP = 128 * 1024;

const LAYOUT_KEY = "flp_summary_layout";

/**
 * The groups in each row group, from the bundle writer's `flp_summary_layout`
 * (`{"v":1,"rg":[[group…]…]}`, one entry per row group; a `dim_totals` row
 * lives with the group it names). Null when absent or not that shape, which
 * readers treat as every group in every row group.
 */
export function summaryLayout(
  metadata: Pick<FileMetaData, "key_value_metadata"> & { row_groups: readonly unknown[] },
): Set<string>[] | null {
  const value = metadata.key_value_metadata?.find((entry) => entry.key === LAYOUT_KEY)?.value;
  if (value === undefined) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const { v, rg } = parsed as { v?: unknown; rg?: unknown };
  if (v !== 1 || !Array.isArray(rg) || rg.length !== metadata.row_groups.length) return null;
  if (!rg.every((groups) => Array.isArray(groups) && groups.every((group) => typeof group === "string"))) return null;
  return (rg as string[][]).map((groups) => new Set(groups));
}

export type SummaryProjectionSource = Pick<FileSource, "fetchFile" | "fetchRange" | "hasFile" | "cacheScope"> & {
  readDerivedFile?: (key: string, identity: string) => Promise<DerivedFile | null>;
  writeDerivedFile?: (key: string, identity: string, bytes: ArrayBuffer, meta: string) => Promise<void>;
};

// Summary keys are immutable (source count, rows and fingerprint), so key and size identify the bytes.
const fileIdentity = (source: SummaryProjectionSource, file: ManifestFile) =>
  [source.cacheScope ?? "default", file.key, file.size];
const partsIdentity = (source: SummaryProjectionSource, file: ManifestFile, spec: SummarySpec) =>
  JSON.stringify([...fileIdentity(source, file), "summary-proj-v1", spec.id]);

/** Whether `readSummaryProjection` can serve `spec` of `file` without a network request. */
export async function hasSummaryProjection(
  source: SummaryProjectionSource,
  file: ManifestFile,
  spec: SummarySpec,
): Promise<boolean> {
  if (await source.hasFile?.(file.key)) return true;
  if (!source.fetchRange || !source.writeDerivedFile || !(file.size > summaryProjectionLimits.wholeBelowBytes)) return false;
  return Boolean(await source.readDerivedFile?.(file.key, partsIdentity(source, file, spec)));
}

export type SummaryProjection = {
  /**
   * Standalone Parquet files that together hold every row of the spec's
   * groups (and possibly others), with at least the spec's columns: the
   * whole file, or one projection per distinct column set.
   */
  parts: ArrayBuffer[];
  whole: boolean;
  /** Served without a network request. */
  cached: boolean;
  downloadedBytes: number;
  cdn?: string | null;
};

const intersects = (a: ReadonlySet<string>, b: ReadonlySet<string>) => [...a].some((item) => b.has(item));

type Read =
  | { kind: "whole" }
  | { kind: "parts"; plans: ReturnType<typeof planParquetProjection>[]; coalesceOptions: CoalesceOptions };

function planSummary(footer: ParquetFooter, spec: SummarySpec): Read {
  const { metadata, size } = footer;
  const layout = summaryLayout(metadata);
  const wanted = new Set([...spec.groups].filter((group) => group !== "dim_totals"));
  const rowGroups = metadata.row_groups.map((_, index) => index);
  const keep = layout ? rowGroups.filter((index) => intersects(layout[index], wanted)) : rowGroups;
  if (!keep.length) keep.push(0);
  const columnsOf = (index: number) => [...spec.columns].filter((column) => {
    const of = spec.only.get(column);
    return !of || !layout || intersects(layout[index], of);
  });
  const byColumns = new Map<string, number[]>();
  for (const index of keep) {
    const id = columnsOf(index).join("\0");
    byColumns.set(id, [...byColumns.get(id) ?? [], index]);
  }
  const plans = [...byColumns.values()].map((indexes) =>
    planParquetProjection(footer, new Set(columnsOf(indexes[0])), (index) => indexes.includes(index)));
  const projected = plans.reduce((sum, plan) => sum + plan.projectedBytes, 0);
  const maxShare = summaryProjectionLimits.wholeAboveShare;
  const coalesceOptions: CoalesceOptions = {
    targetRanges: SUMMARY_TARGET_RANGES,
    softMergeMaxGap: SUMMARY_SOFT_MERGE_MAX_GAP,
    softMergeMaxFileRatio: Number.isFinite(maxShare) ? Math.min(maxShare, 0.6) : SOFT_MERGE_MAX_FILE_RATIO,
  };
  const unified = planCoalescedProjectedRanges(plans, coalesceOptions);
  const coalescedBytes = unified.reduce((sum, r) => sum + (r.end - r.start), 0) + footer.footerLength + 12;
  return projected > maxShare * size || coalescedBytes > maxShare * size
    ? { kind: "whole" }
    : { kind: "parts", plans, coalesceOptions };
}

function splitParts(stored: ArrayBuffer, meta: string, key: string): ArrayBuffer[] {
  let lengths: unknown;
  try {
    lengths = (JSON.parse(meta) as { v?: unknown; parts?: unknown }).parts;
  } catch {
    lengths = null;
  }
  if (!Array.isArray(lengths) || !lengths.every((n) => Number.isSafeInteger(n) && n > 0)
    || lengths.reduce((sum: number, n: number) => sum + n, 0) !== stored.byteLength) {
    throw new Error(`Cached summary projection of ${key} has no part lengths`);
  }
  let offset = 0;
  return (lengths as number[]).map((length) => stored.slice(offset, (offset += length)));
}

/**
 * The parts of `file` that `spec` reads: only its columns and, when the file
 * announces a layout, only the row groups holding its groups. Small files,
 * files already cached whole, files a projection would mostly cover and
 * sources without range reads are read whole instead. A projection is cached
 * under its own identity, never as the whole file.
 */
export async function readSummaryProjection(
  source: SummaryProjectionSource,
  file: ManifestFile,
  spec: SummarySpec,
  signal?: AbortSignal,
): Promise<SummaryProjection> {
  const entryStoreEpoch = currentStoreMetaEpoch();
  const assertProjectionActive = () => {
    signal?.throwIfAborted();
    if (entryStoreEpoch !== currentStoreMetaEpoch()) {
      throw new DOMException("The operation was aborted.", "AbortError");
    }
  };
  const whole = async (): Promise<SummaryProjection> => {
    assertProjectionActive();
    const { bytes, cached, cdn } = await source.fetchFile(file.key, signal);
    assertProjectionActive();
    return { parts: [bytes], whole: true, cached, downloadedBytes: cached ? 0 : bytes.byteLength, cdn };
  };
  const { fetchRange, readDerivedFile, writeDerivedFile } = source;
  if (!fetchRange || !readDerivedFile || !writeDerivedFile || !(file.size > summaryProjectionLimits.wholeBelowBytes)) {
    return whole();
  }
  assertProjectionActive();
  const hasWhole = await source.hasFile?.(file.key);
  assertProjectionActive();
  if (hasWhole) return whole();
  const identity = fileIdentity(source, file);
  const partsId = partsIdentity(source, file, spec);
  const stored = await readDerivedFile(file.key, partsId);
  assertProjectionActive();
  if (stored) {
    const rawStored = await stored.bytes();
    assertProjectionActive();
    return { parts: splitParts(rawStored, stored.meta, file.key), whole: false, cached: true, downloadedBytes: 0 };
  }
  let downloadedBytes = 0;
  const read = (start: number, end: number) => fetchLanes.run("summary", async () => {
    assertProjectionActive();
    const bytes = await fetchRange(file, start, end, signal, "high");
    assertProjectionActive();
    downloadedBytes += bytes.byteLength;
    return bytes;
  });
  const footerId = JSON.stringify([...identity, "summary-footer"]);
  const cachedFooter = await readDerivedFile(file.key, footerId);
  assertProjectionActive();
  const tailStart = cachedFooter ? Number(cachedFooter.meta) : NaN;
  const tail = cachedFooter && Number.isSafeInteger(tailStart) && tailStart >= 0
    ? { start: tailStart, bytes: await cachedFooter.bytes() } : null;
  assertProjectionActive();
  const footer = await readParquetFooter(file.size, (start, end) =>
    tail && start >= tail.start && end <= tail.start + tail.bytes.byteLength
      ? Promise.resolve(tail.bytes.slice(start - tail.start, end - tail.start)) : read(start, end));
  assertProjectionActive();
  const footerWrite = !tail && footer.tail
    ? writeDerivedFile(file.key, footerId, footer.tail.bytes, String(footer.tail.start)).catch(() => {})
    : null;
  const plan = planSummary(footer, spec);
  if (plan.kind === "whole") {
    void footerWrite;
    return whole();
  }
  const grouped = await fetchCoalescedProjectedRanges(plan.plans, read, plan.coalesceOptions);
  const parts = plan.plans.map((projection, index) => assembleProjectedParquet(projection, grouped[index]));
  assertProjectionActive();
  const joined = new Uint8Array(parts.reduce((sum, part) => sum + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    joined.set(new Uint8Array(part), offset);
    offset += part.byteLength;
  }
  assertProjectionActive();
  void writeDerivedFile(file.key, partsId, joined.buffer,
    JSON.stringify({ v: 1, parts: parts.map((part) => part.byteLength) })).catch(() => {});
  return { parts, whole: false, cached: downloadedBytes === 0, downloadedBytes };
}

function mergeDecodedParts(
  decoded: readonly DecodedColumns[],
  wanted: readonly string[],
): DecodedColumns {
  if (decoded.length === 0) return { rows: 0, columns: new Map() };
  if (decoded.length === 1) return decoded[0];
  const rows = decoded.reduce((sum, part) => sum + part.rows, 0);
  const columns = new Map<string, ArrayLike<unknown>>();
  for (const name of wanted) {
    if (!decoded.some((part) => part.columns.has(name))) continue;
    const out: unknown[] = new Array(rows);
    let offset = 0;
    for (const part of decoded) {
      const data = part.columns.get(name);
      if (data) for (let i = 0; i < part.rows; i++) out[offset + i] = data[i];
      offset += part.rows;
    }
    columns.set(name, out);
  }
  return { rows, columns };
}

async function readSummaryPart(
  part: ArrayBuffer,
  wanted: readonly string[],
  utf8: boolean,
  spec: SummarySpec | undefined,
): Promise<DecodedColumns> {
  if (!spec) return readColumns(part, wanted, utf8);
  const metadata = parquetMetadata(part);
  const layout = summaryLayout(metadata);
  if (!layout) return readColumns(part, wanted, utf8, { metadata });
  const wantedGroups = new Set([...spec.groups].filter((group) => group !== "dim_totals"));
  const keep: number[] = [];
  for (let i = 0; i < metadata.row_groups.length; i++) {
    if (intersects(layout[i], wantedGroups)) keep.push(i);
  }
  if (!keep.length && metadata.row_groups.length > 0) keep.push(0);
  if (!keep.length) return { rows: 0, columns: new Map() };

  // Group adjacent row groups in strictly ascending order so text and binary reads align row-for-row.
  const runs: { cols: string[]; rowGroups: number[]; rows: number }[] = [];
  let prevKey: string | null = null;
  for (const index of keep) {
    const cols = wanted.filter((column) => {
      const of = spec.only.get(column);
      return !of || intersects(layout[index], of);
    });
    const key = cols.join("\0");
    const rgRows = Number(metadata.row_groups[index].num_rows);
    if (key === prevKey && runs.length > 0) {
      const last = runs[runs.length - 1];
      last.rowGroups.push(index);
      last.rows += rgRows;
    } else {
      runs.push({ cols, rowGroups: [index], rows: rgRows });
      prevKey = key;
    }
  }
  if (runs.length === 1) {
    const onlyRun = runs[0];
    if (!onlyRun.cols.length) return { rows: onlyRun.rows, columns: new Map() };
    return readColumns(part, onlyRun.cols, utf8, { metadata, rowGroups: onlyRun.rowGroups });
  }
  const segments = await Promise.all(
    runs.map((run) =>
      run.cols.length
        ? readColumns(part, run.cols, utf8, { metadata, rowGroups: run.rowGroups })
        : Promise.resolve<DecodedColumns>({ rows: run.rows, columns: new Map() }),
    ),
  );
  return mergeDecodedParts(segments, wanted);
}

/**
 * `wanted` columns across `parts`, rows in part order. A column one part
 * lacks reads as undefined for that part's rows. When `spec` is supplied and a
 * part carries `flp_summary_layout`, only the row groups and per-group columns
 * needed by `spec` are decoded.
 */
export async function readSummaryColumns(
  parts: readonly ArrayBuffer[],
  wanted: readonly string[],
  utf8 = true,
  spec?: SummarySpec,
): Promise<DecodedColumns> {
  if (!wanted.length || !parts.length) return { rows: 0, columns: new Map() };
  const decoded = await Promise.all(parts.map((part) => readSummaryPart(part, wanted, utf8, spec)));
  return mergeDecodedParts(decoded, wanted);
}
