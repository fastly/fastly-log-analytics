import { FIELD_DUCKDB_TYPES } from "./duckdb";
import type { FileSource } from "./engine";
import type { DashboardFilter } from "./filters";
import type { ManifestFile } from "./manifest";
import {
  assembleProjectedParquet, fetchProjectedRanges, planParquetProjection, readParquetFooter, readsWholeFirst,
  readWholeParquetFooter, timestampRowGroupFilter, type ParquetFooter,
} from "./parquet-projection";
import type { BrowserFileSource } from "./sources";
import type { CustomField } from "./summaries";

// Every selective dashboard query reads these, whatever its cards and filters.
const QUERY_COLUMNS = ["timestamp", "status", "cache", "resp_bytes", "edge"];
// Projected ranges are mostly a few KiB, so their round trips, not bandwidth, bound a cold load.
const RANGE_CONCURRENCY = 16;

/**
 * The raw columns a selective dashboard query reads for these cards and
 * filters, or null when one names a column it can't type, so the caller reads
 * the whole file.
 */
export function dashboardScanColumns(
  fields: readonly string[],
  filters: readonly DashboardFilter[],
  customFields: readonly CustomField[],
): string[] | null {
  const custom = new Set(customFields.map((field) => field.id));
  const columns = new Set([...QUERY_COLUMNS, ...fields, ...filters.map((filter) => filter.field)]);
  for (const column of columns) {
    if (column !== "timestamp" && !Object.hasOwn(FIELD_DUCKDB_TYPES, column) && !custom.has(column)) return null;
  }
  return [...columns];
}

export type ProjectionSource = Pick<FileSource, "cacheScope" | "fetchRange"> &
  Partial<Pick<FileSource, "fetchFile">> &
  Partial<Pick<BrowserFileSource, "readDerivedFile" | "writeDerivedFile">>;

/** A file's projection, sized before its column ranges are fetched. */
export type ProjectedRead = {
  /** Upper bound of the bytes `read` holds. */
  bytes: number;
  read(): Promise<{ bytes: ArrayBuffer; cached: boolean; downloaded: number }>;
};

/**
 * Reads files as standalone Parquet holding only `columns`, through footer-planned
 * byte ranges, or from one whole-file read when `readsWholeFirst` picks the file and the
 * source can fetch whole files. Footers and projections persist as derived files of the source file.
 * With `window`, row groups whose timestamp statistics lie wholly outside it are
 * left out too; the caller still clips rows to the window.
 */
export function projectedReader(
  source: ProjectionSource,
  columns: readonly string[],
  signal?: AbortSignal,
  window?: { fromMs: number; toMs: number },
): (file: ManifestFile) => Promise<ProjectedRead> {
  const fetchFrom = source.fetchRange;
  if (!fetchFrom) throw new Error("Projected Cloud reads need a range-capable file source");
  const wanted = new Set(columns);
  const columnsKey = [...wanted].sort();
  let slots = RANGE_CONCURRENCY;
  const waiting: (() => void)[] = [];
  const fetchRange = async (file: ManifestFile, start: number, end: number) => {
    if (slots === 0) await new Promise<void>((resolve) => waiting.push(resolve));
    else slots--;
    try {
      signal?.throwIfAborted();
      return await fetchFrom(file, start, end, signal);
    } finally {
      const next = waiting.shift();
      if (next) next();
      else slots++;
    }
  };
  return async (file) => {
    if (!(Number.isSafeInteger(file.size) && file.size > 0)) {
      throw new Error(`Cloud file listing has no size to project ${file.key}`);
    }
    const identity = ["dashboard", source.cacheScope ?? "", file.key, file.etag ?? "", file.size, file.schema_version ?? ""];
    const footerKey = JSON.stringify([...identity, "footer"]);
    const projectionKey = (rowGroups?: number[]) =>
      JSON.stringify([...identity, "projection", ...columnsKey, ...(rowGroups ? [{ rowGroups }] : [])]);
    // Without a window the projection's identity needs no footer.
    const stored = window ? null : await source.readDerivedFile?.(file.key, projectionKey());
    if (stored) {
      return { bytes: stored.byteLength, read: async () => ({ bytes: await stored.bytes(), cached: true, downloaded: 0 }) };
    }
    const plan = (footer: ParquetFooter) => planParquetProjection(footer, wanted, window
      ? timestampRowGroupFilter(footer.metadata, window.fromMs, window.toMs) : undefined);
    const keyOf = (footer: ParquetFooter, rowGroups: number[]) =>
      projectionKey(rowGroups.length < footer.metadata.row_groups.length ? rowGroups : undefined);
    const fetchFile = source.fetchFile;
    if (fetchFile && readsWholeFirst(file)) {
      return {
        // The whole file is held until its projection is built.
        bytes: file.size,
        read: async () => {
          signal?.throwIfAborted();
          const whole = await fetchFile(file.key, signal);
          if (whole.bytes.byteLength !== file.size) {
            throw new Error(`Whole-file read of ${file.key} returned ${whole.bytes.byteLength} bytes, not ${file.size}`);
          }
          const footer = await readWholeParquetFooter(whole.bytes);
          const projection = plan(footer);
          const bytes = assembleProjectedParquet(projection,
            await fetchProjectedRanges(projection, async (start, end) => whole.bytes.slice(start, end)));
          void source.writeDerivedFile?.(file.key, keyOf(footer, projection.rowGroups), bytes,
            JSON.stringify(projection.presentColumns));
          return { bytes, cached: whole.cached, downloaded: whole.cached ? 0 : whole.bytes.byteLength };
        },
      };
    }
    let downloaded = 0;
    const counted = async (start: number, end: number) => {
      const part = await fetchRange(file, start, end);
      downloaded += part.byteLength;
      return part;
    };
    const cachedTail = await source.readDerivedFile?.(file.key, footerKey);
    const tailStart = cachedTail ? Number(cachedTail.meta) : NaN;
    const tail = cachedTail && Number.isSafeInteger(tailStart) && tailStart >= 0
      ? { start: tailStart, bytes: await cachedTail.bytes() } : null;
    const footer: ParquetFooter = await readParquetFooter(file.size, (start, end) =>
      tail && start >= tail.start && end <= tail.start + tail.bytes.byteLength
        ? Promise.resolve(tail.bytes.slice(start - tail.start, end - tail.start)) : counted(start, end));
    if (!tail && footer.tail) {
      void source.writeDerivedFile?.(file.key, footerKey, footer.tail.bytes, String(footer.tail.start));
    }
    const projection = plan(footer);
    const key = keyOf(footer, projection.rowGroups);
    const windowed = window ? await source.readDerivedFile?.(file.key, key) : null;
    if (windowed) {
      return { bytes: windowed.byteLength, read: async () => ({ bytes: await windowed.bytes(), cached: true, downloaded: 0 }) };
    }
    return {
      bytes: projection.projectedBytes,
      read: async () => {
        const bytes = assembleProjectedParquet(projection, await fetchProjectedRanges(projection, counted));
        void source.writeDerivedFile?.(file.key, key, bytes, JSON.stringify(projection.presentColumns));
        return { bytes, cached: false, downloaded };
      },
    };
  };
}
