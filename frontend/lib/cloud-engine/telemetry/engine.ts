import { isCdnHit } from "../dashboard/cdn-cache";
import type { RunProgress } from "../dashboard/engine";
import {
  fetchManifest,
  type FetchJson,
  type ManifestFile,
} from "../dashboard/manifest";
import { readColumns, readParquetRows as readRows } from "../dashboard/parquet";
import {
  assembleProjectedParquet,
  fetchProjectedRanges,
  planParquetProjection,
  readParquetFooter,
} from "../dashboard/parquet-projection";
import { fnv1a32 } from "../parquet-key";
import {
  OUTBOX_KINDS,
  type LiveStatus,
  type LogRow,
  type LogsRange,
  type LogsResult,
  type LogsStats,
  type OpsKind,
} from "./kinds";
import {
  OpsRangeUnsupportedError,
  clearOpsDecodedCache,
  deleteDecodedParquet,
  loadDecodedParquet,
  loadIngestFileFold,
} from "./decoded-cache";
import {
  INGEST_LIFECYCLE_COLUMNS,
  createIngestLifecycleFold,
  finalizeIngestLifecycle,
  foldIngestColumns,
  foldIngestRows,
  mergeIngestLifecycleFold,
  type IngestLifecycleFold,
  type IngestLifecycleResult,
} from "./ingest-lifecycle";

export { OpsRangeUnsupportedError, clearOpsDecodedCache };

export { fnv1a32 };
export type {
  IngestLifecycleResult,
  LogRow,
  LogsRange,
  LogsResult,
  LogsStats,
  OpsKind,
};
export type LogResult = LogsResult;

/**
 * Uncached ops Parquet files larger than this may use HTTP Range projection when `columns`
 * is set. Smaller files are fetched whole in a single request so they populate the persistent
 * Browser Cache API instead of paying 2–3 HTTP 206 round-trips on every load.
 */
export const OPS_PROJECTED_MIN_BYTES = 512 * 1024;
/** Initial tail probe size for large ops Parquet footers. */
export const OPS_FOOTER_PROBE_BYTES = 64 * 1024;
/** Maximum fraction of a file's bytes a projected Range read may transfer before falling back to whole-file GET. */
export const OPS_PROJECTED_MAX_RATIO = 0.5;
/** Maximum extra data Range requests allowed per projected ops Parquet file. */
export const OPS_PROJECTED_MAX_RANGES = 4;

export type OpsSource = {
  /** Optional cache partition key enabling in-memory reuse of decoded immutable ops Parquet files. */
  cacheScope?: string;
  fetchJson: FetchJson;
  fetchFile: (
    key: string,
    signal?: AbortSignal,
  ) => Promise<{ bytes: ArrayBuffer; cached: boolean; cdn?: string | null }>;
  fetchRange?: (
    file: ManifestFile,
    start: number,
    end: number,
    signal?: AbortSignal,
  ) => Promise<{ bytes: ArrayBuffer; cached: boolean; cdn?: string | null }>;
  readIngestFold?: (
    key: string,
    signal?: AbortSignal,
  ) => Promise<IngestLifecycleFold | null>;
  writeIngestFold?: (
    key: string,
    fold: IngestLifecycleFold,
    signal?: AbortSignal,
  ) => Promise<void>;
  cachedKeys: () => Promise<string[]>;
  evict: (key: string) => Promise<void>;
};

export function normalizeOpsColumns(
  columns: readonly string[] | undefined,
  dedupe = true,
): string[] | null {
  if (!columns || columns.length === 0) return null;
  const set = new Set<string>(columns);
  set.add("timestamp");
  if (dedupe) set.add("event_id");
  return [...set].sort();
}

function isAbortOrAuthError(err: unknown): boolean {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  if (err instanceof Error) {
    if (err.name === "AbortError") return true;
    if (/\((?:401|403)\)$/.test(err.message)) return true;
  }
  return false;
}

export type ReadLogsOptions = {
  kind: OpsKind;
  from: string;
  to: string;
  signal?: AbortSignal;
  onProgress?: (progress: RunProgress) => void;
  /**
   * For `storage_inventory`, read newest files first and stop once the newest complete
   * snapshot (`snap-<epochSec>-...`) and any overlapping files are loaded.
   */
  latestCompleteSnapshotOnly?: boolean;
  /** Optional column subset to decode (and Range-project on large uncached files). */
  columns?: readonly string[];
  /**
   * Whether large uncached Parquet files (> `OPS_PROJECTED_MIN_BYTES`) should attempt
   * HTTP Range projection when `columns` is set (default `true`). Callers requesting
   * wide column subsets that exceed `OPS_PROJECTED_MAX_RATIO` can set `false` to avoid
   * a wasted 64 KiB footer probe round-trip before falling back to `fetchFile`.
   */
  rangeProject?: boolean;
  /** Deduplicate rows by `event_id` (default `true`). */
  dedupe?: boolean;
  /** Sort rows by `timestamp` descending, then `event_id` (default `true`). */
  sort?: boolean;
};

const SNAPSHOT_ID_TS_RE = /^snap-(\d+)(?:-|$)/;
const SNAPSHOT_CLOCK_SKEW_MS = 5 * 60_000;

function snapshotRequestedAtMs(snapshotId: string): number | null {
  const m = SNAPSHOT_ID_TS_RE.exec(snapshotId);
  if (!m) return null;
  const sec = Number(m[1]);
  return Number.isFinite(sec) && sec > 0 ? sec * 1000 : null;
}

function latestCompleteSnapshotCutoff(
  rows: readonly Record<string, unknown>[],
  current: { terminalTs: number; cutoffMs: number } | null = null,
): { terminalTs: number; cutoffMs: number } | null {
  let best = current;
  for (const row of rows) {
    if (row.complete !== true) continue;
    const snapshotId =
      typeof row.snapshot_id === "string" ? row.snapshot_id.trim() : "";
    if (!snapshotId) continue;
    const requestedAtMs = snapshotRequestedAtMs(snapshotId);
    if (requestedAtMs === null) continue;
    const terminalTs = Number(row.timestamp);
    if (!Number.isFinite(terminalTs)) continue;
    if (!best || terminalTs > best.terminalTs) {
      best = {
        terminalTs,
        cutoffMs: Math.min(requestedAtMs, terminalTs) - SNAPSHOT_CLOCK_SKEW_MS,
      };
    }
  }
  return best;
}

function manifestFileEndMs(
  file: ManifestFile,
  parsed: ParsedKey | null,
): number {
  const endMs = Date.parse(file.end);
  if (Number.isFinite(endMs)) return endMs;
  if (parsed) return parsed.end * 60_000 + 60_000;
  return Number.POSITIVE_INFINITY;
}

type LivePage = {
  rows: Record<string, unknown>[];
  next_cursor: string | null;
  truncated?: boolean;
  status?: LiveStatus;
};

const LIVE_LIMIT = 2000;
const LIVE_SOURCE_CURSOR = "source-v1";
const MAX_LIVE_RESTARTS = 2;
const MAX_LIVE_PAGES = 50;
export const OPS_FETCH_CONCURRENCY = 32;
const CONCURRENCY = OPS_FETCH_CONCURRENCY;
const MINUTE_MS = 60_000;

const KEY_PATTERN =
  /^(.*\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/)(?:hour=(\d{2})\/)?(?:window=(\d+)-(\d+)-[^/]*-n(\d+)|compact=(\d+)-(\d+)-c(\d+)-n(\d+)(?:-([^/]*))?)\.parquet$/;

type ParsedKey = {
  dir: string;
  spanFrom: number;
  spanTo: number;
  kind: "window" | "compact";
  start: number;
  end: number;
  sources: number;
  rows: number;
  /** null for windows and legacy untagged compacts. */
  tags?: Set<number> | null;
  malformed?: boolean;
  fileName: string;
};

function parseTags(text: string, sources: number): Set<number> | null {
  if (text.length !== sources * 8 || !/^[0-9a-fA-F]*$/.test(text)) return null;
  const tags = new Set<number>();
  for (let i = 0; i < text.length; i += 8) {
    tags.add(parseInt(text.slice(i, i + 8), 16));
  }
  return tags;
}

function parseKey(key: string): ParsedKey | null {
  const m = KEY_PATTERN.exec(key);
  if (!m) return null;
  const day = Date.UTC(Number(m[2]), Number(m[3]) - 1, Number(m[4]));
  const hour = m[5] === undefined ? null : Number(m[5]);
  const spanFrom = hour === null ? day : day + hour * 3_600_000;
  const spanTo = spanFrom + (hour === null ? 86_400_000 : 3_600_000);
  const isWindow = m[6] !== undefined;
  const sources = isWindow ? 1 : Number(m[11]);
  const tagText = isWindow ? undefined : m[13];
  const tags =
    isWindow || tagText === undefined ? null : parseTags(tagText, sources);
  return {
    dir: m[1],
    fileName: key.slice(key.lastIndexOf("/") + 1),
    tags,
    malformed: tagText !== undefined && tags === null,
    spanFrom,
    spanTo,
    kind: isWindow ? "window" : "compact",
    start: Number(isWindow ? m[6] : m[9]),
    end: Number(isWindow ? m[7] : m[10]),
    sources,
    rows: Number(isWindow ? m[8] : m[12]),
  };
}

async function readLive(
  source: OpsSource,
  kind: OpsKind,
  fromMs: number,
  signal?: AbortSignal,
): Promise<{
  rows: Record<string, unknown>[];
  status: LiveStatus | null;
  truncated: boolean;
}> {
  let rows: Record<string, unknown>[] = [];
  let status: LiveStatus | null = null;
  let truncated = false;
  const sourceWalk = (OUTBOX_KINDS as readonly OpsKind[]).includes(kind);
  let cursor: string | null = sourceWalk ? LIVE_SOURCE_CURSOR : null;
  let seen = new Set<string>();
  let restarts = 0;
  for (let page = 0; page < MAX_LIVE_PAGES; page++) {
    const params = new URLSearchParams({
      since: String(fromMs),
      limit: String(LIVE_LIMIT),
    });
    if (cursor) params.set("cursor", cursor);
    let body: LivePage;
    try {
      body = (await source.fetchJson(
        `/api/logs/${kind}/live?${params}`,
        signal,
      )) as LivePage;
    } catch (error) {
      // 409: an outbox record this cursor was mid-way through was rewritten.
      // The sources are small; walk them again from the start.
      if (
        sourceWalk &&
        restarts < MAX_LIVE_RESTARTS &&
        error instanceof Error &&
        /\(409\)$/.test(error.message)
      ) {
        restarts += 1;
        rows = [];
        seen = new Set();
        cursor = LIVE_SOURCE_CURSOR;
        continue;
      }
      throw error;
    }
    rows.push(...body.rows);
    status ??= body.status ?? null;
    if (body.truncated) truncated = true;
    cursor = body.next_cursor;
    if (!cursor) return { rows, status, truncated };
    if (seen.has(cursor)) throw new Error("Live log cursor repeated");
    seen.add(cursor);
  }
  throw new Error("Live log read exceeded the page limit");
}

async function mapLimit<T>(
  items: T[],
  fn: (item: T) => Promise<void>,
  signal?: AbortSignal,
): Promise<void> {
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      signal?.throwIfAborted();
      await fn(items[next++]);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, items.length) }, worker),
  );
}

export async function readLogs(
  source: OpsSource,
  options: ReadLogsOptions,
): Promise<LogsResult>;
export async function readLogs(
  source: OpsSource,
  kind: OpsKind,
  range: LogsRange,
  options?: {
    signal?: AbortSignal;
    latestCompleteSnapshotOnly?: boolean;
    columns?: readonly string[];
    rangeProject?: boolean;
    dedupe?: boolean;
    sort?: boolean;
  },
): Promise<LogsResult>;
export async function readLogs(
  source: OpsSource,
  kindOrOptions: OpsKind | ReadLogsOptions,
  rangeArg?: LogsRange,
  optionsArg: {
    signal?: AbortSignal;
    latestCompleteSnapshotOnly?: boolean;
    columns?: readonly string[];
    rangeProject?: boolean;
    dedupe?: boolean;
    sort?: boolean;
  } = {},
): Promise<LogsResult> {
  const kind: OpsKind =
    typeof kindOrOptions === "string" ? kindOrOptions : kindOrOptions.kind;
  const range: LogsRange =
    typeof kindOrOptions === "string"
      ? rangeArg!
      : { from: kindOrOptions.from, to: kindOrOptions.to };
  const signal =
    typeof kindOrOptions === "string" ? optionsArg.signal : kindOrOptions.signal;
  const onProgress = typeof kindOrOptions === "string" ? undefined : kindOrOptions.onProgress;
  const latestCompleteSnapshotOnly =
    typeof kindOrOptions === "string"
      ? optionsArg.latestCompleteSnapshotOnly === true
      : kindOrOptions.latestCompleteSnapshotOnly === true;
  const rangeProject =
    typeof kindOrOptions === "string"
      ? optionsArg.rangeProject !== false
      : kindOrOptions.rangeProject !== false;
  const dedupe =
    typeof kindOrOptions === "string"
      ? optionsArg.dedupe !== false
      : kindOrOptions.dedupe !== false;
  const sort =
    typeof kindOrOptions === "string"
      ? optionsArg.sort !== false
      : kindOrOptions.sort !== false;
  const rawColumns =
    typeof kindOrOptions === "string" ? optionsArg.columns : kindOrOptions.columns;
  const requestedColumns = normalizeOpsColumns(rawColumns, dedupe);
  const columnsKey = requestedColumns ? requestedColumns.join(",") : "*";
  const progress: RunProgress = {
    phase: "cache", cached: 0, toFetch: 0, toDecode: 0, done: 0,
    downloadedBytes: 0, cachedBytes: 0,
  };
  const fromMs = Date.parse(range.from);
  const toMs = Date.parse(range.to);
  const stats: LogsStats = {
    listed: 0,
    read: 0,
    cached: 0,
    cdnHits: 0,
    skippedCovered: 0,
    ignoredCompact: 0,
    evicted: 0,
    liveRows: 0,
  };
  const [live, manifest, cachedKeyList] = await Promise.all([
    readLive(source, kind, fromMs, signal),
    fetchManifest(
      source.fetchJson,
      range.from,
      range.to,
      signal,
      `ops:${kind}`,
    ),
    source.cachedKeys(),
  ]);
  if (!manifest.complete) {
    throw new Error(
      "The file manifest for this range is incomplete; choose a shorter range.",
    );
  }
  stats.listed = manifest.files.length;

  const listed = new Set(manifest.files.map((file) => file.key));
  const cachedKeys = new Set(cachedKeyList);
  const evictions = [...cachedKeys].filter((key) => {
    if (listed.has(key) || !key.startsWith(`processed/ops/${kind}/`))
      return false;
    const parsed = parseKey(key);
    return !!parsed && parsed.spanFrom < toMs && parsed.spanTo > fromMs;
  });
  await Promise.all(
    evictions.map(async (key) => {
      if (source.cacheScope) {
        deleteDecodedParquet(source.cacheScope, key);
      }
      await source.evict(key);
    }),
  );
  stats.evicted = evictions.length;

  const fileChunks: Record<string, unknown>[][] = [];
  const fetchAndDecode = async (
    file: ManifestFile,
    loadSignal?: AbortSignal,
  ): Promise<Record<string, unknown>[]> => {
    if (
      rangeProject &&
      requestedColumns !== null &&
      source.fetchRange &&
      file.size > OPS_PROJECTED_MIN_BYTES &&
      !cachedKeys.has(file.key)
    ) {
      try {
        let anyNetwork = false;
        let anyCdnHit = false;
        let downloaded = 0;
        let cachedBytes = 0;
        const fetchRangeBytes = async (start: number, end: number) => {
          const part = await source.fetchRange!(file, start, end, loadSignal);
          if (part.cached) {
            cachedBytes += part.bytes.byteLength;
          } else {
            anyNetwork = true;
            downloaded += part.bytes.byteLength;
          }
          if (isCdnHit(part.cdn)) anyCdnHit = true;
          return part.bytes;
        };
        const footer = await readParquetFooter(
          file.size,
          fetchRangeBytes,
          OPS_FOOTER_PROBE_BYTES,
        );
        const projection = planParquetProjection(footer, new Set(requestedColumns));
        const tailStart = footer.tail?.start ?? file.size;
        const extraRanges = projection.ranges.filter((r) => r.start < tailStart);
        const extraBytes = extraRanges.reduce(
          (sum, r) => sum + (Math.min(r.end, tailStart) - r.start),
          0,
        );
        const tailBytes = footer.tail ? footer.tail.bytes.byteLength : 0;
        if (projection.chunks.length === 0) {
          const rows = Array.from(
            { length: Number(footer.metadata.num_rows) },
            () => ({}),
          );
          if (!anyNetwork) stats.cached++;
          if (anyCdnHit) stats.cdnHits++;
          stats.read++;
          if (!anyNetwork) progress.cached++;
          progress.cachedBytes += cachedBytes;
          progress.downloadedBytes += downloaded;
          progress.done++;
          onProgress?.({ ...progress });
          return rows;
        }
        if (
          extraBytes + tailBytes < file.size * OPS_PROJECTED_MAX_RATIO &&
          extraRanges.length <= OPS_PROJECTED_MAX_RANGES
        ) {
          const rangeBytes = await fetchProjectedRanges(projection, fetchRangeBytes);
          const projected = assembleProjectedParquet(projection, rangeBytes);
          const rows = await readRows(projected, requestedColumns);
          if (!anyNetwork) stats.cached++;
          if (anyCdnHit) stats.cdnHits++;
          stats.read++;
          if (!anyNetwork) progress.cached++;
          progress.cachedBytes += cachedBytes;
          progress.downloadedBytes += downloaded;
          progress.done++;
          onProgress?.({ ...progress });
          return rows;
        }
      } catch (err) {
        if (loadSignal?.aborted || isAbortOrAuthError(err)) throw err;
      }
    }
    const { bytes, cached, cdn } = await source.fetchFile(file.key, loadSignal);
    if (cached) stats.cached++;
    if (isCdnHit(cdn)) stats.cdnHits++;
    stats.read++;
    if (cached) {
      progress.cached++;
      progress.cachedBytes += bytes.byteLength;
    } else {
      progress.downloadedBytes += bytes.byteLength;
    }
    const rows = await readRows(bytes, requestedColumns ?? undefined);
    progress.done++;
    onProgress?.({ ...progress });
    return rows;
  };
  const load = async (file: ManifestFile) => {
    if (!source.cacheScope) {
      return fetchAndDecode(file, signal);
    }
    const { rows, coalesced } = await loadDecodedParquet(
      source.cacheScope,
      file.key,
      columnsKey,
      (sharedSignal) => fetchAndDecode(file, sharedSignal),
      signal,
    );
    if (coalesced) {
      stats.cached++;
      stats.read++;
      progress.cached++;
      progress.done++;
      onProgress?.({ ...progress });
    }
    return rows;
  };

  const windows = new Map<
    string,
    { file: ManifestFile; parsed: ParsedKey }[]
  >();
  const compacts: { file: ManifestFile; parsed: ParsedKey }[] = [];
  const plain: ManifestFile[] = [];
  for (const file of manifest.files) {
    const parsed = parseKey(file.key);
    if (!parsed) plain.push(file);
    else if (parsed.kind === "compact") compacts.push({ file, parsed });
    else {
      const list = windows.get(parsed.dir) ?? [];
      list.push({ file, parsed });
      windows.set(parsed.dir, list);
    }
  }

  const covered = new Set<string>();
  const coveredBy = (parsed: ParsedKey) =>
    (windows.get(parsed.dir) ?? []).filter(({ parsed: w }) =>
      parsed.tags
        ? parsed.tags.has(fnv1a32(w.fileName))
        : w.start >= parsed.start && w.start <= parsed.end,
    );
  const validCompacts = compacts.filter(({ parsed }) => {
    if (parsed.malformed) return false;
    if (parsed.tags) return true;
    const inRange = coveredBy(parsed);
    return inRange.length === parsed.sources || inRange.length === 0;
  });
  stats.ignoredCompact += compacts.length - validCompacts.length;
  const queueProgress = (files: ManifestFile[]) => {
    for (const file of files) {
      if (cachedKeys.has(file.key)) progress.toDecode++;
      else progress.toFetch++;
    }
    progress.phase = "files";
    onProgress?.({ ...progress });
  };
  if (kind === "storage_inventory" && latestCompleteSnapshotOnly) {
    type Candidate =
      | { kind: "compact"; file: ManifestFile; parsed: ParsedKey; endMs: number }
      | { kind: "window"; file: ManifestFile; parsed: ParsedKey; endMs: number }
      | { kind: "plain"; file: ManifestFile; parsed: null; endMs: number };

    const candidates: Candidate[] = [
      ...validCompacts.map(({ file, parsed }) => ({
        kind: "compact" as const,
        file,
        parsed,
        endMs: manifestFileEndMs(file, parsed),
      })),
      ...[...windows.values()].flat().map(({ file, parsed }) => ({
        kind: "window" as const,
        file,
        parsed,
        endMs: manifestFileEndMs(file, parsed),
      })),
      ...plain.map((file) => ({
        kind: "plain" as const,
        file,
        parsed: null,
        endMs: manifestFileEndMs(file, null),
      })),
    ].sort((a, b) => {
      if (b.endMs !== a.endMs) return b.endMs - a.endMs;
      if (a.kind === b.kind) return b.file.key.localeCompare(a.file.key);
      return a.kind === "compact" ? -1 : b.kind === "compact" ? 1 : 0;
    });

    let bestCutoff = latestCompleteSnapshotCutoff(live.rows);
    let cursor = 0;
    let waveSize = 1;

    while (cursor < candidates.length) {
      const wave: Candidate[] = [];
      const limit = bestCutoff ? Number.POSITIVE_INFINITY : waveSize;
      while (cursor < candidates.length && wave.length < limit) {
        const next = candidates[cursor++];
        if (next.kind === "window" && covered.has(next.file.key)) {
          stats.skippedCovered++;
          continue;
        }
        if (bestCutoff && next.endMs < bestCutoff.cutoffMs) {
          break;
        }
        wave.push(next);
      }
      if (wave.length === 0) break;
      queueProgress(wave.map((item) => item.file));
      await mapLimit(
        wave,
        async (item) => {
          const rows = await load(item.file);
          if (item.kind === "compact") {
            if (rows.length !== item.parsed.rows) {
              stats.ignoredCompact++;
              return;
            }
            for (const { file: w } of coveredBy(item.parsed)) covered.add(w.key);
          }
          fileChunks.push(rows);
          bestCutoff = latestCompleteSnapshotCutoff(rows, bestCutoff);
        },
        signal,
      );
      waveSize = 4;
    }
  } else {
    const potentiallyCovered = new Set<string>();
    for (const { parsed } of validCompacts) {
      for (const { file: w } of coveredBy(parsed)) potentiallyCovered.add(w.key);
    }
    const rest = [...windows.values()].flat().map(({ file }) => file);
    const uncoveredWindows = rest.filter((file) => !potentiallyCovered.has(file.key));
    const coveredWindows = rest.filter((file) => potentiallyCovered.has(file.key));

    type FirstWaveItem =
      | { kind: "compact"; file: ManifestFile; parsed: ParsedKey }
      | { kind: "file"; file: ManifestFile };
    const firstWave: FirstWaveItem[] = [
      ...validCompacts.map(({ file, parsed }) => ({
        kind: "compact" as const,
        file,
        parsed,
      })),
      ...plain.map((file) => ({ kind: "file" as const, file })),
      ...uncoveredWindows.map((file) => ({ kind: "file" as const, file })),
    ];

    queueProgress(firstWave.map((item) => item.file));
    await mapLimit(
      firstWave,
      async (item) => {
        const rows = await load(item.file);
        if (item.kind === "compact") {
          if (rows.length !== item.parsed.rows) {
            stats.ignoredCompact++;
            return;
          }
          for (const { file: w } of coveredBy(item.parsed)) covered.add(w.key);
        }
        fileChunks.push(rows);
      },
      signal,
    );

    const fallbackWindows = coveredWindows.filter((file) => !covered.has(file.key));
    stats.skippedCovered = coveredWindows.length - fallbackWindows.length;
    if (fallbackWindows.length > 0) {
      queueProgress(fallbackWindows);
      await mapLimit(
        fallbackWindows,
        async (file) => {
          fileChunks.push(await load(file));
        },
        signal,
      );
    }
  }

  progress.phase = "aggregate";
  onProgress?.({ ...progress });
  stats.liveRows = live.rows.length;
  let rows: LogRow[];
  if (dedupe) {
    const byId = new Map<string, LogRow>();
    for (const raw of live.rows) {
      const timestamp = Number(raw.timestamp);
      const eventId = String(raw.event_id ?? "");
      if (!Number.isFinite(timestamp) || timestamp < fromMs || timestamp >= toMs)
        continue;
      byId.set(eventId, { ...raw, timestamp, event_id: eventId });
    }
    for (const chunk of fileChunks) {
      for (const raw of chunk) {
        const timestamp = Number(raw.timestamp);
        const eventId = String(raw.event_id ?? "");
        if (!Number.isFinite(timestamp) || timestamp < fromMs || timestamp >= toMs)
          continue;
        byId.set(eventId, { ...raw, timestamp, event_id: eventId });
      }
    }
    rows = [...byId.values()];
  } else {
    rows = [];
    for (const raw of live.rows) {
      const timestamp = Number(raw.timestamp);
      if (!Number.isFinite(timestamp) || timestamp < fromMs || timestamp >= toMs)
        continue;
      rows.push(
        raw.timestamp === timestamp
          ? (raw as LogRow)
          : ({ ...raw, timestamp } as LogRow),
      );
    }
    for (const chunk of fileChunks) {
      for (const raw of chunk) {
        const timestamp = Number(raw.timestamp);
        if (!Number.isFinite(timestamp) || timestamp < fromMs || timestamp >= toMs)
          continue;
        rows.push(
          raw.timestamp === timestamp
            ? (raw as LogRow)
            : ({ ...raw, timestamp } as LogRow),
        );
      }
    }
  }
  if (sort) {
    rows.sort(
      (a, b) =>
        b.timestamp - a.timestamp ||
        String(b.event_id ?? "").localeCompare(String(a.event_id ?? "")),
    );
  }
  const liveTimes = live.rows
    .map((row) => Number(row.timestamp))
    .filter(Number.isFinite);
  return {
    kind,
    from: range.from,
    to: range.to,
    rows,
    oldestLiveMs: liveTimes.length
      ? Math.floor(Math.min(...liveTimes) / MINUTE_MS) * MINUTE_MS
      : null,
    liveTruncated: live.truncated,
    status: live.status,
    stats,
  };
}

export interface ReadIngestLifecycleOptions {
  from: string;
  to: string;
  minimumBilledDays?: number;
  signal?: AbortSignal;
  onProgress?: (p: RunProgress) => void;
}

export async function readIngestLifecycle(
  source: OpsSource,
  options: ReadIngestLifecycleOptions,
): Promise<IngestLifecycleResult> {
  const { from, to, minimumBilledDays, signal, onProgress } = options;
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);
  const columns = [...INGEST_LIFECYCLE_COLUMNS];
  const progress: RunProgress = {
    phase: "cache",
    cached: 0,
    toFetch: 0,
    toDecode: 0,
    done: 0,
    downloadedBytes: 0,
    cachedBytes: 0,
  };
  const stats: LogsStats = {
    listed: 0,
    read: 0,
    cached: 0,
    cdnHits: 0,
    skippedCovered: 0,
    ignoredCompact: 0,
    evicted: 0,
    liveRows: 0,
  };

  const [live, manifest, cachedKeyList] = await Promise.all([
    readLive(source, "ingest_files", fromMs, signal),
    fetchManifest(source.fetchJson, from, to, signal, "ops:ingest_files"),
    source.cachedKeys(),
  ]);
  if (!manifest.complete) {
    throw new Error(
      "The file manifest for this range is incomplete; choose a shorter range.",
    );
  }
  stats.listed = manifest.files.length;

  const listed = new Set(manifest.files.map((file) => file.key));
  const cachedKeys = new Set(cachedKeyList);
  const evictions = [...cachedKeys].filter((key) => {
    if (listed.has(key) || !key.startsWith("processed/ops/ingest_files/")) {
      return false;
    }
    const parsed = parseKey(key);
    return !!parsed && parsed.spanFrom < toMs && parsed.spanTo > fromMs;
  });
  await Promise.all(
    evictions.map(async (key) => {
      if (source.cacheScope) {
        deleteDecodedParquet(source.cacheScope, key);
      }
      await source.evict(key);
    }),
  );
  stats.evicted = evictions.length;

  // INGEST_LIFECYCLE_COLUMNS includes raw_key and output_keys_json, which dominate
  // ingest_files Parquet files (>50% of bytes), so always fetch whole files in one
  // request to avoid wasted 64 KiB Range footer probes and populate Browser Cache API.
  const fetchAndFold = async (
    file: ManifestFile,
    loadSignal: AbortSignal | undefined,
    filterFrom?: number,
    filterTo?: number,
  ): Promise<IngestLifecycleFold> => {
    const { bytes, cached, cdn } = await source.fetchFile(file.key, loadSignal);
    if (cached) stats.cached++;
    if (isCdnHit(cdn)) stats.cdnHits++;
    stats.read++;
    if (cached) {
      progress.cached++;
      progress.cachedBytes += bytes.byteLength;
    } else {
      progress.downloadedBytes += bytes.byteLength;
    }
    const decoded = await readColumns(bytes, columns);
    const fold = createIngestLifecycleFold();
    foldIngestColumns(fold, decoded, filterFrom, filterTo);
    progress.done++;
    onProgress?.({ ...progress });
    return fold;
  };

  const loadFold = async (
    file: ManifestFile,
    parsed: ParsedKey | null,
  ): Promise<IngestLifecycleFold> => {
    const interior =
      parsed !== null && parsed.spanFrom >= fromMs && parsed.spanTo <= toMs;
    if (source.cacheScope && interior) {
      const { fold, coalesced } = await loadIngestFileFold(
        source.cacheScope,
        file.key,
        async (sharedSignal) => {
          if (source.readIngestFold) {
            const persisted = await source
              .readIngestFold(file.key, sharedSignal)
              .catch(() => null);
            sharedSignal.throwIfAborted();
            if (persisted) {
              stats.cached++;
              stats.read++;
              progress.cached++;
              progress.done++;
              onProgress?.({ ...progress });
              return persisted;
            }
          }
          const computed = await fetchAndFold(file, sharedSignal);
          if (source.writeIngestFold && !sharedSignal.aborted) {
            void source
              .writeIngestFold(file.key, computed, sharedSignal)
              .catch(() => {});
          }
          return computed;
        },
        signal,
      );
      if (coalesced) {
        stats.cached++;
        stats.read++;
        progress.cached++;
        progress.done++;
        onProgress?.({ ...progress });
      }
      if (fold.rowCount === 0 || (fold.minTs >= fromMs && fold.maxTs < toMs)) {
        return fold;
      }
      return fetchAndFold(file, signal, fromMs, toMs);
    }
    return fetchAndFold(file, signal, fromMs, toMs);
  };

  const windows = new Map<
    string,
    { file: ManifestFile; parsed: ParsedKey }[]
  >();
  const compacts: { file: ManifestFile; parsed: ParsedKey }[] = [];
  const plain: ManifestFile[] = [];
  for (const file of manifest.files) {
    const parsed = parseKey(file.key);
    if (!parsed) plain.push(file);
    else if (parsed.kind === "compact") compacts.push({ file, parsed });
    else {
      const list = windows.get(parsed.dir) ?? [];
      list.push({ file, parsed });
      windows.set(parsed.dir, list);
    }
  }

  const covered = new Set<string>();
  const coveredBy = (parsed: ParsedKey) =>
    (windows.get(parsed.dir) ?? []).filter(({ parsed: w }) =>
      parsed.tags
        ? parsed.tags.has(fnv1a32(w.fileName))
        : w.start >= parsed.start && w.start <= parsed.end,
    );
  const validCompacts = compacts.filter(({ parsed }) => {
    if (parsed.malformed) return false;
    if (parsed.tags) return true;
    const inRange = coveredBy(parsed);
    return inRange.length === parsed.sources || inRange.length === 0;
  });
  stats.ignoredCompact += compacts.length - validCompacts.length;

  const queueProgress = (queued: ManifestFile[]) => {
    for (const file of queued) {
      if (cachedKeys.has(file.key)) progress.toDecode++;
      else progress.toFetch++;
    }
    progress.phase = "files";
    onProgress?.({ ...progress });
  };

  const potentiallyCovered = new Set<string>();
  for (const { parsed } of validCompacts) {
    for (const { file: w } of coveredBy(parsed)) potentiallyCovered.add(w.key);
  }
  const rest = [...windows.values()].flat();
  const uncoveredWindows = rest.filter(({ file }) => !potentiallyCovered.has(file.key));
  const coveredWindows = rest.filter(({ file }) => potentiallyCovered.has(file.key));

  type FirstWaveItem =
    | { kind: "compact"; file: ManifestFile; parsed: ParsedKey }
    | { kind: "file"; file: ManifestFile; parsed: ParsedKey | null };
  const firstWave: FirstWaveItem[] = [
    ...validCompacts.map(({ file, parsed }) => ({
      kind: "compact" as const,
      file,
      parsed,
    })),
    ...plain.map((file) => ({ kind: "file" as const, file, parsed: null })),
    ...uncoveredWindows.map(({ file, parsed }) => ({
      kind: "file" as const,
      file,
      parsed,
    })),
  ];

  const totalFold = createIngestLifecycleFold();
  queueProgress(firstWave.map((item) => item.file));
  await mapLimit(
    firstWave,
    async (item) => {
      const fold = await loadFold(item.file, item.parsed);
      if (item.kind === "compact") {
        if (fold.rowCount !== item.parsed.rows) {
          stats.ignoredCompact++;
          return;
        }
        for (const { file: w } of coveredBy(item.parsed)) covered.add(w.key);
      }
      mergeIngestLifecycleFold(totalFold, fold);
    },
    signal,
  );

  const fallbackWindows = coveredWindows.filter(({ file }) => !covered.has(file.key));
  stats.skippedCovered = coveredWindows.length - fallbackWindows.length;
  if (fallbackWindows.length > 0) {
    queueProgress(fallbackWindows.map(({ file }) => file));
    await mapLimit(
      fallbackWindows,
      async ({ file, parsed }) => {
        mergeIngestLifecycleFold(totalFold, await loadFold(file, parsed));
      },
      signal,
    );
  }

  progress.phase = "aggregate";
  onProgress?.({ ...progress });
  stats.liveRows = live.rows.length;
  foldIngestRows(totalFold, live.rows, fromMs, toMs);

  return finalizeIngestLifecycle(totalFold, {
    minimumBilledDays,
    liveTruncated: live.truncated,
    stats,
  });
}


export interface IngestEventRow {
  event_id: string;
  timestamp: number;
  key: string;
  outcome: "converted" | "skipped" | "failed" | "quarantined" | string;
  rows_written: number;
  bytes_read: number;
  bytes_written: number;
  error?: string | null;
}

export interface ReadIngestEventsOptions {
  from: string;
  to: string;
  seenIds?: Set<string>;
  lastCursorMs?: number;
  signal?: AbortSignal;
}

export async function readIngestEvents(
  source: OpsSource,
  options: ReadIngestEventsOptions,
): Promise<{
  events: IngestEventRow[];
  nextCursorMs: number;
  seenIds: Set<string>;
}> {
  const seenIds = options.seenIds ?? new Set<string>();
  let nextCursorMs = options.lastCursorMs ?? 0;
  const result = await readLogs(
    source,
    "ingest_files",
    { from: options.from, to: options.to },
    { signal: options.signal },
  );
  const events: IngestEventRow[] = [];
  for (const row of result.rows) {
    const eventId = String(row.event_id ?? "");
    const ts = Number(row.timestamp ?? 0);
    if (!eventId || seenIds.has(eventId)) continue;
    seenIds.add(eventId);
    if (ts > nextCursorMs) nextCursorMs = ts;
    events.push({
      event_id: eventId,
      timestamp: ts,
      key: String(row.key ?? row.raw_key ?? ""),
      outcome: String(row.outcome ?? row.status ?? "converted"),
      rows_written: Number(row.rows_written ?? row.rows ?? 0),
      bytes_read: Number(row.bytes_read ?? 0),
      bytes_written: Number(row.bytes_written ?? 0),
      error: row.error != null ? String(row.error) : null,
    });
  }
  return { events, nextCursorMs, seenIds };
}

export function latestIngestRows(rows: readonly LogRow[]): IngestEventRow[] {
  const byKey = new Map<string, IngestEventRow>();
  for (const row of rows) {
    const key = String(row.key ?? row.raw_key ?? row.event_id ?? "");
    const ts = Number(row.timestamp ?? 0);
    const prev = byKey.get(key);
    if (!prev || ts >= prev.timestamp) {
      byKey.set(key, {
        event_id: String(row.event_id ?? ""),
        timestamp: ts,
        key,
        outcome: String(row.outcome ?? row.status ?? "converted"),
        rows_written: Number(row.rows_written ?? row.rows ?? 0),
        bytes_read: Number(row.bytes_read ?? 0),
        bytes_written: Number(row.bytes_written ?? 0),
        error: row.error != null ? String(row.error) : null,
      });
    }
  }
  return [...byKey.values()].sort((a, b) => b.timestamp - a.timestamp);
}

export function isActiveIngestFile(row: IngestEventRow): boolean {
  return row.outcome !== "deleted" && row.outcome !== "purged";
}
