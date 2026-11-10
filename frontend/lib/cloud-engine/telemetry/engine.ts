import { isCdnHit } from "../dashboard/cdn-cache";
import type { RunProgress } from "../dashboard/engine";
import {
  fetchManifest,
  type FetchJson,
  type ManifestFile,
} from "../dashboard/manifest";
import { readParquetRows as readRows } from "../dashboard/parquet";
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

export { fnv1a32 };
export type { LogRow, LogsRange, LogsResult, LogsStats, OpsKind };
export type LogResult = LogsResult;

export type OpsSource = {
  fetchJson: FetchJson;
  fetchFile: (
    key: string,
    signal?: AbortSignal,
  ) => Promise<{ bytes: ArrayBuffer; cached: boolean; cdn?: string | null }>;
  cachedKeys: () => Promise<string[]>;
  evict: (key: string) => Promise<void>;
};

export type ReadLogsOptions = {
  kind: OpsKind;
  from: string;
  to: string;
  signal?: AbortSignal;
  onProgress?: (progress: RunProgress) => void;
};

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
const CONCURRENCY = 16;
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
  options?: { signal?: AbortSignal },
): Promise<LogsResult>;
export async function readLogs(
  source: OpsSource,
  kindOrOptions: OpsKind | ReadLogsOptions,
  rangeArg?: LogsRange,
  optionsArg: { signal?: AbortSignal } = {},
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
  const [live, manifest] = await Promise.all([
    readLive(source, kind, fromMs, signal),
    fetchManifest(
      source.fetchJson,
      range.from,
      range.to,
      signal,
      `ops:${kind}`,
    ),
  ]);
  if (!manifest.complete) {
    throw new Error(
      "The file manifest for this range is incomplete; choose a shorter range.",
    );
  }
  stats.listed = manifest.files.length;

  const listed = new Set(manifest.files.map((file) => file.key));
  const cachedKeys = new Set(await source.cachedKeys());
  const evictions = [...cachedKeys].filter((key) => {
    if (listed.has(key) || !key.startsWith(`processed/ops/${kind}/`))
      return false;
    const parsed = parseKey(key);
    return !!parsed && parsed.spanFrom < toMs && parsed.spanTo > fromMs;
  });
  await Promise.all(evictions.map((key) => source.evict(key)));
  stats.evicted = evictions.length;

  const fileRows: Record<string, unknown>[] = [];
  const load = async (file: ManifestFile) => {
    const { bytes, cached, cdn } = await source.fetchFile(file.key, signal);
    if (cached) stats.cached++;
    if (isCdnHit(cdn)) stats.cdnHits++;
    stats.read++;
    if (cached) {
      progress.cached++;
      progress.cachedBytes += bytes.byteLength;
    } else {
      progress.downloadedBytes += bytes.byteLength;
    }
    const rows = await readRows(bytes);
    progress.done++;
    onProgress?.({ ...progress });
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
  queueProgress(validCompacts.map(({ file }) => file));
  await mapLimit(
    validCompacts,
    async ({ file, parsed }) => {
      const rows = await load(file);
      if (rows.length !== parsed.rows) {
        stats.ignoredCompact++;
        return;
      }
      fileRows.push(...rows);
      for (const { file: w } of coveredBy(parsed)) covered.add(w.key);
    },
    signal,
  );

  const rest = [...windows.values()].flat().map(({ file }) => file);
  const toRead = [...plain, ...rest.filter((file) => !covered.has(file.key))];
  stats.skippedCovered = rest.length - (toRead.length - plain.length);
  queueProgress(toRead);
  await mapLimit(
    toRead,
    async (file) => {
      fileRows.push(...(await load(file)));
    },
    signal,
  );

  progress.phase = "aggregate";
  onProgress?.({ ...progress });
  stats.liveRows = live.rows.length;
  const byId = new Map<string, LogRow>();
  for (const raw of [...live.rows, ...fileRows]) {
    const timestamp = Number(raw.timestamp);
    const eventId = String(raw.event_id ?? "");
    if (!Number.isFinite(timestamp) || timestamp < fromMs || timestamp >= toMs)
      continue;
    byId.set(eventId, { ...raw, timestamp, event_id: eventId });
  }
  const rows = [...byId.values()].sort(
    (a, b) => b.timestamp - a.timestamp || b.event_id.localeCompare(a.event_id),
  );
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

export const runOpsLogQuery = readLogs;

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
