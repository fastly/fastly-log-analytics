/**
 * Bounded reads for the Cloud `/api/sessions` list.
 *
 * Pass 1 sessionizes only the columns the list ranks and flags on, over the
 * newest files the requested page needs. Pass 2 reads the wide string columns
 * (`ua`, `url`) only for the page's own sessions, over the files their spans
 * overlap.
 */

import { duckSqlString } from "./dashboard/duckdb";
import { requireSafeIdent } from "./sql-validator";

export const SESSIONS_GAP_MS = 1_800_000;
export const SESSIONS_FIRST_SLICE_MS = 15 * 60_000;
/** Pass-2 loads per page: page spans closer than this share one load. */
const ENRICH_MERGE_GAP_MS = SESSIONS_FIRST_SLICE_MS;
const MAX_ENRICH_LOADS = 8;

/** Request columns only page rows need. */
export const SESSIONS_PAGE_ONLY_COLUMNS = ["ua", "url"] as const;
/** Request columns pass 2 reads, besides filtered fields. */
export const SESSIONS_ENRICH_COLUMNS = ["timestamp", "ts_ms", "ip", "ja4", "ua", "url"] as const;

/** A loaded slice of request files, released once its reads are done. */
export interface SessionsRead {
  runSql: (sql: string) => Promise<Record<string, unknown>[]>;
  release?: () => void;
  /** The request columns the loaded files hold. */
  presentColumns?: () => Promise<ReadonlySet<string>>;
}

/**
 * Loads the request files overlapping [fromMs, toMs) with these columns (all
 * of them when null). Rows outside that span may be present; every row inside
 * it is.
 */
export type SessionsRangeLoader = (fromMs: number, toMs: number, columns: readonly string[] | null) => Promise<SessionsRead>;

export function sessionsNewestFirst(body: { sort_by?: unknown; sort_dir?: unknown }): boolean {
  return body.sort_by === "session_start" && body.sort_dir === "desc";
}

/**
 * The earliest row a newest-first slice from `sliceFromMs` reads: a session
 * that starts at t follows a key's silence over [t − 30 min, t), so sessions
 * starting in [slice, end) need only rows from 30 minutes before the slice.
 */
export function sessionsSliceLoadFrom(sliceFromMs: number, startMs: number): number {
  return sliceFromMs > startMs ? Math.max(startMs, sliceFromMs - SESSIONS_GAP_MS) : startMs;
}

/** Where a list read's first load starts: the first newest slice, or the whole range. */
export function sessionsLoadFrom(body: { sort_by?: unknown; sort_dir?: unknown }, startMs: number, endMs: number): number {
  return sessionsNewestFirst(body) ? sessionsSliceLoadFrom(endMs - SESSIONS_FIRST_SLICE_MS, startMs) : startMs;
}

/**
 * Groups page sessions' [start, end] spans into at most MAX_ENRICH_LOADS
 * disjoint spans, each covering whole sessions, so pass 2 loads a few files.
 */
export function sessionsEnrichSpans(spans: readonly { startMs: number; endMs: number }[]): { startMs: number; endMs: number }[] {
  const merged: { startMs: number; endMs: number }[] = [];
  for (const span of [...spans].sort((a, b) => a.startMs - b.startMs)) {
    const last = merged.at(-1);
    if (last && span.startMs <= last.endMs + ENRICH_MERGE_GAP_MS) last.endMs = Math.max(last.endMs, span.endMs);
    else merged.push({ ...span });
  }
  while (merged.length > MAX_ENRICH_LOADS) {
    let closest = 0;
    for (let i = 1; i < merged.length - 1; i++) {
      if (merged[i + 1].startMs - merged[i].endMs < merged[closest + 1].startMs - merged[closest].endMs) closest = i;
    }
    merged[closest].endMs = Math.max(merged[closest].endMs, merged[closest + 1].endMs);
    merged.splice(closest + 1, 1);
  }
  return merged;
}

export interface SessionsPageKey {
  ip: string;
  ja4: string | null;
  startMs: number;
  endMs: number;
}

/**
 * Pass 2: per page session (`i` is its index in `keys`), the smallest UA and
 * the distinct URL count over its key's rows inside its own [start, end],
 * which are exactly its rows since one key's sessions never overlap.
 */
export function buildSessionsEnrichSql(options: {
  tableName?: string;
  keys: readonly SessionsPageKey[];
  hasJa4: boolean;
  msSql: string;
  filterSql: string;
  ua: boolean;
  urls: boolean;
}): string {
  const tableName = requireSafeIdent(options.tableName ?? "requests", "tableName");
  const text = (value: string | null) => value == null ? "CAST(NULL AS VARCHAR)" : `CAST(${duckSqlString(value)} AS VARCHAR)`;
  const values = options.keys.map((key, i) =>
    `(${i}, ${text(key.ip)}, ${text(key.ja4)}, ${Math.trunc(key.startMs)}::BIGINT, ${Math.trunc(key.endMs)}::BIGINT)`);
  const fromMs = Math.min(...options.keys.map((key) => key.startMs));
  const toMs = Math.max(...options.keys.map((key) => key.endMs));
  const stats = [
    options.ua ? `MIN(r."ua") AS ua` : null,
    options.urls ? `COUNT(DISTINCT r."url") AS unique_urls` : null,
  ].filter(Boolean);
  return `
    WITH page_keys AS (
      SELECT * FROM (VALUES ${values.join(", ")}) AS v(i, ip, ja4, s, e)
    ),
    page_rows AS (
      SELECT ${options.msSql} AS t, "ip", ${options.hasJa4 ? `"ja4"` : `NULL AS "ja4"`}${options.ua ? `, "ua"` : ""}${options.urls ? `, "url"` : ""}
      FROM ${tableName}
      WHERE "ip" IS NOT NULL AND "ip" != ''
        AND ${options.msSql} >= ${Math.trunc(fromMs)}
        AND ${options.msSql} <= ${Math.trunc(toMs)}
        AND (${options.filterSql})
    )
    SELECT p.i, ${stats.join(", ")}
    FROM page_keys p
    JOIN page_rows r ON r."ip" = p.ip AND r."ja4" IS NOT DISTINCT FROM p.ja4 AND r.t BETWEEN p.s AND p.e
    GROUP BY p.i
  `;
}
