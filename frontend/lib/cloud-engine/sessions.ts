/**
 * Cloud Engine handlers for Page 15 Sessions (`/sessions`, `/sessions/stream`)
 * and Session Scoring (`/admin/session-scoring`):
 * - `POST /api/sessions`
 * - `POST /api/sessions/detail`
 * - `GET|POST|PUT|PATCH|DELETE /api/services/{id}/scoring/*`
 */

import { escapeSqlLiteral, requireSafeIdent } from "./sql-validator";
import { redactQueryRowsForPii } from "./query";
import { compileFiltersToSql, type DuckDBColumnType } from "./dashboard/duckdb";
import {
  buildSessionsEnrichSql,
  SESSIONS_ENRICH_COLUMNS,
  SESSIONS_FIRST_SLICE_MS,
  SESSIONS_PAGE_ONLY_COLUMNS,
  sessionsEnrichSpans,
  sessionsNewestFirst,
  sessionsSliceLoadFrom,
  type SessionsPageKey,
  type SessionsRangeLoader,
  type SessionsRead,
} from "./sessions-window";
import type { Schema } from "@/types/api";

export const SESSIONS_INACTIVITY_GAP_SECONDS = 1800; // 30 * 60 seconds (INTERVAL 30 MINUTES)
export const SESSIONS_MAX_WINDOW_DAYS = 7;
export const SESSIONS_DEFAULT_MIN_REQS_FLAG = 1000;
export const SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG = 20.0;

export const SESSIONS_SORTABLE_COLUMNS = new Set([
  "session_start",
  "session_end",
  "req_count",
  "edge_count",
  "shield_count",
  "unique_urls",
  "median_rtt_ms",
  "total_bytes",
]);

export interface SessionsListRequest {
  service_id?: string;
  start_time: string;
  end_time: string;
  page?: number;
  limit?: number;
  sort_by?: string;
  sort_dir?: "asc" | "desc";
  flagged_only?: boolean;
  streaming_only?: boolean;
  min_reqs_flag?: number;
  min_4xx_pct_flag?: number;
  filters?: Record<string, { mode?: string; values?: string[] } | string[]>;
}

export interface SessionSummaryRow {
  ip: string;
  ja4?: string | null;
  session_start: string;
  session_end: string;
  req_count: number;
  streaming_reqs?: number;
  is_streaming: boolean;
  edge_sid?: string | null;
  edge_count?: number;
  shield_count?: number;
  asn?: number | null;
  country?: string | null;
  reqs_4xx: number;
  reqs_5xx: number;
  total_bytes?: number;
  median_rtt_ms?: number | null;
  ua?: string | null;
  unique_urls?: number;
  flagged: boolean;
  session_token: string;
}

export interface SessionsListResponse {
  sessions: SessionSummaryRow[];
  total: number;
  page: number;
  limit: number;
  has_rtt: boolean;
  has_ja4: boolean;
  has_edge: boolean;
  has_edge_sid: boolean;
  has_cmcd: boolean;
  min_reqs_flag: number;
  min_4xx_pct_flag: number;
  asn_names: Record<string, string>;
  section_timings: { name: string; duration_ms: number }[];
}

export type SessionDetailRequest = Schema<"SessionDetailRequest">;

export interface SessionDetailResponse {
  data: Record<string, unknown>[];
  columns: string[];
}

export interface CloudSessionsContext {
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>;
  serviceId?: string;
  tableName?: string;
  maskIps?: boolean;
  role?: "admin" | "analyst" | "analyst-no-pii";
  availableColumns?: ReadonlySet<string>;
  rollupAvailable?: boolean;
  runSql?: (sql: string) => Promise<Record<string, unknown>[]>;
  /** Where the rows `runSql` reads begin; older rows aren't loaded. */
  loadedFromMs?: number;
  /** Loads other spans or columns: the list then ranks without `ua`/`url` and reads them for the page only. */
  loadRange?: SessionsRangeLoader;
}

export interface CloudHttpResult<T = unknown> {
  status: number;
  body: T;
}

export interface SessionTokenPayload {
  ip: string;
  ja4: string | null;
  s: string;
  e: string;
  svc: string;
}

function toBase64Url(input: string): string {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromBase64Url(token: string): string {
  const padded = token.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return new TextDecoder().decode(bytes);
}

export function sealSessionToken(payload: SessionTokenPayload): string {
  return toBase64Url(JSON.stringify(payload));
}

export function openSessionToken(token: string): SessionTokenPayload | null {
  try {
    const parsed = JSON.parse(fromBase64Url(token)) as Partial<SessionTokenPayload>;
    if (
      typeof parsed?.ip !== "string" ||
      typeof parsed?.s !== "string" ||
      typeof parsed?.e !== "string"
    ) {
      return null;
    }
    return {
      ip: parsed.ip,
      ja4: typeof parsed.ja4 === "string" ? parsed.ja4 : null,
      s: parsed.s,
      e: parsed.e,
      svc: typeof parsed.svc === "string" ? parsed.svc : "",
    };
  } catch {
    return null;
  }
}

/** Every request column the /api/sessions list reads, over both passes. */
export const SESSIONS_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "ip", "ja4", "cmcd_sid", "edge_sid", "edge", "asn", "country", "status", "resp_bytes",
  "tcp_rtt", "ua", "url",
] as const;

/** The columns a projected /api/sessions load needs, or null when the filters can't name theirs. */
export function sessionsScanColumns(filters: unknown): string[] | null {
  if (filters == null || filters === "") return [...SESSIONS_SCAN_COLUMNS];
  if (typeof filters !== "object" || Array.isArray(filters)) return null;
  return [...new Set([...SESSIONS_SCAN_COLUMNS, ...Object.keys(filters)])];
}

/** The columns the ranking pass reads: `ua` only for the page, `url` too unless it ranks. */
export function sessionsListColumns(filters: unknown, sortBy?: unknown): string[] | null {
  return sessionsScanColumns(filters)?.filter((column) =>
    column !== "ua" && (column !== "url" || sortBy === "unique_urls")) ?? null;
}

function sessionsFilterSql(filters: SessionsListRequest["filters"], customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>) {
  return compileFiltersToSql(Object.entries(filters ?? {}).flatMap(([field, spec]) =>
    (Array.isArray(spec) ? spec : spec.values ?? []).map((value) => ({
      field, value, mode: !Array.isArray(spec) && spec.mode === "exclude" ? "exclude" as const : "include" as const,
    })),
  ), false, [], customColumnTypes);
}

const sessionsMsSql = (cols: ReadonlySet<string>) => cols.has("ts_ms") ? 'CAST("ts_ms" AS BIGINT)' : 'EPOCH_MS("timestamp")';

/**
 * Build the 30-minute inactivity-gap sessionization CTE pipeline (`INTERVAL 30 MINUTES` / 1800s):
 * `base` -> `clients` -> `minutes` -> `minute_sessions` -> `sessions_agg` -> `page` (-> `page_stats`).
 *
 * With `sliceFromMs`, only sessions that start at or after it are listed. Every such session is
 * complete: its rows lie in [sliceFromMs, end), and the 30 minutes before the slice are read for
 * the slice's clients only, so its start is known to follow a full inactivity gap.
 */
export function buildSessionsCteSql(options: {
  tableName?: string;
  startTime: string;
  endTime: string;
  availableColumns?: ReadonlySet<string>;
  minReqsFlag?: number;
  min4xxPctFlag?: number;
  flaggedOnly?: boolean;
  streamingOnly?: boolean;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  limit?: number;
  offset?: number;
  filters?: SessionsListRequest["filters"];
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>;
  sliceFromMs?: number;
}): string {
  const tableName = requireSafeIdent(options.tableName ?? "requests", "tableName");
  const cols =
    options.availableColumns ??
    new Set([
      "timestamp",
      "ip",
      "ja4",
      "status",
      "url",
      "ua",
      "country",
      "asn",
      "resp_bytes",
      "tcp_rtt",
      "edge",
      "edge_sid",
      "cmcd_sid",
    ]);

  const hasJa4 = cols.has("ja4");
  const ja4Select = hasJa4 ? `"ja4"` : `NULL AS "ja4"`;

  const cmcdExpr = cols.has("cmcd_sid")
    ? `COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != '') AS streaming_reqs`
    : `0 AS streaming_reqs`;
  const edgeSidExpr = cols.has("edge_sid")
    ? `MAX("edge_sid") AS edge_sid`
    : `NULL AS edge_sid`;
  const edgeExpr = cols.has("edge")
    ? `SUM(CASE WHEN "edge" = 1 OR "edge" = TRUE THEN 1 ELSE 0 END) AS edge_count, SUM(CASE WHEN "edge" = 0 OR "edge" = FALSE THEN 1 ELSE 0 END) AS shield_count`
    : `0 AS edge_count, 0 AS shield_count`;
  const asnExpr = cols.has("asn") ? `MIN("asn") AS asn` : `NULL AS asn`;
  const countryExpr = cols.has("country")
    ? `MIN("country") AS country`
    : `NULL AS country`;
  const statusExpr = cols.has("status")
    ? `SUM(CASE WHEN "status" >= 400 AND "status" < 500 THEN 1 ELSE 0 END) AS reqs_4xx, SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS reqs_5xx`
    : `0 AS reqs_4xx, 0 AS reqs_5xx`;
  const bytesExpr = cols.has("resp_bytes")
    ? `SUM("resp_bytes") AS total_bytes`
    : `0 AS total_bytes`;
  const rttExpr = cols.has("tcp_rtt")
    ? `MEDIAN("tcp_rtt") / 1000.0 AS median_rtt_ms`
    : `NULL AS median_rtt_ms`;
  const uaExpr = cols.has("ua") ? `MIN("ua") AS ua` : `NULL AS ua`;
  const urlExpr = cols.has("url")
    ? `COUNT(DISTINCT "url") AS unique_urls`
    : `0 AS unique_urls`;

  const minReqsFlag = Math.max(
    1,
    Number(options.minReqsFlag ?? SESSIONS_DEFAULT_MIN_REQS_FLAG),
  );
  const min4xxPctFlag = Math.max(
    0,
    Number(options.min4xxPctFlag ?? SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG),
  );
  const sortCol = SESSIONS_SORTABLE_COLUMNS.has(options.sortBy ?? "")
    ? (options.sortBy as string)
    : "req_count";
  const sortDir = options.sortDir === "asc" ? "ASC" : "DESC";
  const limit = Math.max(1, Math.min(Number(options.limit ?? 50), 500));
  const offset = Math.max(0, Number(options.offset ?? 0));

  const postFilters: string[] = [];
  const startMs = Date.parse(options.startTime);
  const endMs = Date.parse(options.endTime);
  const gapMs = SESSIONS_INACTIVITY_GAP_SECONDS * 1000;
  const sliceFromMs = options.sliceFromMs != null && options.sliceFromMs > startMs
    ? Math.trunc(options.sliceFromMs) : null;
  if (sliceFromMs != null) postFilters.push(`start_t >= ${sliceFromMs}`);
  if (options.flaggedOnly) postFilters.push("flagged = TRUE");
  if (options.streamingOnly) postFilters.push("streaming_reqs > 0");
  const whereClause =
    postFilters.length > 0 ? `WHERE ${postFilters.join(" AND ")}` : "";

  const msSql = sessionsMsSql(cols);
  const filterSql = sessionsFilterSql(options.filters, options.customColumnTypes);
  const rawColumns = ["cmcd_sid", "edge_sid", "edge", "asn", "country", "status", "resp_bytes", "tcp_rtt", "ua", "url"]
    .filter((column) => cols.has(column)).map((column) => `"${column}"`);
  const order = `${sortCol} ${sortDir}, "ip", "ja4", session_start`;
  // Median RTT and distinct URLs don't combine across partial groups and are costly, so each is
  // computed for every session only when it ranks the page, and otherwise for the page's sessions.
  const rankStat = sortCol === "median_rtt_ms" ? rttExpr : sortCol === "unique_urls" ? urlExpr : null;
  const pageStatExprs = [
    ...(sortCol === "median_rtt_ms" ? [] : [{ expr: rttExpr, name: "median_rtt_ms", needs: "tcp_rtt" }]),
    ...(sortCol === "unique_urls" ? [] : [{ expr: urlExpr, name: "unique_urls", needs: "url" }]),
  ];
  const pageStats = pageStatExprs.some(({ needs }) => cols.has(needs));
  const baseSelect = `${msSql} AS t, "ip", ${ja4Select}${rawColumns.map((column) => `, ${column}`).join("")}`;
  const rowsBetween = (fromMs: number, toMs: number) => `
      SELECT ${baseSelect}
      FROM ${tableName}
      WHERE "ip" IS NOT NULL AND "ip" != ''
        AND ${msSql} >= ${fromMs}
        AND ${msSql} < ${toMs}
        AND (${filterSql})`;
  // An explicit semi-join streams the padding rows; a correlated EXISTS buffered them all.
  const sliceSql = sliceFromMs == null ? "" : `
    slice_clients AS MATERIALIZED (
      SELECT DISTINCT "ip"${hasJa4 ? `, "ja4"` : ""}
      FROM (${rowsBetween(sliceFromMs, endMs)})
    ),`;
  const baseSql = sliceFromMs == null ? rowsBetween(startMs, endMs) : `${rowsBetween(sliceFromMs, endMs)}
      UNION ALL
      SELECT p.*
      FROM (${rowsBetween(Math.max(startMs, sliceFromMs - gapMs), sliceFromMs)}) p
      SEMI JOIN slice_clients s ON s."ip" = p."ip"${hasJa4 ? ` AND s."ja4" IS NOT DISTINCT FROM p."ja4"` : ""}`;

  // DuckDB-WASM can't spill, and ordering every raw row by its string client key exhausted its
  // memory. Clients get a dense integer id, and since requests less than a minute apart can't
  // straddle a 30-minute gap, only per-client minutes are ordered to find session boundaries.
  return `
    WITH${sliceSql} base AS NOT MATERIALIZED (${baseSql}
    ),
    clients AS (
      SELECT "ip", "ja4", ROW_NUMBER() OVER () AS k
      FROM base
      GROUP BY "ip", "ja4"
    ),
    keyed AS NOT MATERIALIZED (
      SELECT c.k, b.* EXCLUDE ("ip", "ja4")
      FROM base b
      JOIN clients c ON b."ip" = c."ip" AND b."ja4" IS NOT DISTINCT FROM c."ja4"
    ),
    minutes AS (
      SELECT k, t // 60000 AS minute, MIN(t) AS first_t, MAX(t) AS last_t
      FROM keyed
      GROUP BY k, minute
    ),
    minute_sessions AS (
      SELECT k, minute,
        SUM(CASE WHEN gap IS NULL OR gap > ${gapMs} THEN 1 ELSE 0 END) OVER (
          PARTITION BY k ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sid
      FROM (SELECT k, minute, first_t - LAG(last_t) OVER (PARTITION BY k ORDER BY minute) AS gap FROM minutes)
    ),
    sessions_agg AS (
      SELECT
        b.k,
        m.sid,
        MIN(b.t) AS start_t,
        MAX(b.t) AS end_t,
        COUNT(*) AS req_count,
        ${cmcdExpr},
        ${edgeSidExpr},
        ${edgeExpr},
        ${asnExpr},
        ${countryExpr},
        ${statusExpr},
        ${bytesExpr},
        ${uaExpr}${rankStat ? `,
        ${rankStat}` : ""}
      FROM keyed b
      JOIN minute_sessions m ON b.k = m.k AND b.t // 60000 = m.minute
      GROUP BY b.k, m.sid
    ),
    sessions_flagged AS (
      SELECT c."ip", c."ja4", EPOCH_MS(a.start_t) AS session_start, EPOCH_MS(a.end_t) AS session_end, a.*,
        (
          req_count >= ${minReqsFlag}
          OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= ${min4xxPctFlag})
        ) AS flagged
      FROM sessions_agg a
      JOIN clients c ON a.k = c.k
    ),
    page AS (
      SELECT *
      FROM sessions_flagged
      ${whereClause}
      ORDER BY ${order}
      LIMIT ${limit} OFFSET ${offset}
    )${pageStats ? `,
    page_stats AS (
      SELECT p.k, p.sid, ${pageStatExprs.map(({ expr }) => expr).join(", ")}
      FROM keyed b
      JOIN page p ON b.k = p.k AND b.t BETWEEN p.start_t AND p.end_t
      GROUP BY p.k, p.sid
    )` : ""}
    SELECT p.* EXCLUDE (k, sid, start_t, end_t)${pageStatExprs
      .map(({ expr, name }) => (pageStats ? `, s.${name}` : `, ${expr}`)).join("")}
    FROM page p
    ${pageStats ? "LEFT JOIN page_stats s ON p.k = s.k AND p.sid = s.sid" : ""}
    ORDER BY ${order}
  `;
}

/**
 * Stitch per-hour `sessions.parquet` rollup bundles across hour boundaries using
 * a 30-minute inactivity threshold (`(first_ts - prev_last_ts) > INTERVAL 30 MINUTES`).
 */
export function buildSessionsRollupStitchSql(
  rollupTable = "sessions_rollup",
): string {
  const tbl = requireSafeIdent(rollupTable, "rollupTable");
  return `
    WITH ordered AS (
      SELECT f.*,
        LAG(f.last_ts) OVER (PARTITION BY f.ip, f.ja4 ORDER BY f.first_ts) AS prev_last_ts
      FROM ${tbl} f
    ),
    marked AS (
      SELECT *,
        CASE WHEN prev_last_ts IS NULL OR (first_ts - prev_last_ts) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new
      FROM ordered
    ),
    grouped AS (
      SELECT *,
        SUM(is_new) OVER (PARTITION BY ip, ja4 ORDER BY first_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stitched_sid
      FROM marked
    )
    SELECT
      ip,
      ja4,
      MIN(first_ts) AS session_start,
      MAX(last_ts) AS session_end,
      SUM(req_count) AS req_count,
      SUM(reqs_4xx) AS reqs_4xx,
      SUM(reqs_5xx) AS reqs_5xx,
      SUM(total_bytes) AS total_bytes,
      MIN(country) AS country,
      MIN(asn) AS asn,
      MIN(ua) AS ua
    FROM grouped
    GROUP BY ip, ja4, stitched_sid
  `;
}

/**
 * Pass 2: each page session's smallest UA and distinct URL count (unless that ranked it), read from
 * only the files its span overlaps, one merged span at a time.
 */
async function enrichSessionsPage(
  rows: Record<string, unknown>[],
  body: SessionsListRequest,
  cols: ReadonlySet<string>,
  ctx: CloudSessionsContext,
  loadRange: SessionsRangeLoader,
): Promise<Record<string, unknown>[]> {
  const ua = cols.has("ua");
  const urls = cols.has("url") && body.sort_by !== "unique_urls";
  if (!rows.length || !ua && !urls) return rows;
  const msOf = (value: unknown) =>
    typeof value === "number" ? value : value instanceof Date ? value.getTime() : Date.parse(String(value));
  const keys: SessionsPageKey[] = rows.map((row) => ({
    ip: String(row.ip ?? ""),
    ja4: row.ja4 == null ? null : String(row.ja4),
    startMs: msOf(row.session_start),
    endMs: msOf(row.session_end),
  }));
  const wanted = SESSIONS_ENRICH_COLUMNS.filter((column) =>
    !(SESSIONS_PAGE_ONLY_COLUMNS as readonly string[]).includes(column) || (column === "ua" ? ua : urls));
  const columns = sessionsScanColumns(body.filters) === null ? null
    : [...new Set([...wanted, ...Object.keys(body.filters ?? {})])];
  const stats = new Map<number, Record<string, unknown>>();
  for (const span of sessionsEnrichSpans(keys)) {
    const inSpan = keys.flatMap((key, i) => key.startMs >= span.startMs && key.endMs <= span.endMs ? [i] : []);
    const read = await loadRange(span.startMs, span.endMs + 1, columns);
    try {
      const found = await read.runSql(buildSessionsEnrichSql({
        tableName: ctx.tableName ?? "requests",
        keys: inSpan.map((i) => keys[i]),
        hasJa4: cols.has("ja4"),
        msSql: sessionsMsSql(cols),
        filterSql: sessionsFilterSql(body.filters, ctx.customColumnTypes),
        ua,
        urls,
      }));
      for (const row of found) stats.set(inSpan[Number(row.i)], row);
    } finally {
      read.release?.();
    }
  }
  return rows.map((row, i) => ({
    ...row,
    ...ua ? { ua: stats.get(i)?.ua ?? null } : {},
    ...urls ? { unique_urls: stats.get(i)?.unique_urls ?? 0 } : {},
  }));
}

export async function handleSessionsList(
  body: SessionsListRequest,
  ctx: CloudSessionsContext = {},
): Promise<
  CloudHttpResult<
    SessionsListResponse | { detail: { error: string; message: string } }
  >
> {
  const startMs = Date.parse(body.start_time);
  const endMs = Date.parse(body.end_time);
  if (
    !Number.isFinite(startMs) ||
    !Number.isFinite(endMs) ||
    endMs < startMs
  ) {
    return {
      status: 400,
      body: {
        detail: {
          error: "invalid_window",
          message: "Invalid start_time or end_time.",
        },
      },
    };
  }

  const maxSpanMs = SESSIONS_MAX_WINDOW_DAYS * 86_400_000;
  if (endMs - startMs > maxSpanMs) {
    return {
      status: 400,
      body: {
        detail: {
          error: "invalid_window",
          message:
            "Sessions view is limited to 7 days. Please narrow your date range.",
        },
      },
    };
  }

  const page = Math.max(1, Number(body.page ?? 1));
  const limit = Math.max(1, Math.min(Number(body.limit ?? 50), 500));
  const offset = (page - 1) * limit;
  const minReqsFlag = Number(
    body.min_reqs_flag ?? SESSIONS_DEFAULT_MIN_REQS_FLAG,
  );
  const min4xxPctFlag = Number(
    body.min_4xx_pct_flag ?? SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG,
  );
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  const serviceId = body.service_id ?? ctx.serviceId ?? "default";
  let cols: ReadonlySet<string> =
    ctx.availableColumns ??
    new Set([
      "timestamp",
      "ip",
      "ja4",
      "status",
      "url",
      "ua",
      "country",
      "asn",
      "resp_bytes",
      "tcp_rtt",
      "edge",
      "edge_sid",
      "cmcd_sid",
    ]);

  const t0 = typeof performance !== "undefined" ? performance.now() : Date.now();
  const loadRange = ctx.loadRange;
  const sqlFor = (sliceFromMs?: number) => buildSessionsCteSql({
    tableName: ctx.tableName ?? "requests",
    startTime: body.start_time,
    endTime: body.end_time,
    // With a loader, `ua` and `url` (wide strings) are read only for the page's own sessions.
    availableColumns: loadRange
      ? new Set([...cols].filter((column) => column !== "ua" && (column !== "url" || body.sort_by === "unique_urls")))
      : cols,
    minReqsFlag,
    min4xxPctFlag,
    flaggedOnly: body.flagged_only,
    streamingOnly: body.streaming_only,
    sortBy: body.sort_by,
    sortDir: body.sort_dir,
    limit,
    offset,
    filters: body.filters,
    customColumnTypes: ctx.customColumnTypes,
    sliceFromMs,
  });
  let rawRows: Record<string, unknown>[] = [];
  const held: { read?: SessionsRead } = {};
  try {
    let runSql = ctx.runSql;
    let loadedFromMs = ctx.loadedFromMs ?? startMs;
    // Rows from `fromMs` must be loaded; a wider read replaces the narrower one.
    const ensureFrom = async (fromMs: number) => {
      if (!loadRange || fromMs >= loadedFromMs) return;
      const next = await loadRange(fromMs, endMs, sessionsListColumns(body.filters, body.sort_by));
      held.read?.release?.();
      held.read = next;
      runSql = next.runSql;
      loadedFromMs = fromMs;
      // A first read with no files can't say which columns the data has; a wider one can.
      const present = ctx.availableColumns ? null : await next.presentColumns?.();
      if (present?.size) cols = present;
    };
    // The newest sessions only need the end of the range: widen a slice from the end until it holds
    // the requested page, so memory follows recent traffic rather than the range.
    if (ctx.runSql && sessionsNewestFirst(body)) {
      for (let sliceMs = SESSIONS_FIRST_SLICE_MS; ; sliceMs *= 4) {
        const sliceFromMs = endMs - sliceMs;
        await ensureFrom(sessionsSliceLoadFrom(sliceFromMs, startMs));
        rawRows = await runSql!(sqlFor(sliceFromMs > startMs ? sliceFromMs : undefined));
        if (sliceFromMs <= startMs || rawRows.length >= limit) break;
      }
    } else if (ctx.runSql) {
      await ensureFrom(startMs);
      rawRows = await runSql!(sqlFor());
    }
    held.read?.release?.();
    held.read = undefined;
    if (loadRange) rawRows = await enrichSessionsPage(rawRows, body, cols, ctx, loadRange);
  } finally {
    held.read?.release?.();
  }

  const sessions: SessionSummaryRow[] = rawRows.map((r) => {
    const rawIp = String(r.ip ?? "");
    const rawJa4 = r.ja4 != null ? String(r.ja4) : null;
    const sessionStart = typeof r.session_start === "number"
      ? new Date(r.session_start).toISOString()
      : String(r.session_start ?? body.start_time);
    const sessionEnd = typeof r.session_end === "number"
      ? new Date(r.session_end).toISOString()
      : String(r.session_end ?? body.end_time);
    const reqCount = Number(r.req_count ?? 0);
    const reqs4xx = Number(r.reqs_4xx ?? 0);
    const reqs5xx = Number(r.reqs_5xx ?? 0);
    const flagged =
      typeof r.flagged === "boolean"
        ? r.flagged
        : reqCount >= minReqsFlag ||
          (reqCount > 0 && (reqs4xx * 100.0) / reqCount >= min4xxPctFlag);

    // Seal token BEFORE masking IP so analyst-no-pii can still drill into `/api/sessions/detail`
    const token = sealSessionToken({
      ip: rawIp,
      ja4: rawJa4,
      s: sessionStart,
      e: sessionEnd,
      svc: serviceId,
    });

    return {
      ip: maskIps ? "0.0.0.0" : rawIp,
      ja4: rawJa4,
      session_start: sessionStart,
      session_end: sessionEnd,
      req_count: reqCount,
      streaming_reqs: Number(r.streaming_reqs ?? 0),
      is_streaming: Number(r.streaming_reqs ?? 0) > 0,
      edge_sid: maskIps
        ? "[redacted]"
        : r.edge_sid != null
          ? String(r.edge_sid)
          : null,
      edge_count: Number(r.edge_count ?? 0),
      shield_count: Number(r.shield_count ?? 0),
      asn: r.asn != null ? Number(r.asn) : null,
      country: r.country != null ? String(r.country) : null,
      reqs_4xx: reqs4xx,
      reqs_5xx: reqs5xx,
      total_bytes: Number(r.total_bytes ?? 0),
      median_rtt_ms:
        r.median_rtt_ms != null ? Number(r.median_rtt_ms) : null,
      ua: r.ua != null ? String(r.ua) : null,
      unique_urls: Number(r.unique_urls ?? 0),
      flagged,
      session_token: token,
    };
  });

  const elapsedMs = Math.max(
    0,
    Math.round(
      ((typeof performance !== "undefined" ? performance.now() : Date.now()) -
        t0) *
        100,
    ) / 100,
  );

  return {
    status: 200,
    body: {
      sessions,
      total: sessions.length,
      page,
      limit,
      has_rtt: cols.has("tcp_rtt"),
      has_ja4: cols.has("ja4"),
      has_edge: cols.has("edge"),
      has_edge_sid: cols.has("edge_sid"),
      has_cmcd: cols.has("cmcd_sid"),
      min_reqs_flag: minReqsFlag,
      min_4xx_pct_flag: min4xxPctFlag,
      asn_names: {},
      section_timings: [{ name: "sessions", duration_ms: elapsedMs }],
    },
  };
}

export async function handleSessionsDetail(
  body: SessionDetailRequest,
  ctx: CloudSessionsContext = {},
): Promise<
  CloudHttpResult<
    SessionDetailResponse | { detail: { error: string; field?: string; message?: string } }
  >
> {
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  let targetIp = body.ip ?? "";
  let targetJa4 = body.ja4 ?? null;
  let startTime = body.start_time ?? "";
  let endTime = body.end_time ?? "";

  if (body.session_token) {
    if (maskIps && body.ip && body.ip !== "0.0.0.0") {
      return {
        status: 403,
        body: {
          detail: {
            error: "pii_filter_forbidden",
            field: "ip",
          },
        },
      };
    }
    const opened = openSessionToken(body.session_token);
    if (!opened) {
      return {
        status: 400,
        body: {
          detail: {
            error: "invalid_session_token",
            message: "Invalid or corrupted session_token.",
          },
        },
      };
    }
    targetIp = opened.ip;
    targetJa4 = opened.ja4;
    startTime = opened.s;
    endTime = opened.e;
  } else if (maskIps) {
    if (targetIp && targetIp !== "0.0.0.0") {
      return {
        status: 403,
        body: {
          detail: {
            error: "pii_policy_violation",
            field: "ip",
          },
        },
      };
    }
    if (!targetJa4) {
      return {
        status: 403,
        body: {
          detail: {
            error: "pii_policy_violation",
            field: "ip",
          },
        },
      };
    }
  }

  const tableName = requireSafeIdent(ctx.tableName ?? "requests", "tableName");
  const useJa4Only = maskIps && !body.session_token && (!targetIp || targetIp === "0.0.0.0") && Boolean(targetJa4);
  const ja4Clause =
    targetJa4 != null && targetJa4 !== ""
      ? `${useJa4Only ? "" : " AND "}"ja4" = '${escapeSqlLiteral(targetJa4)}'`
      : "";
  const ipWherePrefix = useJa4Only ? "" : `"ip" = '${escapeSqlLiteral(targetIp)}' AND `;
  const timestampSql = ctx.availableColumns?.has("ts_ms")
    ? 'EPOCH_MS(CAST("ts_ms" AS BIGINT))'
    : 'EPOCH_MS(EPOCH_MS("timestamp"))';
  const sql = `
    SELECT *
    FROM ${tableName}
    WHERE ${ipWherePrefix}${timestampSql} >= EPOCH_MS(CAST(${Date.parse(startTime)} AS BIGINT))
      AND ${timestampSql} <= EPOCH_MS(CAST(${Date.parse(endTime)} AS BIGINT))${useJa4Only ? ` AND ${ja4Clause}` : ja4Clause}
    ORDER BY ${timestampSql} ASC
    LIMIT 500
  `;

  const rawRows = ctx.runSql ? await ctx.runSql(sql) : [];
  const normalizedRows = rawRows.map((r) => {
    const copy: Record<string, unknown> = { ...r };
    if (typeof copy.timestamp === "number") {
      copy.timestamp = new Date(copy.timestamp).toISOString();
    }
    return copy;
  });
  const data = redactQueryRowsForPii(normalizedRows, maskIps);
  const columns = data.length > 0 ? Object.keys(data[0]) : [];

  return {
    status: 200,
    body: {
      data,
      columns,
    },
  };
}

// ── Session Scoring (`edge_score*` Analytics & Configuration) ───────────────

export const SCORING_OPTIONAL_COLUMNS: Record<string, string> = {
  edge_score: "DOUBLE",
  edge_score_l1: "DOUBLE",
  edge_score_l2: "DOUBLE",
  edge_sid: "VARCHAR",
  edge_score_reason: "VARCHAR",
  edge_cookie_compliance: "VARCHAR",
  edge_score_rtt_us: "BIGINT",
  edge_score_exec_us: "BIGINT",
};

/**
 * Synthesize NULL columns for any optional `edge_score*` fields not yet present
 * in the service's Parquet schema (`scoring_source(table, cols)`).
 */
export function buildScoringSourceSql(
  tableName = "requests",
  availableColumns?: ReadonlySet<string>,
): string {
  const tbl = requireSafeIdent(tableName, "tableName");
  if (!availableColumns) return tbl;
  const missingSelects: string[] = [];
  for (const [col, sqlType] of Object.entries(SCORING_OPTIONAL_COLUMNS)) {
    if (!availableColumns.has(col)) {
      missingSelects.push(`CAST(NULL AS ${sqlType}) AS "${col}"`);
    }
  }
  if (missingSelects.length === 0) return tbl;
  return `(SELECT *, ${missingSelects.join(", ")} FROM ${tbl})`;
}

export const TOP_FLAGGED_SQL = `
  SELECT
    edge_sid,
    MAX(edge_score) AS max_score,
    AVG(edge_score) AS avg_score,
    MAX(edge_score_l1) AS max_l1,
    MAX(edge_score_l2) AS max_l2,
    COUNT(*) AS requests,
    MIN(timestamp) AS first_seen,
    MAX(timestamp) AS last_seen,
    MODE(edge_score_reason) AS top_reason,
    MODE(edge_cookie_compliance) AS cookie_compliance
  FROM {source}
  WHERE edge_sid IS NOT NULL AND edge_sid != '' AND edge_score IS NOT NULL
  GROUP BY edge_sid
  ORDER BY max_score DESC, requests DESC
  LIMIT 50
`;

export const SCORE_DISTRIBUTION_SQL = `
  SELECT
    CAST(FLOOR(LEAST(GREATEST(edge_score, 0.0), 0.999) * 10) * 10 AS INTEGER) AS bucket_min,
    COUNT(*) AS requests,
    COUNT(DISTINCT edge_sid) AS sessions
  FROM {source}
  WHERE edge_score IS NOT NULL
  GROUP BY 1
  ORDER BY 1 ASC
`;

export const COMPLIANCE_BREAKDOWN_SQL = `
  SELECT
    COALESCE(NULLIF(edge_cookie_compliance, ''), 'unknown') AS compliance,
    COUNT(*) AS requests,
    COUNT(DISTINCT edge_sid) AS sessions,
    AVG(edge_score) AS avg_score
  FROM {source}
  WHERE edge_score IS NOT NULL
  GROUP BY 1
  ORDER BY requests DESC
`;

export interface ScoringLabelRecord {
  id: string;
  sid: string;
  label: "good" | "bad" | "neutral";
  notes?: string;
  created_at: string;
}

export interface ScoringServiceState {
  enabled: boolean;
  threshold: number;
  enforce_threshold: number;
  l2_enforce: boolean;
  exclude_regex: string;
  enforce_status_code: number;
  labels: Record<string, ScoringLabelRecord>;
}

const scoringStateByService = new Map<string, ScoringServiceState>();

function getOrCreateScoringState(serviceId: string): ScoringServiceState {
  let st = scoringStateByService.get(serviceId);
  if (!st) {
    st = {
      enabled: false,
      threshold: 70,
      enforce_threshold: 90,
      l2_enforce: false,
      exclude_regex: "",
      enforce_status_code: 429,
      labels: {},
    };
    scoringStateByService.set(serviceId, st);
  }
  return st;
}

export async function handleSessionScoringRoute(
  method: string,
  serviceId: string,
  subPath: string,
  body: Record<string, unknown> = {},
  ctx: CloudSessionsContext = {},
): Promise<CloudHttpResult> {
  const m = method.toUpperCase();
  const cleanSub = subPath.replace(/^\/+|\/+$/g, "");
  const st = getOrCreateScoringState(serviceId);

  const buildStatus = () => ({
    enabled: st.enabled,
    scorer_drift: false,
    drift_detail: null,
    drift_known: true,
    service_id: serviceId,
  });

  const buildConfig = () => ({
    status: buildStatus(),
    threshold: { threshold: st.threshold, enforce_threshold: st.enforce_threshold },
    exclude_regex: { pattern: st.exclude_regex },
    enforce_status_code: { status_code: st.enforce_status_code },
    l2_enforce: { enabled: st.l2_enforce },
  });

  const buildHealth = () => ({
    since_hours: 24,
    total_edge_rows: 0,
    scored_rows: 0,
    fire_rate_pct: 0,
    distinct_sids: 0,
    avg_score: null,
    p50_score: null,
    p95_score: null,
    max_score: null,
    scorer_errors: 0,
    fail_open_rate_pct: 0,
    top_reasons: [],
    fail_open_breakdown: [],
    latency: {
      avg_rtt_ms: null,
      p95_rtt_ms: null,
      avg_exec_us: null,
      p95_exec_us: null,
    },
    matrix_staleness: {
      version: 1,
      updated_at: null,
      stale: false,
    },
    l2_enforce: st.l2_enforce,
  });

  const buildEvaluation = () => {
    const labels = Object.values(st.labels);
    const good = labels.filter((l) => l.label === "good").length;
    const bad = labels.filter((l) => l.label === "bad").length;
    const neutral = labels.filter((l) => l.label === "neutral").length;
    return {
      ready: good >= 20 && bad >= 20,
      min_per_class: 20,
      counts: { good, bad, neutral },
      auc_roc: null,
      auc_pr: null,
      f1: null,
      precision: null,
      recall: null,
    };
  };

  const buildAnalytics = () => ({
    top_flagged: { since_hours: 24, count: 0, rows: [] },
    score_distribution: { since_hours: 24, count: 0, rows: [] },
    compliance_breakdown: { since_hours: 24, count: 0, rows: [] },
    latency_timeseries: {
      since_hours: 24,
      count: 0,
      rows: [],
      has_latency: false,
      granularity: "1h",
    },
    health: buildHealth(),
    evaluation: buildEvaluation(),
    evaluation_per_reason: { rows: [] },
  });

  if (cleanSub === "status" && m === "GET") {
    return { status: 200, body: buildStatus() };
  }
  if (cleanSub === "enable" && m === "POST") {
    st.enabled = true;
    return { status: 200, body: buildStatus() };
  }
  if (cleanSub === "disable" && m === "POST") {
    st.enabled = false;
    return { status: 200, body: buildStatus() };
  }
  if ((cleanSub === "redeploy" || cleanSub === "rescore") && m === "POST") {
    return { status: 200, body: { ok: true, status: buildStatus() } };
  }
  if (cleanSub === "config" && m === "GET") {
    return { status: 200, body: buildConfig() };
  }
  if (cleanSub === "health" && m === "GET") {
    return { status: 200, body: buildHealth() };
  }
  if (cleanSub === "analytics" && m === "GET") {
    return { status: 200, body: buildAnalytics() };
  }
  if (cleanSub === "dashboard" && m === "GET") {
    return {
      status: 200,
      body: {
        ...buildConfig(),
        ...buildAnalytics(),
        curves: { roc: [], pr: [] },
        threshold_preview: {
          threshold: st.threshold,
          since_hours: 24,
          total_scored_sessions: 0,
          flagged: { total: 0, good: 0, bad: 0, unlabeled: 0 },
          passed: { good: 0, bad: 0, unlabeled: 0 },
          precision: null,
          recall: null,
        },
      },
    };
  }
  if (cleanSub === "top-flagged" && m === "GET") {
    const source = buildScoringSourceSql(
      ctx.tableName ?? "requests",
      ctx.availableColumns,
    );
    const rows = ctx.runSql
      ? await ctx.runSql(TOP_FLAGGED_SQL.replace("{source}", source))
      : [];
    return { status: 200, body: { since_hours: 24, count: rows.length, rows } };
  }
  if (cleanSub === "score-distribution" && m === "GET") {
    return { status: 200, body: { since_hours: 24, count: 0, rows: [] } };
  }
  if (cleanSub === "compliance-breakdown" && m === "GET") {
    return { status: 200, body: { since_hours: 24, count: 0, rows: [] } };
  }
  if (cleanSub === "latency-timeseries" && m === "GET") {
    return {
      status: 200,
      body: {
        since_hours: 24,
        count: 0,
        rows: [],
        has_latency: false,
        granularity: "1h",
      },
    };
  }
  if (cleanSub === "threshold-preview" && m === "GET") {
    return {
      status: 200,
      body: {
        threshold: st.threshold,
        since_hours: 24,
        total_scored_sessions: 0,
        flagged: { total: 0, good: 0, bad: 0, unlabeled: 0 },
        passed: { good: 0, bad: 0, unlabeled: 0 },
        precision: null,
        recall: null,
      },
    };
  }
  if (cleanSub === "evaluation" && m === "GET") {
    return { status: 200, body: buildEvaluation() };
  }
  if (cleanSub === "evaluation/per-reason" && m === "GET") {
    return { status: 200, body: { rows: [] } };
  }
  if (cleanSub === "curves" && m === "GET") {
    return { status: 200, body: { roc: [], pr: [] } };
  }
  if (cleanSub === "labels" && m === "GET") {
    const list = Object.values(st.labels);
    return {
      status: 200,
      body: {
        labels: list,
        counts: buildEvaluation().counts,
      },
    };
  }
  if (cleanSub === "labels" && m === "POST") {
    const sid = String(body.sid ?? "");
    const label = (body.label as "good" | "bad" | "neutral") ?? "neutral";
    const id = `lbl-${Object.keys(st.labels).length + 1}`;
    const record: ScoringLabelRecord = {
      id,
      sid,
      label,
      notes: typeof body.notes === "string" ? body.notes : undefined,
      created_at: new Date().toISOString(),
    };
    st.labels[id] = record;
    return { status: 200, body: record };
  }
  if (cleanSub.startsWith("labels/") && m === "DELETE") {
    const labelId = cleanSub.slice("labels/".length);
    delete st.labels[labelId];
    return { status: 200, body: { deleted: true, id: labelId } };
  }
  if (cleanSub === "threshold") {
    if (m === "PUT") {
      if (typeof body.threshold === "number") st.threshold = body.threshold;
      if (typeof body.enforce_threshold === "number") {
        st.enforce_threshold = body.enforce_threshold;
      }
    }
    return {
      status: 200,
      body: {
        threshold: st.threshold,
        enforce_threshold: st.enforce_threshold,
      },
    };
  }
  if (cleanSub === "enforce-threshold") {
    if (m === "PUT" && typeof body.enforce_threshold === "number") {
      st.enforce_threshold = body.enforce_threshold;
    }
    return { status: 200, body: { enforce_threshold: st.enforce_threshold } };
  }
  if (cleanSub === "l2-enforce") {
    if (m === "PUT" && typeof body.enabled === "boolean") {
      st.l2_enforce = body.enabled;
    }
    return { status: 200, body: { enabled: st.l2_enforce } };
  }
  if (cleanSub === "exclude-regex") {
    if (m === "PUT" && typeof body.pattern === "string") {
      st.exclude_regex = body.pattern;
    }
    return { status: 200, body: { pattern: st.exclude_regex } };
  }
  if (cleanSub === "exclude-regex/validate" && m === "POST") {
    const pat = typeof body.pattern === "string" ? body.pattern : "";
    try {
      new RegExp(pat);
      return { status: 200, body: { valid: true, pattern: pat } };
    } catch (err) {
      return {
        status: 400,
        body: {
          valid: false,
          error: err instanceof Error ? err.message : String(err),
        },
      };
    }
  }
  if (cleanSub === "enforce-status-code") {
    if (m === "PUT" && typeof body.status_code === "number") {
      st.enforce_status_code = body.status_code;
    }
    return { status: 200, body: { status_code: st.enforce_status_code } };
  }

  return {
    status: 200,
    body: { ok: true, service_id: serviceId, route: `/api/services/${serviceId}/scoring/${cleanSub}` },
  };
}
