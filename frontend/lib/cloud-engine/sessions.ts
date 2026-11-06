/**
 * Cloud Engine handlers for Page 15 Sessions (`/sessions`, `/sessions/stream`)
 * and Session Scoring (`/admin/session-scoring`):
 * - `POST /api/sessions`
 * - `POST /api/sessions/detail`
 * - `GET|POST|PUT|PATCH|DELETE /api/services/{id}/scoring/*`
 */

import { escapeSqlLiteral, requireSafeIdent } from "./sql-validator";

export const SESSIONS_INACTIVITY_GAP_SECONDS = 1800; // 30 * 60 seconds (INTERVAL 30 MINUTES)
export const SESSIONS_MAX_WINDOW_DAYS = 7;
export const MAX_SESSIONS_WINDOW_DAYS = SESSIONS_MAX_WINDOW_DAYS;
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

export interface SessionDetailRequest {
  service_id?: string;
  session_token?: string;
  ip?: string;
  ja4?: string | null;
  start_time?: string;
  end_time?: string;
}

export interface SessionDetailResponse {
  data: Record<string, unknown>[];
  columns: string[];
}

export interface CloudSessionsContext {
  serviceId?: string;
  tableName?: string;
  maskIps?: boolean;
  role?: "admin" | "analyst" | "analyst-no-pii";
  availableColumns?: ReadonlySet<string>;
  rollupAvailable?: boolean;
  runSql?: (sql: string) => Promise<Record<string, unknown>[]>;
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

/**
 * Build the 5-step 30-minute inactivity-gap sessionization CTE pipeline:
 * `base` -> `gaps` -> `marks` (`INTERVAL 30 MINUTES` / 1800s) -> `sessions_raw` -> `sessions_agg`.
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
  const partKey = hasJa4 ? `"ip", "ja4"` : `"ip"`;
  const groupKey = hasJa4 ? `"ip", "ja4"` : `"ip"`;
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
  if (options.flaggedOnly) postFilters.push("flagged = TRUE");
  if (options.streamingOnly) postFilters.push("streaming_reqs > 0");
  const whereClause =
    postFilters.length > 0 ? `WHERE ${postFilters.join(" AND ")}` : "";

  const startEsc = escapeSqlLiteral(options.startTime);
  const endEsc = escapeSqlLiteral(options.endTime);

  return `
    WITH base AS (
      SELECT
        "timestamp" AS ts,
        "ip",
        ${ja4Select},
        ${cols.has("cmcd_sid") ? `"cmcd_sid"` : `NULL AS "cmcd_sid"`},
        ${cols.has("edge_sid") ? `"edge_sid"` : `NULL AS "edge_sid"`},
        ${cols.has("edge") ? `"edge"` : `NULL AS "edge"`},
        ${cols.has("asn") ? `"asn"` : `NULL AS "asn"`},
        ${cols.has("country") ? `"country"` : `NULL AS "country"`},
        ${cols.has("status") ? `"status"` : `NULL AS "status"`},
        ${cols.has("resp_bytes") ? `"resp_bytes"` : `NULL AS "resp_bytes"`},
        ${cols.has("tcp_rtt") ? `"tcp_rtt"` : `NULL AS "tcp_rtt"`},
        ${cols.has("ua") ? `"ua"` : `NULL AS "ua"`},
        ${cols.has("url") ? `"url"` : `NULL AS "url"`}
      FROM ${tableName}
      WHERE "ip" IS NOT NULL AND "ip" != ''
        AND "timestamp" >= CAST('${startEsc}' AS TIMESTAMPTZ)
        AND "timestamp" <= CAST('${endEsc}' AS TIMESTAMPTZ)
    ),
    gaps AS (
      SELECT *,
        epoch_ms(ts) - LAG(epoch_ms(ts)) OVER (PARTITION BY ${partKey} ORDER BY ts) AS gap
      FROM base
    ),
    marks AS (
      SELECT *,
        CASE WHEN gap IS NULL OR gap > ${SESSIONS_INACTIVITY_GAP_SECONDS * 1000} THEN 1 ELSE 0 END AS is_new
      FROM gaps
    ),
    sessions_raw AS (
      SELECT *,
        SUM(is_new) OVER (
          PARTITION BY ${partKey}
          ORDER BY ts
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sid
      FROM marks
    ),
    sessions_agg AS (
      SELECT
        "ip",
        ${hasJa4 ? `"ja4"` : `NULL AS "ja4"`},
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) AS req_count,
        ${cmcdExpr},
        ${edgeSidExpr},
        ${edgeExpr},
        ${asnExpr},
        ${countryExpr},
        ${statusExpr},
        ${bytesExpr},
        ${rttExpr},
        ${uaExpr},
        ${urlExpr}
      FROM sessions_raw
      GROUP BY ${groupKey}, sid
    ),
    sessions_flagged AS (
      SELECT *,
        (
          req_count >= ${minReqsFlag}
          OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= ${min4xxPctFlag})
        ) AS flagged
      FROM sessions_agg
    )
    SELECT *
    FROM sessions_flagged
    ${whereClause}
    ORDER BY ${sortCol} ${sortDir}
    LIMIT ${limit} OFFSET ${offset}
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
  const cols =
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
  const sql = buildSessionsCteSql({
    tableName: ctx.tableName ?? "requests",
    startTime: body.start_time,
    endTime: body.end_time,
    availableColumns: cols,
    minReqsFlag,
    min4xxPctFlag,
    flaggedOnly: body.flagged_only,
    streamingOnly: body.streaming_only,
    sortBy: body.sort_by,
    sortDir: body.sort_dir,
    limit,
    offset,
  });

  const rawRows = ctx.runSql ? await ctx.runSql(sql) : [];
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

  const tableName = requireSafeIdent(ctx.tableName ?? "requests", "tableName");
  const ja4Clause =
    targetJa4 != null && targetJa4 !== ""
      ? ` AND "ja4" = '${escapeSqlLiteral(targetJa4)}'`
      : "";
  const sql = `
    SELECT *
    FROM ${tableName}
    WHERE "ip" = '${escapeSqlLiteral(targetIp)}'
      AND "timestamp" >= CAST('${escapeSqlLiteral(startTime)}' AS TIMESTAMPTZ)
      AND "timestamp" <= CAST('${escapeSqlLiteral(endTime)}' AS TIMESTAMPTZ)${ja4Clause}
    ORDER BY "timestamp" ASC
    LIMIT 500
  `;

  const rawRows = ctx.runSql ? await ctx.runSql(sql) : [];
  const data = rawRows.map((r) => {
    const copy: Record<string, unknown> = { ...r };
    if (typeof copy.timestamp === "number") {
      copy.timestamp = new Date(copy.timestamp).toISOString();
    }
    if (!maskIps) return copy;
    if ("ip" in copy) copy.ip = "0.0.0.0";
    if ("edge_sid" in copy && copy.edge_sid != null) copy.edge_sid = "[redacted]";
    if ("cookie_session" in copy && copy.cookie_session != null) {
      copy.cookie_session = "[redacted]";
    }
    return copy;
  });
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
