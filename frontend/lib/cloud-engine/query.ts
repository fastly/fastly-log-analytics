/**
 * Cloud Engine handlers for Page 15 SQL Query Console (`/query`):
 * - `POST /api/query`
 * - `GET /api/query/schema` (`/api/query/schema/{service_id}`)
 * - `GET /api/presets`
 */

import {
  MAX_QUERY_ROWS,
  SQLValidationError,
  isDistinctFromOperator,
  isNonKeywordTokenPos,
  isSimpleSelectStatement,
  stripSqlComments,
  tokenizeSql,
  validateSqlQuery,
  validateUserSql,
  validate_user_sql,
  type SqlToken,
} from "./sql-validator";
import {
  FIELD_DUCKDB_TYPES,
  RUM_ERRORS_COLUMNS,
  RUM_VITALS_COLUMNS,
  type DuckDBColumnType,
} from "./dashboard/duckdb";
import type { Schema } from "@/types/api";

export {
  MAX_QUERY_ROWS,
  SQLValidationError,
  validateSqlQuery,
  validateUserSql,
  validate_user_sql,
};

export interface PresetQueryItem {
  name: string;
  description: string;
  sql: string;
  category: "Traffic" | "Performance" | "Cache" | "Origin" | "Security" | "Network";
}

export interface QuerySchemaColumn {
  column: string;
  type: string;
}

export interface QuerySchemaResponse {
  source: string;
  table_name: string;
  schema: QuerySchemaColumn[];
  debug_queries: unknown[];
  debug_calls: unknown[];
}

export interface QueryRequest {
  service_id?: string;
  sql: string;
  max_rows?: number;
  explain?: boolean;
  start_time?: string;
  end_time?: string;
}

export type QueryResponse = Schema<"QueryResponse">;

export interface CloudQueryContext {
  serviceId?: string;
  tableName?: string;
  maskIps?: boolean;
  role?: "admin" | "analyst" | "analyst-no-pii";
  allowedTables?: Iterable<string>;
  schemaColumns?: QuerySchemaColumn[];
  runSql?: (sql: string) => Promise<Record<string, unknown>[]>;
  /** Restricts `table` to rows at or after `ms`: the newest window a scan loaded. */
  since?: { table: string; ms: number };
}

export interface CloudHttpResult<T = unknown> {
  status: number;
  body: T;
}

export const DEFAULT_SCHEMA_COLUMNS: readonly QuerySchemaColumn[] = [
  { column: "timestamp", type: "TIMESTAMPTZ" },
  { column: "ip", type: "VARCHAR" },
  { column: "url", type: "VARCHAR" },
  { column: "host", type: "VARCHAR" },
  { column: "method", type: "VARCHAR" },
  { column: "status", type: "BIGINT" },
  { column: "cache", type: "VARCHAR" },
  { column: "elapsed", type: "BIGINT" },
  { column: "resp_bytes", type: "BIGINT" },
  { column: "country", type: "VARCHAR" },
  { column: "city", type: "VARCHAR" },
  { column: "pop", type: "VARCHAR" },
  { column: "asn", type: "BIGINT" },
  { column: "ua", type: "VARCHAR" },
  { column: "ja4", type: "VARCHAR" },
  { column: "cookie_session", type: "VARCHAR" },
];

export const IP_PII_COLUMNS = new Set([
  "ip",
  "client_ip",
  "c_ip",
  "ip_address",
  "remote_addr",
  "oip",
  "origin_ip",
  "xff",
  "x-forwarded-for",
  "x_forwarded_for",
  "true-client-ip",
  "true_client_ip",
  "fastly-client-ip",
  "fastly_client_ip",
]);
export const SESSION_PII_COLUMNS = new Set([
  "cookie_session",
  "cid",
  "client_id",
  "edge_sid",
  "sid",
  "session_id",
  "cmcd_sid",
  "cmcd_cid",
  "cmcd_session",
]);

/**
 * Build the 26 query console presets across 6 categories (`Traffic`, `Performance`,
 * `Cache`, `Origin`, `Security`, `Network`), starting with the 3 canonical presets
 * (`Sample rows`, `Row count`, `Column stats`).
 */
export function getQueryPresets(
  serviceId?: string,
  tableName = "logs",
): PresetQueryItem[] {
  if (serviceId !== undefined && serviceId.trim() === "") {
    return [];
  }
  const tbl = tableName || "logs";
  return [
    // Traffic (5)
    {
      name: "Sample rows",
      description: "Preview 100 raw log rows",
      sql: `SELECT * FROM ${tbl} LIMIT 100`,
      category: "Traffic",
    },
    {
      name: "Row count",
      description: "Total number of rows",
      sql: `SELECT count(*) AS total_rows FROM ${tbl}`,
      category: "Traffic",
    },
    {
      name: "Column stats",
      description: "Non-null counts and unique values per column",
      sql: `SUMMARIZE ${tbl}`,
      category: "Traffic",
    },
    {
      name: "Status breakdown",
      description: "Request count by HTTP status code",
      sql: `SELECT status, count(*) AS requests FROM ${tbl} GROUP BY status ORDER BY requests DESC`,
      category: "Traffic",
    },
    {
      name: "Top requested URLs",
      description: "Most frequently requested URL paths and total egress bytes",
      sql: `SELECT url, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM ${tbl} GROUP BY url ORDER BY requests DESC LIMIT 50`,
      category: "Traffic",
    },
    // Performance (4)
    {
      name: "Slowest endpoints by P95 latency",
      description: "URLs with highest P95 edge response time in milliseconds",
      sql: `SELECT url, count(*) AS requests, round(percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p95_ms FROM ${tbl} GROUP BY url HAVING count(*) >= 10 ORDER BY p95_ms DESC LIMIT 25`,
      category: "Performance",
    },
    {
      name: "Latency percentiles by PoP",
      description: "P50, P95, and P99 response times across Fastly edge PoPs",
      sql: `SELECT pop, count(*) AS requests, round(percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p50_ms, round(percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p95_ms, round(percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p99_ms FROM ${tbl} GROUP BY pop ORDER BY requests DESC LIMIT 50`,
      category: "Performance",
    },
    {
      name: "Tail latency outliers (P99 vs P50)",
      description: "Endpoints where P99 latency exceeds 5x median latency",
      sql: `SELECT url, count(*) AS requests, round(percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p50_ms, round(percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p99_ms FROM ${tbl} GROUP BY url HAVING count(*) >= 20 ORDER BY p99_ms DESC LIMIT 25`,
      category: "Performance",
    },
    {
      name: "Large uncompressed payloads",
      description: "Responses over 100 KB served with high latency",
      sql: `SELECT url, status, resp_bytes, round(elapsed / 1000.0, 2) AS elapsed_ms FROM ${tbl} WHERE resp_bytes > 102400 ORDER BY resp_bytes DESC LIMIT 50`,
      category: "Performance",
    },
    // Cache (4)
    {
      name: "Cache status distribution",
      description: "Request volume and bandwidth share by Fastly cache state",
      sql: `SELECT cache, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM ${tbl} GROUP BY cache ORDER BY requests DESC`,
      category: "Cache",
    },
    {
      name: "Top cache misses by URL",
      description: "URLs generating the most cache MISS responses",
      sql: `SELECT url, count(*) AS misses, sum(resp_bytes) AS miss_bytes FROM ${tbl} WHERE starts_with(cache, 'MISS') GROUP BY url ORDER BY misses DESC LIMIT 25`,
      category: "Cache",
    },
    {
      name: "Uncacheable PASS endpoints",
      description: "Endpoints bypassing cache via PASS",
      sql: `SELECT url, count(*) AS pass_requests FROM ${tbl} WHERE starts_with(cache, 'PASS') GROUP BY url ORDER BY pass_requests DESC LIMIT 25`,
      category: "Cache",
    },
    {
      name: "Low-TTL cached objects",
      description: "Cached HIT responses with short TTLs under 60 seconds",
      sql: `SELECT url, count(*) AS hits, round(avg(ttl), 1) AS avg_ttl_s, round(avg(age), 1) AS avg_age_s FROM ${tbl} WHERE starts_with(cache, 'HIT') AND ttl < 60 GROUP BY url ORDER BY hits DESC LIMIT 25`,
      category: "Cache",
    },
    // Origin (4)
    {
      name: "Origin 5xx errors by URL",
      description: "URLs returning server errors (HTTP 500-599)",
      sql: `SELECT url, status, count(*) AS error_count FROM ${tbl} WHERE status >= 500 GROUP BY url, status ORDER BY error_count DESC LIMIT 50`,
      category: "Origin",
    },
    {
      name: "Backend traffic & error breakdown",
      description: "Requests and 5xx error counts per origin backend",
      sql: `SELECT backend, count(*) AS requests, sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors_5xx FROM ${tbl} WHERE backend IS NOT NULL GROUP BY backend ORDER BY requests DESC`,
      category: "Origin",
    },
    {
      name: "Slowest origin backends",
      description: "Average and P95 latency grouped by backend",
      sql: `SELECT backend, count(*) AS requests, round(avg(elapsed) / 1000.0, 2) AS avg_ms, round(percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0, 2) AS p95_ms FROM ${tbl} WHERE backend IS NOT NULL GROUP BY backend ORDER BY p95_ms DESC`,
      category: "Origin",
    },
    {
      name: "Edge vs shield request split",
      description: "Comparison of edge requests versus origin-shield requests",
      sql: `SELECT edge, cache, count(*) AS requests FROM ${tbl} GROUP BY edge, cache ORDER BY requests DESC`,
      category: "Origin",
    },
    // Security (5)
    {
      name: "Top client IPs by request volume",
      description: "Highest-volume client IP addresses and 4xx/5xx error counts",
      sql: `SELECT ip, country, asn, count(*) AS requests, sum(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors FROM ${tbl} GROUP BY ip, country, asn ORDER BY requests DESC LIMIT 50`,
      category: "Security",
    },
    {
      name: "JA4 TLS fingerprint distribution",
      description: "Top JA4 client TLS fingerprints and distinct IP counts",
      sql: `SELECT ja4, count(*) AS requests, count(DISTINCT ip) AS distinct_ips FROM ${tbl} WHERE ja4 IS NOT NULL AND ja4 != '' GROUP BY ja4 ORDER BY requests DESC LIMIT 25`,
      category: "Security",
    },
    {
      name: "Authentication 401/403 spikes",
      description: "Client IPs triggering HTTP 401 or 403 responses",
      sql: `SELECT ip, url, status, count(*) AS denied_count FROM ${tbl} WHERE status IN (401, 403) GROUP BY ip, url, status ORDER BY denied_count DESC LIMIT 50`,
      category: "Security",
    },
    {
      name: "404 content discovery scanners",
      description: "Client IPs hitting the highest number of distinct 404 URLs",
      sql: `SELECT ip, count(*) AS total_404s, count(DISTINCT url) AS distinct_404_urls FROM ${tbl} WHERE status = 404 GROUP BY ip HAVING count(DISTINCT url) >= 5 ORDER BY distinct_404_urls DESC LIMIT 25`,
      category: "Security",
    },
    {
      name: "WAF blocked & flagged requests",
      description: "Requests where WAF inspection triggered or blocked traffic",
      sql: `SELECT ip, url, waf_resp, status, count(*) AS hits FROM ${tbl} WHERE waf = true OR waf_resp > 0 GROUP BY ip, url, waf_resp, status ORDER BY hits DESC LIMIT 50`,
      category: "Security",
    },
    // Network (4)
    {
      name: "Traffic by country",
      description: "Request volume and bandwidth grouped by client country",
      sql: `SELECT country, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM ${tbl} GROUP BY country ORDER BY requests DESC LIMIT 50`,
      category: "Network",
    },
    {
      name: "Top autonomous systems (ASNs)",
      description: "Request count and egress bytes by client ASN",
      sql: `SELECT asn, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM ${tbl} WHERE asn IS NOT NULL GROUP BY asn ORDER BY requests DESC LIMIT 50`,
      category: "Network",
    },
    {
      name: "Transport protocol mix (HTTP/3 vs HTTP/2)",
      description: "Breakdown of HTTP protocol versions and transport layer",
      sql: `SELECT proto, transport, count(*) AS requests FROM ${tbl} GROUP BY proto, transport ORDER BY requests DESC`,
      category: "Network",
    },
    {
      name: "Proxy & hosting network traffic",
      description: "Requests classified by proxy type and connection speed",
      sql: `SELECT p_type, c_type, count(*) AS requests FROM ${tbl} WHERE p_type IS NOT NULL AND p_type != '' GROUP BY p_type, c_type ORDER BY requests DESC LIMIT 25`,
      category: "Network",
    },
  ];
}

function isPiiSchemaColumn(rawName: string): boolean {
  const norm = rawName.trim().toLowerCase();
  return IP_PII_COLUMNS.has(norm) || SESSION_PII_COLUMNS.has(norm);
}

export function getQuerySchema(
  serviceId: string,
  maskIps = false,
  customColumns?: readonly QuerySchemaColumn[],
): QuerySchemaResponse {
  const base = customColumns && customColumns.length > 0
    ? [...customColumns]
    : [...DEFAULT_SCHEMA_COLUMNS];
  const schema = maskIps
    ? base.filter((col) => {
        const name = String(
          col.column ?? (col as unknown as { name?: string }).name ?? "",
        );
        return !isPiiSchemaColumn(name);
      })
    : base;
  return {
    source: serviceId || "logs",
    table_name: "logs",
    schema,
    debug_queries: [],
    debug_calls: [],
  };
}

const REWRITER_SUBQUERY_START = new Set([
  "select",
  "with",
  "values",
  "summarize",
  "describe",
  "from",
  "table",
]);
const REWRITER_FROM_END = new Set([
  "select",
  "where",
  "group",
  "having",
  "window",
  "qualify",
  "order",
  "limit",
  "offset",
  "fetch",
  "union",
  "intersect",
  "except",
]);

function formatSqlTableIdent(name: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : quoteSqlIdent(name);
}

/**
 * Token-aware SQL rewriter using `tokenizeSql` character spans so string literals
 * (`'FROM main.requests'`) and comments are never altered:
 * - Strips optional `main.` / `"main".` schema prefixes before known/allowed tables.
 * - Rewrites table-position `logs` / `"logs"` (and quoted `targetTable`) to `targetTable`.
 */
function rewriteSqlTableRefs(
  cleanedSql: string,
  targetTable: string,
  extraAllowedTables?: Iterable<string>,
): { tableRewritten: string; tokens: SqlToken[]; userCteAliases: Set<string> } {
  let tokens: SqlToken[];
  try {
    tokens = tokenizeSql(cleanedSql);
  } catch {
    return { tableRewritten: cleanedSql, tokens: [], userCteAliases: new Set() };
  }
  if (tokens.length === 0) {
    return { tableRewritten: cleanedSql, tokens: [], userCteAliases: new Set() };
  }

  const targetLower = targetTable.toLowerCase();
  const targetRef = formatSqlTableIdent(targetTable);
  const knownTables = new Set<string>([
    "logs",
    "requests",
    "client_vitals",
    "client_errors",
    targetLower,
    ...(extraAllowedTables ? Array.from(extraAllowedTables, (t) => t.toLowerCase()) : []),
  ]);

  // Extract all user-defined CTE aliases across all query/subquery WITH blocks
  const userCteAliases = new Set<string>();
  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i];
    if (
      tok.kind !== "word" ||
      tok.lower !== "with" ||
      !(
        i === 0 ||
        tokens[i - 1]?.value === "(" ||
        (i === 1 && (tokens[0].lower === "summarize" || tokens[0].lower === "describe"))
      )
    ) {
      continue;
    }
    let k = i + 1;
    if (tokens[k]?.kind === "word" && tokens[k].lower === "recursive") {
      k++;
    }
    while (k < tokens.length) {
      const nameTok = tokens[k];
      if (nameTok.kind !== "word" && nameTok.kind !== "quoted_ident") break;
      let afterName = k + 1;
      if (tokens[afterName]?.kind === "symbol" && tokens[afterName].value === "(") {
        const colClose = closingParen(tokens, afterName);
        if (colClose >= tokens.length) break;
        afterName = colClose + 1;
      }
      if (!(tokens[afterName]?.kind === "word" && tokens[afterName].lower === "as")) break;
      let bodyOpen = afterName + 1;
      if (tokens[bodyOpen]?.kind === "word" && tokens[bodyOpen].lower === "not") bodyOpen++;
      if (tokens[bodyOpen]?.kind === "word" && tokens[bodyOpen].lower === "materialized") bodyOpen++;
      if (!(tokens[bodyOpen]?.kind === "symbol" && tokens[bodyOpen].value === "(")) break;
      userCteAliases.add(nameTok.lower);
      const closeIdx = closingParen(tokens, bodyOpen);
      if (closeIdx >= tokens.length) break;
      if (tokens[closeIdx + 1]?.kind === "symbol" && tokens[closeIdx + 1].value === ",") {
        k = closeIdx + 2;
        continue;
      }
      break;
    }
  }

  type Frame = {
    kind: "subquery" | "from_join" | "expr";
    close: ")" | "]" | "}";
    savedInFrom: boolean;
  };
  const parenStack: Frame[] = [];
  let inFromClause = false;
  let expectingTableRef = false;
  const edits: Array<{ start: number; end: number; text: string }> = [];

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i];
    const nextTok = i + 1 < tokens.length ? tokens[i + 1] : null;
    const prevTok = i > 0 ? tokens[i - 1] : null;
    const inExprParen =
      parenStack.length > 0 && parenStack[parenStack.length - 1].kind === "expr";

    if (tok.kind === "symbol" && tok.value === "(") {
      let look = i + 1;
      while (
        look < tokens.length &&
        tokens[look].kind === "symbol" &&
        tokens[look].value === "("
      ) {
        look++;
      }
      const lookTok = look < tokens.length ? tokens[look] : null;
      const isTrimFromCall =
        look === i + 1 &&
        lookTok?.kind === "word" &&
        lookTok.lower === "from" &&
        prevTok?.kind === "word" &&
        (prevTok.lower === "trim" ||
          prevTok.lower === "btrim" ||
          prevTok.lower === "ltrim" ||
          prevTok.lower === "rtrim");
      const hasSubqueryHead = Boolean(
        !isTrimFromCall &&
          lookTok?.kind === "word" &&
          REWRITER_SUBQUERY_START.has(lookTok.lower),
      );
      const isDirectSubquery = hasSubqueryHead && look === i + 1;

      if (expectingTableRef && !isDirectSubquery) {
        parenStack.push({ kind: "from_join", close: ")", savedInFrom: inFromClause });
        inFromClause = true;
        expectingTableRef = !hasSubqueryHead;
      } else if (hasSubqueryHead) {
        parenStack.push({ kind: "subquery", close: ")", savedInFrom: inFromClause });
        inFromClause = false;
        expectingTableRef = false;
      } else {
        parenStack.push({ kind: "expr", close: ")", savedInFrom: inFromClause });
        expectingTableRef = false;
      }
      continue;
    }

    if (tok.kind === "symbol" && (tok.value === "[" || tok.value === "{")) {
      if (!expectingTableRef) {
        parenStack.push({
          kind: "expr",
          close: tok.value === "[" ? "]" : "}",
          savedInFrom: inFromClause,
        });
        continue;
      }
    }

    if (
      tok.kind === "symbol" &&
      (tok.value === ")" || tok.value === "]" || tok.value === "}")
    ) {
      if (
        parenStack.length > 0 &&
        parenStack[parenStack.length - 1].close === tok.value
      ) {
        const frame = parenStack.pop()!;
        inFromClause = frame.savedInFrom;
        expectingTableRef = false;
      }
      continue;
    }

    const isNonKeywordPos = isNonKeywordTokenPos(tokens, i);

    if (!inExprParen) {
      if (tok.kind === "word" && !isNonKeywordPos) {
        const lower = tok.lower;
        if (
          (lower === "from" && !isDistinctFromOperator(tokens, i)) ||
          (lower === "table" && prevTok?.value !== ".") ||
          lower === "join"
        ) {
          inFromClause = true;
          expectingTableRef = true;
          continue;
        }
        if (
          (lower === "summarize" || lower === "describe") &&
          (i === 0 ||
            prevTok?.value === "(" ||
            (prevTok?.kind === "word" &&
              (prevTok.lower === "union" ||
                prevTok.lower === "all" ||
                prevTok.lower === "distinct" ||
                prevTok.lower === "name" ||
                prevTok.lower === "intersect" ||
                prevTok.lower === "except")))
        ) {
          if (
            nextTok &&
            !(
              nextTok.kind === "word" &&
              (nextTok.lower === "select" ||
                nextTok.lower === "with" ||
                nextTok.lower === "values" ||
                nextTok.lower === "from" ||
                nextTok.lower === "table")
            )
          ) {
            inFromClause = true;
            expectingTableRef = true;
          }
          continue;
        }
        const isWithinGroupClause =
          lower === "group" &&
          i > 0 &&
          prevTok?.kind === "word" &&
          prevTok.lower === "within" &&
          !isNonKeywordTokenPos(tokens, i - 1) &&
          nextTok?.kind === "symbol" &&
          nextTok.value === "(";
        if (inFromClause && REWRITER_FROM_END.has(lower) && !isWithinGroupClause) {
          inFromClause = false;
          expectingTableRef = false;
        }
      } else if (inFromClause && tok.kind === "symbol" && tok.value === ",") {
        expectingTableRef = true;
        continue;
      }
    }

    if (
      expectingTableRef &&
      tok.kind === "word" &&
      (tok.lower === "lateral" || tok.lower === "only")
    ) {
      continue;
    }

    // Strip `main.` / `"main".` before any known/allowed table anywhere outside strings/comments
    if (
      (tok.kind === "word" || tok.kind === "quoted_ident") &&
      tok.lower === "main" &&
      prevTok?.value !== "." &&
      nextTok?.kind === "symbol" &&
      nextTok.value === "." &&
      i + 2 < tokens.length
    ) {
      const objTok = tokens[i + 2];
      const afterObj = i + 3 < tokens.length ? tokens[i + 3] : null;
      if (
        (objTok.kind === "word" || objTok.kind === "quoted_ident") &&
        knownTables.has(objTok.lower) &&
        !(afterObj?.kind === "symbol" && afterObj.value === "(")
      ) {
        const mapped =
          objTok.lower === "logs" || objTok.lower === targetLower
            ? targetRef
            : formatSqlTableIdent(objTok.value);
        edits.push({ start: tok.start, end: objTok.end, text: mapped });
        i += 2;
        if (expectingTableRef) {
          expectingTableRef = false;
        }
        continue;
      }
    }

    if (
      expectingTableRef &&
      (tok.kind === "word" || tok.kind === "quoted_ident") &&
      !(nextTok?.kind === "symbol" && (nextTok.value === "." || nextTok.value === "("))
    ) {
      if (tok.lower === "logs" || tok.lower === targetLower) {
        edits.push({ start: tok.start, end: tok.end, text: targetRef });
      } else if (tok.kind === "quoted_ident" && knownTables.has(tok.lower)) {
        edits.push({ start: tok.start, end: tok.end, text: formatSqlTableIdent(tok.value) });
      }
      expectingTableRef = false;
    }
  }

  if (edits.length === 0) {
    return { tableRewritten: cleanedSql, tokens, userCteAliases };
  }
  let out = "";
  let cursor = 0;
  for (const edit of edits) {
    out += cleanedSql.slice(cursor, edit.start) + edit.text;
    cursor = edit.end;
  }
  out += cleanedSql.slice(cursor);
  return { tableRewritten: out, tokens, userCteAliases };
}

/**
 * Rewrite `FROM logs` / `JOIN logs` (or service safe table name) to the
 * in-browser DuckDB table name (default `requests`), and append `LIMIT 1000`
 * if no top-level `LIMIT` is present on a SELECT statement.
 */
export function rewriteCloudQuerySql(
  sql: string,
  targetTable = "requests",
  defaultLimit = 1000,
  allowedTables?: Iterable<string>,
): { rewrittenSql: string; hasExplicitLimit: boolean } {
  const cleaned = stripSqlComments(sql).trim().replace(/;\s*$/, "");
  const { tableRewritten } = rewriteSqlTableRefs(cleaned, targetTable, allowedTables);
  const hasExplicitLimit = /\bLIMIT\s+\d+/i.test(tableRewritten);
  if (isSimpleSelectStatement(tableRewritten) && !hasExplicitLimit) {
    return {
      rewrittenSql: `${tableRewritten} LIMIT ${defaultLimit}`,
      hasExplicitLimit: false,
    };
  }
  return {
    rewrittenSql: tableRewritten,
    hasExplicitLimit,
  };
}

const UA_PROD_VER_RE =
  /\b(?:Mozilla|Chrome|Chromium|CriOS|Firefox|FxiOS|Safari|Version|Edg|EdgA|EdgiOS|OPR|AppleWebKit|Gecko|Mobile)\/\d+(?:\.\d+){1,3}\b/gi;
const EMBEDDED_IPV4_RE = /(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])/g;
const EMBEDDED_IPV6_RE =
  /(?<![A-Fa-f0-9:])(?:(?:[A-Fa-f0-9]{1,4}:){1,6}:?(?:\d{1,3}\.){3}\d{1,3}|::(?:[A-Fa-f0-9]{1,4}:){0,5}(?:\d{1,3}\.){3}\d{1,3}|[A-Fa-f0-9]{1,4}(?::[A-Fa-f0-9]{1,4}){2,7}|(?:[A-Fa-f0-9]{1,4}:){1,6}:(?:[A-Fa-f0-9]{1,4}(?::[A-Fa-f0-9]{1,4}){0,5})?|::(?:[A-Fa-f0-9]{1,4}(?::[A-Fa-f0-9]{1,4}){0,6}))(?![A-Fa-f0-9:])/g;

function parseIpv4Octets(cand: string): [number, number, number, number] | null {
  const parts = cand.split(".");
  if (parts.length !== 4) return null;
  const nums: number[] = [];
  for (const p of parts) {
    if (!(p === "0" || (/^[1-9]\d{0,2}$/.test(p) && Number(p) <= 255))) {
      return null;
    }
    nums.push(Number(p));
  }
  return nums as [number, number, number, number];
}

function parseIpv6Groups(cand: string): number[] | null {
  if (
    (cand.startsWith(":") && !cand.startsWith("::")) ||
    (cand.endsWith(":") && !cand.endsWith("::"))
  ) {
    return null;
  }
  const doubleColonParts = cand.split("::");
  if (doubleColonParts.length > 2) return null;

  const parseSide = (side: string, allowTrailingV4: boolean): number[] | null => {
    if (side === "") return [];
    const segs = side.split(":");
    const out: number[] = [];
    for (let idx = 0; idx < segs.length; idx++) {
      const seg = segs[idx];
      if (allowTrailingV4 && idx === segs.length - 1 && seg.includes(".")) {
        const octets = parseIpv4Octets(seg);
        if (!octets) return null;
        out.push((octets[0] << 8) | octets[1], (octets[2] << 8) | octets[3]);
      } else {
        if (!/^[0-9A-Fa-f]{1,4}$/.test(seg)) return null;
        out.push(Number.parseInt(seg, 16));
      }
    }
    return out;
  };

  if (doubleColonParts.length === 1) {
    const groups = parseSide(doubleColonParts[0], true);
    return groups && groups.length === 8 ? groups : null;
  }
  const left = parseSide(doubleColonParts[0], false);
  const right = parseSide(doubleColonParts[1], true);
  if (!left || !right || left.length + right.length >= 8) return null;
  const zeros = new Array<number>(8 - left.length - right.length).fill(0);
  return [...left, ...zeros, ...right];
}

export function scrubIpText(val: string): string {
  if (!val || (!val.includes(".") && !val.includes(":"))) return val;
  const uaSpans: Array<[number, number]> = [];
  UA_PROD_VER_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = UA_PROD_VER_RE.exec(val)) !== null) {
    uaSpans.push([m.index, m.index + m[0].length]);
  }
  const inUa = (start: number, end: number) =>
    uaSpans.some(([s, e]) => s <= start && end <= e);

  let out = val;
  if (out.includes(":")) {
    out = out.replace(EMBEDDED_IPV6_RE, (cand) => {
      const groups = parseIpv6Groups(cand);
      if (!groups) return cand;
      return "0.0.0.0";
    });
  }
  if (out.includes(".")) {
    out = out.replace(EMBEDDED_IPV4_RE, (cand, offset: number) => {
      if (inUa(offset, offset + cand.length)) return cand;
      const octets = parseIpv4Octets(cand);
      if (!octets) return cand;
      return "0.0.0.0";
    });
  }
  return out;
}

const DUCKDB_UA_VER_RE =
  "\\b(Mozilla|Chrome|Chromium|CriOS|Firefox|FxiOS|Safari|Version|Edg|EdgA|EdgiOS|OPR|AppleWebKit|Gecko|Mobile)/([0-9]+)\\.([0-9]+(?:\\.[0-9]+){0,2})\\b";
const DUCKDB_IPV4_OCTET = "(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])";
const DUCKDB_IPV4_CORE = `${DUCKDB_IPV4_OCTET}(?:\\.${DUCKDB_IPV4_OCTET}){3}`;
const DUCKDB_H16 = "[0-9A-Fa-f]{1,4}";
const DUCKDB_IPV6_CORE = [
  `(?:${DUCKDB_H16}:){6}${DUCKDB_IPV4_CORE}`,
  `(?:${DUCKDB_H16}:){7}${DUCKDB_H16}`,
  `(?:${DUCKDB_H16}:){1,5}:${DUCKDB_IPV4_CORE}`,
  `::(?:${DUCKDB_H16}:){0,5}${DUCKDB_IPV4_CORE}`,
  `(?:${DUCKDB_H16}:){1,6}:(?:${DUCKDB_H16}(?::${DUCKDB_H16}){0,5})?`,
  `::(?:${DUCKDB_H16}(?::${DUCKDB_H16}){0,6})`,
].join("|");
const DUCKDB_IPV6_PAT = `(^|[^0-9A-Za-z:])(${DUCKDB_IPV6_CORE})($|[^0-9A-Za-z:.])`;
const DUCKDB_IPV4_PAT = `(^|[^0-9.])(${DUCKDB_IPV4_CORE})($|[^0-9.])`;

function buildDuckDbScrubTextExpr(colExpr: string): string {
  let e = colExpr;
  e = `regexp_replace(${e}, '${DUCKDB_UA_VER_RE}', '\\1/\\2\x01UA\x01\\3', 'gi')`;
  e = `regexp_replace(${e}, '${DUCKDB_IPV6_PAT}', '\\1\x01IP\x01\\3', 'g')`;
  e = `regexp_replace(${e}, '${DUCKDB_IPV6_PAT}', '\\1\x01IP\x01\\3', 'g')`;
  e = `regexp_replace(${e}, '${DUCKDB_IPV4_PAT}', '\\1\x01IP\x01\\3', 'g')`;
  e = `regexp_replace(${e}, '${DUCKDB_IPV4_PAT}', '\\1\x01IP\x01\\3', 'g')`;
  return `replace(replace(${e}, '\x01UA\x01', '.'), '\x01IP\x01', '0.0.0.0')`;
}

function scrubBytesView(bytes: Uint8Array): Uint8Array {
  if (bytes.byteLength === 0) return bytes;
  try {
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    const scrubbed = scrubIpText(decoded);
    return scrubbed === decoded ? bytes : new TextEncoder().encode(scrubbed);
  } catch {
    return new Uint8Array(0);
  }
}

function scrubNestedValue(val: unknown): unknown {
  if (val == null) return val;
  if (typeof val === "string") {
    return val === "" ? val : scrubIpText(val);
  }
  if (
    typeof val === "number" ||
    typeof val === "boolean" ||
    typeof val === "bigint" ||
    val instanceof Date
  ) {
    return val;
  }
  if (val instanceof Uint8Array) {
    return scrubBytesView(val);
  }
  if (ArrayBuffer.isView(val)) {
    return scrubBytesView(new Uint8Array(val.buffer, val.byteOffset, val.byteLength));
  }
  if (val instanceof ArrayBuffer) {
    return scrubBytesView(new Uint8Array(val)).buffer;
  }
  if (Array.isArray(val)) {
    return val.map((item) => scrubNestedValue(item));
  }
  if (val instanceof Map) {
    const outMap = new Map<unknown, unknown>();
    for (const [k, v] of val.entries()) {
      if (typeof k === "string") {
        const lower = k.toLowerCase();
        if (IP_PII_COLUMNS.has(lower) && v != null && v !== "") {
          outMap.set(scrubIpText(k), "0.0.0.0");
          continue;
        }
        if (SESSION_PII_COLUMNS.has(lower) && v != null && v !== "") {
          outMap.set(scrubIpText(k), "[redacted]");
          continue;
        }
        outMap.set(scrubIpText(k), scrubNestedValue(v));
      } else {
        outMap.set(scrubNestedValue(k), scrubNestedValue(v));
      }
    }
    return outMap;
  }
  if (typeof val === "object") {
    const maybeJson = (val as { toJSON?: unknown }).toJSON;
    if (typeof maybeJson === "function") {
      return scrubNestedValue((maybeJson as () => unknown).call(val));
    }
    const rec = val as Record<string, unknown>;
    const outObj: Record<string, unknown> = {};
    for (const k of Object.keys(rec)) {
      const lower = k.toLowerCase();
      const v = rec[k];
      const safeKey = scrubIpText(k);
      if (IP_PII_COLUMNS.has(lower) && v != null && v !== "") {
        outObj[safeKey] = "0.0.0.0";
      } else if (SESSION_PII_COLUMNS.has(lower) && v != null && v !== "") {
        outObj[safeKey] = "[redacted]";
      } else {
        outObj[safeKey] = scrubNestedValue(v);
      }
    }
    return outObj;
  }
  return val;
}

export function redactQueryRowsForPii(
  rows: Record<string, unknown>[],
  maskIps: boolean,
): Record<string, unknown>[] {
  if (!maskIps || rows.length === 0) return rows;
  return rows.map((row) => {
    const copy: Record<string, unknown> = { ...row };
    for (const key of Object.keys(copy)) {
      const lower = key.toLowerCase();
      if (IP_PII_COLUMNS.has(lower) && copy[key] != null && copy[key] !== "") {
        copy[key] = "0.0.0.0";
      } else if (
        SESSION_PII_COLUMNS.has(lower) &&
        copy[key] != null &&
        copy[key] !== ""
      ) {
        copy[key] = "[redacted]";
      } else if (copy[key] != null) {
        copy[key] = scrubNestedValue(copy[key]);
      }
    }
    return copy;
  });
}

function unwrapOuterBalancedParens(sql: string): string {
  let cur = sql.trim();
  while (cur.startsWith("(") && cur.endsWith(")")) {
    let toks: SqlToken[];
    try {
      toks = tokenizeSql(cur);
    } catch {
      break;
    }
    if (
      toks.length >= 2 &&
      toks[0].kind === "symbol" &&
      toks[0].value === "(" &&
      closingParen(toks, 0) === toks.length - 1
    ) {
      cur = cur.slice(toks[0].end, toks[toks.length - 1].start).trim();
    } else {
      break;
    }
  }
  return cur;
}

function mergeLeadingCtes(
  sqlStmt: string,
  cteDefs: string[],
  shadowSelectByTable?: ReadonlyMap<string, string>,
  targetTable?: string,
  shadowDescribeByTable?: ReadonlyMap<string, string>,
  describeCteDefs?: string[],
): string {
  if (cteDefs.length === 0) return sqlStmt;
  const trimmed = sqlStmt.trim();
  if (shadowSelectByTable && shadowSelectByTable.size > 0 && targetTable) {
    const showColsMatch =
      /^SHOW\s+COLUMNS\s+FROM\s+(?:(?:"main"|main)\s*\.\s*)?([A-Za-z0-9_"]+)\s*$/i.exec(
        trimmed,
      );
    if (showColsMatch) {
      const refName = showColsMatch[1].replaceAll('"', "").toLowerCase();
      const descSql =
        shadowDescribeByTable?.get(refName) ??
        (refName === "logs"
          ? shadowDescribeByTable?.get(targetTable.toLowerCase())
          : undefined) ??
        shadowSelectByTable.get(refName) ??
        (refName === "logs"
          ? shadowSelectByTable.get(targetTable.toLowerCase())
          : undefined);
      if (descSql) {
        return `DESCRIBE (${descSql})`;
      }
    }

    const kwTargetMatch = /^(SUMMARIZE|DESCRIBE)(?:\s+|(?=\())([\s\S]+)$/i.exec(
      trimmed,
    );
    if (kwTargetMatch) {
      const kw = kwTargetMatch[1];
      const isDescribe = kw.toUpperCase() === "DESCRIBE";
      const unwrappedTarget = unwrapOuterBalancedParens(kwTargetMatch[2]);
      const sumMatch = isDescribe
        ? /^(?:(?:TABLE|FROM)\s+)?(?:(?:"main"|main)\s*\.\s*)?([A-Za-z0-9_"]+)\s*$/i.exec(
            unwrappedTarget,
          )
        : /^(?:TABLE\s+)?(?:(?:"main"|main)\s*\.\s*)?([A-Za-z0-9_"]+)\s*$/i.exec(
            unwrappedTarget,
          );
      if (sumMatch) {
        const refName = sumMatch[1].replaceAll('"', "").toLowerCase();
        const mapToUse =
          isDescribe && shadowDescribeByTable
            ? shadowDescribeByTable
            : shadowSelectByTable;
        const shadowSql =
          mapToUse.get(refName) ??
          (refName === "logs"
            ? mapToUse.get(targetTable.toLowerCase())
            : undefined) ??
          shadowSelectByTable.get(refName) ??
          (refName === "logs"
            ? shadowSelectByTable.get(targetTable.toLowerCase())
            : undefined);
        if (shadowSql) {
          return `${kw} (${shadowSql})`;
        }
      }
    }
  }

  const sumSubMatch = /^(SUMMARIZE|DESCRIBE)(?:\s+|(?=\())([\s\S]+)$/i.exec(
    trimmed,
  );
  if (sumSubMatch) {
    const kw = sumSubMatch[1];
    const subCtes =
      kw.toUpperCase() === "DESCRIBE" &&
      describeCteDefs &&
      describeCteDefs.length > 0
        ? describeCteDefs
        : cteDefs;
    const unwrapped = unwrapOuterBalancedParens(sumSubMatch[2]);
    if (/^(?:SELECT|WITH|VALUES|FROM|TABLE)\b/i.test(unwrapped)) {
      return `${kw} (${mergeLeadingCtes(unwrapped, subCtes)})`;
    }
    return `${kw} (${mergeLeadingCtes(`SELECT * FROM (${unwrapped})`, subCtes)})`;
  }

  if (/^WITH\s+RECURSIVE\b/i.test(trimmed)) {
    return `WITH RECURSIVE ${cteDefs.join(", ")}, ${trimmed.replace(/^WITH\s+RECURSIVE\s*/i, "")}`;
  }
  if (/^WITH\b/i.test(trimmed)) {
    return `WITH ${cteDefs.join(", ")}, ${trimmed.replace(/^WITH\s*/i, "")}`;
  }
  return `WITH ${cteDefs.join(", ")} ${trimmed}`;
}

/**
 * Execute an ad-hoc SQL query for `POST /api/query` in Cloud mode.
 * Validates the query via `validateUserSql` (returning HTTP 403 `sql_not_permitted`
 * on policy violation), executes against DuckDB-Wasm when `ctx.runSql` is provided,
 * enforces `MAX_QUERY_ROWS` truncation, and applies `mask_ips` (`analyst-no-pii`)
 * IP/session redaction (`0.0.0.0`).
 */
export async function executeCloudQuery(
  req: QueryRequest,
  ctx: CloudQueryContext = {},
): Promise<CloudHttpResult<QueryResponse | { detail: { error: string; message: string } }>> {
  const rawSql = typeof req?.sql === "string" ? req.sql.trim() : "";
  if (!rawSql) {
    return {
      status: 400,
      body: {
        detail: {
          error: "empty_sql",
          message: "No SQL provided",
        },
      },
    };
  }

  const allowedTables = ctx.allowedTables ?? ["logs", "requests"];
  try {
    validateUserSql(rawSql, allowedTables);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: 403,
      body: {
        detail: {
          error: "sql_not_permitted",
          message,
        },
      },
    };
  }

  const t0 = typeof performance !== "undefined" ? performance.now() : Date.now();
  const maxRows = Math.max(
    1,
    Math.min(Number(req.max_rows ?? 1000), MAX_QUERY_ROWS),
  );
  const targetTable = ctx.tableName ?? "requests";
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  const effectiveAllowedSet = new Set<string>([
    ...Array.from(allowedTables),
    targetTable,
  ]);
  let innerSql: string;
  let rawTokens: SqlToken[];
  let userCteAliases: Set<string>;
  try {
    const cleanedRaw = stripSqlComments(rawSql).trim().replace(/;\s*$/, "");
    const rewritten = rewriteSqlTableRefs(cleanedRaw, targetTable, allowedTables);
    innerSql = rewritten.tableRewritten;
    rawTokens = rewritten.tokens;
    userCteAliases = rewritten.userCteAliases;
    validateUserSql(innerSql, effectiveAllowedSet);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: 403,
      body: {
        detail: {
          error: "sql_not_permitted",
          message,
        },
      },
    };
  }

  if (maskIps && userCteAliases.size > 0) {
    const reservedCteNames = new Set<string>([
      "logs",
      "requests",
      "client_vitals",
      "client_errors",
      targetTable.toLowerCase(),
      ...Array.from(allowedTables, (t) => t.toLowerCase()),
    ]);
    for (const cteName of userCteAliases) {
      if (reservedCteNames.has(cteName)) {
        return {
          status: 403,
          body: {
            detail: {
              error: "sql_not_permitted",
              message: `CTE alias '${cteName}' collides with a protected table name.`,
            },
          },
        };
      }
    }
  }

  const hasExplicitLimit = /\bLIMIT\s+\d+/i.test(innerSql);
  const rewrittenSql =
    isSimpleSelectStatement(innerSql) && !hasExplicitLimit
      ? `${innerSql} LIMIT ${maxRows}`
      : innerSql;

  const execSql =
    isSimpleSelectStatement(rewrittenSql) && hasExplicitLimit
      ? `SELECT * FROM (${rewrittenSql}) AS _q LIMIT ${maxRows + 1}`
      : isSimpleSelectStatement(rewrittenSql)
        ? `SELECT * FROM (${innerSql}) AS _q LIMIT ${maxRows + 1}`
        : rewrittenSql;

  try {
    let fetchedRows: Record<string, unknown>[] = [];
    let explainPlan: string | null = null;
    let finalRewrittenSql = rewrittenSql;

    if (ctx.runSql) {
      const cteDefs: string[] = [];
      const describeCteDefs: string[] = [];
      const shadowSelectByTable = new Map<string, string>();
      const shadowDescribeByTable = new Map<string, string>();

      if (maskIps) {
        const referencedIdents = new Set(
          rawTokens
            .filter((t) => t.kind === "word" || t.kind === "quoted_ident")
            .map((t) => t.lower),
        );
        const tablesToShadow: string[] = [targetTable];
        const seenLower = new Set<string>([targetTable.toLowerCase()]);
        for (const allowed of allowedTables) {
          const al = allowed.toLowerCase();
          if (al !== "logs" && !seenLower.has(al) && referencedIdents.has(al)) {
            seenLower.add(al);
            tablesToShadow.push(allowed);
          }
        }
        for (const rumTbl of ["client_vitals", "client_errors"]) {
          if (!seenLower.has(rumTbl) && referencedIdents.has(rumTbl)) {
            seenLower.add(rumTbl);
            tablesToShadow.push(rumTbl);
          }
        }

        for (const tbl of tablesToShadow) {
          const describeTarget =
            tbl === "requests" ? "requests" : `main.${quoteSqlIdent(tbl)}`;
          const descRows = await ctx
            .runSql(`DESCRIBE SELECT * FROM ${describeTarget} LIMIT 0`)
            .catch(() => []);
          const validDesc =
            Array.isArray(descRows) &&
            descRows.length > 0 &&
            typeof descRows[0]?.column_name === "string"
              ? descRows
              : [];
          if (validDesc.length > 0) {
            const replacements: string[] = [];
            const piiColIdents: string[] = [];
            for (const col of validDesc) {
              const colName = String(col.column_name);
              const colLower = colName.toLowerCase();
              const colType = String(col.column_type ?? "VARCHAR").toUpperCase();
              const colIdent = quoteSqlIdent(colName);
              const isScalarTextType =
                /^(?:VAR)?CHAR(?:\(\d+\))?$|^(?:TEXT|STRING|BPCHAR)$/i.test(
                  colType.trim(),
                );
              if (IP_PII_COLUMNS.has(colLower)) {
                piiColIdents.push(colIdent);
                replacements.push(
                  isScalarTextType
                    ? `'0.0.0.0' AS ${colIdent}`
                    : `CAST(NULL AS ${colType}) AS ${colIdent}`,
                );
              } else if (SESSION_PII_COLUMNS.has(colLower)) {
                piiColIdents.push(colIdent);
                replacements.push(
                  isScalarTextType
                    ? `'[redacted]' AS ${colIdent}`
                    : `CAST(NULL AS ${colType}) AS ${colIdent}`,
                );
              } else if (isScalarTextType) {
                replacements.push(
                  `${buildDuckDbScrubTextExpr(colIdent)} AS ${colIdent}`,
                );
              }
            }
            const replaceExpr =
              replacements.length > 0 ? ` * REPLACE (${replacements.join(", ")})` : " *";
            const sinceClause =
              ctx.since && ctx.since.table === tbl
                ? ` WHERE ts_ms >= ${Math.floor(ctx.since.ms)}`
                : "";
            const tblShadowSql = `SELECT${replaceExpr} FROM main.${quoteSqlIdent(tbl)}${sinceClause}`;
            const tblDescribeSql =
              piiColIdents.length > 0
                ? `SELECT * EXCLUDE (${piiColIdents.join(", ")}) FROM (${tblShadowSql})`
                : tblShadowSql;
            shadowSelectByTable.set(tbl.toLowerCase(), tblShadowSql);
            shadowDescribeByTable.set(tbl.toLowerCase(), tblDescribeSql);
            cteDefs.push(`${quoteSqlIdent(tbl)} AS (${tblShadowSql})`);
            describeCteDefs.push(`${quoteSqlIdent(tbl)} AS (${tblDescribeSql})`);
          }
        }

        if (
          targetTable.toLowerCase() !== "logs" &&
          shadowSelectByTable.has(targetTable.toLowerCase()) &&
          !shadowSelectByTable.has("logs") &&
          referencedIdents.has("logs")
        ) {
          const logsAliasCte = `"logs" AS (SELECT * FROM ${quoteSqlIdent(targetTable)})`;
          cteDefs.push(logsAliasCte);
          describeCteDefs.push(logsAliasCte);
        }
      }

      if (
        ctx.since &&
        !(maskIps && shadowSelectByTable.has(ctx.since.table.toLowerCase()))
      ) {
        const sinceCte = `${quoteSqlIdent(ctx.since.table)} AS (SELECT * FROM main.${quoteSqlIdent(ctx.since.table)} WHERE ts_ms >= ${Math.floor(ctx.since.ms)})`;
        cteDefs.push(sinceCte);
        if (describeCteDefs.length > 0) {
          describeCteDefs.push(sinceCte);
        }
      }

      const effectiveExplainTarget = mergeLeadingCtes(
        rewrittenSql,
        cteDefs,
        shadowSelectByTable,
        targetTable,
        shadowDescribeByTable,
        describeCteDefs,
      );
      const effectiveExecSql = mergeLeadingCtes(
        execSql,
        cteDefs,
        shadowSelectByTable,
        targetTable,
        shadowDescribeByTable,
        describeCteDefs,
      );
      finalRewrittenSql = effectiveExplainTarget;

      if (req.explain) {
        const planRows = await ctx.runSql(`EXPLAIN ${effectiveExplainTarget}`);
        explainPlan = planRows
          .map((r) => Object.values(r).join(" "))
          .join("\n");
      }
      fetchedRows = await ctx.runSql(effectiveExecSql);
    } else if (ctx.since) {
      finalRewrittenSql = `WITH ${quoteSqlIdent(ctx.since.table)} AS (SELECT * FROM main.${quoteSqlIdent(ctx.since.table)} WHERE ts_ms >= ${Math.floor(ctx.since.ms)}) ${rewrittenSql}`;
    }

    const truncated = fetchedRows.length > maxRows;
    const slicedRows = truncated ? fetchedRows.slice(0, maxRows) : fetchedRows;
    const hideInternalScopedKey = !/\bscoped_key\b/i.test(req.sql);
    const cleanedRows = hideInternalScopedKey
      ? slicedRows.map((row) => {
          if (!("scoped_key" in row)) return row;
          const rest = { ...row };
          delete rest.scoped_key;
          return rest;
        })
      : slicedRows;
    const redactedRows = redactQueryRowsForPii(cleanedRows, maskIps);
    const columns =
      redactedRows.length > 0 ? Object.keys(redactedRows[0]) : [];
    const elapsedMs = Math.max(
      0,
      Math.round(
        ((typeof performance !== "undefined" ? performance.now() : Date.now()) -
          t0) *
          100,
      ) / 100,
    );

    const debugEntry = {
      engine: "DuckDB-Wasm",
      sql: finalRewrittenSql,
      duration_ms: elapsedMs,
    };

    return {
      status: 200,
      body: {
        columns,
        data: redactedRows,
        row_count: redactedRows.length,
        total_rows: truncated ? -1 : redactedRows.length,
        truncated,
        elapsed_ms: elapsedMs,
        execution_time_ms: elapsedMs,
        ...(explainPlan !== null ? { explain_plan: explainPlan } : {}),
        debug_queries: [debugEntry],
        _debug_queries: [debugEntry],
        debug_calls: [],
        section_timings: [{ name: "query", duration_ms: elapsedMs }],
      },
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: 400,
      body: {
        detail: {
          error: "query_failed",
          message,
        },
      },
    };
  }
}

export interface CloudQueryScanPlan {
  /** Parquet columns the query reads; null loads every column. */
  columns: string[] | null;
  /**
   * Set when the newest rows answer the query, so a recent window can load
   * first. `ordered` queries need the window's rows only (`since`); `limit`
   * is the explicit LIMIT, null when only `max_rows` bounds the result.
   */
  newestFirst: { table: string; ordered: boolean; limit: number | null } | null;
}

const QUERY_VIEW_BASE: Readonly<Record<string, string>> = {
  scoped_key: "VARCHAR", ts_ms: "BIGINT", timestamp: "TIMESTAMPTZ",
};
const QUERY_BASE_TABLES = new Set(["logs", "requests", "client_vitals", "client_errors"]);
// Words that can read columns not named in the SQL.
const ALL_COLUMN_WORDS = new Set([
  "columns", "pivot", "unpivot", "pivot_wider", "pivot_longer", "natural", "summarize", "describe", "show", "main",
]);
const NON_OPERAND_WORDS = new Set([
  "select", "distinct", "all", "on", "by", "from", "where", "group", "order", "having", "limit", "exclude",
  "replace", "rename", "union", "except", "intersect", "as", "and", "or", "not", "qualify", "window", "using",
  "join", "like", "ilike", "in", "is", "between", "case", "when", "then", "else", "with", "filter",
]);
const FROM_LIST_STOP = new Set([
  "where", "group", "order", "having", "limit", "offset", "qualify", "window", "join", "inner", "left", "right",
  "full", "outer", "cross", "natural", "on", "using", "union", "except", "intersect", "sample", "tablesample",
  "lateral", "positional", "asof", "anti", "semi", "pivot", "unpivot", "select", "as", "by", "from", "fetch",
]);
const NEWEST_FIRST_FORBIDDEN = new Set([
  "group", "having", "qualify", "window", "over", "distinct", "union", "intersect", "except", "with", "recursive",
  "join", "using", "sample", "tablesample", "offset", "pivot", "unpivot", "lateral", "filter", "natural",
  "positional", "asof", "values", "unnest", "columns", "fetch", "summarize", "describe", "show",
]);

function quoteSqlIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

const isName = (token?: SqlToken) => token?.kind === "word" || token?.kind === "quoted_ident";

function isOperandEnd(token?: SqlToken): boolean {
  if (!token) return false;
  if (token.kind === "string" || token.kind === "quoted_ident") return true;
  if (token.kind === "word") return !NON_OPERAND_WORDS.has(token.lower);
  return /^[0-9]/.test(token.value) || token.value === ")" || token.value === "]";
}

function isOperandStart(token?: SqlToken): boolean {
  if (!token) return false;
  if (token.kind === "string" || token.kind === "quoted_ident") return true;
  if (token.kind === "word") return !NON_OPERAND_WORDS.has(token.lower);
  return /^[0-9]/.test(token.value) || ["(", "-", "+"].includes(token.value);
}

function closingParen(tokens: readonly SqlToken[], open: number): number {
  let depth = 0;
  for (let i = open; i < tokens.length; i++) {
    if (tokens[i].value === "(" && tokens[i].kind === "symbol") depth++;
    if (tokens[i].value === ")" && tokens[i].kind === "symbol" && --depth === 0) return i;
  }
  return tokens.length;
}

/** A `*` that is not multiplication or `count(*)` expands to every column. */
function hasWildcard(tokens: readonly SqlToken[]): boolean {
  return tokens.some((token, i) => token.kind === "symbol" && token.value === "*" &&
    !(tokens[i - 1]?.value === "(" && tokens[i + 1]?.value === ")" && tokens[i - 2]?.lower === "count") &&
    !(isOperandEnd(tokens[i - 1]) && isOperandStart(tokens[i + 1])));
}

/**
 * Whether a relation name is used as a value (a whole-row struct, as in
 * `hash(r)`), which reads every column without naming any.
 */
function referencesWholeRow(tokens: readonly SqlToken[]): boolean {
  const relations = new Set(QUERY_BASE_TABLES);
  const allowed = new Set<number>();
  const relationAliases = new Set<number>();
  const defineAlias = (i: number) => {
    relations.add(tokens[i].lower);
    allowed.add(i);
    relationAliases.add(i);
    return tokens[i + 1]?.value === "(" ? closingParen(tokens, i + 1) + 1 : i + 1;
  };
  const cteBody = (i: number) => tokens[i]?.lower === "as" &&
    (tokens[i + 1]?.value === "(" || tokens[i + 1]?.lower === "materialized" || tokens[i + 1]?.lower === "not");
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (isName(token) && cteBody(i + 1)) {
      defineAlias(i);
    } else if (isName(token) && tokens[i + 1]?.value === "(" && tokens[i - 1] &&
        (tokens[i - 1].lower === "with" || tokens[i - 1].lower === "recursive" || tokens[i - 1].value === ",") &&
        cteBody(closingParen(tokens, i + 1) + 1)) {
      defineAlias(i);
    }
    if (token.kind !== "word" || (token.lower !== "from" && token.lower !== "join")) continue;
    let j = i + 1;
    while (j < tokens.length) {
      if (tokens[j].value === "(") {
        j = closingParen(tokens, j) + 1;
      } else if (isName(tokens[j]) && !FROM_LIST_STOP.has(tokens[j].lower)) {
        allowed.add(j++);
        while (tokens[j]?.value === "." && isName(tokens[j + 1])) j += 2;
        if (tokens[j]?.value === "(") j = closingParen(tokens, j) + 1;
      } else {
        break;
      }
      if (tokens[j]?.lower === "as" && isName(tokens[j + 1])) j = defineAlias(j + 1);
      else if (isName(tokens[j]) && !FROM_LIST_STOP.has(tokens[j].lower)) j = defineAlias(j);
      if (tokens[j]?.value !== ",") break;
      j++;
    }
  }
  const selectAliases = new Set(tokens.flatMap((token, i) =>
    isName(token) && tokens[i - 1]?.lower === "as" && !relationAliases.has(i) ? [token.lower] : []));
  return tokens.some((token, i) => isName(token) && relations.has(token.lower) && !allowed.has(i) &&
    tokens[i + 1]?.value !== "." && tokens[i - 1]?.value !== "." &&
    !(QUERY_BASE_TABLES.has(token.lower) && selectAliases.has(token.lower)));
}

/**
 * The shape of a query the newest rows answer: one plain SELECT over one
 * table, optionally `ORDER BY timestamp DESC`, with an optional final LIMIT.
 */
function newestFirstShape(
  tokens: readonly SqlToken[],
  targetTable: string,
): { table: string; ordered: boolean; limit: number | null } | null {
  if (tokens[0]?.lower !== "select") return null;
  if (tokens.some((token) => token.kind === "word" && (NEWEST_FIRST_FORBIDDEN.has(token.lower) ||
      token.lower === "select" && token !== tokens[0]) || token.kind === "symbol" && token.value === "#")) return null;
  const depth: number[] = [];
  let level = 0;
  for (const token of tokens) {
    if (token.kind === "symbol" && token.value === ")") level--;
    depth.push(level);
    if (token.kind === "symbol" && token.value === "(") level++;
  }
  const top = (i: number, word: string) => tokens[i]?.kind === "word" && tokens[i].lower === word && depth[i] === 0;
  const froms = tokens.flatMap((_, i) => top(i, "from") ? [i] : []);
  if (froms.length !== 1) return null;
  const from = froms[0];
  const source = tokens[from + 1];
  if (!isName(source) || !QUERY_BASE_TABLES.has(source.lower) ||
      tokens[from + 2]?.value === "(" || tokens[from + 2]?.value === ".") return null;
  const qualifiers = new Set([source.lower]);
  let i = from + 2;
  if (tokens[i]?.lower === "as") i++;
  if (isName(tokens[i]) && !["where", "order", "limit"].includes(tokens[i].lower)) qualifiers.add(tokens[i++].lower);
  if (i < tokens.length && !top(i, "where") && !top(i, "order") && !top(i, "limit")) return null;
  if (tokens.some((token, k) => k > from && depth[k] === 0 && token.kind === "symbol" && token.value === ",")) return null;
  const limitAt = tokens.findIndex((_, k) => k > from && top(k, "limit"));
  let end = tokens.length;
  let limit: number | null = null;
  if (limitAt >= 0) {
    if (limitAt + 2 !== tokens.length || !/^\d+$/.test(tokens[limitAt + 1].value)) return null;
    limit = Number(tokens[limitAt + 1].value);
    end = limitAt;
  }
  const orderAt = tokens.findIndex((_, k) => k > from && top(k, "order"));
  if (orderAt >= 0) {
    let k = orderAt + 1;
    if (tokens[k++]?.lower !== "by") return null;
    if (isName(tokens[k]) && tokens[k + 1]?.value === ".") {
      if (!qualifiers.has(tokens[k].lower)) return null;
      k += 2;
    }
    if (!isName(tokens[k]) || tokens[k++].lower !== "timestamp" || tokens[k++]?.lower !== "desc") return null;
    if (tokens[k]?.lower === "nulls" && tokens[k + 1]?.lower === "last") k += 2;
    if (k !== end) return null;
    // `ORDER BY timestamp` must name the table's column, not a select alias.
    for (let s = 1; s < from; s++) {
      if (isName(tokens[s]) && tokens[s].lower === "timestamp" &&
          (tokens[s - 1].lower === "as" || isOperandEnd(tokens[s - 1]))) return null;
    }
  }
  const table = source.lower === "logs" || source.lower === "requests" ? targetTable : source.lower;
  return { table, ordered: orderAt >= 0, limit };
}

function shadowTables(schemas: Readonly<Record<string, Readonly<Record<string, string>>>>): string {
  return `WITH ${Object.entries(schemas).map(([table, columns]) => `${quoteSqlIdent(table)} AS (SELECT ${
    Object.entries(columns).map(([name, type]) => `NULL::${type} AS ${quoteSqlIdent(name)}`).join(", ")
  } WHERE false)`).join(", ")} `;
}

/**
 * Plans what `/api/query` must load: the columns the SQL reads (null when
 * they can't be proven, e.g. `SELECT *`) and whether the newest rows answer
 * it. DuckDB binds the query against empty tables of the catalog schema to
 * confirm the token-derived columns are all it reads.
 */
export async function planCloudQueryScan(
  req: QueryRequest,
  ctx: {
    tableName?: string;
    allowedTables?: Iterable<string>;
    customColumns?: Readonly<Record<string, DuckDBColumnType>>;
    runSql: (sql: string) => Promise<Record<string, unknown>[]>;
    signal?: AbortSignal;
  },
): Promise<CloudQueryScanPlan> {
  const targetTable = ctx.tableName ?? "requests";
  const rawSql = typeof req?.sql === "string" ? req.sql.trim() : "";
  try {
    if (!rawSql) throw new SQLValidationError("empty_sql", "No SQL provided");
    validateUserSql(rawSql, ctx.allowedTables ?? ["logs", "requests"]);
  } catch {
    // The query is refused before it reads anything; the smallest load lets it say so.
    return { columns: ["timestamp"], newestFirst: { table: targetTable, ordered: false, limit: null } };
  }
  const maxRows = Math.max(1, Math.min(Number(req.max_rows ?? 1000), MAX_QUERY_ROWS));
  const { rewrittenSql, hasExplicitLimit } = rewriteCloudQuerySql(rawSql, targetTable, maxRows);
  let tokens: SqlToken[];
  try {
    tokens = tokenizeSql(rewrittenSql);
  } catch {
    return { columns: null, newestFirst: null };
  }
  const requests: Record<string, string> = {
    ...QUERY_VIEW_BASE,
    ...FIELD_DUCKDB_TYPES,
    ...Object.fromEntries(Object.entries(ctx.customColumns ?? {}).filter(([name]) => !(name in FIELD_DUCKDB_TYPES))),
  };
  const schemas = {
    requests,
    client_vitals: { ...QUERY_VIEW_BASE, ...RUM_VITALS_COLUMNS },
    client_errors: { ...QUERY_VIEW_BASE, ...RUM_ERRORS_COLUMNS },
  };
  const wrapped = `(\n${rewrittenSql}\n) AS _fla_q`;
  const attempt = async <T>(run: () => Promise<T>): Promise<T | null> => {
    try {
      return await run();
    } catch (error) {
      if (ctx.signal?.aborted) throw error;
      return null;
    }
  };

  let columns: string[] | null = null;
  if (targetTable === "requests" && (tokens[0]?.lower === "select" || tokens[0]?.lower === "with") &&
      !tokens.some((token) => token.kind === "word" && ALL_COLUMN_WORDS.has(token.lower) ||
        token.kind === "symbol" && token.value === "#") &&
      !hasWildcard(tokens) && !referencesWholeRow(tokens)) {
    const names = new Set(tokens.filter(isName).map((token) => token.lower));
    const candidates = Object.keys(requests).filter((name) => !(name in QUERY_VIEW_BASE) && names.has(name.toLowerCase()));
    const reduced = { ...schemas, requests: {
      ...QUERY_VIEW_BASE, ...Object.fromEntries(candidates.map((name) => [name, requests[name]])),
    } };
    const describe = async (tables: typeof schemas) => JSON.stringify(
      (await ctx.runSql(`DESCRIBE ${shadowTables(tables)}SELECT * FROM ${wrapped}`))
        .map((row) => [row.column_name, row.column_type]),
    );
    const bound = await attempt(async () => await describe(schemas) === await describe(reduced));
    if (bound) columns = ["timestamp", ...candidates];
  }

  // The appended default LIMIT is `max_rows`, which truncation already bounds.
  const shape = newestFirstShape(hasExplicitLimit ? tokens : tokens.slice(0, -2), targetTable);
  let newestFirst: CloudQueryScanPlan["newestFirst"] = null;
  if (shape) {
    // Aggregates answer a row even from no input; a plain SELECT does not.
    const rows = await attempt(() => ctx.runSql(`${shadowTables(schemas)}SELECT count(*) AS n FROM ${wrapped}`));
    if (rows && Number(rows[0]?.n) === 0) newestFirst = shape;
  }
  return { columns, newestFirst };
}
