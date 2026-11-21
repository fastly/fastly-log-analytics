/**
 * Cloud Engine handlers for Page 15 SQL Query Console (`/query`):
 * - `POST /api/query`
 * - `GET /api/query/schema` (`/api/query/schema/{service_id}`)
 * - `GET /api/presets`
 */

import {
  MAX_QUERY_ROWS,
  SQLValidationError,
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

export function getQuerySchema(
  serviceId: string,
  maskIps = false,
  customColumns?: readonly QuerySchemaColumn[],
): QuerySchemaResponse {
  const base = customColumns && customColumns.length > 0
    ? [...customColumns]
    : [...DEFAULT_SCHEMA_COLUMNS];
  const schema = maskIps
    ? base.filter((col) => col.column !== "cookie_session")
    : base;
  return {
    source: serviceId || "logs",
    table_name: "logs",
    schema,
    debug_queries: [],
    debug_calls: [],
  };
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
): { rewrittenSql: string; hasExplicitLimit: boolean } {
  const cleaned = stripSqlComments(sql).trim().replace(/;\s*$/, "");
  const tableRewritten = cleaned.replace(
    /\b(FROM|JOIN|SUMMARIZE|DESCRIBE)\s+(?:(?:"main"|main)\s*\.\s*)?(?:"logs"|logs|"requests"|requests)\b/gi,
    (_m, kw: string) => `${kw} ${targetTable}`,
  );
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
      }
    }
    return copy;
  });
}

function mergeLeadingCtes(
  sqlStmt: string,
  cteDefs: string[],
  shadowSelectSql?: string,
  targetTable?: string,
): string {
  if (cteDefs.length === 0) return sqlStmt;
  const trimmed = sqlStmt.trim();
  if (shadowSelectSql && targetTable) {
    const sumMatch = /^(SUMMARIZE|DESCRIBE)\s+([A-Za-z0-9_"]+)\s*$/i.exec(trimmed);
    if (sumMatch) {
      const refName = sumMatch[2].replaceAll('"', "").toLowerCase();
      if (refName === targetTable.toLowerCase() || refName === "logs" || refName === "requests") {
        return `${sumMatch[1]} (${shadowSelectSql})`;
      }
    }
  }
  if (/^WITH\s+RECURSIVE\b/i.test(trimmed)) {
    return `WITH RECURSIVE ${cteDefs.join(", ")}, ${trimmed.replace(/^WITH\s+RECURSIVE\s+/i, "")}`;
  }
  if (/^WITH\b/i.test(trimmed)) {
    return `WITH ${cteDefs.join(", ")}, ${trimmed.replace(/^WITH\s+/i, "")}`;
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
  const { rewrittenSql: baseRewrittenSql, hasExplicitLimit } = rewriteCloudQuerySql(
    rawSql,
    targetTable,
    maxRows,
  );
  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");
  const rewrittenSql = maskIps
    ? baseRewrittenSql.replace(
        /(,\s*)(?:(?:"main"|main)\s*\.\s*)?(?:"logs"|logs)\b/gi,
        `$1${targetTable}`,
      )
    : baseRewrittenSql;

  const baseInnerSql = stripSqlComments(rawSql)
    .trim()
    .replace(/;\s*$/, "")
    .replace(
      /\b(FROM|JOIN)\s+(?:(?:"main"|main)\s*\.\s*)?(?:"logs"|logs|"requests"|requests)\b/gi,
      `$1 ${targetTable}`,
    );
  const innerSql = maskIps
    ? baseInnerSql.replace(
        /(,\s*)(?:(?:"main"|main)\s*\.\s*)?(?:"logs"|logs)\b/gi,
        `$1${targetTable}`,
      )
    : baseInnerSql;

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
      let shadowSelectSql: string | undefined;

      if (maskIps) {
        const describeTarget =
          targetTable === "requests" ? "requests" : `main.${quoteSqlIdent(targetTable)}`;
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
          for (const col of validDesc) {
            const colName = String(col.column_name);
            const colLower = colName.toLowerCase();
            const colType = String(col.column_type ?? "VARCHAR").toUpperCase();
            const colIdent = quoteSqlIdent(colName);
            const isTextType = /CHAR|TEXT|STRING/i.test(colType);
            if (IP_PII_COLUMNS.has(colLower)) {
              replacements.push(
                isTextType
                  ? `'0.0.0.0' AS ${colIdent}`
                  : `CAST(NULL AS ${colType}) AS ${colIdent}`,
              );
            } else if (SESSION_PII_COLUMNS.has(colLower)) {
              replacements.push(
                isTextType
                  ? `'[redacted]' AS ${colIdent}`
                  : `CAST(NULL AS ${colType}) AS ${colIdent}`,
              );
            }
          }
          const replaceExpr =
            replacements.length > 0 ? ` * REPLACE (${replacements.join(", ")})` : " *";
          const sinceClause =
            ctx.since && ctx.since.table === targetTable
              ? ` WHERE ts_ms >= ${Math.floor(ctx.since.ms)}`
              : "";
          shadowSelectSql = `SELECT${replaceExpr} FROM main.${quoteSqlIdent(targetTable)}${sinceClause}`;
          cteDefs.push(`${quoteSqlIdent(targetTable)} AS (${shadowSelectSql})`);
        }
      }

      if (ctx.since && !(maskIps && shadowSelectSql && ctx.since.table === targetTable)) {
        cteDefs.push(
          `${quoteSqlIdent(ctx.since.table)} AS (SELECT * FROM main.${quoteSqlIdent(ctx.since.table)} WHERE ts_ms >= ${Math.floor(ctx.since.ms)})`,
        );
      }

      const effectiveExplainTarget = mergeLeadingCtes(
        rewrittenSql,
        cteDefs,
        shadowSelectSql,
        targetTable,
      );
      const effectiveExecSql = mergeLeadingCtes(
        execSql,
        cteDefs,
        shadowSelectSql,
        targetTable,
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
    const redactedRows = redactQueryRowsForPii(slicedRows, maskIps);
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
