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
  validateSqlQuery,
  validateUserSql,
  validate_user_sql,
} from "./sql-validator";

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

export interface QueryResponse {
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  total_rows: number;
  truncated: boolean;
  elapsed_ms: number;
  execution_time_ms: number;
  explain_plan?: string | null;
  debug_queries: { engine: string; sql: string; duration_ms: number }[];
  _debug_queries: { engine: string; sql: string; duration_ms: number }[];
  debug_calls: unknown[];
  section_timings: { name: string; duration_ms: number }[];
}

export interface CloudQueryContext {
  serviceId?: string;
  tableName?: string;
  maskIps?: boolean;
  role?: "admin" | "analyst" | "analyst-no-pii";
  allowedTables?: Iterable<string>;
  schemaColumns?: QuerySchemaColumn[];
  runSql?: (sql: string) => Promise<Record<string, unknown>[]>;
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

const IP_PII_COLUMNS = new Set(["ip", "client_ip", "oip", "xff"]);
const SESSION_PII_COLUMNS = new Set([
  "edge_sid",
  "cmcd_sid",
  "cookie_session",
  "cid",
  "cmcd_cid",
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
    /\b(FROM|JOIN|SUMMARIZE|DESCRIBE)\s+(?:"logs"|logs|"requests"|requests)\b/gi,
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
  const { rewrittenSql, hasExplicitLimit } = rewriteCloudQuerySql(
    rawSql,
    targetTable,
    maxRows,
  );

  const execSql =
    isSimpleSelectStatement(rewrittenSql) && hasExplicitLimit
      ? `SELECT * FROM (${rewrittenSql}) AS _q LIMIT ${maxRows + 1}`
      : isSimpleSelectStatement(rewrittenSql)
        ? `SELECT * FROM (${ stripSqlComments(rawSql).trim().replace(/;\s*$/, "").replace(/\b(FROM|JOIN)\s+(?:"logs"|logs)\b/gi, `$1 ${targetTable}`) }) AS _q LIMIT ${maxRows + 1}`
        : rewrittenSql;

  const maskIps = Boolean(ctx.maskIps || ctx.role === "analyst-no-pii");

  try {
    let fetchedRows: Record<string, unknown>[] = [];
    let explainPlan: string | null = null;

    if (ctx.runSql) {
      if (req.explain) {
        const planRows = await ctx.runSql(`EXPLAIN ${rewrittenSql}`);
        explainPlan = planRows
          .map((r) => Object.values(r).join(" "))
          .join("\n");
      }
      fetchedRows = await ctx.runSql(execSql);
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
      sql: rewrittenSql,
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
