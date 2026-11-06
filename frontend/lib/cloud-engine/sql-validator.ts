/**
 * Cloud Engine in-browser SQL validator for DuckDB-Wasm.
 *
 * Mirrors `backend/core/sql_validator.py`:
 * - Enforces exact 64 KiB (`MAX_INPUT_BYTES = 65_536`) UTF-8 input byte cap
 * - Rejects NUL bytes, empty queries, and multi-statement queries
 * - Positive statement allowlist (`SELECT`, `WITH ... SELECT`, `VALUES`, `SUMMARIZE`, `DESCRIBE`, `SHOW TABLES`, `SHOW COLUMNS`)
 * - Blocks DDL/DML/administrative keywords (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`, `COPY`, `ATTACH`, `DETACH`, `PRAGMA`, `SET`, `INSTALL`, `LOAD`, etc.)
 * - Blocks file/network/metadata table & scalar functions (`read_parquet`, `parquet_scan`, `read_csv`, `read_json`, `glob`, `getenv`, `current_setting`, `httpfs`, `duckdb_*`, `pragma_*`, `sqlite_*`, `postgres_*`, `iceberg_*`, `delta_*`)
 * - Blocks system schemas (`information_schema`, `pg_catalog`, `duckdb`, `system`, `temp`) and DuckDB `FROM 'path.parquet'` string-literal table shorthands
 */

export const MAX_INPUT_BYTES = 65_536;
export const MAX_QUERY_LENGTH = 10_000;
export const MAX_QUERY_ROWS = 10_000;

export const SAFE_IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function escapeSqlLiteral(value: string): string {
  if (typeof value !== "string") {
    throw new TypeError(`escapeSqlLiteral expected string, got ${typeof value}`);
  }
  return value.replace(/'/g, "''");
}

export function isSafeIdent(name: unknown): name is string {
  return typeof name === "string" && SAFE_IDENT_RE.test(name);
}

export function requireSafeIdent(name: string, label: string): string {
  if (!isSafeIdent(name)) {
    throw new Error(`${label}: ${JSON.stringify(name)}`);
  }
  return name;
}

export const BLOCKED_FUNCTIONS = [
  "read_parquet",
  "read_parquet_metadata",
  "read_parquet_schema",
  "parquet_scan",
  "parquet_metadata",
  "parquet_schema",
  "parquet_kv_metadata",
  "parquet_file_metadata",
  "parquet_full_metadata",
  "parquet_bloom_probe",
  "read_csv",
  "read_csv_auto",
  "read_json",
  "read_json_auto",
  "read_json_objects",
  "read_json_objects_auto",
  "read_ndjson",
  "read_ndjson_auto",
  "read_ndjson_objects",
  "read_text",
  "read_text_auto",
  "read_blob",
  "read_blob_auto",
  "read_avro",
  "glob",
  "getenv",
  "current_setting",
  "httpfs",
  "lsdir",
  "iceberg_scan",
  "iceberg_metadata",
  "iceberg_snapshots",
  "sqlite_scan",
  "sqlite_attach",
  "postgres_scan",
  "postgres_attach",
  "postgres_query",
  "mysql_scan",
  "mysql_attach",
  "mysql_query",
  "query",
  "query_table",
  "sniff_csv",
  "which_secret",
] as const;

export const BLOCKED_FUNCTION_PREFIXES = [
  "duckdb_",
  "pragma_",
  "sqlite_",
  "postgres_",
  "iceberg_",
  "delta_",
  "read_",
  "parquet_",
  "sniff_",
] as const;

export const SAFE_FUNCTIONS = new Set([
  "generate_series",
  "range",
  "repeat",
  "repeat_row",
  "unnest",
  "json_each",
  "json_tree",
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "coalesce",
  "nullif",
  "cast",
  "try_cast",
  "date_trunc",
  "epoch",
  "regexp_matches",
  "starts_with",
  "lower",
  "upper",
  "round",
  "percentile_cont",
  "median",
  "mode",
  "stddev_samp",
  "var_samp",
  "greatest",
  "least",
  "abs",
  "sqrt",
  "replace",
  "substr",
  "substring",
  "length",
  "trim",
]);

export const ALLOWED_TABLE_FUNCTIONS = new Set([
  "generate_series",
  "range",
  "repeat",
  "repeat_row",
  "unnest",
  "json_each",
  "json_tree",
]);

export const BLOCKED_SCHEMAS = new Set([
  "information_schema",
  "pg_catalog",
  "duckdb",
  "temp",
  "system",
]);

export const BLOCKED_STATEMENT_KEYWORDS = new Set([
  "drop",
  "delete",
  "insert",
  "update",
  "alter",
  "create",
  "replace",
  "truncate",
  "copy",
  "attach",
  "detach",
  "pragma",
  "set",
  "reset",
  "call",
  "export",
  "import",
  "load",
  "install",
  "uninstall",
  "vacuum",
  "checkpoint",
  "transaction",
  "begin",
  "commit",
  "rollback",
  "grant",
  "revoke",
  "prepare",
  "execute",
  "deallocate",
]);

export const FORBIDDEN_KEYWORDS = BLOCKED_STATEMENT_KEYWORDS;

export const FORBIDDEN_PATTERNS: readonly RegExp[] = [
  /\bread_parquet\s*\(/i,
  /\bparquet_scan\s*\(/i,
  /\bread_csv(?:_auto)?\s*\(/i,
  /\bread_json(?:_auto|_objects|_objects_auto)?\s*\(/i,
  /\bread_ndjson(?:_auto|_objects)?\s*\(/i,
  /\bread_text(?:_auto)?\s*\(/i,
  /\bread_blob(?:_auto)?\s*\(/i,
  /\bglob\s*\(/i,
  /\bgetenv\s*\(/i,
  /\bcurrent_setting\s*\(/i,
  /\bhttpfs\b/i,
  /\binformation_schema\s*\./i,
  /\bpg_catalog\s*\./i,
];

const BLOCKED_FUNCTION_SET = new Set<string>(BLOCKED_FUNCTIONS);

export class SQLValidationError extends Error {
  readonly code = "sql_not_permitted";
  readonly reason: string;

  constructor(reasonOrMessage: string, message?: string) {
    const finalMessage = message ?? reasonOrMessage;
    const finalReason = message ? reasonOrMessage : "sql_not_permitted";
    super(finalMessage);
    this.name = "SQLValidationError";
    this.reason = finalReason;
  }
}

export interface ValidationResult {
  valid: true;
  normalizedSql: string;
  elapsedMs: number;
}

/**
 * Strip `--` line comments and `/* ... * /` block comments while preserving
 * single-quoted string literals and double-quoted identifiers.
 */
export function stripSqlComments(sql: string): string {
  let out = "";
  let i = 0;
  const n = sql.length;

  while (i < n) {
    const ch = sql[i];
    const next = i + 1 < n ? sql[i + 1] : "";

    // Single-quoted literal
    if (ch === "'") {
      out += ch;
      i++;
      while (i < n) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          out += "''";
          i += 2;
          continue;
        }
        if (sql[i] === "'") {
          out += "'";
          i++;
          break;
        }
        out += sql[i];
        i++;
      }
      continue;
    }

    // Double-quoted identifier
    if (ch === '"') {
      out += ch;
      i++;
      while (i < n) {
        if (sql[i] === '"' && sql[i + 1] === '"') {
          out += '""';
          i += 2;
          continue;
        }
        if (sql[i] === '"') {
          out += '"';
          i++;
          break;
        }
        out += sql[i];
        i++;
      }
      continue;
    }

    // Line comment
    if (ch === "-" && next === "-") {
      i += 2;
      while (i < n && sql[i] !== "\n") {
        i++;
      }
      out += " ";
      continue;
    }

    // Block comment (supports nested /* ... */ in DuckDB)
    if (ch === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (sql[i] === "/" && sql[i + 1] === "*") {
          depth++;
          i += 2;
        } else if (sql[i] === "*" && sql[i + 1] === "/") {
          depth--;
          i += 2;
        } else {
          i++;
        }
      }
      if (depth > 0) {
        throw new SQLValidationError("parse_error", "Unterminated SQL block comment.");
      }
      out += " ";
      continue;
    }

    out += ch;
    i++;
  }

  return out;
}

interface SqlToken {
  kind: "word" | "string" | "quoted_ident" | "symbol";
  value: string;
  lower: string;
}

function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  const n = sql.length;

  while (i < n) {
    const ch = sql[i];
    const next = i + 1 < n ? sql[i + 1] : "";

    if (/\s/.test(ch)) {
      i++;
      continue;
    }

    // Line comment
    if (ch === "-" && next === "-") {
      i += 2;
      while (i < n && sql[i] !== "\n") {
        i++;
      }
      continue;
    }

    // Block comment
    if (ch === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (sql[i] === "/" && sql[i + 1] === "*") {
          depth++;
          i += 2;
        } else if (sql[i] === "*" && sql[i + 1] === "/") {
          depth--;
          i += 2;
        } else {
          i++;
        }
      }
      if (depth > 0) {
        throw new SQLValidationError("parse_error", "Unterminated SQL block comment.");
      }
      continue;
    }

    // Single-quoted literal (or dollar-quoted)
    if (ch === "'") {
      let lit = "";
      i++;
      let closed = false;
      while (i < n) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          lit += "'";
          i += 2;
          continue;
        }
        if (sql[i] === "'") {
          i++;
          closed = true;
          break;
        }
        lit += sql[i];
        i++;
      }
      if (!closed) {
        throw new SQLValidationError("parse_error", "Unterminated SQL string literal.");
      }
      tokens.push({ kind: "string", value: lit, lower: lit.toLowerCase() });
      continue;
    }

    // Dollar-quoted literal ($tag$...$tag$) — reject or parse
    if (ch === "$") {
      const m = sql.slice(i).match(/^\$([A-Za-z0-9_]*)\$/);
      if (m) {
        const tag = m[0];
        const endIdx = sql.indexOf(tag, i + tag.length);
        if (endIdx === -1) {
          throw new SQLValidationError("parse_error", "Unterminated dollar-quoted string literal.");
        }
        const lit = sql.slice(i + tag.length, endIdx);
        tokens.push({ kind: "string", value: lit, lower: lit.toLowerCase() });
        i = endIdx + tag.length;
        continue;
      }
    }

    // Double-quoted identifier
    if (ch === '"') {
      let ident = "";
      i++;
      let closed = false;
      while (i < n) {
        if (sql[i] === '"' && sql[i + 1] === '"') {
          ident += '"';
          i += 2;
          continue;
        }
        if (sql[i] === '"') {
          i++;
          closed = true;
          break;
        }
        ident += sql[i];
        i++;
      }
      if (!closed) {
        throw new SQLValidationError("parse_error", "Unterminated double-quoted identifier.");
      }
      tokens.push({ kind: "quoted_ident", value: ident, lower: ident.toLowerCase() });
      continue;
    }

    // Word / identifier
    if (/[A-Za-z_]/.test(ch)) {
      let word = ch;
      i++;
      while (i < n && /[A-Za-z0-9_$]/.test(sql[i])) {
        word += sql[i];
        i++;
      }
      tokens.push({ kind: "word", value: word, lower: word.toLowerCase() });
      continue;
    }

    // Number literal
    if (/[0-9]/.test(ch)) {
      let num = ch;
      i++;
      while (i < n && /[0-9._eE+-]/.test(sql[i])) {
        num += sql[i];
        i++;
      }
      tokens.push({ kind: "symbol", value: num, lower: num.toLowerCase() });
      continue;
    }

    // Single punctuation symbol
    tokens.push({ kind: "symbol", value: ch, lower: ch });
    i++;
  }

  return tokens;
}

const ALLOWED_START_KEYWORDS = new Set([
  "select",
  "with",
  "values",
  "summarize",
  "describe",
  "show",
]);

export function validateUserSql(
  sql: string,
  allowedTables?: Iterable<string>,
): ValidationResult {
  const t0 = typeof performance !== "undefined" ? performance.now() : Date.now();

  if (typeof sql !== "string") {
    throw new SQLValidationError("input_type", "SQL must be a string.");
  }
  if (sql.includes("\0")) {
    throw new SQLValidationError("nul_byte_injection", "Query contains a NUL byte.");
  }

  const byteLen = new TextEncoder().encode(sql).byteLength;
  if (byteLen > MAX_INPUT_BYTES) {
    throw new SQLValidationError(
      "input_too_large",
      `Query exceeds maximum length of ${MAX_INPUT_BYTES} bytes (${byteLen} bytes).`,
    );
  }

  const tokens = tokenizeSql(sql);
  if (tokens.length === 0) {
    throw new SQLValidationError("empty_sql", "SQL query must not be empty.");
  }

  // Strip a single optional trailing semicolon; reject any semicolon before the end.
  let effectiveLen = tokens.length;
  if (tokens[effectiveLen - 1].kind === "symbol" && tokens[effectiveLen - 1].value === ";") {
    effectiveLen--;
  }
  if (effectiveLen === 0) {
    throw new SQLValidationError("empty_sql", "SQL query must not be empty.");
  }
  for (let i = 0; i < effectiveLen; i++) {
    if (tokens[i].kind === "symbol" && tokens[i].value === ";") {
      throw new SQLValidationError(
        "multi_statement",
        "Only a single SQL statement is permitted.",
      );
    }
  }

  const first = tokens[0];
  if (first.kind !== "word" || !ALLOWED_START_KEYWORDS.has(first.lower)) {
    throw new SQLValidationError(
      "statement_not_allowed",
      `Statement type '${first.value}' is not permitted; only SELECT queries are allowed.`,
    );
  }

  if (first.lower === "show") {
    const second = effectiveLen > 1 ? tokens[1].lower : "";
    if (second !== "tables" && second !== "columns") {
      throw new SQLValidationError(
        "statement_not_allowed",
        "Only SHOW TABLES and SHOW COLUMNS are permitted.",
      );
    }
  }

  if (first.lower === "with") {
    const hasSelect = tokens
      .slice(1, effectiveLen)
      .some((t) => t.kind === "word" && t.lower === "select");
    if (!hasSelect) {
      throw new SQLValidationError(
        "statement_not_allowed",
        "WITH CTE must be followed by a SELECT query.",
      );
    }
  }

  // Collect CTE aliases defined via `WITH name AS (` or `, name AS (`
  const cteAliases = new Set<string>();
  for (let i = 0; i < effectiveLen - 2; i++) {
    const prev = i > 0 ? tokens[i - 1] : null;
    const curr = tokens[i];
    const next1 = tokens[i + 1];
    const next2 = tokens[i + 2];
    if (
      prev &&
      ((prev.kind === "word" && (prev.lower === "with" || prev.lower === "recursive")) ||
        (prev.kind === "symbol" && prev.value === ",")) &&
      (curr.kind === "word" || curr.kind === "quoted_ident") &&
      next1.kind === "word" &&
      next1.lower === "as" &&
      next2.kind === "symbol" &&
      next2.value === "("
    ) {
      cteAliases.add(curr.lower);
    }
  }

  const allowedTableSet = allowedTables
    ? new Set(Array.from(allowedTables, (t) => t.toLowerCase()))
    : null;

  for (let i = 0; i < effectiveLen; i++) {
    const tok = tokens[i];
    const nextTok = i + 1 < effectiveLen ? tokens[i + 1] : null;
    const prevTok = i > 0 ? tokens[i - 1] : null;

    if (tok.kind === "word" || tok.kind === "quoted_ident") {
      const lower = tok.lower;

      // Check blocked statement keywords (allow scalar `replace(...)` and `truncate(...)` function calls,
      // and `grouping sets` / `set time zone` checks)
      if (tok.kind === "word" && BLOCKED_STATEMENT_KEYWORDS.has(lower)) {
        const isScalarCall =
          (lower === "replace" || lower === "truncate") &&
          nextTok?.kind === "symbol" &&
          nextTok.value === "(" &&
          prevTok?.lower !== "or";
        if (!isScalarCall) {
          throw new SQLValidationError(
            `keyword_blocked:${lower}`,
            `Keyword '${tok.value}' is not allowed in user queries.`,
          );
        }
      }

      // Check function call: `ident (`
      if (nextTok && nextTok.kind === "symbol" && nextTok.value === "(") {
        if (BLOCKED_FUNCTION_SET.has(lower)) {
          throw new SQLValidationError(
            `function_denylist:${lower}`,
            `Function '${tok.value}' is not allowed in user queries.`,
          );
        }
        for (const prefix of BLOCKED_FUNCTION_PREFIXES) {
          if (lower.startsWith(prefix)) {
            throw new SQLValidationError(
              `function_prefix:${prefix}`,
              `Function '${tok.value}' uses blocked prefix '${prefix}'.`,
            );
          }
        }
      }

      // Check schema qualification: `schema . table`
      if (nextTok && nextTok.kind === "symbol" && nextTok.value === ".") {
        if (BLOCKED_SCHEMAS.has(lower)) {
          throw new SQLValidationError(
            `schema_blocklist:${lower}`,
            `Schema '${tok.value}' is not allowed in user queries.`,
          );
        }
      }

      // Check table references after FROM or JOIN
      if (
        prevTok &&
        prevTok.kind === "word" &&
        (prevTok.lower === "from" || prevTok.lower === "join")
      ) {
        if (tok.value.includes("/") || tok.value.includes("\\") || tok.value.includes(".")) {
          throw new SQLValidationError(
            "catalog_blocklist:table_name_path",
            `Table name '${tok.value}' contains path-like characters.`,
          );
        }
        if (lower.startsWith("pg_")) {
          throw new SQLValidationError(
            "table_prefix:pg_",
            `System table '${tok.value}' is not allowed in user queries.`,
          );
        }
        const isFunctionTable = nextTok && nextTok.kind === "symbol" && nextTok.value === "(";
        if (isFunctionTable && !ALLOWED_TABLE_FUNCTIONS.has(lower)) {
          throw new SQLValidationError(
            `function_table_not_allowed:${lower}`,
            `Table function '${tok.value}' is not allowed in user queries.`,
          );
        }
        if (
          !isFunctionTable &&
          nextTok?.value !== "." &&
          allowedTableSet &&
          !allowedTableSet.has(lower) &&
          !cteAliases.has(lower)
        ) {
          throw new SQLValidationError(
            "table_not_allowed",
            `Table '${tok.value}' is not in the allowed table list.`,
          );
        }
      }
    }

    // Block DuckDB string-literal file scan shorthand: `FROM 'secret.parquet'` or `JOIN '/etc/passwd'`
    if (
      tok.kind === "string" &&
      prevTok &&
      prevTok.kind === "word" &&
      (prevTok.lower === "from" || prevTok.lower === "join")
    ) {
      throw new SQLValidationError(
        "table_path_literal",
        "String literal file paths in FROM/JOIN are not allowed in user queries.",
      );
    }
  }

  const elapsedMs = (typeof performance !== "undefined" ? performance.now() : Date.now()) - t0;
  return {
    valid: true,
    normalizedSql: stripSqlComments(sql).trim().replace(/;\s*$/, ""),
    elapsedMs,
  };
}

export const validate_user_sql = validateUserSql;

export function validateSqlQuery(
  sql: string,
  options?: { maxBytes?: number; allowedTables?: Iterable<string> },
): { valid: boolean; error?: string; reason?: string; code?: string } {
  try {
    if (options?.maxBytes !== undefined) {
      const byteLen = new TextEncoder().encode(sql).byteLength;
      if (byteLen > options.maxBytes) {
        throw new SQLValidationError(
          "input_too_large",
          `Query exceeds maximum length of ${options.maxBytes} bytes (${byteLen} bytes).`,
        );
      }
    }
    validateUserSql(sql, options?.allowedTables);
    return { valid: true };
  } catch (err) {
    if (err instanceof SQLValidationError) {
      return {
        valid: false,
        error: err.message,
        reason: err.reason,
        code: err.code,
      };
    }
    return {
      valid: false,
      error: err instanceof Error ? err.message : String(err),
      reason: "validation_error",
      code: "sql_not_permitted",
    };
  }
}

export function isSimpleSelectStatement(sql: string): boolean {
  try {
    const tokens = tokenizeSql(sql);
    if (tokens.length === 0) return false;
    const first = tokens[0].lower;
    return first === "select" || first === "with";
  } catch {
    return false;
  }
}
