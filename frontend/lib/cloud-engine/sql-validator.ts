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
  "arrow_scan",
  "arrow_scan_dumb",
  "seq_scan",
  "enable_logging",
  "disable_logging",
  "enable_profiling",
  "disable_profiling",
  "truncate_duckdb_logs",
  "force_checkpoint",
  "summary",
  "test_all_types",
  "test_vector_types",
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
  "memory",
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
  "pivot",
  "unpivot",
  "pivot_wider",
  "pivot_longer",
  "recursive",
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

const DUCKDB_UNICODE_SPACE_RE = /[\u00a0\u2000-\u200b\u202f\u205f\u2060\u3000\ufeff]/;
const DISALLOWED_SQL_CTRL_OR_SEP_RE =
  /[\x00-\x08\x0b\x0e-\x1f\x7f-\x9f\u1680\u180e\u2028\u2029]/;
const IDENT_START_RE = /[A-Za-z_\u0080-\uffff]/;
const IDENT_CONT_RE = /[A-Za-z0-9_$\u0080-\uffff]/;
const DOLQ_TAG_RE = /^\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/;

function isSqlHorizSpace(ch: string): boolean {
  return ch === " " || ch === "\t" || ch === "\f" || DUCKDB_UNICODE_SPACE_RE.test(ch);
}

function isSqlWhitespace(ch: string): boolean {
  return ch === "\n" || ch === "\r" || isSqlHorizSpace(ch);
}

function isIdentStartChar(ch: string): boolean {
  return (
    ch.length > 0 &&
    !isSqlWhitespace(ch) &&
    !DISALLOWED_SQL_CTRL_OR_SEP_RE.test(ch) &&
    IDENT_START_RE.test(ch)
  );
}

function isIdentContChar(ch: string): boolean {
  return (
    ch.length > 0 &&
    !isSqlWhitespace(ch) &&
    !DISALLOWED_SQL_CTRL_OR_SEP_RE.test(ch) &&
    IDENT_CONT_RE.test(ch)
  );
}

function scanSqlNumberEnd(sql: string, start: number): number {
  const n = sql.length;
  let i = start + 1;
  if (sql[start] === "0" && i + 1 < n) {
    const radix = sql[i];
    if ((radix === "x" || radix === "X") && /[0-9A-Fa-f_]/.test(sql[i + 1])) {
      i += 2;
      while (i < n && /[0-9A-Fa-f_]/.test(sql[i])) i++;
      return i;
    }
    if ((radix === "b" || radix === "B") && /[01_]/.test(sql[i + 1])) {
      i += 2;
      while (i < n && /[01_]/.test(sql[i])) i++;
      return i;
    }
    if ((radix === "o" || radix === "O") && /[0-7_]/.test(sql[i + 1])) {
      i += 2;
      while (i < n && /[0-7_]/.test(sql[i])) i++;
      return i;
    }
  }
  while (i < n) {
    const c = sql[i];
    if (/[0-9._]/.test(c)) {
      i++;
    } else if ((c === "e" || c === "E") && i + 1 < n && /[0-9]/.test(sql[i + 1])) {
      i += 2;
    } else if (
      (c === "e" || c === "E") &&
      i + 2 < n &&
      (sql[i + 1] === "+" || sql[i + 1] === "-") &&
      /[0-9]/.test(sql[i + 2])
    ) {
      i += 3;
    } else {
      break;
    }
  }
  return i;
}

/**
 * Matches PostgreSQL / DuckDB `libpg_query` (`scan.l` `{quotecontinue}`):
 * horizontal whitespace and `--` line comments containing at least one `\n` or `\r`,
 * followed immediately by `'`. Returns the index of the next opening `'`, or `-1`.
 */
function findQuoteContinuation(sql: string, pos: number): number {
  const n = sql.length;
  let j = pos;
  let sawNewline = false;
  while (j < n) {
    const c = sql[j];
    if (isSqlHorizSpace(c)) {
      j++;
      continue;
    }
    if (c === "\n" || c === "\r") {
      sawNewline = true;
      j++;
      continue;
    }
    if (c === "-" && j + 1 < n && sql[j + 1] === "-") {
      j += 2;
      while (j < n && sql[j] !== "\n" && sql[j] !== "\r") {
        j++;
      }
      continue;
    }
    break;
  }
  return sawNewline && j < n && sql[j] === "'" ? j : -1;
}

/**
 * Strip `--` line comments and nested `/* ... * /` block comments while preserving
 * single-quoted literals, `E'...'` escape literals, `$tag$...$tag$` dollar-quoted
 * literals, and double-quoted identifiers on exact parity with `tokenizeSql`.
 */
export function stripSqlComments(sql: string): string {
  let out = "";
  let i = 0;
  const n = sql.length;
  let prevTokenWasIdent = false;

  while (i < n) {
    const ch = sql[i];
    const next = i + 1 < n ? sql[i + 1] : "";
    const atWordStart = !prevTokenWasIdent;

    if (isSqlWhitespace(ch)) {
      out += DUCKDB_UNICODE_SPACE_RE.test(ch) ? " " : ch;
      i++;
      prevTokenWasIdent = false;
      continue;
    }

    if (DISALLOWED_SQL_CTRL_OR_SEP_RE.test(ch)) {
      throw new SQLValidationError(
        "parse_error",
        "Query contains unsupported control or separator characters.",
      );
    }

    // Reject Unicode escape literals (U&'...' / U&"...") outright
    if (
      atWordStart &&
      (ch === "U" || ch === "u") &&
      next === "&" &&
      i + 2 < n &&
      (sql[i + 2] === "'" || sql[i + 2] === '"')
    ) {
      throw new SQLValidationError(
        "syntax_not_allowed",
        "Unicode escape literals are not permitted in user queries.",
      );
    }

    // PostgreSQL / DuckDB C-escape string literal (`E'...'` / `e'...'` + `{quotecontinue}`)
    if (atWordStart && (ch === "E" || ch === "e") && next === "'") {
      out += sql.slice(i, i + 2);
      i += 2;
      for (;;) {
        let closed = false;
        while (i < n) {
          if (sql[i] === "\\") {
            if (i + 1 >= n) {
              throw new SQLValidationError(
                "parse_error",
                "Unterminated SQL escape string literal.",
              );
            }
            out += sql[i] + sql[i + 1];
            i += 2;
            continue;
          }
          if (sql[i] === "'" && i + 1 < n && sql[i + 1] === "'") {
            out += "''";
            i += 2;
            continue;
          }
          if (sql[i] === "'") {
            out += "'";
            i++;
            closed = true;
            break;
          }
          out += sql[i];
          i++;
        }
        if (!closed) {
          throw new SQLValidationError(
            "parse_error",
            "Unterminated SQL escape string literal.",
          );
        }
        const contQuote = findQuoteContinuation(sql, i);
        if (contQuote === -1) {
          break;
        }
        while (i < contQuote) {
          if (sql[i] === "-" && i + 1 < contQuote && sql[i + 1] === "-") {
            i += 2;
            while (i < contQuote && sql[i] !== "\n" && sql[i] !== "\r") {
              i++;
            }
            out += " ";
          } else if (DUCKDB_UNICODE_SPACE_RE.test(sql[i])) {
            out += " ";
            i++;
          } else {
            out += sql[i];
            i++;
          }
        }
        out += "'";
        i = contQuote + 1;
      }
      prevTokenWasIdent = false;
      continue;
    }

    // Single-quoted literal
    if (ch === "'") {
      out += ch;
      i++;
      let closed = false;
      while (i < n) {
        if (sql[i] === "'" && i + 1 < n && sql[i + 1] === "'") {
          out += "''";
          i += 2;
          continue;
        }
        if (sql[i] === "'") {
          out += "'";
          i++;
          closed = true;
          break;
        }
        out += sql[i];
        i++;
      }
      if (!closed) {
        throw new SQLValidationError("parse_error", "Unterminated SQL string literal.");
      }
      prevTokenWasIdent = false;
      continue;
    }

    // Dollar-quoted literal (`$$...$$` or `$tag$...$tag$`)
    if (ch === "$" && atWordStart) {
      const m = DOLQ_TAG_RE.exec(sql.slice(i));
      if (m) {
        const tag = m[0];
        const endIdx = sql.indexOf(tag, i + tag.length);
        if (endIdx === -1) {
          throw new SQLValidationError(
            "parse_error",
            "Unterminated dollar-quoted string literal.",
          );
        }
        out += sql.slice(i, endIdx + tag.length);
        i = endIdx + tag.length;
        prevTokenWasIdent = false;
        continue;
      }
    }

    // Double-quoted identifier
    if (ch === '"') {
      out += ch;
      i++;
      let closed = false;
      while (i < n) {
        if (sql[i] === '"' && i + 1 < n && sql[i + 1] === '"') {
          out += '""';
          i += 2;
          continue;
        }
        if (sql[i] === '"') {
          out += '"';
          i++;
          closed = true;
          break;
        }
        out += sql[i];
        i++;
      }
      if (!closed) {
        throw new SQLValidationError("parse_error", "Unterminated double-quoted identifier.");
      }
      prevTokenWasIdent = false;
      continue;
    }

    // Line comment (`--` terminated by `\n` or `\r`, preserving newline/CR)
    if (ch === "-" && next === "-") {
      i += 2;
      while (i < n && sql[i] !== "\n" && sql[i] !== "\r") {
        i++;
      }
      out += " ";
      prevTokenWasIdent = false;
      continue;
    }

    // Block comment (supports nested /* ... */ in DuckDB)
    if (ch === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (sql[i] === "/" && i + 1 < n && sql[i + 1] === "*") {
          depth++;
          i += 2;
        } else if (sql[i] === "*" && i + 1 < n && sql[i + 1] === "/") {
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
      prevTokenWasIdent = false;
      continue;
    }

    // Scan full identifier word so `WHERE'x'` and `a1e'x'` never trigger `E'x'`
    if (isIdentStartChar(ch)) {
      let word = ch;
      i++;
      while (i < n && isIdentContChar(sql[i])) {
        word += sql[i];
        i++;
      }
      if (word.toLowerCase() === "uescape") {
        throw new SQLValidationError(
          "syntax_not_allowed",
          "UESCAPE is not permitted in user queries.",
        );
      }
      out += word;
      prevTokenWasIdent = true;
      continue;
    }

    // Number literal (monolithic parity with `tokenizeSql`)
    if (/[0-9]/.test(ch)) {
      const numEnd = scanSqlNumberEnd(sql, i);
      out += sql.slice(i, numEnd);
      i = numEnd;
      prevTokenWasIdent = false;
      continue;
    }

    out += ch;
    i++;
    prevTokenWasIdent = false;
  }

  return out;
}

export interface SqlToken {
  kind: "word" | "string" | "quoted_ident" | "symbol";
  value: string;
  lower: string;
  start: number;
  end: number;
}

export function isDistinctFromOperator(
  tokens: readonly SqlToken[],
  i: number,
): boolean {
  const p1 = i >= 1 ? tokens[i - 1] : undefined;
  if (!p1 || p1.kind !== "word" || p1.lower !== "distinct") {
    return false;
  }
  const p2 = i >= 2 ? tokens[i - 2] : undefined;
  if (!p2 || p2.kind !== "word") {
    return false;
  }
  if (p2.lower === "is") {
    return true;
  }
  if (p2.lower === "not") {
    const p3 = i >= 3 ? tokens[i - 3] : undefined;
    return Boolean(p3 && p3.kind === "word" && p3.lower === "is");
  }
  return false;
}

export function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  const n = sql.length;
  let prevTokenWasIdent = false;

  while (i < n) {
    const ch = sql[i];
    const next = i + 1 < n ? sql[i + 1] : "";
    const atWordStart = !prevTokenWasIdent;

    if (isSqlWhitespace(ch)) {
      i++;
      prevTokenWasIdent = false;
      continue;
    }

    if (DISALLOWED_SQL_CTRL_OR_SEP_RE.test(ch)) {
      throw new SQLValidationError(
        "parse_error",
        "Query contains unsupported control or separator characters.",
      );
    }

    // Line comment (`--` terminated by `\n` or `\r`)
    if (ch === "-" && next === "-") {
      i += 2;
      while (i < n && sql[i] !== "\n" && sql[i] !== "\r") {
        i++;
      }
      prevTokenWasIdent = false;
      continue;
    }

    // Block comment (nested `/* /* ... */ */`)
    if (ch === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (sql[i] === "/" && i + 1 < n && sql[i + 1] === "*") {
          depth++;
          i += 2;
        } else if (sql[i] === "*" && i + 1 < n && sql[i + 1] === "/") {
          depth--;
          i += 2;
        } else {
          i++;
        }
      }
      if (depth > 0) {
        throw new SQLValidationError("parse_error", "Unterminated SQL block comment.");
      }
      prevTokenWasIdent = false;
      continue;
    }

    // Reject Unicode escape literals (U&'...' / U&"...") outright
    if (
      atWordStart &&
      (ch === "U" || ch === "u") &&
      next === "&" &&
      i + 2 < n &&
      (sql[i + 2] === "'" || sql[i + 2] === '"')
    ) {
      throw new SQLValidationError(
        "syntax_not_allowed",
        "Unicode escape literals are not permitted in user queries.",
      );
    }

    // PostgreSQL / DuckDB C-escape string literal (`E'...'` / `e'...'` + `{quotecontinue}`)
    if (atWordStart && (ch === "E" || ch === "e") && next === "'") {
      const tokStart = i;
      let lit = "";
      i += 2;
      for (;;) {
        let closed = false;
        while (i < n) {
          if (sql[i] === "\\") {
            if (i + 1 >= n) {
              throw new SQLValidationError(
                "parse_error",
                "Unterminated SQL escape string literal.",
              );
            }
            lit += sql[i + 1];
            i += 2;
            continue;
          }
          if (sql[i] === "'" && i + 1 < n && sql[i + 1] === "'") {
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
          throw new SQLValidationError(
            "parse_error",
            "Unterminated SQL escape string literal.",
          );
        }
        const contQuote = findQuoteContinuation(sql, i);
        if (contQuote === -1) {
          break;
        }
        i = contQuote + 1;
      }
      tokens.push({ kind: "string", value: lit, lower: lit.toLowerCase(), start: tokStart, end: i });
      prevTokenWasIdent = false;
      continue;
    }

    // Standard single-quoted literal (`'...'` + `{quotecontinue}`)
    if (ch === "'") {
      const tokStart = i;
      let lit = "";
      i++;
      for (;;) {
        let closed = false;
        while (i < n) {
          if (sql[i] === "'" && i + 1 < n && sql[i + 1] === "'") {
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
        const contQuote = findQuoteContinuation(sql, i);
        if (contQuote === -1) {
          break;
        }
        i = contQuote + 1;
      }
      tokens.push({ kind: "string", value: lit, lower: lit.toLowerCase(), start: tokStart, end: i });
      prevTokenWasIdent = false;
      continue;
    }

    // Dollar-quoted literal (`$$...$$` or `$tag$...$tag$`)
    if (ch === "$" && atWordStart) {
      const m = DOLQ_TAG_RE.exec(sql.slice(i));
      if (m) {
        const tokStart = i;
        const tag = m[0];
        const endIdx = sql.indexOf(tag, i + tag.length);
        if (endIdx === -1) {
          throw new SQLValidationError("parse_error", "Unterminated dollar-quoted string literal.");
        }
        const lit = sql.slice(i + tag.length, endIdx);
        i = endIdx + tag.length;
        tokens.push({ kind: "string", value: lit, lower: lit.toLowerCase(), start: tokStart, end: i });
        prevTokenWasIdent = false;
        continue;
      }
    }

    // Double-quoted identifier
    if (ch === '"') {
      const tokStart = i;
      let ident = "";
      i++;
      let closed = false;
      while (i < n) {
        if (sql[i] === '"' && i + 1 < n && sql[i + 1] === '"') {
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
      tokens.push({ kind: "quoted_ident", value: ident, lower: ident.toLowerCase(), start: tokStart, end: i });
      prevTokenWasIdent = false;
      continue;
    }

    // Word / identifier
    if (isIdentStartChar(ch)) {
      const tokStart = i;
      let word = ch;
      i++;
      while (i < n && isIdentContChar(sql[i])) {
        word += sql[i];
        i++;
      }
      const lower = word.toLowerCase();
      if (lower === "uescape") {
        throw new SQLValidationError(
          "syntax_not_allowed",
          "UESCAPE is not permitted in user queries.",
        );
      }
      tokens.push({ kind: "word", value: word, lower, start: tokStart, end: i });
      prevTokenWasIdent = true;
      continue;
    }

    // Number literal
    if (/[0-9]/.test(ch)) {
      const tokStart = i;
      const numEnd = scanSqlNumberEnd(sql, i);
      const num = sql.slice(tokStart, numEnd);
      i = numEnd;
      tokens.push({ kind: "symbol", value: num, lower: num.toLowerCase(), start: tokStart, end: i });
      prevTokenWasIdent = false;
      continue;
    }

    // Single punctuation symbol
    tokens.push({ kind: "symbol", value: ch, lower: ch, start: i, end: i + 1 });
    i++;
    prevTokenWasIdent = false;
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

  const strippedSql = stripSqlComments(sql);
  const tokens = tokenizeSql(sql);
  if (tokens.length === 0) {
    throw new SQLValidationError("empty_sql", "SQL query must not be empty.");
  }
  if (strippedSql !== sql) {
    const strippedTokens = tokenizeSql(strippedSql);
    if (
      strippedTokens.length !== tokens.length ||
      tokens.some(
        (t, idx) =>
          t.kind !== strippedTokens[idx].kind || t.lower !== strippedTokens[idx].lower,
      )
    ) {
      throw new SQLValidationError(
        "parse_error",
        "SQL comment boundary alters token stream.",
      );
    }
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

  const allowedTableSet = allowedTables
    ? new Set(Array.from(allowedTables, (t) => t.toLowerCase()))
    : null;

  if (first.lower === "show") {
    const second = effectiveLen > 1 ? tokens[1].lower : "";
    if (second === "tables") {
      if (allowedTableSet) {
        throw new SQLValidationError(
          "statement_not_allowed",
          "SHOW TABLES is not permitted when table access is restricted.",
        );
      }
      if (effectiveLen > 2) {
        throw new SQLValidationError(
          "statement_not_allowed",
          "Unexpected tokens after SHOW TABLES.",
        );
      }
    } else if (second === "columns") {
      if (
        effectiveLen < 4 ||
        tokens[2].kind !== "word" ||
        tokens[2].lower !== "from"
      ) {
        throw new SQLValidationError(
          "statement_not_allowed",
          "SHOW COLUMNS requires FROM <table>.",
        );
      }
    } else {
      throw new SQLValidationError(
        "statement_not_allowed",
        "Only SHOW TABLES and SHOW COLUMNS FROM <table> are permitted.",
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

  type ScopedCteEntry = {
    nameLower: string;
    activateAfterTokenIdx: number;
    scopeDepth: number;
  };
  const scopedCtes: ScopedCteEntry[] = [];

  const findMatchingCloseParen = (openIdx: number): number => {
    let depth = 0;
    for (let p = openIdx; p < effectiveLen; p++) {
      if (tokens[p].kind === "symbol" && tokens[p].value === "(") {
        depth++;
      } else if (tokens[p].kind === "symbol" && tokens[p].value === ")") {
        depth--;
        if (depth === 0) return p;
      }
    }
    return -1;
  };

  const registerWithCtes = (withIdx: number, scopeDepth: number) => {
    let k = withIdx + 1;
    while (k < effectiveLen) {
      const cteNameTok = tokens[k];
      if (
        (cteNameTok.kind !== "word" && cteNameTok.kind !== "quoted_ident") ||
        (cteNameTok.kind === "word" && cteNameTok.lower === "recursive")
      ) {
        break;
      }
      k++;
      if (k < effectiveLen && tokens[k].kind === "symbol" && tokens[k].value === "(") {
        const colClose = findMatchingCloseParen(k);
        if (colClose === -1) break;
        k = colClose + 1;
      }
      if (!(k < effectiveLen && tokens[k].kind === "word" && tokens[k].lower === "as")) {
        break;
      }
      k++;
      if (k < effectiveLen && tokens[k].kind === "word" && tokens[k].lower === "not") {
        k++;
      }
      if (k < effectiveLen && tokens[k].kind === "word" && tokens[k].lower === "materialized") {
        k++;
      }
      if (!(k < effectiveLen && tokens[k].kind === "symbol" && tokens[k].value === "(")) {
        break;
      }
      const bodyClose = findMatchingCloseParen(k);
      if (bodyClose === -1) break;
      scopedCtes.push({
        nameLower: cteNameTok.lower,
        activateAfterTokenIdx: bodyClose,
        scopeDepth,
      });
      if (
        bodyClose + 1 < effectiveLen &&
        tokens[bodyClose + 1].kind === "symbol" &&
        tokens[bodyClose + 1].value === ","
      ) {
        k = bodyClose + 2;
        continue;
      }
      break;
    }
  };

  const isCteInScope = (lower: string, tokenIdx: number, currentDepth: number): boolean =>
    scopedCtes.some(
      (entry) =>
        entry.nameLower === lower &&
        tokenIdx > entry.activateAfterTokenIdx &&
        entry.scopeDepth <= currentDepth,
    );

  const FROM_CLAUSE_END_KEYWORDS = new Set([
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
  const SUBQUERY_START_KEYWORDS = new Set([
    "select",
    "with",
    "values",
    "from",
    "table",
  ]);

  const checkIdentifierAndCall = (identTok: SqlToken, prev: SqlToken | null, next: SqlToken | null) => {
    const lower = identTok.lower;
    if (identTok.kind === "word" && BLOCKED_STATEMENT_KEYWORDS.has(lower)) {
      const isScalarCall =
        (lower === "replace" || lower === "truncate") &&
        next?.kind === "symbol" &&
        next.value === "(" &&
        prev?.lower !== "or";
      if (!isScalarCall) {
        throw new SQLValidationError(
          `keyword_blocked:${lower}`,
          `Keyword '${identTok.value}' is not allowed in user queries.`,
        );
      }
    }
    if (next && next.kind === "symbol" && next.value === "(") {
      if (BLOCKED_FUNCTION_SET.has(lower)) {
        throw new SQLValidationError(
          `function_denylist:${lower}`,
          `Function '${identTok.value}' is not allowed in user queries.`,
        );
      }
      for (const prefix of BLOCKED_FUNCTION_PREFIXES) {
        if (lower.startsWith(prefix)) {
          throw new SQLValidationError(
            `function_prefix:${prefix}`,
            `Function '${identTok.value}' uses blocked prefix '${prefix}'.`,
          );
        }
      }
    }
    if (next && next.kind === "symbol" && next.value === ".") {
      if (BLOCKED_SCHEMAS.has(lower)) {
        throw new SQLValidationError(
          `schema_blocklist:${lower}`,
          `Schema '${identTok.value}' is not allowed in user queries.`,
        );
      }
    }
  };

  const validateTableObjectToken = (
    objTok: SqlToken,
    afterObj: SqlToken | null,
    allowCte: boolean,
    tokenIdx: number,
    currentDepth: number,
  ) => {
    const lower = objTok.lower;
    if (objTok.value.includes("/") || objTok.value.includes("\\") || objTok.value.includes(".")) {
      throw new SQLValidationError(
        "catalog_blocklist:table_name_path",
        `Table name '${objTok.value}' contains path-like characters.`,
      );
    }
    if (lower.startsWith("pg_")) {
      throw new SQLValidationError(
        "table_prefix:pg_",
        `System table '${objTok.value}' is not allowed in user queries.`,
      );
    }
    const isFunctionTable = afterObj && afterObj.kind === "symbol" && afterObj.value === "(";
    if (isFunctionTable) {
      if (!ALLOWED_TABLE_FUNCTIONS.has(lower)) {
        throw new SQLValidationError(
          `function_table_not_allowed:${lower}`,
          `Table function '${objTok.value}' is not allowed in user queries.`,
        );
      }
    } else if (
      allowedTableSet &&
      !allowedTableSet.has(lower) &&
      !(allowCte && isCteInScope(lower, tokenIdx, currentDepth))
    ) {
      throw new SQLValidationError(
        "table_not_allowed",
        `Table '${objTok.value}' is not in the allowed table list.`,
      );
    }
  };

  type ParenFrame = {
    kind: "subquery" | "from_join" | "expr";
    savedInFrom: boolean;
  };
  const parenStack: ParenFrame[] = [];
  let inFromClause = false;
  let expectingTableRef = false;

  for (let i = 0; i < effectiveLen; i++) {
    const tok = tokens[i];
    const nextTok = i + 1 < effectiveLen ? tokens[i + 1] : null;
    const prevTok = i > 0 ? tokens[i - 1] : null;
    const inExprParen =
      parenStack.length > 0 && parenStack[parenStack.length - 1].kind === "expr";

    if (tok.kind === "word" || tok.kind === "quoted_ident") {
      checkIdentifierAndCall(tok, prevTok, nextTok);
    }

    if (tok.kind === "word") {
      if (
        i > 0 &&
        (tok.lower === "show" || tok.lower === "describe" || tok.lower === "summarize")
      ) {
        throw new SQLValidationError(
          "statement_not_allowed",
          `${tok.value.toUpperCase()} is only permitted as a top-level statement.`,
        );
      }
      if (
        tok.lower === "desc" &&
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
        throw new SQLValidationError(
          "statement_not_allowed",
          "DESC statement shorthand is not permitted; use top-level DESCRIBE <table>.",
        );
      }
      if (
        tok.lower === "with" &&
        (i === 0 ||
          prevTok?.value === "(" ||
          (i === 1 && (tokens[0].lower === "summarize" || tokens[0].lower === "describe")))
      ) {
        registerWithCtes(i, parenStack.length);
      }
    }

    if (tok.kind === "symbol" && tok.value === "(") {
      let look = i + 1;
      while (
        look < effectiveLen &&
        tokens[look].kind === "symbol" &&
        tokens[look].value === "("
      ) {
        look++;
      }
      const lookTok = look < effectiveLen ? tokens[look] : null;
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
          SUBQUERY_START_KEYWORDS.has(lookTok.lower),
      );
      const isDirectSubquery = hasSubqueryHead && look === i + 1;

      if (expectingTableRef && !isDirectSubquery) {
        parenStack.push({
          kind: "from_join",
          savedInFrom: inFromClause,
        });
        inFromClause = true;
        expectingTableRef = !hasSubqueryHead;
      } else if (hasSubqueryHead) {
        parenStack.push({
          kind: "subquery",
          savedInFrom: inFromClause,
        });
        inFromClause = false;
        expectingTableRef = false;
      } else {
        parenStack.push({
          kind: "expr",
          savedInFrom: inFromClause,
        });
        expectingTableRef = false;
      }
      continue;
    }

    if (tok.kind === "symbol" && tok.value === ")") {
      if (expectingTableRef) {
        throw new SQLValidationError("parse_error", "Expected table reference.");
      }
      const frame = parenStack.pop();
      if (!frame) {
        throw new SQLValidationError("parse_error", "Unbalanced closing parenthesis.");
      }
      for (let c = scopedCtes.length - 1; c >= 0; c--) {
        if (scopedCtes[c].scopeDepth > parenStack.length) {
          scopedCtes.splice(c, 1);
        }
      }
      inFromClause = frame.savedInFrom;
      expectingTableRef = false;
      continue;
    }

    if (!inExprParen) {
      if (tok.kind === "word") {
        const lower = tok.lower;
        if (
          (lower === "from" && !isDistinctFromOperator(tokens, i)) ||
          (lower === "table" && prevTok?.value !== ".")
        ) {
          inFromClause = true;
          expectingTableRef = true;
          continue;
        }
        if (lower === "join") {
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
            !nextTok ||
            (nextTok.kind === "symbol" &&
              (nextTok.value === ")" || nextTok.value === ";"))
          ) {
            throw new SQLValidationError(
              "statement_not_allowed",
              `${tok.value.toUpperCase()} requires an explicit table or query target.`,
            );
          }
          if (
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
        if (inFromClause && FROM_CLAUSE_END_KEYWORDS.has(lower)) {
          if (expectingTableRef) {
            throw new SQLValidationError("parse_error", "Expected table reference.");
          }
          inFromClause = false;
          expectingTableRef = false;
        }
      } else if (inFromClause && tok.kind === "symbol" && tok.value === ",") {
        if (expectingTableRef) {
          throw new SQLValidationError("parse_error", "Expected table reference.");
        }
        expectingTableRef = true;
        continue;
      }
    }

    if (expectingTableRef) {
      if (tok.kind === "word" && (tok.lower === "lateral" || tok.lower === "only")) {
        continue;
      }
      if (tok.kind === "string") {
        throw new SQLValidationError(
          "table_path_literal",
          "String literal file paths in table positions are not allowed in user queries.",
        );
      }
      if (tok.kind === "word" || tok.kind === "quoted_ident") {
        if (nextTok && nextTok.kind === "symbol" && nextTok.value === ".") {
          const objTok = i + 2 < effectiveLen ? tokens[i + 2] : null;
          const afterObj = i + 3 < effectiveLen ? tokens[i + 3] : null;
          if (!objTok || (objTok.kind !== "word" && objTok.kind !== "quoted_ident")) {
            throw new SQLValidationError("parse_error", "Malformed qualified table reference.");
          }
          if (afterObj && afterObj.kind === "symbol" && afterObj.value === ".") {
            throw new SQLValidationError(
              `schema_blocklist:${tok.lower}`,
              `Catalog-qualified table '${tok.value}.${objTok.value}' is not allowed in user queries.`,
            );
          }
          if (tok.lower !== "main") {
            throw new SQLValidationError(
              `schema_blocklist:${tok.lower}`,
              `Schema '${tok.value}' is not allowed in user queries.`,
            );
          }
          checkIdentifierAndCall(objTok, nextTok, afterObj);
          validateTableObjectToken(objTok, afterObj, false, i + 2, parenStack.length);
          i += 2;
          expectingTableRef = false;
          continue;
        }
        validateTableObjectToken(tok, nextTok, true, i, parenStack.length);
        expectingTableRef = false;
      } else {
        throw new SQLValidationError("parse_error", "Expected table reference.");
      }
    }
  }

  if (expectingTableRef) {
    throw new SQLValidationError("parse_error", "Expected table reference.");
  }
  if (parenStack.length > 0) {
    throw new SQLValidationError("parse_error", "Unbalanced opening parenthesis.");
  }

  const elapsedMs = (typeof performance !== "undefined" ? performance.now() : Date.now()) - t0;
  return {
    valid: true,
    normalizedSql: strippedSql.trim().replace(/;\s*$/, ""),
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
