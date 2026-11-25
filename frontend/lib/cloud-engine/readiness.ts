import { isNetworkFailure } from "./fetch-retry";
import { isCloudMode } from "./mode";

let sequence = 0;
export type CloudQueryContext = { route: string; search: string };

function context() {
  return { route: globalThis.location.pathname, search: globalThis.location.search };
}

function canObserve(): boolean {
  return isCloudMode() && typeof globalThis.dispatchEvent === "function"
    && typeof globalThis.CustomEvent === "function" && typeof globalThis.location === "object";
}

export function captureCloudQueryContext(): CloudQueryContext | undefined {
  return canObserve() ? context() : undefined;
}

/** Lets a stalled readiness wait report which loading stage the page was left in. */
export function publishCloudLoading(detail: { stage: string | null; jobs: number; progress: object | null }): void {
  if (!canObserve()) return;
  globalThis.dispatchEvent(new CustomEvent("fla:cloud-loading", { detail }));
}

export function publishCloudService(serviceId: string | null): void {
  if (!canObserve()) return;
  globalThis.dispatchEvent(new CustomEvent("fla:cloud-service", { detail: { serviceId } }));
}

function rowCount(data: unknown): number | null {
  if (!data || typeof data !== "object") return null;
  const body = data as Record<string, unknown>;
  const aggregates = body.aggregates && typeof body.aggregates === "object"
    ? body.aggregates as Record<string, unknown> : body;
  const rows = aggregates.total_rows;
  return typeof rows === "number" && Number.isSafeInteger(rows) && rows >= 0 ? rows : null;
}

const MESSAGE_LIMIT = 200;
const MAX_SCAN_INPUT_LEN = 65_536;
const MAX_URL_SCHEME_LEN = 32;

const SQL_KEYWORD_LIST = [
  "SELECT", "WHERE", "AS", "VALUES", "LIKE", "ILIKE", "SIMILAR", "IS", "IN", "EXISTS",
  "AND", "OR", "NOT", "XOR", "FROM", "JOIN", "ON", "USING", "BY", "CASE", "WHEN",
  "THEN", "ELSE", "END", "HAVING", "LIMIT", "OFFSET", "CAST", "TRY_CAST", "SET",
  "INTO", "WITH", "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "DISTINCT", "GROUP",
  "ORDER", "BETWEEN", "NULL", "TRUE", "FALSE", "OVER", "FILTER", "QUALIFY", "WINDOW",
  "ESCAPE",
] as const;

const DUCKDB_BASE_TYPES = new Set<string>([
  "BOOL", "BOOLEAN",
  "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
  "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
  "INT", "INT8", "INT16", "INT32", "INT64", "INT128",
  "UINT8", "UINT16", "UINT32", "UINT64", "UINT128",
  "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
  "VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "BLOB", "BYTEA", "UUID",
  "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_S", "TIMESTAMP_MS",
  "TIMESTAMP_NS", "TIMESTAMP_TZ", "TIMETZ", "INTERVAL",
  "BIT", "BITSTRING", "BIGNUM", "VARINT", "JSON", "LIST", "MAP", "STRUCT", "UNION", "ENUM", "ARRAY",
]);

const COMPOUND_DUCKDB_TYPES = new Set<string>(["STRUCT", "UNION", "ENUM", "MAP"]);
const NUMERIC_PARAM_DUCKDB_TYPES = new Set<string>([
  "DECIMAL", "NUMERIC", "VARCHAR", "CHAR", "BPCHAR", "FLOAT", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
]);
const SAFE_MIXED_CASE_TYPES = new Set<string>(["Bignum", "Varint"]);

const DUCKDB_ALNUM_TYPES = new Set<string>([
  "INT8", "INT16", "INT32", "INT64", "INT128",
  "UINT8", "UINT16", "UINT32", "UINT64", "UINT128",
  "FLOAT4", "FLOAT8", "UTF8",
]);

const SAFE_UPPERCASE_WORDS = new Set<string>([
  ...SQL_KEYWORD_LIST,
  ...DUCKDB_BASE_TYPES,
  "HTTP", "HTTPS", "JSON", "IO", "DB", "SQL", "URL", "URI", "API", "TLS", "SSL",
  "KB", "MB", "GB", "TB", "B", "ID", "IP", "DNS", "TCP", "EOF", "NAN", "INF",
  "OPFS", "WASM", "CDN", "FOS", "KV",
]);

const SAFE_BYTE_UNITS = new Set<string>(["KiB", "MiB", "GiB", "TiB", "KB", "MB", "GB", "TB", "B"]);

const SAFE_HEADER_WORDS = new Set<string>([
  "Out", "of", "Memory", "Error", "Binder", "Parser", "Conversion", "Invalid", "Input",
  "Runtime", "Catalog", "Syntax", "Range", "Constraint", "Permission", "Internal",
  "Fatal", "Serialization", "Sequence", "TransactionContext", "Transaction",
  "Not", "Implemented", "HTTP", "IO", "JSON", "Network", "Type", "Regex",
]);

const SAFE_CLAUSE_START_WORDS = new Set<string>([
  ...SAFE_HEADER_WORDS,
  "Backend", "Header", "Failed", "Template", "Could", "Casting", "Interval", "Malformed", "Unexpected",
  "Expected", "Cannot", "Unable", "No", "Function", "Scalar", "Aggregate", "Table",
  "Column", "View", "Schema", "Database", "Index", "Macro", "Referenced", "Overflow",
  "Candidate", "Did", "Line", "Like", "Map", "List", "Struct", "Duplicate",
  "Ambiguous", "Unimplemented", "You", "Either", "Trailing", "Unrecognized",
  "Union", "Enum", "Bignum", "Varint", "Primary", "Unique", "Foreign", "Check", "Left-shift",
]);

const SAFE_CONTRACTIONS = new Set<string>([
  "can't", "doesn't", "couldn't", "didn't", "isn't", "wasn't",
  "won't", "don't", "haven't", "hasn't", "aren't", "weren't",
]);

const SAFE_CATALOG_OBJECTS = [
  "Scalar Function", "Aggregate Function", "Table Function", "Pragma Function", "Copy Function",
  "Table", "View", "Schema", "Database", "Column", "Index", "Sequence", "Type", "Macro",
  "Function", "Catalog",
] as const;

const SAFE_CONSTRAINT_PHRASES = new Set<string>([
  "primary key constraint",
  "unique constraint",
  "foreign key constraint",
  "check constraint",
  "not null constraint",
]);

const SAFE_KEY_PREFIX_WORDS = new Set<string>([
  "primary", "foreign", "duplicate", "unique", "candidate", "partition", "sort", "join", "map",
]);

const DIAGNOSTIC_VOCAB = new Set<string>([
  // Closed grammar glue
  "a", "after", "an", "and", "are", "as", "at", "be", "because", "before", "between", "by",
  "can", "cannot", "could", "did", "do", "does", "during", "for", "from", "had", "has", "have",
  "if", "in", "into", "is", "it", "its", "may", "might", "must", "near", "need", "no", "not",
  "of", "on", "only", "or", "out", "over", "per", "should", "since", "so", "than", "that",
  "the", "there", "this", "to", "too", "unable", "under", "until", "up", "was", "were", "when",
  "where", "which", "while", "will", "with", "within", "without", "would",
  // Closed DuckDB / HTTP / Parquet / OPFS / Cloud-Engine diagnostic vocabulary
  "abort", "aborted", "access", "according", "add", "addition", "aggregate", "allocate",
  "allocation", "allowed", "ambiguous", "appear", "argument", "arguments", "array", "ascii", "assertion",
  "available", "backend", "bad", "base64", "batch", "begin", "big", "bignum", "binary", "binder", "bit",
  "blob", "block", "blocked", "body", "boolean", "bound", "bounds", "bracket", "browser",
  "budget", "buffer", "build", "byte", "bytes", "cache", "cached", "call", "cancel", "canceled",
  "cancelled", "candidate", "capture", "cast", "casting", "casts", "catalog", "char",
  "character", "characters", "check", "chunk", "class", "clause", "client", "close", "closed",
  "code", "column", "columns", "command", "commit", "compatible", "compile", "complete",
  "compression", "condition", "config", "conflict", "connect", "connection", "const",
  "constant", "constraint", "contains", "context", "conversion", "convert", "corrupt",
  "corrupted", "count", "create", "cursor", "data", "database", "date", "decimal", "decode",
  "decoding", "default", "defined", "delete", "delimited", "denied", "depth", "destination",
  "detail", "details", "directory", "disk", "distinct", "divide", "division", "dollar",
  "double", "drop", "duplicate", "empty", "encode", "encoding", "end", "engine", "entry",
  "enum", "eof", "equal", "error", "errors", "escape", "escaped", "eval", "exceed", "exceeded",
  "exceeds", "exception", "exec", "execute", "execution", "exist", "exists", "expected",
  "expired", "explicit", "expression", "extension", "external", "fail", "failed", "failure",
  "false", "fatal", "fetch", "fetching", "field", "fields", "file", "files", "filter", "float",
  "flush", "footer", "forbidden", "foreign", "format", "found", "frame", "free", "full",
  "function", "functions", "gateway", "given", "global", "group", "handle", "handler",
  "header", "headers", "heap", "hex", "host", "http", "https", "identifier", "illegal",
  "implicit", "implicitly", "incompatible", "incomplete", "index", "init", "initialize",
  "input", "insert", "int", "integer", "internal", "interrupted", "interval", "invalid", "io",
  "item", "join", "json", "junk", "key", "keys", "kind", "large", "latin1", "left", "length",
  "less", "limit", "limited", "line", "list", "literal", "load", "loading", "local", "lock",
  "locked", "log", "long", "lookup", "macro", "magic", "malformed", "many", "map", "match",
  "matched", "matches", "matching", "max", "maximum", "member", "members", "memory", "merge",
  "message", "metadata", "method", "min", "minimum", "mismatch", "missing", "mode", "module",
  "more", "multiple", "multiplication", "name", "named", "names", "namespace", "negative",
  "nested", "network", "next", "node", "non", "none", "null", "number", "numeric", "object",
  "offset", "op", "open", "operation", "operator", "opfs", "option", "order", "origin",
  "output", "overflow", "page", "param", "parameter", "parameters", "parquet", "parse",
  "parser", "parsing", "part", "partial", "partition", "path", "pattern", "pending",
  "permission", "pipe", "plan", "pointer", "pool", "port", "position", "positive", "precision",
  "prefix", "prepare", "primary", "process", "processing", "protocol", "proxy", "query",
  "queue", "quota", "quote", "quoted", "range", "rate", "raw", "reach", "reachable", "read",
  "reader", "reading", "ready", "record", "recursion", "ref", "reference", "referenced",
  "refused", "regex", "register", "rejected", "relation", "remote", "remove", "repeat",
  "replace", "reply", "request", "required", "reset", "resize", "resolve", "resource",
  "response", "result", "retry", "return", "right", "rollback", "root", "row", "rows", "rule",
  "run", "runtime", "scalar", "scale", "scan", "schema", "scope", "second", "seconds",
  "section", "segment", "select", "sequence", "serialization", "serialize", "server",
  "service", "session", "set", "short", "shutdown", "signed", "single", "size", "skip",
  "slice", "slot", "small", "socket", "sort", "source", "space", "span", "specifier", "split",
  "sql", "stack", "stage", "stale", "start", "state", "statement", "static", "status", "step",
  "storage", "store", "stream", "strftime", "string", "strptime", "struct", "subquery",
  "subtraction", "suffix", "sum", "support", "supported", "syntax", "system", "table",
  "tables", "tag", "target", "task", "temp", "template", "temporary", "terminated", "text",
  "thread", "threshold", "time", "timed", "timeout", "timestamp", "timezone", "tls", "total",
  "trailing", "transaction", "transfer", "tree", "true", "truncate", "truncated", "tuple",
  "type", "types", "unclosed", "underflow", "unexpected", "unicode", "unimplemented", "union",
  "unique", "unit", "unknown", "unmatched", "unrecognized", "unsigned", "unsupported",
  "unterminated", "update", "upstream", "uri", "url", "usage", "use", "used", "using", "utf8",
  "valid", "validate", "validation", "value", "values", "varchar", "variable", "varint",
  "vector", "version", "view", "violate", "violates", "violation", "virtual", "wait",
  "waiting", "warning", "wasm", "window", "worker", "write", "writer", "writing", "wrong",
  "zero",
]);

const SAFE_DECODE_ENCODINGS = new Set<string>([
  "base64", "hex", "utf8", "ascii", "json", "blob", "latin1",
]);

const SECRET_STEMS = new Set<string>([
  "token", "tokens", "key", "keys", "secret", "secrets",
  "password", "passwd", "pwd", "authorization", "authenticate", "auth", "bearer",
  "cookie", "cookies", "signature", "sig", "credential", "credentials",
  "session", "sessions", "apikey", "jwt", "nonce", "otp", "pin", "hmac", "negotiate",
]);
const AUTH_SCHEMES = new Set<string>([
  "bearer", "basic", "digest", "token",
  "oauth", "apikey", "hmac", "jwt", "negotiate", "aws4-hmac-sha256",
]);
const COOKIE_STEMS = new Set<string>(["cookie", "cookies"]);
const AUTHENTICATE_KEY_STEMS = new Set<string>(["authenticate"]);
const AUTH_HEADER_KEY_STEMS = new Set<string>(["authorization", "authenticate"]);
const MULTI_PARAM_AUTH_KEY_STEMS = new Set<string>(["authorization", "authenticate", "digest", "oauth", "hmac", "aws4-hmac-sha256"]);
const MULTI_PARAM_AUTH_SCHEMES = new Set<string>(["digest", "oauth", "hmac", "aws4-hmac-sha256"]);

function isSecretJsonKey(decodedKey: string): boolean {
  if (!decodedKey) return false;
  return hasKeyStemSegment(decodedKey, SECRET_STEMS, true) || AUTH_SCHEMES.has(decodedKey.toLowerCase());
}

function scanCookieExpiresValueEnd(line: string, vStart: number): number {
  const len = line.length;
  const skipWs = (pos: number): number => {
    let p = pos;
    while (p < len && isInlineWs(line[p])) p += 1;
    return p;
  };
  const scanDigits = (pos: number, minD: number, maxD: number): number => {
    let p = pos;
    const lim = Math.min(len, pos + maxD);
    while (p < lim && isAsciiDigit(line[p])) p += 1;
    return p - pos >= minD ? p : -1;
  };
  const scanAlpha = (pos: number, minA: number, maxA: number): number => {
    let p = pos;
    const lim = Math.min(len, pos + maxA);
    while (p < lim && isAsciiAlpha(line[p])) p += 1;
    return p - pos >= minA ? p : -1;
  };
  const scanTzOffset = (pos: number): number => {
    if (pos >= len || (line[pos] !== "+" && line[pos] !== "-")) return -1;
    const hEnd = scanDigits(pos + 1, 2, 2);
    if (hEnd === -1) return -1;
    const mStart = line[hEnd] === ":" ? hEnd + 1 : hEnd;
    return scanDigits(mStart, 2, 2);
  };
  const scanOptionalTzAndParen = (pos: number): number => {
    let p = pos;
    const isTzDelim = (idx: number): boolean => (
      idx >= len || isInlineWs(line[idx]) || line[idx] === ";" || line[idx] === ","
    );
    const afterWs1 = skipWs(p);
    if (afterWs1 > p && afterWs1 < len) {
      let tzEnd = -1;
      const alphaEnd = scanAlpha(afterWs1, 1, 5);
      if (alphaEnd !== -1) {
        const offEnd = scanTzOffset(alphaEnd);
        if (offEnd !== -1) {
          tzEnd = offEnd;
        } else if (isTzDelim(alphaEnd)) {
          tzEnd = alphaEnd;
          const afterAlphaWs = skipWs(alphaEnd);
          if (afterAlphaWs > alphaEnd) {
            const spacedOffEnd = scanTzOffset(afterAlphaWs);
            if (spacedOffEnd !== -1 && isTzDelim(spacedOffEnd)) {
              tzEnd = spacedOffEnd;
            }
          }
        }
      } else {
        const offEnd = scanTzOffset(afterWs1);
        if (offEnd !== -1 && isTzDelim(offEnd)) {
          tzEnd = offEnd;
          const afterOffWs = skipWs(offEnd);
          if (afterOffWs > offEnd) {
            const spacedAlphaEnd = scanAlpha(afterOffWs, 1, 5);
            if (spacedAlphaEnd !== -1 && isTzDelim(spacedAlphaEnd)) {
              tzEnd = spacedAlphaEnd;
            }
          }
        } else {
          const yearEnd = scanDigits(afterWs1, 4, 4);
          if (yearEnd !== -1 && !isAsciiDigit(line[yearEnd])) tzEnd = yearEnd;
        }
      }
      if (tzEnd !== -1 && isTzDelim(tzEnd)) {
        p = tzEnd;
      }
    }
    const afterWs2 = skipWs(p);
    if (afterWs2 > p && afterWs2 < len && line[afterWs2] === "(") {
      let parenEnd = afterWs2 + 1;
      while (
        parenEnd < len
        && line[parenEnd] !== ")"
        && line[parenEnd] !== "("
        && line[parenEnd] !== "\r"
        && line[parenEnd] !== "\n"
        && line[parenEnd] !== ";"
        && line[parenEnd] !== ","
      ) {
        parenEnd += 1;
      }
      if (parenEnd > afterWs2 + 1 && parenEnd < len && line[parenEnd] === ")") {
        const closed = parenEnd + 1;
        if (isTzDelim(closed)) {
          p = closed;
        }
      }
    }
    return p;
  };
  const finishDateOrSemi = (pos: number): number => {
    if (pos < len && isInlineWs(line[pos])) {
      for (let s = pos; s < len; s += 1) {
        const ch = line[s]!;
        if (ch === ";") return s;
        if (ch === "=" || ch === "\r" || ch === "\n" || ch === "}" || ch === "]") break;
        if ((ch === '"' || ch === "'" || ch === "`") && line[s + 1] === ":") break;
      }
    }
    return pos;
  };
  const scanTime = (pos: number): number => {
    const hEnd = scanDigits(pos, 1, 2);
    if (hEnd === -1 || line[hEnd] !== ":") return -1;
    const mEnd = scanDigits(hEnd + 1, 2, 2);
    if (mEnd === -1) return -1;
    let p = mEnd;
    if (line[p] === ":") {
      const sEnd = scanDigits(p + 1, 2, 2);
      if (sEnd === -1) return -1;
      p = sEnd;
    }
    if (line[p] === ".") {
      const fEnd = scanDigits(p + 1, 1, 9);
      if (fEnd === -1) return -1;
      p = fEnd;
    }
    return p;
  };
  const isDateBoundary = (pos: number, allowColon: boolean): boolean => {
    if (pos >= len) return true;
    const ch = line[pos]!;
    return isInlineWs(ch) || ch === ";" || ch === "," || ch === "\r" || ch === "\n" || (allowColon && ch === ":");
  };

  // 1. ISO-8601 date / date-time: YYYY-MM-DD([T ]HH:MM(:SS)?(.fff)?(Z|±HH:?MM)?)?
  const isoY = scanDigits(vStart, 4, 4);
  if (isoY !== -1 && line[isoY] === "-") {
    const isoM = scanDigits(isoY + 1, 2, 2);
    if (isoM !== -1 && line[isoM] === "-") {
      const isoD = scanDigits(isoM + 1, 2, 2);
      if (isoD !== -1) {
        let p = isoD;
        let hasTime = false;
        if (line[p] === "T" || line[p] === "t" || isInlineWs(line[p])) {
          const tStart = isInlineWs(line[p]) ? skipWs(p) : p + 1;
          const tEnd = scanTime(tStart);
          if (tEnd !== -1) {
            p = tEnd;
            hasTime = true;
            if (line[p] === "Z" || line[p] === "z") {
              p += 1;
            } else {
              const offEnd = scanTzOffset(p);
              if (offEnd !== -1) p = offEnd;
            }
          }
        }
        if (hasTime) {
          p = scanOptionalTzAndParen(p);
          if (isDateBoundary(p, false)) return finishDateOrSemi(p);
        } else if (isDateBoundary(p, true)) {
          return finishDateOrSemi(p);
        }
      }
    }
  }

  // 2. HTTP / RFC / JS Date: (Weekday,? ws+)? (DD(-|ws+)Mon(-|ws+)YYYY | Mon ws+ DD (ws+ YYYY)?) (ws+ HH:MM(:SS)?)? (ws+ TZ)? (ws+ (...))?
  let p = vStart;
  const wdEnd = scanAlpha(p, 3, 9);
  if (wdEnd !== -1) {
    let afterWd = wdEnd;
    const hasComma = line[afterWd] === ",";
    if (hasComma) afterWd += 1;
    const afterWdWs = skipWs(afterWd);
    if (afterWdWs > afterWd && (hasComma || isAsciiDigit(line[afterWdWs]) || isAsciiAlpha(line[afterWdWs]))) {
      // Check if what follows is a date (so we don't consume Month as Weekday in "Jan 01 2099").
      if (hasComma || isAsciiAlpha(line[afterWdWs]) || scanDigits(afterWdWs, 1, 2) !== -1) {
        // Only treat as weekday if it's followed by a comma OR by Month+Day OR by Day+Month.
        const looksLikeDayMonth = (() => {
          const dEnd = scanDigits(afterWdWs, 1, 2);
          if (dEnd === -1) return false;
          const sepEnd = line[dEnd] === "-" ? dEnd + 1 : skipWs(dEnd);
          return sepEnd > dEnd && scanAlpha(sepEnd, 3, 9) !== -1;
        })();
        const looksLikeMonthDay = (() => {
          const mEnd = scanAlpha(afterWdWs, 3, 9);
          if (mEnd === -1) return false;
          const sepEnd = skipWs(mEnd);
          return sepEnd > mEnd && scanDigits(sepEnd, 1, 2) !== -1;
        })();
        if (hasComma || looksLikeDayMonth || looksLikeMonthDay) {
          p = afterWdWs;
        }
      }
    }
  }

  let dateEnd = -1;
  // Form A: DD(-|ws+)Mon(-|ws+)YYYY
  const dEndA = scanDigits(p, 1, 2);
  if (dEndA !== -1) {
    const sep1 = line[dEndA] === "-" ? dEndA + 1 : skipWs(dEndA);
    if (sep1 > dEndA) {
      const mEndA = scanAlpha(sep1, 3, 9);
      if (mEndA !== -1) {
        const sep2 = line[mEndA] === "-" ? mEndA + 1 : skipWs(mEndA);
        if (sep2 > mEndA) {
          const yEndA = scanDigits(sep2, 2, 4);
          if (yEndA !== -1 && !isAsciiDigit(line[yEndA])) {
            dateEnd = yEndA;
          }
        }
      }
    }
  }
  // Form B: Mon ws+ DD,? (ws+ YYYY)?
  if (dateEnd === -1) {
    const mEndB = scanAlpha(p, 3, 9);
    if (mEndB !== -1) {
      const sep1 = skipWs(mEndB);
      if (sep1 > mEndB) {
        const dEndB = scanDigits(sep1, 1, 2);
        if (dEndB !== -1 && !isAsciiDigit(line[dEndB])) {
          dateEnd = dEndB;
          const afterDay = line[dEndB] === "," ? dEndB + 1 : dEndB;
          const sep2 = skipWs(afterDay);
          if (sep2 > afterDay) {
            const yEndB = scanDigits(sep2, 2, 4);
            if (yEndB !== -1 && !isAsciiDigit(line[yEndB]) && line[yEndB] !== ":") {
              dateEnd = yEndB;
            }
          }
        }
      }
    }
  }

  if (dateEnd !== -1) {
    const afterDateWs = skipWs(dateEnd);
    if (afterDateWs > dateEnd) {
      const tEnd = scanTime(afterDateWs);
      if (tEnd !== -1) {
        const fullEnd = scanOptionalTzAndParen(tEnd);
        if (isDateBoundary(fullEnd, false)) return finishDateOrSemi(fullEnd);
      }
      // Truncated date with hour only (e.g. "01 Jan 2099 00")
      const truncHr = scanDigits(afterDateWs, 1, 2);
      if (truncHr !== -1 && !isAsciiDigit(line[truncHr]) && isDateBoundary(truncHr, true)) {
        return finishDateOrSemi(truncHr);
      }
    }
    if (isDateBoundary(dateEnd, true)) return finishDateOrSemi(dateEnd);
  }

  // 3. Semicolon fallback (O(N), no 128-char cap)
  for (let s = vStart; s < len; s += 1) {
    const ch = line[s]!;
    if (ch === ";") return s;
    if (ch === "=" || ch === "\r" || ch === "\n" || ch === "}" || ch === "]") break;
    if ((ch === '"' || ch === "'" || ch === "`") && line[s + 1] === ":") break;
  }
  return -1;
}

const WORD_CHAR_RE = /[\p{L}\p{N}_]/u;

function isInlineWs(ch: string | undefined): boolean {
  if (ch === undefined) return false;
  if (ch === " " || ch === "\t") return true;
  const code = ch.charCodeAt(0);
  if (code < 0x20) return code === 0x0b || code === 0x0c;
  if (code < 0x80) return false;
  return /\s/.test(ch);
}

function isDollarTagStart(ch: string): boolean {
  const c = ch.charCodeAt(0);
  return (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c === 95 || c >= 0x80;
}

function isDollarTagChar(ch: string): boolean {
  const c = ch.charCodeAt(0);
  return (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || (c >= 48 && c <= 57) || c === 95 || c >= 0x80;
}

function isValidDollarTagBody(raw: string, start: number, end: number): boolean {
  if (start === end) return true;
  if (!isDollarTagStart(raw[start]!)) return false;
  for (let idx = start + 1; idx < end; idx += 1) {
    if (!isDollarTagChar(raw[idx]!)) return false;
  }
  return true;
}

function isAsciiDigit(ch: string | undefined): boolean {
  return ch !== undefined && ch >= "0" && ch <= "9";
}

function isHexDigit(ch: string | undefined): boolean {
  return ch !== undefined && (
    (ch >= "0" && ch <= "9") || (ch >= "a" && ch <= "f") || (ch >= "A" && ch <= "F")
  );
}

function isAsciiLower(ch: string | undefined): boolean {
  return ch !== undefined && ch >= "a" && ch <= "z";
}

function isAsciiUpper(ch: string | undefined): boolean {
  return ch !== undefined && ch >= "A" && ch <= "Z";
}

function isAsciiAlpha(ch: string | undefined): boolean {
  return isAsciiLower(ch) || isAsciiUpper(ch);
}

function hasKeyStemSegment(
  raw: string,
  stems: ReadonlySet<string>,
  allowSpace: boolean,
): boolean {
  if (!raw) return false;
  const len = raw.length;
  if (len >= 3 && len <= 24 && stems.has(raw.toLowerCase())) return true;
  let segStart = 0;
  let prevKind = 0; // 0=other, 1=lower, 2=upper, 3=digit
  for (let i = 0; i <= len; i += 1) {
    const code = i < len ? raw.charCodeAt(i) : -1;
    const kind = code >= 97 && code <= 122
      ? 1
      : code >= 65 && code <= 90
        ? 2
        : code >= 48 && code <= 57
          ? 3
          : 0;
    const isWs = kind === 0 && i < len && (
      code <= 32
        ? (code === 32 || (code >= 9 && code <= 13))
        : (code >= 0x80 && /\s/.test(raw[i]!))
    );
    const isSep = i === len || (kind === 0 && (allowSpace || !isWs));
    if (isSep) {
      const segLen = i - segStart;
      if (segLen >= 3 && segLen <= 24 && stems.has(raw.slice(segStart, i).toLowerCase())) {
        return true;
      }
      segStart = i + 1;
      prevKind = 0;
      continue;
    }
    if (i > segStart && kind !== 0 && prevKind !== 0) {
      const nextCode = i + 1 < len ? raw.charCodeAt(i + 1) : -1;
      const nextLower = nextCode >= 97 && nextCode <= 122;
      const boundary = (prevKind === 1 && kind === 2)
        || (prevKind === 2 && kind === 2 && nextLower)
        || (prevKind === 3 && (kind === 1 || kind === 2))
        || ((prevKind === 1 || prevKind === 2) && kind === 3);
      if (boundary) {
        const segLen = i - segStart;
        if (segLen >= 3 && segLen <= 24 && stems.has(raw.slice(segStart, i).toLowerCase())) {
          return true;
        }
        segStart = i;
      }
    }
    prevKind = kind;
  }
  return false;
}

function scanAuthSchemeTokenEnd(line: string, p: number): number {
  let sEnd = p;
  const limit = line.length;
  while (
    sEnd < limit
    && (
      isAsciiAlpha(line[sEnd])
      || isAsciiDigit(line[sEnd])
      || line[sEnd] === "-"
      || line[sEnd] === "_"
      || line[sEnd] === "."
    )
  ) {
    sEnd += 1;
  }
  while (sEnd > p && line[sEnd - 1] === ".") {
    sEnd -= 1;
  }
  return sEnd;
}

function advancePastSepRun(line: string, pos: number): number {
  let p = pos;
  const len = line.length;
  while (p < len) {
    const ch = line[p];
    if (ch === "=" || (ch === ":" && !line.startsWith("://", p))) {
      p += 1;
      if (line[p] === ">") p += 1;
    } else {
      break;
    }
  }
  return p;
}

function advancePastSpacedSepRun(line: string, pos: number): { afterLastSep: number; vStart: number } {
  const len = line.length;
  let afterLastSep = pos;
  let p = pos;
  while (p < len) {
    const next = advancePastSepRun(line, p);
    if (next > p) {
      afterLastSep = next;
      p = next;
      continue;
    }
    if (isInlineWs(line[p])) {
      p += 1;
      continue;
    }
    break;
  }
  return { afterLastSep, vStart: p };
}

function advancePastAuthSchemeSep(line: string, sEnd: number): number {
  return advancePastSepRun(line, sEnd);
}

function isNonBase64KvParamAfterKey(line: string, keyStart: number, k: number): boolean {
  let eqPos = k;
  while (eqPos < line.length && isInlineWs(line[eqPos])) eqPos += 1;
  const afterSep = advancePastSepRun(line, eqPos);
  if (afterSep === eqPos) return false;
  const sepSlice = line.slice(eqPos, afterSep);
  if (!sepSlice.includes("=")) return false;
  if (eqPos === k && /^={1,2}$/.test(sepSlice)) {
    if (afterSep >= line.length || /[;)\]}]/.test(line[afterSep]!)) {
      return false;
    }
    if (isInlineWs(line[afterSep])) {
      const key = line.slice(keyStart, k);
      const totalLen = afterSep - keyStart;
      let nextPos = afterSep;
      while (nextPos < line.length && isInlineWs(line[nextPos])) nextPos += 1;
      let tokEnd = nextPos;
      while (
        tokEnd < line.length
        && !isInlineWs(line[tokEnd])
        && line[tokEnd] !== ","
        && line[tokEnd] !== ";"
        && line[tokEnd] !== "\r"
        && line[tokEnd] !== "\n"
      ) {
        tokEnd += 1;
      }
      let afterTok = tokEnd;
      while (afterTok < line.length && isInlineWs(line[afterTok])) afterTok += 1;
      const followedByComma = tokEnd > nextPos && line[afterTok] === ",";
      if (
        !followedByComma
        && totalLen >= 16
        && totalLen % 4 === 0
        && /^[A-Za-z0-9+/_-]+$/.test(key)
        && /[A-Z]/.test(key)
        && /[a-z]/.test(key)
        && /[0-9+/]/.test(key)
        && !isSecretKeyStem(key)
      ) {
        return false;
      }
    }
  }
  return true;
}

function isCustomAuthScheme(
  keyName: string,
  rawScheme: string,
  line: string,
  schemeEnd: number,
  allowSpaceInKey: boolean,
): boolean {
  if (!rawScheme || !isAsciiAlpha(rawScheme[0])) return false;
  if (!hasKeyStemSegment(keyName, AUTH_HEADER_KEY_STEMS, allowSpaceInKey)) return false;
  const delim = line[schemeEnd];
  const afterSep = advancePastSepRun(line, schemeEnd);
  const sepSlice = afterSep > schemeEnd ? line.slice(schemeEnd, afterSep) : "";
  const hasSchemeSep = sepSlice.includes(":") || sepSlice.includes(">");
  if (!isInlineWs(delim) && !hasSchemeSep && delim !== "(" && delim !== "[" && delim !== "{") return false;
  let next = hasSchemeSep ? afterSep : schemeEnd;
  while (next < line.length && isInlineWs(line[next])) next += 1;
  if (next >= line.length) return false;
  const nextCh = line[next]!;
  if (
    nextCh === "=" || nextCh === "," || nextCh === ";"
    || nextCh === "." || nextCh === "!" || nextCh === "?"
    || nextCh === ")" || nextCh === "]" || nextCh === "}"
  ) {
    return false;
  }
  if (DIAGNOSTIC_VOCAB.has(rawScheme)) return false;
  return true;
}

function matchByteUnitSuffixEnd(line: string, pos: number): number {
  const len = line.length;
  let p = pos;
  const wsLimit = Math.min(len, pos + 8);
  while (p < wsLimit && isInlineWs(line[p])) p += 1;
  if (p === pos || p >= len) return -1;
  let uEnd = p;
  const uLimit = Math.min(len, p + 8);
  while (uEnd < uLimit && isAsciiAlpha(line[uEnd])) uEnd += 1;
  if (uEnd === p) return -1;
  const unit = line.slice(p, uEnd);
  if (!SAFE_BYTE_UNITS.has(unit) && unit !== "byte" && unit !== "bytes") return -1;
  return uEnd === len || /[\s,.;:!?)\]/}]/.test(line[uEnd]!) ? uEnd : -1;
}

function isSqlPrefixChar(ch: string | undefined): boolean {
  return ch === "E" || ch === "e" || ch === "B" || ch === "b"
    || ch === "X" || ch === "x" || ch === "U" || ch === "u"
    || ch === "N" || ch === "n" || ch === "R" || ch === "r";
}

function detectSafeErrorHeaderEnd(line: string): number {
  const firstColon = line.indexOf(":");
  if (firstColon <= 0 || firstColon > 40) return -1;
  const headWords = line.slice(0, firstColon).trim().split(/\s+/);
  if (
    headWords.length >= 1
    && headWords.length <= 4
    && headWords.at(-1) === "Error"
    && headWords.every((w) => SAFE_HEADER_WORDS.has(w))
  ) {
    return firstColon;
  }
  return -1;
}

// Strip "\nLINE <digits>:..." SQL diagnostic tail in O(N) without regex backtracking.
function stripDiagnosticLineTail(raw: string): string {
  const len = raw.length;
  for (let i = 0; i < len; i += 1) {
    if (raw[i] === "\n" || raw[i] === "\r") {
      let j = i + 1;
      if (raw[i] === "\r" && raw[j] === "\n") j += 1;
      if (raw.startsWith("LINE ", j)) {
        let k = j + 5;
        while (k < len && isAsciiDigit(raw[k])) k += 1;
        if (k > j + 5 && raw[k] === ":") {
          let cut = i;
          while (cut > 0 && (raw[cut - 1] === "\n" || raw[cut - 1] === "\r")) cut -= 1;
          return raw.slice(0, cut);
        }
      }
    }
  }
  return raw;
}

// Redact $$...$$ and $tag$...$tag$ spans (including multiline and arbitrarily long tags) in strict O(N).
function redactDollarQuotedSpans(raw: string): string {
  const firstDollar = raw.indexOf("$");
  if (firstDollar === -1) return raw;
  const len = raw.length;
  const dollarPos: number[] = [];
  for (let p = firstDollar; p !== -1; p = raw.indexOf("$", p + 1)) {
    dollarPos.push(p);
  }
  if (dollarPos.length < 2) return raw;

  const pairCount = dollarPos.length - 1;
  const tagAtPair: (string | null)[] = new Array(pairCount);
  const tagPositions = new Map<string, number[]>();
  let hasAnyTag = false;
  for (let k = 0; k < pairCount; k += 1) {
    const start = dollarPos[k]! + 1;
    const end = dollarPos[k + 1]!;
    if (isValidDollarTagBody(raw, start, end)) {
      const tag = raw.slice(start - 1, end + 1);
      tagAtPair[k] = tag;
      hasAnyTag = true;
      let list = tagPositions.get(tag);
      if (!list) {
        list = [];
        tagPositions.set(tag, list);
      }
      list.push(k);
    } else {
      tagAtPair[k] = null;
    }
  }
  if (!hasAnyTag) return raw;

  const cursors = new Map<string, number>();
  const parts: string[] = [];
  let emittedUpTo = 0;
  let k = 0;
  while (k < pairCount) {
    const tag = tagAtPair[k];
    if (tag === null) {
      k += 1;
      continue;
    }
    const openStart = dollarPos[k]!;
    if (openStart > emittedUpTo) {
      parts.push(raw.slice(emittedUpTo, openStart));
    }
    const list = tagPositions.get(tag)!;
    let cur = cursors.get(tag) ?? 0;
    const minCloseK = k + 2;
    while (cur < list.length && list[cur]! < minCloseK) cur += 1;
    cursors.set(tag, cur);
    parts.push("[redacted]");
    if (cur >= list.length) {
      emittedUpTo = len;
      break;
    }
    const closeK = list[cur]!;
    emittedUpTo = dollarPos[closeK + 1]! + 1;
    k = closeK + 2;
  }
  if (emittedUpTo < len) {
    parts.push(raw.slice(emittedUpTo));
  }
  return parts.join("");
}

function parseSanitizedDuckDbType(s: string, pos: number): { text: string; end: number } | null {
  const len = s.length;
  let i = pos;
  const baseLimit = Math.min(len, pos + 24);
  while (i < baseLimit && (isAsciiAlpha(s[i]) || isAsciiDigit(s[i]) || s[i] === "_")) i += 1;
  if (i === pos) return null;
  if (i < len && (isAsciiAlpha(s[i]) || isAsciiDigit(s[i]) || s[i] === "_")) return null;
  const rawBase = s.slice(pos, i);
  const baseUpper = rawBase.toUpperCase();
  if (!DUCKDB_BASE_TYPES.has(baseUpper)) return null;
  let out = SAFE_MIXED_CASE_TYPES.has(rawBase) ? rawBase : baseUpper;

  if (i < len && s[i] === "(") {
    if (COMPOUND_DUCKDB_TYPES.has(baseUpper)) {
      let depth = 1;
      i += 1;
      while (i < len && depth > 0 && s[i] !== "\n" && s[i] !== "\r") {
        if (s[i] === "'" || s[i] === '"') {
          const q = s[i]!;
          i += 1;
          while (i < len && s[i] !== "\n" && s[i] !== "\r") {
            if (s[i] === q) {
              i += 1;
              if (s[i] === q) {
                i += 1;
                continue;
              }
              break;
            }
            i += 1;
          }
          continue;
        }
        if (s[i] === "(") depth += 1;
        else if (s[i] === ")") depth -= 1;
        i += 1;
      }
      if (depth !== 0) return null;
      out = `${out}([redacted])`;
    } else if (NUMERIC_PARAM_DUCKDB_TYPES.has(baseUpper)) {
      const paramStart = i;
      const paramLimit = Math.min(len, i + 26);
      i += 1;
      while (i < paramLimit && s[i] !== ")") {
        const c = s[i]!;
        if (!isAsciiDigit(c) && c !== "," && c !== " ") return null;
        i += 1;
      }
      if (i >= paramLimit || s[i] !== ")") return null;
      i += 1;
      out += s.slice(paramStart, i);
    } else {
      return null;
    }
  }

  let dims = 0;
  while (dims < 8 && i < len && s[i] === "[") {
    if (s[i + 1] === "]") {
      out += "[]";
      i += 2;
      dims += 1;
      continue;
    }
    let d = i + 1;
    const dLimit = Math.min(len, d + 6);
    while (d < dLimit && isAsciiDigit(s[d])) d += 1;
    if (d > i + 1 && d < len && s[d] === "]") {
      out += s.slice(i, d + 1);
      i = d + 1;
      dims += 1;
      continue;
    }
    break;
  }

  return { text: out, end: i };
}

function sanitizeCastingSuffix(tail: string): string {
  const trimmed = tail.trim();
  if (!trimmed) return "";
  if (trimmed.startsWith("when casting from source column ")) {
    return " when casting from source column [redacted]";
  }
  if (trimmed === "when casting" || trimmed.startsWith("when casting ")) {
    return " when casting [redacted]";
  }
  return "";
}

function sliceFirstLine(s: string): string {
  const nl = s.search(/\r|\n/);
  return nl === -1 ? s : s.slice(0, nl);
}

function isSafeDiagnosticPhrase(phrase: string): boolean {
  const trimmed = phrase.trim();
  if (!trimmed || trimmed.length > 64 || !/^[a-z]+(?: [a-z-]+)*$/.test(trimmed)) return false;
  for (const part of trimmed.split(/[\s-]+/)) {
    if (!part || !DIAGNOSTIC_VOCAB.has(part)) return false;
  }
  return true;
}

// Stage 1: Strictly header-anchored O(N) canonicalizer for DuckDB value/pattern/identifier templates.
function canonicalizeDuckDbTemplates(rawInput: string): { text: string; canonicalized: boolean } {
  if (rawInput.length > MAX_SCAN_INPUT_LEN) {
    const headLine = sliceFirstLine(rawInput.slice(0, 128));
    const hdrEnd = detectSafeErrorHeaderEnd(headLine);
    return {
      text: hdrEnd !== -1 ? `${headLine.slice(0, hdrEnd + 1)} [redacted]` : "[redacted]",
      canonicalized: true,
    };
  }

  const s = redactDollarQuotedSpans(stripDiagnosticLineTail(rawInput));
  const headLine = sliceFirstLine(s);
  const hdrColon = detectSafeErrorHeaderEnd(headLine);
  const hdrLower = hdrColon !== -1 ? headLine.slice(0, hdrColon).trim().toLowerCase() : "";
  let bodyStart = hdrColon !== -1 ? hdrColon + 1 : 0;
  while (bodyStart < s.length && isInlineWs(s[bodyStart])) {
    bodyStart += 1;
  }
  const prefix = s.slice(0, bodyStart);
  const bodyLine1 = headLine.slice(bodyStart);
  const bodyLine1Lower = bodyLine1.toLowerCase();
  let bodyEnd = s.length;
  while (bodyEnd > bodyStart && (s[bodyEnd - 1] === "\n" || s[bodyEnd - 1] === "\r")) {
    bodyEnd -= 1;
  }
  const sTrimEnd = bodyEnd === s.length ? s : s.slice(0, bodyEnd);

  if (s.startsWith("Malformed JSON", bodyStart)) {
    const inputIdx = headLine.indexOf("Input:", bodyStart + 14);
    if (inputIdx !== -1) {
      return { text: `${headLine.slice(0, inputIdx + 6)} [redacted]`, canonicalized: true };
    }
  }

  if (s.startsWith("Could not parse string ", bodyStart)) {
    const accMarker = " according to format specifier ";
    const accIdx = s.lastIndexOf(accMarker);
    if (accIdx > bodyStart + 23) {
      return { text: `${prefix}Could not parse string [redacted]${accMarker}[redacted]`, canonicalized: true };
    }
    return { text: `${prefix}Could not parse string [redacted]`, canonicalized: true };
  }

  if (s.startsWith("Failed to parse format specifier ", bodyStart)) {
    return { text: `${prefix}Failed to parse format specifier [redacted]`, canonicalized: true };
  }

  const cncstMarker = "Could not cast string to ";
  if (s.startsWith(cncstMarker, bodyStart)) {
    const parsed = parseSanitizedDuckDbType(sTrimEnd, bodyStart + cncstMarker.length);
    if (parsed && sTrimEnd[parsed.end] === ":") {
      return { text: `${prefix}${cncstMarker}${parsed.text}: [redacted]`, canonicalized: true };
    }
    return { text: `${prefix}${cncstMarker}[redacted]`, canonicalized: true };
  }

  const valueCastMarkers = [
    "Could not convert string ",
    "Could not cast value ",
    "Could not convert value ",
    "Could not convert bignum ",
  ] as const;
  let matchedCastMarker: string | null = null;
  for (const marker of valueCastMarkers) {
    if (s.startsWith(marker, bodyStart)) {
      matchedCastMarker = marker;
      break;
    }
  }
  if (
    matchedCastMarker === null
    && s.startsWith("Could not cast ", bodyStart)
    && !s.startsWith("Could not cast lambda expression ", bodyStart)
  ) {
    matchedCastMarker = "Could not cast ";
  }
  if (matchedCastMarker !== null) {
    const lastNl = Math.max(sTrimEnd.lastIndexOf("\n"), sTrimEnd.lastIndexOf("\r"));
    const finalLineStart = Math.max(bodyStart + matchedCastMarker.length, lastNl + 1);
    const toIdx = sTrimEnd.lastIndexOf(" to ");
    if (toIdx >= finalLineStart) {
      const parsed = parseSanitizedDuckDbType(sTrimEnd, toIdx + 4);
      if (parsed) {
        const afterLine = sTrimEnd.slice(parsed.end);
        if (afterLine === "" || afterLine.startsWith(" when casting") || /^[,.;:)\s]*$/.test(afterLine)) {
          return {
            text: `${prefix}${matchedCastMarker}[redacted] to ${parsed.text}${sanitizeCastingSuffix(afterLine)}`,
            canonicalized: true,
          };
        }
      }
    }
    return { text: `${prefix}${matchedCastMarker}[redacted] to [redacted]`, canonicalized: true };
  }

  if (s.startsWith("Could not convert DATE (", bodyStart)) {
    return { text: `${prefix}Could not convert DATE ([redacted])`, canonicalized: true };
  }

  const fcdvMarker = "Failed to cast decimal value ";
  if (s.startsWith(fcdvMarker, bodyStart)) {
    const lastNl = Math.max(sTrimEnd.lastIndexOf("\n"), sTrimEnd.lastIndexOf("\r"));
    const finalLineStart = Math.max(bodyStart + fcdvMarker.length, lastNl + 1);
    const toTypeMarker = " to type ";
    const toIdx = sTrimEnd.lastIndexOf(toTypeMarker);
    if (toIdx >= finalLineStart) {
      const parsed = parseSanitizedDuckDbType(sTrimEnd, toIdx + toTypeMarker.length);
      if (parsed) {
        const afterLine = sTrimEnd.slice(parsed.end);
        if (afterLine === "" || afterLine.startsWith(" when casting") || /^[,.;:)\s]*$/.test(afterLine)) {
          return {
            text: `${prefix}${fcdvMarker}[redacted]${toTypeMarker}${parsed.text}${sanitizeCastingSuffix(afterLine)}`,
            canonicalized: true,
          };
        }
      }
    }
    return { text: `${prefix}${fcdvMarker}[redacted] to type [redacted]`, canonicalized: true };
  }

  if (s.startsWith("Failed to cast value:", bodyStart)) {
    return { text: `${prefix}Failed to cast value: [redacted]`, canonicalized: true };
  }

  const cvMarker = "Casting value ";
  if (s.startsWith(cvMarker, bodyStart)) {
    const lastNl = Math.max(sTrimEnd.lastIndexOf("\n"), sTrimEnd.lastIndexOf("\r"));
    const finalLineStart = Math.max(bodyStart + cvMarker.length, lastNl + 1);
    const toTypeMarker = " to type ";
    const toIdx = sTrimEnd.lastIndexOf(toTypeMarker);
    if (toIdx >= finalLineStart) {
      const parsed = parseSanitizedDuckDbType(sTrimEnd, toIdx + toTypeMarker.length);
      if (parsed) {
        const afterLine = sTrimEnd.slice(parsed.end);
        if (afterLine.startsWith(" failed: value is out of range!")) {
          return {
            text: `${prefix}${cvMarker}[redacted]${toTypeMarker}${parsed.text} failed: value is out of range!`,
            canonicalized: true,
          };
        }
        if (afterLine === "" || afterLine.startsWith(" failed") || afterLine.startsWith(" when casting")) {
          return {
            text: `${prefix}${cvMarker}[redacted]${toTypeMarker}${parsed.text}${afterLine.startsWith(" failed") ? " failed" : sanitizeCastingSuffix(afterLine)}`,
            canonicalized: true,
          };
        }
      }
    }
    return { text: `${prefix}${cvMarker}[redacted] to type [redacted]`, canonicalized: true };
  }

  if (s.startsWith("Type ", bodyStart)) {
    const src = parseSanitizedDuckDbType(s, bodyStart + 5);
    if (src) {
      if (s.startsWith(" with value ", src.end)) {
        const cantIdx = s.lastIndexOf(" can't be cast ");
        if (cantIdx > src.end) {
          const destMarker = "destination type ";
          const destIdx = s.indexOf(destMarker, cantIdx);
          if (destIdx !== -1) {
            const bridge = s.slice(cantIdx, destIdx + destMarker.length);
            if (/^ can't be cast (?:to the |because the value is out of range for the )destination type $/.test(bridge)) {
              const dst = parseSanitizedDuckDbType(s, destIdx + destMarker.length);
              const afterLine = dst ? sliceFirstLine(s.slice(dst.end)) : "";
              return {
                text: `${prefix}Type ${src.text} with value [redacted]${bridge}${dst ? dst.text : "[redacted]"}${sanitizeCastingSuffix(afterLine)}`,
                canonicalized: true,
              };
            }
          }
        }
        return { text: `${prefix}Type ${src.text} with value [redacted]`, canonicalized: true };
      }
      if (s.startsWith(" can't be cast as ", src.end)) {
        const dst = parseSanitizedDuckDbType(s, src.end + 18);
        return {
          text: `${prefix}Type ${src.text} can't be cast as ${dst ? dst.text : "[redacted]"}`,
          canonicalized: true,
        };
      }
      if (s.startsWith(" can't be cast ", src.end)) {
        const destMarker = "destination type ";
        const destIdx = s.indexOf(destMarker, src.end);
        if (destIdx !== -1) {
          const dst = parseSanitizedDuckDbType(s, destIdx + destMarker.length);
          return {
            text: `${prefix}Type ${src.text} can't be cast to ${dst ? dst.text : "[redacted]"}`,
            canonicalized: true,
          };
        }
        return { text: `${prefix}Type ${src.text} can't be cast [redacted]`, canonicalized: true };
      }
    }
  }

  const unimplCastMarker = "Unimplemented type for cast (";
  if (s.startsWith(unimplCastMarker, bodyStart)) {
    const src = parseSanitizedDuckDbType(s, bodyStart + unimplCastMarker.length);
    if (src && s.startsWith(" -> ", src.end)) {
      const dst = parseSanitizedDuckDbType(s, src.end + 4);
      return {
        text: `${prefix}${unimplCastMarker}${src.text} -> ${dst ? dst.text : "[redacted]"})`,
        canonicalized: true,
      };
    }
    return { text: `${prefix}${unimplCastMarker}[redacted])`, canonicalized: true };
  }

  const ffRel = bodyLine1Lower.indexOf("field format:");
  const fvorRel = bodyLine1Lower.indexOf("field value out of range:");
  let fieldColonIdx = -1;
  if (ffRel >= 0 && ffRel <= 32 && (ffRel === 0 || isSafeDiagnosticPhrase(bodyLine1Lower.slice(0, ffRel)))) {
    fieldColonIdx = bodyStart + ffRel + 12;
  } else if (fvorRel >= 0 && fvorRel <= 32 && (fvorRel === 0 || isSafeDiagnosticPhrase(bodyLine1Lower.slice(0, fvorRel)))) {
    fieldColonIdx = bodyStart + fvorRel + 24;
  }
  if (fieldColonIdx !== -1) {
    const expMarker = ", expected format is (";
    const expIdx = s.lastIndexOf(expMarker);
    if (expIdx > fieldColonIdx) {
      const closeParen = s.indexOf(")", expIdx + expMarker.length);
      if (closeParen !== -1) {
        const fmtInner = s.slice(expIdx + expMarker.length, closeParen);
        const safeFmt = fmtInner === "YYYY-MM-DD" ? "(YYYY-MM-DD)" : "([redacted])";
        const afterParen = sliceFirstLine(s.slice(closeParen + 1));
        return {
          text: `${s.slice(0, fieldColonIdx + 1)} [redacted], expected format is ${safeFmt}${sanitizeCastingSuffix(afterParen)}`,
          canonicalized: true,
        };
      }
    }
    return { text: `${s.slice(0, fieldColonIdx + 1)} [redacted]`, canonicalized: true };
  }

  const cndsMarker = "Could not decode string ";
  if (s.startsWith(cndsMarker, bodyStart)) {
    const lastNl = Math.max(sTrimEnd.lastIndexOf("\n"), sTrimEnd.lastIndexOf("\r"));
    const finalLineStart = Math.max(bodyStart + cndsMarker.length, lastNl + 1);
    const asIdx = sTrimEnd.lastIndexOf(" as ");
    if (asIdx >= finalLineStart) {
      let k = asIdx + 4;
      const encLimit = Math.min(sTrimEnd.length, k + 16);
      while (k < encLimit && (isAsciiAlpha(sTrimEnd[k]) || isAsciiDigit(sTrimEnd[k]) || sTrimEnd[k] === "_")) k += 1;
      const enc = sTrimEnd.slice(asIdx + 4, k).toLowerCase();
      if (SAFE_DECODE_ENCODINGS.has(enc) && (k === sTrimEnd.length || sTrimEnd[k] === ":" || /\s/.test(sTrimEnd[k]!))) {
        return { text: `${prefix}${cndsMarker}[redacted] as ${enc}`, canonicalized: true };
      }
    }
    return { text: `${prefix}${cndsMarker}[redacted]`, canonicalized: true };
  }

  if (bodyLine1Lower.startsWith("invalid ")) {
    const lower = s.toLowerCase();
    let p = bodyStart + 8;
    let words = 0;
    let wordEnd = p;
    while (words < 3 && p < headLine.length) {
      while (p < headLine.length && s[p] === " ") p += 1;
      const ws = p;
      while (p < headLine.length && (isAsciiAlpha(s[p]) || s[p] === "_")) p += 1;
      if (p === ws || s[p] !== " ") break;
      const w = lower.slice(ws, p);
      if (w === "in" || w === "when" || w === "at" || w === "expected" || !DIAGNOSTIC_VOCAB.has(w)) break;
      words += 1;
      wordEnd = p;
    }
    if (words >= 1 && s[p] !== ":") {
      for (const anchor of [" in column", " when casting", " at position", " expected"]) {
        const aIdx = lower.lastIndexOf(anchor);
        if (aIdx > wordEnd) {
          const tailTrim = sliceFirstLine(s.slice(aIdx + anchor.length)).trim();
          const safeAnchorTail = anchor === " in column" && tailTrim
            ? " [redacted]"
            : anchor === " when casting" && tailTrim
              ? sanitizeCastingSuffix(`when casting ${tailTrim}`).slice(" when casting".length)
              : "";
          return {
            text: `${s.slice(0, wordEnd)} [redacted]${s.slice(aIdx, aIdx + anchor.length)}${safeAnchorTail}`,
            canonicalized: true,
          };
        }
      }
    }
  }

  for (const catalogObj of SAFE_CATALOG_OBJECTS) {
    const catPrefix = `${catalogObj} with name `;
    if (bodyLine1.startsWith(catPrefix)) {
      const dneMarker = " does not exist";
      const dneIdx = headLine.lastIndexOf(dneMarker);
      if (dneIdx > bodyStart + catPrefix.length) {
        const bang = headLine.startsWith(`${dneMarker}!`, dneIdx) ? "!" : "";
        return { text: `${prefix}${catPrefix}[redacted]${dneMarker}${bang}`, canonicalized: true };
      }
      return { text: `${prefix}${catPrefix}[redacted]`, canonicalized: true };
    }
  }

  if (s.startsWith("Overflow in ", bodyStart)) {
    if (s.startsWith("Overflow in division of ", bodyStart)) {
      return { text: `${prefix}Overflow in division of [redacted]`, canonicalized: true };
    }
    const colonIdx = headLine.indexOf(":", bodyStart + 12);
    const lp = headLine.indexOf("(", bodyStart + 12);
    if (colonIdx !== -1 && (lp === -1 || colonIdx < lp)) {
      return { text: `${headLine.slice(0, colonIdx + 1)} [redacted]`, canonicalized: true };
    }
    const rp = lp !== -1 ? headLine.lastIndexOf(")") : -1;
    if (lp !== -1) {
      const bang = rp > lp && headLine[rp + 1] === "!" ? "!" : "";
      return { text: `${headLine.slice(0, lp)}([redacted])${bang}`, canonicalized: true };
    }
  }

  if (s.startsWith("Left-shift value ", bodyStart)) {
    return { text: `${prefix}Left-shift value [redacted] is out of range`, canonicalized: true };
  }

  if (s.startsWith("Interval value ", bodyStart)) {
    return { text: `${prefix}Interval value [redacted] out of range`, canonicalized: true };
  }

  if (s.startsWith("input value ", bodyStart) && bodyLine1Lower.includes(" is out of range")) {
    return { text: `${prefix}input value [redacted] is out of range`, canonicalized: true };
  }

  const noFuncMarker = "No function matches the given name and argument types '";
  if (s.startsWith(noFuncMarker, bodyStart)) {
    const afterOpen = bodyStart + noFuncMarker.length;
    const closeDot = headLine.lastIndexOf("'.");
    const closeQuote = closeDot >= afterOpen ? closeDot : headLine.lastIndexOf("'");
    if (closeQuote >= afterOpen && headLine.slice(closeQuote).startsWith("'. You might need to add explicit type casts.")) {
      return {
        text: `${prefix}No function matches the given name and argument types [redacted]. You might need to add explicit type casts.`,
        canonicalized: true,
      };
    }
    return {
      text: `${prefix}No function matches the given name and argument types [redacted]`,
      canonicalized: true,
    };
  }

  if (s.startsWith("Referenced column ", bodyStart)) {
    const nfIdx = headLine.lastIndexOf(" not found in FROM clause");
    if (nfIdx > bodyStart + 18) {
      return { text: `${prefix}Referenced column [redacted] not found in FROM clause!`, canonicalized: true };
    }
    return { text: `${prefix}Referenced column [redacted] not found`, canonicalized: true };
  }

  if (s.startsWith("Ambiguous reference to column name ", bodyStart)) {
    return { text: `${prefix}Ambiguous reference to column name [redacted]`, canonicalized: true };
  }

  if (s.startsWith("Table ", bodyStart)) {
    const dnhIdx = headLine.lastIndexOf(" does not have a column named ");
    if (dnhIdx > bodyStart + 6) {
      return { text: `${prefix}Table [redacted] does not have a column named [redacted]`, canonicalized: true };
    }
  }

  if (s.startsWith("column ", bodyStart) || s.startsWith("Column ", bodyStart)) {
    const gbMarker = " must appear in the GROUP BY clause";
    const gbIdx = headLine.lastIndexOf(gbMarker);
    if (gbIdx > bodyStart + 7) {
      const hasAgg = headLine.slice(gbIdx).startsWith(`${gbMarker} or must be part of an aggregate function`);
      return {
        text: `${prefix}column [redacted]${gbMarker}${hasAgg ? " or must be part of an aggregate function." : ""}`,
        canonicalized: true,
      };
    }
  }

  if (s.startsWith("Duplicate key ", bodyStart)) {
    const violIdx = headLine.lastIndexOf(" violates ");
    if (violIdx > bodyStart + 14) {
      const rawConstraint = headLine.slice(violIdx + 10).trim();
      const hasDot = rawConstraint.endsWith(".");
      const normalized = (hasDot ? rawConstraint.slice(0, -1) : rawConstraint).toLowerCase();
      const safeConstraint = SAFE_CONSTRAINT_PHRASES.has(normalized)
        ? `${normalized}${hasDot ? "." : ""}`
        : "[redacted]";
      return {
        text: `${prefix}Duplicate key [redacted] violates ${safeConstraint}`,
        canonicalized: true,
      };
    }
    return { text: `${prefix}Duplicate key [redacted]`, canonicalized: true };
  }

  const noFilesMarker = "No files found that match the pattern ";
  if (s.startsWith(noFilesMarker, bodyStart)) {
    return { text: `${prefix}${noFilesMarker}[redacted]`, canonicalized: true };
  }

  const jsonPathMarker = "JSON path error near ";
  if (s.startsWith(jsonPathMarker, bodyStart)) {
    return { text: `${prefix}${jsonPathMarker}[redacted]`, canonicalized: true };
  }

  if (hdrLower === "" || hdrLower === "parser error") {
    const aonMarker = " at or near ";
    const aonIdx = headLine.indexOf(aonMarker, bodyStart);
    if (aonIdx > bodyStart && isSafeDiagnosticPhrase(headLine.slice(bodyStart, aonIdx))) {
      return { text: `${headLine.slice(0, aonIdx + aonMarker.length)}[redacted]`, canonicalized: true };
    }
  }

  return { text: s, canonicalized: false };
}

function matchSafeContractionLen(line: string, pos: number): number {
  if (pos > 0 && WORD_CHAR_RE.test(line[pos - 1]!)) return 0;
  let end = pos;
  const limit = Math.min(line.length, pos + 10);
  while (end < limit && (isAsciiAlpha(line[end]) || line[end] === "'")) end += 1;
  const tok = line.slice(pos, end);
  if (!tok.includes("'") || !SAFE_CONTRACTIONS.has(tok)) return 0;
  if (end < line.length && !/[\s,.;:!?)\]}]/.test(line[end]!)) return 0;
  return end - pos;
}

function redactPostColonDiagnosticTail(line1: string): string {
  const lower = line1.toLowerCase();
  let afterHeader = -1;
  if (lower.startsWith("invalid input error:")) afterHeader = 20;
  else if (lower.startsWith("conversion error:")) afterHeader = 17;
  else if (lower.startsWith("out of range error:")) afterHeader = 19;
  else if (lower.startsWith("regex error:")) afterHeader = 12;
  if (afterHeader === -1) return line1;
  for (let i = afterHeader; i < line1.length; i += 1) {
    if (line1.startsWith("[redacted]", i)) {
      i += 9;
      continue;
    }
    if (line1.startsWith("[url]", i)) {
      i += 4;
      continue;
    }
    const v6End = tryScanIpv6(line1, i);
    if (v6End !== -1) {
      i = v6End - 1;
      continue;
    }
    if (line1[i] === ":") {
      if (line1[i + 1] === ":") {
        i += 1;
        continue;
      }
      return `${line1.slice(0, i + 1)} [redacted]`;
    }
  }
  return line1;
}

function unescapeJsonKey(raw: string, isSqlUnicodeEscape = false): string {
  if (!raw.includes("\\")) return raw;
  let out = "";
  const len = raw.length;
  for (let i = 0; i < len; i += 1) {
    const c = raw[i]!;
    if (c === "\\" && i + 1 < len) {
      const next = raw[i + 1]!;
      if (isSqlUnicodeEscape) {
        if (next === "\\") {
          out += "\\";
          i += 1;
          continue;
        }
        if (
          next === "+"
          && i + 7 < len
          && isHexDigit(raw[i + 2]) && isHexDigit(raw[i + 3])
          && isHexDigit(raw[i + 4]) && isHexDigit(raw[i + 5])
          && isHexDigit(raw[i + 6]) && isHexDigit(raw[i + 7])
        ) {
          const cp = parseInt(raw.slice(i + 2, i + 8), 16);
          if (cp <= 0x10ffff) {
            out += String.fromCodePoint(cp);
            i += 7;
            continue;
          }
        }
        if (
          i + 4 < len
          && isHexDigit(raw[i + 1]) && isHexDigit(raw[i + 2])
          && isHexDigit(raw[i + 3]) && isHexDigit(raw[i + 4])
        ) {
          out += String.fromCharCode(parseInt(raw.slice(i + 1, i + 5), 16));
          i += 4;
          continue;
        }
        out += c;
        continue;
      }
      switch (next) {
        case "\\": out += "\\"; i += 1; continue;
        case '"': out += '"'; i += 1; continue;
        case "'": out += "'"; i += 1; continue;
        case "/": out += "/"; i += 1; continue;
        case "b": out += "\b"; i += 1; continue;
        case "f": out += "\f"; i += 1; continue;
        case "n": out += "\n"; i += 1; continue;
        case "r": out += "\r"; i += 1; continue;
        case "t": out += "\t"; i += 1; continue;
        case "x":
          if (i + 2 < len && isHexDigit(raw[i + 2])) {
            const hexEnd = i + 3 < len && isHexDigit(raw[i + 3]) ? i + 4 : i + 3;
            out += String.fromCharCode(parseInt(raw.slice(i + 2, hexEnd), 16));
            i = hexEnd - 1;
            continue;
          }
          break;
        case "u":
          if (
            i + 5 < len
            && isHexDigit(raw[i + 2]) && isHexDigit(raw[i + 3])
            && isHexDigit(raw[i + 4]) && isHexDigit(raw[i + 5])
          ) {
            out += String.fromCharCode(parseInt(raw.slice(i + 2, i + 6), 16));
            i += 5;
            continue;
          }
          break;
        case "U":
          if (
            i + 9 < len
            && isHexDigit(raw[i + 2]) && isHexDigit(raw[i + 3])
            && isHexDigit(raw[i + 4]) && isHexDigit(raw[i + 5])
            && isHexDigit(raw[i + 6]) && isHexDigit(raw[i + 7])
            && isHexDigit(raw[i + 8]) && isHexDigit(raw[i + 9])
          ) {
            const cp = parseInt(raw.slice(i + 2, i + 10), 16);
            if (cp <= 0x10ffff) {
              out += String.fromCodePoint(cp);
              i += 9;
              continue;
            }
          }
          break;
        default:
          if (next >= "0" && next <= "7") {
            let octEnd = i + 2;
            const octLimit = Math.min(len, i + 4);
            while (octEnd < octLimit && raw[octEnd]! >= "0" && raw[octEnd]! <= "7") {
              octEnd += 1;
            }
            const val = parseInt(raw.slice(i + 1, octEnd), 8);
            if (val <= 0xff) {
              out += String.fromCharCode(val);
              i = octEnd - 1;
              continue;
            }
          }
          break;
      }
    }
    out += c;
  }
  return out;
}

type QuoteEscapeState = {
  backslashEscapesQuotes: boolean;
  sawAmbiguousBackslashQuote: boolean;
};

function scanQuotedLiteralEnd(
  line: string,
  openIdx: number,
  q: string,
  escapeState?: QuoteEscapeState,
): number {
  const len = line.length;
  const useBackslash = escapeState ? escapeState.backslashEscapesQuotes : true;
  let k = openIdx + 1;
  let consecutiveBackslashes = 0;
  while (k < len) {
    const c = line[k];
    if (useBackslash) {
      if (c === "\\") {
        const nextCh = k + 1 < len ? line[k + 1] : "";
        if (nextCh === "\\" || nextCh === q) {
          k += 2;
          continue;
        }
        k += 1;
        continue;
      }
      if (c === q) {
        return k + 1;
      }
      k += 1;
    } else {
      if (c === "\\") {
        consecutiveBackslashes += 1;
        k += 1;
        continue;
      }
      if (c === q) {
        if ((consecutiveBackslashes & 1) === 1 && escapeState) {
          escapeState.sawAmbiguousBackslashQuote = true;
        }
        consecutiveBackslashes = 0;
        if (k + 1 < len && line[k + 1] === q) {
          k += 2;
          continue;
        }
        return k + 1;
      }
      consecutiveBackslashes = 0;
      k += 1;
    }
  }
  return len;
}

function scanJsonValueEnd(line: string, p: number, escapeState?: QuoteEscapeState): number {
  const len = line.length;
  if (p >= len || line.startsWith("[url]", p)) return p;
  let k = p;
  let sawOnlySimpleNumber = true;
  while (k < len && !/[\s,}\]);]/.test(line[k]!)) {
    if (line.startsWith("[url]", k)) break;
    if (line.startsWith("[redacted]", k)) {
      sawOnlySimpleNumber = false;
      k += 10;
      continue;
    }
    const ch = line[k]!;
    if (ch === '"' || ch === "'" || ch === "`") {
      sawOnlySimpleNumber = false;
      k = scanQuotedLiteralEnd(line, k, ch, escapeState);
      continue;
    }
    if (ch === "{" || ch === "[" || ch === "(") {
      sawOnlySimpleNumber = false;
      k = scanBalancedGroupEnd(line, k, true, escapeState);
      continue;
    }
    if (!isAsciiDigit(ch) && ch !== ".") sawOnlySimpleNumber = false;
    k += 1;
  }
  if (sawOnlySimpleNumber && k > p && /^\d{1,6}(?:\.\d{1,3})?$/.test(line.slice(p, k))) {
    const unitEnd = matchByteUnitSuffixEnd(line, k);
    if (unitEnd !== -1) return unitEnd;
  }
  return k;
}

function tryScanRawUrlEnd(line: string, pos: number): number {
  const len = line.length;
  if (pos >= len || !isAsciiAlpha(line[pos])) return -1;
  let j = pos + 1;
  const schemeLimit = Math.min(len, pos + MAX_URL_SCHEME_LEN);
  while (
    j < schemeLimit
    && (isAsciiAlpha(line[j]) || isAsciiDigit(line[j]) || line[j] === "+" || line[j] === "." || line[j] === "-")
  ) {
    j += 1;
  }
  if (!line.startsWith("://", j)) return -1;
  let k = j + 3;
  while (k < len && !/[\s'"`]/.test(line[k]!)) k += 1;
  while (k > j + 3 && (line[k - 1] === "," || line[k - 1] === ";")) k -= 1;
  return k;
}

function consumeJsonSecretValueAfterQuote(
  line1: string,
  pos: number,
  keyCandidate: string,
  escapeState?: QuoteEscapeState,
  isSqlUnicodeEscape = false,
): number {
  const len = line1.length;
  let p = pos;
  while (p < len && isInlineWs(line1[p])) p += 1;
  const afterSep = advancePastSepRun(line1, p);
  if (afterSep === p) return pos;
  const decodedKey = unescapeJsonKey(keyCandidate, isSqlUnicodeEscape);
  if (!isSecretJsonKey(decodedKey)) return pos;
  const decodedKeyLower = decodedKey.toLowerCase();
  const keyAuthScheme = AUTH_SCHEMES.has(decodedKeyLower) ? decodedKeyLower : "";
  p = afterSep;
  while (p < len && isInlineWs(line1[p])) p += 1;
  if (
    p >= len
    || (line1.startsWith("[url]", p) && !isCookieKey(decodedKey, true) && !isMultiParamAuthHeader(decodedKey, keyAuthScheme, true))
  ) {
    return pos;
  }

  const firstCh = line1[p]!;
  const startsQuotedOrGroup = firstCh === '"' || firstCh === "'" || firstCh === "`"
    || firstCh === "{" || firstCh === "[" || firstCh === "(";
  let matchedScheme = keyAuthScheme;
  let matchedValueScheme = false;
  let valStart = p;
  if (!startsQuotedOrGroup && !line1.startsWith("[redacted]", p) && !line1.startsWith("[url]", p)) {
    const sEnd = scanAuthSchemeTokenEnd(line1, p);
    const rawScheme = line1.slice(p, sEnd);
    const maybeScheme = rawScheme.toLowerCase();
    const afterSchemeSep = advancePastAuthSchemeSep(line1, sEnd);
    const hasSchemeSep = afterSchemeSep > sEnd;
    const isKnownScheme = AUTH_SCHEMES.has(maybeScheme)
      && (hasSchemeSep || isInlineWs(line1[sEnd]) || line1[sEnd] === "(" || line1[sEnd] === "[" || line1[sEnd] === "{");
    const isCustomScheme = !isKnownScheme && isCustomAuthScheme(decodedKey, rawScheme, line1, sEnd, true);
    if (isKnownScheme || isCustomScheme) {
      matchedScheme = maybeScheme;
      matchedValueScheme = true;
      valStart = afterSchemeSep;
      while (valStart < len && isInlineWs(line1[valStart])) valStart += 1;
      if (isCustomScheme && (line1[valStart] === ":" || line1[valStart] === "=") && !line1.startsWith("://", valStart)) {
        valStart = advancePastSepRun(line1, valStart);
        while (valStart < len && isInlineWs(line1[valStart])) valStart += 1;
      }
      if (valStart >= len) return Math.max(afterSchemeSep, valStart);
      if (
        (line1.startsWith("[url]", valStart) || tryScanRawUrlEnd(line1, valStart) !== -1)
        && !isCookieKey(decodedKey, true)
        && !isMultiParamAuthHeader(decodedKey, matchedScheme, true)
      ) {
        return afterSchemeSep;
      }
    }
  }

  const cookieMode = isCookieKey(decodedKey, true);
  const authParamMode = isMultiParamAuthHeader(decodedKey, matchedScheme, true);

  let valEnd: number;
  if (cookieMode || authParamMode) {
    valEnd = scanSecretHeaderOrValueEnd(line1, valStart, decodedKey, matchedScheme, true, true, escapeState);
  } else if (line1[valStart] === "=" || (line1[valStart] === ":" && !line1.startsWith("://", valStart))) {
    const { afterLastSep, vStart } = advancePastSpacedSepRun(line1, valStart);
    valEnd = scanJsonValueEnd(line1, vStart, escapeState);
    if (valEnd === vStart) valEnd = afterLastSep;
    if (matchedScheme !== "" && valEnd > valStart) {
      valEnd = scanContinuationParamsEnd(line1, valEnd, false, true, true, escapeState);
    }
  } else if (matchedScheme !== "") {
    const { end: k, keyLower } = scanParamKeyEnd(line1, valStart, true, escapeState);
    const kvEnd = k > valStart && isNonBase64KvParamAfterKey(line1, valStart, k)
      ? tryFinishKeyValueParamEnd(line1, k, keyLower, false, true, escapeState)
      : -1;
    valEnd = kvEnd !== -1
      ? scanContinuationParamsEnd(line1, kvEnd, false, true, true, escapeState)
      : scanJsonValueEnd(line1, valStart, escapeState);
  } else {
    valEnd = scanJsonValueEnd(line1, valStart, escapeState);
  }

  if (valEnd > valStart) return valEnd;
  return matchedValueScheme ? valStart : pos;
}

type QuoteRedactSpan = {
  start: number;
  end: number;
  replacement: "[redacted]" | "[url]";
};

type QuoteScanResult = {
  spans: QuoteRedactSpan[];
  scanEnd: number;
  sawAmbiguousBackslashQuote: boolean;
};

function scanQuotesAndUrlsPass(
  line1: string,
  limit: number,
  backslashEscapesQuotes: boolean,
): QuoteScanResult {
  const spans: QuoteRedactSpan[] = [];
  let outChars = 0;
  let i = 0;
  let sawAmbiguousBackslashQuote = false;
  while (i < limit) {
    if (outChars >= MESSAGE_LIMIT + 128) break;
    if (line1.startsWith("[redacted]", i)) {
      outChars += 10;
      i += 10;
      continue;
    }
    if (line1.startsWith("[url]", i)) {
      outChars += 5;
      i += 5;
      continue;
    }
    const contractionLen = matchSafeContractionLen(line1, i);
    if (contractionLen > 0) {
      outChars += contractionLen;
      i += contractionLen;
      continue;
    }
    if (isAsciiAlpha(line1[i]) && (i === 0 || !WORD_CHAR_RE.test(line1[i - 1]!))) {
      let j = i + 1;
      const schemeLimit = Math.min(limit, i + MAX_URL_SCHEME_LEN);
      while (
        j < schemeLimit
        && (isAsciiAlpha(line1[j]) || isAsciiDigit(line1[j]) || line1[j] === "+" || line1[j] === "." || line1[j] === "-")
      ) {
        j += 1;
      }
      if (j + 2 < limit && line1.startsWith("://", j)) {
        let k = j + 3;
        while (k < limit && !/[\s'"`]/.test(line1[k]!)) k += 1;
        while (k > j + 3 && (line1[k - 1] === "," || line1[k - 1] === ";")) k -= 1;
        spans.push({ start: i, end: k, replacement: "[url]" });
        outChars += 5;
        i = k;
        continue;
      }
    }
    const prevWord = i > 0 && WORD_CHAR_RE.test(line1[i - 1]!);
    const typedLen = prevWord
      ? 0
      : (line1[i] === "U" || line1[i] === "u") && line1[i + 1] === "&" && line1[i + 2] === "'" && i + 2 < limit
        ? 3
        : isSqlPrefixChar(line1[i]) && line1[i + 1] === "'" && i + 1 < limit
          ? 2
          : 0;
    if (typedLen > 0 || line1[i] === "'") {
      const spanStart = i;
      const firstCh = line1[i];
      const isEscapeSingle = typedLen > 0 && (firstCh === "E" || firstCh === "e" || firstCh === "U" || firstCh === "u");
      const useBackslashEscape = isEscapeSingle || backslashEscapesQuotes;
      i += typedLen > 0 ? typedLen : 1;
      const contentStart = i;
      let contentEnd = -1;
      let consecutiveBackslashes = 0;
      while (i < limit) {
        const ch = line1[i];
        if (useBackslashEscape) {
          if (ch === "\\") {
            const nextCh = i + 1 < limit ? line1[i + 1] : "";
            if (nextCh === "\\" || nextCh === "'") {
              i += 2;
              continue;
            }
            i += 1;
            continue;
          }
          if (!isEscapeSingle) {
            const cLen = matchSafeContractionLen(line1, i);
            if (cLen > 0) {
              i += cLen;
              continue;
            }
          }
          if (ch === "'") {
            if (isEscapeSingle && i + 1 < limit && line1[i + 1] === "'") {
              i += 2;
              continue;
            }
            contentEnd = i;
            i += 1;
            break;
          }
          i += 1;
        } else {
          if (ch === "\\") {
            consecutiveBackslashes += 1;
            i += 1;
            continue;
          }
          if (ch === "'") {
            if ((consecutiveBackslashes & 1) === 1) {
              sawAmbiguousBackslashQuote = true;
            }
            consecutiveBackslashes = 0;
            if (i + 1 < limit && line1[i + 1] === "'") {
              i += 2;
              continue;
            }
            contentEnd = i;
            i += 1;
            break;
          }
          consecutiveBackslashes = 0;
          i += 1;
        }
      }
      if (i > limit) i = limit;
      if (contentEnd > contentStart && i < limit) {
        const subState: QuoteEscapeState = { backslashEscapesQuotes, sawAmbiguousBackslashQuote };
        i = Math.min(
          limit,
          consumeJsonSecretValueAfterQuote(line1, i, line1.slice(contentStart, contentEnd), subState, typedLen === 3),
        );
        sawAmbiguousBackslashQuote = subState.sawAmbiguousBackslashQuote;
      }
      spans.push({ start: spanStart, end: i, replacement: "[redacted]" });
      outChars += 10;
      continue;
    }
    if (line1[i] === '"' || line1[i] === "`") {
      const spanStart = i;
      const q = line1[i]!;
      i += 1;
      const contentStart = i;
      let contentEnd = -1;
      let consecutiveBackslashes = 0;
      while (i < limit) {
        const ch = line1[i];
        if (backslashEscapesQuotes) {
          if (ch === "\\") {
            i += 2;
            continue;
          }
          if (ch === q) {
            contentEnd = i;
            i += 1;
            break;
          }
          i += 1;
        } else {
          if (ch === "\\") {
            consecutiveBackslashes += 1;
            i += 1;
            continue;
          }
          if (ch === q) {
            if ((consecutiveBackslashes & 1) === 1) {
              sawAmbiguousBackslashQuote = true;
            }
            consecutiveBackslashes = 0;
            if (i + 1 < limit && line1[i + 1] === q) {
              i += 2;
              continue;
            }
            contentEnd = i;
            i += 1;
            break;
          }
          consecutiveBackslashes = 0;
          i += 1;
        }
      }
      if (i > limit) i = limit;
      if (contentEnd > contentStart && i < limit) {
        const subState: QuoteEscapeState = { backslashEscapesQuotes, sawAmbiguousBackslashQuote };
        i = Math.min(limit, consumeJsonSecretValueAfterQuote(line1, i, line1.slice(contentStart, contentEnd), subState));
        sawAmbiguousBackslashQuote = subState.sawAmbiguousBackslashQuote;
      }
      spans.push({ start: spanStart, end: i, replacement: "[redacted]" });
      outChars += 10;
      continue;
    }
    outChars += 1;
    i += 1;
  }
  return { spans, scanEnd: Math.min(i, limit), sawAmbiguousBackslashQuote };
}

function mergeQuoteScanSpans(
  spansA: readonly QuoteRedactSpan[],
  spansB: readonly QuoteRedactSpan[],
  scanEnd: number,
): QuoteRedactSpan[] {
  const mergedRedacted: QuoteRedactSpan[] = [];
  let ia = 0;
  let ib = 0;
  while (ia < spansA.length || ib < spansB.length) {
    while (ia < spansA.length && (spansA[ia]!.replacement !== "[redacted]" || spansA[ia]!.start >= scanEnd)) ia += 1;
    while (ib < spansB.length && (spansB[ib]!.replacement !== "[redacted]" || spansB[ib]!.start >= scanEnd)) ib += 1;
    if (ia >= spansA.length && ib >= spansB.length) break;
    const takeA = ib >= spansB.length || (ia < spansA.length && spansA[ia]!.start <= spansB[ib]!.start);
    const src = takeA ? spansA[ia++]! : spansB[ib++]!;
    const end = Math.min(src.end, scanEnd);
    const prev = mergedRedacted.at(-1);
    if (prev && src.start <= prev.end) {
      if (end > prev.end) prev.end = end;
    } else {
      mergedRedacted.push({ start: src.start, end, replacement: "[redacted]" });
    }
  }

  const keptUrls: QuoteRedactSpan[] = [];
  let ua = 0;
  let ub = 0;
  let ri = 0;
  while (ua < spansA.length || ub < spansB.length) {
    while (ua < spansA.length && (spansA[ua]!.replacement !== "[url]" || spansA[ua]!.start >= scanEnd)) ua += 1;
    while (ub < spansB.length && (spansB[ub]!.replacement !== "[url]" || spansB[ub]!.start >= scanEnd)) ub += 1;
    if (ua >= spansA.length && ub >= spansB.length) break;
    const takeA = ub >= spansB.length || (ua < spansA.length && spansA[ua]!.start <= spansB[ub]!.start);
    const src = takeA ? spansA[ua++]! : spansB[ub++]!;
    const end = Math.min(src.end, scanEnd);
    while (ri < mergedRedacted.length && mergedRedacted[ri]!.end <= src.start) ri += 1;
    if (ri < mergedRedacted.length && mergedRedacted[ri]!.start < end) continue;
    const prevUrl = keptUrls.at(-1);
    if (prevUrl && src.start <= prevUrl.end) {
      if (end > prevUrl.end) prevUrl.end = end;
    } else {
      keptUrls.push({ start: src.start, end, replacement: "[url]" });
    }
  }

  if (keptUrls.length === 0) return mergedRedacted;
  const combined: QuoteRedactSpan[] = [];
  let rIdx = 0;
  let uIdx = 0;
  while (rIdx < mergedRedacted.length || uIdx < keptUrls.length) {
    if (uIdx >= keptUrls.length || (rIdx < mergedRedacted.length && mergedRedacted[rIdx]!.start <= keptUrls[uIdx]!.start)) {
      combined.push(mergedRedacted[rIdx++]!);
    } else {
      combined.push(keptUrls[uIdx++]!);
    }
  }
  return combined;
}

function emitSpansForLine(line1: string, spans: readonly QuoteRedactSpan[], scanEnd: number): string {
  const truncated = scanEnd < line1.length;
  let emitEnd = scanEnd;
  const lastSpan = spans.at(-1);
  let needTailRedacted = false;
  if (truncated && (!lastSpan || lastSpan.end < scanEnd)) {
    const floor = lastSpan ? lastSpan.end : 0;
    if (scanEnd > floor && !/\s/.test(line1[scanEnd - 1]!) && !/\s/.test(line1[scanEnd]!)) {
      while (emitEnd > floor && !/\s/.test(line1[emitEnd - 1]!)) emitEnd -= 1;
    }
    needTailRedacted = true;
  }
  if (spans.length === 0 && !needTailRedacted) {
    return emitEnd === line1.length ? line1 : line1.slice(0, emitEnd);
  }
  const parts: string[] = [];
  let cursor = 0;
  for (const span of spans) {
    if (span.start > cursor) {
      parts.push(line1.slice(cursor, span.start));
    }
    parts.push(span.replacement);
    cursor = span.end;
  }
  if (cursor < emitEnd) {
    parts.push(line1.slice(cursor, emitEnd));
  }
  if (needTailRedacted) {
    parts.push("[redacted]");
  }
  return parts.join("");
}

// Stage 2A: O(N) strict pair-by-pair quote & URL scanner (closed contraction allowlist only).
// When a non-E quoted literal contains an odd run of backslashes before its closing quote,
// union the SQL-doubling pass and the C/JSON backslash-escape pass so neither dialect can leak.
function redactQuotesAndUrlsLine(line1: string): string {
  const len = line1.length;
  const passA = scanQuotesAndUrlsPass(line1, len, false);
  if (!passA.sawAmbiguousBackslashQuote) {
    return emitSpansForLine(line1, passA.spans, passA.scanEnd);
  }
  const passB = scanQuotesAndUrlsPass(line1, passA.scanEnd, true);
  const scanEnd = Math.min(passA.scanEnd, passB.scanEnd);
  const merged = mergeQuoteScanSpans(passA.spans, passB.spans, scanEnd);
  return emitSpansForLine(line1, merged, scanEnd);
}

function tryScanIpv4(s: string, pos: number): number {
  if (pos > 0 && (isAsciiDigit(s[pos - 1]) || s[pos - 1] === ".")) return -1;
  let i = pos;
  for (let oct = 0; oct < 4; oct += 1) {
    if (oct > 0) {
      if (s[i] !== ".") return -1;
      i += 1;
    }
    const start = i;
    while (i < s.length && isAsciiDigit(s[i]) && i - start < 3) i += 1;
    if (i === start) return -1;
    if (i - start > 1 && s[start] === "0") return -1;
    const val = Number(s.slice(start, i));
    if (val > 255) return -1;
  }
  if (i < s.length && (isAsciiDigit(s[i]) || s[i] === ".")) return -1;
  return i;
}

function tryScanIpv6(s: string, pos: number): number {
  if (pos > 0 && (isAsciiAlpha(s[pos - 1]) || isAsciiDigit(s[pos - 1]))) return -1;
  if (pos > 0 && s[pos - 1] === ":" && s[pos] !== ":") return -1;
  const len = s.length;
  let i = pos;
  let groups = 0;
  let hasDoubleColon = false;
  let embeddedIpv4 = false;

  if (s.startsWith("::", i)) {
    hasDoubleColon = true;
    i += 2;
  }

  while (i < len) {
    if ((hasDoubleColon || groups === 6) && isAsciiDigit(s[i])) {
      const v4End = tryScanIpv4(s, i);
      if (v4End !== -1 && (v4End === len || !isHexDigit(s[v4End]))) {
        i = v4End;
        embeddedIpv4 = true;
        break;
      }
    }
    const gStart = i;
    while (i < len && isHexDigit(s[i]) && i - gStart < 4) i += 1;
    const gLen = i - gStart;
    if (gLen === 0) break;
    if (i < len && (isAsciiAlpha(s[i]) || isAsciiDigit(s[i]))) return -1;
    groups += 1;

    if (s.startsWith("::", i)) {
      if (hasDoubleColon) break;
      hasDoubleColon = true;
      i += 2;
      continue;
    }
    if (s[i] === ":" && i + 1 < len && isHexDigit(s[i + 1])) {
      if (!hasDoubleColon && groups === 8) break;
      i += 1;
      continue;
    }
    break;
  }

  const validGroups = embeddedIpv4
    ? (hasDoubleColon ? groups <= 5 : groups === 6)
    : (hasDoubleColon ? groups >= 1 && groups <= 7 : groups === 8);
  if (!validGroups) return -1;

  if (s[i] === "%" && i + 1 < len && /[0-9A-Za-z._-]/.test(s[i + 1]!)) {
    i += 1;
    while (i < len && /[0-9A-Za-z._-]/.test(s[i]!)) i += 1;
  }

  if (s[i] === ":" && i + 1 < len && isAsciiDigit(s[i + 1])) {
    let pEnd = i + 1;
    while (pEnd < len && isAsciiDigit(s[pEnd]) && pEnd - (i + 1) < 5) pEnd += 1;
    const portVal = Number(s.slice(i + 1, pEnd));
    if (
      portVal >= 1
      && portVal <= 65535
      && (pEnd === len || (!isAsciiAlpha(s[pEnd]) && !isAsciiDigit(s[pEnd]) && s[pEnd] !== ":"))
    ) {
      i = pEnd;
    }
  }

  if (i < len && (isAsciiAlpha(s[i]) || isAsciiDigit(s[i]))) return -1;
  if (s[i] === ":" && i + 1 < len && (isAsciiAlpha(s[i + 1]) || isAsciiDigit(s[i + 1]) || s[i + 1] === ":")) {
    return -1;
  }
  return i;
}

function isSecretKeyStem(word: string): boolean {
  return hasKeyStemSegment(word, SECRET_STEMS, false);
}

function isCookieKey(word: string, allowSpace = false): boolean {
  return hasKeyStemSegment(word, COOKIE_STEMS, allowSpace);
}

function isMultiParamAuthHeader(keyName: string, authScheme: string, allowSpace = false): boolean {
  if (MULTI_PARAM_AUTH_SCHEMES.has(authScheme)) return true;
  if (hasKeyStemSegment(keyName, AUTHENTICATE_KEY_STEMS, allowSpace)) return true;
  if (authScheme === "basic" || authScheme === "bearer") return false;
  return hasKeyStemSegment(keyName, MULTI_PARAM_AUTH_KEY_STEMS, allowSpace);
}

type BalancedGroupCache = {
  line: string;
  allowQuoted: boolean;
  backslashEscapesQuotes: boolean;
  paren: Map<number, number>;
  bracket: Map<number, number>;
  brace: Map<number, number>;
  lastCloseParen: number;
  lastCloseBracket: number;
  lastCloseBrace: number;
  ambig: Set<number>;
};

let balancedGroupCache: BalancedGroupCache | null = null;

function getBalancedGroupCache(
  line: string,
  allowQuoted: boolean,
  backslashEscapesQuotes: boolean,
): BalancedGroupCache {
  if (
    balancedGroupCache !== null
    && balancedGroupCache.line === line
    && balancedGroupCache.allowQuoted === allowQuoted
    && balancedGroupCache.backslashEscapesQuotes === backslashEscapesQuotes
  ) {
    return balancedGroupCache;
  }
  balancedGroupCache = {
    line,
    allowQuoted,
    backslashEscapesQuotes,
    paren: new Map(),
    bracket: new Map(),
    brace: new Map(),
    lastCloseParen: -2,
    lastCloseBracket: -2,
    lastCloseBrace: -2,
    ambig: new Set(),
  };
  return balancedGroupCache;
}

function scanBalancedGroupEnd(
  line: string,
  p: number,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
): number {
  const len = line.length;
  const openCh = line[p];
  const closeCh = openCh === "(" ? ")" : openCh === "[" ? "]" : openCh === "{" ? "}" : "";
  if (!closeCh) return p;

  const useBackslash = escapeState ? escapeState.backslashEscapesQuotes : true;
  const cache = getBalancedGroupCache(line, allowQuoted, useBackslash);
  const map = openCh === "(" ? cache.paren : openCh === "[" ? cache.bracket : cache.brace;

  const cached = map.get(p);
  if (cached !== undefined) {
    if (escapeState && !escapeState.backslashEscapesQuotes && cache.ambig.has(p)) {
      escapeState.sawAmbiguousBackslashQuote = true;
    }
    return cached;
  }

  let lastClose = openCh === "("
    ? cache.lastCloseParen
    : openCh === "["
      ? cache.lastCloseBracket
      : cache.lastCloseBrace;
  if (lastClose === -2) {
    lastClose = line.lastIndexOf(closeCh);
    if (openCh === "(") cache.lastCloseParen = lastClose;
    else if (openCh === "[") cache.lastCloseBracket = lastClose;
    else cache.lastCloseBrace = lastClose;
  }
  if (lastClose <= p) {
    map.set(p, len);
    return len;
  }

  const trackAmbig = allowQuoted && Boolean(escapeState && !escapeState.backslashEscapesQuotes);
  const stack: number[] = [p];
  const ambigStack: boolean[] = [false];
  let k = p + 1;
  while (k < len && stack.length > 0) {
    if (allowQuoted && (line[k] === '"' || line[k] === "'" || line[k] === "`")) {
      if (trackAmbig) {
        const localState: QuoteEscapeState = {
          backslashEscapesQuotes: false,
          sawAmbiguousBackslashQuote: false,
        };
        k = scanQuotedLiteralEnd(line, k, line[k]!, localState);
        if (localState.sawAmbiguousBackslashQuote) {
          escapeState!.sawAmbiguousBackslashQuote = true;
          ambigStack[ambigStack.length - 1] = true;
        }
      } else {
        k = scanQuotedLiteralEnd(line, k, line[k]!, escapeState);
      }
      continue;
    }
    if (line.startsWith("[redacted]", k)) {
      k += 10;
      continue;
    }
    if (line.startsWith("[url]", k)) {
      k += 5;
      continue;
    }
    if (line[k] === openCh) {
      stack.push(k);
      ambigStack.push(false);
    } else if (line[k] === closeCh) {
      const openIdx = stack.pop()!;
      const frameAmbig = ambigStack.pop()!;
      map.set(openIdx, k + 1);
      if (frameAmbig) {
        cache.ambig.add(openIdx);
        if (ambigStack.length > 0) ambigStack[ambigStack.length - 1] = true;
      }
    }
    k += 1;
  }

  let carryAmbig = false;
  while (stack.length > 0) {
    const openIdx = stack.pop()!;
    carryAmbig = ambigStack.pop()! || carryAmbig;
    map.set(openIdx, len);
    if (carryAmbig) cache.ambig.add(openIdx);
  }

  return map.get(p) ?? k;
}

function scanSingleSecretValueEnd(
  line: string,
  p: number,
  allowRedacted = false,
  allowUrl = false,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
): number {
  const len = line.length;
  if (p >= len) return p;
  if (line.startsWith("[url]", p) && !allowUrl) return p;
  if (line.startsWith("[redacted]", p) && !allowRedacted) return p;
  let k = p;
  let sawOnlySimpleNumber = true;
  while (k < len && !/[\s,;)\]}]/.test(line[k]!)) {
    if (line.startsWith("[url]", k)) {
      if (allowUrl) {
        sawOnlySimpleNumber = false;
        k += 5;
        continue;
      }
      break;
    }
    if (line.startsWith("[redacted]", k)) {
      if (allowRedacted) {
        sawOnlySimpleNumber = false;
        k += 10;
        continue;
      }
      break;
    }
    if (allowQuoted && (line[k] === '"' || line[k] === "'" || line[k] === "`")) {
      sawOnlySimpleNumber = false;
      k = scanQuotedLiteralEnd(line, k, line[k]!, escapeState);
      continue;
    }
    if (line[k] === "(" || line[k] === "[" || line[k] === "{") {
      sawOnlySimpleNumber = false;
      k = scanBalancedGroupEnd(line, k, allowQuoted, escapeState);
      continue;
    }
    if (!isAsciiDigit(line[k]) && line[k] !== ".") sawOnlySimpleNumber = false;
    k += 1;
  }
  if (sawOnlySimpleNumber && k > p && /^\d{1,6}(?:\.\d{1,3})?$/.test(line.slice(p, k))) {
    const unitEnd = matchByteUnitSuffixEnd(line, k);
    if (unitEnd !== -1) return unitEnd;
  }
  return k;
}

function consumeChainedUrlEquals(
  line: string,
  segStart: number,
  segEnd: number,
  isCookie: boolean,
  allowQuoted: boolean,
  escapeState?: QuoteEscapeState,
): number {
  const len = line.length;
  let curStart = segStart;
  let curEnd = segEnd;
  while (curEnd > curStart) {
    const slice = line.slice(curStart, curEnd);
    const hasUrl = slice.includes("[url]") || (allowQuoted && slice.includes("://"));
    if (!hasUrl) break;
    let afterUrl = curEnd;
    while (afterUrl < len && isInlineWs(line[afterUrl])) afterUrl += 1;
    if (line[afterUrl] !== "=") break;
    let val2Start = afterUrl + 1;
    while (val2Start < len && (isInlineWs(line[val2Start]) || line[val2Start] === "=")) val2Start += 1;
    const val2End = scanSingleSecretValueEnd(line, val2Start, true, true, allowQuoted, escapeState);
    if (val2End > val2Start) {
      curStart = val2Start;
      curEnd = val2End;
    } else {
      curEnd = val2Start;
      break;
    }
  }
  return curEnd;
}

function scanParamKeyEnd(
  line: string,
  p: number,
  allowQuoted: boolean,
  escapeState?: QuoteEscapeState,
): { end: number; keyLower: string; keySegments: number } {
  const len = line.length;
  let k = p;
  let keyLower = "";
  let keySegments = 0;
  while (k < len) {
    if (line.startsWith("[redacted]", k)) {
      k += 10;
      keySegments += 1;
      continue;
    }
    if (line.startsWith("[url]", k)) {
      k += 5;
      keySegments += 1;
      continue;
    }
    if (allowQuoted) {
      const rawUrlEnd = tryScanRawUrlEnd(line, k);
      if (rawUrlEnd !== -1) {
        k = rawUrlEnd;
        keySegments += 1;
        continue;
      }
      if (line[k] === '"' || line[k] === "'" || line[k] === "`") {
        const q = line[k]!;
        const qEnd = scanQuotedLiteralEnd(line, k, q, escapeState);
        if (qEnd <= k + 1 || line[qEnd - 1] !== q) {
          k = qEnd;
          keySegments += 1;
          break;
        }
        if (keySegments === 0) keyLower = line.slice(k + 1, qEnd - 1).toLowerCase();
        k = qEnd;
        keySegments += 1;
        continue;
      }
    }
    if (line[k] === "(" || line[k] === "[" || line[k] === "{") {
      const grpEnd = scanBalancedGroupEnd(line, k, allowQuoted, escapeState);
      if (grpEnd <= k) break;
      k = grpEnd;
      keySegments += 1;
      continue;
    }
    const identStart = k;
    while (k < len) {
      const ch = line[k]!;
      if (ch === ":" && !line.startsWith("://", k)) {
        const runEnd = advancePastSepRun(line, k);
        let hasEq = false;
        for (let r = k; r < runEnd; r += 1) {
          if (line[r] === "=") {
            hasEq = true;
            break;
          }
        }
        if (hasEq) break;
        k = runEnd;
        continue;
      }
      if (
        isInlineWs(ch)
        || ch === "\n"
        || ch === "\r"
        || ch === "="
        || ch === ","
        || ch === ";"
        || ch === ")"
        || ch === "]"
        || ch === "}"
        || ch === "("
        || ch === "["
        || ch === "{"
        || (allowQuoted && (ch === '"' || ch === "'" || ch === "`"))
      ) {
        break;
      }
      k += 1;
    }
    if (k > identStart) {
      if (keySegments === 0) {
        const raw = line.slice(identStart, k).toLowerCase();
        keyLower = raw.startsWith("$") ? raw.slice(1) : raw;
      }
      keySegments += 1;
      continue;
    }
    break;
  }
  return { end: k, keyLower: keySegments === 1 ? keyLower : "", keySegments };
}

function tryFinishKeyValueParamEnd(
  line: string,
  k: number,
  keyLower: string,
  isCookie: boolean,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
  rejectDoubleEq = false,
): number {
  const len = line.length;
  let eqPos = k;
  while (eqPos < len && isInlineWs(line[eqPos])) eqPos += 1;
  const afterSep = advancePastSepRun(line, eqPos);
  if (afterSep === eqPos) return -1;
  const sepSlice = line.slice(eqPos, afterSep);
  if (!sepSlice.includes("=")) return -1;
  if (rejectDoubleEq && sepSlice.startsWith("==")) {
    return -1;
  }
  const vStart = advancePastSpacedSepRun(line, afterSep).vStart;
  if (rejectDoubleEq && vStart >= len) {
    return -1;
  }
  if (isCookie && keyLower === "expires") {
    const expEnd = scanCookieExpiresValueEnd(line, vStart);
    if (expEnd !== -1) return expEnd;
  }
  let vEnd = scanSingleSecretValueEnd(line, vStart, true, true, allowQuoted, escapeState);
  vEnd = consumeChainedUrlEquals(line, vStart, vEnd, isCookie, allowQuoted, escapeState);
  return vEnd > vStart ? vEnd : vStart;
}

function tryScanKeyValueParamEnd(
  line: string,
  p: number,
  isCookie: boolean,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
  rejectDoubleEq = false,
): number {
  const { end: k, keyLower } = scanParamKeyEnd(line, p, allowQuoted, escapeState);
  if (k <= p) return -1;
  return tryFinishKeyValueParamEnd(line, k, keyLower, isCookie, allowQuoted, escapeState, rejectDoubleEq);
}

function scanContinuationParamsEnd(
  line: string,
  startEnd: number,
  cookieMode: boolean,
  authParamMode: boolean,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
): number {
  const len = line.length;
  let end = startEnd;
  if (cookieMode) {
    while (end < len) {
      let sep = end;
      while (sep < len && isInlineWs(line[sep])) sep += 1;
      let sepCh = line[sep];
      let next: number;
      if (sepCh === ";" || sepCh === ",") {
        next = sep + 1;
      } else if (end > 0 && line[end - 1] === ",") {
        sepCh = ",";
        next = sep;
      } else {
        break;
      }
      while (next < len && isInlineWs(line[next])) next += 1;
      if (next >= len) break;
      if (sepCh === "," && line[next] === "}") {
        break;
      }
      if (sepCh === "," && (line[next] === '"' || line[next] === "'" || line[next] === "`")) {
        if (!allowQuoted) break;
        const qEnd = scanQuotedLiteralEnd(line, next, line[next]!, escapeState);
        const { end: keyEnd } = scanParamKeyEnd(line, next, allowQuoted, escapeState);
        let afterKey = keyEnd;
        while (afterKey < len && isInlineWs(line[afterKey])) afterKey += 1;
        const afterKeySep = advancePastSepRun(line, afterKey);
        const isKvSep = afterKeySep > afterKey && line.slice(afterKey, afterKeySep).includes("=");
        const isJsonSibling = !isKvSep && (
          line[afterKey] === ":"
          || (keyEnd > qEnd && line[keyEnd - 1] === ":")
        );
        const hasGluedSuffix = keyEnd > qEnd && !isJsonSibling;
        if (!isKvSep && !hasGluedSuffix && line[afterKey] !== ";" && line[afterKey] !== ",") break;
      }
      if (line[next] === ";" || line[next] === ",") {
        end = next;
        continue;
      }
      if (line[next] === "=" || (line[next] === ":" && !line.startsWith("://", next))) {
        const kvNext = tryFinishKeyValueParamEnd(line, next, "", true, allowQuoted, escapeState);
        if (kvNext !== -1) {
          end = kvNext;
          continue;
        }
        const { afterLastSep, vStart } = advancePastSpacedSepRun(line, next);
        if (vStart < len && !line.startsWith("[url]", vStart)) {
          const kvAfter = tryScanKeyValueParamEnd(line, vStart, true, allowQuoted, escapeState);
          if (kvAfter !== -1) {
            end = kvAfter;
            continue;
          }
          let vEnd = scanSingleSecretValueEnd(line, vStart, true, false, allowQuoted, escapeState);
          vEnd = consumeChainedUrlEquals(line, vStart, vEnd, true, allowQuoted, escapeState);
          if (vEnd > vStart) {
            end = vEnd;
            continue;
          }
        }
        if (afterLastSep > next) {
          end = afterLastSep;
          continue;
        }
      }
      const { end: itemEnd, keyLower: attrLower } = scanParamKeyEnd(line, next, allowQuoted, escapeState);
      if (itemEnd > next) {
        const kvNext = tryFinishKeyValueParamEnd(line, itemEnd, attrLower, true, allowQuoted, escapeState);
        if (kvNext !== -1) {
          end = kvNext;
          continue;
        }
        let ws = itemEnd;
        while (ws < len && isInlineWs(line[ws])) ws += 1;
        const itemSlice = line.slice(next, itemEnd);
        const isJsonMemberKey = sepCh === "," && (
          itemSlice === "[redacted]:" || (itemSlice === "[redacted]" && line[ws] === ":")
        );
        const hasSentinelOrQuote = !isJsonMemberKey && (
          itemSlice.includes("[url]")
          || itemSlice.includes("[redacted]")
          || (allowQuoted && (itemSlice.includes("://") || /['"`]/.test(itemSlice)))
        );
        const gluedAfterComma = sepCh === "," && sep < len && line[sep] === "," && next === sep + 1;
        if (
          attrLower === "httponly"
          || attrLower === "secure"
          || attrLower === "partitioned"
          || line[ws] === ";"
          || (!isJsonMemberKey && (line[ws] === "," || gluedAfterComma))
          || hasSentinelOrQuote
        ) {
          end = consumeChainedUrlEquals(line, next, itemEnd, true, allowQuoted, escapeState);
          continue;
        }
      }
      break;
    }
  } else if (authParamMode) {
    while (end < len) {
      let sep = end;
      while (sep < len && isInlineWs(line[sep])) sep += 1;
      if (line[sep] !== ",") break;
      let next = sep + 1;
      while (next < len && isInlineWs(line[next])) next += 1;
      if (next >= len) break;
      if (line[next] === ",") {
        end = next;
        continue;
      }
      if (line[next] === "=" || (line[next] === ":" && !line.startsWith("://", next))) {
        const kvNext = tryFinishKeyValueParamEnd(line, next, "", false, allowQuoted, escapeState);
        if (kvNext !== -1) {
          end = kvNext;
          continue;
        }
        const { afterLastSep, vStart } = advancePastSpacedSepRun(line, next);
        if (vStart < len && !line.startsWith("[url]", vStart)) {
          const kvAfter = tryScanKeyValueParamEnd(line, vStart, false, allowQuoted, escapeState);
          if (kvAfter !== -1) {
            end = kvAfter;
            continue;
          }
          let vEnd = scanSingleSecretValueEnd(line, vStart, true, false, allowQuoted, escapeState);
          vEnd = consumeChainedUrlEquals(line, vStart, vEnd, false, allowQuoted, escapeState);
          if (vEnd > vStart) {
            end = vEnd;
            continue;
          }
        }
        if (afterLastSep > next) {
          end = afterLastSep;
          continue;
        }
      }
      const { end: itemEnd, keyLower, keySegments } = scanParamKeyEnd(line, next, allowQuoted, escapeState);
      if (itemEnd > next) {
        const kvNext = tryFinishKeyValueParamEnd(line, itemEnd, keyLower, false, allowQuoted, escapeState);
        if (kvNext !== -1) {
          end = kvNext;
          continue;
        }
        let ws = itemEnd;
        while (ws < len && isInlineWs(line[ws])) ws += 1;
        const itemSlice = line.slice(next, itemEnd);
        const hasUrl = itemSlice.includes("[url]") || (allowQuoted && itemSlice.includes("://"));
        let isJsonMemberKey = itemSlice === "[redacted]:" || (itemSlice === "[redacted]" && line[ws] === ":");
        if (!isJsonMemberKey && allowQuoted && (line[next] === '"' || line[next] === "'" || line[next] === "`")) {
          const qEnd = scanQuotedLiteralEnd(line, next, line[next]!, escapeState);
          isJsonMemberKey = (qEnd === itemEnd && line[ws] === ":") || (qEnd === itemEnd - 1 && line[itemEnd - 1] === ":");
        }
        const hasGluedSentinelOrQuote = keySegments > 1 && !isJsonMemberKey && (
          itemSlice.includes("[redacted]")
          || (allowQuoted && /['"`]/.test(itemSlice))
        );
        if (line[ws] === "," || hasUrl || hasGluedSentinelOrQuote) {
          end = consumeChainedUrlEquals(line, next, itemEnd, false, allowQuoted, escapeState);
          continue;
        }
        if (!isJsonMemberKey && isAsciiAlpha(itemSlice[0])) {
          const schemeEnd = scanAuthSchemeTokenEnd(line, next);
          const afterSchemeSep = advancePastAuthSchemeSep(line, schemeEnd);
          const rawScheme = line.slice(next, schemeEnd);
          const rawSchemeLower = rawScheme.toLowerCase();
          const isValidChallengeScheme = schemeEnd > next
            && (AUTH_SCHEMES.has(rawSchemeLower) || (!DIAGNOSTIC_VOCAB.has(rawScheme) && !SAFE_CLAUSE_START_WORDS.has(rawScheme)));
          if (isValidChallengeScheme) {
            if (keySegments === 1 && afterSchemeSep === itemEnd && ws > itemEnd && ws < len) {
              let valPos = ws;
              if ((line[valPos] === ":" || line[valPos] === "=") && !line.startsWith("://", valPos)) {
                valPos = advancePastSepRun(line, valPos);
                while (valPos < len && isInlineWs(line[valPos])) valPos += 1;
              }
              const nextCh = valPos < len ? line[valPos]! : "";
              if (
                valPos < len
                && nextCh !== "=" && nextCh !== "," && nextCh !== ";"
                && nextCh !== "." && nextCh !== "!" && nextCh !== "?"
                && nextCh !== ")" && nextCh !== "]" && nextCh !== "}"
              ) {
                const challengeKvEnd = tryScanKeyValueParamEnd(line, valPos, false, allowQuoted, escapeState);
                if (challengeKvEnd !== -1) {
                  end = challengeKvEnd;
                  continue;
                }
                const challengeValEnd = scanSingleSecretValueEnd(line, valPos, true, true, allowQuoted, escapeState);
                if (challengeValEnd > valPos) {
                  end = consumeChainedUrlEquals(line, valPos, challengeValEnd, false, allowQuoted, escapeState);
                  continue;
                }
              }
            } else if (
              keySegments >= 2
              && afterSchemeSep < itemEnd
              && (line[afterSchemeSep] === "(" || line[afterSchemeSep] === "[" || line[afterSchemeSep] === "{")
            ) {
              end = consumeChainedUrlEquals(line, next, itemEnd, false, allowQuoted, escapeState);
              continue;
            }
          }
        }
      }
      break;
    }
  }
  return end;
}

function scanSecretHeaderOrValueEnd(
  line: string,
  p: number,
  keyName: string,
  authScheme: string,
  allowRedacted = true,
  allowQuoted = false,
  escapeState?: QuoteEscapeState,
): number {
  const cookieMode = isCookieKey(keyName, allowQuoted);
  let authParamMode = isMultiParamAuthHeader(keyName, authScheme, allowQuoted);
  const allowUrl = cookieMode || authParamMode;
  let end: number;
  if (cookieMode || authParamMode) {
    const startsWithSep = line[p] === "=" || (line[p] === ":" && !line.startsWith("://", p));
    const kvEnd = startsWithSep
      ? tryFinishKeyValueParamEnd(line, p, "", cookieMode, allowQuoted, escapeState)
      : tryScanKeyValueParamEnd(line, p, cookieMode, allowQuoted, escapeState);
    if (kvEnd !== -1) {
      end = kvEnd;
    } else if (startsWithSep) {
      const { afterLastSep, vStart } = advancePastSpacedSepRun(line, p);
      if (vStart < line.length && (allowUrl || !line.startsWith("[url]", vStart))) {
        const kvAfter = tryScanKeyValueParamEnd(line, vStart, cookieMode, allowQuoted, escapeState);
        if (kvAfter !== -1) {
          end = kvAfter;
        } else {
          end = scanSingleSecretValueEnd(line, vStart, allowRedacted, allowUrl, allowQuoted, escapeState);
          end = consumeChainedUrlEquals(line, vStart, end, cookieMode, allowQuoted, escapeState);
          if (end === vStart) end = afterLastSep;
        }
      } else {
        end = afterLastSep;
      }
    } else {
      end = scanSingleSecretValueEnd(line, p, allowRedacted, allowUrl, allowQuoted, escapeState);
      end = consumeChainedUrlEquals(line, p, end, cookieMode, allowQuoted, escapeState);
    }
  } else if (line[p] === "=" || (line[p] === ":" && !line.startsWith("://", p))) {
    const { afterLastSep, vStart } = advancePastSpacedSepRun(line, p);
    end = scanSingleSecretValueEnd(line, vStart, allowRedacted, false, allowQuoted, escapeState);
    if (end === vStart) end = afterLastSep;
    if (authScheme !== "") {
      authParamMode = true;
    }
  } else if (authScheme !== "") {
    const { end: k, keyLower } = scanParamKeyEnd(line, p, allowQuoted, escapeState);
    const kvEnd = k > p && isNonBase64KvParamAfterKey(line, p, k)
      ? tryFinishKeyValueParamEnd(line, k, keyLower, false, allowQuoted, escapeState)
      : -1;
    if (kvEnd !== -1) {
      end = kvEnd;
      authParamMode = true;
    } else {
      end = scanSingleSecretValueEnd(line, p, allowRedacted, false, allowQuoted, escapeState);
    }
  } else {
    end = scanSingleSecretValueEnd(line, p, allowRedacted, false, allowQuoted, escapeState);
  }
  if (end <= p) {
    if (cookieMode || authParamMode) {
      return scanContinuationParamsEnd(line, p, cookieMode, authParamMode, allowQuoted, escapeState);
    }
    return p;
  }
  return scanContinuationParamsEnd(line, end, cookieMode, authParamMode, allowQuoted, escapeState);
}

function tryMatchParameterizedDuckDbType(line: string, pos: number): { text: string; end: number } | null {
  const len = line.length;
  if (pos > 0 && WORD_CHAR_RE.test(line[pos - 1]!)) return null;
  let i = pos;
  const baseLimit = Math.min(len, pos + 24);
  while (i < baseLimit && ((line[i]! >= "A" && line[i]! <= "Z") || isAsciiDigit(line[i]) || line[i] === "_")) {
    i += 1;
  }
  if (i === pos || i === len) return null;
  if (line[i] !== "(" && line[i] !== "[") return null;
  const base = line.slice(pos, i);
  if (!DUCKDB_BASE_TYPES.has(base)) return null;

  let hasParamOrDim = false;
  if (line[i] === "(") {
    if (!NUMERIC_PARAM_DUCKDB_TYPES.has(base)) return null;
    const pLimit = Math.min(len, i + 20);
    i += 1;
    let sawDigit = false;
    let digitRun = 0;
    while (i < pLimit && line[i] !== ")") {
      const c = line[i]!;
      if (isAsciiDigit(c)) {
        sawDigit = true;
        digitRun += 1;
        if (digitRun > 4) return null;
      } else if (c === "," || c === " ") {
        digitRun = 0;
      } else {
        return null;
      }
      i += 1;
    }
    if (!sawDigit || i >= pLimit || line[i] !== ")") return null;
    i += 1;
    hasParamOrDim = true;
  }

  let dims = 0;
  while (dims < 8 && i < len && line[i] === "[") {
    if (line[i + 1] === "]") {
      i += 2;
      dims += 1;
      hasParamOrDim = true;
      continue;
    }
    let d = i + 1;
    const dLimit = Math.min(len, d + 4);
    while (d < dLimit && isAsciiDigit(line[d])) d += 1;
    if (d > i + 1 && d < len && line[d] === "]") {
      i = d + 1;
      dims += 1;
      hasParamOrDim = true;
      continue;
    }
    break;
  }

  if (!hasParamOrDim) return null;
  if (i < len && WORD_CHAR_RE.test(line[i]!)) return null;
  return { text: line.slice(pos, i), end: i };
}

function isFollowedByByteUnit(line: string, pos: number): boolean {
  return matchByteUnitSuffixEnd(line, pos) !== -1;
}

function isFollowedByWord(line: string, pos: number, expectedLower: string): boolean {
  const len = line.length;
  let p = pos;
  while (p < len && isInlineWs(line[p])) p += 1;
  if (p === pos || p + expectedLower.length > len) return false;
  if (line.slice(p, p + expectedLower.length).toLowerCase() !== expectedLower) return false;
  const after = p + expectedLower.length;
  return after === len || /[\s,.;:!?)\]}]/.test(line[after]!);
}

function isSafeDiagnosticNumber(
  rawTok: string,
  line: string,
  afterTokIdx: number,
  prevTokExact: string,
  prevTokLower: string,
  headerLower: string,
  parts: readonly string[],
): boolean {
  if (/^\d{1,6}(?:\.\d{1,3})?$/.test(rawTok) && isFollowedByByteUnit(line, afterTokIdx)) return true;
  if (prevTokExact === "HTTP" && /^[1-5]\d{2}$/.test(rawTok)) return true;
  if (prevTokLower === "byte" && /^\d{1,6}$/.test(rawTok) && isFollowedByWord(line, afterTokIdx, "of")) return true;
  if (prevTokLower === "line" && /^\d{1,5}$/.test(rawTok) && line[afterTokIdx] === ":") return true;
  if (
    parts.at(-3) === "[redacted]"
    && parts.at(-2) === "]"
    && parts.at(-1) === ":"
    && (rawTok === "80" || rawTok === "443")
  ) {
    return true;
  }
  if (headerLower === "parser error" && prevTokExact === "SELECT" && (rawTok === "0" || rawTok === "1")) return true;
  return false;
}

function isAllowlistedCoreWord(word: string, isClauseStart: boolean, inErrorHeader: boolean): boolean {
  if (!word || word.length >= 24) return false;
  if (SAFE_BYTE_UNITS.has(word)) return true;
  if (DUCKDB_ALNUM_TYPES.has(word)) return true;
  if (SAFE_MIXED_CASE_TYPES.has(word)) return true;
  if (SAFE_CONTRACTIONS.has(word)) return true;
  if (/^\d{1,4}(?:\.\d{1,2})?(?:ms|s)$/.test(word)) return true;
  if (DIAGNOSTIC_VOCAB.has(word)) return true;
  if (/^[A-Z]{2,19}$/.test(word)) return SAFE_UPPERCASE_WORDS.has(word);
  if (inErrorHeader && SAFE_HEADER_WORDS.has(word)) return true;
  if (isClauseStart && SAFE_CLAUSE_START_WORDS.has(word)) return true;
  return false;
}

function findSecretKeySep(rawTok: string): { sepIdx: number; keyPart: string } | null {
  const len = rawTok.length;
  let segStart = 0;
  for (let idx = 0; idx < len; idx += 1) {
    const c = rawTok[idx];
    if (c === "=" || c === ":") {
      if (idx > segStart) {
        const seg = rawTok.slice(segStart, idx);
        if (isSecretKeyStem(seg) || AUTH_SCHEMES.has(seg.toLowerCase())) {
          return { sepIdx: idx, keyPart: seg };
        }
      }
      segStart = idx + 1;
    }
  }
  return null;
}

// Stage 2B: Single-pass O(N) closed-vocabulary structural token emitter.
function emitAllowlistedTokens(line: string): string {
  const headerColonIdx = detectSafeErrorHeaderEnd(line);
  const headerLower = headerColonIdx !== -1 ? line.slice(0, headerColonIdx).trim().toLowerCase() : "";
  const parts: string[] = [];
  let i = 0;
  const len = line.length;
  let isClauseStart = true;
  let prevTokLower = "";
  let prevTokExact = "";

  while (i < len) {
    const ch = line[i]!;
    if (/\s/.test(ch)) {
      parts.push(" ");
      i += 1;
      continue;
    }
    if (line.startsWith("[redacted]", i)) {
      parts.push("[redacted]");
      i += 10;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (line.startsWith("[url]", i)) {
      parts.push("[url]");
      i += 5;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (line.startsWith("(YYYY-MM-DD)", i)) {
      parts.push("(YYYY-MM-DD)");
      i += 12;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (line.startsWith("DB::Exception", i) && (i + 13 === len || !WORD_CHAR_RE.test(line[i + 13]!))) {
      parts.push("DB::Exception");
      i += 13;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (line.startsWith("->", i) && (i === 0 || /\s/.test(line[i - 1]!)) && (i + 2 === len || /\s/.test(line[i + 2]!))) {
      parts.push("->");
      i += 2;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    const v6End = tryScanIpv6(line, i);
    if (v6End !== -1) {
      parts.push("[redacted]");
      i = v6End;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    const v4End = tryScanIpv4(line, i);
    if (v4End !== -1) {
      let end4 = v4End;
      if (line[end4] === ":" && isAsciiDigit(line[end4 + 1])) {
        let p = end4 + 1;
        while (p < len && isAsciiDigit(line[p]) && p - (end4 + 1) < 5) p += 1;
        const pv = Number(line.slice(end4 + 1, p));
        if (pv >= 1 && pv <= 65535 && (p === len || !WORD_CHAR_RE.test(line[p]!))) end4 = p;
      }
      parts.push("[redacted]");
      i = end4;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    const memMatch = /^(KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)\/(\d{1,6}(?:\.\d{1,3})?)(?=$|[\s,.;:!?)\]}])/.exec(
      line.slice(i, i + 24),
    );
    if (memMatch) {
      parts.push(memMatch[0]);
      i += memMatch[0].length;
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (ch >= "A" && ch <= "Z") {
      const typedMatch = tryMatchParameterizedDuckDbType(line, i);
      if (typedMatch) {
        parts.push(typedMatch.text);
        i = typedMatch.end;
        isClauseStart = false;
        prevTokLower = "";
        prevTokExact = "";
        continue;
      }
    }
    if (ch === "(" || ch === ")" || ch === "[" || ch === "]" || ch === "," || ch === "+" || ch === "=") {
      parts.push(ch);
      i += 1;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    if (ch === ":" || ch === "." || ch === "!" || ch === "?") {
      if (ch === "." && i + 1 < len && !/\s/.test(line[i + 1]!)) {
        let j = i + 1;
        while (j < len && !/[\s,;:!?()[\]{}]/.test(line[j]!)) {
          if (line.startsWith("[redacted]", j) || line.startsWith("[url]", j)) break;
          j += 1;
        }
        const dotTok = line.slice(i + 1, j);
        const dotBare = dotTok.replace(/^\.+/, "");
        const isDotSecret = dotBare.length > 0 && (
          findSecretKeySep(dotTok) !== null
          || isSecretKeyStem(dotBare)
          || AUTH_SCHEMES.has(dotBare.toLowerCase())
        );
        if (!isDotSecret) {
          parts.push("[redacted]");
          i = j;
          isClauseStart = false;
          prevTokLower = "";
          prevTokExact = "";
          continue;
        }
      } else {
        parts.push(ch);
        i += 1;
        isClauseStart = true;
        prevTokLower = "";
        prevTokExact = "";
        continue;
      }
    }
    if (ch === ";") {
      parts.push(" ");
      i += 1;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }

    const tokStart = i;
    while (i < len && !/\s/.test(line[i]!)) {
      if (line.startsWith("[redacted]", i) || line.startsWith("[url]", i)) break;
      if (
        line[i] === "(" || line[i] === ")" || line[i] === "[" || line[i] === "]"
        || line[i] === "," || line[i] === "+" || line[i] === ";"
      ) {
        break;
      }
      if (line[i] === "{" || line[i] === "}") {
        if (i === tokStart) i += 1;
        break;
      }
      if (
        (line[i] === ":" || line[i] === "." || line[i] === "!" || line[i] === "?")
        && (i + 1 === len || /\s/.test(line[i + 1]!))
      ) {
        break;
      }
      if (line[i] === "=" && i === tokStart) break;
      i += 1;
    }
    const rawTok = line.slice(tokStart, i);

    const secretSep = findSecretKeySep(rawTok);
    if (secretSep !== null) {
      const { sepIdx, keyPart } = secretSep;
      const sepLinePos = tokStart + sepIdx;
      const afterSepInLine = advancePastSepRun(line, sepLinePos);
      const isEmptySep = afterSepInLine >= i;
      const valOffset = isEmptySep ? rawTok.length : afterSepInLine - tokStart;
      const rawPostSep = isEmptySep ? "" : rawTok.slice(valOffset);
      const postSepLower = rawPostSep.toLowerCase();
      let inlineAuthScheme = "";
      let inlineAuthHasSep = false;
      if (!isEmptySep) {
        if (AUTH_SCHEMES.has(postSepLower)) {
          inlineAuthScheme = postSepLower;
        } else {
          let trimEnd = rawPostSep.length;
          while (trimEnd > 0) {
            const c = rawPostSep[trimEnd - 1];
            if (c === "=" || c === ":") {
              trimEnd -= 1;
            } else if (c === ">" && trimEnd >= 2 && (rawPostSep[trimEnd - 2] === "=" || rawPostSep[trimEnd - 2] === ":")) {
              trimEnd -= 2;
            } else {
              break;
            }
          }
          if (trimEnd < rawPostSep.length) {
            const candidate = rawPostSep.slice(0, trimEnd).toLowerCase();
            if (AUTH_SCHEMES.has(candidate)) {
              inlineAuthScheme = candidate;
              inlineAuthHasSep = true;
            }
          }
        }
      }
      const isCustomInlineScheme = !isEmptySep && !inlineAuthScheme && isCustomAuthScheme(keyPart, rawPostSep, line, i, false);
      if (isEmptySep || inlineAuthScheme !== "" || isCustomInlineScheme) {
        let p = isEmptySep ? Math.max(i, afterSepInLine) : i;
        if (inlineAuthScheme !== "" || isCustomInlineScheme) {
          const afterInlineSep = advancePastAuthSchemeSep(line, p);
          if (afterInlineSep > p) {
            p = afterInlineSep;
            inlineAuthHasSep = true;
          }
        }
        const afterInlineSchemeSep = p;
        while (p < len && isInlineWs(line[p])) p += 1;
        if (isCustomInlineScheme && (line[p] === ":" || line[p] === "=") && !line.startsWith("://", p)) {
          p = advancePastSepRun(line, p);
          while (p < len && isInlineWs(line[p])) p += 1;
        }
        const canConsume = isEmptySep
          ? p < len
          : (p > i || inlineAuthHasSep || line[p] === "(" || line[p] === "[" || line[p] === "{") && p < len;
        if (canConsume) {
          let matchedScheme = isEmptySep ? "" : (inlineAuthScheme || postSepLower);
          if (isEmptySep && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
            const sEnd = scanAuthSchemeTokenEnd(line, p);
            const rawScheme = line.slice(p, sEnd);
            const maybeScheme = rawScheme.toLowerCase();
            const afterSchemeSep = advancePastAuthSchemeSep(line, sEnd);
            const hasSchemeSep = afterSchemeSep > sEnd;
            const isKnownScheme = AUTH_SCHEMES.has(maybeScheme)
              && (hasSchemeSep || isInlineWs(line[sEnd]) || line[sEnd] === "(" || line[sEnd] === "[" || line[sEnd] === "{");
            const isCustomScheme = !isKnownScheme && isCustomAuthScheme(keyPart, rawScheme, line, sEnd, false);
            if (isKnownScheme || isCustomScheme) {
              matchedScheme = maybeScheme;
              p = afterSchemeSep;
              while (p < len && isInlineWs(line[p])) p += 1;
              if (isCustomScheme && (line[p] === ":" || line[p] === "=") && !line.startsWith("://", p)) {
                p = advancePastSepRun(line, p);
                while (p < len && isInlineWs(line[p])) p += 1;
              }
              if (line.startsWith("[url]", p) && !isCookieKey(keyPart) && !isMultiParamAuthHeader(keyPart, matchedScheme)) {
                i = afterSchemeSep;
                parts.push("[redacted]");
                isClauseStart = false;
                prevTokLower = "";
                prevTokExact = "";
                continue;
              }
            }
          }
          const allowLeadingUrl = isCookieKey(keyPart) || isMultiParamAuthHeader(keyPart, matchedScheme);
          if (p < len && (allowLeadingUrl || !line.startsWith("[url]", p))) {
            const valEnd = scanSecretHeaderOrValueEnd(line, p, keyPart, matchedScheme, true);
            if (valEnd > p) i = valEnd;
            else if (afterInlineSchemeSep > i) i = afterInlineSchemeSep;
          } else if (afterInlineSchemeSep > i) {
            i = afterInlineSchemeSep;
          }
        } else {
          if (afterInlineSchemeSep > i) i = afterInlineSchemeSep;
          if (!isEmptySep) {
            const cookieMode = isCookieKey(keyPart);
            const authParamMode = isMultiParamAuthHeader(keyPart, inlineAuthScheme || postSepLower);
            if (cookieMode || authParamMode) {
              const contEnd = scanContinuationParamsEnd(line, i, cookieMode, authParamMode);
              if (contEnd > i) i = contEnd;
            }
          }
        }
      } else {
        const valPos = tokStart + valOffset;
        let scanStart = valPos;
        let gluedScheme = "";
        const sEnd = scanAuthSchemeTokenEnd(line, valPos);
        if (sEnd > valPos) {
          const afterSchemeSep = advancePastAuthSchemeSep(line, sEnd);
          if (afterSchemeSep > sEnd) {
            const maybeScheme = line.slice(valPos, sEnd).toLowerCase();
            if (AUTH_SCHEMES.has(maybeScheme)) {
              gluedScheme = maybeScheme;
              if (afterSchemeSep < len) scanStart = afterSchemeSep;
            }
          }
        }
        const valEnd = scanSecretHeaderOrValueEnd(line, scanStart, keyPart, gluedScheme, true);
        if (valEnd > i) i = valEnd;
      }
      parts.push("[redacted]");
      isClauseStart = false;
      prevTokLower = "";
      prevTokExact = "";
      continue;
    }
    const rawLower = rawTok.toLowerCase();
    const bareTok = rawTok.startsWith(".") ? rawTok.replace(/^\.+/, "") : rawTok;
    const bareLower = bareTok.toLowerCase();
    let afterWs = i;
    while (afterWs < len && isInlineWs(line[afterWs])) afterWs += 1;
    const afterExplicitSep = advancePastSepRun(line, afterWs);
    const hasExplicitSep = afterExplicitSep > afterWs;
    const isSqlKeyPhrase = rawLower === "key" && !hasExplicitSep && SAFE_KEY_PREFIX_WORDS.has(prevTokLower);
    const isBenignBareVocabStem = !hasExplicitSep && (rawLower === "session" || rawLower === "sessions" || rawLower === "keys");
    const isBareAuthScheme = (
      (bareLower === "basic" || bareLower === "digest" || bareLower === "oauth" || bareLower === "negotiate")
      && (bareTok !== bareLower || hasExplicitSep)
    ) || (
      MULTI_PARAM_AUTH_SCHEMES.has(bareLower)
      && (
        bareTok !== bareLower
        || hasExplicitSep
        || (afterWs < len && tryScanKeyValueParamEnd(line, afterWs, false, false, undefined, true) !== -1)
      )
    );

    if ((isSecretKeyStem(rawTok) && !isSqlKeyPhrase && !isBenignBareVocabStem) || isBareAuthScheme) {
      let p = afterWs;
      if (hasExplicitSep) {
        p = afterExplicitSep;
        while (p < len && isInlineWs(line[p])) p += 1;
      }
      let matchedScheme = isBareAuthScheme || AUTH_SCHEMES.has(bareLower) ? bareLower : "";
      if (!isBareAuthScheme && p < len && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
        const sEnd = scanAuthSchemeTokenEnd(line, p);
        const rawScheme = line.slice(p, sEnd);
        const maybeScheme = rawScheme.toLowerCase();
        const afterSchemeSep = advancePastAuthSchemeSep(line, sEnd);
        const hasSchemeSep = afterSchemeSep > sEnd;
        const isKnownScheme = AUTH_SCHEMES.has(maybeScheme)
          && (hasSchemeSep || isInlineWs(line[sEnd]) || line[sEnd] === "(" || line[sEnd] === "[" || line[sEnd] === "{");
        const isCustomScheme = !isKnownScheme && hasExplicitSep && isCustomAuthScheme(rawTok, rawScheme, line, sEnd, false);
        if (isKnownScheme || isCustomScheme) {
          const afterScheme = afterSchemeSep;
          matchedScheme = maybeScheme;
          p = afterSchemeSep;
          while (p < len && isInlineWs(line[p])) p += 1;
          if (isCustomScheme && (line[p] === ":" || line[p] === "=") && !line.startsWith("://", p)) {
            p = advancePastSepRun(line, p);
            while (p < len && isInlineWs(line[p])) p += 1;
          }
          if (line.startsWith("[url]", p) && !isCookieKey(rawTok) && !isMultiParamAuthHeader(rawTok, matchedScheme)) {
            parts.push("[redacted]");
            i = afterScheme;
            isClauseStart = false;
            prevTokLower = "";
            prevTokExact = "";
            continue;
          }
        }
      }
      const allowLeadingUrl = isCookieKey(rawTok) || isMultiParamAuthHeader(rawTok, matchedScheme);
      if (p < len && (allowLeadingUrl || !line.startsWith("[url]", p))) {
        const valEnd = scanSecretHeaderOrValueEnd(line, p, rawTok, matchedScheme, true);
        if (valEnd > p) {
          parts.push("[redacted]");
          i = valEnd;
          isClauseStart = false;
          prevTokLower = "";
          prevTokExact = "";
          continue;
        }
      }
    }

    const inHeader = headerColonIdx !== -1 && tokStart < headerColonIdx;
    const allowed = isAllowlistedCoreWord(rawTok, isClauseStart, inHeader)
      || isSafeDiagnosticNumber(rawTok, line, i, prevTokExact, prevTokLower, headerLower, parts);
    parts.push(allowed ? rawTok : "[redacted]");
    isClauseStart = false;
    prevTokLower = allowed ? rawLower : "";
    prevTokExact = allowed ? rawTok : "";
  }

  return parts
    .join("")
    .replace(/\[redacted\](?:\s*\[redacted\])+/g, "[redacted]")
    .replace(/\s+/g, " ")
    .trim();
}

// Error messages can carry SQL, URLs, keys, IPs, or raw row/regex payloads:
// canonicalize known DuckDB error frames in O(N), restrict to line 1, and emit
// only closed-vocabulary structural diagnostic tokens.
function redactedMessage(message: string): string {
  try {
    const { text: stage1, canonicalized } = canonicalizeDuckDbTemplates(message);
    const line1Raw = sliceFirstLine(stage1);
    const quotedRedacted = redactQuotesAndUrlsLine(line1Raw);
    const tailRedacted = canonicalized ? quotedRedacted : redactPostColonDiagnosticTail(quotedRedacted);
    const text = emitAllowlistedTokens(tailRedacted);
    return text.length > MESSAGE_LIMIT ? `${text.slice(0, MESSAGE_LIMIT - 1)}…` : text;
  } finally {
    balancedGroupCache = null;
  }
}

// Response bodies can carry SQL, keys or tokens, so only a response's status is reported.
function failureReason(failure: unknown): string {
  if (failure instanceof Response) {
    return failure.status === 413 ? "HTTP 413: query exceeds the browser scan budget" : `HTTP ${failure.status}`;
  }
  if (isNetworkFailure(failure)) return "NetworkError";
  const name = failure instanceof Error ? failure.name : "";
  const kind = /^[A-Za-z]\w{0,63}$/.test(name) ? name : "Error";
  const message = failure instanceof Error ? redactedMessage(failure.message) : "";
  return message ? `${kind}: ${message}` : kind;
}

export async function measureCloudQuery<T>(
  section: string,
  query: () => Promise<T>,
  selection?: { range: string | null; filters: unknown; start: string | null; end: string | null },
  serviceId: string | null = null,
): Promise<T> {
  if (!canObserve()) return query();
  const search = new URLSearchParams();
  if (selection?.range) search.set("range", selection.range);
  else if (selection) {
    if (selection.start) search.set("start_time", selection.start);
    if (selection.end) search.set("end_time", selection.end);
  }
  if (selection?.filters && typeof selection.filters === "object" && Object.keys(selection.filters).length) {
    search.set("filters", JSON.stringify(selection.filters));
  }
  const detail = { ...context(), ...(selection ? { search: search.toString() } : {}), serviceId, section, id: ++sequence };
  globalThis.dispatchEvent(new CustomEvent("fla:cloud-query", {
    detail: { ...detail, state: "pending", totalRows: null },
  }));
  try {
    const data = await query();
    const failed = data instanceof Response && !data.ok;
    globalThis.dispatchEvent(new CustomEvent("fla:cloud-query", {
      detail: { ...detail, state: failed ? "error" : "success", totalRows: rowCount(data),
        status: data instanceof Response ? data.status : null, ...(failed ? { error: failureReason(data) } : {}) },
    }));
    return data;
  } catch (error) {
    const cancelled = typeof error === "object" && error !== null && (error as { name?: unknown }).name === "AbortError";
    globalThis.dispatchEvent(new CustomEvent("fla:cloud-query", {
      detail: cancelled
        ? { ...detail, state: "cancelled", totalRows: null }
        : { ...detail, state: "error", totalRows: null, error: failureReason(error) },
    }));
    throw error;
  }
}

export function publishCloudWorkerTelemetry(
  payload: object,
  serviceId: string | null = null,
  origin?: CloudQueryContext,
): void {
  if (!canObserve()) return;
  const source = payload as Record<string, unknown>;
  const detail: Record<string, unknown> = { ...(origin ?? context()), serviceId };
  for (const [field, wire] of Object.entries({
    filesListed: "files_listed", filesFetched: "files_fetched", filesCached: "files_cached",
    cdnHits: "cdn_hits", rowsScanned: "rows_scanned", bytesScanned: "bytes_scanned",
    clientElapsedMs: "duration_ms", vcpuMs: "vcpu_ms",
  })) {
    const value = source[wire];
    detail[field] = typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
  }
  globalThis.dispatchEvent(new CustomEvent("fla:cloud-worker", { detail }));
}
