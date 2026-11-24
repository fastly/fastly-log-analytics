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
  "password", "passwd", "pwd", "authorization", "auth", "bearer",
  "cookie", "cookies", "signature", "sig", "credential", "credentials",
  "session", "sessions", "apikey", "jwt", "nonce", "otp", "pin", "hmac",
]);
const AUTH_SCHEMES = new Set<string>(["bearer", "basic", "digest", "token"]);

const WORD_CHAR_RE = /[\p{L}\p{N}_]/u;
const DOLLAR_TAG_CHAR_RE = /[A-Za-z0-9_\u0080-\uffff]/;
const DOLLAR_TAG_START_RE = /[A-Za-z_\u0080-\uffff]/;

function isAsciiDigit(ch: string | undefined): boolean {
  return ch !== undefined && ch >= "0" && ch <= "9";
}

function isHexDigit(ch: string | undefined): boolean {
  return ch !== undefined && (
    (ch >= "0" && ch <= "9") || (ch >= "a" && ch <= "f") || (ch >= "A" && ch <= "F")
  );
}

function isAsciiAlpha(ch: string | undefined): boolean {
  return ch !== undefined && ((ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z"));
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

// Redact $$...$$ and $tag$...$tag$ spans (including multiline) in O(N).
function redactDollarQuotedSpans(raw: string): string {
  if (!raw.includes("$")) return raw;
  const parts: string[] = [];
  let i = 0;
  const len = raw.length;
  while (i < len) {
    if (raw[i] === "$") {
      let j = i + 1;
      if (j < len && raw[j] === "$") {
        const close = raw.indexOf("$$", j + 1);
        parts.push("[redacted]");
        i = close === -1 ? len : close + 2;
        continue;
      }
      if (j < len && DOLLAR_TAG_START_RE.test(raw[j]!)) {
        j += 1;
        let tagLen = 1;
        while (j < len && tagLen < 32 && DOLLAR_TAG_CHAR_RE.test(raw[j]!)) {
          j += 1;
          tagLen += 1;
        }
        if (j < len && raw[j] === "$") {
          const tag = raw.slice(i, j + 1);
          const close = raw.indexOf(tag, j + 1);
          parts.push("[redacted]");
          i = close === -1 ? len : close + tag.length;
          continue;
        }
      }
    }
    parts.push(raw[i]!);
    i += 1;
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
  while (bodyStart < s.length && (s[bodyStart] === " " || s[bodyStart] === "\t")) {
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

function isSecretJsonKey(key: string): boolean {
  if (!key || key.length > 64 || key.includes("\\")) return false;
  const lower = key.toLowerCase();
  if (SECRET_STEMS.has(lower)) return true;
  let segStart = 0;
  for (let idx = 0; idx <= lower.length; idx += 1) {
    if (idx === lower.length || lower[idx] === "_" || lower[idx] === "-" || lower[idx] === " ") {
      if (idx > segStart && idx - segStart <= 24 && SECRET_STEMS.has(lower.slice(segStart, idx))) {
        return true;
      }
      segStart = idx + 1;
    }
  }
  return false;
}

function scanJsonValueEnd(line: string, p: number): number {
  const len = line.length;
  if (p >= len) return p;
  if (line.startsWith("[url]", p)) return p;
  if (line.startsWith("[redacted]", p)) return p + 10;
  const ch = line[p]!;
  if (ch === '"' || ch === "'") {
    let k = p + 1;
    while (k < len) {
      if (line[k] === "\\") {
        k += 2;
        continue;
      }
      if (line[k] === ch) {
        k += 1;
        break;
      }
      k += 1;
    }
    return Math.min(k, len);
  }
  if (ch === "{" || ch === "[" || ch === "(") {
    const closeCh = ch === "{" ? "}" : ch === "[" ? "]" : ")";
    let depth = 1;
    let k = p + 1;
    while (k < len && depth > 0) {
      const c = line[k]!;
      if (c === '"' || c === "'") {
        const q = c;
        k += 1;
        while (k < len) {
          if (line[k] === "\\") {
            k += 2;
            continue;
          }
          if (line[k] === q) {
            k += 1;
            break;
          }
          k += 1;
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
      if (c === ch) depth += 1;
      else if (c === closeCh) depth -= 1;
      k += 1;
    }
    return Math.min(k, len);
  }
  let k = p;
  while (k < len && !/[\s,}\]);]/.test(line[k]!)) {
    if (line.startsWith("[redacted]", k) || line.startsWith("[url]", k)) break;
    k += 1;
  }
  return k;
}

function consumeJsonSecretValueAfterQuote(line1: string, pos: number, keyCandidate: string): number {
  if (!isSecretJsonKey(keyCandidate)) return pos;
  const len = line1.length;
  let p = pos;
  while (p < len && (line1[p] === " " || line1[p] === "\t")) p += 1;
  if (line1[p] !== ":") return pos;
  p += 1;
  while (p < len && (line1[p] === " " || line1[p] === "\t")) p += 1;
  if (p >= len || line1.startsWith("[url]", p)) return pos;
  const valEnd = scanJsonValueEnd(line1, p);
  return valEnd > p ? valEnd : pos;
}

// Stage 2A: O(N) strict pair-by-pair quote & URL scanner (closed contraction allowlist only).
function redactQuotesAndUrlsLine(line1: string): string {
  const parts: string[] = [];
  let outChars = 0;
  let i = 0;
  const len = line1.length;
  while (i < len) {
    if (outChars >= MESSAGE_LIMIT + 128) break;
    if (line1.startsWith("[redacted]", i)) {
      parts.push("[redacted]");
      outChars += 10;
      i += 10;
      continue;
    }
    if (line1.startsWith("[url]", i)) {
      parts.push("[url]");
      outChars += 5;
      i += 5;
      continue;
    }
    const contractionLen = matchSafeContractionLen(line1, i);
    if (contractionLen > 0) {
      const tok = line1.slice(i, i + contractionLen);
      parts.push(tok);
      outChars += contractionLen;
      i += contractionLen;
      continue;
    }
    if (isAsciiAlpha(line1[i]) && (i === 0 || !WORD_CHAR_RE.test(line1[i - 1]!))) {
      let j = i + 1;
      const schemeLimit = Math.min(len, i + MAX_URL_SCHEME_LEN);
      while (
        j < schemeLimit
        && (isAsciiAlpha(line1[j]) || isAsciiDigit(line1[j]) || line1[j] === "+" || line1[j] === "." || line1[j] === "-")
      ) {
        j += 1;
      }
      if (line1.startsWith("://", j)) {
        let k = j + 3;
        while (k < len && !/[\s'"`]/.test(line1[k]!)) k += 1;
        parts.push("[url]");
        outChars += 5;
        i = k;
        continue;
      }
    }
    const prevWord = i > 0 && WORD_CHAR_RE.test(line1[i - 1]!);
    const typedLen = prevWord
      ? 0
      : (line1[i] === "U" || line1[i] === "u") && line1[i + 1] === "&" && line1[i + 2] === "'"
        ? 3
        : isSqlPrefixChar(line1[i]) && line1[i + 1] === "'"
          ? 2
          : 0;
    if (typedLen > 0 || line1[i] === "'") {
      const firstCh = line1[i];
      const isEscapeSingle = typedLen > 0 && (firstCh === "E" || firstCh === "e" || firstCh === "U" || firstCh === "u");
      parts.push("[redacted]");
      outChars += 10;
      i += typedLen > 0 ? typedLen : 1;
      const contentStart = i;
      let contentEnd = -1;
      while (i < len) {
        if (line1[i] === "\\" && line1[i + 1] === "\\") {
          i += 2;
          continue;
        }
        if (isEscapeSingle && line1[i] === "\\") {
          i += 2;
          continue;
        }
        if (line1[i] === "'") {
          if (line1[i + 1] === "'") {
            i += 2;
            continue;
          }
          contentEnd = i;
          i += 1;
          break;
        }
        i += 1;
      }
      if (typedLen === 0 && contentEnd > contentStart && contentEnd - contentStart <= 64) {
        i = consumeJsonSecretValueAfterQuote(line1, i, line1.slice(contentStart, contentEnd));
      }
      continue;
    }
    if (line1[i] === '"' || line1[i] === "`") {
      const q = line1[i];
      parts.push("[redacted]");
      outChars += 10;
      i += 1;
      const contentStart = i;
      let contentEnd = -1;
      while (i < len) {
        if (line1[i] === "\\") {
          i += 2;
          continue;
        }
        if (line1[i] === q) {
          if (line1[i + 1] === q) {
            i += 2;
            continue;
          }
          contentEnd = i;
          i += 1;
          break;
        }
        i += 1;
      }
      if (q === '"' && contentEnd > contentStart && contentEnd - contentStart <= 64) {
        i = consumeJsonSecretValueAfterQuote(line1, i, line1.slice(contentStart, contentEnd));
      }
      continue;
    }
    parts.push(line1[i]!);
    outChars += 1;
    i += 1;
  }
  return parts.join("");
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
  if (!word) return false;
  const lower = word.toLowerCase();
  if (SECRET_STEMS.has(lower)) return true;
  let segStart = 0;
  for (let idx = 0; idx <= lower.length; idx += 1) {
    if (idx === lower.length || lower[idx] === "_" || lower[idx] === "-") {
      if (idx > segStart && idx - segStart <= 24 && SECRET_STEMS.has(lower.slice(segStart, idx))) {
        return true;
      }
      segStart = idx + 1;
    }
  }
  return false;
}

function isCookieKey(word: string): boolean {
  if (!word) return false;
  const lower = word.toLowerCase();
  if (lower === "cookie" || lower === "cookies") return true;
  let segStart = 0;
  for (let idx = 0; idx <= lower.length; idx += 1) {
    if (idx === lower.length || lower[idx] === "_" || lower[idx] === "-") {
      const seg = lower.slice(segStart, idx);
      if (seg === "cookie" || seg === "cookies") return true;
      segStart = idx + 1;
    }
  }
  return false;
}

function isMultiParamAuthHeader(keyName: string, authScheme: string): boolean {
  if (authScheme === "digest") return true;
  if (authScheme === "basic" || authScheme === "bearer") return false;
  const lower = keyName.toLowerCase();
  return lower === "authorization" || lower === "proxy-authorization" || lower === "digest";
}

function scanBalancedGroupEnd(line: string, p: number): number {
  const len = line.length;
  const openCh = line[p];
  const closeCh = openCh === "(" ? ")" : openCh === "[" ? "]" : openCh === "{" ? "}" : "";
  if (!closeCh) return p;
  let depth = 1;
  let k = p + 1;
  while (k < len && depth > 0) {
    if (line.startsWith("[redacted]", k)) {
      k += 10;
      continue;
    }
    if (line.startsWith("[url]", k)) {
      k += 5;
      continue;
    }
    if (line[k] === openCh) depth += 1;
    else if (line[k] === closeCh) depth -= 1;
    k += 1;
  }
  return k;
}

function scanSingleSecretValueEnd(line: string, p: number, allowRedacted = false): number {
  const len = line.length;
  if (p >= len) return p;
  if (line.startsWith("[url]", p)) return p;
  if (line.startsWith("[redacted]", p)) return allowRedacted ? p + 10 : p;
  if (line[p] === "(" || line[p] === "[" || line[p] === "{") {
    return scanBalancedGroupEnd(line, p);
  }
  let k = p;
  while (k < len && !/[\s,;]/.test(line[k]!)) {
    if (line.startsWith("[url]", k)) break;
    if (line.startsWith("[redacted]", k)) {
      if (allowRedacted) {
        k += 10;
        continue;
      }
      break;
    }
    if (line[k] === "(" || line[k] === "[" || line[k] === "{") {
      k = scanBalancedGroupEnd(line, k);
      continue;
    }
    k += 1;
  }
  return k;
}

function tryScanKeyValueParamEnd(line: string, p: number, isCookie: boolean): number {
  const len = line.length;
  let k = p;
  const keyLimit = Math.min(len, p + 64);
  while (
    k < keyLimit
    && (isAsciiAlpha(line[k]) || isAsciiDigit(line[k]) || line[k] === "_" || line[k] === "-" || line[k] === ".")
  ) {
    k += 1;
  }
  if (k === p) return -1;
  let eqPos = k;
  const wsLimit = Math.min(len, k + 16);
  while (eqPos < wsLimit && (line[eqPos] === " " || line[eqPos] === "\t")) eqPos += 1;
  if (line[eqPos] !== "=") return -1;
  const nextAfterEq = line[eqPos + 1];
  if (!isCookie && eqPos === k && (nextAfterEq === undefined || nextAfterEq === " " || nextAfterEq === "\t" || nextAfterEq === "=")) {
    return -1;
  }
  let vStart = eqPos + 1;
  while (vStart < len && (line[vStart] === " " || line[vStart] === "\t")) vStart += 1;
  const vEnd = scanSingleSecretValueEnd(line, vStart, true);
  return vEnd > vStart ? vEnd : eqPos + 1;
}

function scanContinuationParamsEnd(
  line: string,
  startEnd: number,
  cookieMode: boolean,
  authParamMode: boolean,
): number {
  const len = line.length;
  let end = startEnd;
  if (cookieMode) {
    while (end < len) {
      let sep = end;
      while (sep < len && (line[sep] === " " || line[sep] === "\t")) sep += 1;
      if (line[sep] !== ";") break;
      let next = sep + 1;
      while (next < len && (line[next] === " " || line[next] === "\t")) next += 1;
      if (next >= len || line.startsWith("[url]", next)) break;
      const kvNext = tryScanKeyValueParamEnd(line, next, true);
      if (kvNext !== -1) {
        end = kvNext;
        continue;
      }
      let attrEnd = next;
      while (attrEnd < len && (isAsciiAlpha(line[attrEnd]) || line[attrEnd] === "-")) attrEnd += 1;
      const attrLower = line.slice(next, attrEnd).toLowerCase();
      if (attrEnd > next && (attrLower === "httponly" || attrLower === "secure" || line[attrEnd] === ";")) {
        end = attrEnd;
        continue;
      }
      break;
    }
  } else if (authParamMode) {
    while (end < len) {
      let sep = end;
      while (sep < len && (line[sep] === " " || line[sep] === "\t")) sep += 1;
      if (line[sep] !== ",") break;
      let next = sep + 1;
      while (next < len && (line[next] === " " || line[next] === "\t")) next += 1;
      if (next >= len || line.startsWith("[url]", next)) break;
      const kvNext = tryScanKeyValueParamEnd(line, next, false);
      if (kvNext !== -1) {
        end = kvNext;
        continue;
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
): number {
  const cookieMode = isCookieKey(keyName);
  const authParamMode = isMultiParamAuthHeader(keyName, authScheme);
  let end: number;
  if (cookieMode || authParamMode) {
    const kvEnd = tryScanKeyValueParamEnd(line, p, cookieMode);
    end = kvEnd !== -1 ? kvEnd : scanSingleSecretValueEnd(line, p, false);
  } else {
    end = scanSingleSecretValueEnd(line, p, false);
  }
  if (end <= p) return p;
  return scanContinuationParamsEnd(line, end, cookieMode, authParamMode);
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
  const len = line.length;
  let p = pos;
  while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
  if (p === pos || p >= len) return false;
  let uEnd = p;
  const uLimit = Math.min(len, p + 8);
  while (uEnd < uLimit && isAsciiAlpha(line[uEnd])) uEnd += 1;
  if (uEnd === p) return false;
  const unit = line.slice(p, uEnd);
  if (!SAFE_BYTE_UNITS.has(unit) && unit !== "byte" && unit !== "bytes") return false;
  return uEnd === len || /[\s,.;:!?)\]/}]/.test(line[uEnd]!);
}

function isFollowedByWord(line: string, pos: number, expectedLower: string): boolean {
  const len = line.length;
  let p = pos;
  while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
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
        parts.push("[redacted]");
        i = j;
        isClauseStart = false;
        prevTokLower = "";
        prevTokExact = "";
        continue;
      }
      parts.push(ch);
      i += 1;
      isClauseStart = true;
      prevTokLower = "";
      prevTokExact = "";
      continue;
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

    const eqIdx = rawTok.indexOf("=");
    const colIdx = rawTok.indexOf(":");
    const sepIdx = eqIdx > 0 && (colIdx === -1 || eqIdx < colIdx)
      ? eqIdx
      : colIdx > 0
        ? colIdx
        : -1;
    if (sepIdx > 0) {
      const keyPart = rawTok.slice(0, sepIdx);
      if (isSecretKeyStem(keyPart) || AUTH_SCHEMES.has(keyPart.toLowerCase())) {
        const postSep = rawTok.slice(sepIdx + 1);
        const postSepLower = postSep.toLowerCase();
        if (postSep === "" || AUTH_SCHEMES.has(postSepLower)) {
          let p = i;
          while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
          const canConsume = postSep === ""
            ? p < len
            : (p > i || line[p] === "(" || line[p] === "[" || line[p] === "{") && p < len;
          if (canConsume && !line.startsWith("[url]", p)) {
            let matchedScheme = postSep === "" ? "" : postSepLower;
            if (postSep === "" && !line.startsWith("[redacted]", p)) {
              let sEnd = p;
              while (sEnd < len && isAsciiAlpha(line[sEnd])) sEnd += 1;
              const maybeScheme = line.slice(p, sEnd).toLowerCase();
              if (
                AUTH_SCHEMES.has(maybeScheme)
                && (line[sEnd] === " " || line[sEnd] === "\t" || line[sEnd] === "(" || line[sEnd] === "[" || line[sEnd] === "{")
              ) {
                matchedScheme = maybeScheme;
                p = sEnd;
                while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
                if (line.startsWith("[redacted]", p) || line.startsWith("[url]", p)) {
                  i = sEnd;
                  parts.push("[redacted]");
                  isClauseStart = false;
                  prevTokLower = "";
                  prevTokExact = "";
                  continue;
                }
              }
            }
            if (p < len && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
              const valEnd = scanSecretHeaderOrValueEnd(line, p, keyPart, matchedScheme);
              if (valEnd > p) i = valEnd;
            } else if (isCookieKey(keyPart) && line.startsWith("[redacted]", p)) {
              i = scanContinuationParamsEnd(line, p + 10, true, false);
            }
          }
        } else {
          if (
            i < len
            && (line[i] === "(" || line[i] === "[" || line[i] === "{")
            && !line.startsWith("[redacted]", i)
            && !line.startsWith("[url]", i)
          ) {
            i = scanBalancedGroupEnd(line, i);
          }
          const cookieMode = isCookieKey(keyPart);
          let authParamMode = isMultiParamAuthHeader(keyPart, "") && postSep.includes("=");
          if ((cookieMode || isMultiParamAuthHeader(keyPart, "")) && !postSep.includes("=")) {
            let eqP = i;
            const eqLimit = Math.min(len, i + 16);
            while (eqP < eqLimit && (line[eqP] === " " || line[eqP] === "\t")) eqP += 1;
            if (line[eqP] === "=") {
              let vStart = eqP + 1;
              while (vStart < len && (line[vStart] === " " || line[vStart] === "\t")) vStart += 1;
              const vEnd = scanSingleSecretValueEnd(line, vStart, true);
              i = vEnd > vStart ? vEnd : eqP + 1;
              if (isMultiParamAuthHeader(keyPart, "")) authParamMode = true;
            }
          }
          if (cookieMode || authParamMode) {
            i = scanContinuationParamsEnd(line, i, cookieMode, authParamMode);
          }
        }
        parts.push("[redacted]");
        isClauseStart = false;
        prevTokLower = "";
        prevTokExact = "";
        continue;
      }
    }
    const rawLower = rawTok.toLowerCase();
    let afterWs = i;
    while (afterWs < len && (line[afterWs] === " " || line[afterWs] === "\t")) afterWs += 1;
    const hasExplicitSep = line[afterWs] === ":" || line[afterWs] === "=";
    const isSqlKeyPhrase = rawLower === "key" && !hasExplicitSep && SAFE_KEY_PREFIX_WORDS.has(prevTokLower);
    const isBenignBareVocabStem = !hasExplicitSep && (rawLower === "session" || rawLower === "sessions" || rawLower === "keys");
    const isBareAuthScheme = (
      rawTok === "Basic" || rawTok === "BASIC" || rawTok === "Digest" || rawTok === "DIGEST"
    ) || ((rawLower === "basic" || rawLower === "digest") && hasExplicitSep);

    if ((isSecretKeyStem(rawTok) && !isSqlKeyPhrase && !isBenignBareVocabStem) || isBareAuthScheme) {
      let p = afterWs;
      if (hasExplicitSep) {
        p += 1;
        while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
      }
      let matchedScheme = isBareAuthScheme ? rawLower : "";
      if (!isBareAuthScheme && p < len && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
        let sEnd = p;
        while (sEnd < len && isAsciiAlpha(line[sEnd])) sEnd += 1;
        const maybeScheme = line.slice(p, sEnd).toLowerCase();
        if (
          AUTH_SCHEMES.has(maybeScheme)
          && (line[sEnd] === " " || line[sEnd] === "\t" || line[sEnd] === "(" || line[sEnd] === "[" || line[sEnd] === "{")
        ) {
          const afterScheme = sEnd;
          matchedScheme = maybeScheme;
          p = sEnd;
          while (p < len && (line[p] === " " || line[p] === "\t")) p += 1;
          if (line.startsWith("[redacted]", p) || line.startsWith("[url]", p)) {
            parts.push("[redacted]");
            i = afterScheme;
            isClauseStart = false;
            prevTokLower = "";
            prevTokExact = "";
            continue;
          }
        }
      }
      if (p < len && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
        const valEnd = scanSecretHeaderOrValueEnd(line, p, rawTok, matchedScheme);
        if (valEnd > p) {
          parts.push("[redacted]");
          i = valEnd;
          isClauseStart = false;
          prevTokLower = "";
          prevTokExact = "";
          continue;
        }
      } else if (isCookieKey(rawTok) && line.startsWith("[redacted]", p)) {
        parts.push("[redacted]");
        i = scanContinuationParamsEnd(line, p + 10, true, false);
        isClauseStart = false;
        prevTokLower = "";
        prevTokExact = "";
        continue;
      } else if (hasExplicitSep && line.startsWith("[redacted]", p)) {
        parts.push("[redacted]");
        i = p + 10;
        isClauseStart = false;
        prevTokLower = "";
        prevTokExact = "";
        continue;
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
  const { text: stage1, canonicalized } = canonicalizeDuckDbTemplates(message);
  const line1Raw = sliceFirstLine(stage1);
  const quotedRedacted = redactQuotesAndUrlsLine(line1Raw);
  const tailRedacted = canonicalized ? quotedRedacted : redactPostColonDiagnosticTail(quotedRedacted);
  const text = emitAllowlistedTokens(tailRedacted);
  return text.length > MESSAGE_LIMIT ? `${text.slice(0, MESSAGE_LIMIT - 1)}…` : text;
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
