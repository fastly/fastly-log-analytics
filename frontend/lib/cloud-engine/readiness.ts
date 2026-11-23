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

const SQL_KEYWORD_LIST = [
  "SELECT", "WHERE", "AS", "VALUES", "LIKE", "ILIKE", "SIMILAR", "IS", "IN", "EXISTS",
  "AND", "OR", "NOT", "XOR", "FROM", "JOIN", "ON", "USING", "BY", "CASE", "WHEN",
  "THEN", "ELSE", "END", "HAVING", "LIMIT", "OFFSET", "CAST", "TRY_CAST", "SET",
  "INTO", "WITH", "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "DISTINCT", "GROUP",
  "ORDER", "BETWEEN", "NULL", "TRUE", "FALSE", "OVER", "FILTER", "QUALIFY", "WINDOW",
  "ESCAPE",
] as const;
const SQL_KEYWORD_NO_APOSTROPHE = new Set<string>(SQL_KEYWORD_LIST);

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
  "BIT", "BITSTRING", "VARINT", "JSON", "LIST", "MAP", "STRUCT", "UNION", "ENUM", "ARRAY",
]);

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
  "Backend", "Header", "Failed", "Template", "Could", "Malformed", "Unexpected",
  "Expected", "Cannot", "Unable", "No", "Function", "Scalar", "Aggregate", "Table",
  "Column", "View", "Schema", "Database", "Index", "Macro", "Referenced", "Overflow",
  "Candidate", "Did", "Line", "Like", "Map", "List", "Struct", "Duplicate",
]);

const SAFE_DECODE_ENCODINGS = new Set<string>([
  "base64", "hex", "utf8", "ascii", "json", "blob", "latin1",
]);

const SECRET_STEMS = new Set<string>([
  "token", "key", "secret", "password", "passwd", "authorization",
  "auth", "bearer", "cookie", "signature", "sig", "credential", "credentials",
]);
const AUTH_SCHEMES = new Set<string>(["bearer", "basic", "digest", "token"]);

const LETTER_RE = /\p{L}/u;
const LOWER_LETTER_RE = /\p{Ll}/u;
const WORD_CHAR_RE = /[\p{L}\p{N}_]/u;
const CONTRACTION_RE = /^(?:s|t|d|m|re|ve|ll)$/i;
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
          return raw.slice(0, i);
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
        while (j < len && DOLLAR_TAG_CHAR_RE.test(raw[j]!)) j += 1;
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

function scanDuckDbType(s: string, pos: number): number {
  const len = s.length;
  let i = pos;
  while (i < len && (isAsciiAlpha(s[i]) || isAsciiDigit(s[i]) || s[i] === "_")) i += 1;
  const base = s.slice(pos, i).toUpperCase();
  if (!DUCKDB_BASE_TYPES.has(base)) return -1;
  if (i < len && s[i] === "(") {
    let depth = 1;
    i += 1;
    while (i < len && depth > 0 && s[i] !== "\n" && s[i] !== "\r") {
      if (s[i] === "(") depth += 1;
      else if (s[i] === ")") depth -= 1;
      i += 1;
    }
    if (depth !== 0) return -1;
  }
  while (i + 1 < len && s[i] === "[" && s[i + 1] === "]") {
    i += 2;
  }
  return i;
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

// Stage 1: Closed-vocabulary O(N) canonicalizer for DuckDB value/pattern/identifier templates.
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

  const mjIdx = s.indexOf("Malformed JSON");
  if (mjIdx !== -1) {
    const inputIdx = s.indexOf("Input:", mjIdx + 14);
    if (inputIdx !== -1) {
      return { text: `${s.slice(0, inputIdx + 6)} [redacted]`, canonicalized: true };
    }
  }

  const cpsIdx = s.indexOf("Could not parse string ");
  if (cpsIdx !== -1) {
    const accMarker = " according to format specifier ";
    const accIdx = s.lastIndexOf(accMarker);
    if (accIdx > cpsIdx) {
      return { text: `${s.slice(0, cpsIdx + 23)}[redacted]${accMarker}[redacted]`, canonicalized: true };
    }
  }

  const cncsMarker = "Could not convert string ";
  const cncsIdx = s.indexOf(cncsMarker);
  if (cncsIdx !== -1) {
    let searchEnd = s.length;
    while (searchEnd > cncsIdx) {
      const toIdx = s.lastIndexOf(" to ", searchEnd - 1);
      if (toIdx <= cncsIdx) break;
      const typeEnd = scanDuckDbType(s, toIdx + 4);
      if (typeEnd !== -1) {
        const afterLine = sliceFirstLine(s.slice(typeEnd));
        if (afterLine === "" || afterLine.startsWith(" when casting") || /^[,.;:)\s]*$/.test(afterLine)) {
          const typeText = s.slice(toIdx + 4, typeEnd);
          return {
            text: `${s.slice(0, cncsIdx + cncsMarker.length)}[redacted] to ${typeText}${sanitizeCastingSuffix(afterLine)}`,
            canonicalized: true,
          };
        }
      }
      searchEnd = toIdx;
    }
  }

  const typeIdx = s.indexOf("Type ");
  if (typeIdx !== -1) {
    const srcEnd = scanDuckDbType(s, typeIdx + 5);
    if (srcEnd !== -1 && s.startsWith(" with value ", srcEnd)) {
      const cantIdx = s.lastIndexOf(" can't be cast ");
      if (cantIdx > srcEnd) {
        const destMarker = "destination type ";
        const destIdx = s.indexOf(destMarker, cantIdx);
        if (destIdx !== -1) {
          const dstEnd = scanDuckDbType(s, destIdx + destMarker.length);
          if (dstEnd !== -1) {
            const bridge = s.slice(cantIdx, destIdx + destMarker.length);
            if (/^ can't be cast (?:to the |because the value is out of range for the )destination type $/.test(bridge)) {
              const dstText = s.slice(destIdx + destMarker.length, dstEnd);
              const afterLine = sliceFirstLine(s.slice(dstEnd));
              return {
                text: `${s.slice(0, srcEnd + 12)}[redacted]${bridge}${dstText}${sanitizeCastingSuffix(afterLine)}`,
                canonicalized: true,
              };
            }
          }
        }
      }
    }
  }

  const lower = s.toLowerCase();
  const ffIdx = lower.indexOf("field format:");
  const fvorIdx = lower.indexOf("field value out of range:");
  const fieldColonIdx = ffIdx !== -1 ? ffIdx + 12 : fvorIdx !== -1 ? fvorIdx + 24 : -1;
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
  }

  const cndsMarker = "Could not decode string ";
  const cndsIdx = s.indexOf(cndsMarker);
  if (cndsIdx !== -1) {
    let searchEnd = s.length;
    while (searchEnd > cndsIdx) {
      const asIdx = s.lastIndexOf(" as ", searchEnd - 1);
      if (asIdx <= cndsIdx) break;
      let k = asIdx + 4;
      while (k < s.length && (isAsciiAlpha(s[k]) || isAsciiDigit(s[k]) || s[k] === "_")) k += 1;
      const enc = s.slice(asIdx + 4, k).toLowerCase();
      if (SAFE_DECODE_ENCODINGS.has(enc) && (k === s.length || s[k] === ":" || /\s/.test(s[k]!))) {
        return { text: `${s.slice(0, cndsIdx + cndsMarker.length)}[redacted] as ${enc}`, canonicalized: true };
      }
      searchEnd = asIdx;
    }
  }

  const invIdx = lower.indexOf("invalid ");
  if (invIdx !== -1) {
    let p = invIdx + 8;
    let words = 0;
    let wordEnd = p;
    while (words < 3 && p < s.length) {
      while (p < s.length && s[p] === " ") p += 1;
      const ws = p;
      while (p < s.length && (isAsciiAlpha(s[p]) || s[p] === "_")) p += 1;
      if (p === ws || s[p] !== " ") break;
      const w = lower.slice(ws, p);
      if (w === "in" || w === "when" || w === "at" || w === "expected") break;
      words += 1;
      wordEnd = p;
    }
    if (words >= 1) {
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

  const wnMarker = " with name ";
  const dneMarker = " does not exist";
  const wnIdx = s.indexOf(wnMarker);
  if (wnIdx !== -1) {
    const dneIdx = s.indexOf(dneMarker, wnIdx + wnMarker.length);
    if (dneIdx !== -1) {
      const restLine = sliceFirstLine(s.slice(dneIdx));
      const bang = restLine.startsWith(`${dneMarker}!`) ? "!" : "";
      return { text: `${s.slice(0, wnIdx + wnMarker.length)}[redacted]${dneMarker}${bang}`, canonicalized: true };
    }
  }

  const ovIdx = s.indexOf("Overflow in ");
  if (ovIdx !== -1) {
    const lp = s.indexOf("(", ovIdx + 12);
    const rp = lp !== -1 ? s.lastIndexOf(")") : -1;
    if (lp !== -1 && rp > lp) {
      const bang = s[rp + 1] === "!" ? "!" : "";
      return { text: `${s.slice(0, lp)}([redacted])${bang}`, canonicalized: true };
    }
  }

  for (const tailMarker of [" at or near ", " match the pattern ", "JSON path error near "]) {
    const mIdx = s.indexOf(tailMarker);
    if (mIdx !== -1) {
      return { text: `${s.slice(0, mIdx + tailMarker.length)}[redacted]`, canonicalized: true };
    }
  }

  const dupIdx = s.indexOf("Duplicate key ");
  if (dupIdx !== -1) {
    const violIdx = s.indexOf(" violates ", dupIdx + 14);
    if (violIdx !== -1) {
      return {
        text: `${s.slice(0, dupIdx + 14)}[redacted]${sliceFirstLine(s.slice(violIdx))}`,
        canonicalized: true,
      };
    }
  }

  return { text: s, canonicalized: false };
}

function extractBoundedPrevWord(line: string, idx: number): string {
  let start = idx;
  const floor = Math.max(0, idx - 24);
  while (start > floor && WORD_CHAR_RE.test(line[start - 1]!)) {
    start -= 1;
  }
  if (start === floor && start > 0 && WORD_CHAR_RE.test(line[start - 1]!)) return "";
  return line.slice(start, idx);
}

function extractBoundedAfterLetters(
  line: string,
  idx: number,
): { letters: string; fullLen: number; atBoundary: boolean } {
  let end = idx;
  const ceil = Math.min(line.length, idx + 24);
  while (end < ceil && LETTER_RE.test(line[end]!)) {
    end += 1;
  }
  let possessEnd = end;
  if (end + 1 < line.length && line[end] === "'" && (line[end + 1] === "s" || line[end + 1] === "S")) {
    possessEnd = end + 2;
  }
  const nextCh = line[possessEnd];
  const atBoundary = nextCh === undefined || /[\s,.;:!?)\]}]/.test(nextCh);
  return { letters: line.slice(idx, end), fullLen: possessEnd - idx, atBoundary };
}

function isMidWordApostropheO1(line: string, pos: number): boolean {
  if (pos <= 0 || pos + 1 >= line.length) return false;
  if (!LETTER_RE.test(line[pos - 1]!) || !LETTER_RE.test(line[pos + 1]!)) return false;
  const stem = extractBoundedPrevWord(line, pos);
  if (!stem || !/^\p{L}+$/u.test(stem) || SQL_KEYWORD_NO_APOSTROPHE.has(stem.toUpperCase())) return false;
  const { letters, atBoundary } = extractBoundedAfterLetters(line, pos + 1);
  if (!atBoundary) return false;
  return CONTRACTION_RE.test(letters) || letters.length >= 2;
}

// Mark `\'` inside E'...' / U&'...' in one O(N) forward pass so suffix quote parity is exact.
function markEscapedSingleQuotes(line: string): Uint8Array | null {
  if (!line.includes("\\")) return null;
  const len = line.length;
  const escaped = new Uint8Array(len);
  let i = 0;
  while (i < len) {
    const prevWord = i > 0 && WORD_CHAR_RE.test(line[i - 1]!);
    const typedLen = prevWord
      ? 0
      : (line[i] === "U" || line[i] === "u") && line[i + 1] === "&" && line[i + 2] === "'"
        ? 3
        : (line[i] === "E" || line[i] === "e") && line[i + 1] === "'"
          ? 2
          : 0;
    if (typedLen > 0) {
      i += typedLen;
      while (i < len) {
        if (line[i] === "\\" && i + 1 < len) {
          if (line[i + 1] === "'") escaped[i + 1] = 1;
          i += 2;
          continue;
        }
        if (line[i] === "'") {
          if (line[i + 1] === "'") {
            i += 2;
            continue;
          }
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    i += 1;
  }
  return escaped;
}

function buildSuffixNonApostropheQuotes(line: string): Int32Array {
  const len = line.length;
  const escaped = markEscapedSingleQuotes(line);
  const suffix = new Int32Array(len + 1);
  let i = len - 1;
  while (i >= 0) {
    suffix[i] = suffix[i + 1]!;
    if (escaped && escaped[i] === 1) {
      i -= 1;
      continue;
    }
    if (line[i] === "'" && i > 0 && line[i - 1] === "'" && !(escaped && escaped[i - 1] === 1)) {
      suffix[i] = suffix[i + 1]! + 1;
      suffix[i - 1] = suffix[i]! + 1;
      i -= 2;
      continue;
    }
    if (line[i] === "'" && !isMidWordApostropheO1(line, i)) {
      suffix[i] = suffix[i + 1]! + 1;
    }
    i -= 1;
  }
  return suffix;
}

function isStandaloneWordApostropheO1(line: string, pos: number, suffixQuotes: Int32Array): boolean {
  const stem = extractBoundedPrevWord(line, pos);
  if (!stem || !/^\p{L}+$/u.test(stem) || SQL_KEYWORD_NO_APOSTROPHE.has(stem.toUpperCase())) {
    return false;
  }
  if (pos + 1 < line.length && LETTER_RE.test(line[pos + 1]!)) {
    const { letters, fullLen, atBoundary } = extractBoundedAfterLetters(line, pos + 1);
    if (!atBoundary) return false;
    if (CONTRACTION_RE.test(letters) || letters.length >= 2) {
      if (stem.length >= 2 || !isSqlPrefixChar(stem)) return true;
      return suffixQuotes[pos + 1 + fullLen]! % 2 === 0;
    }
  }
  if (
    stem.length >= 2
    && (stem.endsWith("s") || stem.endsWith("S"))
    && LOWER_LETTER_RE.test(stem[stem.length - 2]!)
    && (pos + 1 >= line.length || /\s/.test(line[pos + 1]!))
  ) {
    return true;
  }
  return false;
}

function redactPostColonDiagnosticTail(line1: string): string {
  const lower = line1.toLowerCase();
  let afterHeader = -1;
  if (lower.startsWith("invalid input error:")) afterHeader = 20;
  else if (lower.startsWith("conversion error:")) afterHeader = 17;
  else if (lower.startsWith("regex error:")) afterHeader = 12;
  if (afterHeader === -1) return line1;
  for (let i = afterHeader; i < line1.length; i += 1) {
    const ch = line1[i]!;
    if (ch === "'" || ch === '"' || ch === "`") break;
    if (line1.startsWith("[redacted]", i) || line1.startsWith("[url]", i)) break;
    if (ch === ":") {
      if (line1[i + 1] === ":") {
        i += 1;
        continue;
      }
      return `${line1.slice(0, i + 1)} [redacted]`;
    }
  }
  return line1;
}

// Stage 2A: O(N) strict pair-by-pair quote & URL scanner (never skips quotes across spans).
function redactQuotesAndUrlsLine(line1: string): string {
  const suffixQuotes = buildSuffixNonApostropheQuotes(line1);
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
    if (isAsciiAlpha(line1[i]) && (i === 0 || !WORD_CHAR_RE.test(line1[i - 1]!))) {
      let j = i + 1;
      while (
        j < len
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
    if (!prevWord && isSqlPrefixChar(line1[i]) && line1[i + 1] === "'" && isStandaloneWordApostropheO1(line1, i + 1, suffixQuotes)) {
      const { fullLen } = extractBoundedAfterLetters(line1, i + 2);
      const tok = line1.slice(i, i + 2 + fullLen);
      parts.push(tok);
      outChars += tok.length;
      i += 2 + fullLen;
      continue;
    }
    const typedLen = prevWord
      ? 0
      : (line1[i] === "U" || line1[i] === "u") && line1[i + 1] === "&" && line1[i + 2] === "'"
        ? 3
        : isSqlPrefixChar(line1[i]) && line1[i + 1] === "'"
          ? 2
          : 0;
    const isApostrophe = line1[i] === "'" && prevWord && isStandaloneWordApostropheO1(line1, i, suffixQuotes);
    if (typedLen > 0 && isStandaloneWordApostropheO1(line1, i + typedLen - 1, suffixQuotes)) {
      const { fullLen } = extractBoundedAfterLetters(line1, i + typedLen);
      const tok = line1.slice(i, i + typedLen + fullLen);
      parts.push(tok);
      outChars += tok.length;
      i += typedLen + fullLen;
      continue;
    }
    if (typedLen > 0 || (line1[i] === "'" && !isApostrophe)) {
      const firstCh = line1[i];
      const isEscapeSingle = typedLen > 0 && (firstCh === "E" || firstCh === "e" || firstCh === "U" || firstCh === "u");
      parts.push("[redacted]");
      outChars += 10;
      i += typedLen > 0 ? typedLen : 1;
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
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    if (line1[i] === '"' || line1[i] === "`") {
      const q = line1[i];
      parts.push("[redacted]");
      outChars += 10;
      i += 1;
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
          i += 1;
          break;
        }
        i += 1;
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

function tryScanClockTime(s: string, pos: number): number {
  if (pos > 0 && (isAsciiAlpha(s[pos - 1]) || isAsciiDigit(s[pos - 1]) || s[pos - 1] === ":")) return -1;
  if (pos + 8 > s.length) return -1;
  if (s[pos + 2] !== ":" || s[pos + 5] !== ":") return -1;
  const hh = s.slice(pos, pos + 2);
  const mm = s.slice(pos + 3, pos + 5);
  const ss = s.slice(pos + 6, pos + 8);
  if (!/^(?:[01]\d|2[0-3])$/.test(hh) || !/^[0-5]\d$/.test(mm) || !/^[0-5]\d$/.test(ss)) return -1;
  let end = pos + 8;
  if (s[end] === "." && isAsciiDigit(s[end + 1])) {
    end += 1;
    while (end < s.length && isAsciiDigit(s[end]) && end - (pos + 9) < 6) end += 1;
  }
  if (end < s.length && (isAsciiAlpha(s[end]) || isAsciiDigit(s[end]) || s[end] === ":" || s[end] === "%")) return -1;
  return end;
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
  const lower = word.toLowerCase();
  if (SECRET_STEMS.has(lower)) return true;
  const sep = Math.max(lower.lastIndexOf("_"), lower.lastIndexOf("-"));
  return sep > 0 && SECRET_STEMS.has(lower.slice(sep + 1));
}

function isAllowlistedCoreWord(word: string, isClauseStart: boolean, inErrorHeader: boolean): boolean {
  if (!word || word.length >= 20) return false;
  if (SAFE_BYTE_UNITS.has(word)) return true;
  if (DUCKDB_ALNUM_TYPES.has(word)) return true;
  if (/^\d{1,6}(?:\.\d{1,3})?$/.test(word)) return true;
  if (/^[a-z]{1,19}$/.test(word)) return true;
  if (/^[A-Z]{2,19}$/.test(word)) return SAFE_UPPERCASE_WORDS.has(word);
  if (inErrorHeader && SAFE_HEADER_WORDS.has(word)) return true;
  if (isClauseStart && SAFE_CLAUSE_START_WORDS.has(word)) return true;
  const aposIdx = word.indexOf("'");
  if (aposIdx > 0) {
    const stem = word.slice(0, aposIdx);
    const tail = word.slice(aposIdx + 1);
    if (!/^\p{L}{1,12}$/u.test(stem) || SQL_KEYWORD_NO_APOSTROPHE.has(stem.toUpperCase())) return false;
    if (
      tail === ""
      && stem.length >= 2
      && (stem.endsWith("s") || stem.endsWith("S"))
      && LOWER_LETTER_RE.test(stem[stem.length - 2]!)
    ) {
      return true;
    }
    if (/^\p{L}{1,12}(?:'s)?$/u.test(tail)) return true;
  }
  return false;
}

// Stage 2B: Single-pass O(N) allowlist structural token emitter.
function emitAllowlistedTokens(line: string): string {
  const headerColonIdx = detectSafeErrorHeaderEnd(line);
  const parts: string[] = [];
  let i = 0;
  const len = line.length;
  let isClauseStart = true;

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
      continue;
    }
    if (line.startsWith("[url]", i)) {
      parts.push("[url]");
      i += 5;
      isClauseStart = false;
      continue;
    }
    if (line.startsWith("(YYYY-MM-DD)", i)) {
      parts.push("(YYYY-MM-DD)");
      i += 12;
      isClauseStart = false;
      continue;
    }
    if (line.startsWith("DB::Exception", i) && (i + 13 === len || !WORD_CHAR_RE.test(line[i + 13]!))) {
      parts.push("DB::Exception");
      i += 13;
      isClauseStart = false;
      continue;
    }
    const clockEnd = tryScanClockTime(line, i);
    if (clockEnd !== -1) {
      parts.push(line.slice(i, clockEnd));
      i = clockEnd;
      isClauseStart = false;
      continue;
    }
    const v6End = tryScanIpv6(line, i);
    if (v6End !== -1) {
      parts.push("[redacted]");
      i = v6End;
      isClauseStart = false;
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
      continue;
    }
    const memMatch = /^(KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)\/(\d{1,6}(?:\.\d{1,3})?)(?=$|[\s,.;:!?)\]}])/.exec(
      line.slice(i, i + 24),
    );
    if (memMatch) {
      parts.push(memMatch[0]);
      i += memMatch[0].length;
      isClauseStart = false;
      continue;
    }
    if (ch === "(" || ch === ")" || ch === "[" || ch === "]" || ch === "," || ch === "+" || ch === "=") {
      parts.push(ch);
      i += 1;
      continue;
    }
    if (ch === ":" || ch === "." || ch === "!" || ch === "?") {
      if (ch === "." && i + 1 < len && !/\s/.test(line[i + 1]!)) {
        let j = i + 1;
        while (j < len && !/[\s,;:!?)\]}]/.test(line[j]!)) j += 1;
        parts.push("[redacted]");
        i = j;
        isClauseStart = false;
        continue;
      }
      parts.push(ch);
      i += 1;
      isClauseStart = true;
      continue;
    }
    if (ch === ";") {
      parts.push(" ");
      i += 1;
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
    if (eqIdx > 0 && isSecretKeyStem(rawTok.slice(0, eqIdx))) {
      const label = rawTok.slice(0, eqIdx);
      const safeLabel = /^[A-Za-z][A-Za-z0-9_-]{0,23}$/.test(label) ? label : "[redacted]";
      parts.push(`${safeLabel} [redacted]`);
      isClauseStart = false;
      continue;
    }
    if (isSecretKeyStem(rawTok)) {
      let p = i;
      if (line[p] === ":" || line[p] === "=") p += 1;
      while (p < len && line[p] === " ") p += 1;
      if (p < len && !line.startsWith("[redacted]", p) && !line.startsWith("[url]", p)) {
        let sEnd = p;
        while (sEnd < len && isAsciiAlpha(line[sEnd])) sEnd += 1;
        const maybeScheme = line.slice(p, sEnd).toLowerCase();
        if (AUTH_SCHEMES.has(maybeScheme) && line[sEnd] === " ") {
          p = sEnd;
          while (p < len && line[p] === " ") p += 1;
        }
        let valEnd = p;
        while (valEnd < len && !/[\s,;]/.test(line[valEnd]!)) valEnd += 1;
        if (valEnd > p) {
          const safeLabel = /^[A-Za-z][A-Za-z0-9_-]{0,23}$/.test(rawTok) ? rawTok : "[redacted]";
          parts.push(`${safeLabel} [redacted]`);
          i = valEnd;
          isClauseStart = false;
          continue;
        }
      }
    }

    const inHeader = headerColonIdx !== -1 && tokStart < headerColonIdx;
    parts.push(isAllowlistedCoreWord(rawTok, isClauseStart, inHeader) ? rawTok : "[redacted]");
    isClauseStart = false;
  }

  return parts
    .join("")
    .replace(/\[redacted\](?:\s*\[redacted\])+/g, "[redacted]")
    .replace(/\s+/g, " ")
    .trim();
}

// Error messages can carry SQL, URLs, keys, IPs, or raw row/regex payloads:
// canonicalize known DuckDB error frames in O(N), restrict to line 1, and emit
// only allowlisted structural diagnostic tokens.
function redactedMessage(message: string): string {
  const { text: stage1, canonicalized } = canonicalizeDuckDbTemplates(message);
  const line1Raw = sliceFirstLine(stage1);
  const firstLine = canonicalized ? line1Raw : redactPostColonDiagnosticTail(line1Raw);
  const quotedRedacted = redactQuotesAndUrlsLine(firstLine);
  const text = emitAllowlistedTokens(quotedRedacted);
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
