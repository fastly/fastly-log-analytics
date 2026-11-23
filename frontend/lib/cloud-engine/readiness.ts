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
const SECRET_WORD = String.raw`(?:\w+[_-])?(?:token|key|secret|passw(?:or)?d|authorization|auth|bearer|cookie|signature|sig|credentials?)`;
const DOLLAR_QUOTE_TAG_RE = /^\$([A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/;
const WORD_CHAR_RE = /[\p{L}\p{N}_]/u;
const LETTER_CHAR_RE = /\p{L}/u;
const SQL_PREFIX_CHAR_RE = /^[EeBbXxUuNnRr]$/;
const CONTRACTION_SUFFIX_RE = /^(?:s|t|d|m|re|ve|ll)(?=$|[\s,.;:!?)\]}])/iu;
const NAME_APOSTROPHE_AFTER_RE = /^(\p{L}{2,}(?:'s)?)(?=$|[\s,.;:!?)\]}])/iu;
const PLURAL_POSSESSIVE_STEM_RE = /\p{Ll}s$/u;
const ADJACENT_SQL_OP_SINGLE_RE = /^\s*(?:!=|<>|<=|>=|\|\||[,=<>+\-*/;([[])\s*(?:(?:[EeBbXxUuNnRr]|U&|u&)(?='))?'/;
const ADJACENT_SQL_OP_DOUBLE_RE = /^\s*(?:!=|<>|<=|>=|\|\||[,=<>+\-*/;([[])\s*"/;
const SQL_KEYWORD_LIST = [
  "SELECT", "WHERE", "AS", "VALUES", "LIKE", "ILIKE", "SIMILAR", "IS", "IN", "EXISTS",
  "AND", "OR", "NOT", "XOR", "FROM", "JOIN", "ON", "USING", "BY", "CASE", "WHEN",
  "THEN", "ELSE", "END", "HAVING", "LIMIT", "OFFSET", "CAST", "TRY_CAST", "SET",
  "INTO", "WITH", "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "DISTINCT", "GROUP",
  "ORDER", "BETWEEN", "NULL", "TRUE", "FALSE", "OVER", "FILTER", "QUALIFY", "WINDOW",
  "ESCAPE",
] as const;
const SQL_KEYWORD_NO_APOSTROPHE = new Set<string>(SQL_KEYWORD_LIST);
const PROSE_CONNECTOR_BEFORE_SINGLE_RE = new RegExp(
  String.raw`^[,.;:!?)\]}]*\s+(?:[a-z]+(?:\s+[a-z]+){0,4})\s+(?:(?:[EeBbXxUuNnRr]|U&|u&|${SQL_KEYWORD_LIST.join("|")})(?='))?'`,
  "i",
);
const DUCKDB_DIAGNOSTIC_TAIL_RE = /\r?\nLINE \d+:[\s\S]*$/;
const DUCKDB_CONVERT_TYPE =
  String.raw`(?:BOOL(?:EAN)?|(?:U?TINY|U?SMALL|U?BIG|HUGE|UHUGE)?INT(?:EGER|8|16|32|64|128)?|FLOAT(?:4|8)?|DOUBLE|REAL|DECIMAL|NUMERIC|VARCHAR|CHAR|BPCHAR|TEXT|STRING|BLOB|BYTEA|UUID|DATE|TIME(?:STAMP(?:_S|_MS|_NS|_TZ|TZ)?)?|TIMETZ|INTERVAL|BIT|BITSTRING|VARINT|JSON|LIST|MAP|STRUCT|UNION|ENUM|ARRAY)(?:\([^)\r\n]*\))?(?:\[\])*`;
const DUCKDB_CONVERT_STRING_RE = new RegExp(
  String.raw`(Could not convert string )'[\s\S]*'( to ${DUCKDB_CONVERT_TYPE}(?: when casting\b|[\s:.,;)\r\n]|$))`,
  "g",
);
const DUCKDB_CANT_CAST_VALUE_RE =
  /(Type [A-Z][A-Z0-9_]*(?:\([^)\r\n]*\))?(?:\[\])* with value )'[\s\S]*'( can't be cast to the destination type )/g;
const DUCKDB_FIELD_FORMAT_DQ_RE =
  /(invalid [a-z_ ]*field format:\s*)"[\s\S]*"((?:,\s*expected format\b|\s+when casting\b|\s+in column\b|$))/gi;
const DUCKDB_PARSE_SPECIFIER_DQ_RE =
  /(Could not parse string )"[\s\S]*"( according to format specifier )"[\s\S]*?"/g;
const DUCKDB_MALFORMED_JSON_DQ_RE = /(Malformed JSON[^\r\n]*?\bInput:\s*)"[\s\S]*"/g;
const DUCKDB_INVALID_VALUE_SPAN_RE =
  /(invalid (?:[A-Za-z_]+\s+){0,3}?)(?:'[\s\S]*'|"[\s\S]*")(\s+(?:in column|when casting|at position|expected)\b)/gi;
const DUCKDB_REGEX_INPUT_RE = /(missing \]:\s*)\S+/g;
const IPV4_OCTET = String.raw`(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)`;
const IPV4_BODY = String.raw`(?:${IPV4_OCTET}\.){3}${IPV4_OCTET}`;
const IPV4_LITERAL_RE = new RegExp(String.raw`(?<![0-9.])${IPV4_BODY}(?![0-9.])`, "g");
const IPV6_ZONE = String.raw`(?:%[0-9A-Za-z._-]+)?`;
const IPV6_LITERAL_RE = new RegExp(
  String.raw`(?<![0-9A-Za-z:])(?:` +
    String.raw`(?:[0-9a-fA-F]{1,4}:){6}${IPV4_BODY}${IPV6_ZONE}(?::\d{1,5})?` +
    String.raw`|::(?:[0-9a-fA-F]{1,4}:){0,5}${IPV4_BODY}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,5}:(?:[0-9a-fA-F]{1,4}:){0,4}${IPV4_BODY}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}${IPV6_ZONE}(?::\d{1,5})?` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,7}:${IPV6_ZONE}` +
    String.raw`|:(?::[0-9a-fA-F]{1,4}){1,7}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}${IPV6_ZONE}` +
    String.raw`|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}${IPV6_ZONE}` +
    String.raw`|[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}${IPV6_ZONE}` +
    String.raw`)(?![0-9A-Za-z:])`,
  "gu",
);

function extractPrevWordToken(text: string, idx: number): string {
  let start = idx;
  while (start > 0 && WORD_CHAR_RE.test(text[start - 1])) {
    start -= 1;
  }
  return text.slice(start, idx);
}

function isMidWordLetterApostrophe(text: string, pos: number): boolean {
  if (pos <= 0 || pos + 1 >= text.length) return false;
  if (!LETTER_CHAR_RE.test(text[pos - 1]) || !LETTER_CHAR_RE.test(text[pos + 1])) return false;
  const stem = extractPrevWordToken(text, pos);
  if (!stem || SQL_KEYWORD_NO_APOSTROPHE.has(stem.toUpperCase())) return false;
  const after = text.slice(pos + 1);
  return CONTRACTION_SUFFIX_RE.test(after) || NAME_APOSTROPHE_AFTER_RE.test(after);
}

function countRemainingNonApostropheQuotes(text: string, fromIdx: number): number {
  let count = 0;
  let j = fromIdx;
  while (j < text.length) {
    if (text[j] === "\\" && j + 1 < text.length && text[j + 1] === "\\") {
      j += 2;
      continue;
    }
    if (text[j] === "'") {
      if (j + 1 < text.length && text[j + 1] === "'") {
        count += 2;
        j += 2;
        continue;
      }
      if (!isMidWordLetterApostrophe(text, j)) {
        count += 1;
      }
    }
    j += 1;
  }
  return count;
}

function countRemainingDoubleQuotes(text: string, fromIdx: number): number {
  let count = 0;
  let j = fromIdx;
  while (j < text.length) {
    if (text[j] === "\\") {
      j += 2;
      continue;
    }
    if (text[j] === '"') {
      if (j + 1 < text.length && text[j + 1] === '"') {
        count += 2;
        j += 2;
        continue;
      }
      count += 1;
    }
    j += 1;
  }
  return count;
}

function isStandaloneWordApostropheAt(text: string, quoteIdx: number): boolean {
  const prevToken = extractPrevWordToken(text, quoteIdx);
  if (!prevToken || SQL_KEYWORD_NO_APOSTROPHE.has(prevToken.toUpperCase())) {
    return false;
  }
  const nl = text.indexOf("\n", quoteIdx);
  const lineEnd = nl === -1 ? text.length : nl;
  const lineStart = text.lastIndexOf("\n", quoteIdx - 1) + 1;
  const line = text.slice(lineStart, lineEnd);
  const linePos = quoteIdx - lineStart;
  const afterOnLine = line.slice(linePos + 1);
  const nameMatch = /^\p{L}+$/u.test(prevToken) ? NAME_APOSTROPHE_AFTER_RE.exec(afterOnLine) : null;
  if (CONTRACTION_SUFFIX_RE.test(afterOnLine)) {
    return countRemainingNonApostropheQuotes(line, linePos + 1) % 2 === 0;
  }
  if (nameMatch) {
    return countRemainingNonApostropheQuotes(line, linePos + 1 + nameMatch[1].length) % 2 === 0;
  }
  if (
    prevToken.length >= 2 &&
    PLURAL_POSSESSIVE_STEM_RE.test(prevToken) &&
    (quoteIdx + 1 >= lineEnd || /\s/.test(text[quoteIdx + 1]!))
  ) {
    return countRemainingNonApostropheQuotes(line, linePos + 1) % 2 === 0;
  }
  return false;
}

function preRedactDuckDbTemplates(rawInput: string): string {
  return rawInput
    .replace(DUCKDB_DIAGNOSTIC_TAIL_RE, "")
    .replace(DUCKDB_CONVERT_STRING_RE, "$1[redacted]$2")
    .replace(DUCKDB_CANT_CAST_VALUE_RE, "$1[redacted]$2")
    .replace(DUCKDB_FIELD_FORMAT_DQ_RE, "$1[redacted]$2")
    .replace(DUCKDB_PARSE_SPECIFIER_DQ_RE, "$1[redacted]$2[redacted]")
    .replace(DUCKDB_MALFORMED_JSON_DQ_RE, "$1[redacted]")
    .replace(DUCKDB_INVALID_VALUE_SPAN_RE, "$1[redacted]$2")
    .replace(DUCKDB_REGEX_INPUT_RE, "$1[redacted]");
}

function redactQuotedLiterals(rawInput: string): string {
  const input = preRedactDuckDbTemplates(rawInput);
  let out = "";
  let i = 0;
  let openParenDepth = 0;
  const len = input.length;
  while (i < len) {
    const ch = input[i]!;
    if (ch === "(" || ch === "[") {
      openParenDepth += 1;
      out += ch;
      i += 1;
      continue;
    }
    if (ch === ")" || ch === "]") {
      openParenDepth = Math.max(0, openParenDepth - 1);
      out += ch;
      i += 1;
      continue;
    }
    if (ch === "$") {
      const match = DOLLAR_QUOTE_TAG_RE.exec(input.slice(i));
      if (match) {
        const tag = match[0];
        const endIdx = input.indexOf(tag, i + tag.length);
        out += "[redacted]";
        i = endIdx === -1 ? len : endIdx + tag.length;
        continue;
      }
    }
    const prevToken = extractPrevWordToken(input, i);
    const prevWord = prevToken.length > 0;
    const isPrefixLetterApostrophe =
      !prevWord &&
      SQL_PREFIX_CHAR_RE.test(ch) &&
      input[i + 1] === "'" &&
      isStandaloneWordApostropheAt(input, i + 1);
    if (isPrefixLetterApostrophe) {
      out += ch + "'";
      i += 2;
      continue;
    }
    const typedSingleLen = prevWord
      ? 0
      : (ch === "U" || ch === "u") && input[i + 1] === "&" && input[i + 2] === "'"
        ? 3
        : SQL_PREFIX_CHAR_RE.test(ch) && input[i + 1] === "'"
          ? 2
          : 0;
    const isApostrophe = ch === "'" && prevWord && isStandaloneWordApostropheAt(input, i);
    if (typedSingleLen > 0 || (ch === "'" && !isApostrophe)) {
      const isEscapeSingle = typedSingleLen > 0 && (ch === "E" || ch === "e" || ch === "U" || ch === "u");
      out += "[redacted]";
      i += typedSingleLen > 0 ? typedSingleLen : 1;
      while (i < len) {
        if (input[i] === "\\" && input[i + 1] === "\\") {
          i += 2;
          continue;
        }
        if (isEscapeSingle && input[i] === "\\") {
          i += 2;
          continue;
        }
        if (input[i] === "'") {
          if (input[i + 1] === "'") {
            i += 2;
            continue;
          }
          const rest = input.slice(i + 1);
          if (rest.length > 0 && !ADJACENT_SQL_OP_SINGLE_RE.test(rest)) {
            const remQuotes = countRemainingNonApostropheQuotes(input, i + 1);
            if (remQuotes > 0 && !PROSE_CONNECTOR_BEFORE_SINGLE_RE.test(rest)) {
              i += 1;
              continue;
            }
            if (
              remQuotes === 0 &&
              openParenDepth > 0 &&
              !rest.includes(")") &&
              !rest.includes("]")
            ) {
              i += 1;
              continue;
            }
          }
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    if (ch === '"' || ch === "`") {
      const quote = ch;
      out += "[redacted]";
      i += 1;
      while (i < len) {
        if (input[i] === "\\") {
          i += 2;
          continue;
        }
        if (input[i] === quote) {
          if (input[i + 1] === quote) {
            i += 2;
            continue;
          }
          if (quote === '"') {
            const rest = input.slice(i + 1);
            if (rest.length > 0 && !ADJACENT_SQL_OP_DOUBLE_RE.test(rest)) {
              const remDq = countRemainingDoubleQuotes(input, i + 1);
              if (remDq % 2 === 1) {
                i += 1;
                continue;
              }
            }
          }
          i += 1;
          break;
        }
        i += 1;
      }
      continue;
    }
    out += ch;
    i += 1;
  }
  return out;
}

// Error messages can carry SQL, URLs, keys or tokens: keep the first line,
// mask anything that may be private, and bound what is left.
function redactedMessage(message: string): string {
  const preRedacted = redactQuotedLiterals(message);
  const text = (preRedacted.split(/\r?\n/, 1)[0] ?? "")
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s'"`]+/gi, "[url]")
    .replace(/\S+@\S+/g, "[redacted]")
    .replace(IPV6_LITERAL_RE, "[redacted]")
    .replace(IPV4_LITERAL_RE, "[redacted]")
    .replace(
      new RegExp(
        String.raw`\b(${SECRET_WORD})\b\s*[:=]?\s*(?:(?:bearer|basic|digest|token)\s+)?[^\s,;]+`,
        "gi",
      ),
      "$1 [redacted]",
    )
    .replace(/[\w+/=.-]{20,}/g, "[redacted]")
    .replace(/\s+/g, " ")
    .trim();
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
