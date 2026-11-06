const FNV64_OFFSET = BigInt("0xcbf29ce484222325");
const FNV64_PRIME = BigInt("0x100000001b3");
const MASK64 = BigInt("0xffffffffffffffff");

const FNV32_OFFSET = 0x811c9dc5;
const FNV32_PRIME = 0x01000193;

export function fnv1a32(text: string): number;
export function fnv1a32(fields: readonly string[], semantics?: string): string;
export function fnv1a32(
  input: string | readonly string[],
  semantics = "",
): number | string {
  if (typeof input === "string") {
    let hash = FNV32_OFFSET;
    for (const byte of new TextEncoder().encode(input)) {
      hash = Math.imul(hash ^ byte, FNV32_PRIME) >>> 0;
    }
    return hash;
  }
  let hash = FNV32_OFFSET;
  for (const char of JSON.stringify([semantics, ...input])) {
    hash ^= char.charCodeAt(0);
    hash = Math.imul(hash, FNV32_PRIME) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/**
 * 64-bit FNV-1a over each name followed by `\n` (`src/ingest/bundle.rs` `fingerprint`).
 */
export function fnv1a64(names: readonly string[]): bigint {
  const encoder = new TextEncoder();
  let h = FNV64_OFFSET;
  for (const name of names) {
    for (const b of encoder.encode(`${name}\n`)) {
      h = ((h ^ BigInt(b)) * FNV64_PRIME) & MASK64;
    }
  }
  return h;
}

export interface HivePartitionDate {
  year: number;
  month: number;
  day: number;
  hour?: number;
}

/**
 * Parses `year=YYYY/month=MM/day=DD[/hour=HH]` from a Hive-partitioned object key.
 * Returns `null` on malformed or unrecognized paths.
 */
export function parseHivePartitionPath(key: string): HivePartitionDate | null {
  const match = key.match(
    /\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})(?:\/hour=(\d{2}))?\//,
  );
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = match[4] !== undefined ? Number(match[4]) : undefined;
  if (
    !Number.isFinite(year) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    (hour !== undefined && (hour < 0 || hour > 23))
  ) {
    return null;
  }
  return { year, month, day, hour };
}

/**
 * Parses a compact UTC timestamp `YYYYMMDDTHHMMSSZ` into epoch milliseconds.
 * Returns `null` if malformed.
 */
export function parseCompactTimestamp(compact: string): number | null {
  const match = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(compact);
  if (!match) return null;
  const [, y, mo, d, h, mi, s] = match;
  const month = Number(mo);
  const day = Number(d);
  const hour = Number(h);
  const minute = Number(mi);
  const second = Number(s);
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return null;
  }
  const ms = Date.UTC(Number(y), month - 1, day, hour, minute, second);
  return Number.isFinite(ms) ? ms : null;
}

/**
 * Extracts `[startMs, endMs]` from a request/summary/bundle Parquet object key
 * (`window=START-END-...parquet`, `compact=START-END-...parquet`, `summary=START-END-...parquet`,
 * `hour_bundle=HH-...parquet`, or `day_bundle=...parquet`). Returns `null` on malformed/unrecognized keys.
 */
export function parseParquetPartitionWindow(
  key: string,
): [number, number] | null {
  const windowMatch = key.match(
    /\/(?:window|compact|summary)=(\d+)-(\d+)-[^/]*\.parquet$/,
  );
  if (windowMatch) {
    const startMs = Number(windowMatch[1]) * 1000;
    const endMs = Number(windowMatch[2]) * 1000;
    return Number.isFinite(startMs) &&
      Number.isFinite(endMs) &&
      endMs >= startMs
      ? [startMs, endMs]
      : null;
  }
  const hourBundle = key.match(
    /\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/hour_bundle=(\d{2})-[^/]+\.parquet$/,
  );
  if (hourBundle) {
    const startMs = Date.UTC(
      Number(hourBundle[1]),
      Number(hourBundle[2]) - 1,
      Number(hourBundle[3]),
      Number(hourBundle[4]),
    );
    return Number.isFinite(startMs) ? [startMs, startMs + 3_600_000] : null;
  }
  const dayBundle = key.match(
    /\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/day_bundle=[^/]+\.parquet$/,
  );
  if (dayBundle) {
    const startMs = Date.UTC(
      Number(dayBundle[1]),
      Number(dayBundle[2]) - 1,
      Number(dayBundle[3]),
    );
    return Number.isFinite(startMs) ? [startMs, startMs + 86_400_000] : null;
  }
  return null;
}

export const keyWindowMs = parseParquetPartitionWindow;

export interface ParsedOpsParquetKey {
  key: string;
  table: string;
  hourMs: number;
  startMs: number | null;
  endMs: number | null;
  compact: boolean;
}

/**
 * Parses an ops telemetry Parquet key (`processed/{table}/year=.../hour=HH/{compact|window}=...parquet`).
 * Returns `null` on unrecognized or malformed keys.
 */
export function parseOpsParquetKey(
  key: string,
  expectedTable?: string,
): ParsedOpsParquetKey | null {
  const m =
    /^(?:[^/]+\/)?processed\/([^/]+)\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/hour=(\d{2})\/(compact|window)=(\d+)(?:-(\d+))?-[^/]+\.parquet$/.exec(
      key,
    );
  if (!m) return null;
  const [, table, y, mo, d, h, kind, first, second] = m;
  if (expectedTable && table !== expectedTable) return null;
  const hourMs = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h));
  if (!Number.isFinite(hourMs)) return null;
  const startMs = second ? Number(first) * 1000 : null;
  const endMs = second ? Number(second) * 1000 : null;
  return {
    key,
    table,
    hourMs,
    startMs,
    endMs,
    compact: kind === "compact",
  };
}
