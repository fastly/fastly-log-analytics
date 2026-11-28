import { parquetMetadata, type SchemaElement } from "hyparquet";
import { readColumns } from "./dashboard/parquet";
import {
  emptyAgg,
  type Rollup,
  type RollupAgg,
  type RollupOverview,
  type RollupResidual,
  type RollupValue,
} from "./dashboard/rollup-query";

export const VALUE_RAW_ALIASES = [
  "cache_status", "resp_body_size", "resp_body_bytes", "time_elapsed_usec", "protocol", "is_ssl",
  "tls_version", "ipv6", "waf_action", "ngwaf_decision", "fastly_is_shield", "is_shield", "content_type",
  "resp_header_content_type", "bot_verified", "verified_bot", "ngwaf_bot_name", "bot_category", "is_bot",
  "ofmt", "ifmt",
] as const;

export const VALUE_ALL_FIELDS_BASE = [
  ...VALUE_RAW_ALIASES, "waf", "cache", "proto", "tls", "is_ipv6", "io_output_format",
] as const;

const SERIES_STEP_MS = 300_000;
const DAY_MS = 86_400_000;

const PRESENT = ["value/cache", "value/shield"] as const;
const KEYS = new Set<string>([
  "overview",
  "series5/overall",
  "series5/proto",
  ...PRESENT,
  ...VALUE_ALL_FIELDS_BASE.map((f) => `all_fields/${f}`),
]);

const COLUMNS = [
  "timestamp",
  "resp_bytes",
  "elapsed",
  "edge",
  ...VALUE_ALL_FIELDS_BASE,
] as const;

type Kind = "s" | "i" | "f" | "b" | "ts" | null;

function kindOf(element: SchemaElement): Kind {
  const logical = element.logical_type;
  switch (element.type) {
    case "BOOLEAN":
      return "b";
    case "BYTE_ARRAY":
      return logical?.type === "STRING" || element.converted_type === "UTF8" ? "s" : null;
    case "INT64":
      if (logical?.type === "TIMESTAMP") return logical.unit === "MILLIS" ? "ts" : null;
      if (element.converted_type === "TIMESTAMP_MILLIS") return "ts";
      if (element.converted_type === "TIMESTAMP_MICROS") return null;
      if (logical?.type === "INTEGER" && !logical.isSigned) return null;
      return "i";
    case "INT32":
      return "i";
    case "DOUBLE":
    case "FLOAT":
      return "f";
    default:
      return null;
  }
}

function toFiniteNumber(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "bigint") return Number(v);
  if (v instanceof Date) return Number.isFinite(v.getTime()) ? v.getTime() : null;
  if (typeof v === "string") {
    const trimmed = v.trim();
    if (!trimmed) return null;
    const n = Number(trimmed);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function toBool(v: unknown): boolean | null {
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "bigint") return v !== BigInt(0);
  if (typeof v === "string") {
    const l = v.trim().toLowerCase();
    if (l === "true" || l === "1") return true;
    if (l === "false" || l === "0") return false;
  }
  return null;
}

function fieldValueString(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "string") return v;
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "number" || typeof v === "bigint") return String(v);
  return null;
}

function cacheClass(cache: string | null): string {
  if (!cache) return "";
  const upper = cache.toUpperCase();
  if (upper.startsWith("HIT")) return "HIT";
  if (upper.startsWith("MISS")) return "MISS";
  if (upper.startsWith("PASS")) return "PASS";
  if (upper.startsWith("SYNTH")) return "SYNTH";
  return "OTHER";
}

function emptyValue(): RollupValue {
  return {
    requests: 0,
    respBytes: 0,
    bytesCount: 0,
    elapsedSum: 0,
    elapsedCount: 0,
    ioIn: 0,
    ioOut: 0,
    ratioSum: 0,
    ratioCount: 0,
  };
}

function getOrCreateValue(
  table: Map<string, Map<string, Map<number, RollupValue>>>,
  field: string,
  value: string,
  day: number,
): RollupValue {
  let byVal = table.get(field);
  if (!byVal) {
    byVal = new Map();
    table.set(field, byVal);
  }
  let byDay = byVal.get(value);
  if (!byDay) {
    byDay = new Map();
    byVal.set(value, byDay);
  }
  let sums = byDay.get(day);
  if (!sums) {
    sums = emptyValue();
    byDay.set(day, sums);
  }
  return sums;
}

function getOrCreateSeriesBucket(
  series: Map<string, Map<string, Map<number, RollupAgg>>>,
  field: string,
  value: string,
  bucket: number,
): RollupAgg {
  let byVal = series.get(field);
  if (!byVal) {
    byVal = new Map();
    series.set(field, byVal);
  }
  let byBucket = byVal.get(value);
  if (!byBucket) {
    byBucket = new Map();
    byVal.set(value, byBucket);
  }
  let agg = byBucket.get(bucket);
  if (!agg) {
    agg = emptyAgg();
    byBucket.set(bucket, agg);
  }
  return agg;
}

function bumpField(fields: Map<string, Map<string, number>>, field: string, value: string) {
  let counts = fields.get(field);
  if (!counts) {
    counts = new Map<string, number>();
    fields.set(field, counts);
  }
  counts.set(value, (counts.get(value) ?? 0) + 1);
}

/**
 * Rolls up unsummarized raw Parquet tail/edge files for the `/fastly-value`
 * Summary tab (`overview`, `caching`, `network`) so fresh minutes stay on the
 * summary path instead of falling back to raw DuckDB.
 */
export const VALUE_SUMMARY_TAB_RESIDUAL: RollupResidual = {
  id: "value-summary-tab-residual-v1",
  columns: COLUMNS,
  maxBytes: 64 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const series = new Map<string, Map<string, Map<number, RollupAgg>>>();
    const fields = new Map<string, Map<string, number>>();
    const value = new Map<string, Map<string, Map<number, RollupValue>>>();
    const overview: RollupOverview = {
      requests: 0,
      totalBandwidth: 0,
      threatsBlocked: 0,
      hitElapsedSum: 0,
      hitElapsedCount: 0,
      missElapsedSum: 0,
      missElapsedCount: 0,
    };
    return {
      async add(file) {
        const kinds = new Map<string, Kind>(
          parquetMetadata(file).schema.slice(1).map((element) => [element.name, kindOf(element)]),
        );
        if (kinds.get("timestamp") !== "ts") return;
        const presentCols = COLUMNS.filter((name) => kinds.get(name));
        const { rows, columns } = await readColumns(file, presentCols);
        const empty: ArrayLike<unknown> = { length: rows };
        const ts = columns.get("timestamp") ?? empty;
        const respBytesCol = columns.get("resp_bytes") ?? empty;
        const elapsedCol = columns.get("elapsed") ?? empty;
        const edgeCol = columns.get("edge") ?? empty;
        const cacheCol = columns.get("cache") ?? empty;
        const protoCol = columns.get("proto") ?? empty;
        const wafCol = columns.get("waf") ?? empty;
        const presentFieldCols = VALUE_ALL_FIELDS_BASE
          .map((name) => [name, columns.get(name)] as const)
          .filter((entry): entry is readonly [typeof VALUE_ALL_FIELDS_BASE[number], ArrayLike<unknown>] => entry[1] !== undefined);

        for (let i = 0; i < rows; i++) {
          const tsMs = toFiniteNumber(ts[i]);
          if (tsMs === null || tsMs < fromMs || tsMs >= toMs) continue;

          const body = toFiniteNumber(respBytesCol[i]);
          const bodyInt = body !== null ? Math.trunc(body) : 0;
          const elapsedVal = toFiniteNumber(elapsedCol[i]);
          const edgeBool = toBool(edgeCol[i]);
          const wafBool = toBool(wafCol[i]);
          const cacheCell = cacheCol[i];
          const rawCache = typeof cacheCell === "string" && cacheCell.length > 0 ? cacheCell : null;
          const cls = cacheClass(rawCache);
          const protoCell = protoCol[i];
          const protoStr = typeof protoCell === "string" && protoCell.length > 0 ? protoCell : "Other";

          overview.requests += 1;
          overview.totalBandwidth += bodyInt;
          if (wafBool === true) overview.threatsBlocked += 1;
          if (elapsedVal !== null) {
            if (rawCache === "HIT" || rawCache === "HIT-STALE" || rawCache === "HIT-CLUSTER") {
              overview.hitElapsedSum += elapsedVal;
              overview.hitElapsedCount += 1;
            } else if (rawCache === "MISS") {
              overview.missElapsedSum += elapsedVal;
              overview.missElapsedCount += 1;
            }
          }

          const step = Math.floor(tsMs / SERIES_STEP_MS) * SERIES_STEP_MS;
          const overallAgg = getOrCreateSeriesBucket(series, "overall", "", step);
          overallAgg.requests += 1;
          overallAgg.respBytes += bodyInt;
          const protoAgg = getOrCreateSeriesBucket(series, "proto", protoStr, step);
          protoAgg.requests += 1;
          protoAgg.respBytes += bodyInt;

          const day = Math.floor(tsMs / DAY_MS) * DAY_MS;
          const vCache = getOrCreateValue(value, "cache", cls, day);
          vCache.requests += 1;
          if (body !== null) {
            vCache.respBytes += bodyInt;
            vCache.bytesCount += 1;
          }
          if (elapsedVal !== null) {
            vCache.elapsedSum += elapsedVal;
            vCache.elapsedCount += 1;
          }
          if (edgeBool === false) {
            getOrCreateValue(value, "shield", cls, day).requests += 1;
          }

          for (const [name, col] of presentFieldCols) {
            const sv = fieldValueString(col[i]);
            if (sv !== null) bumpField(fields, name, sv);
          }
        }
      },
      finish(): Rollup {
        return {
          dims: new Map(),
          series,
          seriesStepMs: SERIES_STEP_MS,
          fields,
          truncated: new Set(),
          groups: new Map(),
          present: new Set(PRESENT),
          overview,
          value,
        };
      },
    };
  },
};
