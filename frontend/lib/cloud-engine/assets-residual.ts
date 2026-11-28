import { parquetMetadata, type SchemaElement } from "hyparquet";
import { readColumns } from "./dashboard/parquet";
import type {
  Rollup,
  RollupAsset,
  RollupOverview,
  RollupResidual,
} from "./dashboard/rollup-query";

const SERIES_STEP_MS = 300_000;

const KEYS = new Set<string>([
  "overview",
  "assets/asset",
  "all_fields/cache",
  "all_fields/cache_status",
]);

const COLUMNS = [
  "timestamp",
  "url",
  "path",
  "cache_status",
  "cache",
  "content_encoding",
  "resp_header_content_encoding",
  "resp_body_size",
  "resp_bytes",
  "ttfb_ms",
  "ttfb",
  "time_to_first_byte_ns",
  "time_elapsed_usec",
  "elapsed",
  "origin_Whole_ms",
  "origin_time_ms",
] as const;

type Kind = "s" | "i" | "f" | "ts" | null;

function kindOf(element: SchemaElement): Kind {
  const logical = element.logical_type;
  switch (element.type) {
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

function strOrNull(v: unknown, nonEmpty = false): string | null {
  if (typeof v !== "string") return null;
  if (nonEmpty && v.length === 0) return null;
  return v;
}

const IMAGE_EXTS = [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"];
const DOC_EXTS = [
  ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".csv",
  ".zip", ".tar", ".gz", ".tgz", ".rar", ".7z",
];
const JS_CSS_EXTS = [".js", ".mjs", ".css"];
const FONT_EXTS = [".woff", ".woff2", ".ttf", ".otf", ".eot"];
const VIDEO_EXTS = [".m3u8", ".ts", ".mp4", ".m4s", ".webm"];
const COMPRESSIBLE_EXTS = [".js", ".mjs", ".css", ".svg", ".html", ".json"];

function endsWithAny(lower: string, exts: readonly string[]): boolean {
  for (const ext of exts) {
    if (lower.endsWith(ext)) return true;
  }
  return false;
}

function urlAssetType(url: string): string {
  const lower = url.toLowerCase();
  if (endsWithAny(lower, IMAGE_EXTS)) return "Images";
  if (endsWithAny(lower, DOC_EXTS)) return "Documents";
  if (endsWithAny(lower, JS_CSS_EXTS)) return "JavaScript/CSS";
  if (endsWithAny(lower, FONT_EXTS)) return "Fonts";
  if (endsWithAny(lower, VIDEO_EXTS)) return "Video";
  return "API/Dynamic";
}

function urlCompressible(url: string): number {
  return endsWithAny(url.toLowerCase(), COMPRESSIBLE_EXTS) ? 1 : 0;
}

function encodingNorm(encoding: string | null): string {
  if (!encoding) return "uncompressed";
  const lower = encoding.toLowerCase();
  if (lower.includes("br")) return "br";
  if (lower.includes("gzip")) return "gzip";
  return encoding;
}

function encodingCompressed(encoding: string | null): number {
  if (!encoding) return 0;
  const lower = encoding.toLowerCase();
  return lower === "gzip" || lower === "br" || lower === "deflate" ? 1 : 0;
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
 * Rolls up unsummarized raw Parquet tail/edge files into the `/assets-shield`
 * summary groups (`overview`, `assets/asset`, `all_fields/cache`) so fresh
 * minutes stay on the summary path instead of falling back to raw DuckDB.
 */
export const ASSETS_RESIDUAL: RollupResidual = {
  id: "assets-residual-v1",
  columns: COLUMNS,
  maxBytes: 64 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const assets = new Map<string, RollupAsset>();
    const fields = new Map<string, Map<string, number>>();
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
        const { rows, columns } = await readColumns(file, COLUMNS.filter((name) => kinds.get(name)));
        const empty: ArrayLike<unknown> = { length: rows };
        const ts = columns.get("timestamp") ?? empty;
        const urlCol = columns.get("url") ?? empty;
        const pathCol = columns.get("path") ?? empty;
        const cacheStatusCol = columns.get("cache_status") ?? empty;
        const cacheCol = columns.get("cache") ?? empty;
        const encCol = columns.get("content_encoding") ?? empty;
        const respEncCol = columns.get("resp_header_content_encoding") ?? empty;
        const bodySizeCol = columns.get("resp_body_size") ?? empty;
        const respBytesCol = columns.get("resp_bytes") ?? empty;
        const ttfbMsCol = columns.get("ttfb_ms") ?? empty;
        const ttfbCol = columns.get("ttfb") ?? empty;
        const ttfbNsCol = columns.get("time_to_first_byte_ns") ?? empty;
        const elapsedUsecCol = columns.get("time_elapsed_usec") ?? empty;
        const elapsedCol = columns.get("elapsed") ?? empty;
        const originWholeCol = columns.get("origin_Whole_ms") ?? empty;
        const originTimeCol = columns.get("origin_time_ms") ?? empty;

        for (let i = 0; i < rows; i++) {
          const tsMs = toFiniteNumber(ts[i]);
          if (tsMs === null || tsMs < fromMs || tsMs >= toMs) continue;

          overview.requests += 1;

          const rawCacheStatus = strOrNull(cacheStatusCol[i]);
          if (rawCacheStatus !== null) bumpField(fields, "cache_status", rawCacheStatus);
          const rawCache = strOrNull(cacheCol[i]);
          if (rawCache !== null) bumpField(fields, "cache", rawCache);

          const url = strOrNull(urlCol[i]) ?? strOrNull(pathCol[i]) ?? "";
          const cacheEff = strOrNull(cacheStatusCol[i], true) ?? strOrNull(cacheCol[i], true) ?? "OTHER";
          const enc = strOrNull(encCol[i], true) ?? strOrNull(respEncCol[i], true);

          const key = JSON.stringify([
            urlAssetType(url),
            cacheEff.toUpperCase(),
            encodingNorm(enc),
            encodingCompressed(enc),
            urlCompressible(url),
          ]);
          let agg = assets.get(key);
          if (!agg) {
            agg = { requests: 0, respBytes: 0, ttfbSum: 0, originWaitSum: 0, originRequests: 0 };
            assets.set(key, agg);
          }

          const body = toFiniteNumber(bodySizeCol[i]) ?? toFiniteNumber(respBytesCol[i]) ?? 0;
          const ttfbMs =
            toFiniteNumber(ttfbMsCol[i]) ??
            (() => {
              const s = toFiniteNumber(ttfbCol[i]);
              return s !== null ? s * 1000 : null;
            })() ??
            (() => {
              const ns = toFiniteNumber(ttfbNsCol[i]);
              return ns !== null ? ns * 1e-6 : null;
            })() ??
            (() => {
              const us = toFiniteNumber(elapsedUsecCol[i]) ?? toFiniteNumber(elapsedCol[i]);
              return us !== null ? us * 1e-3 : null;
            })() ??
            0;
          const originMs = toFiniteNumber(originWholeCol[i]) ?? toFiniteNumber(originTimeCol[i]) ?? 0;

          agg.requests += 1;
          agg.respBytes += Math.trunc(body);
          agg.ttfbSum += ttfbMs;
          agg.originWaitSum += originMs;
          if (originMs > 0) agg.originRequests += 1;
        }
      },
      finish(): Rollup {
        return {
          dims: new Map(),
          series: new Map(),
          seriesStepMs: SERIES_STEP_MS,
          fields,
          truncated: new Set(),
          groups: new Map(),
          present: new Set(["assets/asset"]),
          overview,
          assets,
        };
      },
    };
  },
};
