import { parquetMetadata, type SchemaElement } from "hyparquet";
import { Histogram } from "./dashboard/histogram";
import { readColumns } from "./dashboard/parquet";
import {
  emptyAgg,
  type Rollup,
  type RollupAgg,
  type RollupResidual,
  type RollupSample,
  type RollupWaterfall,
} from "./dashboard/rollup-query";
import { SCATTER_SAMPLE_SIZE } from "./performance";

const SERIES_STEP_MS = 300_000;

const PRESENT = ["urls/url", "urls/asn", "urls/ttl_bucket"] as const;
const KEYS = new Set<string>([...PRESENT, "overview/waterfall", "sample"]);

const COLUMNS = [
  "timestamp",
  "url",
  "asn",
  "elapsed",
  "ttfb",
  "ottfb",
  "ottlb",
  "ttl",
  "cache",
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

function stringKey(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "string") {
    const trimmed = v.trim();
    return trimmed ? trimmed : null;
  }
  if (typeof v === "bigint" || typeof v === "number") return String(v);
  return null;
}

function ttlBucketFor(ttl: number): string {
  if (ttl < 60) return "< 60s";
  if (ttl < 3600) return "1m - 1h";
  if (ttl < 86400) return "1h - 1d";
  return "> 1d";
}

function aggOf(values: Map<string, RollupAgg>, key: string): RollupAgg {
  let agg = values.get(key);
  if (!agg) {
    agg = emptyAgg();
    values.set(key, agg);
  }
  return agg;
}

function addElapsed(agg: RollupAgg, elapsed: number | null) {
  if (elapsed === null) return;
  (agg.hists.elapsed ??= new Histogram()).add(elapsed);
  agg.elapsedCount += 1;
  agg.elapsedSum += elapsed;
}

const FNV_OFFSET = BigInt("0xcbf29ce484222325");
const FNV_PRIME = BigInt("0x100000001b3");
const MASK_64 = BigInt("0xffffffffffffffff");

function sampleKey(tsMs: number, url: string | null, row: number): bigint {
  const input = `${tsMs}|${url ?? ""}|${row}`;
  let h = FNV_OFFSET;
  for (let i = 0; i < input.length; i++) {
    h ^= BigInt(input.charCodeAt(i));
    h = (h * FNV_PRIME) & MASK_64;
  }
  return h;
}

function trimSamples(samples: RollupSample[]): RollupSample[] {
  if (samples.length <= SCATTER_SAMPLE_SIZE) return samples;
  samples.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.tsMs - b.tsMs));
  samples.length = SCATTER_SAMPLE_SIZE;
  return samples;
}

/**
 * Rolls up unsummarized raw Parquet tail/edge files into the `/performance`
 * summary groups (`urls/url`, `urls/asn`, `urls/ttl_bucket`, `waterfall`,
 * and `samples`) so fresh minutes are included alongside v3 summaries.
 */
export const PERFORMANCE_RESIDUAL: RollupResidual = {
  id: "performance-residual-v1",
  columns: COLUMNS,
  maxBytes: 64 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const urls = new Map<string, RollupAgg>();
    const asns = new Map<string, RollupAgg>();
    const ttlBuckets = new Map<string, RollupAgg>();
    const waterfall: RollupWaterfall = {
      count: 0,
      edgeProcessing: 0,
      originWait: 0,
      originDownload: 0,
      clientDownload: 0,
    };
    let samples: RollupSample[] = [];
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
        const asnCol = columns.get("asn") ?? empty;
        const elapsedCol = columns.get("elapsed") ?? empty;
        const ttfbCol = columns.get("ttfb") ?? empty;
        const ottfbCol = columns.get("ottfb") ?? empty;
        const ottlbCol = columns.get("ottlb") ?? empty;
        const ttlCol = columns.get("ttl") ?? empty;
        const cacheCol = columns.get("cache") ?? empty;

        for (let i = 0; i < rows; i++) {
          const t = toFiniteNumber(ts[i]);
          if (t === null || t < fromMs || t >= toMs) continue;
          const elapsed = toFiniteNumber(elapsedCol[i]);
          const ttfb = toFiniteNumber(ttfbCol[i]);

          const urlKey = stringKey(urlCol[i]);
          if (urlKey !== null) {
            const agg = aggOf(urls, urlKey);
            agg.requests += 1;
            addElapsed(agg, elapsed);
          }

          const asnKey = stringKey(asnCol[i]);
          if (asnKey !== null) {
            const agg = aggOf(asns, asnKey);
            agg.requests += 1;
            addElapsed(agg, elapsed);
          }

          const ttl = toFiniteNumber(ttlCol[i]);
          if (ttl !== null) {
            aggOf(ttlBuckets, ttlBucketFor(ttl)).requests += 1;
          }

          if (elapsed !== null && ttfb !== null) {
            const elapsedMs = elapsed / 1000;
            const ttfbMs = ttfb * 1000;
            const ottfbRaw = toFiniteNumber(ottfbCol[i]);
            const ottlbRaw = toFiniteNumber(ottlbCol[i]);
            const ottfbMs = ottfbRaw !== null && ottfbRaw > 0 ? ottfbRaw / 1000 : 0;
            const ottlbMs =
              ottfbRaw !== null && ottfbRaw > 0 && ottlbRaw !== null && ottlbRaw > 0
                ? ottlbRaw / 1000
                : 0;
            waterfall.count += 1;
            waterfall.edgeProcessing += Math.max(0, ttfbMs - ottfbMs);
            waterfall.originWait += ottfbMs;
            waterfall.originDownload += Math.max(0, ottlbMs - ottfbMs);
            waterfall.clientDownload += Math.max(0, elapsedMs - Math.max(ottlbMs, ttfbMs));
          }

          if (ttfb !== null) {
            const cacheVal = typeof cacheCol[i] === "string" ? (cacheCol[i] as string) : null;
            samples.push({
              key: sampleKey(t, urlKey, i),
              tsMs: t,
              cache: cacheVal,
              ttfbS: ttfb,
              elapsedUs: elapsed,
              rttUs: null,
            });
            if (samples.length > SCATTER_SAMPLE_SIZE * 2) {
              samples = trimSamples(samples);
            }
          }
        }
      },
      finish(): Rollup {
        return {
          dims: new Map(),
          series: new Map(),
          seriesStepMs: SERIES_STEP_MS,
          fields: new Map(),
          truncated: new Set(),
          waterfall: waterfall.count > 0 ? waterfall : undefined,
          groups: new Map([
            [
              "urls",
              new Map([
                ["url", urls],
                ["asn", asns],
                ["ttl_bucket", ttlBuckets],
              ]),
            ],
          ]),
          samples: trimSamples(samples),
          present: new Set(PRESENT),
        };
      },
    };
  },
};
