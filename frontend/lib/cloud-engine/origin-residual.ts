import { parquetMetadata, type SchemaElement } from "hyparquet";
import { Histogram } from "./dashboard/histogram";
import { readColumns } from "./dashboard/parquet";
import {
  emptyAgg,
  type Rollup,
  type RollupAgg,
  type RollupResidual,
} from "./dashboard/rollup-query";

const FIVE_MINUTES_MS = 300_000;
const MINUTE_MS = 60_000;

const PRESENT = [
  "origin/overall",
  "origin/edge",
  "origin/pop",
  "urls/url",
  "shield_routes/route",
  "series5/overall",
  "series5/origin",
  "series1/origin",
] as const;

const KEYS = new Set<string>([
  ...PRESENT,
  "dims/oip",
  "all_fields/ost",
]);

const COLUMNS = [
  "timestamp",
  "oip",
  "ost",
  "ottfb",
  "ottlb",
  "ttfb",
  "elapsed",
  "obytes",
  "cache",
  "edge",
  "pop",
  "url",
  "rid",
  "prid",
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

function nonEmptyStr(v: unknown): string | null {
  if (typeof v !== "string" || v.length === 0) return null;
  return v;
}

function strOrNull(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}

function legKey(edge: boolean | null): string {
  if (edge === true) return "true";
  if (edge === false) return "false";
  return "";
}

function getOrCreateAgg(map: Map<string, RollupAgg>, key: string): RollupAgg {
  let agg = map.get(key);
  if (!agg) {
    agg = emptyAgg();
    map.set(key, agg);
  }
  return agg;
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

function addOriginRow(
  agg: RollupAgg,
  latUs: number,
  full: boolean,
  miss: boolean,
  pass: boolean,
  ost: number | null,
  ottlb: number | null,
  overhead: number | null,
  obytes: number | null,
) {
  agg.requests += 1;
  (agg.hists.ttfb ??= new Histogram()).add(latUs);
  if (!full) return;
  if (miss) agg.missRequests += 1;
  if (pass) agg.passRequests += 1;
  if (ost !== null) {
    agg.ostTotal += 1;
    if (ost >= 500 && ost < 600) agg.ost5xx += 1;
  }
  if (ottlb !== null) (agg.hists.ottlb ??= new Histogram()).add(ottlb);
  if (overhead !== null) (agg.hists.overhead ??= new Histogram()).add(overhead);
  if (obytes !== null) (agg.hists.obytes ??= new Histogram()).add(obytes);
}

/**
 * Rolls up unsummarized raw Parquet tail/edge files into the `/origin` summary
 * groups (`dims/oip`, `origin/*`, `urls/url`, `all_fields/ost`, `shield_routes`,
 * `series5/*`, `series1/origin`) so fresh minutes stay on the summary path.
 */
export const ORIGIN_RESIDUAL: RollupResidual = {
  id: "origin-residual-v1",
  columns: COLUMNS,
  maxBytes: 64 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const dimsOip = new Map<string, RollupAgg>();
    const originOverall = new Map<string, RollupAgg>();
    const originEdge = new Map<string, RollupAgg>();
    const originPop = new Map<string, RollupAgg>();
    const urls = new Map<string, RollupAgg>();
    const shieldRoutes = new Map<string, RollupAgg>();
    const series5 = new Map<string, Map<string, Map<number, RollupAgg>>>();
    const series1 = new Map<string, Map<string, Map<number, RollupAgg>>>();
    const ostCounts = new Map<string, number>();

    return {
      async add(file) {
        const kinds = new Map<string, Kind>(
          parquetMetadata(file).schema.slice(1).map((element) => [element.name, kindOf(element)]),
        );
        if (kinds.get("timestamp") !== "ts") return;
        const { rows, columns } = await readColumns(file, COLUMNS.filter((name) => kinds.get(name)));
        const empty: ArrayLike<unknown> = { length: rows };
        const ts = columns.get("timestamp") ?? empty;
        const oipCol = columns.get("oip") ?? empty;
        const ostCol = columns.get("ost") ?? empty;
        const ottfbCol = columns.get("ottfb") ?? empty;
        const ottlbCol = columns.get("ottlb") ?? empty;
        const ttfbCol = columns.get("ttfb") ?? empty;
        const elapsedCol = columns.get("elapsed") ?? empty;
        const obytesCol = columns.get("obytes") ?? empty;
        const cacheCol = columns.get("cache") ?? empty;
        const edgeCol = columns.get("edge") ?? empty;
        const popCol = columns.get("pop") ?? empty;
        const urlCol = columns.get("url") ?? empty;
        const ridCol = columns.get("rid") ?? empty;
        const pridCol = columns.get("prid") ?? empty;

        // Window-local shield map for shield_routes join (prid -> [(pop, us)]).
        const shields = new Map<string, Array<{ pop: string; us: number | null }>>();
        const edgeRows: Array<{ rid: string; pop: string; us: number | null }> = [];

        for (let i = 0; i < rows; i++) {
          const tsMs = toFiniteNumber(ts[i]);
          if (tsMs === null || tsMs < fromMs || tsMs >= toMs) continue;

          const ottfb = toFiniteNumber(ottfbCol[i]);
          const ottlb = toFiniteNumber(ottlbCol[i]);
          const ttfb = toFiniteNumber(ttfbCol[i]);
          const ttfbUs = ttfb !== null ? Math.round(ttfb * 1e6) : null;
          const elapsed = toFiniteNumber(elapsedCol[i]);
          const obytesRaw = toFiniteNumber(obytesCol[i]);
          const obytes = obytesRaw !== null ? Math.round(obytesRaw) : null;
          const ost = toFiniteNumber(ostCol[i]);
          const cache = strOrNull(cacheCol[i]);
          const edge = toBool(edgeCol[i]);
          const leg = legKey(edge);
          const pop = strOrNull(popCol[i]);
          const oip = nonEmptyStr(oipCol[i]);
          const url = strOrNull(urlCol[i]);

          const latUs = ottfb !== null ? Math.round(ottfb) : ttfbUs;
          const isHitOrSynth = cache !== null && (cache.startsWith("HIT") || cache.startsWith("SYNTH"));
          const originLat = ottfb === null && isHitOrSynth ? null : latUs;

          if (ost !== null) {
            const k = String(Math.trunc(ost));
            ostCounts.set(k, (ostCounts.get(k) ?? 0) + 1);
          }

          if (oip !== null) {
            const d = getOrCreateAgg(dimsOip, oip);
            d.requests += 1;
            if (ost !== null) {
              d.ostTotal += 1;
              if (ost >= 500 && ost < 600) d.ost5xx += 1;
            }
            if (latUs !== null) d.latUsCount += 1;
            if (ottfb !== null) (d.hists.ottfb ??= new Histogram()).add(Math.round(ottfb));
            if (ttfbUs !== null) (d.hists.ttfb ??= new Histogram()).add(ttfbUs);
          }

          if (originLat !== null) {
            const miss = cache !== null && cache.startsWith("MISS");
            const pass = cache !== null && cache.startsWith("PASS");
            const overhead = elapsed !== null && ottlb !== null ? Math.round(elapsed - ottlb) : null;
            const ottlbInt = ottlb !== null ? Math.round(ottlb) : null;
            addOriginRow(
              getOrCreateAgg(originOverall, ""),
              originLat,
              true,
              miss,
              pass,
              ost,
              ottlbInt,
              overhead,
              obytes,
            );
            addOriginRow(
              getOrCreateAgg(originEdge, leg),
              originLat,
              false,
              miss,
              pass,
              ost,
              ottlbInt,
              overhead,
              obytes,
            );
            if (pop !== null && pop.length > 0) {
              addOriginRow(
                getOrCreateAgg(originPop, pop),
                originLat,
                false,
                miss,
                pass,
                ost,
                ottlbInt,
                overhead,
                obytes,
              );
            }
          }

          if (url !== null) {
            const trimmed = url.trim();
            if (trimmed.length > 0) {
              const u = getOrCreateAgg(urls, trimmed);
              u.requests += 1;
              if (originLat !== null) (u.hists.ttfb ??= new Histogram()).add(originLat);
            }
          }

          const step5 = Math.floor(tsMs / FIVE_MINUTES_MS) * FIVE_MINUTES_MS;
          const overall5 = getOrCreateSeriesBucket(series5, "overall", "", step5);
          overall5.requests += 1;
          const origin5 = getOrCreateSeriesBucket(series5, "origin", leg, step5);
          origin5.requests += 1;
          if (originLat !== null) (origin5.hists.ttfb ??= new Histogram()).add(originLat);
          if (ottlb !== null) (origin5.hists.ottlb ??= new Histogram()).add(Math.round(ottlb));

          const step1 = Math.floor(tsMs / MINUTE_MS) * MINUTE_MS;
          const origin1 = getOrCreateSeriesBucket(series1, "origin", leg, step1);
          origin1.requests += 1;
          if (originLat !== null) (origin1.hists.ttfb ??= new Histogram()).add(originLat);
          if (ottlb !== null) (origin1.hists.ottlb ??= new Histogram()).add(Math.round(ottlb));

          if (leg === "false") {
            const prid = nonEmptyStr(pridCol[i]);
            if (prid !== null) {
              let list = shields.get(prid);
              if (!list) {
                list = [];
                shields.set(prid, list);
              }
              list.push({ pop: pop ?? "", us: latUs });
            }
          } else if (leg === "true") {
            const rid = nonEmptyStr(ridCol[i]);
            if (rid !== null) {
              edgeRows.push({ rid, pop: pop ?? "", us: latUs });
            }
          }
        }

        for (const e of edgeRows) {
          const matched = shields.get(e.rid);
          if (!matched) continue;
          for (const s of matched) {
            const key = JSON.stringify([e.pop, s.pop]);
            const agg = getOrCreateAgg(shieldRoutes, key);
            agg.requests += 1;
            if (e.us !== null && s.us !== null) {
              const delta = e.us - s.us;
              (agg.hists.overhead ??= new Histogram()).add(delta);
              if (delta < 0) agg.errors += 1;
            }
          }
        }
      },
      finish(): Rollup {
        const dims = new Map<string, Map<string, RollupAgg>>();
        if (dimsOip.size > 0) dims.set("oip", dimsOip);
        const fields = new Map<string, Map<string, number>>();
        if (ostCounts.size > 0) fields.set("ost", ostCounts);
        return {
          dims,
          series: series5,
          seriesStepMs: FIVE_MINUTES_MS,
          series1,
          fields,
          truncated: new Set(),
          groups: new Map([
            [
              "origin",
              new Map([
                ["overall", originOverall],
                ["edge", originEdge],
                ["pop", originPop],
              ]),
            ],
            ["urls", new Map([["url", urls]])],
            ["shield_routes", new Map([["route", shieldRoutes]])],
          ]),
          present: new Set(PRESENT),
        };
      },
    };
  },
};
