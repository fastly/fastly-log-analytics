import { parquetMetadata, type SchemaElement } from "hyparquet";
import { Histogram } from "./dashboard/histogram";
import { readColumns } from "./dashboard/parquet";
import {
  emptyAgg,
  type Rollup,
  type RollupAgg,
  type RollupResidual,
  type RollupSample,
} from "./dashboard/rollup-query";

const FIVE_MINUTES_MS = 300_000;
const SAMPLE_K = 300;

const PRESENT = [
  "dims/country",
  "dims/asn",
  "dims/city",
  "dims/pop",
  "dims/proto",
  "series5/overall",
  "series5/asn",
  "series5/city",
  "quality/overall",
  "quality/asn",
  "quality/region",
  "transport/overall",
  "transport/proto",
  "shield_routes/route",
] as const;

const KEYS = new Set<string>([...PRESENT, "sample"]);

const COLUMNS = [
  "timestamp",
  "status",
  "cache",
  "resp_bytes",
  "resp_body_size",
  "country",
  "geo_country",
  "city",
  "geo_city",
  "region",
  "geo_region",
  "asn",
  "as_name",
  "client_as_name",
  "pop",
  "proto",
  "protocol",
  "rtt",
  "tcp_rtt",
  "client_rtt_us",
  "client_tcpi_rtt",
  "ploss",
  "client_ploss",
  "cwnd",
  "client_cwnd",
  "rttvar",
  "client_tcpi_rttvar",
  "retrans",
  "client_tcpi_total_retrans",
  "delivery_rate",
  "client_tcpi_delivery_rate",
  "lat",
  "lon",
  "metro",
  "ttfb",
  "ottfb",
  "elapsed",
  "time_elapsed_usec",
  "edge",
  "rid",
  "prid",
  "url",
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

function isTsHit(cache: string | null): boolean {
  return cache === "HIT" || cache === "HIT-STALE";
}

function addNetDim(
  agg: RollupAgg,
  s5: boolean,
  hit: boolean,
  bytes: number,
  rtt: number | null,
  ploss: number | null,
  cwnd: number | null,
  rttvar: number | null,
  retrans: boolean,
  ttfbUs: number | null,
  withTtfb: boolean,
  label: string | null,
) {
  agg.requests += 1;
  if (s5) agg.status5xx += 1;
  if (hit) agg.cacheHits += 1;
  agg.respBytes += bytes;
  if (rtt !== null) {
    const r = Math.round(rtt);
    agg.rttSum += r;
    agg.rttCount += 1;
    (agg.hists.rtt ??= new Histogram()).add(r);
  }
  if (ploss !== null) {
    agg.plossSum += ploss;
    agg.plossCount += 1;
  }
  if (cwnd !== null) {
    agg.cwndSum += cwnd;
    agg.cwndCount += 1;
  }
  if (rttvar !== null) {
    agg.rttvarSum += rttvar;
    agg.rttvarCount += 1;
  }
  if (retrans) agg.retrans += 1;
  if (withTtfb && ttfbUs !== null) {
    (agg.hists.ttfb ??= new Histogram()).add(ttfbUs);
  }
  if (label !== null && (agg.label === null || label < agg.label)) {
    agg.label = label;
  }
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
  return h >> BigInt(1);
}

function trimSamples(samples: RollupSample[]): RollupSample[] {
  if (samples.length <= SAMPLE_K) return samples;
  samples.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.tsMs - b.tsMs));
  samples.length = SAMPLE_K;
  return samples;
}

/**
 * Rolls up unsummarized raw Parquet tail/edge files into the `/network` summary
 * groups (`dims/*`, `series5/*`, `quality/*`, `transport/*`, `shield_routes`,
 * `sample`) so fresh minutes stay on the summary path.
 */
export const NETWORK_RESIDUAL: RollupResidual = {
  id: "network-residual-v1",
  columns: COLUMNS,
  maxBytes: 64 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const dimCountry = new Map<string, RollupAgg>();
    const dimAsn = new Map<string, RollupAgg>();
    const dimCity = new Map<string, RollupAgg>();
    const dimPop = new Map<string, RollupAgg>();
    const dimProto = new Map<string, RollupAgg>();
    const series5 = new Map<string, Map<string, Map<number, RollupAgg>>>();
    const qualityOverall = new Map<string, RollupAgg>();
    const qualityAsn = new Map<string, RollupAgg>();
    const qualityRegion = new Map<string, RollupAgg>();
    const transportOverall = new Map<string, RollupAgg>();
    const transportProto = new Map<string, RollupAgg>();
    const shieldRoutes = new Map<string, RollupAgg>();
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
        const statusCol = columns.get("status") ?? empty;
        const cacheCol = columns.get("cache") ?? empty;
        const respBytesCol = columns.get("resp_bytes") ?? empty;
        const bodySizeCol = columns.get("resp_body_size") ?? empty;
        const countryCol = columns.get("country") ?? empty;
        const geoCountryCol = columns.get("geo_country") ?? empty;
        const cityCol = columns.get("city") ?? empty;
        const geoCityCol = columns.get("geo_city") ?? empty;
        const regionCol = columns.get("region") ?? empty;
        const geoRegionCol = columns.get("geo_region") ?? empty;
        const asnCol = columns.get("asn") ?? empty;
        const asNameCol = columns.get("as_name") ?? empty;
        const clientAsNameCol = columns.get("client_as_name") ?? empty;
        const popCol = columns.get("pop") ?? empty;
        const protoCol = columns.get("proto") ?? empty;
        const protocolCol = columns.get("protocol") ?? empty;
        const rttCol = columns.get("rtt") ?? empty;
        const tcpRttCol = columns.get("tcp_rtt") ?? empty;
        const clientRttCol = columns.get("client_rtt_us") ?? empty;
        const clientTcpiRttCol = columns.get("client_tcpi_rtt") ?? empty;
        const plossCol = columns.get("ploss") ?? empty;
        const clientPlossCol = columns.get("client_ploss") ?? empty;
        const cwndCol = columns.get("cwnd") ?? empty;
        const clientCwndCol = columns.get("client_cwnd") ?? empty;
        const rttvarCol = columns.get("rttvar") ?? empty;
        const clientRttvarCol = columns.get("client_tcpi_rttvar") ?? empty;
        const retransCol = columns.get("retrans") ?? empty;
        const clientRetransCol = columns.get("client_tcpi_total_retrans") ?? empty;
        const deliveryCol = columns.get("delivery_rate") ?? empty;
        const clientDeliveryCol = columns.get("client_tcpi_delivery_rate") ?? empty;
        const latCol = columns.get("lat") ?? empty;
        const lonCol = columns.get("lon") ?? empty;
        const metroCol = columns.get("metro") ?? empty;
        const ttfbCol = columns.get("ttfb") ?? empty;
        const ottfbCol = columns.get("ottfb") ?? empty;
        const elapsedCol = columns.get("elapsed") ?? empty;
        const elapsedUsecCol = columns.get("time_elapsed_usec") ?? empty;
        const edgeCol = columns.get("edge") ?? empty;
        const ridCol = columns.get("rid") ?? empty;
        const pridCol = columns.get("prid") ?? empty;
        const urlCol = columns.get("url") ?? empty;

        const shields = new Map<string, Array<{ pop: string; us: number | null }>>();
        const edgeRows: Array<{ rid: string; pop: string; us: number | null }> = [];

        for (let i = 0; i < rows; i++) {
          const tsMs = toFiniteNumber(ts[i]);
          if (tsMs === null || tsMs < fromMs || tsMs >= toMs) continue;

          const status = toFiniteNumber(statusCol[i]);
          const s5 = status !== null && status >= 500 && status <= 599;
          const cache = nonEmptyStr(cacheCol[i]);
          const hit = isTsHit(cache);
          const respBytes = Math.trunc(toFiniteNumber(respBytesCol[i]) ?? 0);
          const body = toFiniteNumber(bodySizeCol[i]) ?? toFiniteNumber(respBytesCol[i]);
          const bodyInt = body !== null ? Math.trunc(body) : 0;

          const rawCountry = nonEmptyStr(geoCountryCol[i]) ?? nonEmptyStr(countryCol[i]);
          const country = rawCountry ?? "Unknown";
          const rawCity = nonEmptyStr(geoCityCol[i]) ?? nonEmptyStr(cityCol[i]);
          const city = rawCity ?? "Unknown";
          const region = nonEmptyStr(geoRegionCol[i]) ?? nonEmptyStr(regionCol[i]) ?? "Unknown";
          const asnNum = Math.trunc(toFiniteNumber(asnCol[i]) ?? 0);
          const asnStr = String(asnNum);
          const asName = nonEmptyStr(asNameCol[i]) ?? nonEmptyStr(clientAsNameCol[i]);
          const pop = nonEmptyStr(popCol[i]);
          const proto = nonEmptyStr(protoCol[i]) ?? nonEmptyStr(protocolCol[i]) ?? "Other";
          const transportProtoKey = nonEmptyStr(protoCol[i]) ?? nonEmptyStr(protocolCol[i]) ?? "Unknown";

          const rtt =
            toFiniteNumber(rttCol[i]) ??
            toFiniteNumber(clientRttCol[i]) ??
            toFiniteNumber(clientTcpiRttCol[i]) ??
            toFiniteNumber(tcpRttCol[i]);
          const transportRtt =
            toFiniteNumber(tcpRttCol[i]) ??
            toFiniteNumber(clientRttCol[i]) ??
            toFiniteNumber(clientTcpiRttCol[i]) ??
            toFiniteNumber(rttCol[i]);
          const ploss = toFiniteNumber(plossCol[i]) ?? toFiniteNumber(clientPlossCol[i]);
          const clientPloss = toFiniteNumber(clientPlossCol[i]);
          const cwnd = toFiniteNumber(cwndCol[i]) ?? toFiniteNumber(clientCwndCol[i]);
          const rttvar = toFiniteNumber(rttvarCol[i]) ?? toFiniteNumber(clientRttvarCol[i]);
          const clientRttvar = toFiniteNumber(clientRttvarCol[i]);
          const retransVal = toFiniteNumber(retransCol[i]) ?? toFiniteNumber(clientRetransCol[i]);
          const retrans = retransVal !== null && retransVal > 0;
          const deliveryRate = toFiniteNumber(deliveryCol[i]) ?? toFiniteNumber(clientDeliveryCol[i]);
          const lat = toFiniteNumber(latCol[i]);
          const lon = toFiniteNumber(lonCol[i]);
          const metroRaw = toFiniteNumber(metroCol[i]);
          const metroStr =
            metroRaw !== null && Number.isInteger(metroRaw) && metroRaw > 0 ? String(metroRaw) : null;

          const ttfb = toFiniteNumber(ttfbCol[i]);
          const ttfbUs = ttfb !== null ? Math.round(ttfb * 1e6) : null;
          const ottfb = toFiniteNumber(ottfbCol[i]);
          const tpElapsed = toFiniteNumber(elapsedUsecCol[i]) ?? toFiniteNumber(elapsedCol[i]);

          addNetDim(getOrCreateAgg(dimCountry, country), s5, hit, respBytes, rtt, ploss, cwnd, rttvar, retrans, ttfbUs, false, null);
          addNetDim(getOrCreateAgg(dimAsn, asnStr), s5, hit, respBytes, rtt, ploss, cwnd, rttvar, retrans, ttfbUs, false, asName);
          const cityKey = JSON.stringify([country, city]);
          addNetDim(getOrCreateAgg(dimCity, cityKey), s5, hit, respBytes, rtt, ploss, cwnd, rttvar, retrans, ttfbUs, false, metroStr);
          addNetDim(getOrCreateAgg(dimProto, proto), s5, hit, respBytes, rtt, ploss, cwnd, rttvar, retrans, ttfbUs, false, null);
          if (pop !== null) {
            addNetDim(getOrCreateAgg(dimPop, pop), s5, hit, respBytes, rtt, ploss, cwnd, rttvar, retrans, ttfbUs, true, null);
          }

          const step = Math.floor(tsMs / FIVE_MINUTES_MS) * FIVE_MINUTES_MS;
          const sOverall = getOrCreateSeriesBucket(series5, "overall", "", step);
          sOverall.requests += 1;
          if (s5) sOverall.status5xx += 1;
          if (ploss !== null) { sOverall.plossSum += ploss; sOverall.plossCount += 1; }
          if (rtt !== null) (sOverall.hists.rtt ??= new Histogram()).add(Math.round(rtt));

          const sAsn = getOrCreateSeriesBucket(series5, "asn", asnStr, step);
          sAsn.requests += 1;
          if (s5) sAsn.status5xx += 1;
          if (ploss !== null) { sAsn.plossSum += ploss; sAsn.plossCount += 1; }
          if (rtt !== null) (sAsn.hists.rtt ??= new Histogram()).add(Math.round(rtt));

          if (rawCity !== null) {
            const sCity = getOrCreateSeriesBucket(series5, "city", cityKey, step);
            sCity.requests += 1;
            if (s5) sCity.status5xx += 1;
            if (ploss !== null) { sCity.plossSum += ploss; sCity.plossCount += 1; }
            if (lat !== null && lon !== null) {
              sCity.latSum += lat;
              sCity.lonSum += lon;
              sCity.geoCount += 1;
            }
            if (metroStr !== null && (sCity.label === null || metroStr < sCity.label)) {
              sCity.label = metroStr;
            }
            if (rtt !== null) (sCity.hists.rtt ??= new Histogram()).add(Math.round(rtt));
          }

          const qAll = getOrCreateAgg(qualityOverall, "");
          qAll.requests += 1;
          if (tpElapsed !== null && tpElapsed > 0) {
            qAll.throughputSum += (body ?? 0) * 8.0 / tpElapsed;
            qAll.throughputCount += 1;
          }
          if (rawCountry !== null) {
            const qReg = getOrCreateAgg(qualityRegion, JSON.stringify([rawCountry, region]));
            qReg.requests += 1;
            if (rtt !== null) {
              qReg.rttSum += Math.round(rtt);
              qReg.rttCount += 1;
            }
          }
          if (clientPloss !== null || clientRttvar !== null) {
            const qAsn = getOrCreateAgg(qualityAsn, asnStr);
            qAsn.requests += 1;
            if (clientPloss !== null) { qAsn.plossSum += clientPloss; qAsn.plossCount += 1; }
            if (clientRttvar !== null) { qAsn.rttvarSum += clientRttvar; qAsn.rttvarCount += 1; }
          }

          for (const tAgg of [
            getOrCreateAgg(transportOverall, ""),
            getOrCreateAgg(transportProto, transportProtoKey),
          ]) {
            tAgg.requests += 1;
            tAgg.respBytes += bodyInt;
            if (transportRtt !== null) (tAgg.hists.rtt ??= new Histogram()).add(Math.round(transportRtt));
            if (ploss !== null) { tAgg.plossSum += ploss; tAgg.plossCount += 1; }
            if (deliveryRate !== null) { tAgg.deliverySum += deliveryRate; tAgg.deliveryCount += 1; }
            if (retrans) tAgg.retrans += 1;
          }

          if (ttfb !== null) {
            const url = nonEmptyStr(urlCol[i]);
            samples.push({
              key: sampleKey(tsMs, url, i),
              tsMs,
              ttfbS: ttfb,
              elapsedUs: tpElapsed !== null ? Math.round(tpElapsed) : null,
              rttUs: rtt !== null ? Math.round(rtt) : null,
              cache: cache ?? "",
            });
            if (samples.length > SAMPLE_K * 4) samples = trimSamples(samples);
          }

          const edge = toBool(edgeCol[i]);
          const latUs = ottfb !== null ? Math.round(ottfb) : ttfbUs;
          if (edge === false) {
            const prid = nonEmptyStr(pridCol[i]);
            if (prid !== null) {
              let list = shields.get(prid);
              if (!list) {
                list = [];
                shields.set(prid, list);
              }
              list.push({ pop: pop ?? "", us: latUs });
            }
          } else if (edge === true) {
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
        return {
          dims: new Map([
            ["country", dimCountry],
            ["asn", dimAsn],
            ["city", dimCity],
            ["pop", dimPop],
            ["proto", dimProto],
          ]),
          series: series5,
          seriesStepMs: FIVE_MINUTES_MS,
          fields: new Map(),
          truncated: new Set(),
          groups: new Map([
            [
              "quality",
              new Map([
                ["overall", qualityOverall],
                ["asn", qualityAsn],
                ["region", qualityRegion],
              ]),
            ],
            [
              "transport",
              new Map([
                ["overall", transportOverall],
                ["proto", transportProto],
              ]),
            ],
            ["shield_routes", new Map([["route", shieldRoutes]])],
          ]),
          present: new Set(PRESENT),
          samples: trimSamples(samples),
        };
      },
    };
  },
};
