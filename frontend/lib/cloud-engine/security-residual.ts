import { parquetMetadata } from "hyparquet";
import { fetchLanes, rawLaneForBytes } from "./fetch-lanes";
import { Hll } from "./dashboard/hll";
import type { ManifestFile } from "./dashboard/manifest";
import { readColumns } from "./dashboard/parquet";
import { projectedReader } from "./dashboard/projected-read";
import type { SummaryProjectionSource, SummarySpec } from "./dashboard/summary-projection";
import {
  SECURITY_REQ_HEADER_BUCKETS,
  SECURITY_REQ_HEADER_LAST,
} from "./security";
import type { SecuritySummary } from "./security-rollup";

const HOUR_MS = 3_600_000;
const FIVE_MINUTES_MS = 300_000;
const MINUTE_MS = 60_000;
const PROXY_ROW_LIMIT = 5000;
const RESIDUAL_READ_CONCURRENCY = 4;

/** Maximum total listed bytes of unsummarized raw files rolled up on the client for /security. */
export const SECURITY_RESIDUAL_MAX_BYTES = 64 * 1024 * 1024;

const CUSTOM_FALLBACK_COLUMNS = [
  "client_ip",
  "proxy_type",
  "waf_logged_rules",
  "waf_blocked_rules",
  "is_bot",
] as const;

const TRACKED_FIELDS = ["tls_ciphers_sha", "conn_requests", "p_type", "is_ipv6", "ua"] as const;

const NOT_HUMAN = [
  "bot", "crawler", "spider", "slurp", "archiver", "scraper", "externalagent", "extended", "headless",
];

function agentStem(uaL: string): string {
  const match = /([a-z0-9]+)-(?:user|externalfetcher)/.exec(uaL);
  return match ? match[1] : "";
}

function refererStem(refL: string): string {
  const match = /https?:\/\/(?:www\.)?([^/.:;\s]+)/.exec(refL);
  return match ? match[1] : "";
}

/** Matches summary.rs `ref_stem` / `buildSecurityBotEfficiencySql` `ref_extracted`. */
function refStem(ip: string, ua: string, referer: string): string | null {
  if (!ip) return null;
  const uaL = ua.toLowerCase();
  const fetcher = uaL.includes("-user") || uaL.includes("-externalfetcher");
  if ((!fetcher && (referer === "" || referer === "-")) || NOT_HUMAN.some((w) => uaL.includes(w))) {
    return null;
  }
  const stem = (fetcher ? agentStem(uaL) : "") || refererStem(referer.toLowerCase());
  return stem || null;
}

function verifiedBot(sig: string): string | null {
  if (!sig.toUpperCase().includes("VERIFIED-BOT.")) return null;
  return sig.includes("VERIFIED-BOT.") ? sig.replaceAll("VERIFIED-BOT.", "") : sig;
}

function toNum(v: unknown): number | null {
  if (typeof v === "bigint") return Number(v);
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (v instanceof Date) return v.getTime();
  if (typeof v === "string" && v !== "") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function toStr(v: unknown): string | null {
  if (v == null) return null;
  return String(v);
}

function bucketOf(value: number, bounds: ReadonlyArray<readonly [number, string]>, last: string): string {
  return bounds.find(([upTo]) => value <= upTo)?.[1] ?? last;
}

/** Raw columns required to build the residual `SecuritySummary` rows for `spec`. */
export function securityResidualColumns(spec?: SummarySpec | null): string[] {
  const cols = new Set<string>(["timestamp", "ts_ms", ...CUSTOM_FALLBACK_COLUMNS]);
  const has = (group: string) => !spec || spec.groups.has(group);
  if (has("overview")) cols.add("resp_bytes");
  if (has("all_fields") || has("field_totals")) {
    for (const f of TRACKED_FIELDS) cols.add(f);
  }
  if (has("all_fields_ip")) {
    cols.add("tls_ciphers_sha");
    cols.add("ip");
    cols.add("ua");
    cols.add("referer");
  }
  if (has("bins")) {
    cols.add("req_header_bytes");
    cols.add("waf_sig");
  }
  if (has("header_ip")) {
    cols.add("ip");
    cols.add("req_header_bytes");
  }
  if (has("bot_ua")) {
    cols.add("ua");
    cols.add("resp_bytes");
    cols.add("ip");
  }
  if (has("tls_fp")) {
    cols.add("ja3");
    cols.add("ja4");
    cols.add("p_type");
    cols.add("country");
  }
  if (has("proxy")) {
    for (const c of ["ip", "asn", "rtt_min", "tcpi_min_rtt", "tcp_rtt", "tcpi_rtt", "lat", "lon", "pop"]) {
      cols.add(c);
    }
  }
  return [...cols];
}

type HourAcc = {
  summary: SecuritySummary;
  minuteSet: Set<number>;
  proxyTotal: number;
};

function createHourAcc(): HourAcc {
  return {
    summary: {
      requests: 0,
      minutes: [],
      bins: new Map(),
      binsCut: new Set(),
      fields: new Map(),
      totals: new Map([["_ref_stem", { count: 0, truncated: false }]]),
      sketches: new Map(),
      refSketches: new Map(),
      groups: new Set(["header_ip/ip", "bot_ua/ua", "proxy/sample", "tls_fp/fp", "tls_fp/fp_country"]),
      groupsCut: new Set(),
      bandwidth: 0,
      headerIp: new Map(),
      botUa: new Map(),
      fp: new Map(),
      fpCountry: new Map(),
      proxy: new Map(),
    },
    minuteSet: new Set(),
    proxyTotal: 0,
  };
}

function incField(summary: SecuritySummary, field: string, value: string): void {
  let values = summary.fields.get(field);
  if (!values) {
    values = new Map();
    summary.fields.set(field, values);
  }
  values.set(value, (values.get(value) ?? 0) + 1);
  const total = summary.totals.get(field) ?? { count: 0, truncated: false };
  total.count += 1;
  summary.totals.set(field, total);
}

function incBin(summary: SecuritySummary, field: string, value: string, bucket: number): void {
  let values = summary.bins.get(field);
  if (!values) {
    values = new Map();
    summary.bins.set(field, values);
  }
  let byBucket = values.get(value);
  if (!byBucket) {
    byBucket = new Map();
    values.set(value, byBucket);
  }
  byBucket.set(bucket, (byBucket.get(bucket) ?? 0) + 1);
}

/**
 * Reads unsummarized residual raw Parquet files using column/row-group projection
 * and accumulates them into per-UTC-hour `SecuritySummary` entries so hourly
 * sections (including `ipv6_adoption`) remain exact across hour boundaries.
 */
export async function readSecurityResidual(
  source: SummaryProjectionSource,
  files: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
  spec: SummarySpec | null | undefined,
  signal: AbortSignal,
  assertActive: () => void,
): Promise<{ key: string; summary: SecuritySummary }[]> {
  if (files.length === 0) return [];
  const wanted = securityResidualColumns(spec);
  const project = source.fetchRange
    ? projectedReader(source, wanted, signal, { fromMs, toMs })
    : null;
  const byHour = new Map<number, HourAcc>();
  const getHour = (hourMs: number) => {
    let acc = byHour.get(hourMs);
    if (!acc) {
      acc = createHourAcc();
      byHour.set(hourMs, acc);
    }
    return acc;
  };

  let added = Promise.resolve();
  let next = 0;
  let fileSeq = 0;

  const accumulate = async (bytes: ArrayBuffer, seq: number) => {
    const schemaNames = new Set(parquetMetadata(bytes).schema.map((s) => s.name));
    const presentCustom = CUSTOM_FALLBACK_COLUMNS.filter((c) => schemaNames.has(c));
    const colsToRead = wanted.filter((c) => schemaNames.has(c));
    const { rows, columns } = await readColumns(bytes, colsToRead);
    const tsCol = columns.get("timestamp");
    const tsMsCol = columns.get("ts_ms");
    const ipCol = columns.get("ip");
    const tlsCol = columns.get("tls_ciphers_sha");
    const connCol = columns.get("conn_requests");
    const pTypeCol = columns.get("p_type");
    const ipv6Col = columns.get("is_ipv6");
    const uaCol = columns.get("ua");
    const refCol = columns.get("referer");
    const reqHdrCol = columns.get("req_header_bytes");
    const wafSigCol = columns.get("waf_sig");
    const respBytesCol = columns.get("resp_bytes");
    const ja3Col = columns.get("ja3");
    const ja4Col = columns.get("ja4");
    const countryCol = columns.get("country");
    const asnCol = columns.get("asn");
    const rttMinCol = columns.get("rtt_min");
    const tcpiMinRttCol = columns.get("tcpi_min_rtt");
    const tcpRttCol = columns.get("tcp_rtt");
    const tcpiRttCol = columns.get("tcpi_rtt");
    const latCol = columns.get("lat");
    const lonCol = columns.get("lon");
    const popCol = columns.get("pop");

    const touchedHours = new Set<HourAcc>();
    for (let i = 0; i < rows; i++) {
      const ts = toNum(tsCol?.[i]) ?? toNum(tsMsCol?.[i]);
      if (ts === null || ts < fromMs || ts >= toMs) continue;
      const hourMs = Math.floor(ts / HOUR_MS) * HOUR_MS;
      const acc = getHour(hourMs);
      touchedHours.add(acc);
      const s = acc.summary;

      s.requests += 1;
      acc.minuteSet.add(Math.floor(ts / MINUTE_MS) * MINUTE_MS);
      const respBytes = toNum(respBytesCol?.[i]) ?? 0;
      s.bandwidth += respBytes;

      const ip = toStr(ipCol?.[i]) ?? "";
      const tls = toStr(tlsCol?.[i]);
      if (tls !== null) {
        incField(s, "tls_ciphers_sha", tls);
        if (tls !== "" && ip !== "") {
          let hll = s.sketches.get(tls);
          if (!hll) {
            hll = new Hll();
            s.sketches.set(tls, hll);
          }
          hll.add(ip);
        }
      }
      const conn = toStr(connCol?.[i]);
      if (conn !== null) incField(s, "conn_requests", conn);
      const pType = toStr(pTypeCol?.[i]);
      if (pType !== null) incField(s, "p_type", pType);
      const isIpv6 = toStr(ipv6Col?.[i]);
      if (isIpv6 !== null) incField(s, "is_ipv6", isIpv6);
      const ua = toStr(uaCol?.[i]);
      if (ua !== null) {
        incField(s, "ua", ua);
        if (ua !== "") {
          const entry = s.botUa.get(ua) ?? { bytes: 0, requests: 0, withIp: 0 };
          entry.bytes += respBytes;
          entry.requests += 1;
          if (ip !== "") entry.withIp += 1;
          s.botUa.set(ua, entry);
        }
      }

      const stem = refStem(ip, ua ?? "", toStr(refCol?.[i]) ?? "");
      if (stem !== null) {
        incField(s, "_ref_stem", stem);
        let hll = s.refSketches.get(stem);
        if (!hll) {
          hll = new Hll();
          s.refSketches.set(stem, hll);
        }
        hll.add(ip);
      }

      const bucket5m = Math.floor(ts / FIVE_MINUTES_MS) * FIVE_MINUTES_MS;
      const reqHdr = toNum(reqHdrCol?.[i]);
      if (reqHdr !== null && reqHdr > 0) {
        incBin(s, "req_header_size", bucketOf(reqHdr, SECURITY_REQ_HEADER_BUCKETS, SECURITY_REQ_HEADER_LAST), bucket5m);
        if (ip !== "") {
          s.headerIp.set(ip, Math.max(s.headerIp.get(ip) ?? -Infinity, reqHdr));
        }
      }

      const wafSig = toStr(wafSigCol?.[i]);
      if (wafSig) {
        const bot = verifiedBot(wafSig);
        if (bot !== null) incBin(s, "verified_bot", bot, bucket5m);
      }

      const ja3 = toStr(ja3Col?.[i]);
      const ja4 = toStr(ja4Col?.[i]);
      const fp = (ja3 && ja3 !== "" ? ja3 : null) ?? (ja4 && ja4 !== "" ? ja4 : null);
      if (fp) {
        const entry = s.fp.get(fp) ?? { requests: 0, proxied: 0 };
        entry.requests += 1;
        if (pType && pType !== "") entry.proxied += 1;
        s.fp.set(fp, entry);
        const country = toStr(countryCol?.[i]);
        if (country && country !== "") {
          const key = JSON.stringify([fp, country]);
          s.fpCountry.set(key, (s.fpCountry.get(key) ?? 0) + 1);
        }
      }

      const rttMin = toNum(rttMinCol?.[i]) ?? toNum(tcpiMinRttCol?.[i]);
      if (rttMin !== null) {
        acc.proxyTotal += 1;
        if (s.proxy.size < PROXY_ROW_LIMIT) {
          const sampleKey = `${ts}:${seq}:${i}`;
          s.proxy.set(sampleKey, {
            ip: ipCol?.[i] ?? null,
            asn: asnCol?.[i] ?? null,
            rtt_min: rttMin,
            tcp_rtt: toNum(tcpRttCol?.[i]) ?? toNum(tcpiRttCol?.[i]),
            lat: toNum(latCol?.[i]),
            lon: toNum(lonCol?.[i]),
            pop: popCol?.[i] ?? null,
          });
        } else {
          s.groupsCut.add("proxy/sample");
        }
      }
    }

    // Record custom fallback columns present in the raw schema so callers fall back to DuckDB.
    for (const custom of presentCustom) {
      const col = columns.get(custom);
      let nonNull = 0;
      if (col) {
        for (let i = 0; i < rows; i++) {
          if (col[i] != null) nonNull += 1;
        }
      }
      for (const acc of touchedHours) {
        const prev = acc.summary.totals.get(custom) ?? { count: 0, truncated: false };
        prev.count += nonNull;
        acc.summary.totals.set(custom, prev);
      }
    }
  };

  const worker = async () => {
    while (next < files.length) {
      const file = files[next++];
      const seq = fileSeq++;
      assertActive();
      const bytes = project && file.size > 0
        ? (await (await project(file)).read()).bytes
        : (
            await fetchLanes.run(
              rawLaneForBytes(file.size),
              () => {
                assertActive();
                const lane = rawLaneForBytes(file.size);
                return source.fetchFile(
                  file.key,
                  signal,
                  lane === "large" ? { priority: "low" } : undefined,
                );
              },
              signal,
            )
          ).bytes;
      assertActive();
      const add = added.then(() => {
        assertActive();
        return accumulate(bytes, seq);
      });
      added = add;
      await add;
      assertActive();
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(RESIDUAL_READ_CONCURRENCY, files.length) }, worker),
  );
  assertActive();

  return [...byHour.entries()]
    .sort(([a], [b]) => a - b)
    .map(([hourMs, acc]) => {
      acc.summary.minutes = [...acc.minuteSet].sort((a, b) => a - b);
      return { key: `residual:${hourMs}`, summary: acc.summary };
    });
}
