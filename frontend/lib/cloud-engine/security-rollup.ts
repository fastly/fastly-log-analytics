import { parquetMetadata } from "hyparquet";
import type { FileSource } from "./dashboard/engine";
import { Hll } from "./dashboard/hll";
import { fetchManifest, type ManifestFile } from "./dashboard/manifest";
import { planRollup } from "./dashboard/rollup-query";
import { SUMMARY_VERSION } from "./dashboard/summaries";
import {
  defineSummarySpec,
  readSummaryColumns,
  readSummaryProjection,
  type SummaryProjectionSource,
  type SummarySpec,
} from "./dashboard/summary-projection";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import {
  BOT_UA_CLASSIFY_LIMIT,
  classifyUserAgentCounts,
  loadWellKnownBots,
  matchWellKnownBots,
} from "./bots/classify";
import { summarizeBotEfficiencyRows, summarizeWellKnownBotRows } from "./bots/summarize";
import { hasFilterValues, SECURITY_ROLLUP_SECTIONS } from "./rollup-sections";
import {
  expandSecuritySections,
  maskIpValue,
  SECURITY_CONN_REUSE_BUCKETS,
  SECURITY_CONN_REUSE_LAST,
  SECURITY_REQ_HEADER_BUCKETS,
  SECURITY_REQ_HEADER_LAST,
  type SecurityAggregatesResponse,
  type SecurityBucketItem,
  type SecuritySectionName,
  shapeSecurityProxiesFromRawRows,
  shapeSecurityThreatIntelRows,
  type ThreatIntelListResponse,
} from "./security";

export type SecurityRollupBody = {
  start_time?: string | number | null;
  end_time?: string | number | null;
  filters?: unknown;
  bucket_seconds?: unknown;
  sections?: readonly string[] | null;
  mask_ips?: unknown;
};

type Source = Pick<FileSource, "fetchJson"> & SummaryProjectionSource;

const FIVE_MINUTES_MS = 300_000;
const HOUR_MS = 3_600_000;
const TLS = "tls_ciphers_sha";
const REF_STEM = "_ref_stem";
/** The `all_fields` values the sections read; others are skipped while decoding. */
const FIELDS = new Set([TLS, "conn_requests", "p_type", "is_ipv6", "ua", REF_STEM]);
/** The `all_fields_ip` sketches the sections read. */
const SKETCH_FIELDS = new Set([TLS, REF_STEM]);
// Bounds of the raw scans these sections mirror (`executeSecurityAggregates`).
const WELLKNOWN_UA_CANDIDATES = 5000;
const WELLKNOWN_UA_MATCHES = 500;
const BOT_EFFICIENCY_UA_LIMIT = 2000;
const PROXY_ROW_LIMIT = 5000;
const THREAT_INTEL_LIMIT = 50;
const IPV6_TRUE = new Set(["1", "true", "t", "yes"]);

const GROUPS = ["overview", "time_series", "bins", "dim_totals", "field_totals", "all_fields"];
/** The summary groups and columns /security reads without the TLS fingerprints or the row sections. */
export const SECURITY_SUMMARY_SPEC = defineSummarySpec({ groups: GROUPS, columns: ["count", "requests"] });
/** And with the TLS fingerprints, whose IP counts merge the `all_fields_ip` sketches. */
export const SECURITY_TLS_SUMMARY_SPEC = defineSummarySpec({
  groups: [...GROUPS, "all_fields_ip"],
  columns: ["count", "requests", "ip_sketch"],
  only: { ip_sketch: ["all_fields_ip"] },
});

const PROXY_COLUMNS = ["ip", "asn", "rtt_min", "tcp_rtt", "lat_sum", "lon_sum", "pop"];
/** What each section reads beyond `SECURITY_SUMMARY_SPEC`. */
const SECTION_READS: Partial<Record<SecuritySectionName, { groups: string[]; columns: string[] }>> = {
  tls_fingerprints: { groups: ["all_fields_ip"], columns: ["ip_sketch"] },
  top_ips_header: { groups: ["header_ip"], columns: ["value_max"] },
  wellknown_bots: { groups: ["bot_ua"], columns: [] },
  bot_efficiency: { groups: ["bot_ua", "all_fields_ip"], columns: ["resp_bytes_sum", "total_bandwidth_bytes", "ip_sketch"] },
  proxies: { groups: ["proxy"], columns: PROXY_COLUMNS },
};
/**
 * Columns read only in the row groups of the groups that use them. Only the
 * sketches: each extra column set is one more projected part with its own
 * footer, more than the few null chunks of a numeric column cost.
 */
const ONLY: Record<string, string[]> = { ip_sketch: ["all_fields_ip"] };

/** The summary groups and columns an aggregates read of `sections` uses. */
export function securitySummarySpec(sections: readonly string[] | null | undefined): SummarySpec {
  const groups = new Set(GROUPS);
  const columns = new Set(["count", "requests"]);
  for (const section of expandSecuritySections(sections)) {
    const reads = SECTION_READS[section];
    reads?.groups.forEach((group) => groups.add(group));
    reads?.columns.forEach((column) => columns.add(column));
  }
  return defineSummarySpec({
    groups: [...groups],
    columns: [...columns],
    only: Object.fromEntries(Object.entries(ONLY).filter(([column]) => columns.has(column))),
  });
}

/** The summary groups and columns a threat-intel read uses: `field_totals` names the WAF and bot columns it can't answer. */
export const SECURITY_THREAT_INTEL_SUMMARY_SPEC = defineSummarySpec({
  groups: ["tls_fp", "dim_totals", "field_totals"],
  columns: ["count", "requests"],
});

/** The security rows of one v3 window summary or bundle. */
type SecuritySummary = {
  /** `overview` requests: rows with a timestamp. */
  requests: number;
  /** `time_series` minute buckets (ms) holding at least one request. */
  minutes: number[];
  bins: Map<string, Map<string, Map<number, number>>>;
  binsCut: Set<string>;
  fields: Map<string, Map<string, number>>;
  /** `field_totals`: non-null rows per field (every field present, custom ones included). */
  totals: Map<string, { count: number; truncated: boolean }>;
  sketches: Map<string, Hll>;
  /** `_ref_stem` distinct-IP sketches per referral stem. */
  refSketches: Map<string, Hll>;
  /** `${group}/${field}` of every `dim_totals` row, and of those cut to their top K. */
  groups: Set<string>;
  groupsCut: Set<string>;
  /** `overview` response bytes. */
  bandwidth: number;
  /** `header_ip`: the largest request header per IP. */
  headerIp: Map<string, number>;
  /** `bot_ua`: bytes, requests and requests with an IP per user agent. */
  botUa: Map<string, { bytes: number; requests: number; withIp: number }>;
  /** `tls_fp` `fp`: requests and proxied requests per fingerprint. */
  fp: Map<string, { requests: number; proxied: number }>;
  /** `tls_fp` `fp_country`: requests per JSON `[fp, country]`. */
  fpCountry: Map<string, number>;
  /** `proxy` `sample`: rows by sample key, shaped as `buildSecurityProxiesSql` reads them. */
  proxy: Map<string, Record<string, unknown>>;
};

const num = (value: unknown) =>
  typeof value === "bigint" ? Number(value) : typeof value === "number" ? value : 0;

function bucketMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  return null;
}

/**
 * Decodes the rows /security reads from a v3 summary (whole, or the parts of
 * a projection; with `spec`, only its groups and columns), or null for another
 * format version.
 */
export async function decodeSecuritySummary(
  data: ArrayBuffer | readonly ArrayBuffer[],
  spec?: SummarySpec,
): Promise<SecuritySummary | null> {
  const parts: readonly ArrayBuffer[] = Array.isArray(data) ? data : [data as ArrayBuffer];
  const version = parquetMetadata(parts[0]).key_value_metadata?.find((kv) => kv.key === "flp_summary")?.value;
  if (version !== SUMMARY_VERSION) return null;
  const reads = (column: string) => !spec || spec.columns.has(column);
  const { rows, columns } = await readSummaryColumns(parts, [
    "bundle", "bucket", "field", "value", "truncated", "count", "requests", "value_max", "resp_bytes_sum",
    "total_bandwidth_bytes", "ip", "asn", "rtt_min", "tcp_rtt", "lat_sum", "lon_sum", "pop",
  ].filter(reads));
  const sketches = reads("ip_sketch")
    ? (await readSummaryColumns(parts, ["ip_sketch"], false)).columns.get("ip_sketch") ?? []
    : [];
  const col = (name: string) => columns.get(name) ?? [];
  const [group, bucket, field, value, truncated, count, requests] =
    ["bundle", "bucket", "field", "value", "truncated", "count", "requests"].map(col);
  const out: SecuritySummary = {
    requests: 0, minutes: [], bins: new Map(), binsCut: new Set(), fields: new Map(), totals: new Map(), sketches: new Map(),
    refSketches: new Map(), groups: new Set(), groupsCut: new Set(), bandwidth: 0, headerIp: new Map(), botUa: new Map(),
    fp: new Map(), fpCountry: new Map(), proxy: new Map(),
  };
  const opt = (column: ArrayLike<unknown>, i: number) => (column[i] == null ? null : column[i]);
  for (let i = 0; i < rows; i++) {
    if (spec && !spec.groups.has(group[i] as string)) continue;
    const name = String(field[i] ?? "");
    switch (group[i]) {
      case "overview":
        out.requests += num(requests[i]);
        out.bandwidth += num(col("total_bandwidth_bytes")[i]);
        break;
      case "header_ip": {
        const key = String(value[i] ?? "");
        out.headerIp.set(key, Math.max(out.headerIp.get(key) ?? -Infinity, num(col("value_max")[i])));
        break;
      }
      case "bot_ua": {
        const key = String(value[i] ?? "");
        const into = out.botUa.get(key) ?? { bytes: 0, requests: 0, withIp: 0 };
        into.bytes += num(col("resp_bytes_sum")[i]);
        into.requests += num(requests[i]);
        into.withIp += num(count[i]);
        out.botUa.set(key, into);
        break;
      }
      case "tls_fp": {
        const key = String(value[i] ?? "");
        if (name === "fp") {
          const into = out.fp.get(key) ?? { requests: 0, proxied: 0 };
          into.requests += num(requests[i]);
          into.proxied += num(count[i]);
          out.fp.set(key, into);
        } else if (name === "fp_country") {
          out.fpCountry.set(key, (out.fpCountry.get(key) ?? 0) + num(requests[i]));
        }
        break;
      }
      case "proxy":
        if (name === "sample") {
          out.proxy.set(String(value[i] ?? ""), {
            ip: opt(col("ip"), i),
            asn: opt(col("asn"), i),
            rtt_min: opt(col("rtt_min"), i),
            tcp_rtt: opt(col("tcp_rtt"), i),
            lat: opt(col("lat_sum"), i),
            lon: opt(col("lon_sum"), i),
            pop: opt(col("pop"), i),
          });
        }
        break;
      case "time_series": {
        const t = bucketMs(bucket[i]);
        if (t === null) return null;
        if (num(requests[i]) > 0) out.minutes.push(t);
        break;
      }
      case "bins": {
        const t = bucketMs(bucket[i]);
        if (t === null) return null;
        const values = out.bins.get(name) ?? new Map<string, Map<number, number>>();
        out.bins.set(name, values);
        const key = String(value[i] ?? "");
        const byBucket = values.get(key) ?? new Map<number, number>();
        values.set(key, byBucket);
        byBucket.set(t, (byBucket.get(t) ?? 0) + num(requests[i]));
        break;
      }
      case "dim_totals":
        if (value[i] === "bins" && truncated[i] === true) out.binsCut.add(name);
        out.groups.add(`${value[i]}/${name}`);
        if (truncated[i] === true) out.groupsCut.add(`${value[i]}/${name}`);
        break;
      case "field_totals": {
        const total = out.totals.get(name) ?? { count: 0, truncated: false };
        total.count += num(count[i]);
        total.truncated ||= truncated[i] === true;
        out.totals.set(name, total);
        break;
      }
      case "all_fields": {
        if (!FIELDS.has(name)) break;
        const values = out.fields.get(name) ?? new Map<string, number>();
        out.fields.set(name, values);
        const key = String(value[i] ?? "");
        values.set(key, (values.get(key) ?? 0) + num(count[i]));
        break;
      }
      case "all_fields_ip": {
        const blob = sketches[i];
        if (!SKETCH_FIELDS.has(name) || !(blob instanceof Uint8Array)) break;
        const hll = Hll.fromBytes(blob);
        if (!hll) return null;
        const key = String(value[i] ?? "");
        const into = name === TLS ? out.sketches : out.refSketches;
        const held = into.get(key);
        if (held) held.merge(hll);
        else into.set(key, hll);
        break;
      }
    }
  }
  return out;
}

const DECODED_LIMIT = 64;
const decoded = new Map<string, Promise<SecuritySummary | null>>();

/** `file` decoded for `spec`, or whole when null. */
function decodeCached(source: Source, file: ManifestFile, spec: SummarySpec | null): Promise<SecuritySummary | null> {
  const key = `${source.cacheScope ?? "default"}|${file.key}|${spec?.id ?? "whole"}`;
  const hit = decoded.get(key);
  if (hit) {
    decoded.delete(key);
    decoded.set(key, hit);
    return hit;
  }
  const pending = spec
    ? readSummaryProjection(source, file, spec).then(({ parts }) => decodeSecuritySummary(parts, spec))
    : source.fetchFile(file.key).then(({ bytes }) => decodeSecuritySummary(bytes));
  pending.catch(() => decoded.delete(key));
  decoded.set(key, pending);
  while (decoded.size > DECODED_LIMIT) decoded.delete(decoded.keys().next().value!);
  return pending;
}

export type SecurityRollup = {
  files: { key: string; summary: SecuritySummary }[];
  dataFromMs: number;
  dataThroughMs: number;
};

const isDayBundle = (key: string) => key.includes("/day_bundle=");

/**
 * The v3 summaries that answer [fromMs, toMs) (see `planRollup`), decoded for
 * /security, or null when raw Parquet must answer. `hourly` plans without day
 * bundles, whose `all_fields` can't split a day by hour.
 */
export async function querySecurityRollup(
  source: Source,
  range: { fromMs: number; toMs: number; hourly: boolean; signal?: AbortSignal; spec?: SummarySpec | null },
): Promise<SecurityRollup | null> {
  const { fromMs, toMs, hourly, signal, spec = securitySummarySpec(SECURITY_ROLLUP_SECTIONS) } = range;
  const from = new Date(fromMs).toISOString();
  const to = new Date(toMs).toISOString();
  const [raw, summaries] = await Promise.all([
    fetchManifest(source.fetchJson, from, to, signal),
    fetchManifest(source.fetchJson, from, to, signal, undefined, "summary"),
  ]);
  if (!raw.complete || !summaries.complete) return null;
  const usable = hourly ? summaries.files.filter((file) => !isDayBundle(file.key)) : summaries.files;
  const plan = planRollup(raw.files, usable, fromMs, toMs);
  if (!plan) return null;
  const parts = await Promise.all(plan.files.map((use) => decodeCached(source, use.file, spec)));
  signal?.throwIfAborted();
  if (parts.some((part) => part === null)) return null;
  return {
    files: plan.files.map((use, i) => ({ key: use.file.key, summary: parts[i]! })),
    dataFromMs: plan.dataFromMs,
    dataThroughMs: plan.dataThroughMs,
  };
}

const byCountThenValue = ([a, x]: [string, number], [b, y]: [string, number]) =>
  y - x || (a < b ? -1 : a > b ? 1 : 0);

function sumFields(rollup: SecurityRollup, field: string): Map<string, number> {
  const out = new Map<string, number>();
  for (const { summary } of rollup.files) {
    for (const [value, count] of summary.fields.get(field) ?? []) out.set(value, (out.get(value) ?? 0) + count);
  }
  return out;
}

const fieldCut = (rollup: SecurityRollup, field: string) =>
  rollup.files.some(({ summary }) => summary.totals.get(field)?.truncated === true);

/** Buckets in the raw `ORDER BY MIN(value)` order, keeping the non-empty ones. */
function orderedBuckets(
  totals: Map<string, number>,
  bounds: ReadonlyArray<readonly [number, string]>,
  last: string,
): SecurityBucketItem[] {
  return [...bounds.map(([, label]) => label), last]
    .filter((label) => totals.has(label))
    .map((label) => ({ bucket: label, count: totals.get(label)! }));
}

function bucketOf(value: number, bounds: ReadonlyArray<readonly [number, string]>, last: string): string {
  return bounds.find(([upTo]) => value <= upTo)?.[1] ?? last;
}

/** Sums one `bins` field over its time buckets. */
function binTotals(rollup: SecurityRollup, field: string): Map<string, number> {
  const out = new Map<string, number>();
  for (const { summary } of rollup.files) {
    for (const [value, byBucket] of summary.bins.get(field) ?? []) {
      for (const n of byBucket.values()) out.set(value, (out.get(value) ?? 0) + n);
    }
  }
  return out;
}

/**
 * `buildSecurityConnReuseDistSql` over the `all_fields` connection counts:
 * `TRY_CAST(... AS BIGINT) > 0`. Null for a value other than a plain integer
 * or '', whose cast (rounding, whitespace, overflow) is left to DuckDB.
 */
function connReuseDist(rollup: SecurityRollup): SecurityBucketItem[] | null {
  const totals = new Map<string, number>();
  for (const [value, count] of sumFields(rollup, "conn_requests")) {
    if (value === "") continue;
    if (!/^-?\d{1,15}$/.test(value)) return null;
    const c = Number(value);
    if (c <= 0) continue;
    const label = bucketOf(c, SECURITY_CONN_REUSE_BUCKETS, SECURITY_CONN_REUSE_LAST);
    totals.set(label, (totals.get(label) ?? 0) + count);
  }
  return orderedBuckets(totals, SECURITY_CONN_REUSE_BUCKETS, SECURITY_CONN_REUSE_LAST);
}

/**
 * `buildSecurityIpv6AdoptionSql` per UTC hour, or null when a summary's
 * requests span more than one hour (a day bundle, or rows routed to a window of
 * another hour), since its `all_fields` can't be split.
 */
function ipv6Adoption(rollup: SecurityRollup): { time: string; pct: number }[] | null {
  const hours = new Map<number, { v6: number; total: number }>();
  for (const { summary } of rollup.files) {
    if (summary.requests === 0) continue;
    if (summary.totals.get("is_ipv6")?.truncated) return null;
    const hour = summary.minutes.length ? Math.floor(summary.minutes[0] / HOUR_MS) * HOUR_MS : null;
    if (hour === null || summary.minutes.some((m) => Math.floor(m / HOUR_MS) * HOUR_MS !== hour)) return null;
    let v6 = 0;
    for (const [value, count] of summary.fields.get("is_ipv6") ?? []) {
      if (IPV6_TRUE.has(value.toLowerCase())) v6 += count;
    }
    const into = hours.get(hour) ?? { v6: 0, total: 0 };
    into.v6 += v6;
    into.total += summary.requests;
    hours.set(hour, into);
  }
  return [...hours]
    .sort(([a], [b]) => a - b)
    .map(([hour, { v6, total }]) => ({ time: new Date(hour).toISOString(), pct: roundTo((v6 * 100) / total, 2) }));
}

/**
 * `buildSecurityVerifiedBotsTsSql` from the `verified_bot` bins, or null when
 * the bucket isn't a whole number of the summaries' steps or doesn't tile a
 * day (DuckDB's `time_bucket` counts from 2000-01-03, a day boundary).
 */
function verifiedBotsTs(rollup: SecurityRollup, bucketSeconds: unknown) {
  const requested = bucketSeconds == null ? 300 : Number(bucketSeconds);
  const seconds = Math.max(60, Math.trunc(Number.isFinite(requested) ? requested : 300));
  const stepMs = rollup.files.some(({ key }) => isDayBundle(key)) ? HOUR_MS : FIVE_MINUTES_MS;
  const width = seconds * 1000;
  if (width % stepMs !== 0 || 86_400 % seconds !== 0) return null;
  const counts = new Map<string, number>();
  for (const { summary } of rollup.files) {
    for (const [bot, byBucket] of summary.bins.get("verified_bot") ?? []) {
      for (const [t, n] of byBucket) {
        const key = JSON.stringify([Math.floor(t / width) * width, bot]);
        counts.set(key, (counts.get(key) ?? 0) + n);
      }
    }
  }
  return [...counts]
    .map(([key, count]) => {
      const [t, bot] = JSON.parse(key) as [number, string];
      return { t, bot_type: bot, count };
    })
    .sort((a, b) => a.t - b.t || (a.bot_type < b.bot_type ? -1 : a.bot_type > b.bot_type ? 1 : 0))
    .map(({ t, bot_type, count }) => ({ time: new Date(t).toISOString(), bot_type, count }));
}

const byValue = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const everyGroup = (rollup: SecurityRollup, group: string) =>
  rollup.files.every(({ summary }) => summary.groups.has(group));
const groupCut = (rollup: SecurityRollup, group: string) =>
  rollup.files.some(({ summary }) => summary.groupsCut.has(group));

function mergedBotUa(rollup: SecurityRollup) {
  const out = new Map<string, { bytes: number; requests: number; withIp: number }>();
  for (const { summary } of rollup.files) {
    for (const [ua, t] of summary.botUa) {
      const into = out.get(ua) ?? { bytes: 0, requests: 0, withIp: 0 };
      into.bytes += t.bytes;
      into.requests += t.requests;
      into.withIp += t.withIp;
      out.set(ua, into);
    }
  }
  return out;
}

/** `buildSecurityTopIpsHeaderSql`: each IP's largest header, top 10. */
function topIpsHeader(rollup: SecurityRollup, maskIps: boolean) {
  const max = new Map<string, number>();
  for (const { summary } of rollup.files) {
    for (const [ip, h] of summary.headerIp) max.set(ip, Math.max(max.get(ip) ?? -Infinity, h));
  }
  return [...max]
    .sort(([a, x], [b, y]) => y - x || byValue(a, b))
    .slice(0, 10)
    .map(([ip, max_header]) => ({ ip: maskIpValue(ip, maskIps), max_header }));
}

/**
 * The raw well-known-bots scan: the top candidate user agents by requests, the
 * known bots among them, and their requests from a non-empty IP (the summed
 * `(ua, ip)` pairs, whose IPs only the classification by pending or
 * unverified, not the counts, would read).
 */
async function wellknownBots(botUa: ReturnType<typeof mergedBotUa>) {
  const all = await loadWellKnownBots();
  const matched = [...botUa]
    .sort(([a, x], [b, y]) => y.requests - x.requests || byValue(a, b))
    .slice(0, WELLKNOWN_UA_CANDIDATES)
    .filter(([ua]) => matchWellKnownBots(all, ua).length > 0)
    .slice(0, WELLKNOWN_UA_MATCHES);
  return summarizeWellKnownBotRows(
    matched.filter(([, t]) => t.withIp > 0).map(([ua, t]) => [ua, "ip", t.withIp] as const),
  );
}

/**
 * `buildSecurityBotEfficiencySql`: the top user agents by bytes with the site's
 * bytes, and each referral stem's distinct referring IPs from the merged
 * `_ref_stem` sketches (stems kept without one are left out).
 */
function botEfficiency(rollup: SecurityRollup, botUa: ReturnType<typeof mergedBotUa>) {
  const tagged: [string, string, number, number][] = [];
  const uas = [...botUa]
    .sort(([a, x], [b, y]) => y.bytes - x.bytes || y.requests - x.requests || byValue(a, b))
    .slice(0, BOT_EFFICIENCY_UA_LIMIT);
  if (uas.length) {
    const site = rollup.files.reduce(
      (acc, { summary }) => [acc[0] + summary.requests, acc[1] + summary.bandwidth],
      [0, 0],
    );
    tagged.push(["site", "", site[0], site[1]]);
  }
  for (const [ua, t] of uas) tagged.push(["ua", ua, t.requests, t.bytes]);
  const stems = new Map<string, Hll>();
  for (const { summary } of rollup.files) {
    for (const [stem, sketch] of summary.refSketches) {
      if (stem === "") continue;
      const into = stems.get(stem) ?? new Hll();
      into.merge(sketch);
      stems.set(stem, into);
    }
  }
  const refs = [...stems].map(([stem, sketch]) => [stem, sketch.count()] as const).filter(([, n]) => n > 0);
  refs.sort(([a, x], [b, y]) => y - x || byValue(a, b));
  for (const [stem, n] of refs) tagged.push(["ref", stem, n, 0]);
  return summarizeBotEfficiencyRows(tagged);
}

/** The union of the files' `proxy` samples in sample-key order, and whether it holds every RTT row. */
function proxySample(rollup: SecurityRollup) {
  const rows = new Map<string, Record<string, unknown>>();
  for (const { summary } of rollup.files) for (const [key, row] of summary.proxy) rows.set(key, row);
  const keys = [...rows.keys()].sort(byValue);
  const exact = !groupCut(rollup, "proxy/sample") && keys.length <= PROXY_ROW_LIMIT;
  return { rows: keys.slice(0, PROXY_ROW_LIMIT).map((key) => rows.get(key)!), exact };
}

/**
 * The requested /security sections from decoded summaries, shaped like
 * `executeSecurityAggregates`, or null when one can't be answered exactly
 * (see each section). Counts are exact unless the summaries cut the field to
 * its top K (`lower_bound`); fingerprint IP counts merge HLL sketches
 * (`approximate`).
 */
export async function securityAggregatesFromRollup(
  rollup: SecurityRollup,
  body: SecurityRollupBody,
): Promise<(SecurityAggregatesResponse & { rollup: NetworkRollupMeta }) | null> {
  const sections = expandSecuritySections(body.sections);
  const out: SecurityAggregatesResponse = {};
  const approximate: string[] = [];
  const lowerBound: string[] = [];
  // Raw prefers a custom `client_ip` over `ip` and `proxy_type` over `p_type`; summaries hold only the latter.
  const present = (field: string) => rollup.files.some(({ summary }) => summary.totals.has(field));
  const requests = rollup.files.reduce((n, { summary }) => n + summary.requests, 0);
  const maskIps = Boolean(body.mask_ips);
  const rowSections = ["top_ips_header", "wellknown_bots", "bot_efficiency", "proxies"] as const;
  if (rowSections.some((section) => sections.has(section)) && present("client_ip")) return null;

  if (sections.has("tls_fingerprints")) {
    if (present("client_ip")) return null;
    const top = [...sumFields(rollup, TLS)].filter(([value]) => value !== "").sort(byCountThenValue).slice(0, 20);
    out.tls_fingerprints = top.map(([fingerprint, request_count]) => {
      const merged = new Hll();
      for (const { summary } of rollup.files) {
        const sketch = summary.sketches.get(fingerprint);
        if (sketch) merged.merge(sketch);
      }
      return { fingerprint, ip_count: merged.count(), request_count };
    });
    approximate.push("tls_fingerprints.ip_count");
    if (fieldCut(rollup, TLS)) lowerBound.push("tls_fingerprints.request_count");
  }

  if (sections.has("fingerprint_coverage")) {
    let populated = 0;
    for (const { summary } of rollup.files) {
      const total = summary.totals.get(TLS);
      if (!total) continue;
      const empty = summary.fields.get(TLS)?.get("");
      // A cut field may have dropped '' with the other rare values.
      if (empty === undefined && total.truncated) return null;
      populated += total.count - (empty ?? 0);
    }
    out.fingerprint_coverage = { tls_ciphers_sha: requests > 0 ? populated / requests : 0.0 };
  }

  if (sections.has("req_size_dist")) {
    out.req_size_dist = orderedBuckets(binTotals(rollup, "req_header_size"), SECURITY_REQ_HEADER_BUCKETS, SECURITY_REQ_HEADER_LAST);
    if (rollup.files.some(({ summary }) => summary.binsCut.has("req_header_size"))) lowerBound.push("req_size_dist");
  }

  if (sections.has("conn_reuse_dist")) {
    const dist = connReuseDist(rollup);
    if (!dist) return null;
    out.conn_reuse_dist = dist;
    if (fieldCut(rollup, "conn_requests")) lowerBound.push("conn_reuse_dist");
  }

  if (sections.has("ipv6_adoption")) {
    const points = ipv6Adoption(rollup);
    if (!points) return null;
    out.ipv6_adoption = points;
  }

  if (sections.has("proxy_dist")) {
    if (present("proxy_type")) return null;
    out.proxy_dist = [...sumFields(rollup, "p_type")]
      .filter(([type]) => type !== "")
      .sort(byCountThenValue)
      .map(([type, count]) => ({ type, count }));
    if (fieldCut(rollup, "p_type")) lowerBound.push("proxy_dist");
  }

  if (sections.has("verified_bots_ts")) {
    const points = verifiedBotsTs(rollup, body.bucket_seconds);
    if (!points) return null;
    out.verified_bots_ts = points;
    if (rollup.files.some(({ summary }) => summary.binsCut.has("verified_bot"))) lowerBound.push("verified_bots_ts");
  }

  if (sections.has("ngwaf_verified_bots") || sections.has("ngwaf_verified_bots_ts")) {
    out.ngwaf_configured = false;
    out.ngwaf_verified_bots = [];
    out.ngwaf_verified_bots_ts = [];
  }

  // The top 10 are exact even when a file kept only its top IPs: an IP in the
  // overall top 10 is in the top of the file holding its largest header.
  if (sections.has("top_ips_header")) {
    if (!everyGroup(rollup, "header_ip/ip")) return null;
    out.top_ips_header = topIpsHeader(rollup, maskIps);
  }

  const needsUa = sections.has("wellknown_bots") || sections.has("bot_efficiency");
  if (needsUa && !everyGroup(rollup, "bot_ua/ua")) return null;
  const botUa = needsUa ? mergedBotUa(rollup) : new Map();
  const uaCut = groupCut(rollup, "bot_ua/ua");

  if (sections.has("wellknown_bots")) {
    out.wellknown_bots = await wellknownBots(botUa);
    if (uaCut) lowerBound.push("wellknown_bots");
  }

  if (sections.has("bot_efficiency")) {
    if (!rollup.files.every(({ summary }) => summary.totals.has(REF_STEM))) return null;
    out.bot_efficiency = await botEfficiency(rollup, botUa);
    approximate.push("bot_efficiency.referrals");
    if (uaCut) lowerBound.push("bot_efficiency");
  }

  if (sections.has("top_bots")) {
    const top = [...sumFields(rollup, "ua")].filter(([ua]) => ua !== "").sort(byCountThenValue)
      .slice(0, BOT_UA_CLASSIFY_LIMIT);
    out.bots = await classifyUserAgentCounts(top, 10);
    out.ngwaf_bots = [];
    if (fieldCut(rollup, "ua")) lowerBound.push("top_bots");
  }

  if (sections.has("proxies")) {
    if (!everyGroup(rollup, "proxy/sample")) return null;
    const { rows, exact } = proxySample(rollup);
    Object.assign(out, shapeSecurityProxiesFromRawRows(rows, maskIps));
    if (!exact) approximate.push("proxies");
  }

  return {
    ...out,
    rollup: {
      data_from: new Date(rollup.dataFromMs).toISOString(),
      data_through: new Date(rollup.dataThroughMs).toISOString(),
      approximate,
      lower_bound: lowerBound,
    },
  };
}

/**
 * `executeSecurityThreatIntel` from the `tls_fp` rows: requests, proxied
 * requests and the busiest country per fingerprint, top 50. Null when the
 * summaries lack the rows or the data has WAF rule or `is_bot` columns, whose
 * counts they don't keep. Requests are lower bounds when a file cut its
 * fingerprints, and the country a guess when it cut its countries.
 */
export function threatIntelFromRollup(
  rollup: SecurityRollup,
): (ThreatIntelListResponse & { rollup: NetworkRollupMeta }) | null {
  if (!everyGroup(rollup, "tls_fp/fp") || !everyGroup(rollup, "tls_fp/fp_country")) return null;
  const custom = ["waf_logged_rules", "waf_blocked_rules", "is_bot"];
  if (rollup.files.some(({ summary }) => custom.some((field) => summary.totals.has(field)))) return null;
  const fps = new Map<string, { requests: number; proxied: number }>();
  for (const { summary } of rollup.files) {
    for (const [fp, t] of summary.fp) {
      const into = fps.get(fp) ?? { requests: 0, proxied: 0 };
      into.requests += t.requests;
      into.proxied += t.proxied;
      fps.set(fp, into);
    }
  }
  const countries = new Map<string, number>();
  for (const { summary } of rollup.files) {
    for (const [key, n] of summary.fpCountry) countries.set(key, (countries.get(key) ?? 0) + n);
  }
  const top = new Map<string, [country: string, requests: number]>();
  for (const [key, n] of countries) {
    const [fp, country] = JSON.parse(key) as [string, string];
    const best = top.get(fp);
    if (!best || n > best[1] || (n === best[1] && country < best[0])) top.set(fp, [country, n]);
  }
  const rows = [...fps]
    .sort(([a, x], [b, y]) => y.requests - x.requests || byValue(a, b))
    .slice(0, THREAT_INTEL_LIMIT)
    .map(([fp, t]) => ({
      tls_fingerprint: fp,
      requests: t.requests,
      top_country: top.get(fp)?.[0] ?? null,
      matched_waf_rules_count: 0,
      bot_percentage: 0,
      is_anonymized_proxy: t.proxied > 0,
    }));
  return {
    ...shapeSecurityThreatIntelRows(rows),
    rollup: {
      data_from: new Date(rollup.dataFromMs).toISOString(),
      data_through: new Date(rollup.dataThroughMs).toISOString(),
      approximate: groupCut(rollup, "tls_fp/fp_country") ? ["threat_intel.top_country"] : [],
      lower_bound: groupCut(rollup, "tls_fp/fp") ? ["threat_intel.requests"] : [],
    },
  };
}

const THREAT_INTEL_PATH = /^\/api\/security\/threat-intel\/*$/;

/** Whether summaries may answer this /security read at all, before any range or listing is resolved. */
export function securityRollupEligible(path: string, body: SecurityRollupBody): boolean {
  if (hasFilterValues(body.filters)) return false;
  if (THREAT_INTEL_PATH.test(path)) return true;
  if (!/^\/api\/security\/(?:aggregates|bundle)\/*$/.test(path)) return false;
  const servable = new Set<string>(SECURITY_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * Answers an unfiltered Cloud /security aggregates or threat-intel read from
 * summaries, or returns null when the raw scan must: a filter, an unknown
 * section, a range the summaries don't cover, summaries without a section's
 * rows, or a section they can't answer (see each section). Reads only the
 * summary columns and groups the read uses (`spec`; null reads whole files).
 */
export async function securityFromRollup(
  path: string,
  body: SecurityRollupBody,
  source: Source,
  signal?: AbortSignal,
  spec: SummarySpec | null = THREAT_INTEL_PATH.test(path)
    ? SECURITY_THREAT_INTEL_SUMMARY_SPEC : securitySummarySpec(body.sections),
) {
  if (!securityRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const threatIntel = THREAT_INTEL_PATH.test(path);
  const hourly = !threatIntel && expandSecuritySections(body.sections).has("ipv6_adoption");
  const rollup = await querySecurityRollup(source, { fromMs, toMs, hourly, signal, spec });
  if (!rollup) return null;
  return threatIntel ? threatIntelFromRollup(rollup) : securityAggregatesFromRollup(rollup, body);
}
