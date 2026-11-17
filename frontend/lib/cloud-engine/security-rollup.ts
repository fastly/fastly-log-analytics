import { parquetMetadata } from "hyparquet";
import type { FileSource } from "./dashboard/engine";
import { Hll } from "./dashboard/hll";
import { fetchManifest, type ManifestFile } from "./dashboard/manifest";
import { planRollup } from "./dashboard/rollup-query";
import { SUMMARY_VERSION } from "./dashboard/summaries";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import { hasFilterValues, SECURITY_ROLLUP_SECTIONS } from "./rollup-sections";
import {
  expandSecuritySections,
  SECURITY_CONN_REUSE_BUCKETS,
  SECURITY_CONN_REUSE_LAST,
  SECURITY_REQ_HEADER_BUCKETS,
  SECURITY_REQ_HEADER_LAST,
  type SecurityAggregatesResponse,
  type SecurityBucketItem,
} from "./security";

export type SecurityRollupBody = {
  start_time?: string | number | null;
  end_time?: string | number | null;
  filters?: unknown;
  bucket_seconds?: unknown;
  sections?: readonly string[] | null;
};

type Source = Pick<FileSource, "fetchJson" | "fetchFile" | "cacheScope">;

const FIVE_MINUTES_MS = 300_000;
const HOUR_MS = 3_600_000;
const TLS = "tls_ciphers_sha";
/** The `all_fields` values the sections read; others are skipped while decoding. */
const FIELDS = new Set([TLS, "conn_requests", "p_type", "is_ipv6"]);
const IPV6_TRUE = new Set(["1", "true", "t", "yes"]);

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
};

const num = (value: unknown) =>
  typeof value === "bigint" ? Number(value) : typeof value === "number" ? value : 0;

function bucketMs(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  return null;
}

/** Decodes the rows /security reads from a v3 summary, or null for another format version. */
export async function decodeSecuritySummary(bytes: ArrayBuffer): Promise<SecuritySummary | null> {
  const version = parquetMetadata(bytes).key_value_metadata?.find((kv) => kv.key === "flp_summary")?.value;
  if (version !== SUMMARY_VERSION) return null;
  const { readColumns } = await import("./dashboard/parquet");
  const { rows, columns } = await readColumns(bytes, [
    "bundle", "bucket", "field", "value", "truncated", "count", "requests",
  ]);
  const sketches = (await readColumns(bytes, ["ip_sketch"], false)).columns.get("ip_sketch") ?? [];
  const col = (name: string) => columns.get(name) ?? [];
  const [group, bucket, field, value, truncated, count, requests] =
    ["bundle", "bucket", "field", "value", "truncated", "count", "requests"].map(col);
  const out: SecuritySummary = {
    requests: 0, minutes: [], bins: new Map(), binsCut: new Set(), fields: new Map(), totals: new Map(), sketches: new Map(),
  };
  for (let i = 0; i < rows; i++) {
    const name = String(field[i] ?? "");
    switch (group[i]) {
      case "overview":
        out.requests += num(requests[i]);
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
        if (name !== TLS || !(blob instanceof Uint8Array)) break;
        const hll = Hll.fromBytes(blob);
        if (!hll) return null;
        const key = String(value[i] ?? "");
        const into = out.sketches.get(key);
        if (into) into.merge(hll);
        else out.sketches.set(key, hll);
        break;
      }
    }
  }
  return out;
}

const DECODED_LIMIT = 64;
const decoded = new Map<string, Promise<SecuritySummary | null>>();

function decodeCached(source: Source, file: ManifestFile): Promise<SecuritySummary | null> {
  const key = `${source.cacheScope ?? "default"}|${file.key}`;
  const hit = decoded.get(key);
  if (hit) {
    decoded.delete(key);
    decoded.set(key, hit);
    return hit;
  }
  const pending = source.fetchFile(file.key).then(({ bytes }) => decodeSecuritySummary(bytes));
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
  range: { fromMs: number; toMs: number; hourly: boolean; signal?: AbortSignal },
): Promise<SecurityRollup | null> {
  const { fromMs, toMs, hourly, signal } = range;
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
  const parts = await Promise.all(plan.files.map((use) => decodeCached(source, use.file)));
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

/**
 * The requested /security sections from decoded summaries, shaped like
 * `executeSecurityAggregates`, or null when one can't be answered exactly
 * (see each section). Counts are exact unless the summaries cut the field to
 * its top K (`lower_bound`); fingerprint IP counts merge HLL sketches
 * (`approximate`).
 */
export function securityAggregatesFromRollup(
  rollup: SecurityRollup,
  body: SecurityRollupBody,
): (SecurityAggregatesResponse & { rollup: NetworkRollupMeta }) | null {
  const sections = expandSecuritySections(body.sections);
  const out: SecurityAggregatesResponse = {};
  const approximate: string[] = [];
  const lowerBound: string[] = [];
  // Raw prefers a custom `client_ip` over `ip` and `proxy_type` over `p_type`; summaries hold only the latter.
  const present = (field: string) => rollup.files.some(({ summary }) => summary.totals.has(field));
  const requests = rollup.files.reduce((n, { summary }) => n + summary.requests, 0);

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

/** Whether summaries may answer this /security read at all, before any range or listing is resolved. */
export function securityRollupEligible(path: string, body: SecurityRollupBody): boolean {
  if (!/^\/api\/security\/(?:aggregates|bundle)\/*$/.test(path) || hasFilterValues(body.filters)) return false;
  const servable = new Set<string>(SECURITY_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/**
 * Answers an unfiltered Cloud /security aggregates read of the summary-backed
 * sections, or returns null when the raw scan must: a filter, a row-level
 * section (well-known bots, bot efficiency, header outliers, proxies), a range
 * the summaries don't cover, or a section they can't answer exactly.
 */
export async function securityFromRollup(
  path: string,
  body: SecurityRollupBody,
  source: Source,
  signal?: AbortSignal,
) {
  if (!securityRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const hourly = expandSecuritySections(body.sections).has("ipv6_adoption");
  const rollup = await querySecurityRollup(source, { fromMs, toMs, hourly, signal });
  return rollup ? securityAggregatesFromRollup(rollup, body) : null;
}
