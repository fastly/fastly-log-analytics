import type { RollupResult } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import { hasFilterValues, VALUE_ROLLUP_SECTIONS } from "./rollup-sections";
import { emptyValueSummary, type ValueQueryParams } from "./value";
import type { LoggedHour } from "./value-stats";

export type ValueRollupBody = Pick<ValueQueryParams, "start_time" | "end_time" | "chart_interval"> & {
  filters?: unknown;
  custom_filters?: unknown;
  sections?: readonly string[] | null;
};

type ValueSummary = ReturnType<typeof emptyValueSummary>;
type ValuePoint = NonNullable<ValueSummary["network"]["protocol_time_series"]>[number];

/** `queryValueSummary`'s chart buckets; an unknown interval is a day there too. */
const CHART_INTERVAL_SECONDS: Record<string, number> = {
  "1 second": 1, "1 minute": 60, "5 minutes": 300, "1 hour": 3600, "1 day": 86400,
};

/**
 * Columns the raw scan coalesces ahead of the Cloud ones the summaries read
 * (`cache`, `proto`, `tls`, `is_ipv6`, `resp_bytes`, `elapsed`); any of them
 * logged as a custom field would change the raw answer.
 */
const RAW_ALIASES = [
  "cache_status", "resp_body_size", "resp_body_bytes", "time_elapsed_usec", "protocol", "is_ssl",
  "tls_version", "ipv6", "waf_action", "ngwaf_decision",
];

const ASCII = /^[\x00-\x7f]*$/;
const HIT_STATES = new Set(["HIT", "HIT-STALE", "HIT-CLUSTER"]);

/** Whether summaries may answer this /fastly-value read at all, before any range or listing is resolved. */
export function valueRollupEligible(path: string, body: ValueRollupBody): boolean {
  if (!/^\/api\/value\/summary\/*$/.test(path) || hasFilterValues(body.filters, body.custom_filters)) return false;
  const servable = new Set<string>(VALUE_ROLLUP_SECTIONS);
  return Array.isArray(body.sections) && body.sections.length > 0 &&
    body.sections.every((section) => servable.has(section));
}

/** Requests over the `all_fields` values of `field` that `match` accepts. */
function countWhere(values: Map<string, number> | undefined, match: (value: string) => boolean): number {
  let n = 0;
  for (const [value, count] of values ?? []) if (match(value)) n += count;
  return n;
}

const isH3 = (p: string) => p.startsWith("HTTP/3") || p === "H3" || p === "3";
const isH2 = (p: string) => p.startsWith("HTTP/2") || p === "H2" || p === "2";
const isH11 = (p: string) => p.startsWith("HTTP/1.1") || p === "1.1";

/**
 * The requested /fastly-value overview and network sections from merged
 * summaries, shaped like `queryValueSummary`, or null when they can't match it
 * exactly. Counts come from the `all_fields` value counts (which skip nulls,
 * as the raw `COUNT`s do) and need every value: a field cut to its top K falls
 * back. The overview's hit and miss latency averages come from the overview
 * row's sums, which take only `HIT`/`HIT-STALE`/`HIT-CLUSTER` and `MISS`, so
 * any other cache state the raw prefix match would take falls back, as does a
 * zero byte total (the summary can't tell all-null bytes from zero bytes).
 * The protocol series rebuckets `series5` request counts by protocol.
 */
export function valueSummaryFromRollup(
  result: RollupResult,
  body: ValueRollupBody,
): (Partial<ValueSummary> & { rollup: NetworkRollupMeta & { logged_hours?: LoggedHour[] } }) | null {
  const { rollup } = result;
  const { fields, truncated, overview } = rollup;
  const sections = (body.sections ?? []) as (keyof ValueSummary)[];
  const want = (section: keyof ValueSummary) => sections.includes(section);
  if (!overview || RAW_ALIASES.some((alias) => fields.has(alias))) return null;
  const needed = [...(want("overview") ? ["cache"] : []), ...(want("network") ? ["proto", "tls", "is_ipv6"] : [])];
  if (needed.some((field) => truncated.has(`all_fields/${field}`))) return null;
  for (const field of ["cache", "proto"]) {
    if (needed.includes(field) && [...fields.get(field)?.keys() ?? []].some((value) => !ASCII.test(value))) return null;
  }
  const meta: NetworkRollupMeta & { logged_hours?: LoggedHour[] } = {
    data_from: new Date(result.dataFromMs).toISOString(),
    data_through: new Date(result.dataThroughMs).toISOString(),
    approximate: [],
    lower_bound: [],
  };
  if (want("overview")) {
    // The adapter compares these with Fastly's Stats API hours and drops them from the answer.
    const hourly = rebucketSeries(rollup, "overall", 3_600_000)?.get("") ?? new Map();
    meta.logged_hours = [...hourly].sort(([a], [z]) => a - z).map(([t, agg]) => [t, agg.requests, agg.respBytes]);
  }
  const out = emptyValueSummary();
  const total = overview.requests;
  const pick = () => ({ ...Object.fromEntries(sections.map((section) => [section, out[section]])), rollup: meta });
  if (!total) return pick();

  if (want("overview")) {
    const cache = fields.get("cache");
    const state = (prefix: string) => countWhere(cache, (value) => value.toUpperCase().startsWith(prefix));
    for (const value of cache?.keys() ?? []) {
      const upper = value.toUpperCase();
      if (upper.startsWith("HIT") && !HIT_STATES.has(value)) return null;
      if (upper.startsWith("MISS") && value !== "MISS") return null;
    }
    if (overview.totalBandwidth === 0) return null;
    const hits = state("HIT");
    const hasCache = countWhere(cache, (value) => value !== "") > 0;
    const avg = (sum: number, n: number) => (n ? roundTo(sum / n / 1000, 2) : null);
    const avgHit = avg(overview.hitElapsedSum, overview.hitElapsedCount);
    const avgMiss = avg(overview.missElapsedSum, overview.missElapsedCount);
    out.overview = {
      total_requests: total,
      total_bandwidth_bytes: roundTo(overview.totalBandwidth, 2),
      origin_offload_pct: hasCache ? roundTo(hits * 100 / total, 2) : null,
      cache_acceleration_factor: avgHit !== null && avgHit > 0 && avgMiss !== null ? roundTo(avgMiss / avgHit, 2) : null,
      threats_blocked: null,
      edge_hit_requests: hasCache ? hits : null,
      origin_requests: hasCache ? state("MISS") + state("PASS") : null,
    };
  }

  if (want("network")) {
    const proto = fields.get("proto");
    const tls = fields.get("tls");
    const ipv6 = fields.get("is_ipv6");
    const protocolSamples = countWhere(proto, (value) => value !== "");
    const tlsSamples = countWhere(tls, () => true);
    const ipv6Samples = countWhere(ipv6, () => true);
    const pct = (samples: number, n: number) => (samples ? roundTo(n * 100 / total, 2) : null);
    const series: ValuePoint[] = [];
    if (protocolSamples) {
      if (truncated.has("series5/proto")) return null;
      const bucketMs = (CHART_INTERVAL_SECONDS[body.chart_interval ?? "1 day"] ?? 86400) * 1000;
      const overall = rebucketSeries(rollup, "overall", bucketMs);
      const byProto = rebucketSeries(rollup, "proto", bucketMs);
      if (!overall || !byProto) return null;
      const buckets = new Map<number, { total: number; h3: number; h2: number; h11: number }>();
      for (const [t, agg] of overall.get("") ?? []) buckets.set(t, { total: agg.requests, h3: 0, h2: 0, h11: 0 });
      for (const [value, byBucket] of byProto) {
        const upper = value.toUpperCase();
        const key = isH3(upper) ? "h3" : isH2(upper) ? "h2" : isH11(upper) ? "h11" : null;
        if (!key) continue;
        for (const [t, agg] of byBucket) {
          const bucket = buckets.get(t);
          if (!bucket) return null;
          bucket[key] += agg.requests;
        }
      }
      for (const [t, b] of [...buckets].sort(([a], [z]) => a - z)) {
        const time = new Date(t).toISOString();
        for (const [key, category] of [["h3", "HTTP/3"], ["h2", "HTTP/2"], ["h11", "HTTP/1.1"]] as const) {
          if (b[key]) series.push({ time, value: b[key], category });
        }
        const other = b.total - b.h3 - b.h2 - b.h11;
        if (other) series.push({ time, value: other, category: "Other" });
      }
    }
    out.network = {
      total_requests: total,
      http3_pct: pct(protocolSamples, countWhere(proto, (value) => isH3(value.toUpperCase()))),
      h2_pct: pct(protocolSamples, countWhere(proto, (value) => isH2(value.toUpperCase()))),
      tls_pct: pct(tlsSamples, countWhere(tls, (value) => value !== "")),
      ipv6_pct: pct(ipv6Samples, ipv6?.get("true") ?? 0),
      protocol_time_series: series,
    };
  }
  return pick();
}

/**
 * Answers an unfiltered Cloud /fastly-value read of the overview and network
 * sections from v3 summaries, or returns null when the raw scan must: a
 * filter, another section (caching, performance, security, bots, IO), or a
 * range the summaries don't cover (see `queryRollup`).
 */
export async function valueFromRollup(
  path: string,
  body: ValueRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
) {
  if (!valueRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal });
  return result ? valueSummaryFromRollup(result, body) : null;
}
