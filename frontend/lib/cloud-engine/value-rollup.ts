import type { Rollup, RollupResult, RollupValue } from "./dashboard/rollup-query";
import { queryRollup, rebucketSeries, sumAggs } from "./dashboard/rollup-query";
import { parseEpochMs, roundTo } from "./network";
import type { NetworkRollupMeta } from "./network-rollup";
import { hasFilterValues, VALUE_ROLLUP_SECTIONS } from "./rollup-sections";
import { emptyValueSummary, type ValueQueryParams } from "./value";
import { defineSummarySpec, type SummarySpec } from "./dashboard/summary-projection";
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
 * Columns the raw scan coalesces ahead of (or beside) the Cloud ones the
 * summaries read (`cache`, `proto`, `tls`, `is_ipv6`, `resp_bytes`, `elapsed`,
 * `edge`, `_bot_name`, the Image Optimizer fields); any of them logged as a
 * custom field would change the raw answer.
 */
const RAW_ALIASES = [
  "cache_status", "resp_body_size", "resp_body_bytes", "time_elapsed_usec", "protocol", "is_ssl",
  "tls_version", "ipv6", "waf_action", "ngwaf_decision", "fastly_is_shield", "is_shield", "content_type",
  "resp_header_content_type", "bot_verified", "verified_bot", "ngwaf_bot_name", "bot_category", "is_bot",
  "ofmt", "ifmt",
];

/** The `value` fields each section reads; summaries without one leave the section to the raw scan. */
const VALUE_FIELDS: Partial<Record<keyof ValueSummary, readonly string[]>> = {
  caching: ["cache", "shield"],
  performance: ["cache"],
  security: ["waf_signal"],
  bots: ["bot_name"],
  io: ["cache", "io", "io_both", "io_pair"],
};

/** Sections with a per-day series: summaries keep `value` rows per UTC day only. */
const DAILY_SECTIONS = new Set<keyof ValueSummary>(["caching", "performance", "io"]);

const MODERN_FORMATS = new Set(["webp", "avif", "jpegxl", "jxl"]);

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

function emptyValue(): RollupValue {
  return {
    requests: 0, respBytes: 0, bytesCount: 0, elapsedSum: 0, elapsedCount: 0, ioIn: 0, ioOut: 0, ratioSum: 0, ratioCount: 0,
  };
}

function addValue(into: RollupValue, from: RollupValue) {
  for (const key of Object.keys(into) as (keyof RollupValue)[]) into[key] += from[key];
}

/** One `value` field's sums over its days, by value. */
function valueTotals(rollup: Rollup, field: string): Map<string, RollupValue> {
  const out = new Map<string, RollupValue>();
  for (const [value, days] of rollup.value?.get(field) ?? []) {
    const into = out.get(value) ?? emptyValue();
    out.set(value, into);
    for (const sums of days.values()) addValue(into, sums);
  }
  return out;
}

/** One `value` field's sums by day, then value. */
function valueByDay(rollup: Rollup, field: string): Map<number, Map<string, RollupValue>> {
  const out = new Map<number, Map<string, RollupValue>>();
  for (const [value, days] of rollup.value?.get(field) ?? []) {
    for (const [day, sums] of days) {
      const values = out.get(day) ?? new Map<string, RollupValue>();
      out.set(day, values);
      const into = values.get(value) ?? emptyValue();
      values.set(value, into);
      addValue(into, sums);
    }
  }
  return out;
}

const sumOf = (values: Iterable<RollupValue>) => {
  const out = emptyValue();
  for (const v of values) addValue(out, v);
  return out;
};

/** The top `limit` values by requests (ties by value), as `[value, requests]`. */
function topByRequests(totals: Map<string, RollupValue>, limit: number): [string, number][] {
  return [...totals]
    .map(([value, v]) => [value, v.requests] as [string, number])
    .sort(([a, x], [b, y]) => y - x || (a < b ? -1 : a > b ? 1 : 0))
    .slice(0, limit);
}

const isH3 = (p: string) => p.startsWith("HTTP/3") || p === "H3" || p === "3";
const isH2 = (p: string) => p.startsWith("HTTP/2") || p === "H2" || p === "2";
const isH11 = (p: string) => p.startsWith("HTTP/1.1") || p === "1.1";

/**
 * The requested /fastly-value sections from merged summaries, shaped like
 * `queryValueSummary`, or null when they can't match it. Counts come from the
 * `all_fields` value counts (which skip nulls, as the raw `COUNT`s do) and the
 * per-day `value` rows; a field cut to its top K falls back, except the top-20
 * WAF signal, bot and format-pair lists, whose counts become lower bounds.
 * The overview, caching, performance and IO figures come from the `value`
 * cache-class rows, which split cache states as the raw prefix match does;
 * summaries written before them answer the overview from the overview row's
 * HIT/MISS sums, falling back on any other cache state that match would take.
 * Day series need a "1 day" chart interval. The p99 latency comes from the
 * elapsed histogram (`approximate`). The protocol series rebuckets `series5`
 * request counts by protocol. WAF decisions, verified bots, image content
 * types and URL-level opportunities aren't in Cloud logs, so they read as the
 * raw scan's empty values, and any of those columns logged as a custom field
 * falls back.
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
  const hasValue = (field: string) => rollup.present?.has(`value/${field}`) === true;
  const daily = (CHART_INTERVAL_SECONDS[body.chart_interval ?? "1 day"] ?? 86400) === 86400;
  for (const section of sections) {
    const needs = VALUE_FIELDS[section];
    if (!needs) continue;
    if (!needs.every(hasValue) || (DAILY_SECTIONS.has(section) && !daily)) return null;
  }
  const byClass = hasValue("cache") ? valueTotals(rollup, "cache") : null;
  if (byClass && ["cache", "shield", "io"].some((field) => truncated.has(`value/${field}`))) return null;
  const needed = [
    ...(want("overview") && !byClass ? ["cache"] : []),
    ...(want("network") ? ["proto", "tls", "is_ipv6"] : []),
    ...(want("io") ? ["io_output_format"] : []),
  ];
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
    // The adapter compares these with Fastly Platform Metrics hours and drops them from the answer.
    const hourly = rebucketSeries(rollup, "overall", 3_600_000)?.get("") ?? new Map();
    meta.logged_hours = [...hourly].sort(([a], [z]) => a - z).map(([t, agg]) => [t, agg.requests, agg.respBytes]);
  }
  const out = emptyValueSummary();
  const total = overview.requests;
  const pick = () => ({ ...Object.fromEntries(sections.map((section) => [section, out[section]])), rollup: meta });
  if (!total) return pick();

  const cls = (name: string) => byClass?.get(name) ?? emptyValue();
  const hasCache = byClass ? [...byClass].some(([name, v]) => name !== "" && v.requests > 0) : false;
  const hits = cls("HIT").requests;
  const offload = hasCache ? roundTo(hits * 100 / total, 2) : null;
  const avgOf = (v: RollupValue) => (v.elapsedCount ? roundTo(v.elapsedSum / v.elapsedCount / 1000, 2) : null);
  const avgHit = avgOf(cls("HIT"));
  const avgMiss = avgOf(cls("MISS"));
  const acceleration = avgHit !== null && avgHit > 0 && avgMiss !== null ? roundTo(avgMiss / avgHit, 2) : null;
  const days = byClass ? [...valueByDay(rollup, "cache")].sort(([a], [b]) => a - b) : [];

  if (want("overview")) {
    if (byClass) {
      const bytes = sumOf(byClass.values());
      out.overview = {
        total_requests: total,
        total_bandwidth_bytes: bytes.bytesCount ? roundTo(bytes.respBytes, 2) : null,
        origin_offload_pct: offload,
        cache_acceleration_factor: acceleration,
        threats_blocked: null,
        edge_hit_requests: hasCache ? hits : null,
        origin_requests: hasCache ? cls("MISS").requests + cls("PASS").requests : null,
      };
    } else {
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
  }

  if (want("caching")) {
    const shieldByClass = valueTotals(rollup, "shield");
    const shield = sumOf(shieldByClass.values()).requests;
    const shieldHits = shieldByClass.get("HIT")?.requests ?? 0;
    const bytes = sumOf(byClass!.values());
    const offloadSeries: ValuePoint[] = [];
    const stateSeries: ValuePoint[] = [];
    if (hasCache) {
      for (const [day, values] of days) {
        const time = new Date(day).toISOString();
        const n = (name: string) => values.get(name)?.requests ?? 0;
        const dayTotal = sumOf(values.values()).requests;
        offloadSeries.push({ time, value: roundTo(n("HIT") * 100 / dayTotal, 2) });
        for (const name of ["HIT", "MISS", "PASS"]) stateSeries.push({ time, value: n(name), category: name });
        const other = dayTotal - n("HIT") - n("MISS") - n("PASS");
        if (other) stateSeries.push({ time, value: other, category: "OTHER" });
      }
    }
    out.caching = {
      total_requests: total,
      hit_requests: hasCache ? hits : null,
      miss_requests: hasCache ? cls("MISS").requests : null,
      pass_requests: hasCache ? cls("PASS").requests : null,
      synth_requests: hasCache ? cls("SYNTH").requests : null,
      origin_offload_pct: offload,
      bandwidth_saved_bytes: bytes.bytesCount ? cls("HIT").respBytes : null,
      shield_effectiveness_pct: shield ? roundTo(shieldHits * 100 / shield, 2) : null,
      offload_time_series: offloadSeries,
      cache_state_time_series: stateSeries,
    };
  }

  if (want("performance")) {
    const elapsed = sumAggs(rollup.series.get("overall")?.get("")?.values() ?? []).hists.elapsed;
    const p99 = elapsed?.quantile(0.99);
    const series: ValuePoint[] = [];
    for (const [day, values] of days) {
      const time = new Date(day).toISOString();
      for (const [name, category] of [["HIT", "hit"], ["MISS", "miss"]] as const) {
        const v = values.get(name);
        if (v?.elapsedCount) series.push({ time, value: v.elapsedSum / v.elapsedCount / 1000, category });
      }
    }
    out.performance = {
      total_requests: total,
      cache_accel_factor: acceleration,
      avg_hit_latency_ms: avgHit,
      avg_miss_latency_ms: avgMiss,
      p99_latency_ms: p99 == null ? null : roundTo(p99 / 1000, 2),
      latency_time_series: series,
    };
    if (p99 != null) meta.approximate.push("performance.p99_latency_ms");
  }

  if (want("security")) {
    out.security.total_requests = total;
    out.security.top_waf_signals = topByRequests(valueTotals(rollup, "waf_signal"), 20)
      .map(([signal, count]) => ({ signal, count }));
    if (truncated.has("value/waf_signal")) meta.lower_bound.push("security.top_waf_signals");
  }

  if (want("bots")) {
    const names = valueTotals(rollup, "bot_name");
    const bots = rollup.valueTotals?.get("bot_name") ?? sumOf(names.values()).requests;
    out.bots = {
      total_requests: total,
      bot_requests: bots || null,
      verified_bots: null,
      top_bots: topByRequests(names, 20).map(([name, count]) => ({ name, count })),
    };
    if (truncated.has("value/bot_name")) meta.lower_bound.push("bots.top_bots");
  }

  if (want("io")) {
    const legs = valueTotals(rollup, "io");
    const io = sumOf(legs.values());
    const shieldLeg = legs.get("false");
    const both = sumOf(valueTotals(rollup, "io_both").values());
    const ioDays = valueByDay(rollup, "io");
    const bothDays = valueByDay(rollup, "io_both");
    const ioSeries: ValuePoint[] = [];
    const bandwidthSeries: ValuePoint[] = [];
    const savedSeries: ValuePoint[] = [];
    for (const [day] of days) {
      const time = new Date(day).toISOString();
      const dayIo = sumOf(ioDays.get(day)?.values() ?? []);
      const dayBoth = sumOf(bothDays.get(day)?.values() ?? []);
      if (io.requests) ioSeries.push({ time, value: dayIo.requests });
      if (dayIo.bytesCount) bandwidthSeries.push({ time, value: dayIo.respBytes });
      if (dayBoth.requests) savedSeries.push({ time, value: dayBoth.ioIn - dayBoth.ioOut });
    }
    const formats = [...fields.get("io_output_format") ?? []]
      .filter(([format, count]) => format !== "" && count > 0)
      .sort(([a, x], [b, y]) => y - x || (a < b ? -1 : a > b ? 1 : 0));
    const formatTotal = formats.reduce((sum, [, count]) => sum + count, 0);
    const modern = formats.reduce((sum, [format, count]) => sum + (MODERN_FORMATS.has(format.toLowerCase()) ? count : 0), 0);
    const pairs = valueTotals(rollup, "io_pair");
    out.io = {
      ...out.io,
      total_requests: total,
      io_transforms: io.requests || null,
      io_bandwidth_bytes: io.bytesCount ? io.respBytes : null,
      io_shield_bandwidth_bytes: shieldLeg?.bytesCount ? shieldLeg.respBytes : null,
      io_pct_of_traffic: io.requests ? roundTo(io.requests * 100 / total, 2) : null,
      modern_format_pct: formatTotal ? roundTo(modern * 100 / formatTotal, 2) : null,
      image_request_count: 0,
      image_bandwidth_bytes: null,
      io_actual_bandwidth_saved_bytes: both.requests ? both.ioIn - both.ioOut : null,
      io_actual_compression_ratio: both.requests && both.ioOut > 0 ? roundTo(both.ioIn / both.ioOut, 2) : null,
      io_time_series: ioSeries,
      io_bandwidth_time_series: bandwidthSeries,
      io_compression_time_series: savedSeries,
      format_distribution: formats.map(([format, count]) => ({ format, count, pct: roundTo(count * 100 / formatTotal, 2) })),
      optimization_opportunities: [],
      io_format_conversion_pairs: topByRequests(pairs, 20).map(([key, count]) => {
        const [input_format, output_format] = JSON.parse(key) as [string, string];
        const v = pairs.get(key)!;
        return { input_format, output_format, count, avg_ratio: v.ratioCount ? roundTo(v.ratioSum / v.ratioCount, 2) : null };
      }),
    };
    if (truncated.has("value/io_pair")) meta.lower_bound.push("io.io_format_conversion_pairs");
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

/** The summary groups and columns /fastly-value reads. */
export const VALUE_SUMMARY_SPEC = defineSummarySpec({
  groups: ["overview", "series5", "all_fields", "field_totals", "dim_totals", "value"],
  columns: [
    "count", "requests", "resp_bytes_sum", "total_bandwidth_bytes", "hit_elapsed_sum", "hit_elapsed_count",
    "miss_elapsed_sum", "miss_elapsed_count", "elapsed_hist", "elapsed_sum", "elapsed_count", "io_in_sum",
    "io_out_sum", "ratio_sum", "ratio_count",
  ],
});

/**
 * Answers an unfiltered Cloud /fastly-value read from v3 summaries, or returns
 * null when the raw scan must: a filter, summaries written before the `value`
 * rows a section reads, a chart interval other than a day for a section with a
 * series, or a range the summaries don't cover (see `queryRollup`).
 */
export async function valueFromRollup(
  path: string,
  body: ValueRollupBody,
  source: Parameters<typeof queryRollup>[0],
  signal?: AbortSignal,
  /** Null reads whole files. */
  spec: SummarySpec | null = VALUE_SUMMARY_SPEC,
) {
  if (!valueRollupEligible(path, body)) return null;
  const fromMs = parseEpochMs(body.start_time ?? undefined);
  const toMs = parseEpochMs(body.end_time ?? undefined);
  if (fromMs === null || toMs === null || fromMs >= toMs) return null;
  const result = await queryRollup(source, { fromMs, toMs, signal, spec: spec ?? undefined });
  return result ? valueSummaryFromRollup(result, body) : null;
}
