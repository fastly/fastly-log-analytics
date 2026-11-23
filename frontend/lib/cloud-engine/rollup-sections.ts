// Which /origin, /performance, /security, /assets and /fastly-value sections Cloud summaries can answer. Kept free of
// engine imports so page components can split their requests without loading them.

/** Whether a filter body narrows anything; empty filter objects and value lists don't. */
export function hasFilterValues(...groups: unknown[]): boolean {
  return groups.some((group) => {
    if (!group || typeof group !== "object") return false;
    return Object.entries(group as Record<string, unknown>).some(([key, spec]) => {
      if (key === "filter_mode" || spec == null) return false;
      if (Array.isArray(spec)) return spec.length > 0;
      if (typeof spec === "object") {
        const values = (spec as { values?: unknown }).values;
        return !Array.isArray(values) || values.length > 0;
      }
      return spec !== "";
    });
  });
}

/** An /origin aggregates section. */
export type OriginRollupSection =
  | "summary" | "timeseries" | "slow_urls" | "status_codes" | "path_breakdown" | "pop_latency" | "ip_health";

/**
 * The /origin aggregates sections summaries can answer for these options: every
 * table and the summary always, and the latency series (TTFB or OTTLB, with or
 * without a leg split) only on whole-minute buckets that tile a day (summaries
 * keep 5-minute and per-minute series).
 */
export function originRollupSections(options: {
  metric: string;
  bucketMinutes: number;
  splitByLeg?: boolean;
}): OriginRollupSection[] {
  const { metric, bucketMinutes } = options;
  const series = (metric === "ttlb" || metric === "ttfb") && Number.isInteger(bucketMinutes) &&
    bucketMinutes > 0 && 1440 % bucketMinutes === 0;
  const tables: OriginRollupSection[] = ["summary", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"];
  return series ? ["timeseries", ...tables] : tables;
}

/**
 * The /performance sections summaries can answer: the TTL histogram and the
 * waterfall averages exactly, the URL and network rankings with estimated
 * percentiles, and the scatter from the summaries' own row sample.
 */
export const PERFORMANCE_ROLLUP_SECTIONS = ["ttl_dist", "waterfall", "top_urls", "top_asns", "scatter"] as const;

/**
 * The /security sections summaries can answer: the constant NGWAF rows, TLS
 * fingerprints and their coverage, the header-size, connection-reuse and proxy
 * distributions, hourly IPv6 share, the verified-bot series, the top IPs by
 * header size, well-known and top bots, bot efficiency (referrals estimated)
 * and Proxy Watchdog (from a sample of RTT rows once a range holds more than
 * the summaries keep). Summaries older than the bot, header and proxy rows
 * leave those sections to the raw scan.
 */
export const SECURITY_ROLLUP_SECTIONS = [
  "ngwaf_verified_bots",
  "ngwaf_verified_bots_ts",
  "tls_fingerprints",
  "fingerprint_coverage",
  "req_size_dist",
  "ipv6_adoption",
  "proxy_dist",
  "conn_reuse_dist",
  "verified_bots_ts",
  "top_ips_header",
  "wellknown_bots",
  "bot_efficiency",
  "top_bots",
  "proxies",
] as const;

/** The /assets sections; `summary` covers the totals, the three breakdowns and `missing_fields`. */
export type AssetsSection = "summary" | "large_uncompressed_assets" | "low_ttl_assets";

/** The /assets sections summaries answer exactly: the totals and breakdowns from the `assets` rows. */
export const ASSETS_ROLLUP_SECTIONS = ["summary"] as const satisfies readonly AssetsSection[];

/** The /assets sections the raw scan answers on Cloud once summaries take the rest. */
export const ASSETS_RAW_SECTIONS = ["large_uncompressed_assets", "low_ttl_assets"] as const satisfies readonly AssetsSection[];

/**
 * The cache filter summaries can apply to /assets: null when unfiltered, the
 * filter when the only one is an exact-value `cache` filter (the `assets` key
 * carries the cache status), or false when the raw scan must answer.
 */
export function assetsRollupCacheFilter(filters: unknown): { mode: "include" | "exclude"; values: string[] } | null | false {
  if (!hasFilterValues(filters)) return null;
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) return false;
  const active = Object.entries(filters as Record<string, unknown>).filter(([key, spec]) =>
    key !== "filter_mode" && hasFilterValues({ [key]: spec }));
  if (active.length !== 1) return false;
  const [field, spec] = active[0];
  const { mode, values } = (spec ?? {}) as { mode?: unknown; values?: unknown };
  if (field !== "cache" || (mode !== "include" && mode !== "exclude") || !Array.isArray(values)) return false;
  if (!values.every((value) => typeof value === "string" && value !== "" && !value.includes("*"))) return false;
  return { mode, values: values as string[] };
}

/**
 * The /fastly-value sections summaries answer unfiltered: every one, with the p99 latency estimated and the
 * top-20 WAF signal, bot and format-pair counts lower bounds when a summary cut them (older summaries fall back).
 */
export const VALUE_ROLLUP_SECTIONS = ["overview", "caching", "performance", "security", "bots", "network", "io"] as const;

export const FILTERED_ROLLUP_FIELDS = [
  "status",
  "cache",
  "pop",
  "country",
  "proto",
  "method",
  "content_type",
  "edge",
] as const;

export type FilteredRollupField = (typeof FILTERED_ROLLUP_FIELDS)[number];
export type FilteredRollupFilter = { field: FilteredRollupField; value: string };

const FILTERED_ROLLUP_FIELD_SET = new Set<string>(FILTERED_ROLLUP_FIELDS);
const HOUR_MS = 3_600_000;

function parseRangeEpochMs(value: string | number | null | undefined): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? (value < 1e12 ? value * 1000 : value) : null;
  if (typeof value !== "string" || !value.trim()) return null;
  const num = Number(value);
  if (Number.isFinite(num)) return num < 1e12 ? num * 1000 : num;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Returns `{ field, value }` iff `customFilters` is empty, `filters` has one
 * active field in `FILTERED_ROLLUP_FIELDS` with `mode === "include"` and one
 * exact value, and (when `range` is provided) the time range aligns to whole
 * hour buckets.
 */
export function singleLowCardFilter(
  filters: unknown,
  customFilters?: unknown,
  range?: { start_time?: string | number | null; end_time?: string | number | null },
): FilteredRollupFilter | null {
  if (hasFilterValues(customFilters)) return null;
  if (!filters || typeof filters !== "object" || Array.isArray(filters)) return null;
  const active = Object.entries(filters as Record<string, unknown>).filter(([key, spec]) =>
    key !== "filter_mode" && hasFilterValues({ [key]: spec }));
  if (active.length !== 1) return null;
  const [field, spec] = active[0];
  if (!FILTERED_ROLLUP_FIELD_SET.has(field)) return null;
  const { mode, values } = (spec ?? {}) as { mode?: unknown; values?: unknown };
  if (mode !== "include" || !Array.isArray(values) || values.length !== 1) return null;
  const raw = values[0];
  if (typeof raw !== "string" || raw === "" || raw.includes("*")) return null;
  if (range) {
    const fromMs = parseRangeEpochMs(range.start_time);
    const toMs = parseRangeEpochMs(range.end_time);
    if (fromMs === null || toMs === null || fromMs >= toMs) return null;
    if (fromMs % HOUR_MS !== 0 || toMs % HOUR_MS !== 0) return null;
  }
  if (field === "content_type") {
    const normalized = raw.split(";")[0].trim().toLowerCase();
    return normalized ? { field, value: normalized } : null;
  }
  return { field: field as FilteredRollupField, value: raw };
}
