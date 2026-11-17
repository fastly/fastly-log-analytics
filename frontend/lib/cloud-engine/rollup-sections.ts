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
 * without a leg split) only on whole 5-minute buckets that tile a day.
 */
export function originRollupSections(options: {
  metric: string;
  bucketMinutes: number;
  splitByLeg?: boolean;
}): OriginRollupSection[] {
  const { metric, bucketMinutes } = options;
  const series = (metric === "ttlb" || metric === "ttfb") && Number.isInteger(bucketMinutes) &&
    bucketMinutes % 5 === 0 && bucketMinutes > 0 && 1440 % bucketMinutes === 0;
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
 * distributions, hourly IPv6 share and the verified-bot series.
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

/** The /fastly-value sections summaries answer exactly, unfiltered: the overview cards and the network adoption figures. */
export const VALUE_ROLLUP_SECTIONS = ["overview", "network"] as const;
