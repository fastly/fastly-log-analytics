// Which /origin, /performance and /security sections Cloud summaries can answer. Kept free of
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

/**
 * The /origin aggregates sections summaries can answer for these options:
 * origin IP health and status codes always, and the latency series only for
 * OTTLB (raw TTFB coalesces OTTFB with client TTFB, which no series histogram
 * holds) on whole 5-minute buckets that tile a day without a leg split.
 */
export function originRollupSections(options: {
  metric: string;
  bucketMinutes: number;
  splitByLeg?: boolean;
}): ("ip_health" | "status_codes" | "timeseries")[] {
  const { metric, bucketMinutes, splitByLeg } = options;
  const series = metric === "ttlb" && !splitByLeg && Number.isInteger(bucketMinutes) &&
    bucketMinutes % 5 === 0 && bucketMinutes > 0 && 1440 % bucketMinutes === 0;
  return series ? ["ip_health", "status_codes", "timeseries"] : ["ip_health", "status_codes"];
}

/** The /performance sections summaries can answer exactly: the TTL histogram and the waterfall averages. */
export const PERFORMANCE_ROLLUP_SECTIONS = ["ttl_dist", "waterfall"] as const;

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
