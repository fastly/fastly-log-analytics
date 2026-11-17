// Which /origin and /performance sections Cloud summaries can answer. Kept free of
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
 * origin IP health always, and the latency series only for OTTLB (raw TTFB
 * coalesces OTTFB with client TTFB, which no series histogram holds) on whole
 * 5-minute buckets that tile a day without a leg split.
 */
export function originRollupSections(options: {
  metric: string;
  bucketMinutes: number;
  splitByLeg?: boolean;
}): ("ip_health" | "timeseries")[] {
  const { metric, bucketMinutes, splitByLeg } = options;
  const series = metric === "ttlb" && !splitByLeg && Number.isInteger(bucketMinutes) &&
    bucketMinutes % 5 === 0 && bucketMinutes > 0 && 1440 % bucketMinutes === 0;
  return series ? ["ip_health", "timeseries"] : ["ip_health"];
}

/** The /performance sections summaries can answer exactly: the TTL histogram and the waterfall averages. */
export const PERFORMANCE_ROLLUP_SECTIONS = ["ttl_dist", "waterfall"] as const;
