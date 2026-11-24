export const DAY_MS = 86_400_000;
export const FREE_SECRETS = 10;

/**
 * Published Fastly marginal list rates (`docs/research/09-usage-and-cost-visibility.md`),
 * single-sourced across `summary.ts` and `estimate.ts`.
 */
export const LIST_RATES = {
  storage_rate_per_gb_month: 0.02,
  class_a_rate_per_1k: 0.0025,
  class_b_rate_per_10k: 0.004,
  cdn_egress_rate_per_gb: 0.12,
  min_billed_days: 30,
  compute_request_rate_per_million: 0.5,
  compute_vcpu_rate_per_million_ms: 0.05,
  compute_vcpu_free_ms_per_request: 20,
  compute_vcpu_excess_rate_per_second: 0.00005,
  compute_vcpu_rounding_ms: 10,
  cdn_request_rate_per_10k: 0.01,
  kv_class_a_rate_per_million: 6.5,
  kv_class_b_rate_per_million: 0.55,
  kv_storage_rate_per_gb_month: 0.5,
  secret_rate_per_month: 5,
} as const;

/** Fastly Full Site Delivery monthly free allowances (`fastly.com/pricing#full-site-delivery`). */
export const CDN_FREE_EGRESS_GB_PER_MONTH = 100;
export const CDN_FREE_REQUESTS_PER_MONTH = 1_000_000;

export function billableCdnEgressGb(egressGb: number): number {
  return Math.max(0, egressGb - CDN_FREE_EGRESS_GB_PER_MONTH);
}

export function billableCdnRequests(requests: number): number {
  return Math.max(0, requests - CDN_FREE_REQUESTS_PER_MONTH);
}
