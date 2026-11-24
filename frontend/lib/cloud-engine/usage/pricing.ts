import { LIST_RATES } from "./rates";

export interface PricingTier {
  /** Upper cumulative bound for this tier (Infinity for the top tier). */
  readonly upTo: number;
  /** Rate per `unitSize` units in USD. */
  readonly rate: number;
}

const CDN_EGRESS_NEXT_TIER: Readonly<Record<number, number>> = {
  0.12: 0.08,
  0.19: 0.14,
  0.28: 0.24,
  0.16: 0.12,
};

/**
 * Compute progressive tiered cost. When `rateOverride` matches `defaultFirstPaidRate`,
 * full Fastly progressive tiers apply. When customized (or `0`), the free tier is still
 * respected and all billable units above it are priced at `rateOverride / unitSize`.
 */
export function priceProgressive(
  quantity: number,
  unitSize: number,
  tiers: readonly PricingTier[],
  rateOverride?: number,
  defaultFirstPaidRate?: number,
): number {
  if (!Number.isFinite(quantity) || quantity <= 0) return 0;
  const freeUpTo = tiers[0]?.rate === 0 ? tiers[0].upTo : 0;
  if (
    rateOverride !== undefined &&
    defaultFirstPaidRate !== undefined &&
    Math.abs(rateOverride - defaultFirstPaidRate) > 1e-12
  ) {
    const billable = Math.max(0, quantity - freeUpTo);
    return (billable / unitSize) * rateOverride;
  }
  let prev = 0;
  let total = 0;
  for (const tier of tiers) {
    if (quantity <= prev) break;
    const inTier = Math.min(quantity, tier.upTo) - prev;
    if (inTier > 0 && tier.rate > 0) {
      total += (inTier / unitSize) * tier.rate;
    }
    prev = tier.upTo;
  }
  return total;
}

const STORAGE_TIERS: readonly PricingTier[] = [
  { upTo: 5, rate: 0 },
  { upTo: 50_000, rate: 0.02 },
  { upTo: 500_000, rate: 0.018 },
  { upTo: Infinity, rate: 0.017 },
];

const FOS_CLASS_A_TIERS: readonly PricingTier[] = [
  { upTo: 1_000, rate: 0 },
  { upTo: Infinity, rate: 0.0025 },
];

const FOS_CLASS_B_TIERS: readonly PricingTier[] = [
  { upTo: 10_000, rate: 0 },
  { upTo: Infinity, rate: 0.004 },
];

const CDN_REQUEST_TIERS: readonly PricingTier[] = [
  { upTo: 1_000_000, rate: 0 },
  { upTo: 100_000_000, rate: 0.01 },
  { upTo: Infinity, rate: 0.0095 },
];

const COMPUTE_REQUEST_TIERS: readonly PricingTier[] = [
  { upTo: 10_000_000, rate: 0 },
  { upTo: 500_000_000, rate: 0.5 },
  { upTo: 5_000_000_000, rate: 0.49 },
  { upTo: 20_000_000_000, rate: 0.48 },
  { upTo: 50_000_000_000, rate: 0.45 },
  { upTo: 125_000_000_000, rate: 0.4 },
  { upTo: 500_000_000_000, rate: 0.3 },
  { upTo: Infinity, rate: 0.2 },
];

const COMPUTE_VCPU_TIERS: readonly PricingTier[] = [
  { upTo: 100_000_000, rate: 0 },
  { upTo: 5_000_000_000, rate: 0.05 },
  { upTo: 50_000_000_000, rate: 0.049 },
  { upTo: 200_000_000_000, rate: 0.048 },
  { upTo: 500_000_000_000, rate: 0.045 },
  { upTo: 1_000_000_000_000, rate: 0.04 },
  { upTo: 5_000_000_000_000, rate: 0.03 },
  { upTo: Infinity, rate: 0.02 },
];

const KV_CLASS_A_TIERS: readonly PricingTier[] = [
  { upTo: 100_000, rate: 0 },
  { upTo: 1_000_000, rate: 6.5 },
  { upTo: 10_000_000, rate: 6.0 },
  { upTo: Infinity, rate: 5.5 },
];

const KV_CLASS_B_TIERS: readonly PricingTier[] = [
  { upTo: 1_000_000, rate: 0 },
  { upTo: 100_000_000, rate: 0.55 },
  { upTo: 1_000_000_000, rate: 0.5 },
  { upTo: Infinity, rate: 0.45 },
];

const KV_STORAGE_TIERS: readonly PricingTier[] = [
  { upTo: 1, rate: 0 },
  { upTo: Infinity, rate: 0.5 },
];

const SECRET_TIERS: readonly PricingTier[] = [
  { upTo: 10, rate: 0 },
  { upTo: 100, rate: 5.0 },
  { upTo: 1_000, rate: 4.5 },
  { upTo: Infinity, rate: 4.0 },
];

export function priceStorageGb(gbMonths: number, ratePerGb: number = LIST_RATES.storage_rate_per_gb_month): number {
  return priceProgressive(gbMonths, 1, STORAGE_TIERS, ratePerGb, LIST_RATES.storage_rate_per_gb_month);
}

export function priceFosClassA(ops: number, ratePer1k: number = LIST_RATES.class_a_rate_per_1k): number {
  return priceProgressive(ops, 1_000, FOS_CLASS_A_TIERS, ratePer1k, LIST_RATES.class_a_rate_per_1k);
}

export function priceFosClassB(ops: number, ratePer10k: number = LIST_RATES.class_b_rate_per_10k): number {
  return priceProgressive(ops, 10_000, FOS_CLASS_B_TIERS, ratePer10k, LIST_RATES.class_b_rate_per_10k);
}

export function priceCdnEgress(gb: number, ratePerGb: number = LIST_RATES.cdn_egress_rate_per_gb): number {
  const nextRate = CDN_EGRESS_NEXT_TIER[ratePerGb];
  if (nextRate === undefined) {
    return priceProgressive(gb, 1, [{ upTo: 100, rate: 0 }, { upTo: Infinity, rate: ratePerGb }]);
  }
  return priceProgressive(gb, 1, [
    { upTo: 100, rate: 0 },
    { upTo: 10_000, rate: ratePerGb },
    { upTo: Infinity, rate: nextRate },
  ]);
}

export function priceCdnRequests(reqs: number, ratePer10k: number = LIST_RATES.cdn_request_rate_per_10k): number {
  return priceProgressive(reqs, 10_000, CDN_REQUEST_TIERS, ratePer10k, LIST_RATES.cdn_request_rate_per_10k);
}

export function priceComputeRequests(
  reqs: number,
  ratePerMillion: number = LIST_RATES.compute_request_rate_per_million,
): number {
  return priceProgressive(
    reqs,
    1_000_000,
    COMPUTE_REQUEST_TIERS,
    ratePerMillion,
    LIST_RATES.compute_request_rate_per_million,
  );
}

export function priceComputeVcpuMs(
  vcpuMs: number,
  ratePerMillionMs: number = LIST_RATES.compute_vcpu_rate_per_million_ms,
): number {
  return priceProgressive(
    vcpuMs,
    1_000_000,
    COMPUTE_VCPU_TIERS,
    ratePerMillionMs,
    LIST_RATES.compute_vcpu_rate_per_million_ms,
  );
}

export function priceKvClassA(ops: number, ratePerMillion: number = LIST_RATES.kv_class_a_rate_per_million): number {
  return priceProgressive(ops, 1_000_000, KV_CLASS_A_TIERS, ratePerMillion, LIST_RATES.kv_class_a_rate_per_million);
}

export function priceKvClassB(ops: number, ratePerMillion: number = LIST_RATES.kv_class_b_rate_per_million): number {
  return priceProgressive(ops, 1_000_000, KV_CLASS_B_TIERS, ratePerMillion, LIST_RATES.kv_class_b_rate_per_million);
}

export function priceKvStorageGb(
  gbMonths: number,
  ratePerGb: number = LIST_RATES.kv_storage_rate_per_gb_month,
): number {
  return priceProgressive(gbMonths, 1, KV_STORAGE_TIERS, ratePerGb, LIST_RATES.kv_storage_rate_per_gb_month);
}

export const KV_BINARY_GIB = 1_073_741_824;

export function kvStorageGibMonthsFromHourlyMax(hourlyMaxBytes: readonly number[], hoursInMonth = 720): number {
  if (hourlyMaxBytes.length === 0 || hoursInMonth <= 0) return 0;
  let total = 0;
  for (const bytes of hourlyMaxBytes) {
    if (Number.isFinite(bytes) && bytes > 0) {
      total += bytes / KV_BINARY_GIB / hoursInMonth;
    }
  }
  return total;
}

export function priceSecrets(secrets: number, ratePerMonth: number = LIST_RATES.secret_rate_per_month): number {
  return priceProgressive(secrets, 1, SECRET_TIERS, ratePerMonth, LIST_RATES.secret_rate_per_month);
}
