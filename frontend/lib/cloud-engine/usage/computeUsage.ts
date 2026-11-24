import { dashboardCacheIdentity } from "../dashboard/sources";
import { createSharedRequests } from "../shared-request";
import { activeOpsUser } from "../telemetry/sources";
import type { ComputeUsageResponse } from "./computeSeries";
import { fetchComputeBilling, type ComputeBilling } from "./summary";

/** The compute-billing route rejects spans over 31 days. */
const MAX_SPAN_MS = 31 * 86_400_000;

const sharedBilling = createSharedRequests<ComputeBilling>();

// A /usage load reads the same range's Platform Metrics totals for the Compute charts and the
// Incurred Cost lines; reuse a settled answer briefly so the page makes one Stats fan-out.
const BILLING_TTL_MS = 15_000;
const settledBilling = new Map<string, { at: number; value: ComputeBilling }>();
let billingCacheEpoch = 0;

export function currentBillingCacheEpoch(): number {
  return billingCacheEpoch;
}

export function resetComputeBillingCache(): void {
  billingCacheEpoch += 1;
  settledBilling.clear();
}

/** Ranges ending at "now" differ by milliseconds between the page's requests. */
const billingKey = (range: { from: number; to: number }, scope: "full" | "compute") =>
  JSON.stringify([
    billingCacheEpoch,
    dashboardCacheIdentity(),
    activeOpsUser(),
    scope,
    Math.floor(range.from / 60_000),
    Math.floor(range.to / 60_000),
  ]);

type BillingRead =
  | { billing: ComputeBilling }
  | { billing: null; reason: "invalid_range" | "range_over_31_days" | "stats_unavailable" };

async function readBilling(
  range: { from: number; to: number },
  signal: AbortSignal | undefined,
  fetchBilling: typeof fetchComputeBilling,
  scope: "full" | "compute",
): Promise<BillingRead> {
  if (!(range.to > range.from)) return { billing: null, reason: "invalid_range" };
  if (range.to - range.from > MAX_SPAN_MS) {
    return { billing: null, reason: "range_over_31_days" };
  }
  const startEpoch = billingCacheEpoch;
  const key = billingKey(range, scope);
  const now = Date.now();
  for (const [k, entry] of settledBilling) {
    if (now - entry.at >= BILLING_TTL_MS) settledBilling.delete(k);
  }
  const cached = settledBilling.get(key);
  if (cached) return { billing: cached.value };
  try {
    const billing = await sharedBilling(
      key,
      (shared) => fetchBilling(range, shared),
      { signal },
    );
    if (startEpoch === billingCacheEpoch) {
      settledBilling.set(key, { at: Date.now(), value: billing });
    }
    return { billing };
  } catch {
    return { billing: null, reason: "stats_unavailable" };
  }
}

/** Fastly Platform Metrics totals for the range, or null when they can't be read for it. */
export async function usageComputeBilling(
  range: { from: number; to: number },
  signal?: AbortSignal,
): Promise<ComputeBilling | null> {
  return (await readBilling(range, signal, fetchComputeBilling, "full")).billing;
}

const iso = (ms: number | null) =>
  ms === null ? null : new Date(ms).toISOString();

export async function computeUsage(
  range: { from: number; to: number },
  signal?: AbortSignal,
  fetchBilling: typeof fetchComputeBilling = fetchComputeBilling,
): Promise<ComputeUsageResponse> {
  // The adapter's custom reader uses the shared /usage/platform app+worker payload. Keep that
  // cache separate from the full /usage/compute-billing contract used by current-storage.
  const scope = fetchBilling === fetchComputeBilling ? "full" : "compute";
  const read = await readBilling(range, signal, fetchBilling, scope);
  if (!read.billing) return { source: "unavailable", reason: read.reason };
  const { billing } = read;
  const app = billing.byRole?.app ?? { requests: 0, vcpuMs: 0 };
  const worker = billing.byRole?.worker ?? { requests: 0, vcpuMs: 0 };
  return {
    source: "fastly",
    requests: billing.requests,
    vcpu_ms: billing.executionTimeMs,
    billed_ms: billing.billedTimeMs,
    app_requests: app.requests,
    worker_requests: worker.requests,
    app_vcpu_ms: app.vcpuMs,
    worker_vcpu_ms: worker.vcpuMs,
    covered_from: iso(billing.coveredFrom),
    covered_to: iso(billing.coveredTo),
    data: (billing.computeByHour ?? []).map((hour) => ({
      time: new Date(hour.t).toISOString(),
      app_requests: hour.appRequests,
      worker_requests: hour.workerRequests,
      app_vcpu_ms: hour.appVcpuMs,
      worker_vcpu_ms: hour.workerVcpuMs,
      billed_ms: hour.billedMs,
    })),
  };
}
