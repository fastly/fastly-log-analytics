import type { ComputeUsageResponse } from "./computeSeries";
import { fetchComputeBilling } from "./summary";

/** The compute-billing route rejects spans over 31 days. */
const MAX_SPAN_MS = 31 * 86_400_000;

const iso = (ms: number | null) =>
  ms === null ? null : new Date(ms).toISOString();

export async function computeUsage(
  range: { from: number; to: number },
  signal?: AbortSignal,
  fetchBilling: typeof fetchComputeBilling = fetchComputeBilling,
): Promise<ComputeUsageResponse> {
  if (!(range.to > range.from))
    return { source: "unavailable", reason: "invalid_range" };
  if (range.to - range.from > MAX_SPAN_MS) {
    return { source: "unavailable", reason: "range_over_31_days" };
  }
  const billing = await fetchBilling(range, signal).catch(() => null);
  if (!billing) return { source: "unavailable", reason: "stats_unavailable" };
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
