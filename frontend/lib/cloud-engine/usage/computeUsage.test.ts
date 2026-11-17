import { afterEach, describe, expect, it, vi } from "vitest";
import { computeUsage, resetComputeBillingCache } from "./computeUsage";
import { rollupComputeByDay, type ComputePoint } from "./computeSeries";
import type { ComputeBilling } from "./summary";

const HOUR = 3_600_000;
const from = Date.parse("2026-11-10T00:00:00Z");

const billing = (over: Partial<ComputeBilling> = {}): ComputeBilling => ({
  requests: 34,
  executionTimeMs: 340,
  billedTimeMs: 3_400,
  coveredFrom: from,
  coveredTo: from + 2 * HOUR,
  computeBandwidthBytes: 0,
  cdnRequests: 9_000,
  kvClassA: 0,
  kvClassB: 0,
  secrets: null,
  byRole: {
    app: { requests: 30, vcpuMs: 300 },
    worker: { requests: 4, vcpuMs: 40 },
    front: { requests: 9_000, vcpuMs: 0 },
  },
  computeByHour: [
    {
      t: from,
      appRequests: 10,
      workerRequests: 0,
      appVcpuMs: 100,
      workerVcpuMs: 0,
      billedMs: 1_000,
    },
    {
      t: from + HOUR,
      appRequests: 20,
      workerRequests: 4,
      appVcpuMs: 200,
      workerVcpuMs: 40,
      billedMs: 2_400,
    },
  ],
  ...over,
});

describe("computeUsage", () => {
  afterEach(() => resetComputeBillingCache());

  it("shares one Stats read between the page's requests for the same range", async () => {
    const fetchBilling = vi.fn(async () => billing());
    const to = from + 3 * HOUR;

    await Promise.all([
      computeUsage({ from, to }, undefined, fetchBilling),
      computeUsage({ from, to }, undefined, fetchBilling),
    ]);
    // An open-ended range ends a few ms later on the next request.
    await computeUsage({ from: from + 5, to: to + 5 }, undefined, fetchBilling);
    expect(fetchBilling).toHaveBeenCalledTimes(1);

    await computeUsage({ from, to: to + HOUR }, undefined, fetchBilling);
    expect(fetchBilling).toHaveBeenCalledTimes(2);
  });

  it("reports Fastly's app and worker totals with one hourly point per covered hour", async () => {
    const fetchBilling = vi.fn(async () => billing());
    const to = from + 3 * HOUR;

    const result = await computeUsage({ from, to }, undefined, fetchBilling);

    expect(fetchBilling).toHaveBeenCalledWith({ from, to }, expect.any(AbortSignal));
    expect(result).toEqual({
      source: "fastly",
      requests: 34,
      vcpu_ms: 340,
      billed_ms: 3_400,
      app_requests: 30,
      worker_requests: 4,
      app_vcpu_ms: 300,
      worker_vcpu_ms: 40,
      covered_from: "2026-11-10T00:00:00.000Z",
      covered_to: "2026-11-10T02:00:00.000Z",
      data: [
        {
          time: "2026-11-10T00:00:00.000Z",
          app_requests: 10,
          worker_requests: 0,
          app_vcpu_ms: 100,
          worker_vcpu_ms: 0,
          billed_ms: 1_000,
        },
        {
          time: "2026-11-10T01:00:00.000Z",
          app_requests: 20,
          worker_requests: 4,
          app_vcpu_ms: 200,
          worker_vcpu_ms: 40,
          billed_ms: 2_400,
        },
      ],
    });
  });

  it("does not read the Stats API for a range longer than its 31-day limit", async () => {
    const fetchBilling = vi.fn(async () => billing());

    const result = await computeUsage(
      { from, to: from + 32 * 24 * HOUR },
      undefined,
      fetchBilling,
    );

    expect(result).toEqual({
      source: "unavailable",
      reason: "range_over_31_days",
    });
    expect(fetchBilling).not.toHaveBeenCalled();
  });

  it("reports the Stats API as unavailable instead of failing when the read fails", async () => {
    const fetchBilling = vi.fn(async () => {
      throw new Error("compute_billing_unavailable");
    });

    expect(
      await computeUsage({ from, to: from + HOUR }, undefined, fetchBilling),
    ).toEqual({
      source: "unavailable",
      reason: "stats_unavailable",
    });
  });
});

describe("rollupComputeByDay", () => {
  const point = (time: string, n: number): ComputePoint => ({
    time,
    app_requests: n,
    worker_requests: 2 * n,
    app_vcpu_ms: 10 * n,
    worker_vcpu_ms: 20 * n,
    billed_ms: 100 * n,
  });

  it("sums hours into UTC days and adds no day the Stats API did not cover", () => {
    expect(
      rollupComputeByDay([
        point("2026-11-10T00:00:00.000Z", 1),
        point("2026-11-10T23:00:00.000Z", 2),
        point("2026-11-12T05:00:00.000Z", 4),
      ]),
    ).toEqual([
      { ...point("2026-11-10T00:00:00.000Z", 3) },
      { ...point("2026-11-12T00:00:00.000Z", 4) },
    ]);
  });
});
