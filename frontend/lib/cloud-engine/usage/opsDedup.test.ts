import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearOpsCaches } from "../telemetry/sources";
import { resetOpsLogsClient } from "../telemetry/client";
import { resetComputeBillingCache } from "./computeUsage";
import { fetchEstimatePrefill, resetEstimatePrefillCache } from "./estimatePrefill";
import {
  fetchCloudStorageInventory,
  fetchUsageSummary,
  resetUsageSettingsCache,
} from "./summary";

class MockWorker {
  static instances: MockWorker[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  posted: Array<Record<string, unknown>> = [];

  constructor() {
    MockWorker.instances.push(this);
  }

  postMessage(message: Record<string, unknown>) {
    this.posted.push(message);
    if ("options" in message) {
      const opts = message.options as { kind: string; from: string; to: string };
      const rows =
        opts.kind === "storage_inventory"
          ? [
              {
                snapshot_id: "snap-1",
                timestamp: Date.parse(opts.to) - 60_000,
                dataset: "request",
                key: "processed/request/p1.parquet",
                size: 1024,
                last_modified: Date.parse(opts.to) - 3_600_000,
              },
              {
                snapshot_id: "snap-1",
                timestamp: Date.parse(opts.to) - 59_000,
                complete: true,
              },
            ]
          : [];
      queueMicrotask(() => {
        this.onmessage?.({
          data: {
            id: message.id,
            type: "result",
            result: {
              kind: opts.kind,
              from: opts.from,
              to: opts.to,
              rows,
              liveTruncated: false,
            },
          },
        } as MessageEvent);
      });
    }
  }

  terminate() {}

  jobs(): Array<{ kind: string; from: string; to: string }> {
    return this.posted
      .filter((m) => "options" in m)
      .map((m) => m.options as { kind: string; from: string; to: string });
  }
}

const FIXED_NOW = Date.parse("2026-11-24T23:08:23.000Z");
const FLOOR_NOW = Date.parse("2026-11-24T23:08:00.000Z");

describe("Cloud /usage ops deduplication and memoization", () => {
  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(FIXED_NOW);
    MockWorker.instances = [];
    vi.stubGlobal("Worker", MockWorker);
    resetOpsLogsClient();
    resetUsageSettingsCache();
    resetComputeBillingCache();
    resetEstimatePrefillCache();
    await clearOpsCaches();
  });

  afterEach(async () => {
    resetOpsLogsClient();
    resetUsageSettingsCache();
    resetComputeBillingCache();
    resetEstimatePrefillCache();
    await clearOpsCaches();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("deduplicates concurrent and repeated fetchCloudStorageInventory calls within TTL", async () => {
    const [a, b] = await Promise.all([
      fetchCloudStorageInventory({ now: FLOOR_NOW }),
      fetchCloudStorageInventory({ now: FLOOR_NOW }),
    ]);
    expect(a.snapshotId).toBe("snap-1");
    expect(b.snapshotId).toBe("snap-1");
    expect(MockWorker.instances[0].jobs()).toHaveLength(1);

    // Repeated call within 60s TTL hits the memoized inventory result.
    const c = await fetchCloudStorageInventory({ now: FLOOR_NOW });
    expect(c.snapshotId).toBe("snap-1");
    expect(MockWorker.instances[0].jobs()).toHaveLength(1);

    // Clearing usage settings + ops caches forces a fresh read.
    resetUsageSettingsCache();
    await clearOpsCaches();
    await fetchCloudStorageInventory({ now: FLOOR_NOW });
    expect(MockWorker.instances[0].jobs()).toHaveLength(2);
  });

  it("memoizes fetchUsageSummary within TTL so sequential usage endpoints share one read", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/api/admin/usage-logging")) {
          return Response.json({ enabled: true, min_billed_days: 30 });
        }
        return Response.json({}, { status: 404 });
      }),
    );

    const from = FLOOR_NOW - 86_400_000;
    const to = FLOOR_NOW;
    const first = await fetchUsageSummary({ from, to });
    const jobsAfterFirst = MockWorker.instances[0].jobs().length;
    expect(jobsAfterFirst).toBe(4);

    // Sequential call for the same range reuses the memoized summary with zero new reads.
    const second = await fetchUsageSummary({ from, to });
    expect(second).toBe(first);
    expect(MockWorker.instances[0].jobs()).toHaveLength(jobsAfterFirst);
  });

  it("coalesces a full 24h /usage wave into one read per ops dataset and deduplicates compute-billing", async () => {
    const fetchCalls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        fetchCalls.push(url);
        if (url.includes("/api/admin/usage-logging")) {
          return Response.json({ enabled: true, min_billed_days: 30 });
        }
        if (url.includes("/api/admin/usage/logged-traffic")) {
          return Response.json({ logging: null, days: [] });
        }
        if (url.includes("/api/admin/usage/compute-billing")) {
          return Response.json({ services: [] });
        }
        return Response.json({});
      }),
    );

    const { readLogs } = await import("../telemetry/client");
    const dayFrom = FLOOR_NOW - 86_400_000;
    const deletionFrom = dayFrom - 30 * 86_400_000;

    // Simulate the Cloud /usage initial wave:
    // 1) prefill (7d cron_runs [T-7d, T+60s], 7d fos_usage/ingest_files after settings)
    // 2) current-storage (31d fos_usage/ingest_files after settings, 2d storage_inventory, 24h summary)
    // 3) useLastSync (1h cron_runs [now-1h, now])
    const prefill = fetchEstimatePrefill();
    const billingSettings = fetch("/api/admin/usage-logging").then((r) => r.json());
    const storageFos = billingSettings.then(() =>
      readLogs("fos_usage", {
        from: new Date(deletionFrom).toISOString(),
        to: new Date(FLOOR_NOW).toISOString(),
      }),
    );
    const storageIngest = billingSettings.then(() =>
      readLogs("ingest_files", {
        from: new Date(deletionFrom).toISOString(),
        to: new Date(FLOOR_NOW).toISOString(),
      }),
    );
    const inventory = fetchCloudStorageInventory({ now: FLOOR_NOW });
    const summary = fetchUsageSummary({ from: dayFrom, to: FLOOR_NOW });
    const lastSync = readLogs("cron_runs", {
      from: new Date(FIXED_NOW - 3_600_000).toISOString(),
      to: new Date(FIXED_NOW).toISOString(),
    });

    await Promise.all([prefill, storageFos, storageIngest, inventory, summary, lastSync]);

    const byKind = MockWorker.instances[0].jobs().reduce<Record<string, number>>((acc, job) => {
      acc[job.kind] = (acc[job.kind] ?? 0) + 1;
      return acc;
    }, {});

    // Each ops dataset is read at most once across the entire wave!
    expect(byKind).toEqual({
      cron_runs: 1,
      api_telemetry: 1,
      cron_ticks: 1,
      storage_inventory: 1,
      fos_usage: 1,
      ingest_files: 1,
    });

    // A second fetchEstimatePrefill() call within TTL makes zero additional ops or compute-billing calls.
    const billingCountBefore = fetchCalls.filter((u) =>
      u.includes("/api/admin/usage/compute-billing"),
    ).length;
    await fetchEstimatePrefill();
    const billingCountAfter = fetchCalls.filter((u) =>
      u.includes("/api/admin/usage/compute-billing"),
    ).length;
    expect(billingCountAfter).toBe(billingCountBefore);
    expect(MockWorker.instances[0].jobs()).toHaveLength(6);
  });
});
