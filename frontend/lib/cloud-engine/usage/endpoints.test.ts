import { describe, expect, it } from "vitest";
import { DEFAULT_COMPACTION, DEFAULT_INPUTS, DEFAULT_UNITS } from "./estimate";
import { fixtureUsageInput, FIXTURE_NOW } from "./fixtures";
import { summarizeUsage } from "./summary";
import { calculateStorageBilling } from "./storageBilling";
import { buildCloudUsageEndpoints } from "./endpoints";

const DAY = 86_400_000;

describe("buildCloudUsageEndpoints", () => {
  const fromMs = FIXTURE_NOW - 2 * DAY;
  const toMs = FIXTURE_NOW;

  it("builds complete snake_case OpenAPI payloads without leaking internal compute requests as CDN traffic", () => {
    const summary = summarizeUsage(fixtureUsageInput);
    const intervals = [
      {
        id: "raw_request:r1.zst",
        dataset: "raw_request",
        kind: "deleted" as const,
        bytes: 1_000,
        createdAt: fromMs + 1_000,
        deletedAt: fromMs + 2_000,
        billedFrom: fromMs + 1_000,
        billedUntil: toMs,
        complete: true,
        provenance: "cloud:ingest_files",
      },
      {
        id: "pq_request:p1.parquet",
        dataset: "pq_request",
        kind: "live" as const,
        bytes: 4_000,
        createdAt: fromMs + 1_000,
        deletedAt: null,
        billedFrom: fromMs + 1_000,
        billedUntil: toMs,
        complete: true,
        provenance: "cloud:inventory",
      },
    ];
    const storageSummary = calculateStorageBilling(intervals, fromMs, toMs);
    const storageBilling = {
      intervals,
      summary: storageSummary,
      observedAt: toMs,
      deletedBytes: 1_000,
      deletedBytesComplete: true,
      deletedBytesProvenance: "ingest_files_exact",
      impact: {
        requestLogBytes: 1_000,
        rumLogBytes: 250,
        rawLogsDeleted: true,
        processedRequestBytes: 4_000,
        processedRumBytes: 500,
        processedObjects: 3,
        compactionDeletedBytes: 200,
        retentionDeletedBytes: 100,
      },
    };
    const kvHour = Date.UTC(2026, 9, 29, 0);
    const endpoints = buildCloudUsageEndpoints({
      fromMs,
      toMs,
      by: "day",
      input: fixtureUsageInput,
      summary,
      storageBilling,
      billing: {
        requests: 0,
        executionTimeMs: 0,
        billedTimeMs: 0,
        coveredFrom: null,
        coveredTo: null,
        computeBandwidthBytes: 0,
        cdnRequests: null,
        kvClassA: 15,
        kvClassB: 40,
        kvByHour: [{ t: kvHour, classA: 15, classB: 40 }],
        secrets: 1,
      },
      prefillEstimate: {
        inputs: {
          ...DEFAULT_INPUTS,
          requestsPerDay: 10_000,
          edgeRequestsPerDay: 8_000,
          samplePct: 100,
          logPeriodS: 60,
          edgeOnly: true,
          deleteAfter: true,
          retentionDays: 30,
          rumLogPeriodS: 60,
          rumSamplePct: 100,
          rumRetentionDays: 30,
          rumPageviewsPerDay: 500,
          filesPerPeriod: 2,
          gzBytesPerLine: 200,
          rawBytesPerGzByte: 6,
          parquetBytesPerRow: 120,
        },
        units: DEFAULT_UNITS,
        measuredInputs: { requestsPerDay: true },
        measuredUnits: {},
        window: { days: 7, files: 10, lines: 1_000 },
        compaction: DEFAULT_COMPACTION,
        serviceCompaction: { mode: "off", size_target_mb: 256 },
        rumEnabled: true,
        relay: 0,
        rates: fixtureUsageInput.settings,
        usageLogging: true,
        fosRegion: "us-east-1",
      },
      estimatedBytesPerLine: 1200,
    });

    const storage = endpoints.currentStorage();
    expect(storage.quarantine_bytes).toBe(0);
    expect(storage.live_files).toBeGreaterThanOrEqual(0);
    expect(storage.total_billed_bytes).toBe(storage.live_bytes + storage.deleted_bytes);
    expect(storage.archive_bytes).toBe(4_500);
    expect(storage.archive_objects).toBe(3);
    expect(storage.total_files).toBe(storage.live_files);
    expect(storage.total_bytes).toBe(storage.live_bytes);
    expect(storage.start).toBe(new Date(fromMs).toISOString());
    expect(storage.end).toBe(new Date(toMs).toISOString());
    expect(storage.storage_intervals?.[0]).toMatchObject({
      id: "raw_request:r1.zst",
      created_at: fromMs + 1_000,
      deleted_at: fromMs + 2_000,
      billed_from: fromMs + 1_000,
      billed_until: toMs,
    });

    const compactStorage = buildCloudUsageEndpoints({
      fromMs,
      toMs,
      by: "day",
      input: fixtureUsageInput,
      summary,
      omitIntervals: true,
    }).currentStorage();
    expect("storage_intervals" in compactStorage).toBe(false);

    const bw = endpoints.bandwidth();
    expect(bw).toMatchObject({
      granularity: "day",
      total_log_bytes: summary.bytesRead,
    });
    expect(bw.data[0]).toHaveProperty("requests");

    const activity = endpoints.logActivity();
    expect(activity).toMatchObject({
      granularity: "day",
      total_api_requests: null,
      total_fastly_log_records: null,
    });
    expect(activity.data.every((p) => p.api_requests === null)).toBe(true);

    const ops = endpoints.operations();
    expect(ops.data.some((p) => p.kv_class_a === 15 && p.kv_class_b === 40)).toBe(true);

    const prefill = endpoints.prefill();
    expect(prefill).toMatchObject({
      requests_per_day: 10_000,
      edge_requests_per_day: 8_000,
      sample_rate: 100,
      edge_only: true,
      estimated_bytes_per_line: 1200,
      billing_zone: "MBZ100",
      fos_region: "us-east-1",
    });
    expect(prefill.cloud_measured).toBeDefined();

    const rates = endpoints.prefillRates();
    expect(rates).toMatchObject({
      class_a_rate_per_1k: fixtureUsageInput.settings.class_a_rate_per_1k,
      billing_zone: "MBZ100",
    });

    const defaultZoneRates = buildCloudUsageEndpoints({
      fromMs,
      toMs,
      by: "day",
      input: fixtureUsageInput,
      summary,
    }).prefillRates();
    expect(defaultZoneRates.billing_zone).toBe("MBZ200");
  });
});
