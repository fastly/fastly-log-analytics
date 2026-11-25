import { afterEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { FIXTURE_NOW, fixtureUsageInput } from "./fixtures";
import {
  LIST_RATES,
  type CronRunRow,
  deriveFosUsageFromCronRuns,
  deletedBytesAccountingSince,
  billedVcpuForExecutions,
  billedVcpuMsPerRequest,
  fetchCloudStorageInventory,
  fetchUsageInput,
  fetchUsageKindWithCoverage,
  fetchUsageSettings,
  fetchIngestDeletedBytes,
  fetchIngestDeletedIntervals,
  resetUsageSettingsCache,
  type UsageLogReader,
  incurredCost,
  parseComputeBilling,
  perTenThousandToPerMillion,
  perThousandToPerMillion,
  requestStorageReconcile,
  storageCostUsd,
  summarizeUsage,
  telemetryCostUsd,
} from "./summary";

// Settled settings are reused for a short window; keep each test on its own fetch.
afterEach(() => resetUsageSettingsCache());

describe("rate conversion", () => {
  it("converts per-1k and per-10k rates to per-million exactly", () => {
    expect(perThousandToPerMillion(0.005)).toBe(5);
    expect(perTenThousandToPerMillion(0.01)).toBe(1);
    expect(perThousandToPerMillion(0.0035)).toBe(3.5);
    expect(perTenThousandToPerMillion(0.07)).toBe(7);
  });
});

describe("storageCostUsd", () => {
  it("bills at least min_billed_days of retention", () => {
    expect(storageCostUsd(1, 0.02, 7, 30)).toBeCloseTo(0.02);
    expect(storageCostUsd(1, 0.02, 365, 30)).toBeCloseTo(0.02 * (365 / 30));
    expect(storageCostUsd(2, 0.02, 1, 30)).toBeCloseTo(0.04);
  });

  describe("deletedBytesAccountingSince", () => {
    it("treats an empty read as incomplete without positive coverage evidence", () => {
      expect(deletedBytesAccountingSince([], 0)).toEqual({
        bytes: 0,
        complete: false,
        provenance: "fos_usage_incomplete",
      });
    });

    it("does not invent a deletion history from an empty FOS telemetry read", () => {
      expect(deletedBytesAccountingSince([], 0, true)).toEqual({
        bytes: 0,
        complete: false,
        provenance: "fos_usage_incomplete",
      });
    });
  });
});

describe("fetchIngestDeletedBytes", () => {
  it("recovers historical raw deletions from persisted ingest_files state", async () => {
    const read = vi.fn(async () => ({
      rows: [
        {
          raw_key: "raw/service/a.log.zst",
          raw_size: 4_000,
          raw_deleted: false,
          timestamp: 100,
        },
        {
          raw_key: "raw/service/a.log.zst",
          raw_size: null,
          raw_deleted: true,
          raw_deleted_at: "1970-01-01T00:00:00.500Z",
          timestamp: 500,
        },
        {
          raw_key: "raw/service/b.log.zst",
          raw_size: 2_000,
          raw_deleted: true,
          raw_deleted_at: "1970-01-01T00:00:00.600Z",
          timestamp: 600,
        },
      ],
      liveTruncated: false,
    }));
    await expect(
      fetchIngestDeletedBytes(
        { from: 250, to: 700 },
        { user: "admin", role: "admin" },
        read,
      ),
    ).resolves.toEqual({
      bytes: 6_000,
      complete: true,
      provenance: "ingest_files_exact",
    });
  });
});

describe("fetchCloudStorageInventory", () => {
  const identity = { user: "admin", role: "admin" };

  it("selects the newest complete snapshot_id and returns exact live intervals", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const olderModified = "2026-11-20T00:00:00.000Z";
    const newerModified = "2026-11-21T06:00:00.000Z";
    const read = vi.fn(async () => ({
      rows: [
        {
          snapshot_id: "snap-1",
          dataset: "pq_request",
          key: "processed/request/old.parquet",
          size: 1000,
          last_modified: olderModified,
          complete: false,
          timestamp: Date.parse("2026-11-21T00:00:00.000Z"),
        },
        {
          snapshot_id: "snap-1",
          dataset: null,
          key: null,
          size: 0,
          last_modified: null,
          complete: true,
          timestamp: Date.parse("2026-11-21T00:01:00.000Z"),
        },
        {
          snapshot_id: "snap-2",
          dataset: "pq_request",
          key: "processed/request/new.parquet",
          size: 2048,
          last_modified: newerModified,
          complete: false,
          timestamp: Date.parse("2026-11-22T10:00:00.000Z"),
        },
        {
          snapshot_id: "snap-2",
          dataset: "raw_request",
          key: "raw/request/a.zst",
          size: 512,
          last_modified: newerModified,
          complete: false,
          timestamp: Date.parse("2026-11-22T10:00:01.000Z"),
        },
        {
          snapshot_id: "snap-2",
          dataset: null,
          key: null,
          size: 0,
          last_modified: null,
          complete: true,
          timestamp: Date.parse("2026-11-22T10:00:05.000Z"),
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchCloudStorageInventory({
      now,
      identity,
      read,
    });

    expect(read).toHaveBeenCalledWith(
      "storage_inventory",
      expect.objectContaining({ to: new Date(now).toISOString() }),
      expect.objectContaining(identity),
    );
    expect(result.complete).toBe(true);
    expect(result.snapshotId).toBe("snap-2");
    expect(result.observedAt).toBe(Date.parse("2026-11-22T10:00:05.000Z"));
    expect(result.provenance).toBe("cloud:inventory");
    expect(result.intervals).toEqual([
      {
        id: "pq_request:processed/request/new.parquet",
        dataset: "pq_request",
        kind: "live",
        bytes: 2048,
        createdAt: Date.parse(newerModified),
        deletedAt: null,
        billedFrom: Date.parse(newerModified),
        billedUntil: now,
        complete: true,
        provenance: "cloud:inventory",
      },
      {
        id: "raw_request:raw/request/a.zst",
        dataset: "raw_request",
        kind: "live",
        bytes: 512,
        createdAt: Date.parse(newerModified),
        deletedAt: null,
        billedFrom: Date.parse(newerModified),
        billedUntil: now,
        complete: true,
        provenance: "cloud:inventory",
      },
    ]);
  });

  const snapshotRows = (completedAt: string, inProgressAt?: string) => [
    {
      snapshot_id: "snap-1",
      dataset: "pq_request",
      key: "processed/request/a.parquet",
      size: 1024,
      last_modified: "2026-11-21T06:00:00.000Z",
      complete: false,
      timestamp: Date.parse(completedAt) - 5_000,
    },
    {
      snapshot_id: "snap-1",
      dataset: null,
      key: null,
      size: 0,
      last_modified: null,
      complete: true,
      timestamp: Date.parse(completedAt),
    },
    ...(inProgressAt
      ? [
          {
            snapshot_id: "snap-2-in-progress",
            dataset: "pq_request",
            key: "processed/request/b.parquet",
            size: 4096,
            last_modified: "2026-11-21T06:00:00.000Z",
            complete: false,
            timestamp: Date.parse(inProgressAt),
          },
        ]
      : []),
  ];

  it("keeps the last complete snapshot authoritative while the next one is still listing", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const read = vi.fn(async () => ({
      rows: snapshotRows("2026-11-22T11:10:00.000Z", "2026-11-22T11:59:00.000Z"),
      liveTruncated: false,
    }));

    const result = await fetchCloudStorageInventory({ now, identity, read });

    expect(result.snapshotId).toBe("snap-1");
    expect(result.complete).toBe(true);
    expect(result.provenance).toBe("cloud:inventory");
    expect(result.intervals.map((i) => i.id)).toEqual(["pq_request:processed/request/a.parquet"]);
  });

  it("flags a complete snapshot older than two 6-hour snapshot intervals as stale", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const read = vi.fn(async () => ({
      rows: snapshotRows("2026-11-21T07:00:05.000Z"),
      liveTruncated: false,
    }));

    const result = await fetchCloudStorageInventory({ now, identity, read });

    expect(result.snapshotId).toBe("snap-1");
    expect(result.complete).toBe(false);
    expect(result.provenance).toBe("cloud:inventory_stale");
    expect(result.intervals).toHaveLength(1);
  });

  it("returns empty incomplete result when no snapshot has a terminal marker", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const read = vi.fn(async () => ({
      rows: [
        {
          snapshot_id: "snap-partial",
          dataset: "pq_request",
          key: "processed/request/a.parquet",
          size: 1024,
          last_modified: "2026-11-22T11:00:00.000Z",
          complete: false,
          timestamp: Date.parse("2026-11-22T11:30:00.000Z"),
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchCloudStorageInventory({ now, identity, read });

    expect(result).toEqual({
      intervals: [],
      complete: false,
      snapshotId: null,
      observedAt: null,
      provenance: "cloud:inventory_incomplete",
    });
  });

  it("probes a 2-day lookback first and avoids a 30-day scan when a complete snapshot is found", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const calls: Array<{ from: number; to: number }> = [];
    const read = vi.fn(async (_kind: unknown, range: { from: string; to: string }) => {
      calls.push({ from: Date.parse(range.from), to: Date.parse(range.to) });
      return {
        rows: snapshotRows("2026-11-22T11:10:00.000Z"),
        liveTruncated: false,
      };
    });

    const result = await fetchCloudStorageInventory({ now, identity, read });

    expect(result.complete).toBe(true);
    expect(result.snapshotId).toBe("snap-1");
    expect(calls).toHaveLength(1);
    expect(calls[0].to - calls[0].from).toBe(2 * 86_400_000);
  });

  it("falls back to the 30-day lookback when no complete snapshot exists in the last 2 days", async () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const calls: Array<{ from: number; to: number }> = [];
    const read = vi.fn(async (_kind: unknown, range: { from: string; to: string }) => {
      calls.push({ from: Date.parse(range.from), to: Date.parse(range.to) });
      if (calls.length === 1) {
        return { rows: [], liveTruncated: false };
      }
      return {
        rows: snapshotRows("2026-11-10T11:10:00.000Z"),
        liveTruncated: false,
      };
    });

    const result = await fetchCloudStorageInventory({ now, identity, read });

    expect(calls).toHaveLength(2);
    expect(calls[0].to - calls[0].from).toBe(2 * 86_400_000);
    expect(calls[1].to - calls[1].from).toBe(30 * 86_400_000);
    expect(result.snapshotId).toBe("snap-1");
    expect(result.complete).toBe(false);
    expect(result.provenance).toBe("cloud:inventory_stale");
  });
});

describe("fetchIngestDeletedIntervals", () => {
  const identity = { user: "admin", role: "admin" };

  it("collects the Parquet files each converted raw file produced, once per key", async () => {
    const outputs = (keys: [string, number][]) =>
      JSON.stringify(keys.map(([key, bytes]) => ({ key, rows: 1, bytes })));
    const read = vi.fn(async () => ({
      rows: [
        { timestamp: 10, raw_key: "a", raw_size: 1, state_version: 1, status: "converted",
          output_keys_json: outputs([["processed/request/w1.parquet", 300]]) },
        { timestamp: 20, raw_key: "b", raw_size: 1, state_version: 1, status: "converted",
          output_keys_json: outputs([["processed/request/w1.parquet", 300], ["processed/rum/w2.parquet", 40]]) },
        { timestamp: 30, raw_key: "c", raw_size: 1, state_version: 1, status: "failed", output_keys_json: "[]" },
      ],
      liveTruncated: false,
    }));
    const result = await fetchIngestDeletedIntervals({ from: 0, to: 100 }, identity, read);
    expect(result.outputs).toEqual([
      { key: "processed/request/w1.parquet", bytes: 300, createdAt: 10 },
      { key: "processed/rum/w2.parquet", bytes: 40, createdAt: 20 },
    ]);
  });

  it("builds an exact billed interval from the first and deleted ingest_file states", async () => {
    const read = vi.fn(async () => ({
      rows: [
        { timestamp: 10, raw_key: "a", raw_size: 100, state_version: 1, raw_deleted: false },
        {
          timestamp: 20,
          raw_key: "a",
          raw_size: 100,
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: 20,
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchIngestDeletedIntervals(
      { from: 0, to: 100 },
      identity,
      read,
      { minimumBilledDays: 30 },
    );

    expect(result.complete).toBe(true);
    expect(result.bytes).toBe(100);
    expect(result.provenance).toBe("cloud:ingest_files");
    expect(result.intervals).toEqual([
      expect.objectContaining({
        id: "raw_request:a",
        dataset: "raw_request",
        kind: "minimum_billed",
        bytes: 100,
        createdAt: 10,
        deletedAt: 20,
        billedFrom: 10,
        billedUntil: 30 * 86_400_000 + 10,
        complete: true,
        provenance: "cloud:ingest_files",
      }),
    ]);
  });

  it("stops at deleted_at when deleted after the minimum billing term", async () => {
    const day = 86_400_000;
    const read = vi.fn(async () => ({
      rows: [
        { timestamp: 0, raw_key: "old", raw_size: 500, state_version: 1, raw_deleted: false },
        {
          timestamp: 40 * day,
          raw_key: "old",
          raw_size: 500,
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: 40 * day,
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchIngestDeletedIntervals(
      { from: 0, to: 45 * day },
      identity,
      read,
      { minimumBilledDays: 30 },
    );

    expect(result.intervals).toEqual([
      expect.objectContaining({
        id: "raw_request:old",
        kind: "deleted",
        billedFrom: 0,
        billedUntil: 40 * day,
        complete: true,
      }),
    ]);
  });

  it("marks missing size or creation evidence incomplete and deduplicates by raw key", async () => {
    const read = vi.fn(async () => ({
      rows: [
        {
          timestamp: 500,
          raw_key: "no-creation",
          raw_size: 200,
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: 500,
        },
        {
          timestamp: 500,
          raw_key: "no-creation",
          raw_size: 200,
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: 500,
        },
        {
          timestamp: 100,
          raw_key: "no-size",
          raw_size: null,
          state_version: 1,
          raw_deleted: false,
        },
        {
          timestamp: 200,
          raw_key: "no-size",
          raw_size: null,
          state_version: 2,
          raw_deleted: true,
          raw_deleted_at: 200,
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchIngestDeletedIntervals(
      { from: 0, to: 1000 },
      identity,
      read,
      { minimumBilledDays: 30 },
    );

    expect(result.complete).toBe(false);
    expect(result.provenance).toBe("cloud:ingest_files_incomplete");
    expect(result.intervals).toHaveLength(1);
    expect(result.intervals[0]).toMatchObject({
      id: "raw_request:no-creation",
      bytes: 200,
      complete: false,
    });
  });

  it("collects compacted outputs and deleted Parquet keys without treating them as raw_request deletions", async () => {
    const read = vi.fn(async () => ({
      rows: [
        {
          timestamp: 500,
          dataset: "request",
          status: "compacted",
          output_keys_json: JSON.stringify([
            { key: "processed/request/compact=1.parquet", bytes: 1200 },
          ]),
        },
        {
          timestamp: 600,
          dataset: "request",
          status: "parquet_deleted",
          output_keys_json: JSON.stringify([
            "processed/request/window=a.parquet",
            "processed/request/window=b.parquet",
          ]),
        },
      ],
      liveTruncated: false,
    }));

    const result = await fetchIngestDeletedIntervals(
      { from: 0, to: 1000 },
      identity,
      read,
      { minimumBilledDays: 30 },
    );

    expect(result.complete).toBe(true);
    expect(result.intervals).toHaveLength(0);
    expect(result.outputs).toEqual([
      {
        key: "processed/request/compact=1.parquet",
        bytes: 1200,
        createdAt: 500,
      },
    ]);
    expect([...(result.deletedParquetKeys ?? [])]).toEqual([
      "processed/request/window=a.parquet",
      "processed/request/window=b.parquet",
    ]);
  });
});

describe("summarizeUsage", () => {
  const summary = summarizeUsage(fixtureUsageInput);

  it("counts LIST/DELETE_OBJECTS/PUT as Class A, GET as B, hits separately", () => {
    expect(summary.totals.classA).toBe(72 + 10 + 7 + 100 + 1);
    expect(summary.totals.classB).toBe(30);
    expect(summary.totals.cdnHit).toBe(90);
    expect(summary.totals.usd).toBeCloseTo(
      0.00036 + 0.00005 + 0.000035 + 0.0005 + 0.00003,
    );
  });

  it("groups by UTC day and by caller", () => {
    expect(summary.byDay.map((d) => d.day)).toEqual([
      "2026-10-28",
      "2026-10-29",
    ]);
    expect(summary.byCaller.map((c) => c.caller)).toContain("cron:log_compact");
    expect(
      summary.byCaller.find((c) => c.caller === "cron:retention")?.classA,
    ).toBe(7);
  });

  it("computes the CDN hit ratio on API reads", () => {
    expect(summary.cdnHitRatio).toBeCloseTo(90 / 120);
  });

  it("aggregates compute by route and cron kind, excluding browser rows", () => {
    expect(summary.computeByRoute).toEqual([
      { key: "/api/files", requests: 2, vcpuMs: 40 },
      { key: "/api/file", requests: 1, vcpuMs: 5 },
    ]);
    expect(summary.computeByCron[0]).toEqual({
      key: "request_ingest",
      requests: 1,
      vcpuMs: 200,
    });
    // 3 API rows + 720 legacy ticks + 4 cron executions (285 ms).
    expect(summary.computeRequests).toBe(3 + 720 + 4);
    expect(summary.computeVcpuMs).toBe(45 + 7_200 + 285);
    expect(summary.kvOps).toBe(40_000);
  });

  it("counts a folded file-read row as the requests it stands for", () => {
    const folded = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        api_telemetry: [
          ...fixtureUsageInput.rows.api_telemetry,
          {
            timestamp: fixtureUsageInput.rows.api_telemetry[0].timestamp,
            source: "compute_api",
            path: "/api/file",
            vcpu_ms: 35,
            files_fetched: 7,
          },
        ],
      },
    });
    expect(folded.computeByRoute.find((r) => r.key === "/api/file")).toEqual({
      key: "/api/file",
      requests: 8,
      vcpuMs: 40,
    });
    expect(folded.computeRequests).toBe(3 + 7 + 720 + 4);
    expect(folded.computeVcpuMs).toBe(45 + 35 + 7_200 + 285);
  });

  it("counts idle clock ticks carried on cron_runs rows as compute requests", () => {
    const withIdle = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        cron_runs: [
          ...fixtureUsageInput.rows.cron_runs,
          {
            ...fixtureUsageInput.rows.cron_runs[2],
            idle_ticks: 119,
            idle_vcpu_ms: 238,
          },
        ],
        cron_ticks: [],
      },
    });
    expect(withIdle.computeRequests).toBe(3 + 119 + 5);
    expect(withIdle.computeVcpuMs).toBe(45 + 238 + 485);
  });

  it("counts each clock execution once and every worker execution it dispatched", () => {
    const run = fixtureUsageInput.rows.cron_runs[2];
    const counted = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        cron_runs: [
          {
            ...run,
            run_id: "cron-1-a",
            trigger: "clock",
            worker_executions: 3,
          },
          { ...run, run_id: "cron-1-a", trigger: "clock" },
          { ...run, run_id: "flush-2-b", trigger: "clock", idle_ticks: 4 },
          {
            ...run,
            run_id: "cron-3-c",
            trigger: "manual",
            worker_executions: 1,
          },
        ],
        cron_ticks: [],
      },
    });
    // 3 API rows (incl. the manual trigger), 2 clock executions, 4 idle, 4 worker.
    expect(counted.computeRequests).toBe(3 + 2 + 4 + 4);
  });

  it("counts worker vCPU carried in a run's details beside the admin execution's own", () => {
    const run = fixtureUsageInput.rows.cron_runs[2];
    const withWorkers = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        cron_runs: [
          {
            ...run,
            cron_kind: "request_compact",
            vcpu_ms: 80,
            worker_executions: 4,
            details_json: JSON.stringify({ passes: 4, worker_vcpu_ms: 400 }),
          },
        ],
        cron_ticks: [],
      },
    });
    expect(withWorkers.computeByCron).toEqual([
      { key: "request_compact", requests: 1, vcpuMs: 480 },
    ]);
  });

  const stageRow = (over: Partial<CronRunRow>): CronRunRow => ({
    ...fixtureUsageInput.rows.cron_runs[2],
    details_json: "{}",
    idle_ticks: 0,
    idle_vcpu_ms: 0,
    worker_executions: 0,
    ...over,
  });
  const sums = (s: ReturnType<typeof summarizeUsage>) => ({
    requests: s.computeSegments!.reduce((sum, seg) => sum + seg.requests, 0),
    vcpuMs: s.computeSegments!.reduce((sum, seg) => sum + seg.vcpuMs, 0),
  });

  it("counts a clock run's stage, worker and compaction-pass vCPU once per execution", () => {
    // One clock tick (two tick stages, a maintenance stage and a compaction job on the worker
    // service, 3 worker executions incl. both, 2 compaction passes) and one /_flush execution.
    const run = (kind: string, over: Partial<CronRunRow> = {}) =>
      stageRow({ cron_kind: kind, run_id: `cron-9-ab-${kind}`, trigger: "clock", ...over });
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        api_telemetry: [],
        cron_ticks: [],
        cron_runs: [
          run("request_ingest", {
            vcpu_ms: 120,
            worker_executions: 3,
            idle_ticks: 10,
            idle_vcpu_ms: 20,
            details_json: JSON.stringify({ worker_vcpu_ms: 300, summary_vcpu_ms: 100 }),
          }),
          run("raw_delete", { vcpu_ms: 30 }),
          run("retention_sweep", { vcpu_ms: 60 }),
          run("request_compact", {
            vcpu_ms: 40,
            details_json: JSON.stringify({ passes: 2, worker_vcpu_ms: 90 }),
          }),
          stageRow({ cron_kind: "log_flush", run_id: "flush-9-cd-log_flush", trigger: "clock", vcpu_ms: 25 }),
        ],
      },
    });
    // Tick 1 + flush 1 + worker 3 + passes 2 + idle 10.
    expect(summary.computeRequests).toBe(17);
    // Tick 120 + 30, flush 25, worker 60 + 40 + 300 + 90, idle 20.
    expect(summary.computeVcpuMs).toBe(685);
    expect(sums(summary)).toEqual({ requests: 17, vcpuMs: 685 });
  });

  it("bills a manual run's own stages once, inside its API trigger request, plus its workers", () => {
    const run = (kind: string, over: Partial<CronRunRow> = {}) =>
      stageRow({ cron_kind: kind, run_id: `cron-7-ef-${kind}`, trigger: "manual", ...over });
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      settings: { ...fixtureUsageInput.settings, ...LIST_RATES },
      rows: {
        ...fixtureUsageInput.rows,
        // The trigger request's vCPU covers every stage it ran in its own execution.
        api_telemetry: [
          { timestamp: FIXTURE_NOW, source: "compute_api", path: "/api/cron-runs/trigger", vcpu_ms: 200 },
        ],
        cron_ticks: [],
        cron_runs: [
          run("request_ingest", {
            vcpu_ms: 150,
            worker_executions: 2,
            details_json: JSON.stringify({ worker_vcpu_ms: 80 }),
          }),
          run("retention_sweep", { vcpu_ms: 30 }),
        ],
      },
    });
    expect(summary.computeRequests).toBe(1 + 2);
    expect(summary.computeVcpuMs).toBe(200 + 80 + 30);
    expect(sums(summary)).toEqual({ requests: 3, vcpuMs: 310 });
    // Trigger: 200 ms → 180 billed. Workers: 110 ms / 2 = 55 → 35 excess → 40 each.
    expect(summary.computeBilledVcpuMs).toBe(180 + 2 * 40);
  });

  it("estimates storage as snapshot + PUT bytes - deleted bytes after the snapshot", () => {
    expect(summary.storage.snapshotAt).toBe(
      fixtureUsageInput.now - 3 * 86_400_000,
    );
    const ops = summary.storage.datasets.find((d) => d.dataset === "pq_ops");
    expect(ops).toMatchObject({ bytes: 10_000 + 7_200 - 2_000 });
    const request = summary.storage.datasets.find(
      (d) => d.dataset === "pq_request",
    );
    expect(request?.bytes).toBe(1_000_000 + 5_000_000 + 999);
  });

  it("ignores in-progress partial storage_reconcile rows with empty details_json", () => {
    const withPartial = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        cron_runs: [
          {
            started_at: fixtureUsageInput.now - 60_000,
            cron_kind: "storage_reconcile",
            outcome: "partial",
            vcpu_ms: 2,
            cache_lookups: 0,
            backend_requests_used: 3,
            details_json: "{}",
          },
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            details_json: JSON.stringify({
              datasets: {
                pq_request: { bytes: 1_000_000 },
                pq_rum: { bytes: 250_000 },
                pq_ops: { bytes: 10_000 },
              },
            }),
          },
          ...fixtureUsageInput.rows.cron_runs.slice(1),
        ],
      },
    });
    const rum = withPartial.storage.datasets.find(
      (d) => d.dataset === "pq_rum",
    );
    expect(rum?.bytes).toBe(250_000);
  });

  it("reports retention lag and logging overhead", () => {
    expect(summary.retentionLagMs).toBe(6 * 3_600_000);
    expect(summary.overhead).toMatchObject({ classA: 89, classB: 0, runs: 2 });
    expect(summary.overhead.cacheLookups).toBe(32);
  });
});

describe("unlogged counters", () => {
  const unlogged = {
    ...fixtureUsageInput,
    counters: { kv_ops: null, cache_replaces: null, purges: null },
  };

  it("stay null in the summary", () => {
    const summary = summarizeUsage(unlogged);
    expect(summary.kvOps).toBeNull();
    expect(summary.cacheReplaces).toBeNull();
  });
});

describe("measured storage pricing", () => {
  it("bills min_billed_days when retention is shorter", () => {
    const input = {
      ...fixtureUsageInput,
      days: 1,
      retention_days: { pq_request: 7, pq_ops: 90 },
    };
    const summary = summarizeUsage(input);
    const request = summary.storage.datasets.find(
      (d) => d.dataset === "pq_request",
    )!;
    const ops = summary.storage.datasets.find((d) => d.dataset === "pq_ops")!;
    expect(request.retentionDays).toBe(7);
    const requestGbPerDay = (5_000_000 + 999) / 1e9;
    expect(request.billedGbMonths).toBeCloseTo(requestGbPerDay * 30);
    const opsGbPerDay = 7_200 / 1e9;
    expect(ops.billedGbMonths).toBeCloseTo(opsGbPerDay * 90);
    expect(summary.storage.billedGbMonths).toBeCloseTo(
      request.billedGbMonths + ops.billedGbMonths,
      6,
    );
  });

  it("falls back to settings retention_days when a dataset has none", () => {
    const summary = summarizeUsage(fixtureUsageInput);
    expect(summary.storage.datasets[0].retentionDays).toBe(30);
  });
});

describe("fetchUsageInput", () => {
  afterEach(() => vi.unstubAllGlobals());
  const identity = { user: "u1", role: "admin" };
  const respond = (body: unknown, status = 200) => {
    const fetcher = vi.fn<typeof fetch>(async () =>
      Response.json(body, { status }),
    );
    vi.stubGlobal("fetch", fetcher);
    return fetcher;
  };
  const reader = (rows: Record<string, Record<string, unknown>[]> = {}) =>
    vi.fn<
      (
        kind: string,
        range: { from: string; to: string },
        options: unknown,
      ) => Promise<{
        rows: Record<string, unknown>[];
        liveTruncated?: boolean;
      }>
    >(async (kind) => ({
      rows: rows[kind] ?? [],
      liveTruncated: false,
    }));

  it("reads settings and the four log kinds over the requested window", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(FIXTURE_NOW);
    onTestFinished(() => {
      vi.useRealTimers();
    });
    const fetcher = respond(fixtureUsageInput.settings);
    const read = reader({
      fos_usage: [{ ...fixtureUsageInput.rows.fos_usage[0] }],
      cron_runs: [{ ...fixtureUsageInput.rows.cron_runs[0] }],
    });
    const before = Date.now();
    const input = await fetchUsageInput(7, identity, read);
    expect(String(fetcher.mock.calls[0][0])).toMatch(
      /\/api\/admin\/usage-logging$/,
    );
    expect(read.mock.calls.map(([kind]) => kind).sort()).toEqual([
      "api_telemetry",
      "cron_runs",
      "fos_usage",
    ]);
    const [, range, options] = read.mock.calls[0];
    expect(Date.parse(range.to) - Date.parse(range.from)).toBe(7 * 86_400_000);
    expect(options).toMatchObject(identity);
    expect(input.days).toBe(7);
    expect(input.now).toBeGreaterThanOrEqual(before);
    expect(input.settings).toEqual(fixtureUsageInput.settings);
    expect(input.telemetryCoverage).toEqual({ fos_usage: true });
    expect(input.rows.fos_usage).toEqual([fixtureUsageInput.rows.fos_usage[0]]);
    expect(input.rows.cron_runs).toHaveLength(1);
    expect(input.rows.api_telemetry).toEqual([]);
    expect(input.counters).toEqual({
      kv_ops: null,
      cache_replaces: null,
      purges: null,
    });
  });

  it("records incomplete FOS telemetry coverage when live logs are truncated", async () => {
    respond(fixtureUsageInput.settings);
    const read = reader();
    read.mockImplementation(async (kind) => ({
      rows: [],
      liveTruncated: kind === "fos_usage",
    }));

    const input = await fetchUsageInput(7, identity, read);

    expect(input.telemetryCoverage).toEqual({ fos_usage: false });
  });

  it("reads the idle tick counters on cron_runs rows", async () => {
    respond(fixtureUsageInput.settings);
    const input = await fetchUsageInput(
      1,
      identity,
      reader({
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[2],
            started_at: Date.now() - 1000,
            idle_ticks: BigInt(7),
            idle_vcpu_ms: 14,
          },
        ],
      }),
    );
    expect(input.rows.cron_runs[0]).toMatchObject({
      idle_ticks: 7,
      idle_vcpu_ms: 14,
    });
  });

  it("reads historical bounds instead of shifting them to the current day", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(FIXTURE_NOW + 7 * 86_400_000);
    onTestFinished(() => {
      vi.useRealTimers();
    });
    respond(fixtureUsageInput.settings);
    const row = {
      ...fixtureUsageInput.rows.fos_usage[0],
      timestamp: FIXTURE_NOW - 1000,
    };
    const read = reader({ fos_usage: [row] });
    const bounds = { from: FIXTURE_NOW - 3_600_000, to: FIXTURE_NOW };

    const input = await fetchUsageInput(1, identity, read, undefined, bounds);

    expect(read.mock.calls[0][1]).toEqual({
      from: new Date(bounds.from).toISOString(),
      to: new Date(bounds.to).toISOString(),
    });
    expect(input.rows.fos_usage).toEqual([row]);
    expect(input.now).toBe(bounds.to);
  });

  it("defaults null columns and drops rows outside the window", async () => {
    respond(fixtureUsageInput.settings);
    const now = Date.now();
    const input = await fetchUsageInput(
      1,
      identity,
      reader({
        api_telemetry: [
          {
            timestamp: now - 1000,
            source: "compute_api",
            path: "/api/files",
            vcpu_ms: 5,
            files_fetched: null,
          },
          {
            timestamp: now - 3 * 86_400_000,
            source: "compute_api",
            path: "/api/files",
            vcpu_ms: 9,
            files_fetched: 1,
          },
        ],
      }),
    );
    expect(input.rows.api_telemetry).toEqual([
      {
        timestamp: now - 1000,
        source: "compute_api",
        path: "/api/files",
        vcpu_ms: 5,
        files_fetched: undefined,
      },
    ]);
    expect(input.rows.cron_ticks).toEqual([]);
  });

  it.each([
    [
      "non-boolean enabled",
      { ...fixtureUsageInput.settings, enabled: "yes" },
      {},
    ],
    ["non-object settings", null, {}],
    [
      "non-numeric row field",
      fixtureUsageInput.settings,
      { fos_usage: [{ timestamp: Date.now(), op_count: "x" }] },
    ],
  ])("rejects %s", async (_name, settings, rows) => {
    respond(settings);
    await expect(fetchUsageInput(30, identity, reader(rows))).rejects.toThrow();
  });

  it("rejects HTTP errors and failed log reads", async () => {
    respond({ detail: "nope", error: "nope" }, 500);
    await expect(fetchUsageInput(30, identity, reader())).rejects.toThrow(
      "nope",
    );
    respond(fixtureUsageInput.settings);
    await expect(
      fetchUsageInput(30, identity, async () => {
        throw new Error("Request failed (503)");
      }),
    ).rejects.toThrow("Request failed (503)");
  });
});

describe("usage-logging settings sharing", () => {
  afterEach(() => vi.unstubAllGlobals());
  const identity = { user: "u1", role: "admin" };
  const read = async () => ({ rows: [] });
  const settingsCalls = (fetcher: ReturnType<typeof vi.fn>) =>
    fetcher.mock.calls.filter(([url]) =>
      String(url).endsWith("/api/admin/usage-logging"),
    );
  const deferredFetch = () => {
    const pending: ((response: Response) => void)[] = [];
    const fetcher = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((resolve, reject) => {
          pending.push(resolve);
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("Aborted", "AbortError")),
          );
        }),
    );
    vi.stubGlobal("fetch", fetcher);
    return { fetcher, pending };
  };

  it("fetches the settings once for concurrent readers", async () => {
    const { fetcher, pending } = deferredFetch();
    const reads = Promise.all([
      fetchUsageSettings(),
      fetchUsageSettings(),
      fetchUsageInput(7, identity, read),
      fetchUsageInput(7, identity, read),
    ]);
    await vi.waitFor(() => expect(pending.length).toBeGreaterThan(0));
    expect(settingsCalls(fetcher)).toHaveLength(1);
    for (const resolve of pending)
      resolve(Response.json(fixtureUsageInput.settings));
    const [a, b, c, d] = await reads;
    for (const settings of [a, b, c.settings, d.settings]) {
      expect(settings).toEqual(fixtureUsageInput.settings);
    }
    expect(a).not.toBe(b);
  });

  it("keeps the shared fetch for the others when one reader aborts", async () => {
    const { fetcher, pending } = deferredFetch();
    const leaving = new AbortController();
    const left = fetchUsageSettings(leaving.signal);
    const stays = fetchUsageSettings();
    leaving.abort();
    await expect(left).rejects.toMatchObject({ name: "AbortError" });
    expect((fetcher.mock.calls[0][1] as RequestInit).signal?.aborted).toBe(
      false,
    );
    pending[0](Response.json(fixtureUsageInput.settings));
    await expect(stays).resolves.toEqual(fixtureUsageInput.settings);
    expect(settingsCalls(fetcher)).toHaveLength(1);
  });

  it("aborts the shared fetch once every reader has aborted", async () => {
    const { fetcher } = deferredFetch();
    const first = new AbortController();
    const second = new AbortController();
    const reads = [
      fetchUsageSettings(first.signal),
      fetchUsageSettings(second.signal),
    ];
    first.abort();
    second.abort();
    for (const r of reads)
      await expect(r).rejects.toMatchObject({ name: "AbortError" });
    expect((fetcher.mock.calls[0][1] as RequestInit).signal?.aborted).toBe(
      true,
    );
  });

  it("reuses settled settings for the page's later readers until a write resets them", async () => {
    const fetcher = vi.fn(async () => Response.json(fixtureUsageInput.settings));
    vi.stubGlobal("fetch", fetcher);
    await fetchUsageSettings();
    await expect(fetchUsageInput(7, identity, read)).resolves.toMatchObject({
      settings: fixtureUsageInput.settings,
    });
    expect(settingsCalls(fetcher)).toHaveLength(1);
    resetUsageSettingsCache();
    await fetchUsageSettings();
    expect(settingsCalls(fetcher)).toHaveLength(2);
  });

  it("rejects every reader on failure and fetches again on the next call", async () => {
    const fetcher = vi.fn(async () =>
      Response.json({ detail: "nope", error: "nope" }, { status: 500 }),
    );
    vi.stubGlobal("fetch", fetcher);
    const reads = [fetchUsageSettings(), fetchUsageSettings()];
    for (const r of reads) await expect(r).rejects.toThrow("nope");
    expect(fetcher).toHaveBeenCalledTimes(1);
    fetcher.mockImplementation(async () =>
      Response.json(fixtureUsageInput.settings),
    );
    await expect(fetchUsageSettings()).resolves.toEqual(
      fixtureUsageInput.settings,
    );
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});

describe("requestStorageReconcile", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("posts to the real endpoint and returns whether a pass started", async () => {
    const fetcher = vi.fn<typeof fetch>(async () =>
      Response.json({ started: false }),
    );
    vi.stubGlobal("fetch", fetcher);
    await expect(requestStorageReconcile()).resolves.toBe(false);
    expect(String(fetcher.mock.calls[0][0])).toMatch(
      /\/api\/admin\/usage\/reconcile-storage$/,
    );
    expect(fetcher.mock.calls[0][1]).toMatchObject({ method: "POST" });
  });

  it("surfaces the error detail", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({ detail: "boom", error: "boom" }, { status: 503 }),
      ),
    );
    await expect(requestStorageReconcile()).rejects.toThrow("boom");
  });
});

describe("parseComputeBilling", () => {
  it("sums both services and spans their covered hours", () => {
    expect(
      parseComputeBilling({
        services: [
          {
            role: "app",
            compute_requests: 100,
            compute_execution_time_ms: 250.5,
            compute_request_time_billed_ms: 5_000,
            covered_from: 3_600,
            covered_to: 10_800,
          },
          {
            role: "worker",
            compute_requests: 20,
            compute_execution_time_ms: 40,
            compute_request_time_billed_ms: 1_000,
            covered_from: 7_200,
            covered_to: 14_400,
          },
        ],
      }),
    ).toEqual({
      requests: 120,
      executionTimeMs: 290.5,
      billedTimeMs: 6_000,
      coveredFrom: 3_600_000,
      coveredTo: 14_400_000,
      computeBandwidthBytes: 0,
      cdnRequests: null,
      kvClassA: 0,
      kvClassB: 0,
      secrets: null,
      byRole: {
        app: { requests: 100, vcpuMs: 250.5 },
        worker: { requests: 20, vcpuMs: 40 },
      },
    });
  });

  it("extracts front CDN bandwidth and hourly buckets when present", () => {
    expect(
      parseComputeBilling({
        services: [
          {
            role: "app",
            compute_requests: 100,
            compute_execution_time_ms: 250,
            compute_request_time_billed_ms: 5_000,
          },
          {
            role: "front",
            bandwidth: 2_500_000_000,
            bandwidth_buckets: [
              { start_time: 3_600, bandwidth: 1_000_000_000 },
              { start_time: 7_200, bandwidth: 1_500_000_000 },
            ],
            covered_from: 3_600,
            covered_to: 10_800,
          },
        ],
      }),
    ).toEqual({
      requests: 100,
      executionTimeMs: 250,
      billedTimeMs: 5_000,
      coveredFrom: 3_600_000,
      coveredTo: 10_800_000,
      cdnBandwidthBytes: 2_500_000_000,
      cdnBandwidthByHour: [
        { t: 3_600_000, bytes: 1_000_000_000 },
        { t: 7_200_000, bytes: 1_500_000_000 },
      ],
      computeBandwidthBytes: 0,
      cdnRequests: 0,
      kvClassA: 0,
      kvClassB: 0,
      secrets: null,
      byRole: {
        app: { requests: 100, vcpuMs: 250 },
        front: { requests: 0, vcpuMs: 0 },
      },
    });
  });

  it("merges the app and worker hourly Compute buckets by hour and keeps front traffic out", () => {
    const billing = parseComputeBilling({
      services: [
        {
          role: "app",
          compute_requests: 30,
          compute_execution_time_ms: 300,
          compute_request_time_billed_ms: 3_000,
          compute_requests_buckets: [
            { start_time: 3_600, compute_requests: 10 },
            { start_time: 7_200, compute_requests: 20 },
          ],
          compute_execution_time_ms_buckets: [
            { start_time: 3_600, compute_execution_time_ms: 100 },
            { start_time: 7_200, compute_execution_time_ms: 200 },
          ],
          compute_request_time_billed_ms_buckets: [
            { start_time: 3_600, compute_request_time_billed_ms: 1_000 },
            { start_time: 7_200, compute_request_time_billed_ms: 2_000 },
          ],
        },
        {
          role: "worker",
          compute_requests: 4,
          compute_execution_time_ms: 40,
          compute_request_time_billed_ms: 400,
          compute_requests_buckets: [{ start_time: 7_200, compute_requests: 4 }],
          compute_execution_time_ms_buckets: [
            { start_time: 7_200, compute_execution_time_ms: 40 },
          ],
          compute_request_time_billed_ms_buckets: [
            { start_time: 7_200, compute_request_time_billed_ms: 400 },
          ],
        },
        { role: "front", requests: 9_000, bandwidth: 1 },
      ],
    });
    expect(billing.computeByHour).toEqual([
      {
        t: 3_600_000,
        appRequests: 10,
        workerRequests: 0,
        appVcpuMs: 100,
        workerVcpuMs: 0,
        billedMs: 1_000,
      },
      {
        t: 7_200_000,
        appRequests: 20,
        workerRequests: 4,
        appVcpuMs: 200,
        workerVcpuMs: 40,
        billedMs: 2_400,
      },
    ]);
  });

  it("leaves computeByHour unset when the response has no Compute buckets", () => {
    expect(
      parseComputeBilling({
        services: [{ role: "app", compute_requests: 1 }],
      }).computeByHour,
    ).toBeUndefined();
  });

  it("keeps Compute bandwidth and requests on the Compute side and reads KV and secrets", () => {
    const billing = parseComputeBilling({
      services: [
        {
          role: "app",
          compute_requests: 100,
          compute_execution_time_ms: 250,
          compute_request_time_billed_ms: 5_000,
          bandwidth: 3_000,
          kv_store_class_a_operations: 10,
          kv_store_class_b_operations: 90,
          kv_class_a_buckets: [
            { start_time: 1_761_800_400, kv_store_class_a_operations: 10 },
          ],
          kv_class_b_buckets: [
            { start_time: 1_761_800_400, kv_store_class_b_operations: 90 },
          ],
        },
        {
          role: "worker",
          compute_requests: 7,
          compute_execution_time_ms: 50,
          compute_request_time_billed_ms: 700,
          bandwidth: 200,
          kv_store_class_a_operations: 1,
          kv_store_class_b_operations: 4,
          kv_class_a_buckets: [
            { start_time: 1_761_800_400, kv_store_class_a_operations: 1 },
          ],
          kv_class_b_buckets: [
            { start_time: 1_761_800_400, kv_store_class_b_operations: 4 },
          ],
        },
        {
          role: "front",
          bandwidth: 11_000,
          requests: 7_600,
          kv_store_class_a_operations: 2,
          kv_store_class_b_operations: 3,
          kv_class_a_buckets: [
            { start_time: 1_761_800_400, kv_store_class_a_operations: 2 },
          ],
          kv_class_b_buckets: [
            { start_time: 1_761_800_400, kv_store_class_b_operations: 3 },
          ],
        },
      ],
      secret_store: { store_id: "s", secrets: 10 },
    });
    expect(billing).toMatchObject({
      requests: 107,
      executionTimeMs: 300,
      computeBandwidthBytes: 3_200,
      cdnBandwidthBytes: 11_000,
      cdnRequests: 7_600,
      kvClassA: 13,
      kvClassB: 97,
      kvByHour: [{ t: 1_761_800_400_000, classA: 13, classB: 97 }],
      secrets: 10,
    });
  });
});

describe("telemetryCostUsd", () => {
  it("attributes telemetry dataset/caller FOS ops without double-counting, telemetry API Compute, and KV telemetry share", () => {
    const settings = { ...fixtureUsageInput.settings, ...LIST_RATES };
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      rows: {
        ...fixtureUsageInput.rows,
        fos_usage: [
          {
            timestamp: 1_761_800_000_000,
            op_class: "A",
            op: "PUT",
            dataset: "fos_usage",
            caller: "cron:log_flush",
            op_count: 100,
            bytes_in: 1_000,
            bytes_out: 0,
            bytes_deleted: 0,
            cdn_cache: "DIRECT",
            est_cost_usd: 0.0005,
          },
          {
            timestamp: 1_761_800_000_000,
            op_class: "B",
            op: "GET",
            dataset: "pq_request",
            caller: "api:/api/admin/usage/logged-traffic",
            op_count: 200,
            bytes_in: 0,
            bytes_out: 1_000_000_000,
            bytes_deleted: 0,
            cdn_cache: "MISS",
            est_cost_usd: 0.0001,
          },
          {
            timestamp: 1_761_800_000_000,
            op_class: "B",
            op: "GET",
            dataset: "pq_request",
            caller: "api:/api/files",
            op_count: 500,
            bytes_in: 0,
            bytes_out: 2_000_000_000,
            bytes_deleted: 0,
            cdn_cache: "MISS",
            est_cost_usd: 0.00025,
          },
        ],
        api_telemetry: [
          {
            timestamp: 1_761_800_000_000,
            source: "compute_api",
            path: "/api/admin/usage/compute-billing",
            vcpu_ms: 100,
            files_fetched: 1,
          },
          {
            timestamp: 1_761_800_000_000,
            source: "compute_api",
            path: "/api/files",
            vcpu_ms: 500,
            files_fetched: 1,
          },
        ],
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            cron_kind: "log_flush",
            vcpu_ms: 50,
            kv_ops: 100,
          },
          {
            ...fixtureUsageInput.rows.cron_runs[2],
            cron_kind: "request_ingest",
            vcpu_ms: 100,
            kv_ops: 100,
          },
        ],
      },
    });
    expect(summary.telemetryOps).toMatchObject({ classA: 100, classB: 200 });
    expect(summary.telemetryBytesRead).toBe(1_000_000_000);
    expect(summary.telemetryKvOps).toBe(100);
    const cost = telemetryCostUsd(summary, settings, {
      requests: 1_000,
      executionTimeMs: 10_000,
      billedTimeMs: 10_000,
      coveredFrom: null,
      coveredTo: null,
      cdnBandwidthBytes: 0,
      computeBandwidthBytes: 0,
      cdnRequests: 0,
      kvClassA: 100_000,
      kvClassB: 200_000,
      secrets: 0,
    });
    // KV share is 50% of 100k Class A ($6.50/M => $0.325) + 50% of 200k Class B ($0.55/M => $0.055) = $0.38
    // CDN egress is 1 GB * $0.12 = $0.12
    expect(cost).toBeGreaterThan(0.5);
  });
});

describe("incurredCost", () => {
  const settings = { ...fixtureUsageInput.settings, ...LIST_RATES };
  const base = {
    settings,
    hours: 720,
    billedGbHours: 720,
    classA: 1_000,
    classB: 10_000,
    loggedCdnBytes: 2e9,
    loggedComputeRequests: 1e6,
    loggedComputeVcpuMs: 1e6,
  };

  it("prices every Fastly product the install uses from Fastly's billed figures, front-service requests and bytes included", () => {
    const { total, lines } = incurredCost({
      ...base,
      cdnHits: 5_000,
      billing: {
        requests: 2e6,
        executionTimeMs: 10e6,
        billedTimeMs: 0,
        coveredFrom: null,
        coveredTo: null,
        cdnBandwidthBytes: 10e9,
        computeBandwidthBytes: 5e9,
        cdnRequests: 1e6,
        kvClassA: 1e6,
        kvClassB: 2e6,
        secrets: 20,
      },
    });
    const usd = Object.fromEntries(lines.map((l) => [l.id, l.usd]));
    expect(usd.storage).toBeCloseTo(0.02);
    expect(usd.ops).toBeCloseTo(0.0025 + 0.004);
    // Fastly bills every front request and byte, health probes included: 10 GB front + 5 GB Compute egress
    // = 15 GB, and 1e6 front requests (not the 15,000 logged file deliveries).
    expect(usd.delivery).toBeCloseTo(15 * 0.12 + 100 * 0.01);
    const delivery = lines.find((l) => l.id === "delivery")!;
    expect(delivery.parts?.map((p) => p.label)).toEqual(["Egress", "Requests"]);
    expect(delivery.parts?.[0].usd).toBeCloseTo(15 * 0.12);
    expect(delivery.parts?.[1].usd).toBeCloseTo(100 * 0.01);
    // 10e6 ms across 2e6 requests is 5 ms/req, within the 20 ms/req contract free allowance ($0 excess CPU).
    expect(usd.compute).toBeCloseTo(2 * 0.5);
    expect(usd.kv).toBeCloseTo(6.5 + 2 * 0.55);
    expect(usd.secrets).toBeCloseTo(50);
    expect(usd.config).toBe(0);
    expect(total).toBeCloseTo(0.0265 + 2.8 + 1.0 + 7.6 + 50);
  });

  it("prices logged file deliveries when Fastly read no front service", () => {
    const { lines } = incurredCost({
      ...base,
      cdnHits: 5_000,
      billing: {
        requests: 0, executionTimeMs: 0, billedTimeMs: 0, coveredFrom: null, coveredTo: null,
        computeBandwidthBytes: 5e9, cdnRequests: null, kvClassA: 0, kvClassB: 0, secrets: null,
      },
    });
    const delivery = lines.find((l) => l.id === "delivery")!;
    // 2 GB logged CDN + 5 GB Compute egress; 10,000 Class B + 5,000 CDN hits.
    expect(delivery.usd).toBeCloseTo(7 * 0.12 + 1.5 * 0.01);
  });

  it("falls back to logged Compute and egress and leaves KV and secrets unpriced without Fastly figures", () => {
    const { total, telemetryUsd, lines } = incurredCost({
      ...base,
      billing: null,
      telemetryUsd: 0.42,
    });
    const byId = Object.fromEntries(lines.map((l) => [l.id, l]));
    // 1e6 ms across 1e6 requests is 1 ms/req, within the 20 ms/req contract free allowance ($0 excess CPU).
    expect(byId.compute.usd).toBeCloseTo(0.5);
    expect(byId.compute.source).toBe("logged");
    expect(byId.delivery.usd).toBeCloseTo(2 * 0.12);
    expect(byId.kv.usd).toBeNull();
    expect(byId.secrets.usd).toBeNull();
    expect(total).toBeCloseTo(0.0265 + 0.5 + 0.24);
    expect(telemetryUsd).toBeCloseTo(0.42);
  });

  it("applies the 20 ms free allowance and nearest-10ms rounding per Compute request, not once to the aggregate total", () => {
    // Nearest-10ms rounding on excess above 20 ms:
    // 24 ms => 4 ms excess => rounds to 0 ms ($0)
    // 26 ms => 6 ms excess => rounds to 10 ms ($0.0000005 per request)
    expect(billedVcpuMsPerRequest(24, 20, 10)).toBe(0);
    expect(billedVcpuMsPerRequest(26, 20, 10)).toBe(10);
    expect(billedVcpuForExecutions(24_000, 1_000, settings)).toEqual({
      billedMs: 0,
      usd: 0,
    });
    const billed26 = billedVcpuForExecutions(26_000, 1_000, settings);
    expect(billed26.billedMs).toBe(10_000);
    expect(billed26.usd).toBeCloseTo(0.0005);

    // 1,000 requests at 15 ms each = 15,000 ms total.
    // Aggregate subtraction would falsely bill 15,000 - 20 = 14,980 ms; per-request billing is 0 ms.
    const subFree = incurredCost({
      ...base,
      loggedComputeRequests: 1_000,
      loggedComputeVcpuMs: 15_000,
      billing: null,
    });
    const subFreeCompute = subFree.lines.find((l) => l.id === "compute")!;
    expect(subFreeCompute.usd).toBeCloseTo((1_000 / 1e6) * 0.5);

    // Idle ticks in cron_runs must not dilute active executions:
    // 100 idle ticks with 100 ms total (1 ms/tick => 0 ms billed) + 1 active run with 46 ms (26 ms excess => 30 ms billed).
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      settings,
      rows: {
        fos_usage: [],
        api_telemetry: [],
        cron_ticks: [],
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            run_id: "r-active",
            trigger: "clock",
            cron_kind: "log_flush",
            vcpu_ms: 46,
            worker_executions: 0,
            idle_ticks: 100,
            idle_vcpu_ms: 100,
          },
        ],
      },
    });
    expect(summary.computeRequests).toBe(101);
    expect(summary.computeVcpuMs).toBe(146);
    expect(summary.computeBilledVcpuMs).toBe(30);
    // Telemetry cost bills the 1 log_flush run + 30 ms excess CPU.
    expect(telemetryCostUsd(summary, settings)).toBeCloseTo(
      (1 / 1e6) * 0.5 + (30 / 1000) * 0.00005,
    );
    // Incurred cost (logged fallback) bills 101 requests + 30 ms excess CPU
    // (not 146 ms / 101 requests = 1.44 ms/req => 0 ms!).
    const loggedIncurred = incurredCost({
      settings,
      hours: 24,
      billedGbHours: 0,
      classA: 0,
      classB: 0,
      loggedCdnBytes: 0,
      loggedComputeRequests: summary.computeRequests,
      loggedComputeVcpuMs: summary.computeVcpuMs,
      loggedComputeSegments: summary.computeSegments,
      billing: null,
    });
    expect(
      loggedIncurred.lines.find((l) => l.id === "compute")?.usd,
    ).toBeCloseTo((101 / 1e6) * 0.5 + (30 / 1000) * 0.00005);
  });

  it("rejects a response without services", () => {
    expect(() => parseComputeBilling({ error: "x" })).toThrow(/services/);
  });

  it("derives FOS usage, storage datasets, and logging overhead from cron_runs when fos_usage is empty", () => {
    const baseAt = fixtureUsageInput.rows.cron_runs[0].started_at;
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      settings,
      rows: {
        fos_usage: [],
        api_telemetry: [],
        cron_ticks: [],
        cron_runs: [
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            started_at: baseAt,
            run_id: "r-reconcile",
            trigger: "admin_manual",
            cron_kind: "storage_reconcile",
            outcome: "done",
            details_json: JSON.stringify({
              datasets: {
                pq_request: { bytes: 1_200_000 },
                pq_ops: { bytes: 200_000 },
              },
            }),
          },
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            started_at: baseAt + 1_000,
            run_id: "r-ingest",
            trigger: "clock",
            cron_kind: "request_ingest",
            outcome: "done",
            listed_files: 5,
            converted_files: 5,
            deleted_files: 5,
            parquet_files_written: 1,
            bytes_written: 250_000,
            rows_written: 4_000,
          },
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            started_at: baseAt + 2_000,
            run_id: "r-flush",
            trigger: "clock",
            cron_kind: "log_flush",
            outcome: "done",
            parquet_files_written: 2,
            bytes_written: 18_000,
            rows_written: 120,
          },
          {
            ...fixtureUsageInput.rows.cron_runs[0],
            started_at: baseAt + 3_000,
            run_id: "r-compact",
            trigger: "clock",
            cron_kind: "log_compact",
            outcome: "done",
            listed_files: 4,
            deleted_files: 4,
            parquet_files_written: 1,
            bytes_written: 36_000,
            rows_written: 240,
          },
        ],
      },
    });

    // request_ingest: 1 LIST + 1 PUT (Class A = 2), 5 GETs (Class B = 5)
    // log_flush: 2 PUTs (Class A = 2)
    // log_compact: 1 LIST + 1 PUT + 1 DELETE_OBJECTS batch (Class A = 3), 4 GETs (Class B = 4)
    expect(summary.totals.classA).toBe(7);
    expect(summary.totals.classB).toBe(9);
    expect(summary.bytesWritten).toBe(250_000 + 18_000 + 36_000);
    expect(summary.telemetryOps?.classA).toBe(5);
    expect(summary.telemetryOps?.classB).toBe(4);
    expect(
      summary.storage.datasets.find((d) => d.dataset === "pq_request")?.bytes,
    ).toBe(1_200_000 + 250_000);
    expect(
      summary.storage.datasets.find((d) => d.dataset === "pq_ops")?.bytes,
    ).toBe(200_000 + 18_000);
    expect(summary.overhead.classA).toBe(5);
    expect(summary.overhead.classB).toBe(4);
  });

  it("counts event-driven meta/cloud_dr_state.json pq_ops PUTs in totals, telemetryOps, and pq_ops storage", () => {
    const summary = summarizeUsage({
      ...fixtureUsageInput,
      settings,
      rows: {
        fos_usage: [
          {
            ...fixtureUsageInput.rows.fos_usage[0],
            timestamp: FIXTURE_NOW - 30_000,
            op: "PUT",
            op_class: "A",
            op_count: 2,
            bytes_in: 4_096,
            bytes_out: 0,
            bytes_deleted: 0,
            dataset: "pq_ops",
            caller: "api:/api/admin/services/svc-1",
          },
        ],
        api_telemetry: [],
        cron_runs: [],
        cron_ticks: [],
      },
    });
    expect(summary.totals.classA).toBe(2);
    expect(summary.telemetryOps?.classA).toBe(2);
    expect(summary.bytesWritten).toBe(4_096);
    expect(summary.storage.datasets.find((d) => d.dataset === "pq_ops")?.bytes).toBe(4_096);
  });
});

describe("deriveFosUsageFromCronRuns summary bundles", () => {
  const run = (over: Partial<CronRunRow>): CronRunRow => ({
    started_at: 1,
    cron_kind: "summary_bundle",
    outcome: "done",
    vcpu_ms: 0,
    cache_lookups: 0,
    backend_requests_used: 0,
    details_json: "",
    ...over,
  });
  const puts = (rows: CronRunRow[]) =>
    deriveFosUsageFromCronRuns(rows)
      .filter((row) => row.op === "PUT")
      .reduce((sum, row) => sum + row.op_count, 0);

  it("counts one PUT per file written, since each partial or bundle is one PUT", () => {
    expect(puts([run({ listed_files: 3, parquet_files_written: 2 })])).toBe(2);
  });

  it("counts no PUT for a run that only deleted stale partials", () => {
    expect(
      puts([
        run({ listed_files: 0, deleted_files: 4, parquet_files_written: 0 }),
      ]),
    ).toBe(0);
  });
});

describe("projected ops reads for /usage", () => {
  it("fetchIngestDeletedIntervals requests projected columns without dedupe/sort and handles duplicate unsorted rows accurately", async () => {
    const calls: Array<{
      kind: string;
      options: { columns?: readonly string[]; dedupe?: boolean; sort?: boolean };
    }> = [];
    const t0 = Date.parse("2026-11-20T00:00:00.000Z");
    const reader: UsageLogReader = async (kind, _range, options) => {
      calls.push({ kind, options });
      return {
        liveTruncated: false,
        rows: [
          // Duplicate & unsorted rows for the same raw_key + empty "[]" output_keys_json.
          {
            timestamp: t0 + 60_000,
            raw_key: "raw/request/a.log.gz",
            raw_size: 4_096,
            status: "converted",
            state_version: 2,
            output_keys_json: "[]",
            raw_deleted: true,
            raw_deleted_at: t0 + 60_000,
          },
          {
            timestamp: t0,
            raw_key: "raw/request/a.log.gz",
            raw_size: 4_096,
            status: "converted",
            state_version: 1,
            output_keys_json: JSON.stringify([{ key: "processed/request/p1.parquet", bytes: 1_200 }]),
            raw_deleted: false,
            raw_deleted_at: null,
          },
          {
            timestamp: t0,
            raw_key: "raw/request/a.log.gz",
            raw_size: 4_096,
            status: "converted",
            state_version: 1,
            output_keys_json: JSON.stringify([{ key: "processed/request/p1.parquet", bytes: 1_200 }]),
            raw_deleted: false,
            raw_deleted_at: null,
          },
        ],
      };
    };

    const res = await fetchIngestDeletedIntervals(
      { from: t0 - 1_000, to: t0 + 120_000 },
      { user: "alice", role: "admin" },
      reader,
    );

    expect(calls).toHaveLength(1);
    expect(calls[0].kind).toBe("ingest_files");
    expect(calls[0].options.dedupe).toBe(false);
    expect(calls[0].options.sort).toBe(false);
    expect(calls[0].options.columns).toEqual(
      expect.arrayContaining([
        "timestamp",
        "raw_key",
        "raw_size",
        "status",
        "state_version",
        "output_keys_json",
        "raw_deleted",
        "raw_deleted_at",
      ]),
    );
    expect(calls[0].options.columns).not.toContain("event_id");
    expect(calls[0].options.columns).not.toContain("run_id");
    expect(res.intervals).toHaveLength(1);
    expect(res.intervals[0]).toMatchObject({
      id: "raw_request:raw/request/a.log.gz",
      bytes: 4_096,
      createdAt: t0,
      deletedAt: t0 + 60_000,
    });
    expect(res.outputs).toEqual([
      { key: "processed/request/p1.parquet", bytes: 1_200, createdAt: t0 },
    ]);
  });

  it("fetchCloudStorageInventory requests projected columns without dedupe/sort and fetchUsageKindWithCoverage keeps event_id for fos_usage", async () => {
    const calls: Array<{
      kind: string;
      options: {
        columns?: readonly string[];
        dedupe?: boolean;
        sort?: boolean;
        latestCompleteSnapshotOnly?: boolean;
      };
    }> = [];
    const now = Date.parse("2026-11-24T12:00:00.000Z");
    const reader: UsageLogReader = async (kind, _range, options) => {
      calls.push({ kind, options });
      if (kind === "storage_inventory") {
        return {
          liveTruncated: false,
          rows: [
            {
              timestamp: now - 10_000,
              snapshot_id: "snap-1",
              dataset: "pq_request",
              key: "processed/request/a.parquet",
              size: 2_048,
              last_modified: now - 3_600_000,
              complete: false,
            },
            {
              timestamp: now - 5_000,
              snapshot_id: "snap-1",
              dataset: "",
              key: "",
              size: 0,
              last_modified: null,
              complete: true,
            },
          ],
        };
      }
      return {
        liveTruncated: false,
        rows: [
          {
            timestamp: now - 1_000,
            event_id: "fos-1",
            op: "PUT",
            op_class: "A",
            op_count: 5,
            dataset: "pq_request",
            caller: "cron:request_ingest",
            cdn_cache: "DIRECT",
            bytes_in: 1_000,
            bytes_out: 0,
            bytes_deleted: 0,
            est_cost_usd: 0.0001,
          },
        ],
      };
    };

    const inv = await fetchCloudStorageInventory({
      now,
      identity: { user: "alice", role: "admin" },
      read: reader,
    });
    const fos = await fetchUsageKindWithCoverage(
      "fos_usage",
      { from: now - 86_400_000, to: now },
      { user: "alice", role: "admin" },
      reader,
    );

    expect(calls[0].kind).toBe("storage_inventory");
    expect(calls[0].options.latestCompleteSnapshotOnly).toBe(true);
    expect(calls[0].options.dedupe).toBe(false);
    expect(calls[0].options.sort).toBe(false);
    expect(calls[0].options.columns).not.toContain("event_id");
    expect(inv.intervals).toHaveLength(1);
    expect(inv.intervals[0].bytes).toBe(2_048);

    expect(calls[1].kind).toBe("fos_usage");
    expect(calls[1].options.dedupe).toBeUndefined();
    expect(calls[1].options.columns).toContain("event_id");
    expect(calls[1].options.columns).not.toContain("sample_key");
    expect(fos.rows).toHaveLength(1);
    expect(fos.rows[0].op_count).toBe(5);
  });
});
