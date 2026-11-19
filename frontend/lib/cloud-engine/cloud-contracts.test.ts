import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import type { components, paths } from "./cloud-api.generated";
import { cloudDeleteData, setDeleteDataPollInterval } from "./delete-data";
import {
  deleteLogHistory,
  flushLogs,
  sweepRetention,
  type FlushResult,
  type HistoryJob,
  type SweepResult,
} from "./telemetry/admin";
import {
  DEFAULT_COMPACTION,
  measureEstimate,
  type CompactionSetting,
  type EstimateMeasurements,
} from "./usage/estimate";

type GeneratedDeleteDataStatus = components["schemas"]["DeleteDataStatus"];
type GeneratedCompactionSetting = components["schemas"]["CompactionSetting"];
type GeneratedLogHistoryJob = components["schemas"]["LogHistoryJob"];
type GeneratedFlushResult =
  paths["/api/admin/logs/flush"]["post"]["responses"]["200"]["content"]["application/json"];
type GeneratedSweepResult =
  paths["/api/admin/logs/sweep"]["post"]["responses"]["200"]["content"]["application/json"];

describe("Cloud wire contract parity", () => {
  it("pins replaced Cloud wire types to cloud-api.generated.ts", () => {
    expectTypeOf<CompactionSetting>().toEqualTypeOf<GeneratedCompactionSetting>();
    expectTypeOf<HistoryJob>().toEqualTypeOf<GeneratedLogHistoryJob>();
    expectTypeOf<FlushResult>().toEqualTypeOf<GeneratedFlushResult>();
    expectTypeOf<SweepResult>().toEqualTypeOf<GeneratedSweepResult>();
  });

  describe("DeleteDataStatus", () => {
    beforeEach(() => setDeleteDataPollInterval(1));
    afterEach(() => {
      vi.unstubAllGlobals();
      setDeleteDataPollInterval(5000);
    });

    it("handles omitted optional fields and null error on terminal status", async () => {
      const initial: GeneratedDeleteDataStatus = { status: "fencing" };
      const terminal: GeneratedDeleteDataStatus = { status: "complete", error: null };
      let polls = 0;
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
          const req = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
          const path = new URL(req.url).pathname;
          if (path === "/api/admin/services/svc-1" && req.method === "GET") {
            return Response.json({ service: { service_id: "svc-1" }, generation: "gen-1" });
          }
          if (path === "/api/admin/services/svc-1/delete-data" && req.method === "POST") {
            return Response.json({ job: initial });
          }
          if (path === "/api/admin/services/svc-1/delete-data" && req.method === "GET") {
            polls += 1;
            return Response.json(terminal);
          }
          throw new Error(`Unexpected request: ${req.method} ${path}`);
        }),
      );

      const response = await cloudDeleteData(
        new Request("http://localhost/api/admin/reset-logs?service_id=svc-1", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ confirm: "svc-1" }),
        }),
      );
      expect(response).not.toBeNull();
      const body = await response!.text();
      expect(polls).toBe(1);
      expect(body).toContain('"type":"done"');
      expect(body).toContain("Delete Data complete.");
    });

    it("falls back cleanly when status is none and error is null", async () => {
      const noneJob: GeneratedDeleteDataStatus = { status: "none", error: null };
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
          const req = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
          const path = new URL(req.url).pathname;
          if (path === "/api/admin/services/svc-1" && req.method === "GET") {
            return Response.json({ service: { service_id: "svc-1" }, generation: "gen-1" });
          }
          if (path === "/api/admin/services/svc-1/delete-data" && req.method === "POST") {
            return Response.json({ job: noneJob });
          }
          throw new Error(`Unexpected request: ${req.method} ${path}`);
        }),
      );

      const response = await cloudDeleteData(
        new Request("http://localhost/api/admin/reset-logs?service_id=svc-1", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ confirm: "svc-1" }),
        }),
      );
      const body = await response!.text();
      expect(body).toContain('"type":"error"');
      expect(body).toContain("Delete Data job disappeared.");
    });
  });

  describe("CompactionSetting", () => {
    const baseMeasurements = (
      compaction?: CompactionSetting,
    ): EstimateMeasurements => ({
      days: 1,
      minBilledDays: 30,
      service: {
        log_period: 60,
        sample_rate: 100,
        edge_only: false,
        delete_after: true,
        log_retention_days: 30,
        ...(compaction ? { compaction } : {}),
      },
      traffic: [],
      billing: null,
      fos: null,
      files: [],
      parquet: { bytes: 0, rows: 0 },
      fixedStoredBytes: null,
    });

    it("defaults compaction when omitted from service config", () => {
      const prefill = measureEstimate(baseMeasurements(undefined));
      expect(prefill.serviceCompaction).toBeNull();
      expect(prefill.compaction).toEqual(DEFAULT_COMPACTION);
    });

    it("maps default off and size_target compaction settings", () => {
      const offPrefill = measureEstimate(
        baseMeasurements({ mode: "off", size_target_mb: 64 }),
      );
      expect(offPrefill.serviceCompaction).toEqual({ mode: "off", size_target_mb: 64 });
      expect(offPrefill.compaction.enabled).toBe(false);
      expect(offPrefill.compaction.targetMb).toBe(64);

      const onPrefill = measureEstimate(
        baseMeasurements({ mode: "size_target", size_target_mb: 128 }),
      );
      expect(onPrefill.compaction.enabled).toBe(true);
      expect(onPrefill.compaction.targetMb).toBe(128);
    });
  });

  describe("LogHistoryJob, FlushResult, SweepResult", () => {
    afterEach(() => vi.unstubAllGlobals());

    it("accepts LogHistoryJob with omitted or null older_than_days and error", async () => {
      const omittedJob: HistoryJob = {
        kind: "fos_usage",
        cutoff_ts: 1_700_000_000,
        status: "deleting",
        actor: "admin@example.com",
        started_at: 1_700_000_100,
        updated_at: 1_700_000_100,
        deleted: 0,
      };
      const nullJob: HistoryJob = {
        ...omittedJob,
        older_than_days: null,
        status: "complete",
        error: null,
      };
      vi.stubGlobal(
        "fetch",
        vi
          .fn()
          .mockResolvedValueOnce(Response.json({ job: omittedJob }))
          .mockResolvedValueOnce(Response.json({ job: nullJob })),
      );

      expect((await deleteLogHistory("fos_usage")).job).toEqual(omittedJob);
      expect((await deleteLogHistory("fos_usage", 30)).job).toEqual(nullJob);
    });

    it("parses FlushResult and SweepResult including nullable datasets", async () => {
      const flush: FlushResult = {
        status: "ok",
        rows: 0,
        files: 0,
        purged: 0,
        outbox_depth: 0,
      };
      const sweep: SweepResult = {
        deleted: 0,
        failed: 0,
        bytes: 0,
        more_pending: false,
        datasets: null,
        backend_calls: 0,
      };
      vi.stubGlobal(
        "fetch",
        vi
          .fn()
          .mockResolvedValueOnce(Response.json(flush))
          .mockResolvedValueOnce(Response.json(sweep)),
      );

      expect(await flushLogs()).toEqual(flush);
      expect(await sweepRetention()).toEqual(sweep);
    });
  });
});
