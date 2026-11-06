import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RunProgress } from "./dashboard/engine";

vi.mock("@/lib/api", () => ({ getPageLoadId: () => "test-page" }));
vi.mock("@/stores/filterStore", () => ({
  useDebugStore: { getState: () => ({ apiCallsEnabled: false }) },
}));
vi.mock("./dashboard/engine", () => ({
  runDashboard: vi.fn(),
  clearDashboardMemo: vi.fn(),
}));
vi.mock("./dashboard/duckdb", () => ({
  getDuckDBEngine: () => ({ executeSql: vi.fn().mockResolvedValue([{ count: 1 }]) }),
}));

class MockWorker {
  static instances: MockWorker[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: { message: string }) => void) | null = null;
  messages: { id: number; type?: string }[] = [];
  constructor() { MockWorker.instances.push(this); }
  postMessage(message: { id: number; type?: string }) { this.messages.push(message); }
  terminate() {}
  emit(message: object) { this.onmessage?.({ data: message } as MessageEvent); }
}

const progress: RunProgress = {
  phase: "files", cached: 0, toFetch: 2, toDecode: 0, done: 1,
  downloadedBytes: 1024, cachedBytes: 0,
};

beforeEach(() => {
  vi.resetModules();
  MockWorker.instances = [];
  vi.stubGlobal("Worker", MockWorker);
});
afterEach(() => { vi.unstubAllGlobals(); });

describe("cloud execution progress", () => {
  it("publishes worker progress while preserving caller callbacks and concurrent SQL work", async () => {
    const { DashboardDuckDBClient } = await import("./dashboard/client");
    const { cloudLoadingStore } = await import("./loading-progress");
    const client = new DashboardDuckDBClient();
    const onProgress = vi.fn();
    const run = client.run({ from: "2026-10-29", to: "2026-10-30", onProgress });
    const sql = client.executeSql("SELECT 1");
    const worker = MockWorker.instances[0];
    const [runJob, sqlJob] = worker.messages;
    worker.emit({ id: runJob.id, type: "progress", progress });
    expect(cloudLoadingStore.getSnapshot()?.progress).toEqual(progress);
    expect(onProgress).toHaveBeenCalledWith(progress);
    worker.emit({ id: runJob.id, type: "result", result: {} });
    await run;
    expect(cloudLoadingStore.getSnapshot()?.stage).toBe("files");
    worker.emit({ id: sqlJob.id, type: "sqlResult", result: [{ count: 1 }] });
    await expect(sql).resolves.toEqual([{ count: 1 }]);
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    client.terminate();
  });

  it("cleans up aborts, worker errors and termination, ignoring stale progress", async () => {
    const { DashboardDuckDBClient } = await import("./dashboard/client");
    const { cloudLoadingStore } = await import("./loading-progress");
    const client = new DashboardDuckDBClient();
    const controller = new AbortController();
    const run = client.run({ from: "2026-10-29", to: "2026-10-30", signal: controller.signal });
    const rejected = expect(run).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    await rejected;
    const worker = MockWorker.instances[0];
    worker.emit({ id: 1, type: "progress", progress });
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    const sql = client.executeSql("SELECT 1");
    const failed = expect(sql).rejects.toThrow("broken");
    worker.emit({ id: 2, type: "error", message: "broken" });
    await failed;
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    const pending = client.executeSql("SELECT 2");
    const terminated = expect(pending).rejects.toMatchObject({ name: "AbortError" });
    client.terminate();
    await terminated;
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });

  it("tracks inline fallback and clears failures", async () => {
    vi.stubGlobal("Worker", undefined);
    const engine = await import("./dashboard/engine");
    vi.mocked(engine.runDashboard).mockImplementationOnce(async (_source, options) => {
      options.onProgress?.(progress);
      throw new Error("inline failed");
    });
    const { DashboardDuckDBClient } = await import("./dashboard/client");
    const { cloudLoadingStore } = await import("./loading-progress");
    const client = new DashboardDuckDBClient();
    const run = client.run({ from: "2026-10-29", to: "2026-10-30" });
    expect(cloudLoadingStore.getSnapshot()?.progress).toEqual(progress);
    await expect(run).rejects.toThrow("inline failed");
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    await expect(client.executeSql("SELECT 1")).resolves.toEqual([{ count: 1 }]);
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });

  it("tracks telemetry discovery and worker file progress, then removes failures", async () => {
    const { readLogs } = await import("./telemetry/client");
    const { cloudLoadingStore } = await import("./loading-progress");
    const onProgress = vi.fn();
    const run = readLogs({ kind: "fos_usage", from: "2026-10-29", to: "2026-10-30", onProgress });
    expect(cloudLoadingStore.getSnapshot()?.stage).toBe("listing");
    const worker = MockWorker.instances[0];
    worker.emit({ id: 1, type: "progress", progress });
    expect(cloudLoadingStore.getSnapshot()).toMatchObject({ stage: "files", progress });
    expect(onProgress).toHaveBeenCalledWith(progress);
    const failed = expect(run).rejects.toThrow("telemetry failed");
    worker.emit({ id: 1, type: "error", message: "telemetry failed" });
    await failed;
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    worker.emit({ id: 1, type: "progress", progress });
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    const refresh = readLogs({ kind: "fos_usage", from: "2026-10-29", to: "2026-10-30" });
    worker.emit({ id: 2, type: "progress", progress });
    worker.emit({ id: 2, type: "result", result: { rows: [] } });
    await expect(refresh).resolves.toEqual({ rows: [] });
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });
});
