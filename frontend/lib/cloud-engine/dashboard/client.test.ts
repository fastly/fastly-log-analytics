import { afterEach, describe, expect, it, vi } from "vitest";

class FakeWorker {
  static created: FakeWorker[] = [];
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: unknown = null;
  readonly postMessage = vi.fn();
  readonly terminate = vi.fn();
  constructor(readonly url: URL, readonly options: unknown) {
    FakeWorker.created.push(this);
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
  FakeWorker.created = [];
});

describe("answerRollupInWorker", () => {
  it("runs the summary read in the dashboard engine worker under the page's cache scope", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const { answerRollupInWorker } = await import("./client");
    const { dashboardCacheScope } = await import("./sources");
    const params = { start_time: "2026-11-05T00:00:00Z", end_time: "2026-11-06T00:00:00Z", sections: ["summary"] };

    const pending = answerRollupInWorker("/api/network-health", params, "svc-1");
    expect(pending).not.toBeNull();
    const [worker] = FakeWorker.created;
    expect(worker.url.pathname).toMatch(/engine\.worker\.ts$/);
    const [message] = worker.postMessage.mock.calls[0] as [{ id: number }];
    expect(message).toEqual({
      id: message.id, type: "rollup", path: "/api/network-health", params, serviceId: "svc-1",
      cacheScope: dashboardCacheScope(),
    });
    worker.onmessage!({ data: { id: message.id, type: "rollupResult", result: { served: "worker" } } });
    await expect(pending).resolves.toEqual({ served: "worker" });
  });

  it("cancels the worker read when the caller aborts", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const { answerRollupInWorker } = await import("./client");
    const controller = new AbortController();
    const pending = answerRollupInWorker("/api/origin/aggregates", {}, undefined, controller.signal);
    const [worker] = FakeWorker.created;
    const [{ id }] = worker.postMessage.mock.calls[0] as [{ id: number }];
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.postMessage).toHaveBeenLastCalledWith({ id, type: "cancel" });
  });

  it("returns null where there is no Worker, so the page reads in-thread", async () => {
    vi.stubGlobal("Worker", undefined);
    const { answerRollupInWorker } = await import("./client");
    expect(answerRollupInWorker("/api/network-health", {}, "svc-1")).toBeNull();
  });
});

describe("persistent storage", () => {
  it("asks once for persistent storage when the engine worker starts", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    const persist = vi.fn().mockResolvedValue(true);
    vi.stubGlobal("navigator", { storage: { persist } });
    const { answerRollupInWorker } = await import("./client");
    answerRollupInWorker("/api/network-health", {}, "svc-1");
    answerRollupInWorker("/api/network-health", {}, "svc-1");
    expect(persist).toHaveBeenCalledTimes(1);
  });

  it("starts the worker when the browser refuses persistence", async () => {
    vi.stubGlobal("Worker", FakeWorker);
    vi.stubGlobal("navigator", {
      storage: { persist: vi.fn().mockRejectedValue(new Error("denied")) },
    });
    const { answerRollupInWorker } = await import("./client");
    expect(answerRollupInWorker("/api/network-health", {}, "svc-1")).not.toBeNull();
    await Promise.resolve();
  });
});
