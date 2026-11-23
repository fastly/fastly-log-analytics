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

describe("tab cache identity", () => {
  const stubCaches = () => {
    const buckets = new Map<string, Map<string, Response>>();
    vi.stubGlobal("caches", {
      async open(name: string) {
        const bucket = buckets.get(name) ?? new Map<string, Response>();
        buckets.set(name, bucket);
        return {
          async match(key: RequestInfo | URL) { return bucket.get(String(key))?.clone(); },
          async put(key: RequestInfo | URL, value: Response) { bucket.set(String(key), value.clone()); },
          async delete(key: RequestInfo | URL) { return bucket.delete(String(key)); },
          async keys() { return [...bucket.keys()].map((url) => new Request(url)); },
        };
      },
      async keys() { return [...buckets.keys()]; },
      async delete(name: string) { return buckets.delete(name); },
    });
    return buckets;
  };
  // A new tab or reload is a fresh module instance sharing localStorage and CacheStorage.
  const freshTab = async () => {
    vi.resetModules();
    const client = await import("./client");
    const sources = await import("./sources");
    // sources also exports a storage-only setDashboardCacheIdentity; the tab uses client's.
    return { ...sources, ...client };
  };
  const seed = async () => {
    const tab = await freshTab();
    await tab.setDashboardCacheIdentity("alice:admin");
    await (await caches.open(tab.CACHE_NAME)).put("http://localhost/fos/a.parquet", new Response("bytes"));
    return tab.dashboardCacheScope();
  };

  it("keeps a new tab of the same identity on the persisted files and scope", async () => {
    localStorage.clear();
    const buckets = stubCaches();
    const scope = await seed();

    const tab = await freshTab();
    await tab.setDashboardCacheIdentity("alice:admin");
    expect(buckets.get(tab.CACHE_NAME)?.has("http://localhost/fos/a.parquet")).toBe(true);
    expect(tab.dashboardCacheScope()).toBe(scope);
  });

  it("clears for a new tab of another identity, or after an unfinished transition", async () => {
    localStorage.clear();
    const buckets = stubCaches();
    await seed();
    const bob = await freshTab();
    await bob.setDashboardCacheIdentity("bob:analyst");
    expect(buckets.get(bob.CACHE_NAME)?.size ?? 0).toBe(0);

    await seed();
    // A tab closed mid-clear leaves the identity written but not the ready marker.
    localStorage.removeItem("flp-dashboard-cache-ready");
    const tab = await freshTab();
    await tab.setDashboardCacheIdentity("alice:admin");
    expect(buckets.get(tab.CACHE_NAME)?.size ?? 0).toBe(0);
  });
});
