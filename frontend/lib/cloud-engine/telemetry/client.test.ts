import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/api", () => ({ getPageLoadId: () => "test-page" }));

type Job = {
  id: number;
  type?: string;
  options?: { kind: string; from: string; to: string };
};

class MockWorker {
  static instances: MockWorker[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: { message: string }) => void) | null = null;
  messages: Job[] = [];
  constructor() {
    MockWorker.instances.push(this);
  }
  postMessage(message: Job) {
    this.messages.push(message);
  }
  terminate() {}
  emit(message: object) {
    this.onmessage?.({ data: message } as MessageEvent);
  }
  jobs() {
    return this.messages.filter((message) => message.type !== "cancel");
  }
  cancels() {
    return this.messages.filter((message) => message.type === "cancel");
  }
}

const range = { from: "2026-11-01T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };
const identity = { user: "u1", role: "admin" };
const result = (kind: string) => ({ kind, rows: [{ event_id: "a", timestamp: 1 }] });

beforeEach(() => {
  vi.resetModules();
  MockWorker.instances = [];
  vi.stubGlobal("Worker", MockWorker);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

async function load() {
  const client = await import("./client");
  const sources = await import("./sources");
  return { ...client, ...sources };
}

describe("readLogs request sharing", () => {
  it("sends one read for concurrent identical reads and gives every caller the result", async () => {
    const { readLogs } = await load();
    const reads = Array.from({ length: 4 }, () =>
      readLogs("cron_runs", range, identity),
    );
    const worker = MockWorker.instances[0];
    expect(worker.jobs()).toHaveLength(1);
    worker.emit({ id: worker.jobs()[0].id, type: "result", result: result("cron_runs") });
    for (const read of reads) {
      await expect(read).resolves.toEqual(result("cron_runs"));
    }
  });

  it("reads separately for a different kind, range or user", async () => {
    const { readLogs } = await load();
    void readLogs("cron_runs", range, identity);
    void readLogs("fos_usage", range, identity);
    void readLogs("cron_runs", { ...range, to: "2026-11-08T00:01:00.000Z" }, identity);
    void readLogs("cron_runs", range, { user: "u2", role: "admin" });
    expect(MockWorker.instances[0].jobs()).toHaveLength(4);
  });

  it("does not share reads across signed-in sessions", async () => {
    const { readLogs, syncOpsCacheForSession } = await load();
    await syncOpsCacheForSession({ userId: "alice" });
    void readLogs("cron_runs", range, identity);
    await syncOpsCacheForSession({ userId: "bob" });
    void readLogs("cron_runs", range, identity);
    expect(MockWorker.instances[0].jobs()).toHaveLength(2);
  });

  it("keeps the read going for the others when one caller aborts", async () => {
    const { readLogs } = await load();
    const leaving = new AbortController();
    const left = readLogs("cron_runs", range, { ...identity, signal: leaving.signal });
    const stays = readLogs("cron_runs", range, identity);
    const worker = MockWorker.instances[0];
    leaving.abort();
    await expect(left).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.cancels()).toEqual([]);
    worker.emit({ id: worker.jobs()[0].id, type: "result", result: result("cron_runs") });
    await expect(stays).resolves.toEqual(result("cron_runs"));
  });

  it("cancels the read once every caller has aborted, and the next caller starts a new one", async () => {
    const { readLogs } = await load();
    const first = new AbortController();
    const second = new AbortController();
    const a = readLogs("cron_runs", range, { ...identity, signal: first.signal });
    const b = readLogs("cron_runs", range, { ...identity, signal: second.signal });
    const worker = MockWorker.instances[0];
    first.abort();
    second.abort();
    await expect(a).rejects.toMatchObject({ name: "AbortError" });
    await expect(b).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.cancels()).toEqual([{ id: worker.jobs()[0].id, type: "cancel" }]);
    void readLogs("cron_runs", range, identity);
    expect(worker.jobs()).toHaveLength(2);
  });

  it("rejects every caller on failure and retries on the next call", async () => {
    const { readLogs } = await load();
    const reads = [readLogs("cron_runs", range, identity), readLogs("cron_runs", range, identity)];
    const worker = MockWorker.instances[0];
    worker.emit({ id: worker.jobs()[0].id, type: "error", message: "read failed" });
    for (const read of reads) await expect(read).rejects.toThrow("read failed");
    const retry = readLogs("cron_runs", range, identity);
    expect(worker.jobs()).toHaveLength(2);
    worker.emit({ id: worker.jobs()[1].id, type: "result", result: result("cron_runs") });
    await expect(retry).resolves.toEqual(result("cron_runs"));
  });

  it("reads again after the shared read has settled", async () => {
    const { readLogs } = await load();
    const first = readLogs("cron_runs", range, identity);
    const worker = MockWorker.instances[0];
    worker.emit({ id: worker.jobs()[0].id, type: "result", result: result("cron_runs") });
    await first;
    void readLogs("cron_runs", range, identity);
    expect(worker.jobs()).toHaveLength(2);
  });

  it("reports shared progress to every caller", async () => {
    const { readLogs } = await load();
    const one = vi.fn();
    const two = vi.fn();
    const a = readLogs({ kind: "cron_runs", ...range, onProgress: one });
    const b = readLogs({ kind: "cron_runs", ...range, onProgress: two });
    const worker = MockWorker.instances[0];
    expect(worker.jobs()).toHaveLength(1);
    const progress = {
      phase: "files", cached: 0, toFetch: 1, toDecode: 0, done: 1,
      downloadedBytes: 10, cachedBytes: 0,
    };
    const id = worker.jobs()[0].id;
    worker.emit({ id, type: "progress", progress });
    worker.emit({ id, type: "result", result: result("cron_runs") });
    await Promise.all([a, b]);
    expect(one).toHaveBeenCalledWith(progress);
    expect(two).toHaveBeenCalledWith(progress);
  });

  it("passes explicit non-shared identity to Worker, terminates Worker via resetOpsLogsClient, and fences in-flight ops CacheStorage writes across clearOpsCaches and setDashboardCacheIdentity", async () => {
    const bucketEntries = new Map<string, Map<string, Response>>();
    let putGateRelease: (() => void) | null = null;
    let putGateStarted: (() => void) | null = null;
    vi.stubGlobal("caches", {
      async open(name: string) {
        let store = bucketEntries.get(name);
        if (!store) {
          store = new Map();
          bucketEntries.set(name, store);
        }
        return {
          async match(req: RequestInfo | URL) {
            return store!.get(String(req))?.clone();
          },
          async put(req: RequestInfo | URL, res: Response) {
            if (putGateStarted) {
              const notify = putGateStarted;
              putGateStarted = null;
              notify();
              await new Promise<void>((r) => { putGateRelease = r; });
            }
            store!.set(String(req), res.clone());
          },
          async delete(req: RequestInfo | URL) {
            return store!.delete(String(req));
          },
          async keys() {
            return [...store!.keys()].map((u) => new Request(u));
          },
        };
      },
      async keys() {
        return [...bucketEntries.keys()];
      },
      async delete(name: string) {
        return bucketEntries.delete(name);
      },
    });

    const clientMod = await load();
    const { clearDashboardData, setDashboardCacheIdentity } = await import("../dashboard/client");
    const resetFn = (clientMod as Record<string, unknown>).resetOpsLogsClient as (() => void) | undefined;
    expect(typeof resetFn).toBe("function");

    // 1. Empty identity uses volatile per-epoch scope (NOT shared "default") and forwards identity in Worker message:
    const pendingWorkerRead = clientMod.readLogs({ kind: "cron_runs", ...range });
    const w1 = MockWorker.instances[0];
    const firstJob = w1.jobs()[0] as Job & { identity?: { user: string; role: string } };
    expect(firstJob.identity?.user).toBeTruthy();
    expect(firstJob.identity?.user).not.toBe("default");

    let terminated = false;
    w1.terminate = () => { terminated = true; };
    resetFn!();
    expect(terminated).toBe(true);
    await expect(pendingWorkerRead).rejects.toMatchObject({ name: "AbortError" });

    // 2. In-flight browserOpsSource.fetchFile paused before fetch settles across clearOpsCaches() must NOT re-create ops cache:
    let resolveOpsFetch!: (res: Response) => void;
    let opsSignalObserved: AbortSignal | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
        opsSignalObserved = init?.signal ?? undefined;
        return new Promise<Response>((resolve) => {
          resolveOpsFetch = resolve;
        });
      }),
    );

    await clientMod.syncOpsCacheForSession({ userId: "alice" });
    const opsSrc = clientMod.browserOpsSource("http://localhost");
    const inFlightFile = opsSrc
      .fetchFile("processed/ops/audit_log/part-1.parquet")
      .then(() => "RESOLVED", () => "REJECTED");

    await new Promise((r) => setTimeout(r, 5));
    await clientMod.clearOpsCaches();
    expect(opsSignalObserved?.aborted).toBe(true);
    resolveOpsFetch(new Response(new Uint8Array([1, 2, 3, 4]), { status: 200 }));
    expect(await inFlightFile).toBe("REJECTED");
    expect([...bucketEntries.keys()]).toEqual([]);

    // 3. Drain activeOpsWrites during clearOpsCaches() AND wipe ops caches during setDashboardCacheIdentity() & clearDashboardData():
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(new Uint8Array([9, 8, 7, 6]), { status: 200 })),
    );
    const putStartedPromise = new Promise<void>((r) => { putGateStarted = r; });
    const gatedFetch = opsSrc.fetchFile("processed/ops/audit_log/part-2.parquet");
    await putStartedPromise;
    let clearSettled = false;
    const clearPending = clientMod.clearOpsCaches().then(() => { clearSettled = true; });
    await new Promise((r) => setTimeout(r, 5));
    expect(clearSettled).toBe(false);
    (putGateRelease as (() => void) | null)?.();
    await clearPending;
    await gatedFetch.catch(() => {});
    expect([...bucketEntries.keys()]).toEqual([]);

    // Populate ops cache for Alice, then switch dashboard identity to Bob: must wipe Alice's ops cache!
    await setDashboardCacheIdentity("alice:admin");
    await clientMod.syncOpsCacheForSession({ userId: "alice" });
    const aliceOps = clientMod.browserOpsSource("http://localhost");
    await aliceOps.fetchFile("processed/ops/audit_log/part-3.parquet");
    expect([...bucketEntries.keys()]).toEqual(["flp-ops-parquet-v1:alice:admin"]);

    await setDashboardCacheIdentity("bob:analyst");
    expect([...bucketEntries.keys()]).toEqual([]);

    // Populate again, then clearDashboardData() must also wipe ops caches:
    await clientMod.syncOpsCacheForSession({ userId: "alice" });
    const aliceOps2 = clientMod.browserOpsSource("http://localhost");
    await aliceOps2.fetchFile("processed/ops/audit_log/part-4.parquet");
    expect([...bucketEntries.keys()]).toEqual(["flp-ops-parquet-v1:alice:admin"]);
    await clearDashboardData();
    expect([...bucketEntries.keys()]).toEqual([]);
  });
});
