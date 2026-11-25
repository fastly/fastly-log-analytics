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

  it("memoizes settled reads within the short TTL and reads again after TTL expiry or cache clear", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(Date.parse("2026-11-08T00:00:00.000Z"));
    try {
      const { readLogs } = await load();
      const { clearOpsCaches } = await import("./sources");
      const first = readLogs("cron_runs", range, identity);
      const worker = MockWorker.instances[0];
      worker.emit({ id: worker.jobs()[0].id, type: "result", result: result("cron_runs") });
      await expect(first).resolves.toEqual(result("cron_runs"));

      // Within 15s TTL: served from settled cache without starting a second worker job.
      await expect(readLogs("cron_runs", range, identity)).resolves.toEqual(result("cron_runs"));
      expect(worker.jobs()).toHaveLength(1);

      // After TTL expires: dispatches a fresh worker job.
      vi.advanceTimersByTime(15_001);
      const second = readLogs("cron_runs", range, identity);
      expect(worker.jobs()).toHaveLength(2);
      worker.emit({ id: worker.jobs()[1].id, type: "result", result: result("cron_runs") });
      await expect(second).resolves.toEqual(result("cron_runs"));

      // clearOpsCaches() invalidates the settled cache immediately.
      await clearOpsCaches();
      const third = readLogs("cron_runs", range, identity);
      expect(worker.jobs()).toHaveLength(3);
      worker.emit({ id: worker.jobs()[2].id, type: "result", result: result("cron_runs") });
      await expect(third).resolves.toEqual(result("cron_runs"));
    } finally {
      vi.useRealTimers();
    }
  });

  it("slices narrower in-flight and settled subset windows from a covering superset read", async () => {
    const { readLogs } = await load();
    const wide = { from: "2026-10-08T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };
    const day = { from: "2026-11-07T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };
    const week = { from: "2026-11-01T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };

    const wideRead = readLogs("fos_usage", wide, identity);
    const dayRead = readLogs("fos_usage", day, identity);
    const worker = MockWorker.instances[0];
    // Concurrent 24h read attaches to the in-flight 31d read.
    expect(worker.jobs()).toHaveLength(1);

    const oldRow = { event_id: "old", timestamp: Date.parse("2026-10-15T12:00:00.000Z") };
    const weekRow = { event_id: "week", timestamp: Date.parse("2026-11-03T12:00:00.000Z") };
    const dayRow = { event_id: "day", timestamp: Date.parse("2026-11-07T12:00:00.000Z") };
    worker.emit({
      id: worker.jobs()[0].id,
      type: "result",
      result: {
        kind: "fos_usage",
        from: wide.from,
        to: wide.to,
        rows: [oldRow, weekRow, dayRow],
        liveTruncated: false,
      },
    });

    await expect(wideRead).resolves.toMatchObject({ rows: [oldRow, weekRow, dayRow] });
    await expect(dayRead).resolves.toMatchObject({
      from: day.from,
      to: day.to,
      rows: [dayRow],
    });

    // Settled 31d read also serves a subsequent 7d read with zero new worker jobs.
    const weekRead = await readLogs("fos_usage", week, identity);
    expect(worker.jobs()).toHaveLength(1);
    expect(weekRead).toMatchObject({
      from: week.from,
      to: week.to,
      rows: [weekRow, dayRow],
    });
  });

  it("falls back to a fresh read when a covering superset read was truncated", async () => {
    const { readLogs } = await load();
    const wide = { from: "2026-10-08T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };
    const day = { from: "2026-11-07T00:00:00.000Z", to: "2026-11-08T00:00:00.000Z" };

    const wideRead = readLogs("fos_usage", wide, identity);
    const dayRead = readLogs("fos_usage", day, identity);
    const worker = MockWorker.instances[0];
    expect(worker.jobs()).toHaveLength(1);

    worker.emit({
      id: worker.jobs()[0].id,
      type: "result",
      result: {
        kind: "fos_usage",
        from: wide.from,
        to: wide.to,
        rows: [],
        liveTruncated: true,
      },
    });

    await expect(wideRead).resolves.toMatchObject({ liveTruncated: true });
    // Wait a microtask for dayRead to launch its fallback job.
    await Promise.resolve();
    expect(worker.jobs()).toHaveLength(2);
    expect(worker.jobs()[1].options).toMatchObject({ kind: "fos_usage", ...day });

    const dayRow = { event_id: "day", timestamp: Date.parse("2026-11-07T12:00:00.000Z") };
    worker.emit({
      id: worker.jobs()[1].id,
      type: "result",
      result: {
        kind: "fos_usage",
        from: day.from,
        to: day.to,
        rows: [dayRow],
        liveTruncated: false,
      },
    });
    await expect(dayRead).resolves.toMatchObject({
      liveTruncated: false,
      rows: [dayRow],
    });
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

  it("resets activeOpsUser to null on full clearOpsCaches() while preserving non-matching user wipe", async () => {
    const sourcesMod = await import("./sources");
    await sourcesMod.syncOpsCacheForSession({ userId: "alice", needsLogin: false });
    expect(sourcesMod.activeOpsUser()).toBe("alice");

    await sourcesMod.clearOpsCaches({ user: "bob" });
    expect(sourcesMod.activeOpsUser()).toBe("alice");

    await sourcesMod.clearOpsCaches();
    expect(sourcesMod.activeOpsUser()).toBeNull();
  });

  it("uses volatile-ops scope when dashboardCacheIdentity is anonymous and normalizes user:role in Worker context when dashId is null", async () => {
    const bucketEntries = new Map<string, Map<string, Response>>();
    vi.stubGlobal("caches", {
      async open(name: string) {
        let store = bucketEntries.get(name);
        if (!store) {
          store = new Map();
          bucketEntries.set(name, store);
        }
        return {
          async match(req: RequestInfo | URL) { return store!.get(String(req))?.clone(); },
          async put(req: RequestInfo | URL, res: Response) { store!.set(String(req), res.clone()); },
          async delete(req: RequestInfo | URL) { return store!.delete(String(req)); },
          async keys() { return [...store!.keys()].map((u) => new Request(u)); },
        };
      },
      async keys() { return [...bucketEntries.keys()]; },
      async delete(name: string) { return bucketEntries.delete(name); },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(new Uint8Array([1, 2, 3, 4]), { status: 200 })),
    );

    const clientMod = await load();
    const { clearDashboardData, setDashboardCacheIdentity } = await import("../dashboard/client");

    // 1. After unauthenticated bootstrap, dashboardCacheIdentity() === "anonymous" and activeOpsUser() === null:
    await setDashboardCacheIdentity("anonymous");
    await clientMod.clearOpsCaches();

    const pendingAnonRead = clientMod.readLogs("cron_runs", range);
    const worker = MockWorker.instances[0];
    const anonJob = worker.jobs()[0] as Job & { identity?: { user: string; role: string } };
    expect(anonJob.identity?.user).toMatch(/^volatile-ops-\d+$/);
    expect(anonJob.identity?.user).not.toBe("anonymous");
    worker.emit({ id: anonJob.id, type: "result", result: result("cron_runs") });
    await pendingAnonRead;

    // 2. Explicit "anonymous" is treated as unauthenticated and falls back to volatile-ops:
    expect(clientMod.currentOpsIdentity({ user: "anonymous", role: "admin" }).user).toMatch(
      /^volatile-ops-\d+$/,
    );

    // 3. Inside Web Worker (where localStorage / dashId is null and activeOpsUser is null),
    // currentOpsIdentity and browserOpsSource split "alice:admin" when role is omitted,
    // while preserving colon-bearing user.id values ("org:alice", "urn:fastly:user:alice", "org:admin") across hops!
    await clearDashboardData();
    await clientMod.clearOpsCaches();
    expect(clientMod.currentOpsIdentity({ user: "alice:admin" })).toEqual({
      user: "alice",
      role: "admin",
    });
    expect(clientMod.currentOpsIdentity({ user: "alice:analyst-no-pii" })).toEqual({
      user: "alice",
      role: "analyst-no-pii",
    });
    expect(clientMod.currentOpsIdentity({ user: "alice:analyst_masked" })).toEqual({
      user: "alice",
      role: "analyst-no-pii",
    });
    const workerSrc = clientMod.browserOpsSource("http://localhost", {
      user: "alice:admin",
    } as unknown as { user: string; role: string });
    await workerSrc.fetchFile("processed/ops/audit_log/part-worker.parquet");
    expect([...bucketEntries.keys()]).toEqual(["flp-ops-parquet-v1:alice:admin"]);

    // 4. BUG-T5-ITER25-01: Colon-bearing user.id values must NOT collide, lose segments across hops, or drop -no-pii!
    bucketEntries.clear();
    await setDashboardCacheIdentity("org:alice:analyst-no-pii");
    await clientMod.syncOpsCacheForSession({
      userId: "org:alice",
      role: "analyst",
      maskIps: true,
      needsLogin: false,
    });
    const hop0Alice = clientMod.currentOpsIdentity();
    expect(hop0Alice).toEqual({ user: "org:alice", role: "analyst-no-pii" });
    const hop1Alice = clientMod.currentOpsIdentity(hop0Alice);
    expect(hop1Alice).toEqual({ user: "org:alice", role: "analyst-no-pii" });
    expect(clientMod.currentOpsIdentity({ user: "org:alice" })).toEqual({
      user: "org:alice",
      role: "analyst-no-pii",
    });
    const aliceColonSrc = clientMod.browserOpsSource("http://localhost", hop0Alice);
    await aliceColonSrc.fetchFile("processed/ops/audit_log/part-alice.parquet");

    await setDashboardCacheIdentity("org:bob:analyst-no-pii");
    await clientMod.syncOpsCacheForSession({
      userId: "org:bob",
      role: "analyst",
      maskIps: true,
      needsLogin: false,
    });
    const bobColonSrc = clientMod.browserOpsSource("http://localhost", clientMod.currentOpsIdentity());
    await bobColonSrc.fetchFile("processed/ops/audit_log/part-bob.parquet");
    expect([...bucketEntries.keys()]).toEqual(["flp-ops-parquet-v1:org:bob:analyst-no-pii"]);

    // Multi-colon URN user.id across multiple hops + user-scoped clearOpsCaches leaves 0 orphaned caches:
    bucketEntries.clear();
    await setDashboardCacheIdentity("urn:fastly:user:alice:admin");
    await clientMod.syncOpsCacheForSession({
      userId: "urn:fastly:user:alice",
      role: "admin",
      needsLogin: false,
    });
    const urnHop0 = clientMod.currentOpsIdentity();
    const urnHop1 = clientMod.currentOpsIdentity(urnHop0);
    const urnHop2 = clientMod.currentOpsIdentity(urnHop1);
    expect(urnHop2).toEqual({ user: "urn:fastly:user:alice", role: "admin" });
    const urnSrc = clientMod.browserOpsSource("http://localhost", urnHop2);
    await urnSrc.fetchFile("processed/ops/audit_log/part-urn.parquet");
    expect([...bucketEntries.keys()]).toEqual(["flp-ops-parquet-v1:urn:fastly:user:alice:admin"]);
    await clientMod.clearOpsCaches({ user: "urn:fastly:user:alice" });
    expect([...bucketEntries.keys()]).toEqual([]);

    // Colon-bearing user whose suffix happens to be "admin" ("org:admin") keeps full user when role is supplied,
    // AND when signed-in as "org:admin" with role "analyst-no-pii", passing { user: "org:admin" } without role
    // preserves { user: "org:admin", role: "analyst-no-pii" } instead of splitting into { user: "org", role: "admin" }!
    expect(clientMod.currentOpsIdentity({ user: "org:admin", role: "analyst-no-pii" })).toEqual({
      user: "org:admin",
      role: "analyst-no-pii",
    });
    expect(clientMod.currentOpsIdentity({ user: "anonymous:guest1" }).user).toBe("anonymous:guest1");

    await setDashboardCacheIdentity("org:admin:analyst-no-pii");
    await clientMod.syncOpsCacheForSession({
      userId: "org:admin",
      role: "analyst",
      maskIps: true,
      needsLogin: false,
    });
    expect(clientMod.currentOpsIdentity()).toEqual({
      user: "org:admin",
      role: "analyst-no-pii",
    });
    expect(clientMod.currentOpsIdentity({ user: "org:admin" })).toEqual({
      user: "org:admin",
      role: "analyst-no-pii",
    });

    // BUG-T5-ITER26-02 (dashboardRoleForUser): colon-bearing dashId without a known role suffix ("org:alice")
    // must NOT treat "alice" as user "org"'s role, and ":analyst_masked" must normalize to "analyst-no-pii":
    await clientMod.clearOpsCaches();
    await setDashboardCacheIdentity("org:alice");
    await clientMod.syncOpsCacheForSession({
      userId: "org",
      needsLogin: false,
    });
    expect(clientMod.currentOpsIdentity()).toEqual({
      user: "org",
      role: "admin",
    });

    await clientMod.clearOpsCaches();
    await setDashboardCacheIdentity("u1:analyst_masked");
    await clientMod.syncOpsCacheForSession({
      userId: "u1",
      needsLogin: false,
    });
    expect(clientMod.currentOpsIdentity()).toEqual({
      user: "u1",
      role: "analyst-no-pii",
    });
  });

  it("does not coalesce latestCompleteSnapshotOnly reads with full storage_inventory reads", async () => {
    const { readLogs } = await load();
    const p1 = readLogs("storage_inventory", range, {
      latestCompleteSnapshotOnly: true,
    });
    const p2 = readLogs("storage_inventory", range);
    const w = MockWorker.instances[0];
    expect(w.jobs()).toHaveLength(2);
    expect(
      (w.jobs()[0].options as { latestCompleteSnapshotOnly?: boolean })
        .latestCompleteSnapshotOnly,
    ).toBe(true);
    expect(
      (w.jobs()[1].options as { latestCompleteSnapshotOnly?: boolean })
        .latestCompleteSnapshotOnly,
    ).toBeUndefined();
    const emptyResult = {
      rows: [],
      stats: {
        listed: 0,
        read: 0,
        cached: 0,
        cdnHits: 0,
        skippedCovered: 0,
        ignoredCompact: 0,
        evicted: 0,
        liveRows: 0,
      },
      liveStatus: null,
      liveTruncated: false,
    };
    w.emit({ id: w.jobs()[0].id, type: "result", result: emptyResult });
    w.emit({ id: w.jobs()[1].id, type: "result", result: emptyResult });
    await Promise.all([p1, p2]);
  });

  it("shares projected reads safely: projected never satisfies full/superset, while full satisfies projected", async () => {
    const { readLogs } = await load();
    const workerRes = (kind: string) => ({
      kind,
      from: range.from,
      to: range.to,
      rows: [{ event_id: "e1", timestamp: Date.parse(range.from) + 1000, raw_key: "k1" }],
      liveTruncated: false,
    });

    // 1. Two identical projected reads share one worker job and forward columns/dedupe/sort.
    const p1 = readLogs("ingest_files", range, {
      ...identity,
      columns: ["raw_key", "timestamp"],
      dedupe: false,
      sort: false,
    });
    const p2 = readLogs("ingest_files", range, {
      ...identity,
      columns: ["timestamp", "raw_key"],
      dedupe: false,
      sort: false,
    });
    const worker = MockWorker.instances[0];
    expect(worker.jobs()).toHaveLength(1);
    expect(worker.jobs()[0].options).toMatchObject({
      kind: "ingest_files",
      columns: ["raw_key", "timestamp"],
      dedupe: false,
      sort: false,
    });

    // 2. A concurrent full read must NOT attach to the projected/non-deduped read.
    const fullRead = readLogs("ingest_files", range, identity);
    expect(worker.jobs()).toHaveLength(2);

    // 3. A concurrent projected read asking for a different column subset DOES attach to the full read.
    const otherProj = readLogs("ingest_files", range, {
      ...identity,
      columns: ["timestamp", "raw_size"],
      dedupe: false,
      sort: false,
    });
    expect(worker.jobs()).toHaveLength(2);

    worker.emit({ id: worker.jobs()[0].id, type: "result", result: workerRes("ingest_files") });
    worker.emit({ id: worker.jobs()[1].id, type: "result", result: workerRes("ingest_files") });
    await Promise.all([p1, p2, fullRead, otherProj]);

    // 4. After settling, another projected read hits the settled full read (0 new jobs).
    await readLogs("ingest_files", range, {
      ...identity,
      columns: ["timestamp", "status"],
      dedupe: false,
      sort: false,
    });
    expect(worker.jobs()).toHaveLength(2);
  });
});
