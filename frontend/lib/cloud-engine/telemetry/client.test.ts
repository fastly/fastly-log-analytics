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
});
