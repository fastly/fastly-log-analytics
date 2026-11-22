// @vitest-environment node
import { describe, expect, it } from "vitest";
import { FilteredResultCache, type FilteredFileResult } from "./filtered-cache";

class FakeCache {
  readonly entries = new Map<string, { body: ArrayBuffer; headers: Headers }>();

  async match(request: RequestInfo | URL) {
    const hit = this.entries.get(String(request));
    return hit
      ? new Response(hit.body.slice(0), { headers: hit.headers })
      : undefined;
  }

  async put(request: RequestInfo | URL, response: Response) {
    this.entries.set(String(request), {
      body: await response.arrayBuffer(),
      headers: new Headers(response.headers),
    });
  }

  async delete(request: RequestInfo | URL) {
    return this.entries.delete(String(request));
  }

  async keys() {
    return [...this.entries.keys()].map((url) => new Request(url));
  }
}

const result = (requests: number, host = "a.example"): FilteredFileResult => ({
  data: {
    minTs: 60_000,
    maxTs: 119_000,
    totalRequests: requests,
    hasBytes: true,
    buckets: [
      { t: 60_000, requests, bytes: 10 * requests, s4xx: 0, s5xx: 0, hits: requests, cacheRows: requests },
    ],
    counts: { host: [[host, requests]] },
    nonNull: { host: requests },
  },
  truncated: requests > 2 ? ["host"] : [],
  rawRows: requests,
  byteLength: 1000,
});

function store(
  cache: FakeCache,
  options: Partial<ConstructorParameters<typeof FilteredResultCache>[0]> = {},
) {
  return new FilteredResultCache({
    baseUrl: "https://app.example",
    scope: "svc",
    openCache: async () => cache as unknown as Cache,
    memoryBytes: 0,
    ...options,
  });
}

describe("FilteredResultCache", () => {
  it("persists results across instances and never answers another id", async () => {
    const cache = new FakeCache();
    const first = store(cache);
    await first.write("file-a|q1", result(3));
    await first.flush();
    const second = store(cache);
    expect(await second.read("file-a|q1")).toEqual(result(3));
    expect(await second.read("file-a|q2")).toBeNull();
  });

  it("rejects an entry stored under a colliding hash for another id", async () => {
    const cache = new FakeCache();
    const collide = store(cache, { hash: async () => "same" });
    await collide.write("file-a|filter=US", result(1));
    expect(await collide.read("file-a|filter=DE")).toBeNull();
    expect(await collide.read("file-a|filter=US")).toEqual(result(1));
  });

  it("evicts the least recently used entries past the byte budget", async () => {
    const cache = new FakeCache();
    let clock = 1;
    const probe = store(new FakeCache());
    await probe.write("x", result(1, "a"));
    const size = probe.persistedBytes();
    const lru = store(cache, { maxBytes: size * 2 + 1, now: () => clock++ });
    await lru.write("a", result(1, "a"));
    await lru.write("b", result(1, "b"));
    expect(await lru.read("a")).not.toBeNull();
    await lru.write("c", result(1, "c"));
    expect(await lru.read("b")).toBeNull();
    expect(await lru.read("a")).not.toBeNull();
    expect(await lru.read("c")).not.toBeNull();
    expect(lru.persistedBytes()).toBeLessThanOrEqual(size * 2 + 1);
  });

  it("bounds the entry count", async () => {
    const cache = new FakeCache();
    let clock = 1;
    const bounded = store(cache, { maxEntries: 2, now: () => clock++ });
    for (const id of ["a", "b", "c"]) await bounded.write(id, result(1, id));
    expect(await bounded.read("a")).toBeNull();
    expect(await bounded.read("b")).not.toBeNull();
    expect(await bounded.read("c")).not.toBeNull();
    expect(cache.entries.size).toBeLessThanOrEqual(3);
  });

  it("drops entries unused for longer than the age limit", async () => {
    const cache = new FakeCache();
    let clock = 1_000;
    const aged = store(cache, { maxAgeMs: 500, now: () => clock });
    await aged.write("old", result(1));
    clock = 1_400;
    expect(await aged.read("old")).not.toBeNull();
    clock = 2_000;
    expect(await aged.read("old")).toBeNull();
    await aged.write("new", result(2));
    expect(await aged.read("new")).not.toBeNull();
  });

  it("adopts entries whose index flush was lost so the budget still counts them", async () => {
    const cache = new FakeCache();
    const lost = store(cache);
    await lost.write("a", result(1));
    const reopened = store(cache);
    expect(await reopened.read("a")).toEqual(result(1));
    expect(reopened.persistedBytes()).toBeGreaterThan(0);
  });

  it("serves repeat reads from memory without touching the cache", async () => {
    const cache = new FakeCache();
    const memory = store(cache, { memoryBytes: 1024 * 1024 });
    await memory.write("a", result(1));
    cache.entries.clear();
    expect(await memory.read("a")).toEqual(result(1));
  });

  it("suppresses pending write and debounced index flush when clearMemory runs mid-flight on a standalone instance", async () => {
    const cache = new FakeCache();
    let releaseHash: () => void = () => {};
    let notifyHashStarted: () => void = () => {};
    let pauseNextHash = false;

    const standalone = store(cache, {
      memoryBytes: 1024 * 1024,
      hash: async (id) => {
        if (pauseNextHash) {
          pauseNextHash = false;
          notifyHashStarted?.();
          await new Promise<void>((r) => {
            releaseHash = r;
          });
        }
        return `hash-${id}`;
      },
    });

    // 1. Completed write schedules a debounced flushTimer; clearMemory() must cancel the timer before it flushes the index
    await standalone.write("pre-logout", result(2, "secret.example"));
    cache.entries.clear();
    standalone.clearMemory();
    await standalone.drain();
    expect(cache.entries.size).toBe(0);
    expect(await standalone.read("pre-logout")).toBeNull();

    // 2. Mid-flight write() paused at async boundary when clearMemory() runs must NOT put entry or index into CacheStorage
    pauseNextHash = true;
    const hashStarted = new Promise<void>((r) => {
      notifyHashStarted = r;
    });
    const inFlightWrite = standalone.write("mid-logout", result(5, "alice.example"));
    await hashStarted;
    standalone.clearMemory();
    releaseHash?.();
    await inFlightWrite;
    await standalone.drain();
    expect(cache.entries.size).toBe(0);
    expect(standalone.persistedBytes()).toBe(0);
    expect(await standalone.read("mid-logout")).toBeNull();
  });
});
