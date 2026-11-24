// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { readLogs } from "./engine";
import {
  browserOpsSource,
  clearOpsCaches,
  syncOpsCacheForSession,
} from "./sources";

function installMemoryCaches() {
  const buckets = new Map<string, Map<string, Response>>();
  vi.stubGlobal("caches", {
    async open(name: string) {
      let store = buckets.get(name);
      if (!store) {
        store = new Map();
        buckets.set(name, store);
      }
      return {
        async match(req: RequestInfo | URL) {
          return store!.get(String(req))?.clone();
        },
        async put(req: RequestInfo | URL, res: Response) {
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
      return [...buckets.keys()];
    },
    async delete(name: string) {
      return buckets.delete(name);
    },
  });
  return buckets;
}

describe("browserOpsSource fetchFile deduplication", () => {
  beforeEach(async () => {
    installMemoryCaches();
    await clearOpsCaches();
    await syncOpsCacheForSession({ userId: "alice", role: "admin" });
  });

  afterEach(async () => {
    await clearOpsCaches();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("deduplicates concurrent cold-cache fetchFile calls across separate browserOpsSource instances", async () => {
    const payload = new Uint8Array([10, 20, 30, 40]);
    let resolveFetch!: (res: Response) => void;
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const key = "processed/ops/fos_usage/year=2026/month=11/day=24/window=1-2-a-n1.parquet";
    const src24h = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const src7d = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const src30d = browserOpsSource("http://localhost", { user: "alice", role: "admin" });

    const p1 = src24h.fetchFile(key);
    const p2 = src7d.fetchFile(key);
    const p3 = src30d.fetchFile(key);

    await new Promise((r) => setTimeout(r, 10));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    resolveFetch(
      new Response(payload, {
        status: 200,
        headers: { "x-flp-cdn-cache": "HIT" },
      }),
    );

    const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
    expect(new Uint8Array(r1.bytes)).toEqual(payload);
    expect(new Uint8Array(r2.bytes)).toEqual(payload);
    expect(new Uint8Array(r3.bytes)).toEqual(payload);
    expect(r1.bytes).not.toBe(r2.bytes);
    expect(r1.cached).toBe(false);
    expect(r1.cdn).toBe("HIT");
    expect(r2.cached).toBe(true);
    expect(r3.cached).toBe(true);

    // Subsequent sequential call hits CacheStorage without another network fetch.
    const r4 = await src24h.fetchFile(key);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(r4.cached).toBe(true);
    expect(new Uint8Array(r4.bytes)).toEqual(payload);
  });

  it("fetches overlapping Parquet files once during concurrent 24h, 7d, and 30d readLogs calls", async () => {
    const sharedKey =
      "processed/ops/fos_usage/year=2026/month=11/day=24/hour=00/window=100-104-a-n1.parquet";
    const weekKey =
      "processed/ops/fos_usage/year=2026/month=11/day=20/hour=00/window=200-204-b-n1.parquet";
    const monthKey =
      "processed/ops/fos_usage/year=2026/month=11/day=01/hour=00/window=300-304-c-n1.parquet";

    const makeParquet = (ts: number, id: string) =>
      parquetWriteBuffer({
        columnData: [
          { name: "timestamp", type: "TIMESTAMP", data: [new Date(ts)] },
          { name: "event_id", type: "STRING", data: [id] },
        ],
        codec: "SNAPPY",
      });

    const bytesByKey: Record<string, ArrayBuffer> = {
      [sharedKey]: await makeParquet(Date.parse("2026-11-24T00:30:00Z"), "e-shared"),
      [weekKey]: await makeParquet(Date.parse("2026-11-20T00:30:00Z"), "e-week"),
      [monthKey]: await makeParquet(Date.parse("2026-11-01T00:30:00Z"), "e-month"),
    };

    const fileFetchUrls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input));
        if (url.pathname.startsWith("/api/logs/")) {
          return new Response(JSON.stringify({ rows: [], next_cursor: null }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (url.pathname === "/api/files") {
          const from = url.searchParams.get("from")!;
          const files =
            from === "2026-11-23T00:00:00.000Z"
              ? [{ key: sharedKey, start: "2026-11-24T00:00:00Z", end: "2026-11-24T01:00:00Z", size: 1, rows: 1 }]
              : from === "2026-11-17T00:00:00.000Z"
                ? [
                    { key: sharedKey, start: "2026-11-24T00:00:00Z", end: "2026-11-24T01:00:00Z", size: 1, rows: 1 },
                    { key: weekKey, start: "2026-11-20T00:00:00Z", end: "2026-11-20T01:00:00Z", size: 1, rows: 1 },
                  ]
                : [
                    { key: sharedKey, start: "2026-11-24T00:00:00Z", end: "2026-11-24T01:00:00Z", size: 1, rows: 1 },
                    { key: weekKey, start: "2026-11-20T00:00:00Z", end: "2026-11-20T01:00:00Z", size: 1, rows: 1 },
                    { key: monthKey, start: "2026-11-01T00:00:00Z", end: "2026-11-01T01:00:00Z", size: 1, rows: 1 },
                  ];
          return new Response(
            JSON.stringify({ files, next_cursor: null, complete: true }),
            { status: 200, headers: { "content-type": "application/json" } },
          );
        }
        if (url.pathname === "/api/file") {
          const key = url.searchParams.get("key")!;
          fileFetchUrls.push(key);
          await new Promise((r) => setTimeout(r, 15));
          return new Response(bytesByKey[key], { status: 200 });
        }
        throw new Error(`Unexpected URL: ${url}`);
      }),
    );

    const id = { user: "alice", role: "admin" };
    const [r24h, r7d, r30d] = await Promise.all([
      readLogs(browserOpsSource("http://localhost", id), "fos_usage", {
        from: "2026-11-23T00:00:00.000Z",
        to: "2026-11-24T01:00:00.000Z",
      }),
      readLogs(browserOpsSource("http://localhost", id), "fos_usage", {
        from: "2026-11-17T00:00:00.000Z",
        to: "2026-11-24T01:00:00.000Z",
      }),
      readLogs(browserOpsSource("http://localhost", id), "fos_usage", {
        from: "2026-10-25T00:00:00.000Z",
        to: "2026-11-24T01:00:00.000Z",
      }),
    ]);

    expect(r24h.rows.map((r) => r.event_id)).toEqual(["e-shared"]);
    expect(r7d.rows.map((r) => r.event_id)).toEqual(["e-shared", "e-week"]);
    expect(r30d.rows.map((r) => r.event_id)).toEqual(["e-shared", "e-week", "e-month"]);
    expect(fileFetchUrls.sort()).toEqual([monthKey, sharedKey, weekKey].sort());
  });

  it("keeps the shared fetch alive when one caller aborts, and aborts it when all callers abort", async () => {
    let observedSignal: AbortSignal | undefined;
    let resolveFetch!: (res: Response) => void;
    const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      observedSignal = init?.signal ?? undefined;
      return new Promise<Response>((resolve) => {
        resolveFetch = resolve;
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const srcA = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const srcB = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const key = "processed/ops/cron_runs/part-abort.parquet";

    const ctrlA = new AbortController();
    const ctrlB = new AbortController();
    const pA = srcA.fetchFile(key, ctrlA.signal);
    const pB = srcB.fetchFile(key, ctrlB.signal);

    await new Promise((r) => setTimeout(r, 10));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    ctrlA.abort();
    await expect(pA).rejects.toMatchObject({ name: "AbortError" });
    expect(observedSignal?.aborted).toBe(false);

    resolveFetch(new Response(new Uint8Array([7, 8]), { status: 200 }));
    const resB = await pB;
    expect(new Uint8Array(resB.bytes)).toEqual(new Uint8Array([7, 8]));

    // Now test all callers aborting.
    const key2 = "processed/ops/cron_runs/part-abort-all.parquet";
    const ctrlC = new AbortController();
    const ctrlD = new AbortController();
    const pC = srcA.fetchFile(key2, ctrlC.signal);
    const pD = srcB.fetchFile(key2, ctrlD.signal);
    await new Promise((r) => setTimeout(r, 10));
    ctrlC.abort();
    ctrlD.abort();
    await expect(pC).rejects.toMatchObject({ name: "AbortError" });
    await expect(pD).rejects.toMatchObject({ name: "AbortError" });
    expect(observedSignal?.aborted).toBe(true);
  });

  it("does not share in-flight fetches across different user/role identities", async () => {
    const resolvers: Array<(res: Response) => void> = [];
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolvers.push(resolve);
        }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const key = "processed/ops/fos_usage/part-identity.parquet";
    const aliceSrc = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const bobSrc = browserOpsSource("http://localhost", { user: "bob", role: "analyst" });

    const pAlice = aliceSrc.fetchFile(key);
    const pBob = bobSrc.fetchFile(key);
    await new Promise((r) => setTimeout(r, 10));

    expect(fetchMock).toHaveBeenCalledTimes(2);
    resolvers[0](new Response(new Uint8Array([1]), { status: 200 }));
    resolvers[1](new Response(new Uint8Array([2]), { status: 200 }));
    const [rAlice, rBob] = await Promise.all([pAlice, pBob]);
    expect(new Uint8Array(rAlice.bytes)).toEqual(new Uint8Array([1]));
    expect(new Uint8Array(rBob.bytes)).toEqual(new Uint8Array([2]));
  });

  it("aborts shared in-flight fetches on clearOpsCaches and does not cache failures", async () => {
    let observedSignal: AbortSignal | undefined;
    let resolveFirst!: (res: Response) => void;
    const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      observedSignal = init?.signal ?? undefined;
      return new Promise<Response>((resolve) => {
        resolveFirst = resolve;
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const key = "processed/ops/ingest_files/part-clear.parquet";
    const srcA = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const srcB = browserOpsSource("http://localhost", { user: "alice", role: "admin" });

    const pA = srcA.fetchFile(key);
    const pB = srcB.fetchFile(key);
    await new Promise((r) => setTimeout(r, 10));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await clearOpsCaches();
    expect(observedSignal?.aborted).toBe(true);
    resolveFirst(new Response(new Uint8Array([1, 2, 3]), { status: 200 }));
    await expect(pA).rejects.toMatchObject({ name: "AbortError" });
    await expect(pB).rejects.toMatchObject({ name: "AbortError" });

    // Failure is not cached: a failed fetch rejects all waiters, and retry fetches again.
    let attempt = 0;
    const retryMock = vi.fn(async () => {
      attempt += 1;
      if (attempt === 1) return new Response("boom", { status: 500 });
      return new Response(new Uint8Array([42]), { status: 200 });
    });
    vi.stubGlobal("fetch", retryMock);
    await syncOpsCacheForSession({ userId: "alice", role: "admin" });

    const srcC = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const srcD = browserOpsSource("http://localhost", { user: "alice", role: "admin" });
    const [f1, f2] = await Promise.allSettled([
      srcC.fetchFile(key),
      srcD.fetchFile(key),
    ]);
    expect(f1.status).toBe("rejected");
    expect(f2.status).toBe("rejected");
    expect(retryMock).toHaveBeenCalledTimes(1);

    const ok = await srcC.fetchFile(key);
    expect(retryMock).toHaveBeenCalledTimes(2);
    expect(new Uint8Array(ok.bytes)).toEqual(new Uint8Array([42]));
  });
});
