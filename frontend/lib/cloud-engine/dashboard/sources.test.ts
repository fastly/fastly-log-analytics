// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { CARD_FIELDS } from "./summaries";
import { estimateRollupBytes } from "./rollup-codec";
import {
  CACHE_NAME,
  ROLLUP_CACHE_NAME,
  ROLLUP_CACHE_VERSION,
  ROLLUP_SEMANTICS,
  browserSource,
  clearRequestLogCaches,
  dashboardCacheIdentity,
  dashboardCacheScope,
  keyFromFileUrl,
  pruneRequestCache,
  rollupCacheVersion,
  updateDashboardCacheIdentity,
} from "./sources";
import { FILTERED_CACHE_NAME } from "./filtered-cache";
import {
  STORE_META_CACHE,
  readStoreError,
  recordStoreError,
} from "./file-store";

afterEach(() => vi.unstubAllGlobals());

describe("browserSource", () => {
  it("keeps a field search bound to its service in manifest requests and cache identity", async () => {
    const fetcher = vi.fn(async () => Response.json({ files: [], complete: true }));
    vi.stubGlobal("fetch", fetcher);
    const source = browserSource("").withOptions({ serviceId: "search-service" });
    await source.fetchJson("/api/files?from=2026-10-29&to=2026-10-30");
    const [path, init] = fetcher.mock.calls[0] as unknown as [string, RequestInit];
    expect(new URL(path, "http://localhost").searchParams.get("service_id")).toBe("search-service");
    expect(new Headers(init.headers).get("x-service-id")).toBe("search-service");
    expect(source.fetchJson.manifestCacheScope?.()).not.toBe(
      browserSource("").withOptions({ serviceId: "other-service" }).fetchJson.manifestCacheScope?.(),
    );
    expect(source.cacheScope).not.toBe(browserSource("").withOptions({ serviceId: "other-service" }).cacheScope);
  });

  it("reads x-flp-cdn-cache on network fetches", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(new Uint8Array([1]), {
            headers: { "x-flp-cdn-cache": "HIT" },
          }),
      ),
    );
    const file = await browserSource("").fetchFile("k");
    expect(file).toMatchObject({ cached: false, cdn: "HIT" });
  });

  it("reads one exact byte range of an immutable file", async () => {
    const body = new Uint8Array([7, 8, 9]);
    const fetcher = vi.fn(async (_url: string, _init?: RequestInit) => new Response(body, {
      status: 206,
      headers: { "content-range": "bytes 10-12/100", etag: "\"e1\"", "x-flp-cdn-cache": "HIT" },
    }));
    vi.stubGlobal("fetch", fetcher);
    const calls: unknown[] = [];
    const source = browserSource("", { serviceId: "svc", onApiCall: (call) => calls.push(call) });
    const file = { key: "flp/processed/request/year=2026/a.parquet", start: "", end: "", size: 100, rows: 1, etag: "\"e1\"" };
    expect(new Uint8Array(await source.fetchRange(file, 10, 13))).toEqual(body);
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("/fos/flp/processed/request/year=2026/a.parquet");
    expect(new Headers(init!.headers).get("range")).toBe("bytes=10-12");
    expect(new Headers(init!.headers).get("x-service-id")).toBe("svc");
    expect(calls).toEqual([expect.objectContaining({ status: 206, bytes: 3, cache: "cdn" })]);
    expect(init!.priority).toBeUndefined();
    await source.fetchRange(file, 10, 13, undefined, "high");
    expect(fetcher.mock.calls[1][1]!.priority).toBe("high");

    for (const [status, range, etag, bytes] of [
      [200, "bytes 10-12/100", "\"e1\"", 3],
      [206, "bytes 10-12/101", "\"e1\"", 3],
      [206, "bytes 0-2/100", "\"e1\"", 3],
      [206, "bytes 10-12/100", "\"e2\"", 3],
      [206, "bytes 10-12/100", "\"e1\"", 2],
    ] as const) {
      fetcher.mockResolvedValueOnce(new Response(new Uint8Array(bytes), {
        status, headers: { "content-range": range, etag },
      }));
      await expect(source.fetchRange(file, 10, 13)).rejects.toThrow(/a\.parquet/);
    }
    await expect(source.fetchRange(file, 10, 101)).rejects.toThrow(/outside/);
  });

  it("reports whether a file can be served without the network", async () => {
    const store = new Map<string, Response>([
      ["/api/file?key=on-disk", new Response(new Uint8Array([2]))],
    ]);
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async (req: string) => store.get(req)?.clone()),
        put: vi.fn(async () => {}),
      })),
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(new Uint8Array([1]))),
    );
    const source = browserSource("");
    expect(await source.hasFile!("on-disk")).toBe(true);
    expect(await source.hasFile!("in-memory")).toBe(false);
    await source.fetchFile("in-memory");
    expect(await source.hasFile!("in-memory")).toBe(true);
  });

  it("coalesces concurrent file misses and gives every waiter owned bytes", async () => {
    let resolveFetch!: (response: Response) => void;
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const telemetry: unknown[] = [];
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("", {
      onApiCall: (call) => telemetry.push(call),
    });

    const pending = Array.from({ length: 25 }, () =>
      source.fetchFile("shared", undefined, { transfer: true }),
    );
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    resolveFetch(new Response(new Uint8Array([10, 20, 30])));

    const files = await Promise.all(pending);
    expect(files.filter((file) => file.cached)).toHaveLength(24);
    expect(new Set(files.map((file) => file.bytes)).size).toBe(25);
    structuredClone(files[0].bytes, { transfer: [files[0].bytes] });
    expect(files[0].bytes.byteLength).toBe(0);
    expect(files.slice(1).every((file) => file.bytes.byteLength === 3)).toBe(true);
    expect(telemetry).toHaveLength(1);
  });

  it("coalesces staggered CacheStorage misses through persistence", async () => {
    let releaseCacheMiss!: () => void;
    let releaseStore!: () => void;
    const cacheMiss = new Promise<void>((resolve) => {
      releaseCacheMiss = resolve;
    });
    const stored = new Promise<void>((resolve) => {
      releaseStore = resolve;
    });
    let matchCount = 0;
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async () => {
          matchCount += 1;
          await cacheMiss;
          return undefined;
        }),
        put: vi.fn(async () => {
          await stored;
        }),
      })),
    });
    const fetchMock = vi.fn(
      async () => new Response(new Uint8Array([4, 5, 6])),
    );
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");

    const first = source.fetchFile("staggered");
    await vi.waitFor(() => expect(matchCount).toBe(1));
    const second = source.fetchFile("staggered");
    await Promise.resolve();
    await Promise.resolve();
    expect(matchCount).toBe(1);
    releaseCacheMiss();
    expect((await first).cached).toBe(false);
    expect((await second).cached).toBe(true);
    const third = source.fetchFile("staggered");

    expect((await third).cached).toBe(true);
    releaseStore();
    await stored;
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });

  it("keeps a shared file fetch alive when one waiter aborts", async () => {
    let resolveFetch!: (response: Response) => void;
    let networkSignal: AbortSignal | undefined;
    const fetchMock = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((resolve) => {
          networkSignal = init?.signal as AbortSignal | undefined;
          resolveFetch = resolve;
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const firstAbort = new AbortController();
    const source = browserSource("");

    const first = source.fetchFile("shared", firstAbort.signal);
    const second = source.fetchFile("shared");
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    firstAbort.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });
    expect(networkSignal?.aborted).toBe(false);

    resolveFetch(new Response(new Uint8Array([7])));
    expect(new Uint8Array((await second).bytes)).toEqual(new Uint8Array([7]));
  });

  it("aborts a shared file fetch when its last waiter aborts", async () => {
    let networkSignal: AbortSignal | undefined;
    const fetchMock = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          networkSignal = init?.signal as AbortSignal | undefined;
          networkSignal?.addEventListener("abort", () => {
            reject(networkSignal?.reason);
          });
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const abort = new AbortController();
    const pending = browserSource("").fetchFile("shared", abort.signal);

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    abort.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(networkSignal?.aborted).toBe(true);
  });

  it("starts fresh immediately after the last waiter aborts", async () => {
    const abortSignals: AbortSignal[] = [];
    const resolvers: ((response: Response) => void)[] = [];
    const fetchMock = vi.fn(
      (_url: string, init?: RequestInit) =>
        new Promise<Response>((resolve, reject) => {
          const signal = init?.signal as AbortSignal;
          abortSignals.push(signal);
          resolvers.push(resolve);
          signal.addEventListener("abort", () => reject(signal.reason));
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    const abort = new AbortController();

    const first = source.fetchFile("retry-now", abort.signal);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    abort.abort();
    const firstFailure = expect(first).rejects.toMatchObject({
      name: "AbortError",
    });
    const retry = source.fetchFile("retry-now");
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));

    await firstFailure;
    resolvers[1](new Response(new Uint8Array([8])));
    expect((await retry).cached).toBe(false);
    expect(abortSignals[0].aborted).toBe(true);
  });

  it("hands transferred bytes to the caller without keeping a memory copy", async () => {
    const fetchMock = vi.fn(async () => new Response(new Uint8Array([1])));
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    await source.fetchFile("kept");
    const owned = await source.fetchFile("kept", undefined, {
      transfer: true,
    });
    expect(owned.cached).toBe(true);
    expect(await source.hasFile!("kept")).toBe(false);
    await source.fetchFile("taken", undefined, { transfer: true });
    expect(await source.hasFile!("taken")).toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("persists transferred files to CacheStorage even when the caller detaches the buffer", async () => {
    const store = new Map<string, Response>();
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async (req: string) => store.get(req)?.clone()),
        delete: vi.fn(async (req: string) => store.delete(req)),
        put: vi.fn(async (req: string, res: Response) => {
          store.set(req, res.clone());
        }),
      })),
    });
    const fetchMock = vi.fn(
      async () => new Response(new Uint8Array([10, 20, 30, 40])),
    );
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    const first = await source.fetchFile("detached", undefined, {
      transfer: true,
    });
    // Simulate DuckDB Web Worker buffer transfer detaching `first.bytes` immediately.
    structuredClone(first.bytes, { transfer: [first.bytes] });
    expect(first.bytes.byteLength).toBe(0);
    // Wait for async CacheStorage write queue to flush, then verify a second fetch hits CacheStorage.
    await new Promise((r) => setTimeout(r, 10));
    const second = await source.fetchFile("detached", undefined, {
      transfer: true,
    });
    expect(second.cached).toBe(true);
    expect(new Uint8Array(second.bytes)).toEqual(
      new Uint8Array([10, 20, 30, 40]),
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("does not share an in-flight or cached file read across services", async () => {
    const pending = new Map<string, (response: Response) => void>();
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      const serviceId = new Headers(init?.headers).get("x-service-id") ?? "";
      return await new Promise<Response>((resolve) => pending.set(serviceId, resolve));
    });
    vi.stubGlobal("fetch", fetchMock);
    const base = browserSource("");
    const a = base.withOptions({ serviceId: "service-a" });
    const b = base.withOptions({ serviceId: "service-b" });

    const first = a.fetchFile("same-file");
    const second = b.fetchFile("same-file");
    await vi.waitFor(() => expect(pending.size).toBe(2));
    pending.get("service-a")!(new Response(new Uint8Array([1])));
    pending.get("service-b")!(new Response(new Uint8Array([2])));

    const [aFile, bFile] = await Promise.all([first, second]);
    expect(new Uint8Array(aFile.bytes)).toEqual(new Uint8Array([1]));
    expect(new Uint8Array(bFile.bytes)).toEqual(new Uint8Array([2]));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("reports telemetry per derived source while sharing its caches", async () => {
    const fetchMock = vi.fn<
      (url: string, init?: RequestInit) => Promise<Response>
    >(async () => new Response(new Uint8Array([1])));
    vi.stubGlobal("fetch", fetchMock);
    const base = browserSource("");
    const callsA: string[] = [];
    const callsB: string[] = [];
    const a = base.withOptions({
      getPageLoadId: () => "page-a",
      onApiCall: (call) => callsA.push(call.page_load_id ?? ""),
    });

    const b = base.withOptions({
      getPageLoadId: () => "page-b",
      onApiCall: (call) => callsB.push(call.page_load_id ?? ""),
    });
    await Promise.all([a.fetchFile("x"), b.fetchFile("y")]);
    const headers = fetchMock.mock.calls.map(
      ([, init]) => (init?.headers as Record<string, string>)["X-Page-Load-ID"],
    );
    expect(headers).toEqual(["page-a", "page-b"]);
    expect(callsA).toEqual(["page-a"]);
    expect(callsB).toEqual(["page-b"]);
    expect(await a.hasFile!("y")).toBe(true);
  });

  it("isolates manifest identities in memory and persistent file caches", async () => {
    const store = new Map<string, Response>();
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async (req: string) => store.get(req)?.clone()),
        delete: vi.fn(async (req: string) => store.delete(req)),
        put: vi.fn(async (req: string, res: Response) => {
          store.set(req, res.clone());
        }),
      })),
    });
    let version = 0;
    const fetchMock = vi.fn(async () => new Response(new Uint8Array([++version])));
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    const first = await source.fetchFile("replaced", undefined, { identity: "v1" });
    expect(new Uint8Array(first.bytes)).toEqual(new Uint8Array([1]));
    expect((await source.fetchFile("replaced", undefined, { identity: "v1" })).cached).toBe(true);
    const second = await source.fetchFile("replaced", undefined, { identity: "v2" });
    expect(new Uint8Array(second.bytes)).toEqual(new Uint8Array([2]));
    await new Promise((resolve) => setTimeout(resolve, 10));
    const previous = await source.fetchFile("replaced", undefined, { identity: "v1", transfer: true });
    expect(previous.cached).toBe(true);
    expect(new Uint8Array(previous.bytes)).toEqual(new Uint8Array([1]));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("reuses persisted range files from a new tab of the same signed-in identity", async () => {
    const storage = (values: Map<string, string>) => ({
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => void values.set(key, value),
      removeItem: (key: string) => void values.delete(key),
    });
    const shared = new Map<string, string>();
    const store = new Map<string, Response>();
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async (req: string) => store.get(req)?.clone()),
        delete: vi.fn(async (req: string) => store.delete(req)),
        put: vi.fn(async (req: string, res: Response) => {
          store.set(req, res.clone());
        }),
      })),
    });
    const fetchMock = vi.fn(async () => new Response(new Uint8Array([7])));
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("window", {});
    vi.stubGlobal("localStorage", storage(shared));
    const rangeIdentity = () => JSON.stringify([dashboardCacheScope(), "svc", "requests", "file-1", "etag", 1, ""]);

    vi.stubGlobal("sessionStorage", storage(new Map()));
    const coldTab = browserSource("");
    if (dashboardCacheIdentity() !== "user-1:admin") updateDashboardCacheIdentity("user-1:admin");
    expect((await coldTab.fetchFile("file-1", undefined, { identity: rangeIdentity() })).cached).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 10));

    vi.stubGlobal("sessionStorage", storage(new Map()));
    const warmTab = browserSource("");
    expect(dashboardCacheIdentity()).toBe("user-1:admin");
    expect((await warmTab.fetchFile("file-1", undefined, { identity: rangeIdentity() })).cached).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    updateDashboardCacheIdentity("user-2:viewer");
    expect((await browserSource("").fetchFile("file-1", undefined, { identity: rangeIdentity() })).cached).toBe(false);
  });

  it("persists derived files under their exact identity for the next page load, in the request-log cache", async () => {
    const store = new Map<string, Response>();
    const open = vi.fn(async () => ({
      match: vi.fn(async (req: string) => store.get(req)?.clone()),
      delete: vi.fn(async (req: string) => store.delete(req)),
      put: vi.fn(async (req: string, res: Response) => {
        store.set(req, res.clone());
      }),
    }));
    vi.stubGlobal("caches", { open });
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const key = "flp/processed/request/year=2026/a.parquet";
    const first = browserSource("");
    expect(await first.readDerivedFile(key, "projection-a")).toBeNull();
    await first.writeDerivedFile(key, "projection-a", new Uint8Array([1, 2, 3]).buffer, "[\"url\",\"ua\"]");

    const calls: unknown[] = [];
    const next = browserSource("", { onApiCall: (call) => calls.push(call) });
    const hit = await next.readDerivedFile(key, "projection-a");
    expect(hit).toMatchObject({ byteLength: 3, meta: "[\"url\",\"ua\"]" });
    expect(new Uint8Array(await hit!.bytes())).toEqual(new Uint8Array([1, 2, 3]));
    expect(await next.readDerivedFile(key, "projection-b")).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(calls).toEqual([expect.objectContaining({ source: "Browser Cache", bytes: 3, cache: "browser" })]);
    expect(open).toHaveBeenCalledWith(CACHE_NAME);
    expect([...store.keys()].map(keyFromFileUrl)).toEqual([key]);
  });

  it("posts one beacon and swallows failures", async () => {
    const fetchMock = vi.fn(async () => {
      throw new Error("offline");
    });
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      browserSource("").beacon!({ files_listed: 1 }),
    ).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe("/api/telemetry/query");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({ files_listed: 1 });
  });

  it("folds successful file fetches into the next beacon when asked", async () => {
    const fetchMock = vi.fn<
      (url: string, init?: RequestInit) => Promise<Response>
    >(async (url) =>
      url.includes("missing")
        ? new Response("", { status: 404 })
        : url.includes("/api/file")
          ? new Response(new Uint8Array([1]), {
              headers: { "x-flp-vcpu-ms": "4" },
            })
          : new Response(null, { status: 204 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const base = browserSource("", undefined, { foldFileTelemetry: true });
    const run = base.withOptions({ getPageLoadId: () => "page-a" });
    await run.fetchFile("a");
    await base.fetchFile("b");
    await run.fetchFile("a");
    await expect(run.fetchFile("missing")).rejects.toThrow();
    const fold = fetchMock.mock.calls
      .filter(([url]) => url.includes("/api/file"))
      .map(
        ([, init]) =>
          (init?.headers as Record<string, string>)["X-FLP-Fold-Telemetry"],
      );
    expect(fold).toEqual(["1", "1", "1"]);
    await run.beacon!({ files_fetched: 2 });
    await run.beacon!({ files_fetched: 0 });
    const bodies = fetchMock.mock.calls
      .filter(([url]) => url.includes("/api/telemetry/query"))
      .map(([, init]) => JSON.parse(init?.body as string));
    expect(bodies).toEqual([
      { files_fetched: 2, compute_requests: 2, vcpu_ms: 8 },
      { files_fetched: 0 },
    ]);
  });

  it("leaves file fetches unflagged by default", async () => {
    const fetchMock = vi.fn<
      (url: string, init?: RequestInit) => Promise<Response>
    >(async () => new Response(new Uint8Array([1])));
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    await source.fetchFile("a");
    await source.beacon!({ files_fetched: 1 });
    const [[, fileInit], [, beaconInit]] = fetchMock.mock.calls;
    expect(fileInit?.headers).toBeUndefined();
    expect(JSON.parse(beaconInit?.body as string)).toEqual({
      files_fetched: 1,
    });
  });

  it("persists and reads back per-file rollups via CacheStorage", async () => {
    const store = new Map<string, Response>();
    const openMock = vi.fn(async () => ({
      match: vi.fn(async (req: string) => store.get(req)?.clone()),
      put: vi.fn(async (req: string, res: Response) => {
        store.set(req, res.clone());
      }),
    }));
    vi.stubGlobal("caches", { open: openMock });

    const source = browserSource("");
    const rollup = {
      version: 1 as const,
      key: "pq/2026-10-29/file-1.parquet",
      minTs: 1793268000000,
      maxTs: 1793268060000,
      rawRows: 10,
      byteLength: 512,
      totalRequests: 10,
      hasBytes: true,
      buckets: [
        {
          t: 1793268000000,
          requests: 10,
          bytes: 1024,
          s4xx: 1,
          s5xx: 0,
          hits: 8,
          cacheRows: 10,
        },
      ],
      counts: { host: [["example.com", 10] as [string, number]] },
      nonNull: { host: 10 },
    };

    await source.writeRollup!(rollup.key, rollup);
    expect(openMock).toHaveBeenCalledWith(ROLLUP_CACHE_NAME);
    const loadedFromMemory = await source.readRollup!(rollup.key);
    expect(loadedFromMemory).toEqual(rollup);
    const loadedFromCacheStorage = await browserSource("").readRollup!(
      rollup.key,
    );
    expect(loadedFromCacheStorage).toEqual(rollup);
    // Rollups that also count custom fields are a separate cache entry.
    const custom = { ...rollup, counts: { ...rollup.counts, tenant: [] } };
    await source.writeRollup!(rollup.key, custom, "tenant:string");
    expect(await source.readRollup!(rollup.key)).toEqual(rollup);
    expect(await source.readRollup!(rollup.key, "tenant:string")).toEqual(
      custom,
    );
    const fresh = browserSource("");
    expect(await fresh.readRollup!(rollup.key)).toEqual(rollup);
    expect(await fresh.readRollup!(rollup.key, "tenant:string")).toEqual(
      custom,
    );
    expect(await fresh.readRollup!(rollup.key, "tenant:number")).toBeNull();
    const stored = store.values().next().value!;
    expect(stored.headers.get("content-type")).toBe("application/octet-stream");
    const head = new Uint8Array(await stored.clone().arrayBuffer());
    expect(new TextDecoder().decode(head.subarray(0, 3))).toBe("FLR");
  });

  it("ignores JSON rollups stored under the current cache version", async () => {
    const key = "pq/json.parquet";
    const { cacheFor } = fakeCacheStorage({});
    await cacheFor(ROLLUP_CACHE_NAME).put(
      `/__flp_rollup?${new URLSearchParams({ v: ROLLUP_CACHE_VERSION, key })}`,
      new Response(
        JSON.stringify({ version: 1, key, counts: {}, nonNull: {} }),
      ),
    );
    expect(await browserSource("").readRollup!(key)).toBeNull();
  });

  it("stores per-file rollups in CacheStorage and bounds in-memory rollups by estimated bytes", async () => {
    const store = new Map<string, Response>();
    store.set("/__flp_rollup_snapshot?v=1", new Response("{}"));
    const matchMock = vi.fn(async (req: string) => store.get(req)?.clone());
    const deleteMock = vi.fn(async (req: string) => store.delete(req));
    const openMock = vi.fn(async () => ({
      match: matchMock,
      delete: deleteMock,
      put: vi.fn(async (req: string, res: Response) => {
        store.set(req, res.clone());
      }),
    }));
    vi.stubGlobal("caches", { open: openMock });

    const makeRollup = (key: string, rawRows = 5) => ({
      version: 1 as const,
      key,
      minTs: 1793268000000,
      maxTs: 1793268060000,
      rawRows,
      byteLength: 256,
      totalRequests: rawRows,
      hasBytes: true,
      buckets: [],
      counts: {
        url: Array.from(
          { length: 1_000 },
          (_, i) => [`/${key}/${i}`, 1] as [string, number],
        ),
      },
      nonNull: { url: 1_000 },
    });
    const each = estimateRollupBytes(makeRollup("pq/1.parquet", 2_000_000));

    const writer = browserSource("", undefined, {
      maxMemoryRollupBytes: Math.floor(each * 2.5),
    });
    await Promise.all([
      writer.writeRollup!(
        "pq/1.parquet",
        makeRollup("pq/1.parquet", 2_000_000),
      ),
      writer.writeRollup!(
        "pq/2.parquet",
        makeRollup("pq/2.parquet", 2_000_000),
      ),
      writer.writeRollup!(
        "pq/3.parquet",
        makeRollup("pq/3.parquet", 2_000_000),
      ),
    ]);
    expect(deleteMock).toHaveBeenCalledWith("/__flp_rollup_snapshot?v=1");
    expect(store.has("/__flp_rollup_snapshot?v=1")).toBe(false);
    expect(store.size).toBe(3);
    expect(openMock).toHaveBeenCalledTimes(1);

    // pq/1.parquet was evicted by the byte cap, so reading it hits CacheStorage,
    // while pq/2.parquet and pq/3.parquet are served directly from memory.
    matchMock.mockClear();
    const r2 = await writer.readRollup!("pq/2.parquet");
    const r3 = await writer.readRollup!("pq/3.parquet");
    expect(r2?.key).toBe("pq/2.parquet");
    expect(r3?.key).toBe("pq/3.parquet");
    expect(matchMock).toHaveBeenCalledTimes(0);

    const r1 = await writer.readRollup!("pq/1.parquet");
    expect(r1?.key).toBe("pq/1.parquet");
    expect(matchMock).toHaveBeenCalledTimes(1);
  });

  it("keeps compact rollups of many rows in memory", async () => {
    const matchMock = vi.fn(async () => undefined);
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: matchMock,
        put: vi.fn(async () => {}),
      })),
    });
    const source = browserSource("");
    for (const key of ["a", "b", "c"]) {
      await source.writeRollup!(key, {
        version: 1,
        key,
        minTs: 0,
        maxTs: 1,
        rawRows: 2_000_000,
        byteLength: 256,
        totalRequests: 2_000_000,
        hasBytes: false,
        buckets: [],
        counts: { status: [[200, 2_000_000]] },
        nonNull: { status: 2_000_000 },
      });
    }
    for (const key of ["a", "b", "c"]) {
      expect((await source.readRollup!(key))?.key).toBe(key);
    }
    expect(matchMock).not.toHaveBeenCalled();
  });
});

function fakeCacheStorage(names: Record<string, string[]>) {
  const stores = new Map<string, Map<string, Response>>(
    Object.entries(names).map(([name, urls]) => [
      name,
      new Map(urls.map((url) => [url, new Response("x")])),
    ]),
  );
  const opened = new Map<string, ReturnType<typeof makeCache>>();
  const cacheFor = (name: string) => {
    if (!stores.has(name)) stores.set(name, new Map());
    const store = stores.get(name)!;
    if (opened.get(name)?.store !== store) opened.set(name, makeCache(store));
    return opened.get(name)!;
  };
  const makeCache = (store: Map<string, Response>) => {
    return {
      store,
      match: async (url: string) => store.get(url)?.clone(),
      put: async (url: string, res: Response) => {
        store.set(url, res);
      },
      delete: async (url: string) => store.delete(url),
      keys: async () => [...store.keys()].map((url) => ({ url })),
    };
  };
  vi.stubGlobal("caches", {
    open: async (name: string) => cacheFor(name),
    keys: async () => [...stores.keys()],
    delete: async (name: string) => stores.delete(name),
  });
  return { stores, cacheFor };
}

describe("browserSource filtered results", () => {
  const result = {
    data: {
      minTs: 1,
      maxTs: 2,
      totalRequests: 2,
      hasBytes: true,
      buckets: [{ t: 0, requests: 2, bytes: 3, s4xx: 0, s5xx: 0, hits: 1, cacheRows: 2 }],
      counts: { host: [["a.example", 2]] as [string, number][] },
      nonNull: { host: 2 },
    },
    truncated: [],
    rawRows: 5,
    byteLength: 100,
  };

  it("persists across page loads and stays within one service", async () => {
    fakeCacheStorage({});
    const scope = { cacheScope: "session" };
    const first = browserSource("https://app.example", { serviceId: "a" }, scope);
    await first.writeFilteredResult!("file|query", result);
    first.clearMemoryCache!();
    const reload = browserSource("https://app.example", { serviceId: "a" }, scope);
    expect(await reload.readFilteredResult!("file|query")).toEqual(result);
    const other = browserSource("https://app.example", { serviceId: "b" }, scope);
    expect(await other.readFilteredResult!("file|query")).toBeNull();
  });

  it("forgets in-memory results on clearMemoryCache and tolerates no Cache API", async () => {
    const source = browserSource("https://app.example", undefined, { cacheScope: "s" });
    await source.writeFilteredResult!("file|query", result);
    source.clearMemoryCache!();
    expect(await source.readFilteredResult!("file|query")).toBeNull();
  });
});

describe("rollup cache versioning", () => {
  it("derives the version from the card fields and aggregation semantics", () => {
    const current = rollupCacheVersion(CARD_FIELDS, ROLLUP_SEMANTICS);
    expect(ROLLUP_CACHE_NAME).toBe(`flp-parquet-rollup-${current}`);
    expect(rollupCacheVersion([...CARD_FIELDS], ROLLUP_SEMANTICS)).toBe(
      current,
    );
    expect(
      rollupCacheVersion([...CARD_FIELDS, "new_field"], ROLLUP_SEMANTICS),
    ).not.toBe(current);
    expect(rollupCacheVersion(CARD_FIELDS.slice(1), ROLLUP_SEMANTICS)).not.toBe(
      current,
    );
    expect(rollupCacheVersion(CARD_FIELDS, `${ROLLUP_SEMANTICS}+1`)).not.toBe(
      current,
    );
  });

  it("deletes stale rollup caches on open and ignores rollups keyed by another version", async () => {
    const key = "pq/1.parquet";
    const { stores } = fakeCacheStorage({
      "flp-parquet-rollup-v1": [
        `/__flp_rollup?${new URLSearchParams({ v: "1", key })}`,
      ],
      "flp-parquet-rollup-deadbeef": [],
      [CACHE_NAME]: [],
      [ROLLUP_CACHE_NAME]: [
        `/__flp_rollup?${new URLSearchParams({ v: "1", key })}`,
      ],
    });
    expect(await browserSource("").readRollup!(key)).toBeNull();
    expect([...stores.keys()].sort()).toEqual(
      [CACHE_NAME, ROLLUP_CACHE_NAME].sort(),
    );
  });
});

describe("clearRequestLogCaches", () => {
  it("removes every request Parquet and rollup cache and keeps unrelated caches", async () => {
    const { stores } = fakeCacheStorage({
      [CACHE_NAME]: ["/api/file?key=a"],
      [ROLLUP_CACHE_NAME]: ["/__flp_rollup?key=a"],
      "flp-parquet-rollup-v1": ["/__flp_rollup?key=a"],
      [FILTERED_CACHE_NAME]: ["/__flp_filtered?id=a"],
      "next-static": [],
    });
    await clearRequestLogCaches();
    expect([...stores.keys()]).toEqual(["next-static"]);
  });
});

describe("pruneRequestCache", () => {
  const prefix = "svc/requests/";
  const key = (hour: number, start: number, run = "r") =>
    `${prefix}year=2026/month=10/day=29/hour=${String(hour).padStart(2, "0")}/window=${start}-${start + 60}-${run}-n1.parquet`;
  const fileUrl = (k: string) => `/api/file?${new URLSearchParams({ key: k })}`;
  const at = (iso: string) => Date.parse(iso) / 1000;
  const live = key(12, at("2026-10-29T12:00:00Z"));
  const superseded = key(12, at("2026-10-29T12:01:00Z"), "old");
  const outside = key(14, at("2026-10-29T14:00:00Z"), "other");
  const expired = key(1, at("2026-10-01T01:00:00Z"));
  const otherService = `other/requests/year=2026/month=10/day=29/hour=12/window=${at("2026-10-29T12:02:00Z")}-${at("2026-10-29T12:03:00Z")}-r-n1.parquet`;
  const manifest = {
    complete: true,
    files: [
      {
        key: live,
        start: "2026-10-29T12:00:00Z",
        end: "2026-10-29T12:01:00Z",
        size: 1,
        rows: 1,
      },
    ],
  };
  const range = [
    Date.parse("2026-10-29T12:00:00Z"),
    Date.parse("2026-10-29T13:00:00Z"),
  ] as const;

  it("drops entries missing from the manifest range or past retention", async () => {
    const { cacheFor } = fakeCacheStorage({
      [CACHE_NAME]: [live, superseded, outside, expired, otherService].map(
        fileUrl,
      ),
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;
    const deleted = await pruneRequestCache(cache, manifest, ...range, {
      retentionCutoffMs: Date.parse("2026-10-02T00:00:00Z"),
    });
    expect(deleted.sort()).toEqual([expired, superseded].sort());
    const left = (await cache.keys()).map((r) =>
      new URL(r.url, "http://x").searchParams.get("key"),
    );
    expect(left.sort()).toEqual([live, outside, otherService].sort());
  });

  it("prunes only entries owned by the current service scope", async () => {
    const scoped = (k: string, scope: string) =>
      `${fileUrl(k)}&scope=${encodeURIComponent(scope)}`;
    const { cacheFor } = fakeCacheStorage({
      [CACHE_NAME]: [scoped(superseded, "service-a"), scoped(superseded, "service-b")],
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;

    expect(await pruneRequestCache(cache, manifest, ...range, {
      scope: "service-a",
      retentionCutoffMs: 0,
    })).toEqual([superseded]);
    expect((await cache.keys()).map((request) => new URL(request.url, "http://x").searchParams.get("scope")))
      .toEqual(["service-b"]);
  });

  it("prunes owned manifest and derived identities without crossing services", async () => {
    const { cacheFor, stores } = fakeCacheStorage({});
    const fetchMock = vi.fn(
      async () => new Response(new Uint8Array([1])),
    );
    vi.stubGlobal("fetch", fetchMock);
    const identity = JSON.stringify(["manifest", "etag", 17]);
    const derivedIdentity = JSON.stringify(["columns", ["status", "url"]]);
    const base = browserSource("", undefined, { cacheScope: "session" });

    await Promise.all([
      base.withOptions({ serviceId: "service-a" }).fetchFile(superseded, undefined, { identity }),
      base.withOptions({ serviceId: "service-b" }).fetchFile(superseded, undefined, { identity }),
    ]);
    await Promise.all([
      base.withOptions({ serviceId: "service-a" }).writeDerivedFile!(
        superseded,
        derivedIdentity,
        new Uint8Array([2]).buffer,
        "[]",
      ),
      base.withOptions({ serviceId: "service-b" }).writeDerivedFile!(
        superseded,
        derivedIdentity,
        new Uint8Array([2]).buffer,
        "[]",
      ),
    ]);
    await vi.waitFor(() =>
      expect(stores.get(CACHE_NAME)?.size).toBe(4),
    );
    expect(
      [...stores.get(CACHE_NAME)!.keys()]
        .filter(
          (url) =>
            new URL(url, "http://localhost").searchParams.get("scope") ===
            "session:service-a",
        )
        .map(
          (url) =>
            new URL(url, "http://localhost").searchParams.get("identity"),
        )
        .sort(),
    ).toEqual([identity, `derived:${derivedIdentity}`].sort());

    const cache = cacheFor(CACHE_NAME) as unknown as Cache;
    expect(await pruneRequestCache(cache, manifest, ...range, {
      scope: "session:service-a",
      retentionCutoffMs: 0,
    })).toEqual([superseded, superseded]);
    expect(
      [...stores.get(CACHE_NAME)!.keys()].map(
        (url) =>
          new URL(url, "http://localhost").searchParams.get("scope"),
      ),
    ).toEqual(["session:service-b", "session:service-b"]);
  });

  it("limits explicit unscoped pruning to entries without an owner", async () => {
    const scoped = (k: string, owner: string | null) =>
      owner === null
        ? fileUrl(k)
        : `${fileUrl(k)}&scope=${encodeURIComponent(owner)}`;
    const { cacheFor, stores } = fakeCacheStorage({
      [CACHE_NAME]: [
        scoped(superseded, null),
        scoped(superseded, "session:service-a"),
      ],
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;

    expect(await pruneRequestCache(cache, manifest, ...range, {
      scope: undefined,
      retentionCutoffMs: 0,
    })).toEqual([superseded]);
    expect(
      [...stores.get(CACHE_NAME)!.keys()].map(
        (url) =>
          new URL(url, "http://localhost").searchParams.get("scope"),
      ),
    ).toEqual(["session:service-a"]);
  });

  it("expires summaries and bundles by retention but never as superseded", async () => {
    const dir = "svc/processed/summary/request/year=2026/month=10/day=";
    const bundles =
      "svc/processed/summary_bundle/request/year=2026/month=10/day=";
    const window = `${dir}29/hour=12/summary=${at("2026-10-29T12:05:00Z")}-${at("2026-10-29T12:06:00Z")}-r-n1.parquet`;
    const oldWindow = `${dir}01/hour=01/summary=${at("2026-10-01T01:00:00Z")}-${at("2026-10-01T01:01:00Z")}-r-n1.parquet`;
    const hour = `${bundles}29/hour_bundle=12-c1-n1-h0000000000000001.parquet`;
    const oldHour = `${bundles}01/hour_bundle=23-c1-n1-h0000000000000001.parquet`;
    const oldDay = `${bundles}01/day_bundle=c1-n1-h0000000000000001.parquet`;
    const { cacheFor } = fakeCacheStorage({
      [CACHE_NAME]: [window, oldWindow, hour, oldHour, oldDay].map(fileUrl),
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;
    const deleted = await pruneRequestCache(cache, manifest, ...range, {
      retentionCutoffMs: Date.parse("2026-10-02T00:00:00Z"),
    });
    expect(deleted.sort()).toEqual([oldDay, oldHour, oldWindow].sort());
  });

  it("keeps everything in range when the manifest is incomplete", async () => {
    const { cacheFor } = fakeCacheStorage({
      [CACHE_NAME]: [live, superseded].map(fileUrl),
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;
    const deleted = await pruneRequestCache(
      cache,
      { ...manifest, complete: false },
      ...range,
      { retentionCutoffMs: 0 },
    );
    expect(deleted).toEqual([]);
  });

  it("bounds the number of deletions per run", async () => {
    const stale = Array.from({ length: 5 }, (_, i) =>
      key(1, at("2026-10-01T01:00:00Z") + i * 60),
    );
    const { cacheFor } = fakeCacheStorage({
      [CACHE_NAME]: stale.map(fileUrl),
    });
    const cache = cacheFor(CACHE_NAME) as unknown as Cache;
    const options = {
      retentionCutoffMs: Date.parse("2026-10-02T00:00:00Z"),
      maxDeletes: 2,
    };
    expect(
      await pruneRequestCache(cache, manifest, ...range, options),
    ).toHaveLength(2);
    expect(await cache.keys()).toHaveLength(3);
    expect(
      await pruneRequestCache(cache, manifest, ...range, options),
    ).toHaveLength(2);
    expect(await cache.keys()).toHaveLength(1);
  });

  it("prunes the file and rollup caches through the browser source", async () => {
    const rollupUrl = (k: string) =>
      `/__flp_rollup?${new URLSearchParams({ v: rollupCacheVersion(CARD_FIELDS, ROLLUP_SEMANTICS), key: k })}`;
    const { stores } = fakeCacheStorage({
      [CACHE_NAME]: [live, superseded].map(fileUrl),
      [ROLLUP_CACHE_NAME]: [live, superseded].map(rollupUrl),
    });
    await browserSource("").pruneCache!(manifest, ...range);
    expect([...stores.get(CACHE_NAME)!.keys()]).toEqual([fileUrl(live)]);
    expect([...stores.get(ROLLUP_CACHE_NAME)!.keys()]).toEqual([
      rollupUrl(live),
    ]);
  });
});

describe("persistent Parquet store", () => {
  const MiB = 1024 * 1024;
  const fileUrl = (k: string) => `/api/file?${new URLSearchParams({ key: k })}`;
  const at = (minute: number) =>
    Date.parse("2026-10-29T12:00:00Z") / 1000 + minute * 60;
  const key = (minute: number) =>
    `svc/requests/year=2026/month=10/day=29/hour=12/window=${at(minute)}-${at(minute + 1)}-r-n1.parquet`;
  const entry = (minute: number) => ({
    key: key(minute),
    start: new Date(at(minute) * 1000).toISOString(),
    end: new Date(at(minute + 1) * 1000).toISOString(),
    size: MiB,
    rows: 1,
  });
  const range = [
    Date.parse("2026-10-29T12:00:00Z"),
    Date.parse("2026-10-29T13:00:00Z"),
  ] as const;

  function storage(usage: () => number, quota = 10 * MiB) {
    vi.stubGlobal("navigator", {
      storage: { estimate: vi.fn(async () => ({ usage: usage(), quota })) },
    });
  }

  function sizedStores() {
    const fake = fakeCacheStorage({});
    const used = () =>
      [...(fake.stores.get(CACHE_NAME)?.values() ?? [])].reduce(
        (sum, res) => sum + Number(res.headers.get("content-length") ?? 0),
        0,
      );
    return { ...fake, used };
  }

  const network = () =>
    vi.fn<(url: string, init?: RequestInit) => Promise<Response>>(
      async () =>
        new Response(new Uint8Array(MiB), {
          headers: { "content-length": String(MiB) },
        }),
    );

  it("bypasses the HTTP cache so Parquet bytes are stored only once", async () => {
    fakeCacheStorage({});
    const fetchMock = network();
    vi.stubGlobal("fetch", fetchMock);
    await browserSource("").fetchFile(key(0));
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ cache: "no-store" });
  });

  it("evicts the least recently used files outside the manifest before writing", async () => {
    const { stores, used } = sizedStores();
    storage(() => used(), 10 * MiB);
    vi.stubGlobal("fetch", network());
    const source = browserSource("");
    for (let minute = 0; minute < 8; minute += 1) {
      await source.fetchFile(key(minute));
      await flushStore();
    }
    // Minutes 0 and 1 were used most recently through the manifest, so they stay.
    await source.pruneCache!(
      { complete: false, files: [entry(0), entry(1)] },
      ...range,
    );
    await source.fetchFile(key(8));
    await flushStore();
    const left = [...stores.get(CACHE_NAME)!.keys()];
    expect(left).toContain(fileUrl(key(0)));
    expect(left).toContain(fileUrl(key(1)));
    expect(left).toContain(fileUrl(key(8)));
    expect(left).not.toContain(fileUrl(key(2)));
    expect(used()).toBeLessThanOrEqual(8 * MiB);
    expect(await readStoreError()).toBeNull();
  });

  it("records a quota error when no unpinned entry can be evicted", async () => {
    const { stores } = sizedStores();
    storage(() => 9.5 * MiB, 10 * MiB);
    vi.stubGlobal("fetch", network());
    const source = browserSource("");
    await source.pruneCache!({ complete: false, files: [entry(0)] }, ...range);
    await source.fetchFile(key(0));
    await flushStore();
    expect(stores.get(CACHE_NAME)?.size ?? 0).toBe(0);
    expect(await readStoreError()).toMatchObject({
      cache: CACHE_NAME,
      key: key(0),
      bytes: MiB,
      usage: 9.5 * MiB,
      quota: 10 * MiB,
    });
  });

  it("records put failures instead of swallowing them", async () => {
    const { cacheFor } = fakeCacheStorage({});
    const files = cacheFor(CACHE_NAME);
    files.put = async () => {
      throw new DOMException("Quota exceeded", "QuotaExceededError");
    };
    storage(() => 0);
    vi.stubGlobal("fetch", network());
    const file = await browserSource("").fetchFile(key(3));
    expect(file.bytes.byteLength).toBe(MiB);
    await flushStore();
    expect(await readStoreError()).toMatchObject({
      cache: CACHE_NAME,
      key: key(3),
      message: expect.stringContaining("QuotaExceededError"),
    });
  });

  it("clears the store index and errors with the request-log caches", async () => {
    const { stores } = fakeCacheStorage({});
    await recordStoreError({
      at: 1,
      cache: CACHE_NAME,
      key: "k",
      bytes: 1,
      message: "full",
      usage: null,
      quota: null,
    });
    const { MANIFEST_SNAPSHOT_CACHE } = await import("./manifest-store");
    const manifestCache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    await manifestCache.put(
      "/__flp_manifest_snapshot?v=1&scope=test",
      new Response("{}"),
    );
    expect(stores.has(STORE_META_CACHE)).toBe(true);
    expect(stores.has(MANIFEST_SNAPSHOT_CACHE)).toBe(true);
    await clearRequestLogCaches();
    expect(stores.has(STORE_META_CACHE)).toBe(false);
    expect(stores.has(MANIFEST_SNAPSHOT_CACHE)).toBe(false);
  });

  it("clearDashboardData and inline/worker dashboard runs isolate manifest caches by serviceId and wipe on logout", async () => {
    const ls = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (k: string) => ls.get(k) ?? null,
      setItem: (k: string, v: string) => {
        ls.set(k, v);
      },
      removeItem: (k: string) => {
        ls.delete(k);
      },
    });
    const { stores } = fakeCacheStorage({});
    const { MANIFEST_SNAPSHOT_CACHE } = await import("./manifest-store");
    const { DashboardDuckDBClient, clearDashboardData } = await import("./client");

    updateDashboardCacheIdentity("alice-corp:admin");
    expect(dashboardCacheScope()).toContain("alice-corp:admin");
    const mCache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    await mCache.put("/__flp_manifest_snapshot?v=1&scope=alice", new Response("{}"));

    const requestedUrls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        requestedUrls.push(String(input));
        return Response.json({
          files: [],
          next_cursor: null,
          complete: true,
          catalog_generation: 3,
        });
      }),
    );
    const client = new DashboardDuckDBClient();
    await client.run({
      from: "2026-10-29T00:00:00.000Z",
      to: "2026-10-29T01:00:00.000Z",
      serviceId: "svc-isolated",
    } as Parameters<typeof client.run>[0]);
    expect(
      requestedUrls.some((u) =>
        new URL(u, "http://localhost").searchParams.get("service_id") === "svc-isolated",
      ),
    ).toBe(true);

    await clearDashboardData();
    expect(dashboardCacheIdentity()).toBeNull();
    expect(ls.has("flp-dashboard-cache-identity")).toBe(false);
    expect(ls.has("flp-dashboard-cache-session")).toBe(false);
    expect(stores.has(MANIFEST_SNAPSHOT_CACHE)).toBe(false);
  });

  it("fetches processed Parquet keys via /fos/<key> and reports normal file path in telemetry", async () => {
    fakeCacheStorage({});
    const fetchedUrls: string[] = [];
    const calls: unknown[] = [];
    const liveKey =
      "goal2-live-20261029/processed/summary/request/year=2026/month=11/day=04/hour=19/summary=1793819640-1793819760-b1fb986e0bd3s0-n14.parquet";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        fetchedUrls.push(String(input));
        return new Response(new Uint8Array([1, 2, 3, 4]), {
          status: 200,
          headers: { "x-flp-cdn-cache": "HIT" },
        });
      }),
    );
    const source = browserSource("", {
      onApiCall: (call) => calls.push(call),
    });
    const res = await source.fetchFile(liveKey);
    expect(res.bytes.byteLength).toBe(4);
    expect(res.cdn).toBe("HIT");
    expect(fetchedUrls).toEqual([`/fos/${liveKey}`]);
    expect(calls).toMatchObject([
      {
        source: "CDN Cache",
        method: "GET",
        path: liveKey,
        status: 200,
        cache: "cdn",
        endpoint: "GET /api/file",
      },
    ]);
  });

  it("wipes range-loader singleton memoryFiles on logout/identity switch and forwards x-service-id on whole-file reads", async () => {
    const ls = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (k: string) => ls.get(k) ?? null,
      setItem: (k: string, v: string) => {
        ls.set(k, v);
      },
      removeItem: (k: string) => {
        ls.delete(k);
      },
    });
    fakeCacheStorage({});
    const { parquetWriteBuffer } = await import("hyparquet-writer");
    const { ensureCloudRange } = await import("../range-loader");
    const { clearManifestCache } = await import("./manifest");
    const { getDuckDBEngine } = await import("./duckdb");
    const { clearDashboardData, setDashboardCacheIdentity } = await import("./client");

    clearManifestCache();
    await getDuckDBEngine().reset();

    const fromIso = "2026-11-21T00:00:00.000Z";
    const toIso = "2026-11-21T01:00:00.000Z";
    const rows = 800;
    const adminBytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: 400,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(Date.parse(fromIso) + i)) },
        { name: "url", type: "STRING", data: Array.from({ length: rows }, (_, i) => `/alice-pii-secret-${i}-${"p".repeat(80)}`) },
        { name: "client_ip", type: "STRING", data: Array.from({ length: rows }, () => "198.51.100.42") },
      ],
    });
    expect(adminBytes.byteLength).toBeGreaterThan(64 * 1024);

    // Relative root-prefix key routes through `/api/file?key=...`, where x-service-id header applies.
    const relKey = "processed/request/year=2026/month=11/day=21/hour=00/window=1-2.parquet";
    let activeRole: "admin" | "analyst-no-pii" = "admin";
    const seenFileServiceHeaders: Array<string | null> = [];
    let wholeFileNetworkHits = 0;

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input), "http://localhost");
        const headers = new Headers(init?.headers);
        if (url.pathname === "/api/files") {
          return Response.json({
            files: [
              {
                key: relKey,
                start: fromIso,
                end: toIso,
                size: adminBytes.byteLength,
                rows,
                etag: activeRole === "admin" ? "\"etag-admin-v1\"" : "\"etag-bob-v2\"",
              },
            ],
            next_cursor: null,
            complete: true,
            catalog_generation: 1,
          });
        }
        if (url.pathname === "/api/file") {
          const range = headers.get("range");
          if (range) {
            const m = /^bytes=(\d+)-(\d+)$/.exec(range)!;
            const slice = adminBytes.slice(Number(m[1]), Number(m[2]) + 1);
            return new Response(slice, {
              status: 206,
              headers: {
                "content-range": `bytes ${m[1]}-${m[2]}/${adminBytes.byteLength}`,
                etag: activeRole === "admin" ? "\"etag-admin-v1\"" : "\"etag-bob-v2\"",
              },
            });
          }
          wholeFileNetworkHits++;
          seenFileServiceHeaders.push(headers.get("x-service-id"));
          if (activeRole !== "admin") {
            return new Response("Forbidden", { status: 403 });
          }
          return new Response(adminBytes.slice(0), { status: 200 });
        }
        return new Response("Not Found", { status: 404 });
      }),
    );

    await setDashboardCacheIdentity("alice:admin");
    const adminRead = await ensureCloudRange(
      fromIso,
      toIso,
      new AbortController().signal,
      ["request"],
      { serviceId: "svc-a", projection: { columns: ["timestamp", "url", "client_ip"] } },
    );
    expect(wholeFileNetworkHits).toBe(1);
    expect(seenFileServiceHeaders).toEqual(["svc-a"]);
    adminRead.release?.();

    // Auditor Challenge 3: clearRequestLogCaches() with UNCHANGED (scope, serviceId, etag) must evict >64 KiB memoryFiles in RAM!
    await clearRequestLogCaches();
    await getDuckDBEngine().reset();
    clearManifestCache();
    const adminReadAfterCacheClear = await ensureCloudRange(
      fromIso,
      toIso,
      new AbortController().signal,
      ["request"],
      { serviceId: "svc-a", projection: { columns: ["timestamp", "url", "client_ip"] } },
    );
    expect(wholeFileNetworkHits).toBe(2);
    adminReadAfterCacheClear.release?.();

    // Sign out Alice and sign in Bob (AnalystNoPii on svc-b, forbidden 403 on whole-file /api/file).
    // IMPORTANT: Do NOT manually call getDuckDBEngine().reset() here — clearDashboardData() MUST reset it!
    await clearDashboardData();
    const postLogoutRows = await getDuckDBEngine()
      .readContext()
      .executeSql("SELECT count(*) AS cnt FROM requests");
    expect(postLogoutRows).toEqual([{ cnt: 0 }]);

    activeRole = "analyst-no-pii";
    await setDashboardCacheIdentity("bob:analyst-no-pii");

    await expect(
      ensureCloudRange(
        fromIso,
        toIso,
        new AbortController().signal,
        ["request"],
        { serviceId: "svc-b", projection: { columns: ["timestamp", "url"] } },
      ),
    ).rejects.toThrow(/403/);
    expect(wholeFileNetworkHits).toBe(3);
    expect(seenFileServiceHeaders).toEqual(["svc-a", "svc-a", "svc-b"]);
  });

  it("aborts in-flight <65% fetchRange + ensureCloudRange and >=65% fetchFile/storeQueue across clearDashboardData leaving 0 DuckDB rows and empty CacheStorage", async () => {
    const ls = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (k: string) => ls.get(k) ?? null,
      setItem: (k: string, v: string) => {
        ls.set(k, v);
      },
      removeItem: (k: string) => {
        ls.delete(k);
      },
    });
    const { stores } = fakeCacheStorage({});
    const { parquetWriteBuffer } = await import("hyparquet-writer");
    const { ensureCloudRange, cloudFileSource } = await import("../range-loader");
    const { clearManifestCache } = await import("./manifest");
    const { getDuckDBEngine } = await import("./duckdb");
    const { clearDashboardData, setDashboardCacheIdentity } = await import("./client");

    clearManifestCache();
    await getDuckDBEngine().reset();

    const fromIso = "2026-11-21T02:00:00.000Z";
    const toIso = "2026-11-21T03:00:00.000Z";
    const rows = 800;
    // Large padding column ensures ["timestamp", "ip"] projection is well under 65% of file size -> uses fetchRange!
    const widePiiBytes = parquetWriteBuffer({
      codec: "UNCOMPRESSED",
      rowGroupSize: 400,
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(Date.parse(fromIso) + i)) },
        { name: "ip", type: "STRING", data: Array.from({ length: rows }, () => "203.0.113.55") },
        { name: "cookie_session", type: "STRING", data: Array.from({ length: rows }, () => "ALICE_RAW_COOKIE_999") },
        { name: "url", type: "STRING", data: Array.from({ length: rows }, (_, i) => `/alice-secret-${i}-${"x".repeat(520)}`) },
      ],
    });
    expect(widePiiBytes.byteLength).toBeGreaterThan(400 * 1024);

    const relKey = "processed/request/year=2026/month=11/day=21/hour=02/window=10-20.parquet";
    const pendingDataGate: Array<() => void> = [];
    let sawDataRangeRead = false;
    let notifyDataRangeStarted: (() => void) | null = null;
    const dataRangeStarted = new Promise<void>((resolve) => {
      notifyDataRangeStarted = resolve;
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input), "http://localhost");
        const headers = new Headers(init?.headers);
        const sig = init?.signal;
        sig?.throwIfAborted();
        if (url.pathname === "/api/files") {
          return Response.json({
            files: [
              {
                key: relKey,
                start: fromIso,
                end: toIso,
                size: widePiiBytes.byteLength,
                rows,
                etag: "\"etag-wide-alice\"",
              },
            ],
            next_cursor: null,
            complete: true,
            catalog_generation: 1,
          });
        }
        if (url.pathname === "/api/file") {
          const range = headers.get("range");
          if (range) {
            const m = /^bytes=(\d+)-(\d+)$/.exec(range)!;
            const start = Number(m[1]);
            const end = Number(m[2]);
            // Let the tail footer read complete immediately; hold the column chunk range read open across logout!
            const isFooterTail = end === widePiiBytes.byteLength - 1 && start > 0;
            if (!isFooterTail) {
              sawDataRangeRead = true;
              notifyDataRangeStarted?.();
              await new Promise<void>((resolve, reject) => {
                const onAbort = () => reject(sig?.reason ?? new DOMException("Aborted", "AbortError"));
                if (sig?.aborted) {
                  onAbort();
                  return;
                }
                sig?.addEventListener("abort", onAbort, { once: true });
                pendingDataGate.push(() => {
                  sig?.removeEventListener("abort", onAbort);
                  resolve();
                });
              });
            }
            sig?.throwIfAborted();
            return new Response(widePiiBytes.slice(start, end + 1), {
              status: 206,
              headers: {
                "content-range": `bytes ${start}-${end}/${widePiiBytes.byteLength}`,
                etag: "\"etag-wide-alice\"",
              },
            });
          }
          // Also hold whole-file fetch open across logout to test fetchFile / storeQueue epoch guard!
          await new Promise<void>((resolve) => {
            pendingDataGate.push(resolve);
          });
          return new Response(widePiiBytes.slice(0), { status: 200 });
        }
        return new Response("Not Found", { status: 404 });
      }),
    );

    await setDashboardCacheIdentity("alice:admin");
    const inFlightRangePromise = ensureCloudRange(
      fromIso,
      toIso,
      new AbortController().signal,
      ["request"],
      { serviceId: "svc-alice", projection: { columns: ["timestamp", "ip"] } },
    ).then(
      () => "RESOLVED",
      () => "REJECTED",
    );
    const inFlightWholePromise = cloudFileSource("svc-alice")
      .fetchFile(relKey, undefined, { identity: "whole-inflight-alice" })
      .then(
        () => "RESOLVED",
        () => "REJECTED",
      );

    await dataRangeStarted;
    expect(sawDataRangeRead).toBe(true);

    // Sign out Alice while both <65% fetchRange and whole-file fetchFile are in flight!
    await clearDashboardData();

    // Release any mock network gates in case unpatched code didn't abort them
    while (pendingDataGate.length > 0) {
      pendingDataGate.shift()?.();
    }
    const rangeOutcome = await inFlightRangePromise;
    await inFlightWholePromise;
    await flushStore();
    await flushStore();

    expect(rangeOutcome).toBe("REJECTED");
    const duckRowsAfter = await getDuckDBEngine()
      .readContext()
      .executeSql("SELECT count(*) AS cnt FROM requests");
    expect(duckRowsAfter).toEqual([{ cnt: 0 }]);
    expect([...stores.keys()]).toEqual([]);
  });

  it("tracks post-bootstrap cacheScope updates so warm reloads hit derived file cache", async () => {
    const ls = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (k: string) => ls.get(k) ?? null,
      setItem: (k: string, v: string) => {
        ls.set(k, v);
      },
      removeItem: (k: string) => {
        ls.delete(k);
      },
    });
    const store = new Map<string, Response>();
    vi.stubGlobal("caches", {
      open: vi.fn(async () => ({
        match: vi.fn(async (req: string) => store.get(req)?.clone()),
        delete: vi.fn(async (req: string) => store.delete(req)),
        put: vi.fn(async (req: string, res: Response) => {
          store.set(req, res.clone());
        }),
      })),
    });
    const { setDashboardCacheIdentity } = await import("./client");

    // Simulate module-import browserSource("") constructed before /api/bootstrap sets identity.
    const coldSource = browserSource("");
    await setDashboardCacheIdentity("user:admin");
    const scopedCold = coldSource.withOptions!({ serviceId: "svc-demo" });
    await scopedCold.writeDerivedFile!(
      "processed/request/year=2026/month=11/day=21/hour=00/compact=1-2.parquet",
      "proj-network",
      new Uint8Array([1, 2, 3, 4]).buffer,
      "meta-v1",
    );

    // Simulate warm page reload constructing a fresh browserSource("") after identity is persisted.
    const warmSource = browserSource("").withOptions!({ serviceId: "svc-demo" });
    const hit = await warmSource.readDerivedFile!(
      "processed/request/year=2026/month=11/day=21/hour=00/compact=1-2.parquet",
      "proj-network",
    );
    expect(hit).not.toBeNull();
    expect(hit?.meta).toBe("meta-v1");
    expect(new Uint8Array(await hit!.bytes())).toEqual(new Uint8Array([1, 2, 3, 4]));
  });

  it("coalesces concurrent identical fetchRange calls into a single HTTP 206 request", async () => {
    let resolveFetch!: (res: Response) => void;
    const fetchMock = vi.fn(
      () =>
        new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const source = browserSource("");
    const file = {
      key: "processed/request/year=2026/month=11/day=21/hour=00/compact=1-2.parquet",
      start: "2026-11-21T00:00:00Z",
      end: "2026-11-21T01:00:00Z",
      size: 100,
      rows: 10,
      etag: '"e1"',
    };
    const p1 = source.fetchRange!(file, 0, 16);
    const p2 = source.fetchRange!(file, 0, 16);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    resolveFetch(
      new Response(new Uint8Array(16).fill(42), {
        status: 206,
        headers: { "content-range": "bytes 0-15/100", etag: '"e1"' },
      }),
    );
    const [b1, b2] = await Promise.all([p1, p2]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(new Uint8Array(b1)).toEqual(new Uint8Array(16).fill(42));
    expect(new Uint8Array(b2)).toEqual(new Uint8Array(16).fill(42));
    // Distinct buffers so transferring one to a Worker cannot detach the other.
    expect(b1).not.toBe(b2);
  });
});

const flushStore = () => new Promise((resolve) => setTimeout(resolve, 0));
