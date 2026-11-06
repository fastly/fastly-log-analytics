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
  pruneRequestCache,
  rollupCacheVersion,
} from "./sources";
import {
  STORE_META_CACHE,
  readStoreError,
  recordStoreError,
} from "./file-store";

afterEach(() => vi.unstubAllGlobals());

describe("browserSource", () => {
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
    expect(stores.has(STORE_META_CACHE)).toBe(true);
    await clearRequestLogCaches();
    expect(stores.has(STORE_META_CACHE)).toBe(false);
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
});

const flushStore = () => new Promise((resolve) => setTimeout(resolve, 0));
