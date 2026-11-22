import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearManifestCache,
  fetchManifest,
  invalidateManifestCache,
  observeManifestCommit,
  type ManifestFile,
} from "./manifest";
import { ManifestRequestError, SNAPSHOT_REUSE_MS } from "./catalog-manifest";
import { manifestSnapshotUrl, persistManifestSnapshot } from "./manifest-store";

const H = 3_600_000;
const T0 = Date.parse("2026-01-10T00:00:00Z");
const iso = (ms: number) => new Date(ms).toISOString();

function file(startMs: number, minutes = 1): ManifestFile {
  return {
    key: `f-${startMs}`,
    start: iso(startMs),
    end: iso(startMs + minutes * 60_000),
    size: 10,
    rows: 1,
  };
}

function fakeManifestCacheStorage() {
  const entries = new Map<string, Response>();
  const cache = {
    match: vi.fn(async (url: string) => entries.get(url)?.clone()),
    put: vi.fn(async (url: string, value: Response) => {
      entries.set(url, value.clone());
    }),
    delete: vi.fn(async (url: string) => entries.delete(url)),
  } as unknown as Cache;
  const storage = {
    open: vi.fn(async () => cache),
    delete: vi.fn(async () => true),
  };
  vi.stubGlobal("caches", storage);
  return { cache, entries, storage };
}

/** Serves every listed file overlapping the query, with the server's cache hint. */
function server(state: {
  files: ManifestFile[];
  gen: number;
  before: number;
  from?: number;
}) {
  const calls: string[] = [];
  const fetchJson = vi.fn(async (url: string) => {
    const params = new URL(url, "http://x").searchParams;
    const from = Date.parse(params.get("from")!);
    const to = Date.parse(params.get("to")!);
    calls.push(params.get("from")!);
    return {
      files: state.files.filter(
        (f) => Date.parse(f.end) > from && Date.parse(f.start) < to,
      ),
      next_cursor: null,
      complete: true,
      cache: {
        gen: state.gen,
        from: iso(state.from ?? T0 - 30 * 24 * H),
        before: iso(state.before),
      },
    };
  });
  return { fetchJson, calls };
}

const keys = (files: ManifestFile[]) => files.map((f) => f.key).sort();

describe("closed-hour manifest cache", () => {
  beforeEach(() => {
    clearManifestCache();
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0 + 6 * H);
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("hydrates a complete persisted range and uses a delta instead of re-listing", async () => {
    fakeManifestCacheStorage();
    const original = file(T0);
    const entered = file(T0 + 2 * H);
    const calls: URLSearchParams[] = [];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        calls.push(params);
        if (!params.has("since_generation")) {
          return {
            files: [original],
            next_cursor: null,
            complete: true,
            catalog_generation: 7,
          };
        }
        expect(params.get("since_generation")).toBe("7");
        expect(params.get("previous_from")).toBe(iso(T0));
        expect(params.get("previous_to")).toBe(iso(T0 + 2 * H));
        return {
          files: [entered],
          changes: [],
          summary_partitions: [],
          base_generation: 7,
          catalog_generation: 8,
          next_cursor: null,
          complete: true,
        };
      }),
      {
        manifestCacheScope: () => '["session","service"]',
      },
    );

    await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    vi.resetModules();
    const { fetchManifest: fetchAfterReload } = await import("./manifest");
    calls.length = 0;
    const result = await fetchAfterReload(
      fetchJson,
      iso(T0 + 60_000),
      iso(T0 + 2 * H + 60_000),
    );
    expect(calls).toHaveLength(1);
    expect(calls[0].get("since_generation")).toBe("7");
    expect(keys(result.files)).toEqual(keys([entered]));
  });

  it("hydrates a persisted listing with summaries and keeps them through a summaries delta", async () => {
    fakeManifestCacheStorage();
    const original = file(T0);
    const entered = file(T0 + 2 * H);
    const summary = { ...file(T0, 60), key: "summary-hour", kind: "hour" as const };
    const calls: URLSearchParams[] = [];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        calls.push(params);
        expect(params.get("summaries")).toBe("1");
        if (!params.has("since_generation")) {
          return {
            files: [original],
            summary_files: [summary],
            next_cursor: null,
            complete: true,
            catalog_generation: 7,
          };
        }
        return {
          files: [entered],
          summary_files: [],
          changes: [],
          summary_partitions: [],
          base_generation: 7,
          catalog_generation: 8,
          next_cursor: null,
          complete: true,
        };
      }),
      { manifestCacheScope: () => '["session","service"]' },
    );
    const range = [iso(T0), iso(T0 + 3 * H)] as const;

    await fetchManifest(fetchJson, ...range, undefined, undefined, undefined, true);
    vi.resetModules();
    const { fetchManifest: fetchAfterReload } = await import("./manifest");
    calls.length = 0;
    const result = await fetchAfterReload(
      fetchJson,
      ...range,
      undefined,
      undefined,
      undefined,
      true,
    );
    expect(calls).toHaveLength(1);
    expect(calls[0].get("since_generation")).toBe("7");
    expect(keys(result.files)).toEqual(keys([original, entered]));
    expect(result.summaryFiles).toEqual([summary]);
  });

  it("replaces a day's windows with an open-hour bundle and its uncovered windows through a summaries delta and reload", async () => {
    fakeManifestCacheStorage();
    const raw = file(T0);
    const window = (minute: number) => ({
      ...file(T0 + minute * 60_000, 5),
      key: `processed/summary/request/w-${minute}.parquet`,
      kind: "window" as const,
    });
    const [w0, w5, w10] = [window(0), window(5), window(10)];
    const bundle = {
      ...file(T0, 10),
      key: "processed/summary_bundle/request/year=2026/month=01/day=10/open_bundle=00-1-c2-n2-h0.parquet",
      kind: "hour" as const,
      sources: 2,
    };
    const calls: URLSearchParams[] = [];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        calls.push(params);
        if (!params.has("since_generation")) {
          return {
            files: [raw],
            summary_files: [w0, w5, w10],
            next_cursor: null,
            complete: true,
            catalog_generation: 7,
          };
        }
        const base = Number(params.get("since_generation"));
        return {
          files: [],
          summary_files: base === 7 ? [bundle, w10] : [],
          changes: [],
          summary_partitions:
            base === 7 ? [{ from: iso(T0), to: iso(T0 + 24 * H) }] : [],
          base_generation: base,
          catalog_generation: 8,
          next_cursor: null,
          complete: true,
        };
      }),
      { manifestCacheScope: () => '["session","service"]' },
    );
    const range = [iso(T0), iso(T0 + 3 * H)] as const;
    const list = (fetch: typeof fetchManifest) =>
      fetch(fetchJson, ...range, undefined, undefined, undefined, true);

    await list(fetchManifest);
    for (const since of ["7", "8"]) {
      vi.resetModules();
      const { fetchManifest: fetchAfterReload } = await import("./manifest");
      calls.length = 0;
      const result = await list(fetchAfterReload);
      expect(calls.map((c) => c.get("since_generation"))).toEqual([since]);
      expect(keys(result.summaryFiles!)).toEqual(keys([bundle, w10]));
    }
  });

  it("re-lists instead of replaying a summaries delta onto a listing that omitted summaries", async () => {
    const calls: URLSearchParams[] = [];
    const summary = { ...file(T0, 60), key: "summary-hour", kind: "hour" as const };
    const fetchJson = vi.fn(async (input: string) => {
      const params = new URL(input, "http://x").searchParams;
      calls.push(params);
      return {
        files: [file(T0)],
        ...(calls.length > 1 ? { summary_files: [summary] } : {}),
        next_cursor: null,
        complete: true,
        catalog_generation: 7,
      };
    });
    const args = [fetchJson, iso(T0), iso(T0 + 3 * H), undefined, undefined, undefined, true] as const;

    expect((await fetchManifest(...args)).summaryFiles).toBeUndefined();
    vi.setSystemTime(T0 + 6 * H + SNAPSHOT_REUSE_MS + 1);
    const result = await fetchManifest(...args);
    expect(calls).toHaveLength(2);
    expect(calls[1].has("since_generation")).toBe(false);
    expect(result.summaryFiles).toEqual([summary]);
  });

  it("clears persisted snapshots on an explicit manifest cache clear", async () => {
    const { storage } = fakeManifestCacheStorage();
    const state = { files: [file(T0)], gen: 1, before: T0 };
    const { fetchJson, calls } = server(state);

    await fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    clearManifestCache();
    await fetchManifest(fetchJson, iso(T0), iso(T0 + H));

    expect(storage.delete).toHaveBeenCalled();
    expect(calls).toEqual([iso(T0), iso(T0)]);
  });

  it("waits to invalidate only its raw persisted scope on resync", async () => {
    const { cache, entries } = fakeManifestCacheStorage();
    const scope = '["persisted-resync-scope","service"]';
    const cacheKey = JSON.stringify([scope, "request", "raw", false]);
    const otherScopeKey = JSON.stringify([
      '["persisted-resync-other","service"]',
      "request",
      "raw",
      false,
    ]);
    const stale = {
      from: iso(T0),
      to: iso(T0 + 2 * H),
      generation: 7,
      manifest: {
        files: [file(T0)],
        complete: true,
        catalogGeneration: 7,
      },
    };
    await persistManifestSnapshot(cacheKey, stale);
    await persistManifestSnapshot(otherScopeKey, stale);

    let releaseDelete = () => {};
    const deletion = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    const originalDelete = entries.delete.bind(entries);
    cache.delete = vi.fn(async (url: string) => {
      await deletion;
      return originalDelete(url);
    }) as unknown as Cache["delete"];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        expect(params.has("since_generation")).toBe(false);
        return {
          files: [file(T0), file(T0 + H)],
          next_cursor: null,
          complete: true,
          catalog_generation: 9,
        };
      }),
      { manifestCacheScope: () => scope },
    );

    observeManifestCommit(fetchJson, { type: "resync", generation: 9 });
    const fetching = fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    await Promise.resolve();
    expect(fetchJson).not.toHaveBeenCalled();

    releaseDelete();
    await expect(fetching).resolves.toMatchObject({ catalogGeneration: 9 });
    expect(cache.delete).toHaveBeenCalledWith(manifestSnapshotUrl(cacheKey));
    expect(entries.has(manifestSnapshotUrl(otherScopeKey))).toBe(true);
  });

  it("drains overlapping resync invalidations before fetching", async () => {
    const { cache } = fakeManifestCacheStorage();
    const releases: Array<() => void> = [];
    const starts: Array<() => void> = [];
    const scope = '["overlapping-resync","service"]';
    // Gates the raw listing's snapshot; its summaries twin deletes at once.
    cache.delete = vi.fn((url: string) =>
      decodeURIComponent(url).includes(',true]')
        ? Promise.resolve(true)
        : new Promise<boolean>((resolve) => {
            releases.push(() => resolve(true));
            starts.shift()?.();
          }),
    ) as unknown as Cache["delete"];
    const fetchJson = Object.assign(
      vi.fn(async () => ({
        files: [file(T0)],
        next_cursor: null,
        complete: true,
        catalog_generation: 9,
      })),
      { manifestCacheScope: () => scope },
    );

    observeManifestCommit(fetchJson, { type: "resync", generation: 8 });
    const firstStarted = new Promise<void>((resolve) => starts.push(resolve));
    const fetching = fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    await firstStarted;
    observeManifestCommit(fetchJson, { type: "resync", generation: 9 });
    releases.shift()!();
    const secondStarted = new Promise<void>((resolve) => starts.push(resolve));
    await secondStarted;
    await Promise.resolve();
    await Promise.resolve();
    expect(fetchJson).not.toHaveBeenCalled();

    releases.shift()!();
    await expect(fetching).resolves.toMatchObject({ catalogGeneration: 9 });
    expect(cache.delete).toHaveBeenCalledTimes(4);
  });

  it("does not reuse a recent listing after an equal-generation resync", async () => {
    let listings = 0;
    const fetchJson = Object.assign(
      vi.fn(async () => {
        listings++;
        return {
          files: [file(T0 + listings * 60_000)],
          next_cursor: null,
          complete: true,
          catalog_generation: 7,
        };
      }),
      { manifestCacheScope: () => '["equal-resync-recent","service"]' },
    );

    await fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    observeManifestCommit(fetchJson, { type: "resync", generation: 7 });
    const result = await fetchManifest(fetchJson, iso(T0), iso(T0 + H));

    expect(listings).toBe(2);
    expect(result.files[0].key).toBe(file(T0 + 2 * 60_000).key);
  });

  it("does not join an in-flight listing after an equal-generation resync", async () => {
    const resolvers: Array<(manifest: object) => void> = [];
    const fetchJson = Object.assign(
      vi.fn(
        () =>
          new Promise<object>((resolve) => {
            resolvers.push(resolve);
          }),
      ),
      { manifestCacheScope: () => '["equal-resync-flight","service"]' },
    );
    const first = fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    await vi.waitFor(() => expect(fetchJson).toHaveBeenCalledTimes(1));

    observeManifestCommit(fetchJson, { type: "resync", generation: 7 });
    const second = fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    await vi.waitFor(() => expect(fetchJson).toHaveBeenCalledTimes(2));

    resolvers[1]({
      files: [file(T0 + H / 2)],
      next_cursor: null,
      complete: true,
      catalog_generation: 7,
    });
    await expect(second).resolves.toMatchObject({ catalogGeneration: 7 });

    resolvers[0]({
      files: [file(T0)],
      next_cursor: null,
      complete: true,
      catalog_generation: 7,
    });
    await expect(first).rejects.toThrow("cache identity changed");
  });

  it("bypasses persisted hydration when resync deletion fails", async () => {
    const { cache } = fakeManifestCacheStorage();
    const scope = '["persisted-delete-failed","service"]';
    const cacheKey = JSON.stringify([scope, "request", "raw", false]);
    await persistManifestSnapshot(cacheKey, {
      from: iso(T0),
      to: iso(T0 + 2 * H),
      generation: 7,
      manifest: {
        files: [file(T0)],
        complete: true,
        catalogGeneration: 7,
      },
    });
    cache.delete = vi
      .fn()
      .mockRejectedValue(new Error("cache delete unavailable"));
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        expect(params.has("since_generation")).toBe(false);
        return {
          files: [file(T0 + H)],
          next_cursor: null,
          complete: true,
          catalog_generation: 9,
        };
      }),
      { manifestCacheScope: () => scope },
    );

    observeManifestCommit(fetchJson, { type: "resync", generation: 9 });
    const result = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));

    expect(keys(result.files)).toEqual(keys([file(T0 + H)]));
  });

  it("orders an older persist before resync invalidation", async () => {
    const { cache, entries } = fakeManifestCacheStorage();
    const scope = '["persist-before-resync","service"]';
    const cacheKey = JSON.stringify([scope, "request", "raw", false]);
    const operations: string[] = [];
    let releasePut = () => {};
    const putBlocked = new Promise<void>((resolve) => {
      releasePut = resolve;
    });
    let putStarted = () => {};
    const started = new Promise<void>((resolve) => {
      putStarted = resolve;
    });
    const originalPut = cache.put.bind(cache);
    const originalDelete = cache.delete.bind(cache);
    cache.put = vi.fn(async (url: string, value: Response) => {
      operations.push("put:start");
      putStarted();
      await putBlocked;
      await originalPut(url, value);
      operations.push("put:end");
    }) as unknown as Cache["put"];
    cache.delete = vi.fn(async (url: string) => {
      operations.push("delete");
      return originalDelete(url);
    }) as unknown as Cache["delete"];
    const fetchJson = Object.assign(
      vi.fn(async () => ({
        files: [file(T0)],
        next_cursor: null,
        complete: true,
        catalog_generation: 7,
      })),
      { manifestCacheScope: () => scope },
    );

    const listing = fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    await started;
    observeManifestCommit(fetchJson, { type: "resync", generation: 7 });
    releasePut();

    await expect(listing).rejects.toThrow("cache identity changed");
    expect(operations).toEqual(["put:start", "put:end", "delete", "delete"]);
    expect(entries.has(manifestSnapshotUrl(cacheKey))).toBe(false);
  });

  it("aborts promptly while persistent invalidation is pending", async () => {
    const { cache } = fakeManifestCacheStorage();
    let releaseDelete = () => {};
    const deletion = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    cache.delete = vi.fn(async () => {
      await deletion;
      return true;
    }) as unknown as Cache["delete"];
    const fetchJson = Object.assign(vi.fn(), {
      manifestCacheScope: () => '["abort-invalidation","service"]',
    });
    const owner = new AbortController();

    observeManifestCommit(fetchJson, { type: "resync", generation: 7 });
    const listing = fetchManifest(
      fetchJson,
      iso(T0),
      iso(T0 + H),
      owner.signal,
    );
    owner.abort();

    await expect(listing).rejects.toMatchObject({ name: "AbortError" });
    expect(fetchJson).not.toHaveBeenCalled();
    releaseDelete();
  });

  it("replaces a stale persisted base after catalog resync", async () => {
    fakeManifestCacheStorage();
    const scope = '["persisted-resync","service"]';
    const cacheKey = JSON.stringify([scope, "request", "raw", false]);
    await persistManifestSnapshot(cacheKey, {
      from: iso(T0),
      to: iso(T0 + 2 * H),
      generation: 7,
      manifest: {
        files: [file(T0)],
        complete: true,
        catalogGeneration: 7,
      },
    });
    let fullLists = 0;
    let deltaLists = 0;
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        if (!params.has("since_generation")) {
          fullLists++;
          return {
            files: [file(T0), file(T0 + H)],
            next_cursor: null,
            complete: true,
            catalog_generation: 9,
          };
        }
        deltaLists++;
        if (deltaLists === 1) {
          throw new ManifestRequestError(409, "catalog_resync_required");
        }
        return {
          files: [],
          changes: [],
          summary_partitions: [],
          base_generation: 9,
          catalog_generation: 9,
          next_cursor: null,
          complete: true,
        };
      }),
      { manifestCacheScope: () => scope },
    );

    vi.resetModules();
    const { fetchManifest: fetchAfterReload } = await import("./manifest");
    const current = await fetchAfterReload(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(current.catalogGeneration).toBe(9);
    expect(deltaLists).toBe(1);
    expect(fullLists).toBe(1);

    vi.resetModules();
    const { fetchManifest: fetchAfterSecondReload } =
      await import("./manifest");
    await fetchAfterSecondReload(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(deltaLists).toBe(2);
    expect(fullLists).toBe(1);
  });

  it("falls back to a full listing when CacheStorage read rejects", async () => {
    vi.stubGlobal("caches", {
      open: vi
        .fn()
        .mockRejectedValue(new DOMException("denied", "SecurityError")),
    });
    const { fetchJson, calls } = server({
      files: [file(T0)],
      gen: 1,
      before: T0,
    });

    await expect(
      fetchManifest(fetchJson, iso(T0), iso(T0 + H)),
    ).resolves.toBeDefined();
    expect(calls).toEqual([iso(T0)]);
  });

  it("preserves network errors after CacheStorage read rejection", async () => {
    vi.stubGlobal("caches", {
      open: vi
        .fn()
        .mockRejectedValue(new DOMException("denied", "SecurityError")),
    });
    const networkError = new Error("manifest network failed");
    const fetchJson = vi.fn().mockRejectedValue(networkError);

    await expect(fetchManifest(fetchJson, iso(T0), iso(T0 + H))).rejects.toBe(
      networkError,
    );
  });

  it("does not let hydrated delta failures consume snapshot capacity", async () => {
    const { entries } = fakeManifestCacheStorage();
    const original = file(T0);
    const entered = file(T0 + 2 * H);
    const calls: URLSearchParams[] = [];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        calls.push(params);
        if (!params.has("since_generation")) {
          return {
            files: [original],
            next_cursor: null,
            complete: true,
            catalog_generation: 7,
          };
        }
        return {
          files: [entered],
          changes: [],
          summary_partitions: [],
          base_generation: 7,
          catalog_generation: 8,
          next_cursor: null,
          complete: true,
        };
      }),
      { manifestCacheScope: () => '["installed","service"]' },
    );

    await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));

    for (let index = 0; index < 8; index++) {
      const scope = `["hydrated-failure-${index}","service"]`;
      await persistManifestSnapshot(
        JSON.stringify([scope, "request", "raw", false]),
        {
          from: iso(T0),
          to: iso(T0 + 2 * H),
          generation: 7,
          manifest: {
            files: [original],
            complete: true,
            catalogGeneration: 7,
          },
        },
      );
      const failing = Object.assign(
        vi.fn(async () => {
          throw new Error("delta failed");
        }),
        { manifestCacheScope: () => scope },
      );
      await expect(
        fetchManifest(failing, iso(T0), iso(T0 + 2 * H)),
      ).rejects.toThrow("delta failed");
    }

    entries.clear();
    const installed = Object.assign(
      vi.fn(async () => ({
        files: [original],
        next_cursor: null,
        complete: true,
        catalog_generation: 7,
      })),
      { manifestCacheScope: () => '["installed-after-failures","service"]' },
    );
    await fetchManifest(installed, iso(T0), iso(T0 + 2 * H));

    calls.length = 0;
    const result = await fetchManifest(
      fetchJson,
      iso(T0 + 60_000),
      iso(T0 + 2 * H + 60_000),
    );

    expect(calls).toHaveLength(1);
    expect(calls[0].get("since_generation")).toBe("7");
    expect(keys(result.files)).toEqual(keys([entered]));
  });

  it("follows empty catalog pages and merges every page's inline summaries", async () => {
    const raw = file(T0);
    const first = { ...file(T0), key: "summary-first" };
    const second = { ...file(T0 + H), key: "summary-second" };
    const fetchJson = vi
      .fn()
      .mockResolvedValueOnce({
        files: [],
        next_cursor: "page-2",
        complete: false,
        catalog_generation: 7,
        summary_files: [first],
      })
      .mockResolvedValueOnce({
        files: [raw],
        next_cursor: "page-3",
        complete: false,
        catalog_generation: 7,
        summary_files: [first, second],
      })
      .mockResolvedValueOnce({
        files: [],
        next_cursor: null,
        complete: true,
        catalog_generation: 7,
        summary_files: [],
      });
    const result = await fetchManifest(
      fetchJson,
      iso(T0),
      iso(T0 + 2 * H),
      undefined,
      undefined,
      undefined,
      true,
    );
    expect(result.complete).toBe(true);
    expect(result.files).toEqual([raw]);
    expect(result.summaryFiles).toEqual([first, second]);
    expect(fetchJson).toHaveBeenCalledTimes(3);
    for (const [url] of fetchJson.mock.calls) {
      expect(new URL(url, "http://x").searchParams.get("summaries")).toBe("1");
    }
  });

  it("serves a raw-only caller from a concurrent or just-taken listing with summaries", async () => {
    const summary = { ...file(T0, 60), key: "summary-hour", kind: "hour" as const };
    const fetchJson = vi.fn(async () => ({
      files: [file(T0)],
      summary_files: [summary],
      next_cursor: null,
      complete: true,
      catalog_generation: 7,
    }));
    const range = [iso(T0), iso(T0 + 3 * H)] as const;
    const [withSummaries, concurrent] = await Promise.all([
      fetchManifest(fetchJson, ...range, undefined, undefined, undefined, true),
      fetchManifest(fetchJson, ...range),
    ]);
    const after = await fetchManifest(fetchJson, ...range);
    expect(fetchJson).toHaveBeenCalledTimes(1);
    expect(withSummaries.summaryFiles).toEqual([summary]);
    for (const raw of [concurrent, after]) {
      expect(keys(raw.files)).toEqual([file(T0).key]);
      expect(raw.summaryFiles).toBeUndefined();
    }
  });

  it("shares one listing between concurrent callers of the same range", async () => {
    const { fetchJson, calls } = server({
      files: [file(T0)],
      gen: 1,
      before: T0,
    });
    const args = [fetchJson, iso(T0), iso(T0 + H)] as const;
    const results = await Promise.all([
      fetchManifest(...args),
      fetchManifest(...args),
      fetchManifest(...args),
      fetchManifest(...args),
    ]);
    expect(calls).toHaveLength(1);
    for (const result of results)
      expect(keys(result.files)).toEqual(keys(results[0].files));
  });

  function slowServer() {
    let listings = 0;
    let aborted = 0;
    const fetchJson = vi.fn(async (_url: string, signal?: AbortSignal) => {
      listings++;
      await new Promise((resolve) => setTimeout(resolve, 5));
      if (signal?.aborted) {
        aborted++;
        throw Object.assign(new Error("aborted"), { name: "AbortError" });
      }
      return {
        files: [file(T0)],
        next_cursor: null,
        complete: true,
        catalog_generation: 1,
      };
    });
    return { fetchJson, count: () => ({ listings, aborted }) };
  }

  it("keeps the shared listing alive for other callers when its first caller aborts", async () => {
    const owner = new AbortController();
    const { fetchJson, count } = slowServer();
    const first = fetchManifest(fetchJson, iso(T0), iso(T0 + H), owner.signal);
    const second = fetchManifest(
      fetchJson,
      iso(T0),
      iso(T0 + H),
      new AbortController().signal,
    );
    owner.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });
    expect(keys((await second).files)).toEqual(keys([file(T0)]));
    expect(count()).toEqual({ listings: 1, aborted: 0 });
  });

  it("aborts the shared listing only once every caller has aborted", async () => {
    const a = new AbortController();
    const b = new AbortController();
    const { fetchJson, count } = slowServer();
    const first = fetchManifest(fetchJson, iso(T0), iso(T0 + H), a.signal);
    const second = fetchManifest(fetchJson, iso(T0), iso(T0 + H), b.signal);
    a.abort();
    b.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });
    await expect(second).rejects.toMatchObject({ name: "AbortError" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(count()).toEqual({ listings: 1, aborted: 1 });
    const third = await fetchManifest(
      fetchJson,
      iso(T0),
      iso(T0 + H),
      new AbortController().signal,
    );
    expect(keys(third.files)).toEqual(keys([file(T0)]));
    expect(count().listings).toBe(2);
  });

  it("rejects pages from different committed catalog generations", async () => {
    const fetchJson = vi
      .fn()
      .mockResolvedValueOnce({
        files: [file(T0)],
        next_cursor: "page-2",
        complete: false,
        catalog_generation: 7,
      })
      .mockResolvedValueOnce({
        files: [file(T0 + H)],
        next_cursor: null,
        complete: true,
        catalog_generation: 8,
      });
    await expect(
      fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H)),
    ).rejects.toThrow("generation changed");
  });

  it("rejects an incomplete terminal page rather than returning an empty success", async () => {
    const fetchJson = vi.fn().mockResolvedValue({
      files: [],
      next_cursor: null,
      complete: false,
      catalog_generation: 7,
    });

    await expect(
      fetchManifest(fetchJson, iso(T0), iso(T0 + H)),
    ).rejects.toThrow("incomplete");
  });

  it("restarts after a paged read timeout without reusing the partial generation", async () => {
    const first = file(T0);
    const second = file(T0 + H);
    let page = 0;
    const fetchJson = vi.fn(async (url: string) => {
      const cursor = new URL(url, "http://x").searchParams.get("cursor");
      page += 1;
      if (page === 1) {
        return {
          files: [first],
          next_cursor: "page-2",
          complete: false,
          catalog_generation: 7,
        };
      }
      if (page === 2)
        throw new DOMException("object read timed out", "TimeoutError");
      expect(cursor).toBeNull();
      return {
        files: [first, second],
        next_cursor: null,
        complete: true,
        catalog_generation: 8,
      };
    });

    await expect(
      fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H)),
    ).rejects.toMatchObject({ name: "TimeoutError" });
    const recovered = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(keys(recovered.files)).toEqual(keys([first, second]));
    expect(recovered.catalogGeneration).toBe(8);
  });

  it("re-lists only from the end of the closed hours while the generation holds", async () => {
    const state = {
      files: [file(T0), file(T0 + H + 5 * 60_000), file(T0 + 4 * H)],
      gen: 3,
      before: T0 + 3 * H,
    };
    const { fetchJson, calls } = server(state);
    const first = await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(keys(first.files)).toHaveLength(3);

    state.files.push(file(T0 + 4 * H + 30 * 60_000));
    calls.length = 0;
    const second = await fetchManifest(
      fetchJson,
      iso(T0 + 60_000),
      iso(T0 + 5 * H + 60_000),
    );
    expect(calls).toEqual([iso(T0 + 3 * H)]);
    expect(second.complete).toBe(true);
    // The minute-0 file left the sliding range; everything else is kept.
    expect(keys(second.files)).toEqual(keys(state.files.slice(1)));
  });

  it("re-lists everything when a late commit moved the generation", async () => {
    const state = {
      files: [file(T0), file(T0 + 4 * H)],
      gen: 3,
      before: T0 + 3 * H,
    };
    const { fetchJson, calls } = server(state);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));

    state.files.push(file(T0 + 2 * H));
    state.gen = 4;
    calls.length = 0;
    vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS);
    const second = await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0 + 3 * H), iso(T0)]);
    expect(keys(second.files)).toEqual(keys(state.files));
  });

  it("rediscovers a wholly closed range after a commit notification", async () => {
    const state = { files: [file(T0)], gen: 3, before: T0 + 3 * H };
    const { fetchJson, calls } = server(state);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    state.files.push(file(T0 + H));
    state.gen++;
    calls.length = 0;
    invalidateManifestCache();
    const current = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(calls).toEqual([iso(T0)]);
    expect(keys(current.files)).toEqual(keys(state.files));
  });

  it("ignores listings without a hint, other datasets and expired segments", async () => {
    const state = { files: [file(T0)], gen: 1, before: T0 + 3 * H };
    const { fetchJson, calls } = server(state);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H), undefined, "rum");
    calls.length = 0;
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0)]);

    vi.setSystemTime(T0 + 8 * H);
    calls.length = 0;
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0)]);

    clearManifestCache();
    calls.length = 0;
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0)]);
  });

  it("does not reuse closed hours that retention may have removed since", async () => {
    const state = {
      files: [file(T0), file(T0 + 4 * H)],
      gen: 2,
      before: T0 + 3 * H,
      from: T0,
    };
    const { fetchJson, calls } = server(state);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    state.from = T0 + H;
    calls.length = 0;
    vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0 + 3 * H), iso(T0)]);
  });

  it("serves a just-taken snapshot of the same range without another listing", async () => {
    const state = {
      files: [file(T0), file(T0 + 4 * H)],
      gen: 3,
      before: T0 + 3 * H,
    };
    const { fetchJson, calls } = server(state);
    const first = await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    calls.length = 0;
    vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS - 1);
    expect(await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H))).toEqual(
      first,
    );
    expect(calls).toEqual([]);
    // A different range or a snapshot past the window lists again.
    await fetchManifest(fetchJson, iso(T0 + 60_000), iso(T0 + 5 * H + 60_000));
    expect(calls).toEqual([iso(T0 + 3 * H)]);
    calls.length = 0;
    vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS);
    await fetchManifest(fetchJson, iso(T0 + 60_000), iso(T0 + 5 * H + 60_000));
    expect(calls).toEqual([iso(T0 + 3 * H)]);
  });

  it("does not serve a young snapshot that a commit notification has moved past", async () => {
    const state = { files: [file(T0)], gen: 3, before: T0 + 3 * H };
    const { fetchJson, calls } = server(state);
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    state.files.push(file(T0 + H));
    state.gen = 4;
    calls.length = 0;
    observeManifestCommit(fetchJson, { type: "commit", generation: 4 });
    const current = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(calls).not.toEqual([]);
    expect(keys(current.files)).toEqual(keys(state.files));
  });

  it("rejects in-flight discovery whose cache identity changed while waiting on store invalidation", async () => {
    let releaseDelete!: () => void;
    const deleteGate = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    const entries = new Map<string, Response>();
    const cache = {
      match: vi.fn(async (url: string) => entries.get(url)?.clone()),
      put: vi.fn(async (url: string, value: Response) => {
        entries.set(url, value.clone());
      }),
      delete: vi.fn(async (url: string) => entries.delete(url)),
    } as unknown as Cache;
    let firstDelete = true;
    vi.stubGlobal("caches", {
      open: vi.fn(async () => cache),
      delete: vi.fn(async () => {
        if (firstDelete) {
          firstDelete = false;
          await deleteGate;
        }
        entries.clear();
        return true;
      }),
    });

    let activeScope = '["https://origin","user-a","svc-a"]';
    const fetchJson = Object.assign(
      vi.fn(async () => ({
        files: [
          {
            key: "user-a-only/processed/request/year=2026/secret-a.parquet",
            start: iso(T0),
            end: iso(T0 + 60_000),
            size: 10,
            rows: 1,
          },
        ],
        next_cursor: null,
        complete: true,
        catalog_generation: 5,
      })),
      {
        manifestCacheScope: () => activeScope,
      },
    );

    // Start an initial store invalidation whose CacheStorage.delete is still in-flight.
    clearManifestCache();
    // User A starts fetchManifest while pendingStoreInvalidation is active.
    const userAPromise = fetchManifest(fetchJson, iso(T0), iso(T0 + H));
    // Before CacheStorage.delete finishes, User A signs out / switches to User B.
    activeScope = '["https://origin","user-b","svc-a"]';
    clearManifestCache();
    releaseDelete();

    await expect(userAPromise).rejects.toThrow(
      /File manifest cache identity changed during discovery/,
    );
    const userBKey = JSON.stringify([activeScope, "request", "raw", false]);
    expect(entries.has(manifestSnapshotUrl(userBKey))).toBe(false);
  });

  it("skips doomed delta calls when observed catalog generation is more than 32 commits ahead of persisted snapshot", async () => {
    const { entries } = fakeManifestCacheStorage();
    const scope = '["https://origin","user-stale","svc-a"]';
    const key = JSON.stringify([scope, "request", "raw", false]);
    await persistManifestSnapshot(key, {
      from: iso(T0),
      to: iso(T0 + H),
      generation: 10,
      manifest: {
        files: [file(T0)],
        complete: true,
        catalogGeneration: 10,
      },
    });
    expect(entries.has(manifestSnapshotUrl(key))).toBe(true);

    const calls: URLSearchParams[] = [];
    const fetchJson = Object.assign(
      vi.fn(async (input: string) => {
        const params = new URL(input, "http://x").searchParams;
        calls.push(params);
        return {
          files: [file(T0 + H)],
          next_cursor: null,
          complete: true,
          catalog_generation: 80,
        };
      }),
      { manifestCacheScope: () => scope },
    );

    observeManifestCommit(fetchJson, { type: "ready", generation: 80 });
    const manifest = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(manifest.catalogGeneration).toBe(80);
    expect(calls).toHaveLength(1);
    expect(calls[0].has("since_generation")).toBe(false);
  });

  it("restarts paginated full discovery once when a pagination cursor expires with 409", async () => {
    let attempts = 0;
    const fetchJson = vi.fn(async (input: string) => {
      const params = new URL(input, "http://x").searchParams;
      attempts++;
      if (attempts === 1) {
        return {
          files: [file(T0)],
          next_cursor: "expired-cursor",
          complete: false,
          catalog_generation: 11,
        };
      }
      if (params.get("cursor") === "expired-cursor") {
        throw new ManifestRequestError(409, "Catalog snapshot expired; restart discovery");
      }
      return {
        files: [file(T0), file(T0 + H)],
        next_cursor: null,
        complete: true,
        catalog_generation: 12,
      };
    });

    const manifest = await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
    expect(manifest.catalogGeneration).toBe(12);
    expect(keys(manifest.files)).toEqual(keys([file(T0), file(T0 + H)]));
    expect(attempts).toBe(3);
  });
});
