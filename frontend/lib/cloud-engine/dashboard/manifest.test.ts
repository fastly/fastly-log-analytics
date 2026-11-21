import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearManifestCache,
  fetchManifest,
  invalidateManifestCache,
  observeManifestCommit,
  type ManifestFile,
} from "./manifest";
import { SNAPSHOT_REUSE_MS } from "./catalog-manifest";

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

  it("shares one listing between concurrent callers of the same range", async () => {
    const { fetchJson, calls } = server({ files: [file(T0)], gen: 1, before: T0 });
    const args = [fetchJson, iso(T0), iso(T0 + H)] as const;
    const results = await Promise.all([
      fetchManifest(...args),
      fetchManifest(...args),
      fetchManifest(...args),
      fetchManifest(...args),
    ]);
    expect(calls).toHaveLength(1);
    for (const result of results) expect(keys(result.files)).toEqual(keys(results[0].files));
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
      return { files: [file(T0)], next_cursor: null, complete: true, catalog_generation: 1 };
    });
    return { fetchJson, count: () => ({ listings, aborted }) };
  }

  it("keeps the shared listing alive for other callers when its first caller aborts", async () => {
    const owner = new AbortController();
    const { fetchJson, count } = slowServer();
    const first = fetchManifest(fetchJson, iso(T0), iso(T0 + H), owner.signal);
    const second = fetchManifest(fetchJson, iso(T0), iso(T0 + H), new AbortController().signal);
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
    const third = await fetchManifest(fetchJson, iso(T0), iso(T0 + H), new AbortController().signal);
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
      if (page === 2) throw new DOMException("object read timed out", "TimeoutError");
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
    const recovered = await fetchManifest(
      fetchJson,
      iso(T0),
      iso(T0 + 2 * H),
    );
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
    const state = { files: [file(T0), file(T0 + 4 * H)], gen: 3, before: T0 + 3 * H };
    const { fetchJson, calls } = server(state);
    const first = await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    calls.length = 0;
    vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS - 1);
    expect(await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H))).toEqual(first);
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
});
