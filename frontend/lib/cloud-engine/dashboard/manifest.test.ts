import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearManifestCache,
  fetchManifest,
  invalidateManifestCache,
  type ManifestFile,
} from "./manifest";

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
  afterEach(() => vi.useRealTimers());

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
    await fetchManifest(fetchJson, iso(T0), iso(T0 + 5 * H));
    expect(calls).toEqual([iso(T0 + 3 * H), iso(T0)]);
  });
});
