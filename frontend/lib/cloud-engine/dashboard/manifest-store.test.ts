import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MANIFEST_SNAPSHOT_CACHE,
  MANIFEST_SNAPSHOT_MAX_AGE_MS,
  deleteManifestSnapshot,
  deleteManifestSnapshots,
  manifestSnapshotUrl,
  persistManifestSnapshot,
  readManifestSnapshot,
  type ManifestSnapshot,
} from "./manifest-store";
import { MAX_IDENTITIES } from "./catalog-manifest";
import { readStoreError } from "./file-store";

const scope = '["https://example.test","session","service"]';
const cacheKey = JSON.stringify([scope, "request", "raw", false]);
const otherCacheKey = JSON.stringify(["fetch:2", "request", "raw", false]);
const from = "2026-01-10T00:00:00.000Z";
const to = "2026-01-11T00:00:00.000Z";

function snapshot(): ManifestSnapshot {
  return {
    from,
    to,
    generation: 7,
    manifest: {
      files: [{ key: "f1", start: from, end: to, size: 10, rows: 1 }],
      complete: true,
      catalogGeneration: 7,
    },
  };
}

function fakeCaches(initial: Record<string, Record<string, Response>> = {}) {
  const entries = new Map(
    Object.entries(initial).map(([name, responses]) => [
      name,
      new Map(Object.entries(responses)),
    ]),
  );
  const deleted = vi.fn(async (name: string) => entries.delete(name));
  const open = vi.fn(async (name: string) => {
    let responses = entries.get(name);
    if (!responses) {
      responses = new Map();
      entries.set(name, responses);
    }
    return {
      match: vi.fn(async (url: string) => responses.get(url)?.clone()),
      put: vi.fn(async (url: string, response: Response) => {
        responses.set(url, response.clone());
      }),
      delete: vi.fn(async (url: string) => responses.delete(url)),
    } as unknown as Cache;
  });
  vi.stubGlobal("caches", { open, delete: deleted });
  return { entries, open, deleted };
}

afterEach(() => vi.unstubAllGlobals());

describe("manifest snapshots", () => {
  it("round-trips a complete scoped generation snapshot", async () => {
    fakeCaches();
    await persistManifestSnapshot(cacheKey, snapshot());
    expect(await readManifestSnapshot(cacheKey, Date.now())).toEqual(
      snapshot(),
    );
  });

  it.each([
    "not-json",
    JSON.stringify([scope, "request", "summary", false]),
    JSON.stringify([scope, "request", "raw", true]),
    JSON.stringify([scope, "request", "raw", false, "extra"]),
  ])("rejects non-raw-request cache scope %s", async (invalidCacheKey) => {
    const { entries } = fakeCaches();
    await persistManifestSnapshot(invalidCacheKey, snapshot());
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.size ?? 0).toBe(0);

    const url = manifestSnapshotUrl(invalidCacheKey);
    entries.set(
      MANIFEST_SNAPSHOT_CACHE,
      new Map([
        [
          url,
          new Response(
            JSON.stringify({
              formatVersion: 1,
              cacheKey: invalidCacheKey,
              savedAt: Date.now(),
              snapshot: snapshot(),
            }),
          ),
        ],
      ]),
    );
    expect(await readManifestSnapshot(invalidCacheKey)).toBeNull();
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.has(url)).toBe(false);
  });

  it.each([
    ["kind", "hour"],
    ["sources", 2],
  ])("rejects raw files carrying summary-only %s", async (field, value) => {
    const { entries } = fakeCaches();
    const summaryFile = { ...snapshot().manifest.files[0], [field]: value };
    const invalid = {
      ...snapshot(),
      manifest: { ...snapshot().manifest, files: [summaryFile] },
    };

    await persistManifestSnapshot(cacheKey, invalid);
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.size ?? 0).toBe(0);

    const url = manifestSnapshotUrl(cacheKey);
    entries.set(
      MANIFEST_SNAPSHOT_CACHE,
      new Map([
        [
          url,
          new Response(
            JSON.stringify({
              formatVersion: 1,
              cacheKey,
              savedAt: Date.now(),
              snapshot: invalid,
            }),
          ),
        ],
      ]),
    );
    expect(await readManifestSnapshot(cacheKey)).toBeNull();
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.has(url)).toBe(false);
  });

  it("rejects and removes expired, wrong-scope, incomplete, or invalid raw records", async () => {
    const { entries } = fakeCaches();
    const original = snapshot();
    const savedAt = Date.now();
    const url = manifestSnapshotUrl(cacheKey);
    const records = [
      {
        formatVersion: 1,
        cacheKey,
        savedAt: savedAt - MANIFEST_SNAPSHOT_MAX_AGE_MS,
        snapshot: original,
      },
      {
        formatVersion: 1,
        cacheKey: otherCacheKey,
        savedAt,
        snapshot: original,
      },
      {
        formatVersion: 1,
        cacheKey,
        savedAt,
        snapshot: {
          ...original,
          manifest: { ...original.manifest, complete: false },
        },
      },
      {
        formatVersion: 1,
        cacheKey,
        savedAt,
        snapshot: {
          ...original,
          manifest: { ...original.manifest, summaryFiles: [] },
        },
      },
      {
        formatVersion: 1,
        cacheKey,
        savedAt,
        snapshot: {
          ...original,
          manifest: {
            ...original.manifest,
            files: [
              {
                ...original.manifest.files[0],
                start: "not-an-instant",
              },
            ],
          },
        },
      },
      {
        formatVersion: 1,
        cacheKey,
        savedAt,
        snapshot: {
          ...original,
          manifest: {
            ...original.manifest,
            files: [null],
          },
        },
      },
    ];

    for (const record of records) {
      entries.set(
        MANIFEST_SNAPSHOT_CACHE,
        new Map([[url, new Response(JSON.stringify(record))]]),
      );
      expect(await readManifestSnapshot(cacheKey, savedAt)).toBeNull();
      expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.has(url)).toBe(false);
    }
  });

  it("rejects records over the byte and identity limits", async () => {
    const { entries } = fakeCaches();
    const savedAt = Date.now();
    const url = manifestSnapshotUrl(cacheKey);
    const original = snapshot();
    const files = [
      {
        ...original.manifest.files[0],
        key: "x".repeat(4 * 1024 * 1024),
      },
    ];
    entries.set(
      MANIFEST_SNAPSHOT_CACHE,
      new Map([
        [
          url,
          new Response(
            JSON.stringify({
              formatVersion: 1,
              cacheKey,
              savedAt,
              snapshot: {
                ...original,
                manifest: { ...original.manifest, files },
              },
            }),
          ),
        ],
      ]),
    );
    expect(await readManifestSnapshot(cacheKey, savedAt)).toBeNull();

    files[0] = { ...files[0], key: "f0" };
    files.push(
      ...Array.from({ length: MAX_IDENTITIES }, (_, index) => ({
        ...original.manifest.files[0],
        key: `f${index + 1}`,
      })),
    );
    entries.set(
      MANIFEST_SNAPSHOT_CACHE,
      new Map([
        [
          url,
          new Response(
            JSON.stringify({
              formatVersion: 1,
              cacheKey,
              savedAt,
              snapshot: {
                ...original,
                manifest: { ...original.manifest, files },
              },
            }),
          ),
        ],
      ]),
    );
    expect(await readManifestSnapshot(cacheKey, savedAt)).toBeNull();
  }, 15_000);

  it("falls back and records CacheStorage failures", async () => {
    const { entries, open } = fakeCaches();
    open.mockImplementation(async (name: string) => {
      if (name === MANIFEST_SNAPSHOT_CACHE)
        throw new Error("cache unavailable");
      let responses = entries.get(name);
      if (!responses) {
        responses = new Map();
        entries.set(name, responses);
      }
      return {
        match: vi.fn(async (url: string) => responses.get(url)?.clone()),
        put: vi.fn(async (url: string, response: Response) => {
          responses.set(url, response.clone());
        }),
        delete: vi.fn(async (url: string) => responses.delete(url)),
      } as unknown as Cache;
    });

    expect(await readManifestSnapshot(cacheKey)).toBeNull();
    expect(await readStoreError()).toMatchObject({
      cache: MANIFEST_SNAPSHOT_CACHE,
      key: `manifest:read:${cacheKey}`,
      message: "Error: cache unavailable",
    });
  });

  it("refuses invalid writes without persisting them", async () => {
    const { entries } = fakeCaches();
    await persistManifestSnapshot(cacheKey, {
      ...snapshot(),
      manifest: { ...snapshot().manifest, complete: false },
    });
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.size ?? 0).toBe(0);
    expect(await readStoreError()).toMatchObject({
      cache: MANIFEST_SNAPSHOT_CACHE,
      key: `manifest:write:${cacheKey}`,
    });
  });

  it("deletes only the selected scoped record", async () => {
    const { entries } = fakeCaches();
    await persistManifestSnapshot(cacheKey, snapshot());
    await persistManifestSnapshot(otherCacheKey, snapshot());
    expect(entries.get(MANIFEST_SNAPSHOT_CACHE)?.size).toBe(2);

    await deleteManifestSnapshot(cacheKey);

    expect(
      entries.get(MANIFEST_SNAPSHOT_CACHE)?.has(manifestSnapshotUrl(cacheKey)),
    ).toBe(false);
    expect(
      entries
        .get(MANIFEST_SNAPSHOT_CACHE)
        ?.has(manifestSnapshotUrl(otherCacheKey)),
    ).toBe(true);
  });

  it("deletes the snapshot cache namespace", async () => {
    const { deleted } = fakeCaches();
    await persistManifestSnapshot(cacheKey, snapshot());

    await deleteManifestSnapshots();

    expect(deleted).toHaveBeenCalledWith(MANIFEST_SNAPSHOT_CACHE);
  });
});
