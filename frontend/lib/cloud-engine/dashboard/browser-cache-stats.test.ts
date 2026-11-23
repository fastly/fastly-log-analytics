import { afterEach, describe, expect, it, vi } from "vitest";

import {
  clearAllBrowserCaches,
  inspectBrowserCaches,
  labelForCacheBucket,
} from "./browser-cache-stats";
import { OPFS_DIRECTORY } from "./store-index";

function makeFakeCaches(
  initial: Record<string, Record<string, Response>> = {},
) {
  const norm = (u: string) => {
    const parsed = new URL(u, "http://localhost");
    return `${parsed.pathname}${parsed.search}`;
  };
  const stores = new Map<string, Map<string, Response>>();
  for (const [name, entries] of Object.entries(initial)) {
    const m = new Map<string, Response>();
    for (const [k, v] of Object.entries(entries)) m.set(norm(k), v);
    stores.set(name, m);
  }
  return {
    stores,
    api: {
      keys: vi.fn(async () => [...stores.keys()]),
      open: vi.fn(async (name: string) => {
        if (!stores.has(name)) stores.set(name, new Map());
        const map = stores.get(name)!;
        return {
          keys: vi.fn(async () =>
            [...map.keys()].map((url) => new Request(`http://localhost${url}`)),
          ),
          match: vi.fn(async (req: Request | string) => {
            const url = typeof req === "string" ? req : req.url;
            return map.get(norm(url))?.clone();
          }),
          put: vi.fn(async (req: Request | string, res: Response) => {
            const url = typeof req === "string" ? req : req.url;
            map.set(norm(url), res);
          }),
          delete: vi.fn(async (req: Request | string) => {
            const url = typeof req === "string" ? req : req.url;
            return map.delete(norm(url));
          }),
        };
      }),
      delete: vi.fn(async (name: string) => stores.delete(name)),
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("browser-cache-stats", () => {
  it("labels known and dynamic cache buckets", () => {
    expect(labelForCacheBucket("flp-parquet-v1")).toBe("Request Parquet Cache");
    expect(labelForCacheBucket("flp-parquet-rollup-v4")).toBe(
      "Pre-Aggregated Rollup Cache",
    );
    expect(labelForCacheBucket("flp-ops-parquet-v1")).toBe(
      "Operational Telemetry Cache",
    );
    expect(labelForCacheBucket("custom-cache")).toBe("custom-cache");
  });

  it("summarizes OPFS, CacheStorage buckets, storage quota, and last store error", async () => {
    const metaPayload = JSON.stringify({
      "svc/a.parquet": [1700000000000, 4096],
    });
    const errorPayload = JSON.stringify({
      at: 1700000001000,
      cache: "flp-parquet-v1",
      key: "svc/b.parquet",
      bytes: 8192,
      usage: 900_000,
      quota: 1_000_000,
      message: "QuotaExceededError: storage full",
    });

    const fakeCaches = makeFakeCaches({
      "flp-parquet-v1": {
        "http://localhost/fos/a.parquet": new Response("12345678", {
          headers: { "content-length": "4096" },
        }),
      },
      "flp-parquet-meta-v1": {
        "/__flp_store_index?v=1": new Response(metaPayload),
        "/__flp_store_error?v=1": new Response(errorPayload),
      },
      "flp-manifest-v1": {
        "http://localhost/__flp_manifest/svc1": new Response("abcdef"),
      },
    });
    vi.stubGlobal("caches", fakeCaches.api);

    const opfsFiles = [
      {
        kind: "file" as const,
        getFile: vi.fn(async () => ({ size: 10_000 })),
      },
      {
        kind: "directory" as const,
        values: async function* () {
          yield {
            kind: "file" as const,
            getFile: vi.fn(async () => ({ size: 2_500 })),
          };
        },
      },
    ];
    const opfsRoot = {
      getDirectoryHandle: vi.fn(async (name: string) => {
        if (name !== OPFS_DIRECTORY) {
          throw new DOMException("Missing", "NotFoundError");
        }
        return {
          kind: "directory" as const,
          values: async function* () {
            for (const item of opfsFiles) yield item;
          },
        };
      }),
      removeEntry: vi.fn(async () => {}),
    };
    vi.stubGlobal("navigator", {
      storage: {
        estimate: vi.fn(async () => ({ usage: 50_000, quota: 1_000_000 })),
        persisted: vi.fn(async () => true),
        getDirectory: vi.fn(async () => opfsRoot),
      },
    });

    const snapshot = await inspectBrowserCaches();

    expect(snapshot.usage).toBe(50_000);
    expect(snapshot.quota).toBe(1_000_000);
    expect(snapshot.persisted).toBe(true);
    expect(snapshot.indexedFiles).toBe(1);
    expect(snapshot.indexedBytes).toBe(4096);
    expect(snapshot.lastError?.message).toContain("QuotaExceededError");
    expect(snapshot.opfs).toEqual({ available: true, files: 2, bytes: 12_500 });
    expect(snapshot.caches.map((b) => b.name)).toEqual([
      "flp-manifest-v1",
      "flp-parquet-meta-v1",
      "flp-parquet-v1",
    ]);
    expect(
      snapshot.caches.find((b) => b.name === "flp-parquet-v1"),
    ).toMatchObject({
      entries: 1,
      bytes: 4096,
      label: "Request Parquet Cache",
    });
    expect(snapshot.totalCachedEntries).toBe(2 + 1 + 2 + 1);
    expect(snapshot.totalCachedBytes).toBe(
      12_500 + 4096 + metaPayload.length + errorPayload.length + 6,
    );
  });

  it("clears browser caches and returns an empty refreshed snapshot", async () => {
    const fakeCaches = makeFakeCaches({
      "flp-parquet-v1": {
        "http://localhost/fos/a.parquet": new Response("1234", {
          headers: { "content-length": "1024" },
        }),
      },
      "flp-manifest-v1": {
        "http://localhost/__flp_manifest/svc1": new Response("ok"),
      },
    });
    vi.stubGlobal("caches", fakeCaches.api);

    let opfsCleared = false;
    const opfsRoot = {
      getDirectoryHandle: vi.fn(async () => {
        if (opfsCleared) {
          throw new DOMException("Missing", "NotFoundError");
        }
        return {
          kind: "directory" as const,
          values: async function* () {
            yield {
              kind: "file" as const,
              getFile: vi.fn(async () => ({ size: 2048 })),
            };
          },
        };
      }),
      removeEntry: vi.fn(async () => {
        opfsCleared = true;
      }),
    };
    vi.stubGlobal("navigator", {
      storage: {
        estimate: vi.fn(async () => ({ usage: 0, quota: 1_000_000 })),
        persisted: vi.fn(async () => false),
        getDirectory: vi.fn(async () => opfsRoot),
      },
    });

    const after = await clearAllBrowserCaches();

    expect(opfsRoot.removeEntry).toHaveBeenCalledWith(OPFS_DIRECTORY, {
      recursive: true,
    });
    expect(after.opfs).toEqual({ available: true, files: 0, bytes: 0 });
    expect(after.caches).toEqual([]);
    expect(after.totalCachedEntries).toBe(0);
    expect(after.totalCachedBytes).toBe(0);
  });
});
