import { getStoreStats, type StoreError } from "./file-store";
import { clearManifestCache } from "./manifest";
import { clearDashboardBrowserCaches } from "./sources";
import { OPFS_DIRECTORY } from "./store-index";
import { clearOpsCaches } from "../telemetry/sources";

export interface BrowserCacheBucketStat {
  name: string;
  label: string;
  entries: number;
  bytes: number;
}

export interface BrowserCacheSnapshot {
  usage: number | null;
  quota: number | null;
  persisted: boolean | null;
  indexedFiles: number;
  indexedBytes: number;
  lastError: StoreError | null;
  opfs: {
    available: boolean;
    files: number;
    bytes: number;
  };
  caches: BrowserCacheBucketStat[];
  totalCachedEntries: number;
  totalCachedBytes: number;
  measuredAt: number;
}

const KNOWN_BUCKET_LABELS: Record<string, string> = {
  "flp-parquet-v1": "Request Parquet Cache",
  "flp-parquet-meta-v1": "Cache Metadata & LRU Index",
  "flp-manifest-v1": "Manifest Snapshots",
  "flp-filtered-v1": "Filtered Query Results",
};

export function labelForCacheBucket(name: string): string {
  if (KNOWN_BUCKET_LABELS[name]) return KNOWN_BUCKET_LABELS[name];
  if (name.startsWith("flp-parquet-rollup-")) return "Pre-Aggregated Rollup Cache";
  if (name.startsWith("flp-ops-")) return "Operational Telemetry Cache";
  return name;
}

interface OpfsDirEntry {
  kind: "file" | "directory";
  getFile?: () => Promise<{ size: number }>;
  values?: () => AsyncIterable<OpfsDirEntry>;
}

async function walkOpfsDirectory(
  dir: OpfsDirEntry,
): Promise<{ files: number; bytes: number }> {
  let files = 0;
  let bytes = 0;
  if (typeof dir.values !== "function") return { files, bytes };
  for await (const entry of dir.values()) {
    if (entry.kind === "file" && typeof entry.getFile === "function") {
      try {
        const file = await entry.getFile();
        files += 1;
        bytes += Number(file.size) || 0;
      } catch {
        // File may have been evicted concurrently during scan.
      }
    } else if (entry.kind === "directory") {
      const nested = await walkOpfsDirectory(entry);
      files += nested.files;
      bytes += nested.bytes;
    }
  }
  return { files, bytes };
}

async function inspectOpfsStore(): Promise<{
  available: boolean;
  files: number;
  bytes: number;
}> {
  if (typeof navigator === "undefined" || !navigator.storage) {
    return { available: false, files: 0, bytes: 0 };
  }
  const getDirectory = (
    navigator.storage as StorageManager & {
      getDirectory?: () => Promise<{
        getDirectoryHandle: (
          name: string,
          options?: { create?: boolean },
        ) => Promise<OpfsDirEntry>;
      }>;
    }
  ).getDirectory;
  if (typeof getDirectory !== "function") {
    return { available: false, files: 0, bytes: 0 };
  }
  try {
    const root = await getDirectory.call(navigator.storage);
    try {
      const dir = await root.getDirectoryHandle(OPFS_DIRECTORY, {
        create: false,
      });
      const stats = await walkOpfsDirectory(dir);
      return { available: true, ...stats };
    } catch (error) {
      if (error instanceof DOMException && error.name === "NotFoundError") {
        return { available: true, files: 0, bytes: 0 };
      }
      if (
        error &&
        typeof error === "object" &&
        "name" in error &&
        (error as { name?: string }).name === "NotFoundError"
      ) {
        return { available: true, files: 0, bytes: 0 };
      }
      return { available: false, files: 0, bytes: 0 };
    }
  } catch {
    return { available: false, files: 0, bytes: 0 };
  }
}

async function responseByteLength(response: Response): Promise<number> {
  const header = Number(response.headers.get("content-length"));
  if (Number.isFinite(header) && header > 0) return header;
  try {
    return (await response.clone().arrayBuffer()).byteLength;
  } catch {
    return 0;
  }
}

async function inspectCacheStorage(): Promise<BrowserCacheBucketStat[]> {
  if (typeof caches === "undefined" || typeof caches.keys !== "function") {
    return [];
  }
  try {
    const names = (await caches.keys()).sort();
    const buckets: BrowserCacheBucketStat[] = [];
    for (const name of names) {
      try {
        const cache = await caches.open(name);
        const requests = await cache.keys();
        let bytes = 0;
        for (const req of requests) {
          const match = await cache.match(req);
          if (match) bytes += await responseByteLength(match);
        }
        if (requests.length > 0) {
          buckets.push({
            name,
            label: labelForCacheBucket(name),
            entries: requests.length,
            bytes,
          });
        }
      } catch {
        // Cache may have been deleted concurrently.
      }
    }
    return buckets;
  } catch {
    return [];
  }
}

async function readPersistedState(): Promise<boolean | null> {
  if (
    typeof navigator === "undefined" ||
    !navigator.storage ||
    typeof navigator.storage.persisted !== "function"
  ) {
    return null;
  }
  try {
    return await navigator.storage.persisted();
  } catch {
    return null;
  }
}

export async function inspectBrowserCaches(): Promise<BrowserCacheSnapshot> {
  const [storeStats, persisted, opfs, cacheBuckets] = await Promise.all([
    getStoreStats(),
    readPersistedState(),
    inspectOpfsStore(),
    inspectCacheStorage(),
  ]);
  const cacheEntries = cacheBuckets.reduce((sum, b) => sum + b.entries, 0);
  const cacheBytes = cacheBuckets.reduce((sum, b) => sum + b.bytes, 0);
  return {
    usage: storeStats.usage,
    quota: storeStats.quota,
    persisted,
    indexedFiles: storeStats.indexedFiles,
    indexedBytes: storeStats.indexedBytes,
    lastError: storeStats.lastError,
    opfs,
    caches: cacheBuckets,
    totalCachedEntries: opfs.files + cacheEntries,
    totalCachedBytes: opfs.bytes + cacheBytes,
    measuredAt: Date.now(),
  };
}

export async function clearAllBrowserCaches(): Promise<BrowserCacheSnapshot> {
  clearManifestCache();
  await Promise.allSettled([clearDashboardBrowserCaches(), clearOpsCaches()]);
  return inspectBrowserCaches();
}
