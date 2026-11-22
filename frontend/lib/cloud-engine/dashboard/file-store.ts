/**
 * Bookkeeping for the persistent request Parquet store (CacheStorage
 * `flp-parquet-v1`): a last-used index for LRU eviction, a quota check before
 * every write, and the last write failure, which /cache shows.
 */

export const STORE_META_CACHE = "flp-parquet-meta-v1";
/** The share of the origin quota the dashboard may fill before evicting. */
export const STORE_QUOTA_FRACTION = 0.8;
const INDEX_URL = "/__flp_store_index?v=1";
const ERROR_URL = "/__flp_store_error?v=1";
const INDEX_FLUSH_MS = 2_000;
const MAX_EVICTIONS_PER_WRITE = 512;

export type StoreError = {
  at: number;
  cache: string;
  key: string;
  bytes: number;
  message: string;
  usage: number | null;
  quota: number | null;
};

async function openMeta(): Promise<Cache | null> {
  try {
    return typeof caches === "undefined"
      ? null
      : await caches.open(STORE_META_CACHE);
  } catch {
    return null;
  }
}

async function readJson<T>(url: string): Promise<T | null> {
  try {
    const hit = await (await openMeta())?.match(url);
    return hit ? ((await hit.json()) as T) : null;
  } catch {
    return null;
  }
}

async function writeJson(url: string, value: unknown): Promise<void> {
  const meta = await openMeta();
  await meta?.put(
    url,
    new Response(JSON.stringify(value), {
      headers: { "content-type": "application/json" },
    }),
  );
}

/** The most recent failed or skipped store write, if any. */
export function readStoreError(): Promise<StoreError | null> {
  return readJson<StoreError>(ERROR_URL);
}

export async function recordStoreError(error: StoreError): Promise<void> {
  try {
    await writeJson(ERROR_URL, error);
  } catch {
    // Storage is full enough that even the error note failed; the console keeps it.
    console.warn("flp: browser cache write failed", error);
  }
}

export async function storageEstimate(): Promise<{
  usage: number | null;
  quota: number | null;
}> {
  try {
    const estimate = await globalThis.navigator?.storage?.estimate?.();
    return {
      usage: estimate?.usage ?? null,
      quota: estimate?.quota ?? null,
    };
  } catch {
    return { usage: null, quota: null };
  }
}

export function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.name && !error.message.includes(error.name)
      ? `${error.name}: ${error.message}`
      : error.message;
  }
  return String(error);
}

type IndexEntry = [lastUsed: number, bytes: number];

/**
 * Last-used times for cached files, persisted in the meta cache. Keys in the
 * active manifest are pinned and never evicted. Tabs and workers each keep a
 * copy and the last flush wins; an entry missing from the index counts as
 * least recently used.
 */
export class StoreIndex {
  private entries = new Map<string, IndexEntry>();
  private pinned = new Set<string>();
  private loaded: Promise<void> | null = null;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private clock = 0;

  private load(): Promise<void> {
    if (!this.loaded) {
      this.loaded = readJson<Record<string, IndexEntry>>(INDEX_URL).then(
        (stored) => {
          for (const [key, entry] of Object.entries(stored ?? {})) {
            if (!this.entries.has(key)) this.entries.set(key, entry);
            this.clock = Math.max(this.clock, entry[0]);
          }
        },
      );
    }
    return this.loaded;
  }

  private now(): number {
    this.clock = Math.max(Date.now(), this.clock + 1);
    return this.clock;
  }

  touch(key: string, bytes?: number): void {
    const known = this.entries.get(key);
    const size = bytes ?? known?.[1];
    if (size === undefined) return;
    this.entries.set(key, [this.now(), size]);
    this.scheduleFlush();
  }

  forget(key: string): void {
    if (this.entries.delete(key)) this.scheduleFlush();
  }

  /** Marks the manifest's keys as in use: pinned and most recently used. */
  useManifest(keys: Iterable<string>): void {
    this.pinned = new Set(keys);
    for (const key of this.pinned) this.touch(key);
  }

  reset(): void {
    this.entries.clear();
    this.pinned.clear();
    this.loaded = null;
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = null;
  }

  private scheduleFlush(): void {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flush();
    }, INDEX_FLUSH_MS);
  }

  async flush(): Promise<void> {
    try {
      await this.load();
      await writeJson(INDEX_URL, Object.fromEntries(this.entries));
    } catch {
      // The index only orders eviction; losing a flush is harmless.
    }
  }

  /**
   * Evicts least recently used, unpinned files until `bytes` more fit under
   * `STORE_QUOTA_FRACTION` of the quota. Returns false if they can't.
   */
  async makeRoom(
    openCache: () => Promise<Cache | null>,
    bytes: number,
    onEvict: (key: string) => void,
  ): Promise<{ ok: boolean; usage: number | null; quota: number | null }> {
    const { usage, quota } = await storageEstimate();
    if (usage === null || quota === null) return { ok: true, usage, quota };
    let excess = usage + bytes - quota * STORE_QUOTA_FRACTION;
    if (excess <= 0) return { ok: true, usage, quota };
    const cache = await openCache();
    if (!cache) return { ok: false, usage, quota };
    await this.load();
    const candidates: { url: string; key: string; at: number }[] = [];
    for (const request of await cache.keys()) {
      const parsed = new URL(request.url, "http://x");
      const key =
        parsed.searchParams.get("key") ??
        (parsed.pathname.startsWith("/fos/")
          ? decodeURIComponent(parsed.pathname.slice(5))
          : null);
      if (!key || this.pinned.has(key)) continue;
      candidates.push({
        url: request.url,
        key,
        at: this.entries.get(key)?.[0] ?? 0,
      });
    }
    candidates.sort((a, b) => a.at - b.at);
    for (const candidate of candidates.slice(0, MAX_EVICTIONS_PER_WRITE)) {
      if (excess <= 0) break;
      const size =
        this.entries.get(candidate.key)?.[1] ??
        (await cachedSize(cache, candidate.url));
      if (await cache.delete(candidate.url)) excess -= size;
      this.forget(candidate.key);
      onEvict(candidate.key);
    }
    return { ok: excess <= 0, usage, quota };
  }
}

async function cachedSize(cache: Cache, url: string): Promise<number> {
  const hit = await cache.match(url);
  if (!hit) return 0;
  const header = Number(hit.headers.get("content-length"));
  return Number.isFinite(header) && header > 0
    ? header
    : (await hit.arrayBuffer()).byteLength;
}

export async function getStoreStats(): Promise<{
  indexedFiles: number;
  indexedBytes: number;
  usage: number | null;
  quota: number | null;
  lastError: StoreError | null;
}> {
  const [index, estimate, lastError] = await Promise.all([
    readJson<Record<string, IndexEntry>>(INDEX_URL),
    storageEstimate(),
    readStoreError(),
  ]);
  const entries = Object.values(index ?? {});
  const indexedFiles = entries.length;
  const indexedBytes = entries.reduce((sum, [, bytes]) => sum + (Number(bytes) || 0), 0);
  return {
    indexedFiles,
    indexedBytes,
    usage: estimate.usage,
    quota: estimate.quota,
    lastError,
  };
}
