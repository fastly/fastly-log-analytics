import type { FileRollupData } from "./engine";
import { decodeRollup, encodeRollup, estimateRollupBytes } from "./rollup-codec";

/** One file's aggregates under one filter set, as DuckDB returned them. */
export type FilteredFileResult = {
  data: FileRollupData;
  /** Card fields whose `counts` hold only the top values. */
  truncated: string[];
  rawRows: number;
  byteLength: number;
};

export type FilteredResultCacheOptions = {
  baseUrl: string;
  scope: string;
  cacheName?: string;
  maxBytes?: number;
  maxEntries?: number;
  maxAgeMs?: number;
  memoryBytes?: number;
  now?: () => number;
  openCache?: () => Promise<Cache>;
  hash?: (id: string) => Promise<string>;
};

export const FILTERED_CACHE_NAME = "flp-filtered-v1";
const FORMAT = 1;
const BYTES_HEADER = "x-flp-bytes";
const FLUSH_DELAY_MS = 1_000;

type IndexEntry = [used: number, bytes: number];

async function sha256(id: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(id));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Persistent per-file filtered results in the Cache API. Files are immutable,
 * so a result keyed by file identity plus the query hash never goes stale; the
 * bounds only cap space. An LRU index lives beside the entries and is
 * reconciled with the cache's own keys on first use, so a lost flush (another
 * tab, a closed page) neither leaks space nor drops entries.
 */
export class FilteredResultCache {
  private readonly cacheName: string;
  private readonly maxBytes: number;
  private readonly maxEntries: number;
  private readonly maxAgeMs: number;
  private readonly memoryBytes: number;
  private readonly now: () => number;
  private readonly openCache: () => Promise<Cache>;
  private readonly hash: (id: string) => Promise<string>;
  private readonly prefix: string;
  private readonly indexUrl: string;

  private cache: Promise<Cache> | null = null;
  private index: Map<string, IndexEntry> | null = null;
  private loading: Promise<Map<string, IndexEntry>> | null = null;
  private readonly removed = new Set<string>();
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private flushing: Promise<void> = Promise.resolve();
  private totalBytes = 0;

  private readonly memory = new Map<string, { result: FilteredFileResult; bytes: number }>();
  private memoryTotal = 0;

  constructor(options: FilteredResultCacheOptions) {
    this.cacheName = options.cacheName ?? FILTERED_CACHE_NAME;
    this.maxBytes = options.maxBytes ?? 128 * 1024 * 1024;
    this.maxEntries = options.maxEntries ?? 16_384;
    this.maxAgeMs = options.maxAgeMs ?? 7 * 24 * 60 * 60 * 1000;
    this.memoryBytes = options.memoryBytes ?? 64 * 1024 * 1024;
    this.now = options.now ?? Date.now;
    this.openCache = options.openCache ?? (() => caches.open(this.cacheName));
    this.hash = options.hash ?? sha256;
    const base = options.baseUrl.replace(/\/+$/, "");
    const scope = encodeURIComponent(options.scope);
    this.prefix = `${base}/__flp_filtered?v=${FORMAT}&scope=${scope}&id=`;
    this.indexUrl = `${base}/__flp_filtered_index?v=${FORMAT}&scope=${scope}`;
  }

  async read(id: string): Promise<FilteredFileResult | null> {
    const remembered = this.memory.get(id);
    if (remembered) {
      this.memory.delete(id);
      this.memory.set(id, remembered);
      return remembered.result;
    }
    const url = this.prefix + (await this.hash(id));
    const index = await this.loadIndex();
    const entry = index.get(url);
    if (!entry) return null;
    const cache = await this.open();
    if (this.now() - entry[0] > this.maxAgeMs) {
      this.drop(index, url);
      await cache.delete(url);
      this.scheduleFlush();
      return null;
    }
    const response = await cache.match(url);
    if (!response) {
      this.drop(index, url);
      this.scheduleFlush();
      return null;
    }
    const decoded = await decodeRollup(new Uint8Array(await response.arrayBuffer()));
    if (!decoded) return null;
    let stored: unknown;
    try {
      stored = JSON.parse(decoded.key);
    } catch {
      return null;
    }
    if (
      !Array.isArray(stored) ||
      stored[0] !== id ||
      !Array.isArray(stored[1]) ||
      !stored[1].every((field) => typeof field === "string")
    ) {
      return null;
    }
    entry[0] = this.now();
    this.scheduleFlush();
    const { version: _v, key: _k, rawRows, byteLength, edgeRollup: _e, ...data } = decoded;
    const result: FilteredFileResult = {
      data,
      truncated: stored[1] as string[],
      rawRows,
      byteLength,
    };
    this.remember(id, result);
    return result;
  }

  async write(id: string, result: FilteredFileResult): Promise<void> {
    const url = this.prefix + (await this.hash(id));
    const index = await this.loadIndex();
    const bytes = await encodeRollup({
      version: 1,
      key: JSON.stringify([id, result.truncated]),
      rawRows: result.rawRows,
      byteLength: result.byteLength,
      ...result.data,
    });
    this.remember(id, result);
    if (bytes.byteLength > this.maxBytes) return;
    const cache = await this.open();
    this.drop(index, url);
    const now = this.now();
    const evicted: string[] = [];
    for (const [key, [used]] of index) {
      if (now - used > this.maxAgeMs) evicted.push(key);
    }
    for (const key of evicted) this.drop(index, key);
    // Map order is insertion order, not use order, so sort by last use.
    const byAge = [...index].sort((a, b) => a[1][0] - b[1][0]);
    let next = 0;
    while (
      next < byAge.length &&
      (this.totalBytes + bytes.byteLength > this.maxBytes || index.size + 1 > this.maxEntries)
    ) {
      const key = byAge[next++][0];
      this.drop(index, key);
      evicted.push(key);
    }
    await Promise.all(evicted.map((key) => cache.delete(key)));
    await cache.put(
      url,
      new Response(bytes as BlobPart, {
        headers: {
          "content-type": "application/octet-stream",
          [BYTES_HEADER]: String(bytes.byteLength),
        },
      }),
    );
    index.set(url, [now, bytes.byteLength]);
    this.removed.delete(url);
    this.totalBytes += bytes.byteLength;
    this.scheduleFlush();
  }

  /** Writes the LRU index now; pending debounced writes are folded in. */
  async flush(): Promise<void> {
    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.flushing = this.flushing.then(() => this.writeIndex());
    await this.flushing;
  }

  persistedBytes(): number {
    return this.totalBytes;
  }

  clearMemory(): void {
    this.memory.clear();
    this.memoryTotal = 0;
  }

  private open(): Promise<Cache> {
    this.cache ??= Promise.resolve()
      .then(this.openCache)
      .catch((error: unknown) => {
        this.cache = null;
        throw error;
      });
    return this.cache;
  }

  private drop(index: Map<string, IndexEntry>, url: string): void {
    const entry = index.get(url);
    if (!entry) return;
    index.delete(url);
    this.totalBytes -= entry[1];
    this.removed.add(url);
  }

  private remember(id: string, result: FilteredFileResult): void {
    if (this.memoryBytes <= 0) return;
    const bytes =
      estimateRollupBytes({ version: 1, key: id, rawRows: 0, byteLength: 0, ...result.data }) +
      result.truncated.length * 64;
    if (bytes > this.memoryBytes) return;
    const previous = this.memory.get(id);
    if (previous) {
      this.memory.delete(id);
      this.memoryTotal -= previous.bytes;
    }
    this.memory.set(id, { result, bytes });
    this.memoryTotal += bytes;
    for (const [key, value] of this.memory) {
      if (this.memoryTotal <= this.memoryBytes) break;
      this.memory.delete(key);
      this.memoryTotal -= value.bytes;
    }
  }

  private loadIndex(): Promise<Map<string, IndexEntry>> {
    if (this.index) return Promise.resolve(this.index);
    this.loading ??= this.reconcile().then(
      (index) => {
        this.index = index;
        this.totalBytes = [...index.values()].reduce((sum, [, bytes]) => sum + bytes, 0);
        return index;
      },
      (error: unknown) => {
        this.loading = null;
        throw error;
      },
    );
    return this.loading;
  }

  private async storedIndex(cache: Cache): Promise<Map<string, IndexEntry>> {
    const response = await cache.match(this.indexUrl);
    if (!response) return new Map();
    try {
      const parsed = (await response.json()) as Record<string, unknown>;
      const index = new Map<string, IndexEntry>();
      for (const [url, value] of Object.entries(parsed)) {
        if (
          Array.isArray(value) &&
          value.length === 2 &&
          value.every((n) => typeof n === "number" && Number.isFinite(n))
        ) {
          index.set(url, [value[0], value[1]]);
        }
      }
      return index;
    } catch {
      return new Map();
    }
  }

  private async reconcile(): Promise<Map<string, IndexEntry>> {
    const cache = await this.open();
    const stored = await this.storedIndex(cache);
    const index = new Map<string, IndexEntry>();
    for (const request of await cache.keys()) {
      const url = typeof request === "string" ? request : request.url;
      if (!url.startsWith(this.prefix)) continue;
      const known = stored.get(url);
      if (known) {
        index.set(url, known);
        continue;
      }
      const response = await cache.match(url);
      const bytes = Number(response?.headers.get(BYTES_HEADER));
      if (response && Number.isFinite(bytes) && bytes > 0) index.set(url, [this.now(), bytes]);
    }
    return index;
  }

  private scheduleFlush(): void {
    if (this.flushTimer !== null) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flushing = this.flushing.then(() => this.writeIndex()).catch(() => undefined);
    }, FLUSH_DELAY_MS);
    // Node keeps the process alive on a pending timer; browsers ignore this.
    (this.flushTimer as { unref?: () => void }).unref?.();
  }

  private async writeIndex(): Promise<void> {
    if (!this.index) return;
    const cache = await this.open();
    // Another tab may have flushed entries this one never saw.
    const merged = await this.storedIndex(cache);
    for (const url of this.removed) merged.delete(url);
    for (const [url, entry] of this.index) {
      const other = merged.get(url);
      merged.set(url, other && other[0] > entry[0] ? other : entry);
    }
    this.removed.clear();
    for (const [url, entry] of merged) {
      if (this.index.has(url)) continue;
      this.index.set(url, entry);
      this.totalBytes += entry[1];
    }
    await cache.put(
      this.indexUrl,
      new Response(JSON.stringify(Object.fromEntries(merged)), {
        headers: { "content-type": "application/json" },
      }),
    );
  }
}
