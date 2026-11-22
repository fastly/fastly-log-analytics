import { fnv1a32, keyWindowMs } from "../parquet-key";
import { fetchRetrying } from "../fetch-retry";
import { MissingCloudFileError } from "./file-errors";
import { CARD_FIELDS } from "./summaries";
import { isCdnHit } from "./cdn-cache";
import { parseServerTiming, type ServerTimingEntry } from "../../call-timeline";
import type { FileRollup, FileSource } from "./engine";
import {
  STORE_META_CACHE,
  StoreIndex,
  bumpStoreMetaEpoch,
  currentStoreMetaEpoch,
  describeError,
  getStoreStats,
  recordStoreError,
  resetAndDrainStoreMeta,
  storageEstimate,
} from "./file-store";
import { FILTERED_CACHE_NAME, FilteredResultCache } from "./filtered-cache";
import type { Manifest } from "./manifest";
import { MANIFEST_SNAPSHOT_CACHE } from "./manifest-store";
import {
  MAX_MANIFEST_BYTES,
  ManifestRequestError,
  drainCatalogManifestStore,
} from "./catalog-manifest";
import {
  decodeRollup,
  encodeRollup,
  estimateRollupBytes,
} from "./rollup-codec";
import {
  OPFS_DIRECTORY,
  OPFSParquetStore,
  bumpOpfsEpoch,
  clearOPFSParquetStore,
} from "./store-index";

export { getStoreStats, keyWindowMs };

export const SESSION_CACHE_KEY = "flp-dashboard-cache-session";
export const IDENTITY_CACHE_KEY = "flp-dashboard-cache-identity";
let volatileCacheScope: string | undefined;
let volatileCacheIdentity: string | null = null;

// Shared by every tab of the origin so a new tab of the same signed-in identity reuses persisted
// files; an identity change or sign-out rotates the scope and clears the caches.
export function dashboardCacheScope(): string {
  try {
    let session = localStorage.getItem(SESSION_CACHE_KEY);
    if (!session) {
      session = crypto.randomUUID();
      localStorage.setItem(SESSION_CACHE_KEY, session);
    }
    return JSON.stringify([
      session,
      localStorage.getItem(IDENTITY_CACHE_KEY) ?? "",
    ]);
  } catch {
    const session = (volatileCacheScope ??= crypto.randomUUID());
    return JSON.stringify([session, volatileCacheIdentity ?? ""]);
  }
}

export function dashboardManifestCacheScope(
  baseUrl: string,
  serviceId?: string,
  cacheScope = dashboardCacheScope(),
): string {
  let normalizedBaseUrl = baseUrl.replace(/\/+$/, "");
  try {
    const url = new URL(
      baseUrl || "/",
      globalThis.location?.origin ?? "http://localhost",
    );
    normalizedBaseUrl =
      url.origin + url.pathname.replace(/\/+$/, "") + url.search;
  } catch {
    /* Preserve non-URL test adapters while still normalizing trailing slashes. */
  }
  return JSON.stringify([normalizedBaseUrl, cacheScope, serviceId ?? ""]);
}

export function dashboardCacheIdentity(): string | null {
  try {
    if (typeof localStorage === "undefined") return volatileCacheIdentity;
    const stored = localStorage.getItem(IDENTITY_CACHE_KEY);
    volatileCacheIdentity = stored;
    return stored;
  } catch {
    return volatileCacheIdentity;
  }
}

type ActiveSourceClosure = {
  clearMemoryCache: () => void;
  drainStoreQueue: () => Promise<void>;
};

const activeSources = new Set<WeakRef<ActiveSourceClosure>>();
const sourceKeepAlive = new WeakMap<object, ActiveSourceClosure>();
const pendingSourceClosures = new Set<ActiveSourceClosure>();

export function clearBrowserSourceMemoryCaches(): void {
  bumpStoreMetaEpoch();
  bumpOpfsEpoch();
  for (const ref of activeSources) {
    const instance = ref.deref();
    if (!instance) {
      activeSources.delete(ref);
      continue;
    }
    instance.clearMemoryCache();
  }
}

async function drainBrowserSourceStoreQueues(): Promise<void> {
  const pending: Promise<void>[] = [];
  for (const ref of activeSources) {
    const instance = ref.deref();
    if (!instance) {
      activeSources.delete(ref);
      continue;
    }
    pending.push(instance.drainStoreQueue());
  }
  if (pending.length > 0) {
    await Promise.allSettled(pending);
  }
}

export function updateDashboardCacheIdentity(
  identity: string | null,
  options: { syncStorage?: boolean } = {},
): void {
  volatileCacheIdentity = identity;
  volatileCacheScope = undefined;
  clearBrowserSourceMemoryCaches();
  if (options.syncStorage === false) return;
  try {
    if (typeof localStorage !== "undefined") {
      localStorage.removeItem(SESSION_CACHE_KEY);
      if (identity === null) localStorage.removeItem(IDENTITY_CACHE_KEY);
      else localStorage.setItem(IDENTITY_CACHE_KEY, identity);
    }
  } catch {
    volatileCacheIdentity = identity;
    /* A fresh in-memory scope still isolates this session. */
  }
}

export const setDashboardCacheIdentity = updateDashboardCacheIdentity;

export function parseVcpuMsHeader(
  input?: Pick<Headers, "get"> | string | null,
): number | null {
  const raw =
    typeof input === "string" ? input : input?.get?.("x-flp-vcpu-ms");
  if (raw == null || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

export async function mapLimit<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(Math.max(1, limit), items.length) },
    async () => {
      while (nextIndex < items.length) {
        const index = nextIndex++;
        results[index] = await fn(items[index], index);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

function scopedCacheUrl(
  url: string,
  scope?: string,
  identity?: string,
): string {
  if (!scope && identity === undefined) return url;
  const params = new URLSearchParams();
  if (scope) params.set("scope", scope);
  if (identity !== undefined) params.set("identity", identity);
  return `${url}${url.includes("?") ? "&" : "?"}${params}`;
}

async function manifestJson(response: Response): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) return response.json();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_MANIFEST_BYTES) {
        await reader.cancel();
        throw new Error("File manifest exceeded the metadata byte limit");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(bytes);
  let at = 0;
  for (const chunk of chunks) {
    body.set(chunk, at);
    at += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder().decode(body));
}

export const CACHE_NAME = "flp-parquet-v1";
const ROLLUP_CACHE_PREFIX = "flp-parquet-rollup-";
/** Bump when per-file rollup aggregation changes without a CARD_FIELDS change. */
export const ROLLUP_SEMANTICS = "rollup-2-binary";
export const BROWSER_CACHE_RETENTION_MS = 30 * 24 * 3600 * 1000;
const MAX_PRUNE_DELETES = 256;

/** FNV-1a over the field list and semantics tag, as 8 hex digits. */
export function rollupCacheVersion(
  fields: readonly string[],
  semantics: string,
): string {
  return fnv1a32(fields, semantics);
}

export const ROLLUP_CACHE_VERSION = rollupCacheVersion(
  CARD_FIELDS,
  ROLLUP_SEMANTICS,
);
export const ROLLUP_CACHE_NAME = `${ROLLUP_CACHE_PREFIX}${ROLLUP_CACHE_VERSION}`;
const MAX_MEMORY_ROLLUP_BYTES = 128 * 1024 * 1024;
const MAX_MEMORY_FILE_BYTES = 128 * 1024 * 1024;

export async function openCache(name = CACHE_NAME): Promise<Cache | null> {
  try {
    return typeof caches === "undefined" ? null : await caches.open(name);
  } catch {
    return null;
  }
}

export async function cacheGetBytes(
  cache: Cache | null,
  url: string,
): Promise<ArrayBuffer | null> {
  if (!cache) return null;
  try {
    const hit = await cache.match(url);
    return hit ? await hit.arrayBuffer() : null;
  } catch {
    return null;
  }
}

export async function cachePutBytes(
  cache: Cache | null,
  url: string,
  bytes: ArrayBuffer,
  headers: Record<string, string> = {
    "content-type": "application/octet-stream",
  },
): Promise<void> {
  if (!cache) return;
  try {
    await cache.put(url, new Response(bytes.slice(0), { headers }));
  } catch {
    /* quota or private-browsing failure */
  }
}

/**
 * The rollup version for runs that also count custom fields (`variant`, see
 * `customFieldsKey`); without any it is `ROLLUP_CACHE_VERSION`, so existing
 * entries stay valid.
 */
export function rollupVariantVersion(variant = ""): string {
  return variant
    ? rollupCacheVersion([...CARD_FIELDS, variant], ROLLUP_SEMANTICS)
    : ROLLUP_CACHE_VERSION;
}

function rollupCacheUrl(baseUrl: string, key: string, variant = ""): string {
  return `${baseUrl}/__flp_rollup?${new URLSearchParams({ v: rollupVariantVersion(variant), key })}`;
}

export async function deleteCaches(
  match: (name: string) => boolean,
): Promise<void> {
  if (typeof caches === "undefined") return;
  try {
    for (const name of await caches.keys()) {
      if (match(name)) await caches.delete(name);
    }
  } catch {
    // Cache cleanup is best-effort.
  }
}

/** Deletes every cache holding request-log Parquet files, their rollups or store index. */
export async function clearRequestLogCaches(): Promise<void> {
  clearBrowserSourceMemoryCaches();
  await drainBrowserSourceStoreQueues();
  await drainCatalogManifestStore();
  await resetAndDrainStoreMeta();
  await clearOPFSParquetStore();
  await deleteCaches(
    (name) =>
      name === CACHE_NAME ||
      name === STORE_META_CACHE ||
      name === FILTERED_CACHE_NAME ||
      name === MANIFEST_SNAPSHOT_CACHE ||
      name.startsWith(ROLLUP_CACHE_PREFIX),
  );
}

export const clearDashboardBrowserCaches = clearRequestLogCaches;

const keyPrefix = (key: string) => {
  const at = key.indexOf("year=");
  return at < 0 ? null : key.slice(0, at);
};

/**
 * Deletes cached entries past the retention cutoff, and entries whose window lies
 * inside a complete manifest's range but which the manifest no longer lists
 * (superseded or deleted objects). Deletions are capped per run; later runs finish.
 */
export async function pruneRequestCache(
  cache: Cache,
  manifest: Manifest,
  fromMs: number,
  toMs: number,
  options: {
    retentionCutoffMs?: number;
    maxDeletes?: number;
    scope?: string;
  } = {},
): Promise<string[]> {
  const cutoff =
    options.retentionCutoffMs ?? Date.now() - BROWSER_CACHE_RETENTION_MS;
  const maxDeletes = options.maxDeletes ?? MAX_PRUNE_DELETES;
  const listed = new Set(manifest.files.map((file) => file.key));
  const prefixes = new Set(
    manifest.files.map((file) => keyPrefix(file.key)).filter(Boolean),
  );
  const deleted: string[] = [];
  const filtersScope = Object.hasOwn(options, "scope");
  for (const request of await cache.keys()) {
    if (deleted.length >= maxDeletes) break;
    if (
      filtersScope &&
      (new URL(request.url, "http://localhost").searchParams.get("scope") ??
        undefined) !== options.scope
    ) continue;
    const key = keyFromFileUrl(request.url);
    if (!key) continue;
    const window = keyWindowMs(key);
    if (!window) continue;
    const expired = window[1] <= cutoff;
    const superseded =
      manifest.complete &&
      !listed.has(key) &&
      prefixes.has(keyPrefix(key)) &&
      window[0] >= fromMs &&
      window[1] <= toMs;
    if (expired || superseded) {
      await cache.delete(request.url);
      deleted.push(key);
    }
  }
  return deleted;
}

export type BrowserSourceCallEvent = {
  source:
    | "Compute API"
    | "Fastly API"
    | "FOS S3"
    | "CDN Cache"
    | "Browser Cache";
  method: string;
  path: string;
  status: number | null;
  duration_ms: number;
  vcpu_ms?: number | null;
  bytes?: number | null;
  cache?: "hit" | "miss" | "browser" | "cdn" | null;
  error?: string | null;
  endpoint: string;
  page_load_id?: string;
  /** Epoch milliseconds, so page and worker calls share one timeline. */
  start_ms?: number;
  server_timing?: ServerTimingEntry[];
};

export type BrowserSourceOptions = {
  getPageLoadId?: () => string;
  debugResponses?: () => boolean;
  onApiCall?: (call: BrowserSourceCallEvent) => void;
  serviceId?: string;
};

export type BrowserSourceTelemetry = BrowserSourceOptions;

const epochNow = () =>
  typeof performance !== "undefined"
    ? performance.timeOrigin + performance.now()
    : Date.now();

/** Stamps each call with its start, taken as its report time less its duration. */
function withCallStart(
  options?: BrowserSourceOptions,
): BrowserSourceOptions | undefined {
  const onApiCall = options?.onApiCall;
  if (!onApiCall) return options;
  return {
    ...options,
    onApiCall: (call) =>
      onApiCall(
        call.start_ms === undefined
          ? { ...call, start_ms: epochNow() - call.duration_ms }
          : call,
      ),
  };
}

export function emitDebugSubCalls(
  response: Response | undefined,
  endpoint: string,
  pageLoadId: string | undefined,
  onApiCall?: (call: BrowserSourceCallEvent) => void,
) {
  if (!onApiCall) return;
  const debugCallsHeader = response?.headers?.get?.("x-flp-debug-calls");
  if (!debugCallsHeader) return;
  try {
    const subCalls = JSON.parse(debugCallsHeader);
    if (!Array.isArray(subCalls)) return;
    for (const sub of subCalls) {
      if (!sub || typeof sub !== "object") continue;
      const rawSource = typeof sub.source === "string" ? sub.source : "";
      const cdnCache =
        typeof sub.cdn_cache === "string"
          ? sub.cdn_cache.toUpperCase()
          : undefined;
      const cdnHit =
        rawSource === "cdn_cache" && (!cdnCache || isCdnHit(cdnCache))
          ? true
          : isCdnHit(cdnCache);
      const isFos =
        !cdnHit &&
        (rawSource === "fos_usage" ||
          rawSource === "fos_s3" ||
          rawSource === "cdn_cache");
      const source: BrowserSourceCallEvent["source"] = cdnHit
        ? "CDN Cache"
        : isFos
          ? "FOS S3"
          : "Fastly API";
      const method = String(
        sub.method ?? sub.op ?? (source === "FOS S3" ? "S3" : "GET"),
      );
      const rawPath = String(sub.path || sub.endpoint || "");
      const opCount =
        typeof sub.op_count === "number" && sub.op_count > 1
          ? sub.op_count
          : null;
      const path =
        opCount && rawPath && !rawPath.includes(" ops)")
          ? `${rawPath} (${opCount} ops)`
          : rawPath || endpoint;
      const rawStatus =
        typeof sub.status === "number" && sub.status > 0
          ? sub.status
          : typeof sub.status_code === "number" && sub.status_code > 0
            ? sub.status_code
            : null;
      const error =
        typeof sub.error === "string" && sub.error.length > 0
          ? sub.error
          : null;
      const status = rawStatus ?? (error ? null : 200);
      const rawBytes =
        typeof sub.bytes === "number" && sub.bytes > 0
          ? sub.bytes
          : typeof sub.bytes_out === "number" && sub.bytes_out > 0
            ? sub.bytes_out
            : typeof sub.bytes_in === "number" && sub.bytes_in > 0
              ? sub.bytes_in
              : null;
      const cache: BrowserSourceCallEvent["cache"] = cdnHit
        ? "cdn"
        : cdnCache === "MISS"
          ? "miss"
          : null;
      onApiCall({
        source,
        method,
        path,
        status,
        duration_ms: typeof sub.duration_ms === "number" ? sub.duration_ms : 0,
        bytes: rawBytes,
        cache,
        error,
        endpoint,
        page_load_id: pageLoadId,
      });
    }
  } catch {
    /* ignore malformed debug header */
  }
}

export type BrowserFileSource = FileSource & {
  fetchRange: NonNullable<FileSource["fetchRange"]>;
  /**
   * The same caches with different telemetry, so concurrent requests in one
   * worker each report their own page load and calls.
   */
  withOptions(options?: BrowserSourceOptions): BrowserFileSource;
  /**
   * A file derived from `key` (such as a column projection) persisted in the
   * request-log cache under its caller-built identity, so a later page load or
   * tab reuses it. Evicted and pruned with `key`.
   */
  readDerivedFile(key: string, identity: string): Promise<DerivedFile | null>;
  writeDerivedFile(key: string, identity: string, bytes: ArrayBuffer, meta: string): Promise<void>;
};

export type DerivedFile = { byteLength: number; meta: string; bytes(): Promise<ArrayBuffer> };

const DERIVED_BYTES_HEADER = "x-flp-derived-bytes";
const DERIVED_META_HEADER = "x-flp-derived-meta";

const DIRECT_CDN_PARQUET_KEY =
  /^[A-Za-z0-9._=-]+(?:\/[A-Za-z0-9._=-]+)*\/processed\/(?:request|rum|ops|summary|summary_bundle)\/[A-Za-z0-9._=/-]*year=\d{4}\/[A-Za-z0-9._=/-]+\.parquet$/;

export function isDirectCdnParquetKey(key: string): boolean {
  return (
    key.length <= 512 &&
    !key.includes("..") &&
    !key.includes("//") &&
    DIRECT_CDN_PARQUET_KEY.test(key)
  );
}

export function fileRequestPath(key: string): string {
  return isDirectCdnParquetKey(key)
    ? `/fos/${key}`
    : `/api/file?${new URLSearchParams({ key })}`;
}

export function fileDisplayPath(key: string): string {
  return isDirectCdnParquetKey(key)
    ? key
    : `/api/file?${new URLSearchParams({ key })}`;
}

export function keyFromFileUrl(rawUrl: string): string | null {
  try {
    const parsed = new URL(rawUrl, "http://localhost");
    const byQuery = parsed.searchParams.get("key");
    if (byQuery !== null) return byQuery;
    if (parsed.pathname.startsWith("/fos/")) {
      return decodeURIComponent(parsed.pathname.slice(5));
    }
  } catch {
    /* ignore malformed cache URL */
  }
  return null;
}

export function browserSource(
  baseUrl: string,
  defaultOptions?: BrowserSourceOptions,
  limits: {
    maxMemoryRollupBytes?: number;
    foldFileTelemetry?: boolean;
    cacheScope?: string;
  } = {},
): BrowserFileSource {
  const currentCacheScope = () =>
    limits.cacheScope ??
    (typeof window !== "undefined" ? dashboardCacheScope() : undefined);
  // Successful file reads are reported in the next beacon instead of one server row each.
  const folded = { requests: 0, vcpuMs: 0 };
  const maxMemoryRollupBytes =
    limits.maxMemoryRollupBytes ?? MAX_MEMORY_ROLLUP_BYTES;
  const memoryRollups = new Map<
    string,
    { rollup: FileRollup; bytes: number; variant: string }
  >();
  let memoryRollupBytes = 0;
  const memoryFiles = new Map<string, ArrayBuffer>();
  const memoryFileIdentities = new Map<string, string | undefined>();
  let memoryFileBytes = 0;
  type InFlightFile = {
    controller: AbortController;
    promise: Promise<{ bytes: ArrayBuffer; cached: boolean; cdn: string | null }>;
    waiters: number;
    networkDone: boolean;
    complete: boolean;
  };
  type InFlightRange = {
    controller: AbortController;
    promise: Promise<ArrayBuffer>;
    waiters: number;
    complete: boolean;
  };
  const inFlightFiles = new Map<string, InFlightFile>();
  const inFlightRanges = new Map<string, InFlightRange>();
  const activeRangeControllers = new Set<AbortController>();
  let cacheEpoch = 0;
  const waitSharedRange = (
    entry: InFlightRange,
    signal?: AbortSignal,
  ): Promise<ArrayBuffer> => {
    signal?.throwIfAborted();
    entry.controller.signal.throwIfAborted();
    entry.waiters += 1;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      entry.waiters -= 1;
      if (entry.waiters === 0 && !entry.complete) {
        entry.controller.abort(signal?.reason);
      }
    };
    return new Promise<ArrayBuffer>((resolve, reject) => {
      const onCallerAbort = () => {
        cleanup();
        release();
        reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
      };
      const onSharedAbort = () => {
        cleanup();
        release();
        reject(
          entry.controller.signal.reason ??
            new DOMException("The operation was aborted.", "AbortError"),
        );
      };
      const cleanup = () => {
        if (signal) signal.removeEventListener("abort", onCallerAbort);
        entry.controller.signal.removeEventListener("abort", onSharedAbort);
      };
      if (signal) signal.addEventListener("abort", onCallerAbort, { once: true });
      entry.controller.signal.addEventListener("abort", onSharedAbort, { once: true });
      entry.promise.then(
        (bytes) => {
          cleanup();
          release();
          resolve(bytes);
        },
        (error) => {
          cleanup();
          release();
          reject(error);
        },
      );
    });
  };
  let fileCachePromise: Promise<Cache | null> | null = null;
  let rollupCachePromise: Promise<Cache | null> | null = null;
  let pruning = false;
  const storeIndex = new StoreIndex();
  const opfsStores = new Map<string, OPFSParquetStore>();
  const opfsStore = (variant = "") => {
    const scope = currentCacheScope() ?? "browser";
    const key = `${scope}\0${variant}`;
    let store = opfsStores.get(key);
    if (!store) {
      store = new OPFSParquetStore(
        JSON.stringify([
          baseUrl,
          scope,
          ROLLUP_CACHE_VERSION,
          variant,
        ]),
      );
      opfsStores.set(key, store);
      if (opfsStores.size > 8)
        opfsStores.delete(opfsStores.keys().next().value!);
    }
    return store;
  };
  const fileIdentity = (file: import("./manifest").ManifestFile) =>
    JSON.stringify([
      file.key,
      file.size,
      file.etag ?? "",
      file.schema_version ?? "",
    ]);
  const filteredCaches = new Map<string, FilteredResultCache>();
  const filteredCache = (scope: string) => {
    let cache = filteredCaches.get(scope);
    if (!cache) {
      const createdEpoch = cacheEpoch;
      cache = new FilteredResultCache({
        baseUrl,
        scope,
        openCache: () =>
          cacheEpoch === createdEpoch
            ? caches.open(FILTERED_CACHE_NAME)
            : Promise.reject(new Error("Cache cleared")),
      });
      filteredCaches.set(scope, cache);
    }
    return cache;
  };
  // One write at a time, so each quota check sees the previous write's usage.
  let storeQueue: Promise<void> = Promise.resolve();
  const activeOpfsWrites = new Set<Promise<unknown>>();
  let drainingFilteredCaches: Promise<void>[] = [];
  let pendingStoreWrites = 0;
  let storeIndexFlushPinTimer: ReturnType<typeof setTimeout> | null = null;
  const clearClosureMemory = () => {
    cacheEpoch += 1;
    for (const request of inFlightFiles.values()) {
      request.controller.abort();
    }
    inFlightFiles.clear();
    for (const request of inFlightRanges.values()) {
      request.controller.abort();
    }
    inFlightRanges.clear();
    for (const controller of activeRangeControllers) controller.abort();
    activeRangeControllers.clear();
    memoryRollups.clear();
    memoryRollupBytes = 0;
    memoryFiles.clear();
    memoryFileIdentities.clear();
    memoryFileBytes = 0;
    fileCachePromise = null;
    rollupCachePromise = null;
    opfsStores.clear();
    storeIndex.reset();
    if (storeIndexFlushPinTimer !== null) {
      clearTimeout(storeIndexFlushPinTimer);
      storeIndexFlushPinTimer = null;
    }
    const draining = [...filteredCaches.values()].map((c) => c.drain());
    for (const cache of filteredCaches.values()) cache.clearMemory();
    filteredCaches.clear();
    if (draining.length > 0) {
      drainingFilteredCaches = [...drainingFilteredCaches, ...draining];
    }
    syncClosurePin();
  };
  const closureHandle: ActiveSourceClosure = {
    clearMemoryCache: clearClosureMemory,
    drainStoreQueue: async () => {
      const snap = drainingFilteredCaches;
      drainingFilteredCaches = [];
      await Promise.allSettled([
        storeQueue,
        ...activeOpfsWrites,
        ...snap,
        ...[...filteredCaches.values()].map((c) => c.drain()),
      ]);
    },
  };
  activeSources.add(new WeakRef(closureHandle));
  function syncClosurePin(): void {
    if (
      pendingStoreWrites > 0 ||
      activeOpfsWrites.size > 0 ||
      storeIndexFlushPinTimer !== null ||
      inFlightFiles.size > 0 ||
      inFlightRanges.size > 0 ||
      activeRangeControllers.size > 0
    ) {
      pendingSourceClosures.add(closureHandle);
    } else {
      pendingSourceClosures.delete(closureHandle);
    }
  }
  const pinStoreIndexFlush = () => {
    if (storeIndexFlushPinTimer !== null) {
      clearTimeout(storeIndexFlushPinTimer);
    }
    storeIndexFlushPinTimer = setTimeout(() => {
      storeIndexFlushPinTimer = null;
      syncClosurePin();
    }, 2_200);
    (storeIndexFlushPinTimer as { unref?: () => void }).unref?.();
    syncClosurePin();
  };
  const touchStoreIndex = (key: string, bytes?: number) => {
    storeIndex.touch(key, bytes);
    pinStoreIndexFlush();
  };
  const forgetStoreIndex = (key: string) => {
    storeIndex.forget(key);
    pinStoreIndexFlush();
  };
  const pinManifestStoreIndex = (keys: Iterable<string>) => {
    storeIndex.useManifest(keys);
    pinStoreIndexFlush();
  };

  const buildRequestHeaders = (
    options: BrowserSourceOptions | undefined,
    extra?: Record<string, string>,
  ) => {
    const headers: Record<string, string> = { ...extra };
    const pageLoadId = options?.getPageLoadId?.();
    if (pageLoadId) {
      headers["X-Page-Load-ID"] = pageLoadId;
    }
    if (options?.debugResponses?.()) {
      headers["X-Debug-Responses"] = "1";
    }
    if (options?.serviceId) headers["x-service-id"] = options.serviceId;
    return Object.keys(headers).length > 0 ? headers : undefined;
  };

  const getFileCache = (): Promise<Cache | null> => {
    const openEpoch = cacheEpoch;
    if (!fileCachePromise) {
      const pending = openCache(CACHE_NAME).then((cache) =>
        openEpoch === cacheEpoch ? cache : null,
      );
      if (openEpoch === cacheEpoch) fileCachePromise = pending;
      return pending;
    }
    return fileCachePromise.then((cache) =>
      openEpoch === cacheEpoch ? cache : null,
    );
  };

  const getRollupCache = (): Promise<Cache | null> => {
    const openEpoch = cacheEpoch;
    if (!rollupCachePromise) {
      const pending = deleteCaches(
        (name) =>
          name.startsWith(ROLLUP_CACHE_PREFIX) && name !== ROLLUP_CACHE_NAME,
      )
        .then(() =>
          openEpoch === cacheEpoch ? openCache(ROLLUP_CACHE_NAME) : null,
        )
        .then((cache) => {
          if (!cache || openEpoch !== cacheEpoch) return null;
          void cache
            ?.delete?.(`${baseUrl}/__flp_rollup_snapshot?v=1`)
            .catch(() => {});
          return cache;
        });
      if (openEpoch === cacheEpoch) rollupCachePromise = pending;
      return pending;
    }
    return rollupCachePromise.then((cache) =>
      openEpoch === cacheEpoch ? cache : null,
    );
  };

  const rollupMemoryKey = (key: string, scope?: string) =>
    JSON.stringify([scope ?? null, key]);
  const forgetRollup = (key: string, scope?: string): void => {
    const memoryKey = rollupMemoryKey(key, scope);
    const existing = memoryRollups.get(memoryKey);
    if (!existing) return;
    memoryRollupBytes -= existing.bytes;
    memoryRollups.delete(memoryKey);
  };

  const rememberRollup = (
    key: string,
    rollup: FileRollup,
    variant: string,
    bytes = estimateRollupBytes(rollup),
    scope?: string,
  ): void => {
    const memoryKey = rollupMemoryKey(key, scope);
    forgetRollup(key, scope);
    if (bytes > maxMemoryRollupBytes) return;
    while (
      memoryRollups.size > 0 &&
      memoryRollupBytes + bytes > maxMemoryRollupBytes
    ) {
      const oldestKey = memoryRollups.keys().next().value;
      if (oldestKey === undefined) break;
      const existing = memoryRollups.get(oldestKey);
      if (existing) {
        memoryRollupBytes -= existing.bytes;
        memoryRollups.delete(oldestKey);
      }
    }
    memoryRollups.set(memoryKey, { rollup, bytes, variant });
    memoryRollupBytes += bytes;
  };

  const rememberFile = (key: string, bytes: ArrayBuffer, identity?: string): void => {
    const existing = memoryFiles.get(key);
    if (existing) {
      memoryFileBytes -= existing.byteLength;
      memoryFiles.delete(key);
      memoryFileIdentities.delete(key);
    }
    const size = bytes.byteLength;
    if (size > MAX_MEMORY_FILE_BYTES) return;
    while (
      memoryFiles.size > 0 &&
      memoryFileBytes + size > MAX_MEMORY_FILE_BYTES
    ) {
      const oldestKey = memoryFiles.keys().next().value;
      if (oldestKey === undefined) break;
      const oldest = memoryFiles.get(oldestKey);
      memoryFiles.delete(oldestKey);
      memoryFileIdentities.delete(oldestKey);
      if (oldest) memoryFileBytes -= oldest.byteLength;
    }
    memoryFiles.set(key, bytes);
    memoryFileIdentities.set(key, identity);
    memoryFileBytes += size;
  };

  const forgetFile = (key: string): void => {
    const bytes = memoryFiles.get(key);
    if (bytes) memoryFileBytes -= bytes.byteLength;
    memoryFiles.delete(key);
    memoryFileIdentities.delete(key);
  };

  /**
   * Writes after making room under the quota. A write that can't fit or fails
   * is recorded for /cache; the query still uses the bytes it already has.
   */
  const store = (
    cacheName: string,
    cache: Cache,
    url: string,
    key: string,
    body: () => Promise<{
      data: BodyInit;
      bytes: number;
      headers: HeadersInit;
    }>,
    tracked: boolean,
    callEpoch: number,
  ): Promise<void> => {
    const callMetaEpoch = currentStoreMetaEpoch();
    if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
      return Promise.resolve();
    }
    pendingStoreWrites += 1;
    syncClosurePin();
    storeQueue = storeQueue.then(async () => {
      if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
        return;
      }
      let size = 0;
      let room: { usage: number | null; quota: number | null } = {
        usage: null,
        quota: null,
      };
      try {
        const { data, bytes, headers } = await body();
        if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
          return;
        }
        size = bytes;
        const made = await storeIndex.makeRoom(
          () => (callEpoch === cacheEpoch ? getFileCache() : Promise.resolve(null)),
          bytes,
          forgetFile,
        );
        if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
          return;
        }
        room = made;
        if (!made.ok) {
          await recordStoreError(
            {
              at: Date.now(),
              cache: cacheName,
              key,
              bytes,
              message:
                "Not enough browser storage: every cached file is in use by the current range.",
              usage: made.usage,
              quota: made.quota,
            },
            callMetaEpoch,
          );
          return;
        }
        if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
          return;
        }
        await cache.put(url, new Response(data, { headers }));
        if (tracked && callEpoch === cacheEpoch) touchStoreIndex(key, bytes);
      } catch (error) {
        if (callEpoch !== cacheEpoch || callMetaEpoch !== currentStoreMetaEpoch()) {
          return;
        }
        await recordStoreError(
          {
            at: Date.now(),
            cache: cacheName,
            key,
            bytes: size,
            message: describeError(error),
            usage: room.usage,
            quota: room.quota,
          },
          callMetaEpoch,
        );
      } finally {
        pendingStoreWrites = Math.max(0, pendingStoreWrites - 1);
        syncClosurePin();
      }
    });
    return storeQueue;
  };

  const derivedCacheUrl = (
    key: string,
    identity: string,
    options?: BrowserSourceOptions,
  ) =>
    scopedCacheUrl(
      `${baseUrl}${fileRequestPath(key)}`,
      fileOwnerScope(options),
      `derived:${identity}`,
    );

  const sourceScope = (options?: BrowserSourceOptions) => {
    const scope = currentCacheScope();
    return options?.serviceId
      ? `${scope ?? "browser"}:${options.serviceId}`
      : scope ?? "browser";
  };
  const fileScope = (options?: BrowserSourceOptions) =>
    options?.serviceId ? sourceScope(options) : currentCacheScope();
  const fileOwnerScope = (options?: BrowserSourceOptions) =>
    options?.serviceId ? sourceScope(options) : undefined;
  const fileMemoryIdentity = (
    options: BrowserSourceOptions | undefined,
    identity?: string,
  ) => JSON.stringify([fileOwnerScope(options) ?? null, identity ?? null]);
  const fileMemoryIdentityOwnedBy = (
    stored: string | undefined,
    options: BrowserSourceOptions | undefined,
  ) => {
    if (stored === undefined) return false;
    const decoded = JSON.parse(stored) as [unknown, unknown];
    return decoded[0] === (fileOwnerScope(options) ?? null);
  };
  const sourceWith = (
    sourceOptions?: BrowserSourceOptions,
  ): BrowserFileSource => {
    const options = withCallStart(sourceOptions);
    const instance: BrowserFileSource = {
    async readDerivedFile(key, identity) {
      const callEpoch = cacheEpoch;
      if (callEpoch !== cacheEpoch) return null;
      const cache = await getFileCache();
      if (!cache || callEpoch !== cacheEpoch) return null;
      const hit = await cache.match(derivedCacheUrl(key, identity, options));
      if (callEpoch !== cacheEpoch) {
        await hit?.body?.cancel();
        return null;
      }
      const byteLength = Number(hit?.headers.get(DERIVED_BYTES_HEADER));
      if (!hit || !Number.isSafeInteger(byteLength) || byteLength < 0) {
        await hit?.body?.cancel();
        return null;
      }
      let decodedMeta = "";
      try {
        decodedMeta = decodeURIComponent(hit.headers.get(DERIVED_META_HEADER) ?? "");
      } catch {
        await hit.body?.cancel();
        return null;
      }
      return {
        byteLength,
        meta: decodedMeta,
        bytes: async () => {
          if (callEpoch !== cacheEpoch) {
            await hit.body?.cancel();
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          const start = performance.now();
          const bytes = await hit.arrayBuffer();
          if (callEpoch !== cacheEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          if (bytes.byteLength !== byteLength) throw new Error(`Cached derived file for ${key} is truncated`);
          touchStoreIndex(key);
          options?.onApiCall?.({
            source: "Browser Cache",
            method: "GET",
            path: fileDisplayPath(key),
            status: 200,
            duration_ms: Math.max(0, Math.round(performance.now() - start)),
            bytes: byteLength,
            cache: "browser",
            endpoint: "GET /api/file",
            page_load_id: options?.getPageLoadId?.(),
          });
          return bytes;
        },
      };
    },
    async writeDerivedFile(key, identity, bytes, meta) {
      const callEpoch = cacheEpoch;
      // Copied before any await: the caller may hand the buffer to DuckDB, which can detach it.
      const data = new Blob([bytes]);
      if (callEpoch !== cacheEpoch) return;
      const cache = await getFileCache();
      if (!cache || callEpoch !== cacheEpoch) return;
      // Tracked through the full file's index entry so its recorded size stays the file's own.
      await store(CACHE_NAME, cache, derivedCacheUrl(key, identity, options), key, async () => ({
        data,
        bytes: data.size,
        headers: {
          [DERIVED_BYTES_HEADER]: String(data.size),
          [DERIVED_META_HEADER]: encodeURIComponent(meta),
        },
      }), false, callEpoch);
      if (callEpoch === cacheEpoch) touchStoreIndex(key);
    },
    get cacheScope() {
      return sourceScope(options);
    },
    async readFilteredResult(id) {
      try {
        return await filteredCache(sourceScope(options)).read(id);
      } catch {
        return null;
      }
    },
    async writeFilteredResult(id, result) {
      const callEpoch = cacheEpoch;
      if (callEpoch !== cacheEpoch) return;
      try {
        await filteredCache(sourceScope(options)).write(id, result);
      } catch {
        /* quota, private browsing or no Cache API: the result stays in memory */
      }
    },
    withOptions: (next) => sourceWith(next),
    async openParquet(file, signal, variant) {
      const callEpoch = cacheEpoch;
      const openMetaEpoch = currentStoreMetaEpoch();
      signal?.throwIfAborted();
      if (callEpoch !== cacheEpoch || openMetaEpoch !== currentStoreMetaEpoch()) {
        throw new DOMException("The operation was aborted.", "AbortError");
      }
      const origin = globalThis.location?.origin;
      const reqPath = fileRequestPath(file.key);
      const path = fileDisplayPath(file.key);
      const url = new URL(`${baseUrl}${reqPath}`, origin ?? "http://localhost");
      if (origin && url.origin !== origin)
        throw new Error(
          "Selective Parquet reads require the authenticated same-origin file route",
        );
      const controller = new AbortController();
      const onCallerAbort = () => controller.abort(signal?.reason);
      if (signal) {
        if (signal.aborted) controller.abort(signal.reason);
        else signal.addEventListener("abort", onCallerAbort, { once: true });
      }
      activeRangeControllers.add(controller);
      syncClosurePin();
      let onOpAbort: (() => void) | undefined;
      const aborted = new Promise<never>((_, reject) => {
        onOpAbort = () =>
          reject(
            controller.signal.reason ??
              new DOMException("The operation was aborted.", "AbortError"),
          );
        if (controller.signal.aborted) onOpAbort();
        else controller.signal.addEventListener("abort", onOpAbort, { once: true });
      });
      const openWork = (async () => {
        const local = await opfsStore(variant).open(
          fileIdentity(file),
          controller.signal,
        );
        if (callEpoch !== cacheEpoch) {
          local?.release();
          throw new DOMException("The operation was aborted.", "AbortError");
        }
        if (local)
          return {
            kind: "opfs" as const,
            ...local,
            cached: true,
            fallbackUrl: url.href,
            onPersistenceError: async (error: unknown) => {
              if (
                callEpoch !== cacheEpoch ||
                openMetaEpoch !== currentStoreMetaEpoch()
              ) {
                return;
              }
              const estimate = await storageEstimate();
              if (
                callEpoch !== cacheEpoch ||
                openMetaEpoch !== currentStoreMetaEpoch()
              ) {
                return;
              }
              await recordStoreError(
                {
                  at: Date.now(),
                  cache: OPFS_DIRECTORY,
                  key: file.key,
                  bytes: file.size,
                  message: `OPFS read unavailable; using nonpersistent selective HTTP reads: ${describeError(error)}`,
                  ...estimate,
                },
                openMetaEpoch,
              );
            },
          };
        // Catalog keys are write-once and the listing carries their size, so
        // the probe round trip only runs for files without a listed size.
        if (file.size > 0 && !file.etag)
          return { kind: "http" as const, url: url.href, cached: false };
        const start = performance.now();
        const response = await fetchRetrying(url.href, {
          credentials: "same-origin",
          cache: "no-store",
          signal: controller.signal,
          headers: buildRequestHeaders(options, { Range: "bytes=0-0" }),
        });
        try {
          if (callEpoch !== cacheEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          const range = /^bytes 0-0\/(\d+)$/.exec(
            response.headers.get("content-range") ?? "",
          );
          if (
            response.status !== 206 ||
            !range ||
            !Number.isSafeInteger(Number(range[1])) ||
            Number(range[1]) <= 0 ||
            (file.size > 0 && Number(range[1]) !== file.size)
          ) {
            throw new Error(
              `Selective file ${file.key} failed (${response.status}): a valid 206 Content-Range with matching object length is required`,
            );
          }
          if (file.etag && response.headers.get("etag") !== file.etag)
            throw new Error(`Immutable file identity changed: ${file.key}`);
          const probe = await response.arrayBuffer();
          if (callEpoch !== cacheEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          if (probe.byteLength !== 1)
            throw new Error("Invalid HTTP Range response length");
          const cdn = response.headers.get("x-flp-cdn-cache");
          const cdnHit = isCdnHit(cdn);
          const vcpuMs = parseVcpuMsHeader(response.headers);
          options?.onApiCall?.({
            source: cdnHit
              ? "CDN Cache"
              : cdn !== null && vcpuMs === null
                ? "FOS S3"
                : "Compute API",
            method: "GET",
            path,
            status: 206,
            duration_ms: Math.round(performance.now() - start),
            bytes: 1,
            vcpu_ms: vcpuMs,
            cache: cdnHit ? "cdn" : "miss",
            endpoint: "GET /api/file",
            server_timing: parseServerTiming(response.headers.get("server-timing")),
            page_load_id: options?.getPageLoadId?.(),
          });
          return { kind: "http" as const, url: url.href, cached: false };
        } finally {
          if (!response.bodyUsed) await response.body?.cancel();
        }
      })();
      openWork.catch(() => {});
      try {
        return await Promise.race([openWork, aborted]);
      } finally {
        activeRangeControllers.delete(controller);
        syncClosurePin();
        if (signal) signal.removeEventListener("abort", onCallerAbort);
        if (onOpAbort) controller.signal.removeEventListener("abort", onOpAbort);
      }
    },
    async hasPersistentFile(file, signal, variant) {
      const callEpoch = cacheEpoch;
      if (callEpoch !== cacheEpoch) return false;
      const found = await opfsStore(variant).has(
        fileIdentity(file),
        file.size,
        signal,
      );
      return callEpoch === cacheEpoch && found;
    },
    async persistFile(file, signal, variant) {
      const callEpoch = cacheEpoch;
      signal?.throwIfAborted();
      if (callEpoch !== cacheEpoch) {
        throw new DOMException("The operation was aborted.", "AbortError");
      }
      const reqPath = fileRequestPath(file.key);
      const path = fileDisplayPath(file.key);
      const origin = globalThis.location?.origin;
      const url = new URL(`${baseUrl}${reqPath}`, origin ?? "http://localhost");
      if (origin && url.origin !== origin) {
        throw new Error(
          "Persistent Parquet reads require the authenticated same-origin file route",
        );
      }
      const controller = new AbortController();
      const onCallerAbort = () => controller.abort(signal?.reason);
      if (signal) {
        if (signal.aborted) controller.abort(signal.reason);
        else signal.addEventListener("abort", onCallerAbort, { once: true });
      }
      activeRangeControllers.add(controller);
      syncClosurePin();
      let onPersistAbort: (() => void) | undefined;
      const aborted = new Promise<never>((_, reject) => {
        onPersistAbort = () =>
          reject(
            controller.signal.reason ??
              new DOMException("The operation was aborted.", "AbortError"),
          );
        if (controller.signal.aborted) onPersistAbort();
        else controller.signal.addEventListener("abort", onPersistAbort, { once: true });
      });
      const persistWork = (async () => {
        let response: Response | undefined;
        let started = 0;
        let failure: unknown;
        try {
          await opfsStore(variant).persist(
            fileIdentity(file),
            file.size,
            async () => {
              if (callEpoch !== cacheEpoch) {
                throw new DOMException("The operation was aborted.", "AbortError");
              }
              started = performance.now();
              response = await fetchRetrying(url.href, {
                credentials: "same-origin",
                cache: "no-store",
                signal: controller.signal,
                headers: buildRequestHeaders(
                  options,
                  limits.foldFileTelemetry
                    ? { "X-FLP-Fold-Telemetry": "1" }
                    : undefined,
                ),
              });
              if (callEpoch !== cacheEpoch) {
                await response.body?.cancel();
                throw new DOMException("The operation was aborted.", "AbortError");
              }
              if (file.etag && response.headers.get("etag") !== file.etag) {
                await response.body?.cancel();
                throw new Error(`Immutable file identity changed: ${file.key}`);
              }
              return response;
            },
            controller.signal,
          );
          if (callEpoch !== cacheEpoch) {
            throw new DOMException("The operation was aborted.", "AbortError");
          }
          return { downloaded: response !== undefined };
        } catch (error) {
          failure = error;
          throw error;
        } finally {
          if (response && callEpoch === cacheEpoch) {
            const vcpuMs = parseVcpuMsHeader(response.headers);
            const cdn = response.headers.get("x-flp-cdn-cache");
            const cdnHit = isCdnHit(cdn);
            const directCdn = cdn !== null && vcpuMs === null;
            if (limits.foldFileTelemetry && !directCdn) {
              folded.requests++;
              if (vcpuMs !== null) folded.vcpuMs += vcpuMs;
            }
            const pageLoadId = options?.getPageLoadId?.();
            options?.onApiCall?.({
              source: cdnHit
                ? "CDN Cache"
                : directCdn
                  ? "FOS S3"
                  : "Compute API",
              method: "GET",
              path,
              status: response.status,
              duration_ms: Math.round(performance.now() - started),
              bytes: failure ? null : file.size,
              vcpu_ms: vcpuMs,
              cache: cdnHit ? "cdn" : "miss",
              error: failure ? describeError(failure) : null,
              endpoint: "GET /api/file",
              server_timing: parseServerTiming(response.headers.get("server-timing")),
              page_load_id: pageLoadId,
            });
            emitDebugSubCalls(
              response,
              "GET /api/file",
              pageLoadId,
              options?.onApiCall,
            );
          }
        }
      })();
      persistWork.catch(() => {});
      try {
        return await Promise.race([persistWork, aborted]);
      } finally {
        activeRangeControllers.delete(controller);
        syncClosurePin();
        if (signal) signal.removeEventListener("abort", onCallerAbort);
        if (onPersistAbort) controller.signal.removeEventListener("abort", onPersistAbort);
      }
    },
    fetchJson: Object.assign(
      async (url: string, signal?: AbortSignal) => {
        const headers = buildRequestHeaders(options);
        if (options?.serviceId) {
          const scopedUrl = new URL(url, globalThis.location?.origin ?? "http://localhost");
          scopedUrl.searchParams.set("service_id", options.serviceId);
          url = `${scopedUrl.pathname}${scopedUrl.search}`;
        }
        const pageLoadId = options?.getPageLoadId?.();
        const start =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        const response = await fetchRetrying(baseUrl + url, {
          credentials: "same-origin",
          ...(headers ? { headers } : {}),
          signal,
        });
        const end =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        const durationMs = Math.max(0, Math.round(end - start));
        const endpoint = `GET ${url.split("?")[0]}`;
        const vcpuMs = parseVcpuMsHeader(response?.headers);
        options?.onApiCall?.({
          source: "Compute API",
          method: "GET",
          path: url,
          status: response?.status ?? null,
          duration_ms: durationMs,
          vcpu_ms: vcpuMs,
          endpoint,
          server_timing: parseServerTiming(
            response?.headers?.get?.("server-timing"),
          ),
          page_load_id: pageLoadId,
        });
        emitDebugSubCalls(response, endpoint, pageLoadId, options?.onApiCall);
        if (!response.ok) {
          let code: string | undefined;
          if (response.status === 409 || response.status === 503) {
            try {
              const body = (await manifestJson(response)) as {
                code?: unknown;
                error?: unknown;
                detail?: unknown;
              };
              if (typeof body.code === "string") code = body.code;
              else if (typeof body.error === "string") code = body.error;
              else if (typeof body.detail === "string") code = body.detail;
            } catch {
              // Non-JSON error response; preserve HTTP status without a code.
            }
          }
          throw new ManifestRequestError(response.status, code);
        }
        return url.split("?", 1)[0] === "/api/files"
          ? manifestJson(response)
          : response.json();
      },
      {
        manifestCacheScope: () =>
          dashboardManifestCacheScope(
            baseUrl,
            options?.serviceId,
            limits.cacheScope ?? dashboardCacheScope(),
          ),
      },
    ),
    async fetchRange(file, start, end, signal, priority) {
      if (!(Number.isSafeInteger(start) && Number.isSafeInteger(end) &&
          start >= 0 && end > start && (file.size <= 0 || end <= file.size)))
        throw new Error(`Range ${start}-${end} is outside ${file.key}`);
      signal?.throwIfAborted();
      const callEpoch = cacheEpoch;
      const rangeKey = JSON.stringify([
        fileOwnerScope(options) ?? null,
        file.key,
        file.etag ?? "",
        start,
        end,
      ]);
      const existing = inFlightRanges.get(rangeKey);
      if (existing && !existing.controller.signal.aborted) {
        const bytes = await waitSharedRange(existing, signal);
        return bytes.slice(0);
      }
      const controller = new AbortController();
      const path = fileDisplayPath(file.key);
      const headers = buildRequestHeaders(options, {
        Range: `bytes=${start}-${end - 1}`,
        ...(limits.foldFileTelemetry ? { "X-FLP-Fold-Telemetry": "1" } : {}),
      });
      const begin = performance.now();
      const entry: InFlightRange = {
        controller,
        waiters: 0,
        complete: false,
        promise: (async () => {
          const response = await fetchRetrying(`${baseUrl}${fileRequestPath(file.key)}`, {
            credentials: "same-origin",
            cache: "no-store",
            signal: controller.signal,
            ...(priority ? { priority } : {}),
            ...(headers ? { headers } : {}),
          });
          try {
            if (response.status === 404) throw new MissingCloudFileError(file.key);
            const range = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(
              response.headers.get("content-range") ?? "",
            );
            if (
              response.status !== 206 ||
              !range ||
              Number(range[1]) !== start ||
              Number(range[2]) !== end - 1 ||
              (file.size > 0 && Number(range[3]) !== file.size)
            )
              throw new Error(
                `Range read of ${file.key} failed (${response.status}): the exact 206 Content-Range is required`,
              );
            if (file.etag && response.headers.get("etag") !== file.etag)
              throw new Error(`Immutable file identity changed: ${file.key}`);
            const bytes = await response.arrayBuffer();
            if (bytes.byteLength !== end - start)
              throw new Error(`Range read of ${file.key} returned ${bytes.byteLength} bytes, not ${end - start}`);
            if (callEpoch !== cacheEpoch) throw new Error("Cache cleared");
            const cdn = response.headers.get("x-flp-cdn-cache");
            const cdnHit = isCdnHit(cdn);
            const vcpuMs = parseVcpuMsHeader(response.headers);
            const directCdn = cdn !== null && vcpuMs === null;
            if (limits.foldFileTelemetry && !directCdn) {
              folded.requests += 1;
              if (vcpuMs !== null) folded.vcpuMs += vcpuMs;
            }
            options?.onApiCall?.({
              source: cdnHit ? "CDN Cache" : directCdn ? "FOS S3" : "Compute API",
              method: "GET",
              path,
              status: 206,
              duration_ms: Math.max(0, Math.round(performance.now() - begin)),
              vcpu_ms: vcpuMs,
              bytes: bytes.byteLength,
              cache: cdnHit ? "cdn" : "miss",
              endpoint: "GET /api/file",
              server_timing: parseServerTiming(response.headers.get("server-timing")),
              page_load_id: options?.getPageLoadId?.(),
            });
            return bytes;
          } finally {
            if (!response.bodyUsed) await response.body?.cancel();
          }
        })().finally(() => {
          entry.complete = true;
          if (inFlightRanges.get(rangeKey) === entry) {
            inFlightRanges.delete(rangeKey);
          }
          syncClosurePin();
        }),
      };
      entry.promise.catch(() => {});
      inFlightRanges.set(rangeKey, entry);
      syncClosurePin();
      const bytes = await waitSharedRange(entry, signal);
      return bytes.slice(0);
    },
    async fetchFile(key, signal, mode) {
      const callEpoch = cacheEpoch;
      const transfer = Boolean(mode?.transfer);
      const reqPath = fileRequestPath(key);
      const path = fileDisplayPath(key);
      const pageLoadId = options?.getPageLoadId?.();
      const ownerScope = fileOwnerScope(options);
      const requestScope = fileMemoryIdentity(options, mode?.identity);
      const inMemory = memoryFiles.get(key);
      if (inMemory && memoryFileIdentities.get(key) === requestScope) {
        if (transfer) {
          memoryFiles.delete(key);
          memoryFileIdentities.delete(key);
          memoryFileBytes -= inMemory.byteLength;
        } else {
          rememberFile(key, inMemory, requestScope);
        }
        options?.onApiCall?.({
          source: "Browser Cache",
          method: "GET",
          path,
          status: 200,
          duration_ms: 0,
          bytes: inMemory.byteLength,
          cache: "browser",
          endpoint: "GET /api/file",
          page_load_id: pageLoadId,
        });
        return { bytes: inMemory.slice(0), cached: true, cdn: null };
      }
      const url = `${baseUrl}${reqPath}`;
      const cacheUrl = scopedCacheUrl(url, ownerScope, mode?.identity);
      signal?.throwIfAborted();
      let inFlight = inFlightFiles.get(cacheUrl);
      const cached = inFlight !== undefined;
      if (!inFlight) {
        const entryEpoch = callEpoch;
        const controller = new AbortController();
        const entry: InFlightFile = {
          controller,
          promise: undefined as unknown as InFlightFile["promise"],
          waiters: 0,
          networkDone: false,
          complete: false,
        };
        const finish = () => {
          if (inFlightFiles.get(cacheUrl) !== entry) return;
          entry.complete = true;
          if (entry.waiters === 0) {
            inFlightFiles.delete(cacheUrl);
            syncClosurePin();
          }
        };
        entry.promise = Promise.resolve().then(async () => {
          const cacheStart =
            typeof performance !== "undefined" ? performance.now() : Date.now();
          try {
            if (entryEpoch !== cacheEpoch) {
              throw controller.signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
            }
            const cache = await getFileCache();
            if (entryEpoch !== cacheEpoch) {
              throw controller.signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
            }
            const hit = await cache?.match(cacheUrl);
            if (hit) {
              const bytes = await hit.arrayBuffer();
              if (entryEpoch !== cacheEpoch) {
                throw controller.signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
              }
              touchStoreIndex(key, bytes.byteLength);
              const cacheEnd =
                typeof performance !== "undefined"
                  ? performance.now()
                  : Date.now();
              if (!transfer && entryEpoch === cacheEpoch) rememberFile(key, bytes, requestScope);
              options?.onApiCall?.({
                source: "Browser Cache",
                method: "GET",
                path,
                status: 200,
                duration_ms: Math.max(0, Math.round(cacheEnd - cacheStart)),
                bytes: bytes.byteLength,
                cache: "browser",
                endpoint: "GET /api/file",
                page_load_id: pageLoadId,
              });
              entry.networkDone = true;
              finish();
              return { bytes, cached: true, cdn: null };
            }
            const headers = buildRequestHeaders(
              options,
              limits.foldFileTelemetry
                ? { "X-FLP-Fold-Telemetry": "1" }
                : undefined,
            );
            const start =
              typeof performance !== "undefined"
                ? performance.now()
                : Date.now();
            // CacheStorage is the only persistent copy; skip the HTTP cache,
            // which would otherwise keep a second one because /api/file is immutable.
            const response = await fetchRetrying(url, {
              credentials: "same-origin",
              cache: "no-store",
              ...(headers ? { headers } : {}),
              ...(mode?.priority ? { priority: mode.priority } : {}),
              signal: controller.signal,
            });
            if (!response.ok) {
              const end =
                typeof performance !== "undefined"
                  ? performance.now()
                  : Date.now();
              options?.onApiCall?.({
                source: "Compute API",
                method: "GET",
                path,
                status: response.status,
                duration_ms: Math.max(0, Math.round(end - start)),
                endpoint: "GET /api/file",
                server_timing: parseServerTiming(response.headers.get("server-timing")),
                page_load_id: pageLoadId,
              });
              if (response.status === 404) throw new MissingCloudFileError(key);
              throw new Error(`File ${key} failed (${response.status})`);
            }
            const bytes = await response.arrayBuffer();
            if (entryEpoch !== cacheEpoch) {
              throw controller.signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
            }
            const end =
              typeof performance !== "undefined"
                ? performance.now()
                : Date.now();
            const cdn = response.headers.get("x-flp-cdn-cache");
            const cdnHit = isCdnHit(cdn);
            const vcpuMs = parseVcpuMsHeader(response.headers);
            const directCdn = cdn !== null && vcpuMs === null;
            if (limits.foldFileTelemetry && !directCdn) {
              folded.requests += 1;
              if (vcpuMs !== null) folded.vcpuMs += vcpuMs;
            }
            options?.onApiCall?.({
              source: cdnHit
                ? "CDN Cache"
                : directCdn
                  ? "FOS S3"
                  : "Compute API",
              method: "GET",
              path,
              status: response.status,
              duration_ms: Math.max(0, Math.round(end - start)),
              vcpu_ms: vcpuMs,
              bytes: bytes.byteLength,
              cache: cdnHit ? "cdn" : "miss",
              endpoint: "GET /api/file",
              server_timing: parseServerTiming(response.headers.get("server-timing")),
              page_load_id: pageLoadId,
            });
            emitDebugSubCalls(
              response,
              "GET /api/file",
              pageLoadId,
              options?.onApiCall,
            );
            if (cache && entryEpoch === cacheEpoch) {
              const byteLength = bytes.byteLength;
              const persistence = store(
                CACHE_NAME,
                cache,
                cacheUrl,
                key,
                async () => ({
                  data: bytes.slice(0),
                  bytes: byteLength,
                  headers: response.headers,
                }),
                true,
                entryEpoch,
              );
              void persistence.then(finish, finish);
            } else {
              entry.networkDone = true;
              finish();
              return { bytes, cached: false, cdn };
            }
            entry.networkDone = true;
            return { bytes, cached: false, cdn };
          } catch (error) {
            entry.networkDone = true;
            finish();
            throw error;
          }
        });
        inFlightFiles.set(cacheUrl, entry);
        syncClosurePin();
        inFlight = entry;
      }
      const request = inFlight;
      request.waiters += 1;
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        request.waiters -= 1;
        if (request.waiters === 0) {
          if (!request.networkDone) {
            if (inFlightFiles.get(cacheUrl) === request) {
              inFlightFiles.delete(cacheUrl);
            }
            request.controller.abort();
          }
          if (
            request.complete &&
            inFlightFiles.get(cacheUrl) === request
          ) {
            inFlightFiles.delete(cacheUrl);
          }
          syncClosurePin();
        }
      };
      let onAbort: (() => void) | undefined;
      const aborted =
        signal &&
        new Promise<never>((_, reject) => {
          onAbort = () => {
            release();
            reject(signal.reason);
          };
          signal.addEventListener("abort", onAbort, { once: true });
        });
      try {
        const network = await (aborted
          ? Promise.race([request.promise, aborted])
          : request.promise);
        if (callEpoch !== cacheEpoch) {
          throw new DOMException("The operation was aborted.", "AbortError");
        }
        if (!transfer) rememberFile(key, network.bytes.slice(0), requestScope);
        return {
          bytes: network.bytes.slice(0),
          cached: cached || network.cached,
          cdn: network.cdn,
        };
      } finally {
        if (signal && onAbort) signal.removeEventListener("abort", onAbort);
        release();
      }
    },
    async hasFile(key) {
      const callEpoch = cacheEpoch;
      const requestScope = fileMemoryIdentity(options);
      if (
        memoryFiles.has(key) &&
        memoryFileIdentities.get(key) === requestScope
      ) return true;
      try {
        if (callEpoch !== cacheEpoch) return false;
        const cache = await getFileCache();
        if (!cache || callEpoch !== cacheEpoch) return false;
        const url = `${baseUrl}${fileRequestPath(key)}`;
        const hit = await cache.match(scopedCacheUrl(url, fileOwnerScope(options)));
        if (callEpoch !== cacheEpoch) {
          await hit?.body?.cancel();
          return false;
        }
        return Boolean(hit);
      } catch {
        return false;
      }
    },
    async readRollup(key, variant = "") {
      const callEpoch = cacheEpoch;
      const rollupScope = fileScope(options);
      const inMemory = memoryRollups.get(rollupMemoryKey(key, rollupScope));
      if (inMemory?.variant === variant) {
        if (callEpoch === cacheEpoch) {
          rememberRollup(key, inMemory.rollup, variant, inMemory.bytes, rollupScope);
        }
        return inMemory.rollup;
      }
      try {
        if (rollupScope) {
          if (callEpoch !== cacheEpoch) return null;
          const access = await opfsStore(`rollup:${rollupScope}:${variant}`).open(key);
          if (access) {
            try {
              if (callEpoch !== cacheEpoch) return null;
              const file = await access.handle.getFile();
              const decoded = await decodeRollup(
                new Uint8Array(await file.arrayBuffer()),
              );
              if (decoded && decoded.key === key && callEpoch === cacheEpoch) {
                rememberRollup(key, decoded, variant, undefined, rollupScope);
                return decoded;
              }
            } finally {
              access.release();
            }
          }
        }
        if (callEpoch !== cacheEpoch) return null;
        const cache = await getRollupCache();
        if (!cache || callEpoch !== cacheEpoch) return null;
        const hit = await cache.match(
          scopedCacheUrl(rollupCacheUrl(baseUrl, key, variant), rollupScope),
        );
        if (!hit || callEpoch !== cacheEpoch) {
          await hit?.body?.cancel();
          return null;
        }
        const decoded = await decodeRollup(
          new Uint8Array(await hit.arrayBuffer()),
        );
        if (!decoded || decoded.key !== key || callEpoch !== cacheEpoch) return null;
        rememberRollup(key, decoded, variant, undefined, rollupScope);
        return decoded;
      } catch {
        return null;
      }
    },
    async writeRollup(key, rollup, variant = "") {
      const callEpoch = cacheEpoch;
      if (callEpoch !== cacheEpoch) return;
      const rollupScope = fileScope(options);
      rememberRollup(key, rollup, variant, undefined, rollupScope);
      if (rollupScope) {
        const controller = new AbortController();
        activeRangeControllers.add(controller);
        syncClosurePin();
        const op = (async () => {
          try {
            const body = await encodeRollup(rollup);
            if (callEpoch !== cacheEpoch || controller.signal.aborted) return;
            await opfsStore(`rollup:${rollupScope}:${variant}`).persist(
              key,
              body.byteLength,
              async () => {
                if (callEpoch !== cacheEpoch || controller.signal.aborted) {
                  throw new DOMException("The operation was aborted.", "AbortError");
                }
                return new Response(body as BodyInit);
              },
              controller.signal,
            );
          } catch {
            /* The OPFS store records genuine failures; memory remains usable. */
          }
        })();
        activeOpfsWrites.add(op);
        syncClosurePin();
        try {
          await op;
        } finally {
          activeRangeControllers.delete(controller);
          activeOpfsWrites.delete(op);
          syncClosurePin();
        }
        return;
      }
      if (callEpoch !== cacheEpoch) return;
      const cache = await getRollupCache().catch(() => null);
      if (!cache || callEpoch !== cacheEpoch) return;
      await store(
        ROLLUP_CACHE_NAME,
        cache,
        scopedCacheUrl(rollupCacheUrl(baseUrl, key, variant), rollupScope),
        key,
        async () => {
          const body = await encodeRollup(rollup);
          return {
            data: body as BodyInit,
            bytes: body.byteLength,
            headers: { "content-type": "application/octet-stream" },
          };
        },
        false,
        callEpoch,
      );
    },
    async beacon(body) {
      if (folded.requests > 0) {
        body = {
          ...body,
          compute_requests: folded.requests,
          vcpu_ms: folded.vcpuMs,
        };
        folded.requests = 0;
        folded.vcpuMs = 0;
      }
      try {
        const headers = buildRequestHeaders(options, {
          "content-type": "application/json",
        }) ?? { "content-type": "application/json" };
        await fetch(`${baseUrl}/api/telemetry/query`, {
          method: "POST",
          credentials: "same-origin",
          headers,
          body: JSON.stringify(body),
          keepalive: true,
        });
      } catch {
        // Telemetry is best-effort and must never affect the query.
      }
    },
    async pruneCache(manifest, fromMs, toMs) {
      const callEpoch = cacheEpoch;
      if (callEpoch !== cacheEpoch) return;
      pinManifestStoreIndex([
        ...manifest.files.map((file) => file.key),
        ...(manifest.summaryFiles ?? []).map((file) => file.key),
      ]);
      if (pruning) return;
      pruning = true;
      try {
        const [fileCache, rollupCache] = await Promise.all([
          getFileCache(),
          getRollupCache(),
        ]);
        if (callEpoch !== cacheEpoch) return;
        for (const key of fileCache
          ? await pruneRequestCache(fileCache, manifest, fromMs, toMs, {
              scope: fileOwnerScope(options),
            })
          : []) {
          if (callEpoch !== cacheEpoch) return;
          if (fileMemoryIdentityOwnedBy(memoryFileIdentities.get(key), options)) {
            forgetFile(key);
          }
          forgetStoreIndex(key);
        }
        for (const key of rollupCache
          ? await pruneRequestCache(rollupCache, manifest, fromMs, toMs, {
              scope: fileScope(options),
            })
          : []) {
          if (callEpoch !== cacheEpoch) return;
          forgetRollup(key, fileScope(options));
        }
      } catch {
        // Pruning is best-effort and must never fail the query.
      } finally {
        pruning = false;
      }
    },
    clearMemoryCache() {
      clearClosureMemory();
    },
    };
    sourceKeepAlive.set(instance, closureHandle);
    return instance;
  };
  return sourceWith(defaultOptions);
}
