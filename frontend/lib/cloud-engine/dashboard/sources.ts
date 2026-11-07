import { fnv1a32, keyWindowMs } from "../parquet-key";
import { CARD_FIELDS } from "./summaries";
import { isCdnHit } from "./cdn-cache";
import type { FileRollup, FileSource } from "./engine";
import {
  STORE_META_CACHE,
  StoreIndex,
  describeError,
  getStoreStats,
  recordStoreError,
  storageEstimate,
} from "./file-store";
import type { Manifest } from "./manifest";
import { MAX_MANIFEST_BYTES, ManifestRequestError } from "./catalog-manifest";
import {
  decodeRollup,
  encodeRollup,
  estimateRollupBytes,
} from "./rollup-codec";
import {
  OPFS_DIRECTORY,
  OPFSParquetStore,
  clearOPFSParquetStore,
} from "./store-index";

export { getStoreStats, keyWindowMs };

const SESSION_CACHE_KEY = "flp-dashboard-cache-session";
const IDENTITY_CACHE_KEY = "flp-dashboard-cache-identity";
let volatileCacheScope: string | undefined;

export function dashboardCacheScope(): string {
  try {
    let session = sessionStorage.getItem(SESSION_CACHE_KEY);
    if (!session) {
      session = crypto.randomUUID();
      sessionStorage.setItem(SESSION_CACHE_KEY, session);
    }
    return JSON.stringify([
      session,
      sessionStorage.getItem(IDENTITY_CACHE_KEY) ?? "",
    ]);
  } catch {
    return (volatileCacheScope ??= crypto.randomUUID());
  }
}

export function dashboardCacheIdentity(): string | null {
  try {
    return sessionStorage.getItem(IDENTITY_CACHE_KEY);
  } catch {
    return null;
  }
}

export function updateDashboardCacheIdentity(identity: string | null): void {
  volatileCacheScope = undefined;
  try {
    sessionStorage.removeItem(SESSION_CACHE_KEY);
    if (identity === null) sessionStorage.removeItem(IDENTITY_CACHE_KEY);
    else sessionStorage.setItem(IDENTITY_CACHE_KEY, identity);
  } catch {
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

function scopedCacheUrl(url: string, scope?: string): string {
  if (!scope) return url;
  return `${url}${url.includes("?") ? "&" : "?"}scope=${encodeURIComponent(scope)}`;
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
  await clearOPFSParquetStore();
  await deleteCaches(
    (name) =>
      name === CACHE_NAME ||
      name === STORE_META_CACHE ||
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
  options: { retentionCutoffMs?: number; maxDeletes?: number } = {},
): Promise<string[]> {
  const cutoff =
    options.retentionCutoffMs ?? Date.now() - BROWSER_CACHE_RETENTION_MS;
  const maxDeletes = options.maxDeletes ?? MAX_PRUNE_DELETES;
  const listed = new Set(manifest.files.map((file) => file.key));
  const prefixes = new Set(
    manifest.files.map((file) => keyPrefix(file.key)).filter(Boolean),
  );
  const deleted: string[] = [];
  for (const request of await cache.keys()) {
    if (deleted.length >= maxDeletes) break;
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
};

export type BrowserSourceOptions = {
  getPageLoadId?: () => string;
  debugResponses?: () => boolean;
  onApiCall?: (call: BrowserSourceCallEvent) => void;
  serviceId?: string;
};

export type BrowserSourceTelemetry = BrowserSourceOptions;

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
  /**
   * The same caches with different telemetry, so concurrent requests in one
   * worker each report their own page load and calls.
   */
  withOptions(options?: BrowserSourceOptions): BrowserFileSource;
};

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
  const cacheScope =
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
  let fileCachePromise: Promise<Cache | null> | null = null;
  let rollupCachePromise: Promise<Cache | null> | null = null;
  let pruning = false;
  const storeIndex = new StoreIndex();
  const opfsStores = new Map<string, OPFSParquetStore>();
  const opfsStore = (variant = "") => {
    let store = opfsStores.get(variant);
    if (!store) {
      store = new OPFSParquetStore(
        JSON.stringify([
          baseUrl,
          cacheScope ?? "browser",
          ROLLUP_CACHE_VERSION,
          variant,
        ]),
      );
      opfsStores.set(variant, store);
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
  // One write at a time, so each quota check sees the previous write's usage.
  let storeQueue: Promise<void> = Promise.resolve();

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
    if (!fileCachePromise) fileCachePromise = openCache(CACHE_NAME);
    return fileCachePromise;
  };

  const getRollupCache = (): Promise<Cache | null> => {
    if (!rollupCachePromise) {
      rollupCachePromise = deleteCaches(
        (name) =>
          name.startsWith(ROLLUP_CACHE_PREFIX) && name !== ROLLUP_CACHE_NAME,
      )
        .then(() => openCache(ROLLUP_CACHE_NAME))
        .then((cache) => {
          void cache
            ?.delete?.(`${baseUrl}/__flp_rollup_snapshot?v=1`)
            .catch(() => {});
          return cache;
        });
    }
    return rollupCachePromise;
  };

  const forgetRollup = (key: string): void => {
    const existing = memoryRollups.get(key);
    if (!existing) return;
    memoryRollupBytes -= existing.bytes;
    memoryRollups.delete(key);
  };

  const rememberRollup = (
    key: string,
    rollup: FileRollup,
    variant: string,
    bytes = estimateRollupBytes(rollup),
  ): void => {
    forgetRollup(key);
    if (bytes > maxMemoryRollupBytes) return;
    while (
      memoryRollups.size > 0 &&
      memoryRollupBytes + bytes > maxMemoryRollupBytes
    ) {
      const oldestKey = memoryRollups.keys().next().value;
      if (oldestKey === undefined) break;
      forgetRollup(oldestKey);
    }
    memoryRollups.set(key, { rollup, bytes, variant });
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
  ): Promise<void> => {
    storeQueue = storeQueue.then(async () => {
      let size = 0;
      let room: { usage: number | null; quota: number | null } = {
        usage: null,
        quota: null,
      };
      try {
        const { data, bytes, headers } = await body();
        size = bytes;
        const made = await storeIndex.makeRoom(getFileCache, bytes, forgetFile);
        room = made;
        if (!made.ok) {
          await recordStoreError({
            at: Date.now(),
            cache: cacheName,
            key,
            bytes,
            message:
              "Not enough browser storage: every cached file is in use by the current range.",
            usage: made.usage,
            quota: made.quota,
          });
          return;
        }
        await cache.put(url, new Response(data, { headers }));
        if (tracked) storeIndex.touch(key, bytes);
      } catch (error) {
        await recordStoreError({
          at: Date.now(),
          cache: cacheName,
          key,
          bytes: size,
          message: describeError(error),
          usage: room.usage,
          quota: room.quota,
        });
      }
    });
    return storeQueue;
  };

  const sourceWith = (options?: BrowserSourceOptions): BrowserFileSource => ({
    cacheScope: options?.serviceId
      ? `${cacheScope ?? "browser"}:${options.serviceId}`
      : cacheScope ?? "browser",
    withOptions: (next) => sourceWith(next),
    async openParquet(file, signal, variant) {
      signal?.throwIfAborted();
      const origin = globalThis.location?.origin;
      const reqPath = fileRequestPath(file.key);
      const path = fileDisplayPath(file.key);
      const url = new URL(`${baseUrl}${reqPath}`, origin ?? "http://localhost");
      if (origin && url.origin !== origin)
        throw new Error(
          "Selective Parquet reads require the authenticated same-origin file route",
        );
      const local = await opfsStore(variant).open(fileIdentity(file), signal);
      if (local)
        return {
          kind: "opfs",
          ...local,
          cached: true,
          fallbackUrl: url.href,
          onPersistenceError: async (error) =>
            recordStoreError({
              at: Date.now(),
              cache: OPFS_DIRECTORY,
              key: file.key,
              bytes: file.size,
              message: `OPFS read unavailable; using nonpersistent selective HTTP reads: ${describeError(error)}`,
              ...(await storageEstimate()),
            }),
        };
      const start = performance.now();
      const response = await fetch(url.href, {
        credentials: "same-origin",
        cache: "no-store",
        signal,
        headers: buildRequestHeaders(options, { Range: "bytes=0-0" }),
      });
      try {
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
          page_load_id: options?.getPageLoadId?.(),
        });
        return { kind: "http", url: url.href, cached: false };
      } finally {
        if (!response.bodyUsed) await response.body?.cancel();
      }
    },
    async hasPersistentFile(file, signal, variant) {
      return opfsStore(variant).has(fileIdentity(file), file.size, signal);
    },
    async persistFile(file, signal, variant) {
      const reqPath = fileRequestPath(file.key);
      const path = fileDisplayPath(file.key);
      const origin = globalThis.location?.origin;
      const url = new URL(`${baseUrl}${reqPath}`, origin ?? "http://localhost");
      if (origin && url.origin !== origin) {
        throw new Error(
          "Persistent Parquet reads require the authenticated same-origin file route",
        );
      }
      let response: Response | undefined;
      let started = 0;
      let failure: unknown;
      try {
        await opfsStore(variant).persist(
          fileIdentity(file),
          file.size,
          async () => {
            started = performance.now();
            response = await fetch(url.href, {
              credentials: "same-origin",
              cache: "no-store",
              signal,
              headers: buildRequestHeaders(
                options,
                limits.foldFileTelemetry
                  ? { "X-FLP-Fold-Telemetry": "1" }
                  : undefined,
              ),
            });
            if (file.etag && response.headers.get("etag") !== file.etag) {
              await response.body?.cancel();
              throw new Error(`Immutable file identity changed: ${file.key}`);
            }
            return response;
          },
          signal,
        );
        return { downloaded: response !== undefined };
      } catch (error) {
        failure = error;
        throw error;
      } finally {
        if (response) {
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
        const response = await fetch(baseUrl + url, {
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
          page_load_id: pageLoadId,
        });
        emitDebugSubCalls(response, endpoint, pageLoadId, options?.onApiCall);
        if (!response.ok) {
          let code: string | undefined;
          if (response.status === 409) {
            const body = (await manifestJson(response)) as {
              code?: unknown;
              error?: unknown;
            };
            if (typeof body.code === "string") code = body.code;
            else if (typeof body.error === "string") code = body.error;
          }
          throw new ManifestRequestError(response.status, code);
        }
        return url.split("?", 1)[0] === "/api/files"
          ? manifestJson(response)
          : response.json();
      },
      {
        manifestCacheScope: () =>
          JSON.stringify([
            baseUrl,
            limits.cacheScope ?? dashboardCacheScope(),
            ...(options?.serviceId ? [options.serviceId] : []),
          ]),
      },
    ),
    async fetchFile(key, signal, mode) {
      const transfer = Boolean(mode?.transfer);
      const reqPath = fileRequestPath(key);
      const path = fileDisplayPath(key);
      const pageLoadId = options?.getPageLoadId?.();
      const inMemory = memoryFiles.get(key);
      if (inMemory && memoryFileIdentities.get(key) === mode?.identity) {
        if (transfer) {
          memoryFiles.delete(key);
          memoryFileIdentities.delete(key);
          memoryFileBytes -= inMemory.byteLength;
        } else {
          rememberFile(key, inMemory, mode?.identity);
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
        return { bytes: inMemory, cached: true, cdn: null };
      }
      const url = `${baseUrl}${reqPath}`;
      const cacheUrl = scopedCacheUrl(url, mode?.identity === undefined
        ? cacheScope : JSON.stringify([cacheScope, mode.identity]));
      const cacheStart =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const cache = await getFileCache();
      const hit = await cache?.match(cacheUrl);
      if (hit) {
        const bytes = await hit.arrayBuffer();
        storeIndex.touch(key, bytes.byteLength);
        const cacheEnd =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        if (!transfer) rememberFile(key, bytes, mode?.identity);
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
        return { bytes, cached: true, cdn: null };
      }
      const headers = buildRequestHeaders(
        options,
        limits.foldFileTelemetry ? { "X-FLP-Fold-Telemetry": "1" } : undefined,
      );
      const start =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      // CacheStorage is the only persistent copy; skip the HTTP cache, which
      // would otherwise keep a second one because /api/file is immutable.
      const response = await fetch(url, {
        credentials: "same-origin",
        cache: "no-store",
        ...(headers ? { headers } : {}),
        signal,
      });
      if (!response.ok) {
        const end =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        options?.onApiCall?.({
          source: "Compute API",
          method: "GET",
          path,
          status: response.status,
          duration_ms: Math.max(0, Math.round(end - start)),
          endpoint: "GET /api/file",
          page_load_id: pageLoadId,
        });
        throw new Error(`File ${key} failed (${response.status})`);
      }
      const bytes = await response.arrayBuffer();
      const end =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      if (!transfer) rememberFile(key, bytes, mode?.identity);
      const cdn = response.headers.get("x-flp-cdn-cache");
      const cdnHit = isCdnHit(cdn);
      const vcpuMs = parseVcpuMsHeader(response?.headers);
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
        page_load_id: pageLoadId,
      });
      emitDebugSubCalls(
        response,
        "GET /api/file",
        pageLoadId,
        options?.onApiCall,
      );
      if (cache) {
        const byteLength = bytes.byteLength;
        const cacheBytes = transfer ? bytes.slice(0) : bytes;
        void store(
          CACHE_NAME,
          cache,
          cacheUrl,
          key,
          async () => ({
            data: cacheBytes,
            bytes: byteLength,
            headers: response.headers,
          }),
          true,
        );
      }
      return {
        bytes,
        cached: false,
        cdn,
      };
    },
    async hasFile(key) {
      if (memoryFiles.has(key) && memoryFileIdentities.get(key) === undefined) return true;
      try {
        const cache = await getFileCache();
        const url = `${baseUrl}${fileRequestPath(key)}`;
        return Boolean(await cache?.match(scopedCacheUrl(url, cacheScope)));
      } catch {
        return false;
      }
    },
    async readRollup(key, variant = "") {
      const inMemory = memoryRollups.get(key);
      if (inMemory?.variant === variant) {
        rememberRollup(key, inMemory.rollup, variant, inMemory.bytes);
        return inMemory.rollup;
      }
      try {
        if (cacheScope) {
          const access = await opfsStore(`rollup:${variant}`).open(key);
          if (access) {
            try {
              const file = await access.handle.getFile();
              const decoded = await decodeRollup(
                new Uint8Array(await file.arrayBuffer()),
              );
              if (decoded && decoded.key === key) {
                rememberRollup(key, decoded, variant);
                return decoded;
              }
            } finally {
              access.release();
            }
          }
        }
        const cache = await getRollupCache();
        const hit = await cache?.match(
          scopedCacheUrl(rollupCacheUrl(baseUrl, key, variant), cacheScope),
        );
        if (!hit) return null;
        const decoded = await decodeRollup(
          new Uint8Array(await hit.arrayBuffer()),
        );
        if (!decoded || decoded.key !== key) return null;
        rememberRollup(key, decoded, variant);
        return decoded;
      } catch {
        return null;
      }
    },
    async writeRollup(key, rollup, variant = "") {
      rememberRollup(key, rollup, variant);
      if (cacheScope) {
        const body = await encodeRollup(rollup);
        try {
          await opfsStore(`rollup:${variant}`).persist(
            key,
            body.byteLength,
            async () => new Response(body as BodyInit),
          );
        } catch {
          /* The OPFS store records failures; memory remains usable. */
        }
        return;
      }
      const cache = await getRollupCache().catch(() => null);
      if (!cache) return;
      await store(
        ROLLUP_CACHE_NAME,
        cache,
        scopedCacheUrl(rollupCacheUrl(baseUrl, key, variant), cacheScope),
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
      storeIndex.useManifest([
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
        for (const key of fileCache
          ? await pruneRequestCache(fileCache, manifest, fromMs, toMs)
          : []) {
          forgetFile(key);
          storeIndex.forget(key);
        }
        for (const key of rollupCache
          ? await pruneRequestCache(rollupCache, manifest, fromMs, toMs)
          : []) {
          forgetRollup(key);
        }
      } catch {
        // Pruning is best-effort and must never fail the query.
      } finally {
        pruning = false;
      }
    },
    clearMemoryCache() {
      memoryRollups.clear();
      memoryRollupBytes = 0;
      memoryFiles.clear();
      memoryFileIdentities.clear();
      memoryFileBytes = 0;
      fileCachePromise = null;
      rollupCachePromise = null;
      storeIndex.reset();
    },
  });
  return sourceWith(defaultOptions);
}
