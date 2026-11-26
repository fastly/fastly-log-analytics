import { dashboardCacheIdentity } from "./dashboard/sources";
import { fetchRetrying } from "./fetch-retry";

// Every Cloud scan needs the extents, catalog and services lookups, and a page
// load runs many scans. Share one answer while it keeps being used within a short
// window, so a long page load reads it once, but never past a hard cap; writes clear it.
const METADATA_TTL_MS = 15_000;
const METADATA_MAX_AGE_MS = 60_000;
type MetadataSnapshot = { status: number; contentType: string | null; body: string };
type MetadataEntry = {
  at: number;
  usedAt: number;
  identity: string | null;
  epoch: number;
  controller: AbortController;
  value: Promise<MetadataSnapshot>;
};
let metadataEpoch = 0;
let metadataAbortEpoch = 0;
const activeMetadataControllers = new Set<AbortController>();
const metadataCache = new Map<string, MetadataEntry>();
const metadataInvalidationListeners = new Set<() => void>();

function metadataAbortError(): DOMException {
  return new DOMException("The operation was aborted.", "AbortError");
}

export function resetCloudMetadataCache(
  options: { abortActive?: boolean } = {},
): void {
  const abortActive = options.abortActive ?? true;
  metadataEpoch += 1;
  if (abortActive) {
    metadataAbortEpoch += 1;
    for (const controller of activeMetadataControllers) {
      controller.abort(metadataAbortError());
    }
    activeMetadataControllers.clear();
  }
  metadataCache.clear();
}

export function invalidateCloudMetadataCache(): void {
  resetCloudMetadataCache({ abortActive: false });
}

export function resetCloudMetadataPaths(paths: readonly string[]): void {
  for (const path of paths) metadataCache.delete(path);
}

export function subscribeCloudMetadataInvalidation(listener: () => void): () => void {
  metadataInvalidationListeners.add(listener);
  return () => metadataInvalidationListeners.delete(listener);
}

export function invalidateCloudConfigMetadata(): void {
  resetCloudMetadataPaths(["/api/services", "/api/bootstrap", "/api/log-fields/catalog"]);
  for (const listener of metadataInvalidationListeners) listener();
}

export async function fetchCloudMetadata(path: string, signal: AbortSignal): Promise<Response> {
  signal.throwIfAborted();
  const callCacheEpoch = metadataEpoch;
  const callAbortEpoch = metadataAbortEpoch;
  const now = Date.now();
  // Another tab signing in as someone else changes the shared identity; never hand them this one's answer.
  const identity = dashboardCacheIdentity();
  let entry = metadataCache.get(path);
  const identityMatches =
    entry !== undefined &&
    (entry.identity === identity || (entry.identity === null && identity !== null));
  if (
    entry &&
    entry.epoch === callCacheEpoch &&
    !entry.controller.signal.aborted &&
    identityMatches &&
    now - entry.usedAt <= METADATA_TTL_MS &&
    now - entry.at < METADATA_MAX_AGE_MS
  ) {
    if (entry.identity === null && identity !== null) {
      entry.identity = identity;
    }
    entry.usedAt = now;
  } else {
    const controller = new AbortController();
    activeMetadataControllers.add(controller);
    const created: MetadataEntry = {
      at: now,
      usedAt: now,
      identity,
      epoch: callCacheEpoch,
      controller,
      value: undefined as unknown as Promise<MetadataSnapshot>,
    };
    const evict = () => {
      if (metadataCache.get(path) === created) metadataCache.delete(path);
    };
    const value = fetchRetrying(path, { signal: controller.signal })
      .then(async (response) => ({
        status: response.status,
        contentType: response.headers.get("content-type"),
        body: await response.text(),
      }))
      .finally(() => {
        activeMetadataControllers.delete(controller);
      });
    created.value = value;
    if (callCacheEpoch === metadataEpoch && !controller.signal.aborted) {
      metadataCache.set(path, created);
    }
    value.then(
      (snapshot) => {
        if (callCacheEpoch !== metadataEpoch || snapshot.status < 200 || snapshot.status > 299) {
          evict();
        }
      },
      evict,
    );
    entry = created;
  }
  const activeEntry = entry;
  let onCallerAbort!: () => void;
  let onSharedAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onCallerAbort = () => reject(signal.reason ?? metadataAbortError());
    onSharedAbort = () =>
      reject(activeEntry.controller.signal.reason ?? metadataAbortError());
    if (signal.aborted) {
      onCallerAbort();
      return;
    }
    if (activeEntry.controller.signal.aborted) {
      onSharedAbort();
      return;
    }
    signal.addEventListener("abort", onCallerAbort, { once: true });
    activeEntry.controller.signal.addEventListener("abort", onSharedAbort, {
      once: true,
    });
  });
  try {
    const snapshot = await Promise.race([activeEntry.value, aborted]);
    signal.throwIfAborted();
    if (callAbortEpoch !== metadataAbortEpoch || activeEntry.controller.signal.aborted) {
      throw metadataAbortError();
    }
    return new Response(snapshot.body, {
      status: snapshot.status,
      headers: snapshot.contentType ? { "content-type": snapshot.contentType } : undefined,
    });
  } finally {
    signal.removeEventListener("abort", onCallerAbort);
    activeEntry.controller.signal.removeEventListener("abort", onSharedAbort);
  }
}
