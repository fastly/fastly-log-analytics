import { dashboardCacheIdentity } from "./dashboard/sources";
import { fetchRetrying } from "./fetch-retry";

// Every Cloud scan needs the extents, catalog and services lookups, and a page
// load runs many scans. Share one answer while it keeps being used within a short
// window, so a long page load reads it once, but never past a hard cap; writes clear it.
const METADATA_TTL_MS = 15_000;
const METADATA_MAX_AGE_MS = 60_000;
type MetadataSnapshot = { status: number; contentType: string | null; body: string };
type MetadataEntry = { at: number; usedAt: number; identity: string | null; value: Promise<MetadataSnapshot> };
const metadataCache = new Map<string, MetadataEntry>();

export function resetCloudMetadataCache(): void {
  metadataCache.clear();
}

export async function fetchCloudMetadata(path: string, signal: AbortSignal): Promise<Response> {
  signal.throwIfAborted();
  const now = Date.now();
  // Another tab signing in as someone else changes the shared identity; never hand them this one's answer.
  const identity = dashboardCacheIdentity();
  let entry = metadataCache.get(path);
  if (entry && entry.identity === identity && now - entry.usedAt <= METADATA_TTL_MS &&
      now - entry.at < METADATA_MAX_AGE_MS) {
    entry.usedAt = now;
  } else {
    const value = fetchRetrying(path).then(async (response) => ({
      status: response.status, contentType: response.headers.get("content-type"), body: await response.text(),
    }));
    const created = { at: now, usedAt: now, identity, value };
    entry = created;
    metadataCache.set(path, created);
    const evict = () => { if (metadataCache.get(path) === created) metadataCache.delete(path); };
    value.then((snapshot) => { if (snapshot.status < 200 || snapshot.status > 299) evict(); }, evict);
  }
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    const snapshot = await Promise.race([entry.value, aborted]);
    return new Response(snapshot.body, {
      status: snapshot.status,
      headers: snapshot.contentType ? { "content-type": snapshot.contentType } : undefined,
    });
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
