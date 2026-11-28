import {
  accountManifestPage,
  MAX_IDENTITIES,
  MAX_MANIFEST_BYTES,
  type ManifestBudget,
} from "./catalog-manifest";
import {
  currentStoreMetaEpoch,
  describeError,
  recordStoreError,
  storageEstimate,
  type StoreError,
} from "./file-store";
import type { Manifest, ManifestFile } from "./manifest";

export const MANIFEST_SNAPSHOT_CACHE = "flp-manifest-v1";
export const MANIFEST_SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const FORMAT_VERSION = 1;

export type ManifestSnapshot = {
  from: string;
  to: string;
  generation: number;
  manifest: Manifest;
};

export type PersistedManifestSnapshot = {
  snapshot: ManifestSnapshot;
  savedAt: number;
  windows?: ManifestSnapshot[];
};

type PersistedManifest = {
  formatVersion: number;
  cacheKey: string;
  savedAt: number;
  snapshot: ManifestSnapshot;
  windows?: ManifestSnapshot[];
};

export function manifestSnapshotUrl(cacheKey: string): string {
  return `/__flp_manifest_snapshot?v=${FORMAT_VERSION}&scope=${encodeURIComponent(cacheKey)}`;
}

function validIso(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

/** Whether `value` is a persistable key, and for which summaries setting. */
function cacheKeySummaries(value: unknown): boolean | null {
  if (typeof value !== "string") return null;
  let key: unknown;
  try {
    key = JSON.parse(value);
  } catch {
    return null;
  }
  return Array.isArray(key) &&
    key.length === 4 &&
    typeof key[0] === "string" &&
    key[0].length > 0 &&
    key[1] === "request" &&
    key[2] === "raw" &&
    typeof key[3] === "boolean"
    ? key[3]
    : null;
}

function validFile(
  value: unknown,
  from: string,
  to: string,
  summary = false,
): value is ManifestFile {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const file = value as Partial<ManifestFile>;
  return (
    typeof file.key === "string" &&
    file.key.length > 0 &&
    validIso(file.start) &&
    validIso(file.end) &&
    Date.parse(file.start) < Date.parse(file.end) &&
    Date.parse(file.end) > Date.parse(from) &&
    Date.parse(file.start) < Date.parse(to) &&
    typeof file.size === "number" &&
    Number.isFinite(file.size) &&
    file.size >= 0 &&
    typeof file.rows === "number" &&
    Number.isFinite(file.rows) &&
    file.rows >= 0 &&
    (file.etag === undefined || typeof file.etag === "string") &&
    (file.schema_version === undefined ||
      typeof file.schema_version === "string" ||
      (typeof file.schema_version === "number" &&
        Number.isFinite(file.schema_version))) &&
    (file.kind === undefined ||
      (summary &&
        (file.kind === "day" ||
          file.kind === "hour" ||
          file.kind === "window"))) &&
    (file.sources === undefined ||
      (typeof file.sources === "number" &&
        Number.isSafeInteger(file.sources) &&
        file.sources >= 0))
  );
}

function validSnapshotPayload(
  snapshot: unknown,
  summaries: boolean,
): snapshot is ManifestSnapshot {
  const s = snapshot as Partial<ManifestSnapshot> | undefined;
  const manifest = s?.manifest as Partial<Manifest> | undefined;
  const rangeFrom = s?.from;
  const rangeTo = s?.to;
  const generation = s?.generation;
  if (
    !s ||
    typeof s !== "object" ||
    !validIso(rangeFrom) ||
    !validIso(rangeTo) ||
    Date.parse(rangeFrom) >= Date.parse(rangeTo) ||
    typeof generation !== "number" ||
    !Number.isSafeInteger(generation) ||
    generation < 0 ||
    !manifest ||
    typeof manifest !== "object" ||
    Array.isArray(manifest) ||
    manifest.complete !== true ||
    manifest.catalogGeneration !== generation ||
    !Array.isArray(manifest.files) ||
    (summaries
      ? !Array.isArray(manifest.summaryFiles) ||
        manifest.summaryRange?.from !== rangeFrom ||
        manifest.summaryRange?.to !== rangeTo
      : manifest.summaryFiles !== undefined ||
        manifest.summaryRange !== undefined)
  )
    return false;

  const files = manifest.files;
  const summaryFiles = manifest.summaryFiles ?? [];
  return (
    files.every((file) => validFile(file, rangeFrom, rangeTo)) &&
    summaryFiles.every((file) => validFile(file, rangeFrom, rangeTo, true))
  );
}

function parseSnapshotRecord(
  body: unknown,
  cacheKey: string,
  now: number,
): PersistedManifestSnapshot | null {
  const summaries = cacheKeySummaries(cacheKey);
  if (
    summaries === null ||
    !Number.isFinite(now) ||
    !body ||
    typeof body !== "object" ||
    Array.isArray(body)
  )
    return null;
  const record = body as Partial<PersistedManifest>;
  if (
    record.formatVersion !== FORMAT_VERSION ||
    record.cacheKey !== cacheKey ||
    typeof record.savedAt !== "number" ||
    !Number.isFinite(record.savedAt) ||
    record.savedAt > now ||
    now - record.savedAt >= MANIFEST_SNAPSHOT_MAX_AGE_MS
  )
    return null;
  if (!validSnapshotPayload(record.snapshot, summaries)) return null;
  const extra = record.windows;
  if (
    extra !== undefined &&
    (!Array.isArray(extra) || !extra.every((w) => validSnapshotPayload(w, summaries)))
  )
    return null;

  const budget: ManifestBudget = { bytes: 0, identities: new Set() };
  try {
    for (const w of [record.snapshot, ...(extra ?? [])]) {
      accountManifestPage(
        { files: w.manifest.files, summary_files: w.manifest.summaryFiles ?? [] },
        budget,
      );
    }
  } catch {
    return null;
  }
  return budget.bytes <= MAX_MANIFEST_BYTES &&
    budget.identities.size <= MAX_IDENTITIES
    ? {
        snapshot: record.snapshot,
        savedAt: record.savedAt,
        ...(extra?.length ? { windows: extra } : {}),
      }
    : null;
}

function parseSnapshot(
  body: unknown,
  cacheKey: string,
  now: number,
): ManifestSnapshot | null {
  return parseSnapshotRecord(body, cacheKey, now)?.snapshot ?? null;
}

async function boundedBody(
  response: Response,
  limit: number,
): Promise<ArrayBuffer | null> {
  const reader = response.body?.getReader();
  if (!reader) {
    const raw = await response.arrayBuffer();
    return raw.byteLength <= limit ? raw : null;
  }
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > limit) {
      void reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  const raw = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    raw.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return raw.buffer;
}

async function report(
  action: string,
  cacheKey: string,
  bytes: number,
  error: unknown,
  callMetaEpoch = currentStoreMetaEpoch(),
): Promise<void> {
  if (callMetaEpoch !== currentStoreMetaEpoch()) return;
  const estimate = await storageEstimate();
  if (callMetaEpoch !== currentStoreMetaEpoch()) return;
  const detail = {
    at: Date.now(),
    cache: MANIFEST_SNAPSHOT_CACHE,
    key: `manifest:${action}:${cacheKey}`,
    bytes,
    message: describeError(error),
    usage: estimate.usage,
    quota: estimate.quota,
  } satisfies StoreError;
  try {
    await recordStoreError(detail, callMetaEpoch);
  } catch (telemetryError) {
    console.warn(
      "flp: browser cache error telemetry failed",
      describeError(telemetryError),
    );
  }
}

export async function readManifestSnapshotWithAge(
  cacheKey: string,
  now = Date.now(),
): Promise<PersistedManifestSnapshot | null> {
  const callMetaEpoch = currentStoreMetaEpoch();
  let bytes = 0;
  try {
    if (typeof caches === "undefined" || callMetaEpoch !== currentStoreMetaEpoch()) {
      return null;
    }
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    if (callMetaEpoch !== currentStoreMetaEpoch()) return null;
    const url = manifestSnapshotUrl(cacheKey);
    const response = await cache.match(url);
    if (!response) return null;
    if (callMetaEpoch !== currentStoreMetaEpoch()) {
      await response.body?.cancel();
      return null;
    }
    const raw = await boundedBody(response, MAX_MANIFEST_BYTES);
    if (callMetaEpoch !== currentStoreMetaEpoch()) return null;
    if (!raw) {
      await cache.delete(url);
      return null;
    }
    bytes = raw.byteLength;
    let body: unknown;
    try {
      body = JSON.parse(new TextDecoder().decode(raw));
    } catch {
      if (callMetaEpoch === currentStoreMetaEpoch()) await cache.delete(url);
      return null;
    }
    const record = parseSnapshotRecord(body, cacheKey, now);
    if (!record && callMetaEpoch === currentStoreMetaEpoch()) {
      await cache.delete(url);
    }
    return callMetaEpoch === currentStoreMetaEpoch() ? record : null;
  } catch (error) {
    await report("read", cacheKey, bytes, error, callMetaEpoch);
    return null;
  }
}

export async function readManifestSnapshot(
  cacheKey: string,
  now = Date.now(),
): Promise<ManifestSnapshot | null> {
  return (await readManifestSnapshotWithAge(cacheKey, now))?.snapshot ?? null;
}

export async function persistManifestSnapshot(
  cacheKey: string,
  snapshot: ManifestSnapshot,
  windows?: ManifestSnapshot[],
): Promise<void> {
  const callMetaEpoch = currentStoreMetaEpoch();
  let bytes = 0;
  try {
    if (typeof caches === "undefined" || callMetaEpoch !== currentStoreMetaEpoch()) {
      return;
    }
    const now = Date.now();
    const extra = windows?.filter((w) => !(w.from === snapshot.from && w.to === snapshot.to));
    const record: PersistedManifest = {
      formatVersion: FORMAT_VERSION,
      cacheKey,
      savedAt: now,
      snapshot,
      ...(extra?.length ? { windows: extra } : {}),
    };
    if (!parseSnapshot(record, cacheKey, now))
      throw new Error("Refusing to persist an invalid file manifest snapshot");
    const response = new Response(JSON.stringify(record));
    bytes = (await response.clone().arrayBuffer()).byteLength;
    if (bytes > MAX_MANIFEST_BYTES)
      throw new Error("File manifest exceeded the metadata byte limit");
    if (callMetaEpoch !== currentStoreMetaEpoch()) return;
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    if (callMetaEpoch !== currentStoreMetaEpoch()) return;
    await cache.put(manifestSnapshotUrl(cacheKey), response);
  } catch (error) {
    await report("write", cacheKey, bytes, error, callMetaEpoch);
  }
}

export async function deleteManifestSnapshot(
  cacheKey: string,
): Promise<boolean> {
  const callMetaEpoch = currentStoreMetaEpoch();
  try {
    if (typeof caches === "undefined") return true;
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    await cache.delete(manifestSnapshotUrl(cacheKey));
    return true;
  } catch (error) {
    await report("delete", cacheKey, 0, error, callMetaEpoch);
    return false;
  }
}

export async function deleteManifestSnapshots(): Promise<boolean> {
  const callMetaEpoch = currentStoreMetaEpoch();
  try {
    if (typeof caches === "undefined") return true;
    await caches.delete(MANIFEST_SNAPSHOT_CACHE);
    return true;
  } catch (error) {
    await report("clear", "*", 0, error, callMetaEpoch);
    return false;
  }
}
