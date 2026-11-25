import { createSharedRequests } from "../shared-request";
import type { IngestLifecycleFold } from "./ingest-lifecycle";

export class OpsRangeUnsupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OpsRangeUnsupportedError";
  }
}

const MAX_DECODED_PARQUET_ENTRIES = 512;
const decodedParquetCache = new Map<string, Record<string, unknown>[]>();
const sharedDecodedParquet = createSharedRequests<Record<string, unknown>[]>();
const ingestFoldCache = new Map<string, IngestLifecycleFold>();
const sharedIngestFolds = createSharedRequests<IngestLifecycleFold>();
let decodedCacheGeneration = 0;

export function clearOpsDecodedCache(): void {
  decodedCacheGeneration += 1;
  decodedParquetCache.clear();
  ingestFoldCache.clear();
}

function decodedCacheKey(scope: string, key: string, columnsKey: string): string {
  return `${scope}\0${key}\0${columnsKey}`;
}

export function getDecodedParquet(
  scope: string,
  key: string,
  columnsKey: string,
): Record<string, unknown>[] | undefined {
  const exact = decodedCacheKey(scope, key, columnsKey);
  let hit = decodedParquetCache.get(exact);
  let hitKey = exact;
  if (hit === undefined && columnsKey !== "*") {
    const full = decodedCacheKey(scope, key, "*");
    hit = decodedParquetCache.get(full);
    hitKey = full;
  }
  if (hit !== undefined) {
    decodedParquetCache.delete(hitKey);
    decodedParquetCache.set(hitKey, hit);
  }
  return hit;
}

export function setDecodedParquet(
  scope: string,
  key: string,
  columnsKey: string,
  rows: Record<string, unknown>[],
): void {
  const full = decodedCacheKey(scope, key, columnsKey);
  decodedParquetCache.delete(full);
  decodedParquetCache.set(full, rows);
  if (decodedParquetCache.size > MAX_DECODED_PARQUET_ENTRIES) {
    const oldest = decodedParquetCache.keys().next().value;
    if (oldest !== undefined) decodedParquetCache.delete(oldest);
  }
}

export function deleteDecodedParquet(scope: string, key: string): void {
  decodedCacheGeneration += 1;
  const prefix = `${scope}\0${key}\0`;
  for (const cacheKey of [...decodedParquetCache.keys()]) {
    if (cacheKey.startsWith(prefix)) decodedParquetCache.delete(cacheKey);
  }
  ingestFoldCache.delete(`${scope}\0${key}`);
}

export async function loadIngestFileFold(
  scope: string,
  key: string,
  loader: (signal: AbortSignal) => Promise<IngestLifecycleFold>,
  signal?: AbortSignal,
): Promise<{ fold: IngestLifecycleFold; coalesced: boolean }> {
  signal?.throwIfAborted();
  const foldKey = `${scope}\0${key}`;
  const cached = ingestFoldCache.get(foldKey);
  if (cached !== undefined) {
    ingestFoldCache.delete(foldKey);
    ingestFoldCache.set(foldKey, cached);
    return { fold: cached, coalesced: true };
  }
  const gen = decodedCacheGeneration;
  const inFlightKey = `${gen}\0${foldKey}`;
  let started = false;
  const fold = await sharedIngestFolds(
    inFlightKey,
    async (sharedSignal) => {
      started = true;
      const computed = await loader(sharedSignal);
      if (decodedCacheGeneration === gen) {
        ingestFoldCache.delete(foldKey);
        ingestFoldCache.set(foldKey, computed);
        if (ingestFoldCache.size > MAX_DECODED_PARQUET_ENTRIES) {
          const oldest = ingestFoldCache.keys().next().value;
          if (oldest !== undefined) ingestFoldCache.delete(oldest);
        }
      }
      return computed;
    },
    { signal },
  );
  return { fold, coalesced: !started };
}

/**
 * Returns decoded Parquet rows from cache or coalesces concurrent in-flight loads for the
 * same `(scope, key, columnsKey)` so overlapping ops reads share one fetch and decode.
 */
export async function loadDecodedParquet(
  scope: string,
  key: string,
  columnsKey: string,
  loader: (signal: AbortSignal) => Promise<Record<string, unknown>[]>,
  signal?: AbortSignal,
): Promise<{ rows: Record<string, unknown>[]; coalesced: boolean }> {
  signal?.throwIfAborted();
  const cached = getDecodedParquet(scope, key, columnsKey);
  if (cached !== undefined) {
    return { rows: cached, coalesced: true };
  }
  const gen = decodedCacheGeneration;
  const inFlightKey = `${gen}\0${decodedCacheKey(scope, key, columnsKey)}`;
  let started = false;
  const rows = await sharedDecodedParquet(
    inFlightKey,
    async (sharedSignal) => {
      started = true;
      const decoded = await loader(sharedSignal);
      if (decodedCacheGeneration === gen) {
        setDecodedParquet(scope, key, columnsKey, decoded);
      }
      return decoded;
    },
    { signal },
  );
  return { rows, coalesced: !started };
}
