export class OpsRangeUnsupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OpsRangeUnsupportedError";
  }
}

const MAX_DECODED_PARQUET_ENTRIES = 512;
const decodedParquetCache = new Map<string, Record<string, unknown>[]>();

export function clearOpsDecodedCache(): void {
  decodedParquetCache.clear();
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
  const prefix = `${scope}\0${key}\0`;
  for (const cacheKey of [...decodedParquetCache.keys()]) {
    if (cacheKey.startsWith(prefix)) decodedParquetCache.delete(cacheKey);
  }
}
