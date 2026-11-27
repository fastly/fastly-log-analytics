export const DUCKDB_HARDENED_WORKER_FILE: string;
export const DUCKDB_LEGACY_WORKER_SHIM: string;

export interface DuckdbWorkerScopeLike {
  __flpDuckdbWorkerHardened?: boolean;
  location?: { origin?: string; href?: string };
  fetch?: typeof fetch;
  addEventListener?: (
    type: string,
    listener: (event: { reason?: unknown; preventDefault?: () => void }) => void,
  ) => void;
}

export function installDuckdbWorkerHardening(scope?: DuckdbWorkerScopeLike): void;
export function renderHardenedDuckdbWorker(workerSource: string): string;

export interface SyncDuckdbAssetsOptions {
  srcDir?: string;
  vendorDir?: string;
  publicDir?: string;
  version?: string;
}

export interface SyncDuckdbAssetsResult {
  copied: boolean;
  destDir: string;
  version: string;
}

export function syncDuckdbAssets(options?: SyncDuckdbAssetsOptions): SyncDuckdbAssetsResult;
