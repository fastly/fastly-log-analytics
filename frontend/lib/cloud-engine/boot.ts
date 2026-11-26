import { CLOUD_RAW_SCAN_PREFIXES, CLOUD_ROLLUP_WORKER_PREFIXES, matchesCloudRoute } from "./boot-routes";
import { prewarmRollupWorker } from "./dashboard/client";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { fetchCloudMetadata } from "./metadata-cache";

/**
 * Starts what every analytical page waits on before it can list files, at boot
 * rather than after the page's first query: the shared range lookup, the
 * rollup worker where summaries answer, and, where the page will scan raw
 * Parquet, the DuckDB-WASM instance. Call after the session's cache identity is
 * set, so the shared answers belong to it.
 */
export function bootCloudEngine(pathname: string): void {
  fetchCloudMetadata("/api/log-extents", new AbortController().signal).catch(() => {});
  if (matchesCloudRoute(pathname, CLOUD_ROLLUP_WORKER_PREFIXES)) prewarmRollupWorker();
  if (matchesCloudRoute(pathname, CLOUD_RAW_SCAN_PREFIXES)) getDuckDBEngine().prewarm().catch(() => {});
}
