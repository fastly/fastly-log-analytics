import { ALL_TABLE_FIELDS, getDuckDBEngine } from "./dashboard/duckdb";
import type { CustomField } from "./dashboard/summaries";
import { fetchManifest } from "./dashboard/manifest";
import { browserSource, dashboardCacheScope } from "./dashboard/sources";
import { beginCloudLoading } from "./loading-progress";

const MAX_RANGE_FILES = 256;
const MAX_RANGE_BYTES = 64 * 1024 * 1024;
const source = browserSource("");

export async function ensureCloudRange(
  from: string,
  to: string,
  signal: AbortSignal,
  datasets: readonly string[] = ["request"],
  options: { serviceId?: string; customFields?: readonly CustomField[] } = {},
): Promise<void> {
  const loading = beginCloudLoading("listing");
  let success = false;
  try {
    const fetchJson = options.serviceId ? Object.assign(
      (path: string, signal?: AbortSignal) => {
        const url = new URL(path, globalThis.location?.origin ?? "http://localhost");
        url.searchParams.set("service_id", options.serviceId!);
        return source.fetchJson(`${url.pathname}${url.search}`, signal);
      },
      { manifestCacheScope: () => `${dashboardCacheScope()}:${options.serviceId}` },
    ) : source.fetchJson;
    const manifests = await Promise.all(datasets.map((dataset) =>
      fetchManifest(fetchJson, from, to, signal, dataset, "raw"),
    ));
    if (manifests.some((manifest) => !manifest.complete)) {
      throw new Error("Cloud file listing is incomplete; choose a smaller range");
    }
    const files = manifests.flatMap((manifest, index) =>
      manifest.files.map((file) => ({ ...file, dataset: datasets[index] })),
    );
    if (files.length > MAX_RANGE_FILES ||
        files.reduce((total, file) => total + file.size, 0) > MAX_RANGE_BYTES) {
      throw new Error("Cloud query range exceeds the browser scan budget; choose a smaller range");
    }
    const progress = { phase: "files" as const, cached: 0, toFetch: files.length,
      toDecode: 0, done: 0, downloadedBytes: 0, cachedBytes: 0 };
    loading.update({ stage: "files", progress: { ...progress } });
    const engine = getDuckDBEngine();
    await engine.reset();
    const customFields = options.customFields?.filter((field) => !ALL_TABLE_FIELDS.includes(field.id)) ?? [];
    const lease = customFields.length ? engine.lease(customFields) : null;
    const scope = dashboardCacheScope();
    let bytes = 0;
    try { for (const file of files) {
      signal.throwIfAborted();
      const fetched = await source.fetchFile(file.key, signal);
      bytes += fetched.bytes.byteLength;
      if (bytes > MAX_RANGE_BYTES) throw new Error("Cloud query scan exceeded the browser byte budget");
      if (fetched.cached) {
        progress.cached++;
        progress.cachedBytes += fetched.bytes.byteLength;
      } else {
        progress.downloadedBytes += fetched.bytes.byteLength;
      }
      const item = { key: file.key, scopedKey: `${scope}:${file.etag ?? ""}:${file.key}`, bytes: fetched.bytes.slice(0) };
      if (file.dataset === "rum" && file.key.includes("/client_vitals/")) await engine.syncRumFiles([item], [], signal);
      else if (file.dataset === "rum" && file.key.includes("/client_errors/")) await engine.syncRumFiles([], [item], signal);
      else if (file.dataset === "rum") throw new Error("Unrecognized Cloud RUM file kind");
      else if (lease) await lease.ingestFile(item.scopedKey, item.key, item.bytes, undefined, signal);
      else await engine.syncWindow([item], signal);
      progress.done++;
      loading.update({ progress: { ...progress } });
    } } finally { lease?.release(); }
    success = true;
  } finally {
    loading.finish(success);
  }
}
