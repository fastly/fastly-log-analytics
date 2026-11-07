import { ALL_TABLE_FIELDS, getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import type { CustomField } from "./dashboard/summaries";
import { fetchManifest } from "./dashboard/manifest";
import { browserSource, dashboardCacheScope } from "./dashboard/sources";
import { beginCloudLoading } from "./loading-progress";

const MAX_RANGE_FILES = 256;
const MAX_RANGE_BYTES = 64 * 1024 * 1024;
const source = browserSource("");
const requestReads = new WeakMap<Request, DuckDBReadContext>();

/** A range the browser won't scan: retrying the same request fails the same way. */
export class CloudBudgetError extends Error {}

export function bindCloudReadContext(request: Request, context: DuckDBReadContext): Request {
  requestReads.set(request, context);
  return request;
}

export function cloudReadContext(request: Request): DuckDBReadContext {
  const context = requestReads.get(request) ?? getDuckDBEngine().readContext();
  context.assertCurrent();
  return context;
}

export async function ensureCloudRange(
  from: string,
  to: string,
  signal: AbortSignal,
  datasets: readonly string[] = ["request"],
  options: { serviceId?: string; customFields?: readonly CustomField[] } = {},
): Promise<DuckDBReadContext> {
  const engine = getDuckDBEngine();
  const generation = await engine.beginRequest();
  generation.assertCurrent();
  signal.throwIfAborted();
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
    // Native discovery has second precision; SQL retains the original bounds.
    const listFrom = new Date(Math.floor(Date.parse(from) / 1000) * 1000).toISOString();
    const listTo = new Date(Math.ceil(Date.parse(to) / 1000) * 1000).toISOString();
    const manifests = await Promise.all(datasets.map((dataset) =>
      fetchManifest(fetchJson, listFrom, listTo, signal, dataset, "raw"),
    ));
    generation.assertCurrent();
    signal.throwIfAborted();
    if (manifests.some((manifest) => !manifest.complete)) {
      throw new Error("Cloud file listing is incomplete; choose a smaller range");
    }
    const scope = dashboardCacheScope();
    const files = manifests.flatMap((manifest, index) =>
      manifest.files.map((file) => ({ ...file, dataset: datasets[index],
        scopedKey: JSON.stringify([scope, options.serviceId ?? "", datasets[index],
          file.key, file.etag ?? "", file.size, file.schema_version ?? ""]),
      })),
    );
    if (files.length > MAX_RANGE_FILES ||
        files.reduce((total, file) => total + file.size, 0) > MAX_RANGE_BYTES) {
      throw new CloudBudgetError("Cloud query range exceeds the browser scan budget; choose a smaller range");
    }
    const progress = { phase: "files" as const, cached: 0, toFetch: files.length,
      toDecode: 0, done: 0, downloadedBytes: 0, cachedBytes: 0 };
    loading.update({ stage: "files", progress: { ...progress } });
    const kind = (file: typeof files[number]) => file.dataset !== "rum" ? "request"
      : file.key.includes("/client_vitals/") ? "vitals"
        : file.key.includes("/client_errors/") ? "errors" : null;
    if (files.some((file) => kind(file) === null)) throw new Error("Unrecognized Cloud RUM file kind");
    await engine.retainFiles({
      request: files.filter((file) => kind(file) === "request").map((file) => file.scopedKey),
      vitals: files.filter((file) => kind(file) === "vitals").map((file) => file.scopedKey),
      errors: files.filter((file) => kind(file) === "errors").map((file) => file.scopedKey),
    }, signal);
    generation.assertCurrent();
    const customFields = options.customFields?.filter((field) => !ALL_TABLE_FIELDS.includes(field.id)) ?? [];
    const lease = engine.lease(customFields);
    const residentBytesByKey = new Map<string, number>();
    for (const file of files) {
      const fileKind = kind(file);
      if (!fileKind) throw new Error("Unrecognized Cloud RUM file kind");
      const meta = lease.getFileMeta(file.scopedKey, fileKind);
      if (meta) residentBytesByKey.set(file.scopedKey, meta.byteLength);
    }
    let bytes = 0;
    let handedOff = false;
    try {
      for (const file of files) {
        signal.throwIfAborted();
        const fileKind = kind(file);
        if (!fileKind) throw new Error("Unrecognized Cloud RUM file kind");
        const residentBytes = residentBytesByKey.get(file.scopedKey);
        if (residentBytes !== undefined) {
          bytes += residentBytes;
          if (bytes > MAX_RANGE_BYTES) throw new CloudBudgetError("Cloud query scan exceeded the browser byte budget");
          progress.cached++;
          progress.cachedBytes += residentBytes;
          progress.done++;
          loading.update({ progress: { ...progress } });
          continue;
        }
        const fetched = await source.fetchFile(file.key, signal, { identity: file.scopedKey });
        generation.assertCurrent();
        signal.throwIfAborted();
        bytes += fetched.bytes.byteLength;
        if (bytes > MAX_RANGE_BYTES) throw new CloudBudgetError("Cloud query scan exceeded the browser byte budget");
        if (fetched.cached) {
          progress.cached++;
          progress.cachedBytes += fetched.bytes.byteLength;
        } else {
          progress.downloadedBytes += fetched.bytes.byteLength;
        }
        const item = { key: file.key, scopedKey: file.scopedKey, bytes: fetched.bytes };
        await lease.ingestFile(item.scopedKey, item.key, item.bytes, undefined, signal, fileKind);
        generation.assertCurrent();
        signal.throwIfAborted();
        progress.done++;
        loading.update({ progress: { ...progress } });
      }
      generation.assertCurrent();
      const read = lease.readContext();
      handedOff = true;
      success = true;
      return read;
    } finally {
      if (!handedOff) lease.release();
    }
  } finally {
    loading.finish(success);
  }
}
