import { ALL_TABLE_FIELDS, getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import type { CustomField } from "./dashboard/summaries";
import { fetchManifest } from "./dashboard/manifest";
import { browserSource, dashboardCacheScope } from "./dashboard/sources";
import {
  assembleProjectedParquet, fetchProjectedRanges, planParquetProjection, readParquetFooter, type ParquetProjection,
} from "./dashboard/parquet-projection";
import { beginCloudLoading } from "./loading-progress";

const MAX_RANGE_FILES = 256;
const MAX_RANGE_BYTES = 64 * 1024 * 1024;
const PREFETCH_CONCURRENCY = 6;
const RANGE_CONCURRENCY = 6;
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
  options: {
    serviceId?: string;
    customFields?: readonly CustomField[];
    /**
     * Loads only these request columns, read as byte ranges, and budgets the
     * range on their footer-measured bytes (at most `maxBytes`, default the
     * range byte budget) instead of whole-file sizes.
     */
    projection?: { columns: readonly string[]; maxBytes?: number };
  } = {},
): Promise<DuckDBReadContext> {
  const projection = options.projection;
  if (projection && datasets.some((dataset) => dataset !== "request"))
    throw new Error("Cloud column projection applies to request files only");
  const projectedColumns = projection ? new Set(projection.columns) : null;
  const byteLimit = Math.min(projection?.maxBytes ?? MAX_RANGE_BYTES, MAX_RANGE_BYTES);
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
          file.key, file.etag ?? "", file.size, file.schema_version ?? "",
          ...projectedColumns ? ["projection", ...[...projectedColumns].sort()] : []]),
      })),
    );
    if (files.length > MAX_RANGE_FILES || !projection &&
        files.reduce((total, file) => total + file.size, 0) > MAX_RANGE_BYTES) {
      throw new CloudBudgetError("Cloud query range exceeds the browser scan budget; choose a smaller range");
    }
    const downloads = new AbortController();
    const abortDownloads = () => downloads.abort(signal.reason);
    signal.addEventListener("abort", abortDownloads, { once: true });
    if (signal.aborted) abortDownloads();
    let slots = RANGE_CONCURRENCY;
    const waiting: (() => void)[] = [];
    const fetchRange = async (file: typeof files[number], start: number, end: number) => {
      if (slots === 0) await new Promise<void>((resolve) => waiting.push(resolve));
      else slots--;
      try {
        downloads.signal.throwIfAborted();
        return await source.fetchRange(file, start, end, downloads.signal);
      } finally {
        const next = waiting.shift();
        if (next) next();
        else slots++;
      }
    };
    try {
      const plans = new Map<string, ParquetProjection>();
      if (projectedColumns) {
        loading.update({ stage: "files" });
        const unknown = files.filter((file) => !engine.getFileMeta(file.scopedKey));
        if (unknown.some((file) => !(file.size > 0))) throw new Error("Cloud file listing has no file size to project");
        await Promise.all(unknown.map(async (file) => {
          const footer = await readParquetFooter(file.size, (start, end) => fetchRange(file, start, end));
          plans.set(file.scopedKey, planParquetProjection(footer, projectedColumns));
        }));
        generation.assertCurrent();
        signal.throwIfAborted();
        const needed = files.reduce((total, file) => total +
          (plans.get(file.scopedKey)?.projectedBytes ?? engine.getFileMeta(file.scopedKey)?.byteLength ?? 0), 0);
        if (needed > byteLimit) {
          throw new CloudBudgetError("Cloud query range exceeds the browser scan budget; choose a smaller range");
        }
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
      const pending = files.filter((file) => !residentBytesByKey.has(file.scopedKey));
      const fetchProjected = async (file: typeof files[number]) => {
        let plan = plans.get(file.scopedKey);
        if (!plan) {
          // Resident at the budget check but evicted since.
          plan = planParquetProjection(
            await readParquetFooter(file.size, (start, end) => fetchRange(file, start, end)), projectedColumns!);
        }
        let downloaded = 0;
        const parts = await fetchProjectedRanges(plan, async (start, end) => {
          const part = await fetchRange(file, start, end);
          downloaded += part.byteLength;
          return part;
        });
        return { bytes: assembleProjectedParquet(plan, parts), cached: false, downloaded, presentColumns: plan.presentColumns };
      };
      const fetchOne = (file: typeof files[number]): Promise<{
        bytes: ArrayBuffer; cached: boolean; downloaded?: number; presentColumns?: string[];
      }> => projectedColumns ? fetchProjected(file)
        : source.fetchFile(file.key, downloads.signal, { identity: file.scopedKey });
      const fetches = new Map<string, ReturnType<typeof fetchOne>>();
      let nextFetch = 0;
      const prefetch = () => {
        while (nextFetch < pending.length && fetches.size < PREFETCH_CONCURRENCY && !downloads.signal.aborted) {
          const file = pending[nextFetch++];
          const fetched = fetchOne(file);
          fetched.catch(() => {});
          fetches.set(file.scopedKey, fetched);
        }
      };
      try {
        prefetch();
        for (const file of files) {
          signal.throwIfAborted();
          const fileKind = kind(file);
          if (!fileKind) throw new Error("Unrecognized Cloud RUM file kind");
          const residentBytes = residentBytesByKey.get(file.scopedKey);
          if (residentBytes !== undefined) {
            bytes += residentBytes;
            if (bytes > byteLimit) throw new CloudBudgetError("Cloud query scan exceeded the browser byte budget");
            progress.cached++;
            progress.cachedBytes += residentBytes;
            progress.done++;
            loading.update({ progress: { ...progress } });
            continue;
          }
          const fetched = await (fetches.get(file.scopedKey) ?? fetchOne(file));
          fetches.delete(file.scopedKey);
          prefetch();
          generation.assertCurrent();
          signal.throwIfAborted();
          bytes += fetched.bytes.byteLength;
          if (bytes > byteLimit) throw new CloudBudgetError("Cloud query scan exceeded the browser byte budget");
          if (fetched.cached) {
            progress.cached++;
            progress.cachedBytes += fetched.bytes.byteLength;
          } else {
            progress.downloadedBytes += fetched.downloaded ?? fetched.bytes.byteLength;
          }
          const item = { key: file.key, scopedKey: file.scopedKey, bytes: fetched.bytes };
          await lease.ingestFile(item.scopedKey, item.key, item.bytes, undefined, signal, fileKind,
            fetched.presentColumns ? { deferView: true, presentColumns: fetched.presentColumns } : { deferView: true });
          generation.assertCurrent();
          signal.throwIfAborted();
          progress.done++;
          loading.update({ progress: { ...progress } });
        }
        await lease.publishView(signal);
        generation.assertCurrent();
        const read = lease.readContext();
        handedOff = true;
        success = true;
        return read;
      } finally {
        if (!handedOff) lease.release();
      }
    } finally {
      signal.removeEventListener("abort", abortDownloads);
      downloads.abort();
    }
  } finally {
    loading.finish(success);
  }
}
