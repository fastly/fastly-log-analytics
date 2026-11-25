import { ALL_TABLE_FIELDS, getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import type { CustomField } from "./dashboard/summaries";
import { fetchManifest, type ManifestFile } from "./dashboard/manifest";
import {
  browserSource,
  dashboardCacheScope,
  dashboardManifestCacheScope,
  type DerivedFile,
} from "./dashboard/sources";
import {
  assembleProjectedParquet, fetchProjectedRanges, filterRowGroupFilter, planParquetProjection, readParquetFooter,
  readsWholeFirst, readWholeParquetFooter, shouldFallbackToWhole, timestampRowGroupFilter, wholeReadLimits,
  type ParquetFooter, type ParquetProjection, type RowGroupFilterPredicate,
} from "./dashboard/parquet-projection";
import { prefersWholeForWideProjection, wholeFileIdentity } from "./dashboard/projected-read";
import { beginCloudLoading } from "./loading-progress";
import { fetchLanes, rawLaneForBytes } from "./fetch-lanes";
import { CloudBudgetError, MAX_REGISTERED_BYTES, type CloudBudgetDetail } from "./dashboard/budget";

const MAX_RANGE_FILES = 256;
// RUM window files are a few KiB each and the open hour's stay loose until they age into a
// compact, so a day lists more of them than the request table does; the byte budget, not the file
// count, bounds that scan.
const MAX_RUM_RANGE_FILES = 2048;
const MAX_RANGE_BYTES = MAX_REGISTERED_BYTES;
const PREFETCH_CONCURRENCY = 6;
// Projected ranges are mostly a few KiB, so their round trips, not bandwidth, bound a cold load.
const RANGE_CONCURRENCY = 16;
const source = browserSource("");
const requestReads = new WeakMap<Request, DuckDBReadContext>();
const activeRangeDownloads = new Set<AbortController>();

export { CloudBudgetError, type CloudBudgetDetail };

export function formatBudgetAmount(unit: CloudBudgetDetail["unit"], amount: number): string {
  return unit === "files" ? `${amount} files` : `${(amount / (1024 * 1024)).toFixed(1)} MiB`;
}

function overBudget(detail: CloudBudgetDetail): CloudBudgetError {
  return new CloudBudgetError(`Cloud query range needs ${formatBudgetAmount(detail.unit, detail.needed)} but the browser scan budget is ${
    formatBudgetAmount(detail.unit, detail.limit)}; choose a shorter time range`, detail);
}

/**
 * The Cloud file source the range loader reads through, scoped to a service
 * when one is named; its cache scope follows the signed-in identity.
 */
export function cloudFileSource(serviceId?: string) {
  const scoped = serviceId ? source.withOptions({ serviceId }) : source;
  return { ...scoped, cacheScope: `${dashboardCacheScope()}:${serviceId ?? ""}` };
}

export function resetRangeLoaderCache(): void {
  for (const downloads of activeRangeDownloads) {
    downloads.abort(new DOMException("The operation was aborted.", "AbortError"));
  }
  activeRangeDownloads.clear();
  source.clearMemoryCache?.();
}

function scopedFetchJson(serviceId?: string) {
  if (!serviceId) return source.fetchJson;
  return Object.assign(
    (path: string, signal?: AbortSignal) => {
      const url = new URL(path, globalThis.location?.origin ?? "http://localhost");
      url.searchParams.set("service_id", serviceId);
      return source.fetchJson(`${url.pathname}${url.search}`, signal);
    },
    { manifestCacheScope: () => dashboardManifestCacheScope("", serviceId) },
  );
}

const fileIdentity = (scope: string, serviceId: string | undefined, dataset: string, file: ManifestFile) =>
  [scope, serviceId ?? "", dataset, file.key, file.etag ?? "", file.size, file.schema_version ?? ""];

type FileSource = ReturnType<typeof source.withOptions>;

/**
 * A file's Parquet footer. Footers persist under the file's own identity, so
 * any projection of it, and a schema-only read, reuse them. `check` runs after
 * every await; `canPersist` gates writing a freshly read footer.
 */
async function readPersistedFooter(
  fileSource: FileSource,
  file: { key: string; size: number; footerKey: string },
  fetchRange: (start: number, end: number) => Promise<ArrayBuffer>,
  check: () => void,
  canPersist: () => boolean,
) {
  check();
  const cached = await fileSource.readDerivedFile(file.key, file.footerKey);
  check();
  const tailStart = cached ? Number(cached.meta) : NaN;
  const tail = cached && Number.isSafeInteger(tailStart) && tailStart >= 0
    ? { start: tailStart, bytes: await cached.bytes() } : null;
  check();
  const footer = await readParquetFooter(file.size, (start, end) =>
    tail && start >= tail.start && end <= tail.start + tail.bytes.byteLength
      ? Promise.resolve(tail.bytes.slice(start - tail.start, end - tail.start)) : fetchRange(start, end));
  check();
  if (!tail && footer.tail && canPersist()) {
    void fileSource.writeDerivedFile(file.key, file.footerKey, footer.tail.bytes, String(footer.tail.start));
  }
  return footer;
}

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
     * range on their footer-measured bytes (at most `maxBytes`, capped at and
     * defaulting to what DuckDB can register) instead of whole-file sizes. Files a listing from
     * `wider.from` to the range end would include also load `wider.columns`,
     * so they share the cache entries of an earlier load of that range.
     */
    projection?: {
      columns: readonly string[];
      /** Skip files whose schema has none of these columns without downloading their data ranges. */
      requireAnyColumns?: readonly string[];
      /** Exact-value include/exclude filters used to prune non-matching Parquet row groups via footer statistics. */
      filters?: readonly RowGroupFilterPredicate[];
      /** Fetch files up to this size whole-first when total candidates fit `byteLimit`, avoiding footer probes and range fan-out across multi-pass scans (e.g. /sessions). */
      preferWholeUpToBytes?: number;
      maxBytes?: number;
      wider?: { from: string; columns: readonly string[] };
    };
  } = {},
): Promise<DuckDBReadContext> {
  const projection = options.projection;
  if (projection && datasets.some((dataset) => dataset !== "request"))
    throw new Error("Cloud column projection applies to request files only");
  const projectedColumns = projection ? new Set(projection.columns) : null;
  const requiredAnyColumns = projection?.requireAnyColumns?.length ? new Set(projection.requireAnyColumns) : null;
  const wider = projection?.wider;
  const widerColumns = wider ? new Set([...projection.columns, ...wider.columns]) : null;
  // The same second-floored bound as the listing below.
  const widerFromMs = wider ? Math.floor(Date.parse(wider.from) / 1000) * 1000 : NaN;
  if (wider && !Number.isFinite(widerFromMs)) throw new Error("Invalid Cloud projection range");
  // DuckDB registers at most MAX_REGISTERED_BYTES, so a larger projection budget could only fail later.
  const byteLimit = Math.min(projection?.maxBytes ?? MAX_RANGE_BYTES, MAX_REGISTERED_BYTES);
  const engine = getDuckDBEngine();
  const generation = await engine.beginRequest();
  generation.assertCurrent();
  signal.throwIfAborted();
  const loading = beginCloudLoading("listing");
  // WASM instantiation needs nothing from the listing, so it overlaps it. A
  // failure here surfaces from the first DuckDB operation below.
  engine.prewarm().catch(() => {});
  let success = false;
  try {
    const fetchJson = scopedFetchJson(options.serviceId);
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
    const fileSource = options.serviceId ? source.withOptions({ serviceId: options.serviceId }) : source;
    const scope = dashboardCacheScope();
    const wholeScope = options.serviceId ? `${scope}:${options.serviceId}` : scope;
    const files = manifests.flatMap((manifest, index) =>
      manifest.files.map((file) => {
        const identity = fileIdentity(scope, options.serviceId, datasets[index], file);
        const isWider = Boolean(widerColumns && Date.parse(file.end) > widerFromMs);
        const columns = isWider ? widerColumns : projectedColumns;
        return { ...file, dataset: datasets[index], identity, isWider, columns,
          wholeKey: wholeFileIdentity(wholeScope, file, datasets[index]),
          footerKey: JSON.stringify([...identity, "footer"]),
          scopedKey: JSON.stringify([...identity,
            ...columns ? ["projection", ...[...columns].sort()] : []]),
        };
      }),
    );
    const rangeFromMs = Date.parse(from);
    const rangeToMs = Date.parse(to);
    const rowGroupFilters = projection?.filters?.length ? projection.filters : null;
    const planForFile = (file: typeof files[number], footer: ParquetFooter): ParquetProjection => {
      const winFrom = file.isWider && Number.isFinite(widerFromMs)
        ? Math.min(rangeFromMs, widerFromMs) : rangeFromMs;
      const byTime = Number.isFinite(winFrom) && Number.isFinite(rangeToMs)
        ? timestampRowGroupFilter(footer.metadata, winFrom, rangeToMs) : null;
      const byFilters = rowGroupFilters ? filterRowGroupFilter(footer.metadata, rowGroupFilters) : null;
      const keep = byTime && byFilters
        ? (rg: number) => byTime(rg) && byFilters(rg)
        : byTime ?? byFilters ?? undefined;
      return planParquetProjection(footer, file.columns!, keep);
    };
    const fileLimit = datasets.every((dataset) => dataset === "rum") ? MAX_RUM_RANGE_FILES : MAX_RANGE_FILES;
    if (files.length > fileLimit) throw overBudget({ unit: "files", needed: files.length, limit: fileLimit });
    const wholeBytes = files.reduce((total, file) => total + file.size, 0);
    if (!projection && wholeBytes > MAX_RANGE_BYTES) {
      throw overBudget({ unit: "bytes", needed: wholeBytes, limit: MAX_RANGE_BYTES });
    }
    const downloads = new AbortController();
    activeRangeDownloads.add(downloads);
    const abortDownloads = () => downloads.abort(signal.reason);
    signal.addEventListener("abort", abortDownloads, { once: true });
    if (signal.aborted) abortDownloads();
    let slots = RANGE_CONCURRENCY;
    const waiting: (() => void)[] = [];
    const inSlot = async <T>(read: () => Promise<T>, lane: "raw" | "large" = "raw") => {
      if (slots === 0) await new Promise<void>((resolve) => waiting.push(resolve));
      else slots--;
      try {
        return await fetchLanes.run(lane, () => {
          downloads.signal.throwIfAborted();
          return read();
        }, downloads.signal);
      } finally {
        const next = waiting.shift();
        if (next) next();
        else slots++;
      }
    };
    const fetchRange = (file: typeof files[number], start: number, end: number) =>
      inSlot(() => fileSource.fetchRange(file, start, end, downloads.signal, "low"));
    const canPersistDerived = () => {
      if (downloads.signal.aborted) return false;
      try {
        generation.assertCurrent();
        return true;
      } catch {
        return false;
      }
    };
    const readFooter = (file: typeof files[number]) => readPersistedFooter(fileSource, file,
      (start, end) => fetchRange(file, start, end), () => {
        generation.assertCurrent();
        downloads.signal.throwIfAborted();
      }, canPersistDerived);
    const fetchWhole = async (file: typeof files[number]) => {
      const lane = rawLaneForBytes(file.size);
      const fetched = await inSlot(
        () => fileSource.fetchFile(file.key, downloads.signal, {
          identity: file.wholeKey,
          ...(lane === "large" ? { priority: "low" as const } : {}),
        }),
        lane,
      );
      if (fetched.bytes.byteLength !== file.size) {
        throw new Error(`Whole-file read of ${file.key} returned ${fetched.bytes.byteLength} bytes, not ${file.size}`);
      }
      return fetched;
    };
    try {
      const plans = new Map<string, ParquetProjection>();
      const persisted = new Map<string, DerivedFile>();
      // Files read whole skip their footer reads. They download before the budget check, so they
      // are read whole only when together they fit the budget.
      const wholeFirst = new Set<string>();
      const wholes = new Map<string, Awaited<ReturnType<typeof fetchWhole>>>();
      const skippedKeys = new Set<string>();
      const preferWholeUpToBytes = projection?.preferWholeUpToBytes ?? 0;
      const rangeWindow = { fromMs: Date.parse(from), toMs: Date.parse(to) };
      const shouldReadWholeFirst = (file: typeof files[number], nowMs: number) =>
        readsWholeFirst(file, nowMs)
        || (projectedColumns !== null && prefersWholeForWideProjection(file, projectedColumns.size, rangeWindow))
        || (preferWholeUpToBytes > 0 && file.size > 0 && file.size <= preferWholeUpToBytes);
      if (projectedColumns) {
        loading.update({ stage: "files" });
        const unknown = files.filter((file) => !engine.getFileMeta(file.scopedKey));
        if (unknown.some((file) => !(file.size > 0))) throw new Error("Cloud file listing has no file size to project");
        const now = Date.now();
        // When `requireAnyColumns` is set, read the footer first so files lacking all required
        // columns are skipped without downloading them whole.
        const candidates = requiredAnyColumns ? [] : unknown.filter((file) => shouldReadWholeFirst(file, now));
        const candidateBytes = candidates.reduce((total, file) => total + file.size, 0);
        if (candidateBytes <= byteLimit) {
          for (const file of candidates) wholeFirst.add(file.scopedKey);
        } else if (candidates.some((file) => prefersWholeForWideProjection(file, projectedColumns.size, rangeWindow))) {
          throw overBudget({ unit: "bytes", needed: candidateBytes, limit: byteLimit });
        }
        await Promise.all(unknown.map(async (file) => {
          generation.assertCurrent();
          downloads.signal.throwIfAborted();
          const stored = await fileSource.readDerivedFile(file.key, file.scopedKey);
          generation.assertCurrent();
          downloads.signal.throwIfAborted();
          if (stored) {
            persisted.set(file.scopedKey, stored);
            return;
          }
          let whole: Awaited<ReturnType<typeof fetchWhole>> | undefined;
          let footer: ParquetFooter;
          const hasCachedWhole = wholeFirst.has(file.scopedKey)
            || Boolean(await fileSource.hasFile?.(file.key, { identity: file.wholeKey }));
          if (hasCachedWhole) {
            whole = await fetchWhole(file);
            generation.assertCurrent();
            downloads.signal.throwIfAborted();
            footer = await readWholeParquetFooter(whole.bytes);
          } else {
            footer = await readFooter(file);
          }
          const plan = planForFile(file, footer);
          if (plan.rowGroups.length === 0) {
            skippedKeys.add(file.scopedKey);
            return;
          }
          if (plan.rowGroups.length < footer.metadata.row_groups.length) {
            file.scopedKey = JSON.stringify([
              ...file.identity, "projection", ...[...file.columns!].sort(), { rowGroups: plan.rowGroups },
            ]);
            if (engine.getFileMeta(file.scopedKey)) return;
            const prunedStored = await fileSource.readDerivedFile(file.key, file.scopedKey);
            generation.assertCurrent();
            downloads.signal.throwIfAborted();
            if (prunedStored) {
              persisted.set(file.scopedKey, prunedStored);
              return;
            }
          }
          if (whole) wholes.set(file.scopedKey, whole);
          plans.set(file.scopedKey, plan);
        }));
        generation.assertCurrent();
        signal.throwIfAborted();
        if (requiredAnyColumns) {
          for (const file of files) {
            const plan = plans.get(file.scopedKey);
            if (plan && !plan.presentColumns.some((col) => requiredAnyColumns.has(col))) {
              skippedKeys.add(file.scopedKey);
              continue;
            }
            const stored = persisted.get(file.scopedKey);
            if (stored) {
              try {
                const cols: unknown = JSON.parse(stored.meta);
                if (Array.isArray(cols) && !cols.some((col) => typeof col === "string" && requiredAnyColumns.has(col))) {
                  skippedKeys.add(file.scopedKey);
                }
              } catch {
                // Malformed cached metadata will be rejected when read below.
              }
            }
          }
        }
      }
      const activeFiles = skippedKeys.size ? files.filter((file) => !skippedKeys.has(file.scopedKey)) : files;
      if (projectedColumns) {
        const needed = activeFiles.reduce((total, file) => total + (plans.get(file.scopedKey)?.projectedBytes
          ?? persisted.get(file.scopedKey)?.byteLength ?? engine.getFileMeta(file.scopedKey)?.byteLength ?? 0), 0);
        if (needed > byteLimit) throw overBudget({ unit: "bytes", needed, limit: byteLimit });
        const wireNeeded = activeFiles.reduce((total, file) => {
          const plan = plans.get(file.scopedKey);
          if (!plan) return total;
          const held = wholes.get(file.scopedKey);
          if (held?.cached) return total;
          const plannedRangeBytes = plan.ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
          const willFetchWhole = Boolean(held) || shouldFallbackToWhole(plan, file.size, plannedRangeBytes);
          return total + (willFetchWhole ? file.size : plannedRangeBytes);
        }, 0);
        if (wireNeeded > byteLimit) throw overBudget({ unit: "bytes", needed: wireNeeded, limit: byteLimit });
      }
      const progress = { phase: "files" as const, cached: 0, toFetch: activeFiles.length,
        toDecode: 0, done: 0, downloadedBytes: 0, cachedBytes: 0 };
      loading.update({ stage: "files", progress: { ...progress } });
      const kind = (file: typeof files[number]) => file.dataset !== "rum" ? "request"
        : file.key.includes("/client_vitals/") ? "vitals"
          : file.key.includes("/client_errors/") ? "errors" : null;
      if (activeFiles.some((file) => kind(file) === null)) throw new Error("Unrecognized Cloud RUM file kind");
      await engine.retainFiles({
        request: activeFiles.filter((file) => kind(file) === "request").map((file) => file.scopedKey),
        vitals: activeFiles.filter((file) => kind(file) === "vitals").map((file) => file.scopedKey),
        errors: activeFiles.filter((file) => kind(file) === "errors").map((file) => file.scopedKey),
      }, signal);
      generation.assertCurrent();
      const customFields = options.customFields?.filter((field) => !ALL_TABLE_FIELDS.includes(field.id)) ?? [];
      const lease = engine.lease(customFields);
      const residentBytesByKey = new Map<string, number>();
      for (const file of activeFiles) {
        const fileKind = kind(file);
        if (!fileKind) throw new Error("Unrecognized Cloud RUM file kind");
        const meta = lease.getFileMeta(file.scopedKey, fileKind);
        if (meta) residentBytesByKey.set(file.scopedKey, meta.byteLength);
      }
      let bytes = 0;
      let handedOff = false;
      const pending = activeFiles.filter((file) => !residentBytesByKey.has(file.scopedKey));
      const fetchProjected = async (file: typeof files[number]) => {
        const stored = persisted.get(file.scopedKey);
        if (stored) {
          const presentColumns: unknown = JSON.parse(stored.meta);
          if (!Array.isArray(presentColumns) || !presentColumns.every((name) => typeof name === "string")) {
            throw new Error(`Cached projection of ${file.key} has no column list`);
          }
          return { bytes: await stored.bytes(), cached: true, presentColumns };
        }
        let held = wholes.get(file.scopedKey);
        wholes.delete(file.scopedKey);
        let plan = plans.get(file.scopedKey);
        // Resident at the budget check but evicted since.
        if (!plan && shouldReadWholeFirst(file, Date.now())) {
          held = await fetchWhole(file);
          plan = planForFile(file, await readWholeParquetFooter(held.bytes));
        }
        plan ??= planForFile(file, await readFooter(file));
        const plannedRangeBytes = plan.ranges.reduce((sum, r) => sum + (r.end - r.start), 0);
        // When coalesced ranges already cover most of a file (e.g. heavily compacted multi-row-group
        // files), fetch the file once via `fileSource.fetchFile` so concurrent page sections share a
        // single in-flight/cached download instead of each pulling the file separately via ranges.
        if (held || shouldFallbackToWhole(plan, file.size, plannedRangeBytes)) {
          const fetched = held ?? await fetchWhole(file);
          const parts = await fetchProjectedRanges(plan, async (start, end) => fetched.bytes.slice(start, end));
          const bytes = assembleProjectedParquet(plan, parts);
          if (canPersistDerived()) {
            void fileSource.writeDerivedFile(file.key, file.scopedKey, bytes, JSON.stringify(plan.presentColumns));
          }
          return {
            bytes,
            cached: fetched.cached,
            downloaded: fetched.cached ? 0 : fetched.bytes.byteLength,
            presentColumns: plan.presentColumns,
          };
        }
        let downloaded = 0;
        const parts = await fetchProjectedRanges(plan, async (start, end) => {
          const part = await fetchRange(file, start, end);
          downloaded += part.byteLength;
          return part;
        });
        const bytes = assembleProjectedParquet(plan, parts);
        if (canPersistDerived()) {
          void fileSource.writeDerivedFile(file.key, file.scopedKey, bytes, JSON.stringify(plan.presentColumns));
        }
        return { bytes, cached: false, downloaded, presentColumns: plan.presentColumns };
      };
      const fetchOne = (file: typeof files[number]): Promise<{
        bytes: ArrayBuffer; cached: boolean; downloaded?: number; presentColumns?: string[];
      }> => {
        if (projectedColumns) return fetchProjected(file);
        const lane = rawLaneForBytes(file.size);
        return inSlot(
          () => fileSource.fetchFile(file.key, downloads.signal, {
            identity: file.scopedKey,
            ...(lane === "large" ? { priority: "low" as const } : {}),
          }),
          lane,
        );
      };
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
        for (const file of activeFiles) {
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
      activeRangeDownloads.delete(downloads);
      signal.removeEventListener("abort", abortDownloads);
      downloads.abort();
    }
  } finally {
    loading.finish(success);
  }
}
