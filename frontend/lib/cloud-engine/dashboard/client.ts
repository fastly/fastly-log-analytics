import { getPageLoadId } from "@/lib/api";
import { useDebugStore } from "@/stores/filterStore";
import { BaseWorkerRpcClient } from "../worker-rpc";
import { beginCloudLoading } from "../loading-progress";
import { resetCloudMetadataCache } from "../metadata-cache";
import { resetRangeLoaderCache } from "../range-loader";
import { clearSecurityRollupCache } from "../security-rollup";
import { clearCloudSessionReferences } from "../session-reference";
import { resetOpsLogsClient } from "../telemetry/client";
import { clearOpsCaches } from "../telemetry/sources";
import { resetUsageSettingsCache } from "../usage/summary";
import { resetComputeBillingCache } from "../usage/computeUsage";
import { clearServiceStatsCache } from "../value-stats";
import { getDuckDBEngine } from "./duckdb";
import {
  clearDashboardMemo,
  runDashboard as runInline,
  searchFieldSummaries as searchSummariesInline,
  searchFieldValues as searchInline,
  type DashboardResult,
  type EngineOptions,
  type RunProgress,
  type Scalar,
  type SearchFieldOptions,
} from "./engine";
import type { WorkerMessage, WorkerRequest } from "./engine.worker";
import type { DashboardFilter } from "./filters";
import { clearManifestCache, type Manifest } from "./manifest";
import { clearRollupQueryCache } from "./rollup-query";
import {
  IDENTITY_CACHE_KEY,
  SESSION_CACHE_KEY,
  browserSource,
  clearRequestLogCaches,
  dashboardCacheIdentity,
  dashboardCacheScope,
  updateDashboardCacheIdentity,
  type BrowserSourceTelemetry,
} from "./sources";
import type { CardField, CustomField } from "./summaries";

function debugTelemetry(): BrowserSourceTelemetry {
  return {
    getPageLoadId: () => getPageLoadId() ?? "",
    onApiCall: (call) => {
      const state = useDebugStore.getState();
      if (state.apiCallsEnabled) state.addApiCall(call);
    },
  };
}

let inlineSource: ReturnType<typeof browserSource> | null = null;
const getInlineSource = () =>
  (inlineSource ??= browserSource(
    typeof window !== "undefined" ? window.location.origin : "",
    debugTelemetry(),
  ));

let persistRequested = false;

/** Best effort: persistent storage keeps OPFS and Cache Storage files out of eviction. */
function requestPersistentStorage(): void {
  if (persistRequested) return;
  persistRequested = true;
  void globalThis.navigator?.storage?.persist?.().catch(() => false);
}

class DashboardWorkerClient extends BaseWorkerRpcClient<
  WorkerRequest,
  WorkerMessage
> {
  private readonly callbacks = new Map<
    number,
    {
      resolve: (val: unknown) => void;
      onProgress?: (progress: RunProgress) => void;
    }
  >();

  constructor() {
    super(() => {
      requestPersistentStorage();
      return new Worker(new URL("./engine.worker.ts", import.meta.url), {
        type: "module",
      });
    });
  }

  private handleMessage = (msg: WorkerMessage) => {
    const cb = this.callbacks.get(msg.id);
    if (!cb) return;
    if (msg.type === "progress") {
      cb.onProgress?.(msg.progress);
      return;
    }
    if (msg.type === "apiCall") {
      const state = useDebugStore.getState();
      if (state.apiCallsEnabled) state.addApiCall(msg.call);
      return;
    }
    if (
      msg.type === "result" ||
      msg.type === "rollupResult" ||
      msg.type === "searchResult" ||
      msg.type === "searchSummariesResult" ||
      msg.type === "sqlResult" ||
      msg.type === "presentColumnsResult"
    ) {
      if (this.finishJob(msg.id)) {
        cb.resolve(msg.result);
      }
    }
  };

  async run(options: EngineOptions): Promise<DashboardResult> {
    const loading = beginCloudLoading("files");
    let success = false;
    try {
      const result = await this.runWithProgress({
        ...options,
        onProgress: (progress) => {
          loading.update({ progress });
          options.onProgress?.(progress);
        },
      });
      success = true;
      return result;
    } finally {
      loading.finish(success);
    }
  }

  private runWithProgress(options: EngineOptions): Promise<DashboardResult> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      const source = getInlineSource().withOptions({
        ...debugTelemetry(),
        serviceId: options.serviceId,
      });
      return runInline(source, options);
    }

    const id = this.allocateId();
    const {
      signal,
      onProgress,
      now: _now,
      wallNow: _wallNow,
      ...serializable
    } = options;
    return new Promise<DashboardResult>((resolve, reject) => {
      this.callbacks.set(id, {
        resolve: resolve as (val: unknown) => void,
        onProgress,
      });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: "run",
        options: serializable,
        pageLoadId: getPageLoadId(),
        reportApiCalls: useDebugStore.getState().apiCallsEnabled,
        cacheScope: dashboardCacheScope(),
      };
      worker.postMessage(req);
    });
  }

  search(
    options: SearchFieldOptions,
    mode: "search" | "searchSummaries",
  ): Promise<unknown> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      const source = getInlineSource().withOptions({
        ...debugTelemetry(),
        serviceId: options.serviceId,
      });
      return mode === "searchSummaries"
        ? searchSummariesInline(source, options)
        : searchInline(source, options);
    }

    const id = this.allocateId();
    const { signal, now: _now, ...serializable } = options;
    return new Promise((resolve, reject) => {
      this.callbacks.set(id, { resolve });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: mode,
        options: serializable,
        pageLoadId: getPageLoadId(),
        reportApiCalls: useDebugStore.getState().apiCallsEnabled,
        cacheScope: dashboardCacheScope(),
      };
      worker.postMessage(req);
    });
  }

  answerRollup(
    path: string,
    params: Record<string, unknown>,
    serviceId: string | undefined,
    signal?: AbortSignal,
  ): Promise<unknown> | null {
    const worker = this.getWorker(this.handleMessage);
    if (!worker) return null;
    if (signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const id = this.allocateId();
    return new Promise((resolve, reject) => {
      this.callbacks.set(id, { resolve });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: "rollup",
        path,
        params,
        serviceId,
        cacheScope: dashboardCacheScope(),
      };
      worker.postMessage(req);
    });
  }

  async executeSql<T = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    signal?: AbortSignal,
  ): Promise<T[]> {
    const loading = beginCloudLoading("files");
    let success = false;
    try {
      const result = await this.executeSqlWithProgress<T>(sql, params, signal);
      success = true;
      return result;
    } finally {
      loading.finish(success);
    }
  }

  private executeSqlWithProgress<T = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    signal?: AbortSignal,
  ): Promise<T[]> {
    if (signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      return getDuckDBEngine().executeSql<T>(sql, params, signal);
    }
    const id = this.allocateId();
    return new Promise<T[]>((resolve, reject) => {
      this.callbacks.set(id, {
        resolve: resolve as (val: unknown) => void,
      });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: "execute_sql",
        sql,
        params,
      };
      worker.postMessage(req);
    });
  }

  querySql<T = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    signal?: AbortSignal,
  ): Promise<T[]> {
    return this.executeSql<T>(sql, params, signal);
  }

  async getPresentColumns(
    scopedKeys?: readonly string[],
    signal?: AbortSignal,
  ): Promise<Set<string>> {
    if (signal?.aborted) {
      throw new DOMException("Aborted", "AbortError");
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      return getDuckDBEngine().getPresentColumns(scopedKeys);
    }
    const id = this.allocateId();
    const cols = await new Promise<string[]>((resolve, reject) => {
      this.callbacks.set(id, {
        resolve: resolve as (val: unknown) => void,
      });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: "get_present_columns",
        scopedKeys,
      };
      worker.postMessage(req);
    });
    return new Set(cols);
  }

  async syncRumFiles(
    vitalsFiles: readonly {
      scopedKey?: string;
      key: string;
      bytes: ArrayBuffer;
    }[],
    errorFiles: readonly {
      scopedKey?: string;
      key: string;
      bytes: ArrayBuffer;
    }[] = [],
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) {
      throw new DOMException("Aborted", "AbortError");
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      return getDuckDBEngine().syncRumFiles(vitalsFiles, errorFiles, signal);
    }
    const id = this.allocateId();
    await new Promise<void>((resolve, reject) => {
      this.callbacks.set(id, {
        resolve: () => resolve(),
      });
      this.registerJob(id, worker, signal, reject, () => {
        this.callbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        type: "sync_rum_files",
        vitalsFiles,
        errorFiles,
      };
      worker.postMessage(req);
    });
  }

  prewarm(): void {
    const worker = this.getWorker(this.handleMessage);
    if (!worker) return;
    const id = this.allocateId();
    worker.postMessage({ id, type: "prewarm" } satisfies WorkerRequest);
  }

  catalogChanged(): void {
    const worker = this.getWorker(this.handleMessage);
    if (!worker) return;
    const id = this.allocateId();
    worker.postMessage({ id, type: "catalogChanged" } satisfies WorkerRequest);
  }
}

let rpcClient: DashboardWorkerClient | null = null;
const getClient = () => (rpcClient ??= new DashboardWorkerClient());
export { DashboardWorkerClient as DashboardDuckDBClient };

export function executeSql<T = Record<string, unknown>>(
  sql: string,
  params: readonly unknown[] = [],
  signal?: AbortSignal,
): Promise<T[]> {
  return getClient().executeSql<T>(sql, params, signal);
}

export const querySql = executeSql;

export function getPresentColumns(
  scopedKeys?: readonly string[],
  signal?: AbortSignal,
): Promise<Set<string>> {
  return getClient().getPresentColumns(scopedKeys, signal);
}

export function syncRumFiles(
  vitalsFiles: readonly {
    scopedKey?: string;
    key: string;
    bytes: ArrayBuffer;
  }[],
  errorFiles: readonly {
    scopedKey?: string;
    key: string;
    bytes: ArrayBuffer;
  }[] = [],
  signal?: AbortSignal,
): Promise<void> {
  return getClient().syncRumFiles(vitalsFiles, errorFiles, signal);
}

/**
 * Answers a Cloud summary read (`answerFromRollup`) in the dashboard engine
 * worker, so decoding and merging summaries never blocks the page. Returns
 * null where there is no Worker; the caller then answers in-thread.
 */
export function answerRollupInWorker(
  path: string,
  params: Record<string, unknown>,
  serviceId: string | undefined,
  signal?: AbortSignal,
): Promise<unknown> | null {
  return getClient().answerRollup(path, params, serviceId, signal);
}

export function prewarmDashboardEngine(): void {
  getClient().prewarm();
}

export function notifyDashboardCatalogChange(): void {
  rpcClient?.catalogChanged();
}

type IdentityTransitionTarget = {
  kind: "clear" | "set";
  identity: string | null;
  syncStorage: boolean;
};

let appliedTabIdentity: string | null = null;
let targetTransitionKey: string | null = null;
let inFlightIdentityTransition: Promise<void> | null = null;
let registeredStorageWindow: EventTarget | null = null;

async function executeIdentityTransition(
  target: IdentityTransitionTarget,
): Promise<void> {
  appliedTabIdentity = null;
  rpcClient?.terminate();
  rpcClient = null;
  resetOpsLogsClient();
  inlineSource = null;
  clearDashboardMemo();
  clearManifestCache();
  clearRollupQueryCache();
  clearSecurityRollupCache();
  resetRangeLoaderCache();
  clearCloudSessionReferences();
  resetCloudMetadataCache();
  resetUsageSettingsCache();
  resetComputeBillingCache();
  clearServiceStatsCache();
  updateDashboardCacheIdentity(target.identity, {
    syncStorage: target.syncStorage,
  });
  await Promise.all([
    getDuckDBEngine().reset(),
    clearRequestLogCaches(),
    clearOpsCaches(),
  ]);
  appliedTabIdentity = target.kind === "clear" ? null : target.identity;
}

function runIdentityTransition(
  target: IdentityTransitionTarget,
): Promise<void> {
  ensureStorageListenerRegistered();
  const key =
    target.kind === "clear" ? "clear" : `set:${target.identity ?? ""}`;
  if (inFlightIdentityTransition === null) {
    if (
      target.kind === "set" &&
      appliedTabIdentity === target.identity &&
      (!target.syncStorage || dashboardCacheIdentity() === target.identity)
    ) {
      return Promise.resolve();
    }
    const current = executeIdentityTransition(target);
    targetTransitionKey = key;
    inFlightIdentityTransition = current;
    void current.then(
      () => {
        if (inFlightIdentityTransition === current) {
          inFlightIdentityTransition = null;
          targetTransitionKey = null;
        }
      },
      () => {
        if (inFlightIdentityTransition === current) {
          inFlightIdentityTransition = null;
          targetTransitionKey = null;
        }
      },
    );
    return current;
  }
  if (targetTransitionKey === key) {
    return inFlightIdentityTransition;
  }
  const previous = inFlightIdentityTransition;
  const chained = previous
    .catch(() => {})
    .then(() => executeIdentityTransition(target));
  targetTransitionKey = key;
  inFlightIdentityTransition = chained;
  void chained.then(
    () => {
      if (inFlightIdentityTransition === chained) {
        inFlightIdentityTransition = null;
        targetTransitionKey = null;
      }
    },
    () => {
      if (inFlightIdentityTransition === chained) {
        inFlightIdentityTransition = null;
        targetTransitionKey = null;
      }
    },
  );
  return chained;
}

function handleCrossTabStorageEvent(event: Event): void {
  const storageEvent = event as StorageEvent;
  if (
    storageEvent.storageArea !== null &&
    storageEvent.storageArea !== undefined &&
    typeof localStorage !== "undefined" &&
    storageEvent.storageArea !== localStorage
  ) {
    return;
  }
  if (storageEvent.key === null) {
    void runIdentityTransition({
      kind: "clear",
      identity: null,
      syncStorage: false,
    });
    return;
  }
  if (storageEvent.key === IDENTITY_CACHE_KEY) {
    const next = storageEvent.newValue;
    if (next === null || next === "") {
      void runIdentityTransition({
        kind: "clear",
        identity: null,
        syncStorage: false,
      });
    } else if (next !== appliedTabIdentity) {
      void runIdentityTransition({
        kind: "set",
        identity: next,
        syncStorage: false,
      });
    }
    return;
  }
  if (storageEvent.key === SESSION_CACHE_KEY && storageEvent.newValue === null) {
    const currentStored = dashboardCacheIdentity();
    if (currentStored === null || currentStored === "") {
      void runIdentityTransition({
        kind: "clear",
        identity: null,
        syncStorage: false,
      });
    } else if (currentStored !== appliedTabIdentity) {
      void runIdentityTransition({
        kind: "set",
        identity: currentStored,
        syncStorage: false,
      });
    }
  }
}

function ensureStorageListenerRegistered(): void {
  if (
    typeof window === "undefined" ||
    typeof window.addEventListener !== "function"
  ) {
    return;
  }
  if (registeredStorageWindow === window) return;
  registeredStorageWindow = window;
  window.addEventListener("storage", handleCrossTabStorageEvent);
}

ensureStorageListenerRegistered();

export async function clearDashboardData(): Promise<void> {
  await runIdentityTransition({
    kind: "clear",
    identity: null,
    syncStorage: true,
  });
}

export async function setDashboardCacheIdentity(
  identity: string,
): Promise<void> {
  await runIdentityTransition({
    kind: "set",
    identity,
    syncStorage: true,
  });
}

export function runDashboard(options: EngineOptions): Promise<DashboardResult> {
  return getClient().run(options);
}

export function runInWorker(
  options: Omit<EngineOptions, "customFields"> & {
    customFields?: readonly CustomField[] | readonly string[];
    interval?: unknown;
    metric?: unknown;
  },
  signal?: AbortSignal,
): Promise<DashboardResult>;
export function runInWorker(
  from: string,
  to: string,
  filters: DashboardFilter[],
  options?: {
    edgeOnly?: boolean;
    manifest?: Manifest;
    signal?: AbortSignal;
    onProgress?: (progress: RunProgress) => void;
    customFields?: readonly CustomField[];
  },
): Promise<DashboardResult>;
export function runInWorker(
  fromOrOptions:
    | string
    | (Omit<EngineOptions, "customFields"> & {
        customFields?: readonly CustomField[] | readonly string[];
      }),
  toOrSignal?: string | AbortSignal,
  filters?: DashboardFilter[],
  options: {
    edgeOnly?: boolean;
    manifest?: Manifest;
    signal?: AbortSignal;
    onProgress?: (progress: RunProgress) => void;
    customFields?: readonly CustomField[];
  } = {},
): Promise<DashboardResult> {
  if (typeof fromOrOptions === "object" && fromOrOptions !== null) {
    const customFields: CustomField[] | undefined =
      fromOrOptions.customFields?.map((f) =>
        typeof f === "string" ? { id: f, type: "string" as const } : f,
      );
    return runDashboard({
      ...fromOrOptions,
      customFields,
      signal:
        (toOrSignal instanceof AbortSignal ? toOrSignal : undefined) ??
        fromOrOptions.signal,
    });
  }
  return runDashboard({
    from: fromOrOptions,
    to: toOrSignal as string,
    filters,
    ...options,
  });
}

export function searchFieldValues(
  options: SearchFieldOptions,
): Promise<{ value: Scalar; count: number }[]> {
  return getClient().search(options, "search") as Promise<
    { value: Scalar; count: number }[]
  >;
}

export function searchFieldValuesInWorker(
  from: string,
  to: string,
  column: CardField,
  query: string,
  filters: DashboardFilter[],
  options: {
    edgeOnly?: boolean;
    manifest?: Manifest;
    limit?: number;
    signal?: AbortSignal;
    customFields?: readonly CustomField[];
    serviceId?: string;
  } = {},
): Promise<{ value: Scalar; count: number }[]> {
  return searchFieldValues({
    from,
    to,
    column,
    query,
    filters,
    ...options,
  });
}

export function searchFieldSummaries(
  options: SearchFieldOptions,
): Promise<{ entries: { value: Scalar; count: number }[]; partial: boolean }> {
  return getClient().search(options, "searchSummaries") as Promise<{
    entries: { value: Scalar; count: number }[];
    partial: boolean;
  }>;
}

export function searchFieldSummariesInWorker(
  from: string,
  to: string,
  column: CardField,
  query: string,
  filters: DashboardFilter[],
  options: {
    edgeOnly?: boolean;
    manifest?: Manifest;
    limit?: number;
    signal?: AbortSignal;
    customFields?: readonly CustomField[];
    serviceId?: string;
  } = {},
): Promise<{ entries: { value: Scalar; count: number }[]; partial: boolean }> {
  return searchFieldSummaries({
    from,
    to,
    column,
    query,
    filters,
    ...options,
  });
}

export const queryFieldValues = searchFieldValues;
export const getFieldValues = searchFieldValues;
export { clearRequestLogCaches, getStoreStats } from "./sources";
