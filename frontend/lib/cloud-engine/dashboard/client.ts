import { getPageLoadId } from "@/lib/api";
import { useDebugStore } from "@/stores/filterStore";
import { BaseWorkerRpcClient } from "../worker-rpc";
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
import {
  browserSource,
  clearRequestLogCaches,
  dashboardCacheIdentity,
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
    super(
      () =>
        new Worker(new URL("./engine.worker.ts", import.meta.url), {
          type: "module",
        }),
    );
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

  run(options: EngineOptions): Promise<DashboardResult> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) return runInline(getInlineSource(), options);

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
      return mode === "searchSummaries"
        ? searchSummariesInline(getInlineSource(), options)
        : searchInline(getInlineSource(), options);
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
      };
      worker.postMessage(req);
    });
  }

  executeSql<T = Record<string, unknown>>(
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

export function prewarmDashboardEngine(): void {
  getClient().prewarm();
}

export async function clearDashboardData(): Promise<void> {
  rpcClient?.terminate();
  rpcClient = null;
  clearDashboardMemo();
  clearManifestCache();
  updateDashboardCacheIdentity(null);
  await clearRequestLogCaches();
}

export async function setDashboardCacheIdentity(
  identity: string,
): Promise<void> {
  const previous = dashboardCacheIdentity();
  if (previous === identity) return;
  rpcClient?.terminate();
  rpcClient = null;
  clearDashboardMemo();
  clearManifestCache();
  updateDashboardCacheIdentity(identity);
  if (previous !== null) {
    await clearRequestLogCaches();
  }
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
