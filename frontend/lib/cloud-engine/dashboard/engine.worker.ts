import { createWorkerRpcHost } from "../worker-rpc";
import { getDuckDBEngine } from "./duckdb";
import {
  clearDashboardMemo,
  runDashboard,
  searchFieldSummaries,
  searchFieldValues,
  type DashboardResult,
  type EngineOptions,
  type RunProgress,
  type Scalar,
  type SearchFieldOptions,
} from "./engine";
import { clearManifestCache } from "./manifest";
import { browserSource, type BrowserSourceCallEvent } from "./sources";

type RunMessage = {
  id: number;
  type: "run";
  options: Omit<EngineOptions, "signal" | "onProgress" | "now" | "wallNow">;
  pageLoadId?: string | null;
  reportApiCalls?: boolean;
  cacheScope?: string;
};

type SearchMessage = {
  id: number;
  type: "search" | "searchSummaries";
  options: Omit<SearchFieldOptions, "signal" | "now">;
  pageLoadId?: string | null;
  reportApiCalls?: boolean;
  cacheScope?: string;
};

type ControlMessage = {
  id: number;
  type: "prewarm" | "clearCache";
  pageLoadId?: string | null;
  reportApiCalls?: boolean;
};

type ExecuteSqlMessage = {
  id: number;
  type: "execute_sql";
  sql: string;
  params?: readonly unknown[];
};

type GetPresentColumnsMessage = {
  id: number;
  type: "get_present_columns";
  scopedKeys?: readonly string[];
};

type SyncRumFilesMessage = {
  id: number;
  type: "sync_rum_files";
  vitalsFiles: readonly {
    scopedKey?: string;
    key: string;
    bytes: ArrayBuffer;
  }[];
  errorFiles?: readonly {
    scopedKey?: string;
    key: string;
    bytes: ArrayBuffer;
  }[];
};

export type WorkerRequest =
  | RunMessage
  | SearchMessage
  | ControlMessage
  | ExecuteSqlMessage
  | GetPresentColumnsMessage
  | SyncRumFilesMessage;

export type WorkerMessage =
  | { id: number; type: "progress"; progress: RunProgress }
  | { id: number; type: "apiCall"; call: BrowserSourceCallEvent }
  | { id: number; type: "result"; result: DashboardResult | null }
  | {
      id: number;
      type: "searchResult";
      result: { value: Scalar; count: number }[];
    }
  | {
      id: number;
      type: "searchSummariesResult";
      result: { entries: { value: Scalar; count: number }[]; partial: boolean };
    }
  | {
      id: number;
      type: "sqlResult";
      result: Record<string, unknown>[];
    }
  | {
      id: number;
      type: "presentColumnsResult";
      result: string[];
    }
  | { id: number; type: "error"; message: string };

// The page owns the cache scope (session + signed-in identity): a worker has no
// localStorage, so deriving it here would mint a fresh scope per worker and
// orphan every file persisted by the previous page load.
let sharedSource: ReturnType<typeof browserSource> | null = null;
let sharedScope: string | undefined;
const sourceForScope = (cacheScope: string | undefined) => {
  if (!sharedSource || sharedScope !== cacheScope) {
    sharedSource?.clearMemoryCache?.();
    sharedScope = cacheScope;
    sharedSource = browserSource(
      typeof self !== "undefined" && self.location ? self.location.origin : "",
      undefined,
      { foldFileTelemetry: true, cacheScope },
    );
  }
  return sharedSource;
};

self.onmessage = createWorkerRpcHost<WorkerRequest>(async (msg, ctx) => {
  if (msg.type === "prewarm") {
    await getDuckDBEngine().prewarm();
    ctx.post({ id: msg.id, type: "result", result: null });
    return;
  }
  if (msg.type === "clearCache") {
    clearDashboardMemo();
    clearManifestCache();
    sharedSource?.clearMemoryCache?.();
    await getDuckDBEngine().reset();
    ctx.post({ id: msg.id, type: "result", result: null });
    return;
  }
  if (msg.type === "execute_sql") {
    const rows = await getDuckDBEngine().executeSql(
      msg.sql,
      msg.params ?? [],
      ctx.signal,
    );
    ctx.post({ id: msg.id, type: "sqlResult", result: rows });
    return;
  }
  if (msg.type === "get_present_columns") {
    const cols = await getDuckDBEngine().getPresentColumns(msg.scopedKeys);
    ctx.post({ id: msg.id, type: "presentColumnsResult", result: [...cols] });
    return;
  }
  if (msg.type === "sync_rum_files") {
    await getDuckDBEngine().syncRumFiles(
      msg.vitalsFiles,
      msg.errorFiles ?? [],
      ctx.signal,
    );
    ctx.post({ id: msg.id, type: "result", result: null });
    return;
  }

  const source = sourceForScope(
    "cacheScope" in msg ? msg.cacheScope : sharedScope,
  ).withOptions({
    serviceId: msg.type === "search" || msg.type === "searchSummaries"
      ? msg.options.serviceId : undefined,
    getPageLoadId: () => msg.pageLoadId ?? "",
    onApiCall: msg.reportApiCalls
      ? (call: BrowserSourceCallEvent) =>
          ctx.post({ id: msg.id, type: "apiCall", call })
      : undefined,
  });

  if (msg.type === "run") {
    const result = await runDashboard(source, {
      ...msg.options,
      signal: ctx.signal,
      onProgress: (progress: RunProgress) =>
        ctx.post({ id: msg.id, type: "progress", progress }),
    });
    ctx.post({ id: msg.id, type: "result", result });
  } else if (msg.type === "search") {
    const result = await searchFieldValues(source, {
      ...msg.options,
      signal: ctx.signal,
    });
    ctx.post({ id: msg.id, type: "searchResult", result });
  } else if (msg.type === "searchSummaries") {
    const result = await searchFieldSummaries(source, {
      ...msg.options,
      signal: ctx.signal,
    });
    ctx.post({ id: msg.id, type: "searchSummariesResult", result });
  }
});
