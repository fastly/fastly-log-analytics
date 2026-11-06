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
};

type SearchMessage = {
  id: number;
  type: "search" | "searchSummaries";
  options: Omit<SearchFieldOptions, "signal" | "now">;
  pageLoadId?: string | null;
  reportApiCalls?: boolean;
};

type ControlMessage = {
  id: number;
  type: "prewarm" | "clearCache";
  pageLoadId?: string | null;
  reportApiCalls?: boolean;
};

export type WorkerRequest = RunMessage | SearchMessage | ControlMessage;

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
  | { id: number; type: "error"; message: string };

const sharedSource = browserSource(
  typeof self !== "undefined" && self.location ? self.location.origin : "",
  undefined,
  { foldFileTelemetry: true },
);

self.onmessage = createWorkerRpcHost<WorkerRequest>(async (msg, ctx) => {
  if (msg.type === "prewarm") {
    await getDuckDBEngine().prewarm();
    ctx.post({ id: msg.id, type: "result", result: null });
    return;
  }
  if (msg.type === "clearCache") {
    clearDashboardMemo();
    clearManifestCache();
    sharedSource.clearMemoryCache?.();
    await getDuckDBEngine().reset();
    ctx.post({ id: msg.id, type: "result", result: null });
    return;
  }

  const source = sharedSource.withOptions({
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
