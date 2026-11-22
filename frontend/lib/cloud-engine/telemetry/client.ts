import { getPageLoadId } from "@/lib/api";
import { BaseWorkerRpcClient } from "../worker-rpc";
import { beginCloudLoading } from "../loading-progress";
import { createSharedRequests } from "../shared-request";
import type { RunProgress } from "../dashboard/engine";
import {
  readLogs as readLogsInline,
  type LogResult,
  type LogsRange,
  type LogsResult,
  type OpsKind,
  type ReadLogsOptions,
} from "./engine";
import { activeOpsUser, browserOpsSource } from "./sources";

type WorkerRequest = {
  id: number;
  options: Omit<ReadLogsOptions, "signal" | "onProgress">;
  pageLoadId: string | null;
};

type WorkerMessage =
  | { id: number; type: "result"; result: LogResult }
  | { id: number; type: "progress"; progress: RunProgress }
  | { id: number; type: "error"; message: string };

class OpsWorkerClient extends BaseWorkerRpcClient<
  WorkerRequest,
  WorkerMessage
> {
  private readonly resolvers = new Map<number, (result: LogResult) => void>();
  private readonly progressCallbacks = new Map<number, (progress: RunProgress) => void>();

  constructor() {
    super(
      () =>
        new Worker(new URL("./engine.worker.ts", import.meta.url), {
          type: "module",
        }),
    );
  }

  private handleMessage = (msg: WorkerMessage) => {
    if (msg.type === "progress") {
      this.progressCallbacks.get(msg.id)?.(msg.progress);
      return;
    }
    if (msg.type === "result") {
      const resolve = this.resolvers.get(msg.id);
      if (this.finishJob(msg.id)) {
        resolve?.(msg.result);
      }
    }
  };

  async run(options: ReadLogsOptions): Promise<LogResult> {
    const loading = beginCloudLoading("listing");
    let success = false;
    try {
      const result = await this.runWithProgress({
        ...options,
        onProgress: (progress) => {
          loading.update({ stage: "files", progress });
          options.onProgress?.(progress);
        },
      });
      success = true;
      return result;
    } finally {
      loading.finish(success);
    }
  }

  private runWithProgress(options: ReadLogsOptions): Promise<LogResult> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    if (!worker) {
      return readLogsInline(
        browserOpsSource(
          typeof window !== "undefined" ? window.location.origin : "",
          getPageLoadId,
        ),
        options,
      );
    }

    const id = this.allocateId();
    const { signal, onProgress, ...serializable } = options;
    return new Promise<LogResult>((resolve, reject) => {
      this.resolvers.set(id, resolve);
      if (onProgress) this.progressCallbacks.set(id, onProgress);
      this.registerJob(id, worker, signal, reject, () => {
        this.resolvers.delete(id);
        this.progressCallbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        options: serializable,
        pageLoadId: getPageLoadId(),
      };
      worker.postMessage(req);
    });
  }
}

let rpcClient: OpsWorkerClient | null = null;
const getClient = () => (rpcClient ??= new OpsWorkerClient());

const sharedReads = createSharedRequests<LogsResult, RunProgress>();

// Several panels often ask for the same kind and range at once; they share one
// read. The key carries every identity the request could depend on.
function sharedRead(
  { kind, from, to, signal, onProgress }: ReadLogsOptions,
  scope: { user?: string; role?: string } = {},
): Promise<LogsResult> {
  const key = JSON.stringify([
    kind,
    from,
    to,
    scope.user ?? null,
    scope.role ?? null,
    activeOpsUser(),
    getPageLoadId(),
  ]);
  return sharedReads(
    key,
    (shared, notify) =>
      getClient().run({ kind, from, to, signal: shared, onProgress: notify }),
    { signal, onProgress },
  );
}

export function readLogs(options: ReadLogsOptions): Promise<LogsResult>;
export function readLogs(
  kind: OpsKind,
  range: LogsRange,
  options?: { user?: string; role?: string; signal?: AbortSignal },
): Promise<LogsResult>;
export function readLogs(
  kindOrOptions: OpsKind | ReadLogsOptions,
  range?: LogsRange,
  options?: { user?: string; role?: string; signal?: AbortSignal },
): Promise<LogsResult> {
  if (typeof kindOrOptions === "string") {
    return sharedRead(
      {
        kind: kindOrOptions,
        from: range!.from,
        to: range!.to,
        signal: options?.signal,
      },
      { user: options?.user, role: options?.role },
    );
  }
  return sharedRead(kindOrOptions);
}

export const queryOpsLogs = readLogs;

export function opsLogRange(
  hours: number,
  nowMs = Date.now(),
): { from: string; to: string } {
  const end = Math.ceil(nowMs / 60_000) * 60_000;
  return {
    from: new Date(end - hours * 3_600_000).toISOString(),
    to: new Date(end).toISOString(),
  };
}
