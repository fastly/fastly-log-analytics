import { getPageLoadId } from "@/lib/api";
import { BaseWorkerRpcClient } from "../worker-rpc";
import {
  readLogs as readLogsInline,
  type LogResult,
  type LogsRange,
  type LogsResult,
  type OpsKind,
  type ReadLogsOptions,
} from "./engine";
import { browserOpsSource } from "./sources";

type WorkerRequest = {
  id: number;
  options: Omit<ReadLogsOptions, "signal">;
  pageLoadId: string | null;
};

type WorkerMessage =
  | { id: number; type: "result"; result: LogResult }
  | { id: number; type: "error"; message: string };

class OpsWorkerClient extends BaseWorkerRpcClient<
  WorkerRequest,
  WorkerMessage
> {
  private readonly resolvers = new Map<number, (result: LogResult) => void>();

  constructor() {
    super(
      () =>
        new Worker(new URL("./engine.worker.ts", import.meta.url), {
          type: "module",
        }),
    );
  }

  private handleMessage = (msg: WorkerMessage) => {
    if (msg.type === "result") {
      const resolve = this.resolvers.get(msg.id);
      if (this.finishJob(msg.id)) {
        resolve?.(msg.result);
      }
    }
  };

  run(options: ReadLogsOptions): Promise<LogResult> {
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
    const { signal, ...serializable } = options;
    return new Promise<LogResult>((resolve, reject) => {
      this.resolvers.set(id, resolve);
      this.registerJob(id, worker, signal, reject, () => {
        this.resolvers.delete(id);
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
    return getClient().run({
      kind: kindOrOptions,
      from: range!.from,
      to: range!.to,
      signal: options?.signal,
    });
  }
  return getClient().run(kindOrOptions);
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
