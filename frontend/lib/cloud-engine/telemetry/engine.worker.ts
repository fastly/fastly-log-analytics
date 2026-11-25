import { createWorkerRpcHost } from "../worker-rpc";
import {
  readIngestLifecycle,
  readLogs,
  type ReadIngestLifecycleOptions,
  type ReadLogsOptions,
} from "./engine";
import { browserOpsSource, type OpsIdentity } from "./sources";

type JobMessage =
  | {
      id: number;
      type?: string;
      op?: "logs";
      options: Omit<ReadLogsOptions, "signal" | "onProgress">;
      pageLoadId?: string | null;
      identity?: OpsIdentity;
      reportApiCalls?: boolean;
    }
  | {
      id: number;
      type?: string;
      op: "ingestLifecycle";
      options: Omit<ReadIngestLifecycleOptions, "signal" | "onProgress">;
      pageLoadId?: string | null;
      identity?: OpsIdentity;
      reportApiCalls?: boolean;
    };

self.onmessage = createWorkerRpcHost<JobMessage>(async (msg, ctx) => {
  const source = browserOpsSource(
    self.location.origin,
    msg.identity,
    {
      getPageLoadId: () => msg.pageLoadId ?? "",
      onApiCall: msg.reportApiCalls
        ? (call) => ctx.post({ id: msg.id, type: "apiCall", call })
        : undefined,
    },
  );
  if (msg.op === "ingestLifecycle") {
    const result = await readIngestLifecycle(source, {
      ...msg.options,
      signal: ctx.signal,
      onProgress: (progress) => ctx.post({ id: msg.id, type: "progress", progress }),
    });
    ctx.post({ id: msg.id, type: "result", result });
    return;
  }
  const result = await readLogs(source, {
    ...msg.options,
    signal: ctx.signal,
    onProgress: (progress) => ctx.post({ id: msg.id, type: "progress", progress }),
  });
  ctx.post({ id: msg.id, type: "result", result });
});
