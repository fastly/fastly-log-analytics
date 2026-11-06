import { createWorkerRpcHost } from "../worker-rpc";
import { readLogs, type ReadLogsOptions } from "./engine";
import { browserOpsSource } from "./sources";

type JobMessage = {
  id: number;
  type?: string;
  options: Omit<ReadLogsOptions, "signal">;
  pageLoadId?: string | null;
};

self.onmessage = createWorkerRpcHost<JobMessage>(async (msg, ctx) => {
  const source = browserOpsSource(
    self.location.origin,
    () => msg.pageLoadId ?? null,
  );
  const result = await readLogs(source, { ...msg.options, signal: ctx.signal });
  ctx.post({ id: msg.id, type: "result", result });
});
