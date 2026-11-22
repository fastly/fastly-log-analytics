export type WorkerCancelMsg = { id: number; type: "cancel" };

export type WorkerErrorMsg = { id: number; type: "error"; message: string };

export type PendingJob = {
  reject: (err: Error) => void;
  cleanup: () => void;
};

export interface WorkerHostContext {
  signal: AbortSignal;
  post: (msg: Record<string, unknown>, transfer?: Transferable[]) => void;
}

/**
 * Shared browser Web Worker RPC client with `AbortSignal` propagation,
 * pending job tracking, error rejection, and worker termination recovery.
 */
export class BaseWorkerRpcClient<TReq extends { id: number }, TMsg extends { id: number; type: string }> {
  private worker: Worker | null = null;
  private nextId = 1;
  private readonly pending = new Map<number, PendingJob>();

  constructor(private readonly createWorker: () => Worker) {}

  protected allocateId(): number {
    return this.nextId++;
  }

  protected getWorker(onMessage: (msg: TMsg) => void): Worker | null {
    if (this.worker) return this.worker;
    if (typeof Worker === "undefined") return null;
    try {
      const worker = this.createWorker();
      worker.onmessage = (event: MessageEvent<TMsg>) => {
        const msg = event.data;
        if (msg.type === "error") {
          const job = this.pending.get(msg.id);
          if (job) {
            this.pending.delete(msg.id);
            job.cleanup();
            job.reject(
              new Error(
                (msg as unknown as WorkerErrorMsg).message ?? "Worker error",
              ),
            );
          }
          return;
        }
        onMessage(msg);
      };
      worker.onerror = (event) => {
        this.failAll(new Error(event.message || "Worker error"));
      };
      this.worker = worker;
      return worker;
    } catch {
      return null;
    }
  }

  protected registerJob(
    id: number,
    worker: Worker,
    signal: AbortSignal | undefined,
    reject: (err: Error) => void,
    extraCleanup?: () => void,
  ): void {
    const onAbort = () => {
      const job = this.pending.get(id);
      if (!job) return;
      this.pending.delete(id);
      job.cleanup();
      const cancelMsg: WorkerCancelMsg = { id, type: "cancel" };
      worker.postMessage(cancelMsg);
      reject(new DOMException("Aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    this.pending.set(id, {
      reject,
      cleanup: () => {
        signal?.removeEventListener("abort", onAbort);
        extraCleanup?.();
      },
    });
  }

  protected finishJob(id: number): boolean {
    const job = this.pending.get(id);
    if (!job) return false;
    this.pending.delete(id);
    job.cleanup();
    return true;
  }

  terminate(): void {
    this.failAll(new DOMException("Aborted", "AbortError"));
  }

  private failAll(err: Error): void {
    this.worker?.terminate();
    this.worker = null;
    const jobs = [...this.pending.values()];
    this.pending.clear();
    for (const job of jobs) {
      job.cleanup();
      job.reject(err);
    }
  }
}

/**
 * Shared Web Worker RPC host dispatcher that manages per-job `AbortController`
 * instances, handles `{ type: "cancel" }` messages, and suppresses late replies
 * after cancellation.
 */
export function createWorkerRpcHost<
  TMsg extends { id: number; type?: string },
>(
  handler: (msg: TMsg, ctx: WorkerHostContext) => Promise<void> | void,
): (event: MessageEvent<TMsg | WorkerCancelMsg>) => Promise<void> {
  const jobs = new Map<number, AbortController>();

  const post = (
    id: number,
    msg: Record<string, unknown>,
    transfer?: Transferable[],
  ) => {
    if (!jobs.has(id)) return;
    if (transfer && transfer.length > 0) {
      (self as unknown as Worker).postMessage(msg, transfer);
    } else {
      (self as unknown as Worker).postMessage(msg);
    }
  };

  return async (event: MessageEvent<TMsg | WorkerCancelMsg>) => {
    const msg = event.data;
    if (msg.type === "cancel") {
      jobs.get(msg.id)?.abort();
      jobs.delete(msg.id);
      return;
    }

    const controller = new AbortController();
    jobs.set(msg.id, controller);
    try {
      await handler(msg as TMsg, {
        signal: controller.signal,
        post: (reply, transfer) => post(msg.id, reply, transfer),
      });
    } catch (err) {
      if (!controller.signal.aborted && jobs.has(msg.id)) {
        const errMsg: WorkerErrorMsg = {
          id: msg.id,
          type: "error",
          message: err instanceof Error ? err.message : String(err),
        };
        (self as unknown as Worker).postMessage(errMsg);
      }
    } finally {
      jobs.delete(msg.id);
    }
  };
}
