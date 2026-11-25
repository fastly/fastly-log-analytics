import { getPageLoadId } from "@/lib/api";
import { useDebugStore, type ApiCallEntry } from "@/stores/filterStore";
import { BaseWorkerRpcClient } from "../worker-rpc";
import { beginCloudLoading } from "../loading-progress";
import type { RunProgress } from "../dashboard/engine";
import { dashboardCacheIdentity } from "../dashboard/sources";
import {
  normalizeOpsColumns,
  readIngestLifecycle as readIngestLifecycleInline,
  readLogs as readLogsInline,
  type IngestLifecycleResult,
  type LogResult,
  type LogsRange,
  type LogsResult,
  type OpsKind,
  type ReadIngestLifecycleOptions,
  type ReadLogsOptions,
} from "./engine";
import {
  activeOpsUser,
  browserOpsSource,
  currentOpsCacheEpoch,
  currentOpsIdentity,
  type OpsIdentity,
} from "./sources";

type WorkerRequest =
  | {
      id: number;
      op?: "logs";
      options: Omit<ReadLogsOptions, "signal" | "onProgress">;
      pageLoadId: string | null;
      identity: OpsIdentity;
      reportApiCalls?: boolean;
    }
  | {
      id: number;
      op: "ingestLifecycle";
      options: Omit<ReadIngestLifecycleOptions, "signal" | "onProgress">;
      pageLoadId: string | null;
      identity: OpsIdentity;
      reportApiCalls?: boolean;
    };

type WorkerMessage =
  | { id: number; type: "result"; result: LogResult | IngestLifecycleResult }
  | { id: number; type: "progress"; progress: RunProgress }
  | { id: number; type: "apiCall"; call: ApiCallEntry }
  | { id: number; type: "error"; message: string };

class OpsWorkerClient extends BaseWorkerRpcClient<
  WorkerRequest,
  WorkerMessage
> {
  private readonly resolvers = new Map<
    number,
    (result: LogResult | IngestLifecycleResult) => void
  >();
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
    if (msg.type === "apiCall") {
      const state = useDebugStore.getState();
      if (state.apiCallsEnabled) state.addApiCall(msg.call);
      return;
    }
    if (msg.type === "result") {
      const resolve = this.resolvers.get(msg.id);
      if (this.finishJob(msg.id)) {
        resolve?.(msg.result);
      }
    }
  };

  async run(options: ReadLogsOptions, identity: OpsIdentity): Promise<LogResult> {
    const loading = beginCloudLoading("listing");
    let success = false;
    try {
      const result = await this.runWithProgress(
        {
          ...options,
          onProgress: (progress) => {
            loading.update({ stage: "files", progress });
            options.onProgress?.(progress);
          },
        },
        identity,
      );
      success = true;
      return result;
    } finally {
      loading.finish(success);
    }
  }

  async runIngestLifecycle(
    options: ReadIngestLifecycleOptions,
    identity: OpsIdentity,
  ): Promise<IngestLifecycleResult> {
    const loading = beginCloudLoading("listing");
    let success = false;
    try {
      const result = await this.runIngestLifecycleWithProgress(
        {
          ...options,
          onProgress: (progress) => {
            loading.update({ stage: "files", progress });
            options.onProgress?.(progress);
          },
        },
        identity,
      );
      success = true;
      return result;
    } finally {
      loading.finish(success);
    }
  }

  private runWithProgress(
    options: ReadLogsOptions,
    identity: OpsIdentity,
  ): Promise<LogResult> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    const reportApiCalls = useDebugStore.getState().apiCallsEnabled;
    if (!worker) {
      return readLogsInline(
        browserOpsSource(
          typeof window !== "undefined" ? window.location.origin : "",
          identity,
          {
            getPageLoadId: () => getPageLoadId() ?? "",
            onApiCall: reportApiCalls
              ? (call) => {
                  const state = useDebugStore.getState();
                  if (state.apiCallsEnabled) state.addApiCall(call);
                }
              : undefined,
          },
        ),
        options,
      );
    }

    const id = this.allocateId();
    const { signal, onProgress, ...serializable } = options;
    return new Promise<LogResult>((resolve, reject) => {
      this.resolvers.set(id, resolve as (result: LogResult | IngestLifecycleResult) => void);
      if (onProgress) this.progressCallbacks.set(id, onProgress);
      this.registerJob(id, worker, signal, reject, () => {
        this.resolvers.delete(id);
        this.progressCallbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        options: serializable,
        pageLoadId: getPageLoadId(),
        identity,
        reportApiCalls,
      };
      worker.postMessage(req);
    });
  }

  private runIngestLifecycleWithProgress(
    options: ReadIngestLifecycleOptions,
    identity: OpsIdentity,
  ): Promise<IngestLifecycleResult> {
    if (options.signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    const worker = this.getWorker(this.handleMessage);
    const reportApiCalls = useDebugStore.getState().apiCallsEnabled;
    if (!worker) {
      return readIngestLifecycleInline(
        browserOpsSource(
          typeof window !== "undefined" ? window.location.origin : "",
          identity,
          {
            getPageLoadId: () => getPageLoadId() ?? "",
            onApiCall: reportApiCalls
              ? (call) => {
                  const state = useDebugStore.getState();
                  if (state.apiCallsEnabled) state.addApiCall(call);
                }
              : undefined,
          },
        ),
        options,
      );
    }

    const id = this.allocateId();
    const { signal, onProgress, ...serializable } = options;
    return new Promise<IngestLifecycleResult>((resolve, reject) => {
      this.resolvers.set(
        id,
        resolve as (result: LogResult | IngestLifecycleResult) => void,
      );
      if (onProgress) this.progressCallbacks.set(id, onProgress);
      this.registerJob(id, worker, signal, reject, () => {
        this.resolvers.delete(id);
        this.progressCallbacks.delete(id);
      });
      const req: WorkerRequest = {
        id,
        op: "ingestLifecycle",
        options: serializable,
        pageLoadId: getPageLoadId(),
        identity,
        reportApiCalls,
      };
      worker.postMessage(req);
    });
  }
}

let rpcClient: OpsWorkerClient | null = null;
const getClient = () => (rpcClient ??= new OpsWorkerClient());

const OPS_READ_TTL_MS = 15_000;
const MAX_SETTLED_OPS_READS = 32;

interface Waiter {
  onProgress?: (progress: RunProgress) => void;
}

interface InFlightOpsRead {
  scopeKey: string;
  from: string;
  to: string;
  fromMs: number;
  toMs: number;
  columns: ReadonlySet<string> | null;
  dedupe: boolean;
  sort: boolean;
  startEpoch: number;
  controller: AbortController;
  waiters: Set<Waiter>;
  promise: Promise<LogsResult>;
}

interface SettledOpsRead {
  scopeKey: string;
  from: string;
  to: string;
  fromMs: number;
  toMs: number;
  columns: ReadonlySet<string> | null;
  dedupe: boolean;
  sort: boolean;
  at: number;
  result: LogsResult;
}

function coversReadOptions(
  entry: { columns: ReadonlySet<string> | null; dedupe: boolean; sort: boolean },
  reqColumns: readonly string[] | null,
  reqDedupe: boolean,
  reqSort: boolean,
): boolean {
  if (reqDedupe && !entry.dedupe) return false;
  if (reqSort && !entry.sort) return false;
  if (entry.columns === null) return true;
  if (reqColumns === null) return false;
  for (const col of reqColumns) {
    if (!entry.columns.has(col)) return false;
  }
  return true;
}

const inFlightOpsReads = new Set<InFlightOpsRead>();
const settledOpsReads = new Map<string, SettledOpsRead>();

interface InFlightIngestLifecycle {
  key: string;
  controller: AbortController;
  waiters: Set<Waiter>;
  promise: Promise<IngestLifecycleResult>;
}

interface SettledIngestLifecycle {
  at: number;
  result: IngestLifecycleResult;
}

const inFlightIngestLifecycle = new Map<string, InFlightIngestLifecycle>();
const settledIngestLifecycle = new Map<string, SettledIngestLifecycle>();

export function resetOpsReadCache(): void {
  settledOpsReads.clear();
  settledIngestLifecycle.clear();
}

export function resetOpsLogsClient(): void {
  rpcClient?.terminate();
  rpcClient = null;
  inFlightOpsReads.clear();
  settledOpsReads.clear();
  inFlightIngestLifecycle.clear();
  settledIngestLifecycle.clear();
}

function abortError(): DOMException {
  return new DOMException("Aborted", "AbortError");
}

function sliceLogsResult(
  result: LogsResult,
  from: string,
  to: string,
  fromMs: number,
  toMs: number,
): LogsResult {
  if (result.from === from && result.to === to) return result;
  if (!Array.isArray(result.rows) || !Number.isFinite(fromMs) || !Number.isFinite(toMs)) {
    return result;
  }
  return {
    ...result,
    from,
    to,
    rows: result.rows.filter((row) => {
      const ts = Number(row.timestamp);
      return Number.isFinite(ts) ? ts >= fromMs && ts < toMs : true;
    }),
  };
}

function pruneSettledReads(now: number): void {
  for (const [key, entry] of settledOpsReads) {
    if (now - entry.at >= OPS_READ_TTL_MS) settledOpsReads.delete(key);
  }
}

function findCoveringSettled(
  scopeKey: string,
  from: string,
  to: string,
  fromMs: number,
  toMs: number,
  columns: readonly string[] | null,
  columnsKey: string,
  dedupe: boolean,
  sort: boolean,
): SettledOpsRead | null {
  const now = Date.now();
  pruneSettledReads(now);
  const exact = settledOpsReads.get(
    `${scopeKey}|${from}|${to}|${columnsKey}|${dedupe ? 1 : 0}|${sort ? 1 : 0}`,
  );
  if (exact) return exact;
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs < fromMs) {
    return null;
  }
  let best: SettledOpsRead | null = null;
  let bestSpan = Number.POSITIVE_INFINITY;
  for (const entry of settledOpsReads.values()) {
    if (entry.scopeKey !== scopeKey) continue;
    if (!coversReadOptions(entry, columns, dedupe, sort)) continue;
    if (!Number.isFinite(entry.fromMs) || !Number.isFinite(entry.toMs)) continue;
    if (entry.fromMs <= fromMs && entry.toMs >= toMs) {
      const span = entry.toMs - entry.fromMs;
      if (span < bestSpan) {
        best = entry;
        bestSpan = span;
      }
    }
  }
  return best;
}

function findCoveringInFlight(
  scopeKey: string,
  from: string,
  to: string,
  fromMs: number,
  toMs: number,
  columns: readonly string[] | null,
  dedupe: boolean,
  sort: boolean,
): InFlightOpsRead | null {
  let best: InFlightOpsRead | null = null;
  let bestSpan = Number.POSITIVE_INFINITY;
  for (const entry of inFlightOpsReads) {
    if (entry.scopeKey !== scopeKey) continue;
    if (!coversReadOptions(entry, columns, dedupe, sort)) continue;
    if (entry.from === from && entry.to === to) return entry;
    if (
      Number.isFinite(fromMs) &&
      Number.isFinite(toMs) &&
      toMs >= fromMs &&
      Number.isFinite(entry.fromMs) &&
      Number.isFinite(entry.toMs) &&
      entry.fromMs <= fromMs &&
      entry.toMs >= toMs
    ) {
      const span = entry.toMs - entry.fromMs;
      if (span < bestSpan) {
        best = entry;
        bestSpan = span;
      }
    }
  }
  return best;
}

function attachToInFlight(
  entry: InFlightOpsRead,
  signal: AbortSignal | undefined,
  onProgress: ((progress: RunProgress) => void) | undefined,
): Promise<LogsResult> {
  if (signal?.aborted) return Promise.reject(abortError());
  const waiter: Waiter = { onProgress };
  entry.waiters.add(waiter);
  return new Promise<LogsResult>((resolve, reject) => {
    const cleanup = () => {
      entry.waiters.delete(waiter);
      signal?.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      cleanup();
      if (entry.waiters.size === 0) {
        inFlightOpsReads.delete(entry);
        entry.controller.abort(signal?.reason ?? abortError());
      }
      reject(abortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    entry.promise.then(
      (res) => {
        cleanup();
        resolve(res);
      },
      (err) => {
        cleanup();
        reject(err);
      },
    );
  });
}

function startOpsRead(
  kind: OpsKind,
  from: string,
  to: string,
  fromMs: number,
  toMs: number,
  scopeKey: string,
  startEpoch: number,
  identity: OpsIdentity,
  signal: AbortSignal | undefined,
  onProgress: ((progress: RunProgress) => void) | undefined,
  latestCompleteSnapshotOnly: boolean | undefined,
  columns: string[] | null,
  columnsKey: string,
  rangeProject: boolean,
  dedupe: boolean,
  sort: boolean,
): Promise<LogsResult> {
  const controller = new AbortController();
  const waiters = new Set<Waiter>();
  const columnSet = columns ? new Set(columns) : null;
  const entry: InFlightOpsRead = {
    scopeKey,
    from,
    to,
    fromMs,
    toMs,
    columns: columnSet,
    dedupe,
    sort,
    startEpoch,
    controller,
    waiters,
    promise: null as unknown as Promise<LogsResult>,
  };
  entry.promise = getClient()
    .run(
      {
        kind,
        from,
        to,
        signal: controller.signal,
        ...(latestCompleteSnapshotOnly
          ? { latestCompleteSnapshotOnly: true }
          : {}),
        ...(columns ? { columns } : {}),
        ...(rangeProject ? {} : { rangeProject: false }),
        ...(dedupe ? {} : { dedupe: false }),
        ...(sort ? {} : { sort: false }),
        onProgress: (progress) => {
          for (const waiter of waiters) {
            waiter.onProgress?.(progress);
          }
        },
      },
      identity,
    )
    .then((res) => {
      if (
        startEpoch === currentOpsCacheEpoch() &&
        res.liveTruncated !== true &&
        Number.isFinite(fromMs) &&
        Number.isFinite(toMs)
      ) {
        const settledKey = `${scopeKey}|${from}|${to}|${columnsKey}|${dedupe ? 1 : 0}|${sort ? 1 : 0}`;
        settledOpsReads.delete(settledKey);
        if (settledOpsReads.size >= MAX_SETTLED_OPS_READS) {
          const oldest = settledOpsReads.keys().next().value;
          if (oldest !== undefined) settledOpsReads.delete(oldest);
        }
        settledOpsReads.set(settledKey, {
          scopeKey,
          from,
          to,
          fromMs,
          toMs,
          columns: columnSet,
          dedupe,
          sort,
          at: Date.now(),
          result: res,
        });
      }
      return res;
    })
    .finally(() => {
      inFlightOpsReads.delete(entry);
    });
  inFlightOpsReads.add(entry);
  return attachToInFlight(entry, signal, onProgress);
}

// Several panels often ask for the same kind and overlapping ranges at once
// (e.g. 30d storage billing, 7d prefill, 24h summary, 1h last-sync); share one
// read when an in-flight or recently settled read covers the requested window.
async function sharedRead(
  {
    kind,
    from,
    to,
    signal,
    onProgress,
    latestCompleteSnapshotOnly,
    columns,
    rangeProject: rawRangeProject,
    dedupe: rawDedupe,
    sort: rawSort,
  }: ReadLogsOptions,
  scope: { user?: string; role?: string } = {},
): Promise<LogsResult> {
  if (signal?.aborted) throw abortError();
  const identity = currentOpsIdentity(scope);
  const startEpoch = currentOpsCacheEpoch();
  const rangeProject = rawRangeProject !== false;
  const dedupe = rawDedupe !== false;
  const sort = rawSort !== false;
  const normalizedColumns = normalizeOpsColumns(columns, dedupe);
  const columnsKey = normalizedColumns ? normalizedColumns.join(",") : "*";
  const scopeKey = JSON.stringify([
    kind,
    identity.user,
    identity.role,
    activeOpsUser(),
    dashboardCacheIdentity(),
    getPageLoadId(),
    startEpoch,
    Boolean(latestCompleteSnapshotOnly),
  ]);
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);

  const settled = findCoveringSettled(
    scopeKey,
    from,
    to,
    fromMs,
    toMs,
    normalizedColumns,
    columnsKey,
    dedupe,
    sort,
  );
  if (settled) {
    return settled.from === from && settled.to === to
      ? settled.result
      : sliceLogsResult(settled.result, from, to, fromMs, toMs);
  }

  const inflight = findCoveringInFlight(
    scopeKey,
    from,
    to,
    fromMs,
    toMs,
    normalizedColumns,
    dedupe,
    sort,
  );
  if (inflight) {
    const res = await attachToInFlight(inflight, signal, onProgress);
    const isStrictSubset = inflight.from !== from || inflight.to !== to;
    if (isStrictSubset && res.liveTruncated === true) {
      return startOpsRead(
        kind,
        from,
        to,
        fromMs,
        toMs,
        scopeKey,
        startEpoch,
        identity,
        signal,
        onProgress,
        latestCompleteSnapshotOnly,
        normalizedColumns,
        columnsKey,
        rangeProject,
        dedupe,
        sort,
      );
    }
    return isStrictSubset
      ? sliceLogsResult(res, from, to, fromMs, toMs)
      : res;
  }

  return startOpsRead(
    kind,
    from,
    to,
    fromMs,
    toMs,
    scopeKey,
    startEpoch,
    identity,
    signal,
    onProgress,
    latestCompleteSnapshotOnly,
    normalizedColumns,
    columnsKey,
    rangeProject,
    dedupe,
    sort,
  );
}

export function readLogs(options: ReadLogsOptions): Promise<LogsResult>;
export function readLogs(
  kind: OpsKind,
  range: LogsRange,
  options?: {
    user?: string;
    role?: string;
    signal?: AbortSignal;
    latestCompleteSnapshotOnly?: boolean;
    columns?: readonly string[];
    rangeProject?: boolean;
    dedupe?: boolean;
    sort?: boolean;
  },
): Promise<LogsResult>;
export function readLogs(
  kindOrOptions: OpsKind | ReadLogsOptions,
  range?: LogsRange,
  options?: {
    user?: string;
    role?: string;
    signal?: AbortSignal;
    latestCompleteSnapshotOnly?: boolean;
    columns?: readonly string[];
    rangeProject?: boolean;
    dedupe?: boolean;
    sort?: boolean;
  },
): Promise<LogsResult> {
  if (typeof kindOrOptions === "string") {
    return sharedRead(
      {
        kind: kindOrOptions,
        from: range!.from,
        to: range!.to,
        signal: options?.signal,
        latestCompleteSnapshotOnly: options?.latestCompleteSnapshotOnly,
        columns: options?.columns,
        rangeProject: options?.rangeProject,
        dedupe: options?.dedupe,
        sort: options?.sort,
      },
      { user: options?.user, role: options?.role },
    );
  }
  return sharedRead(kindOrOptions);
}

export const queryOpsLogs = readLogs;

export function readIngestLifecycle(
  range: LogsRange,
  options?: {
    user?: string;
    role?: string;
    minimumBilledDays?: number;
    signal?: AbortSignal;
    onProgress?: (progress: RunProgress) => void;
  },
): Promise<IngestLifecycleResult> {
  const signal = options?.signal;
  if (signal?.aborted) return Promise.reject(abortError());
  const identity = currentOpsIdentity({
    user: options?.user,
    role: options?.role,
  });
  const minimumBilledDays = options?.minimumBilledDays ?? 30;
  const startEpoch = currentOpsCacheEpoch();
  const key = JSON.stringify([
    "ingestLifecycle",
    identity.user,
    identity.role,
    activeOpsUser(),
    dashboardCacheIdentity(),
    getPageLoadId(),
    startEpoch,
    range.from,
    range.to,
    minimumBilledDays,
  ]);
  const now = Date.now();
  for (const [k, entry] of settledIngestLifecycle) {
    if (now - entry.at >= OPS_READ_TTL_MS) settledIngestLifecycle.delete(k);
  }
  const settled = settledIngestLifecycle.get(key);
  if (settled) return Promise.resolve(settled.result);

  let entry = inFlightIngestLifecycle.get(key);
  if (!entry) {
    const controller = new AbortController();
    const waiters = new Set<Waiter>();
    entry = {
      key,
      controller,
      waiters,
      promise: null as unknown as Promise<IngestLifecycleResult>,
    };
    const active = entry;
    active.promise = getClient()
      .runIngestLifecycle(
        {
          from: range.from,
          to: range.to,
          minimumBilledDays,
          signal: controller.signal,
          onProgress: (progress) => {
            for (const waiter of waiters) {
              waiter.onProgress?.(progress);
            }
          },
        },
        identity,
      )
      .then((res) => {
        if (
          startEpoch === currentOpsCacheEpoch() &&
          res.liveTruncated !== true
        ) {
          settledIngestLifecycle.delete(key);
          if (settledIngestLifecycle.size >= MAX_SETTLED_OPS_READS) {
            const oldest = settledIngestLifecycle.keys().next().value;
            if (oldest !== undefined) settledIngestLifecycle.delete(oldest);
          }
          settledIngestLifecycle.set(key, { at: Date.now(), result: res });
        }
        return res;
      })
      .finally(() => {
        if (inFlightIngestLifecycle.get(key) === active) {
          inFlightIngestLifecycle.delete(key);
        }
      });
    inFlightIngestLifecycle.set(key, active);
  }

  const waiter: Waiter = { onProgress: options?.onProgress };
  entry.waiters.add(waiter);
  const activeEntry = entry;
  return new Promise<IngestLifecycleResult>((resolve, reject) => {
    const cleanup = () => {
      activeEntry.waiters.delete(waiter);
      signal?.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      cleanup();
      if (activeEntry.waiters.size === 0) {
        if (inFlightIngestLifecycle.get(key) === activeEntry) {
          inFlightIngestLifecycle.delete(key);
        }
        activeEntry.controller.abort(signal?.reason ?? abortError());
      }
      reject(abortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    activeEntry.promise.then(
      (res) => {
        cleanup();
        resolve(res);
      },
      (err) => {
        cleanup();
        reject(err);
      },
    );
  });
}

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
