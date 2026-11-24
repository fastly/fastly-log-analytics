import type { RunProgress } from "./dashboard/engine";
import { publishCloudLoading } from "./readiness";

export type LoadingStage = "session" | "range" | "listing" | "files";
export type LoadingState = {
  stage: LoadingStage;
  progress: RunProgress | null;
  refreshing: boolean;
};

const stages: LoadingStage[] = ["session", "range", "listing", "files"];
const jobs = new Map<symbol, LoadingState>();
const completedBatchProgress: RunProgress[] = [];
const listeners = new Set<() => void>();
let snapshot: LoadingState | null = null;
let hasResults = false;

function publish(): void {
  const active = [...jobs.values()];
  if (!active.length) {
    completedBatchProgress.length = 0;
    snapshot = null;
  } else {
    const activeProgress = active.flatMap((job) => job.progress ? [job.progress] : []);
    const progress = [...activeProgress, ...completedBatchProgress];
    // Once any job in the active batch is reading files, keep the popup on "files"
    // so late-arriving "listing" jobs don't flicker the card back to stage 3/4.
    const stage = progress.length > 0
      ? "files"
      : stages.find((stage) => active.some((job) => job.stage === stage))!;
    snapshot = {
      stage,
      refreshing: active.some((job) => job.refreshing),
      progress: progress.length ? {
        phase: progress.every((p) => p.phase === "aggregate") ? "aggregate"
          : progress.some((p) => p.phase === "files" || !p.phase) ? "files" : "cache",
        cached: progress.reduce((sum, p) => sum + p.cached, 0),
        toFetch: progress.reduce((sum, p) => sum + p.toFetch, 0),
        toDecode: progress.reduce((sum, p) => sum + p.toDecode, 0),
        done: progress.reduce((sum, p) => sum + p.done, 0),
        downloadedBytes: progress.reduce((sum, p) => sum + p.downloadedBytes, 0),
        cachedBytes: progress.reduce((sum, p) => sum + p.cachedBytes, 0),
        ...(progress.some((p) => p.rangedFiles !== undefined)
          ? { rangedFiles: progress.reduce((sum, p) => sum + (p.rangedFiles ?? 0), 0) } : {}),
      } : null,
    };
  }
  listeners.forEach((listener) => listener());
  publishCloudLoading({ stage: snapshot?.stage ?? null, jobs: active.length, progress: snapshot?.progress ?? null });
}

export const cloudLoadingStore = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  },
  getSnapshot: (): LoadingState | null => snapshot,
  getServerSnapshot: (): null => null,
  reset(): void {
    jobs.clear();
    completedBatchProgress.length = 0;
    hasResults = false;
    publish();
  },
};

export function beginCloudLoading(
  stage: LoadingStage,
  options: { refreshing?: boolean } = {},
): { update: (patch: Partial<LoadingState>) => void; finish: (success?: boolean) => void } {
  const id = Symbol();
  jobs.set(id, { stage, progress: null, refreshing: options.refreshing ?? hasResults });
  publish();
  return {
    update(patch) {
      const current = jobs.get(id);
      if (!current) return;
      jobs.set(id, { ...current, ...patch });
      publish();
    },
    finish(success = true) {
      const current = jobs.get(id);
      if (!current) return;
      if (success && current.stage === "files") hasResults = true;
      jobs.delete(id);
      // Retain completed file totals while sibling jobs remain active so counts never drop backwards
      // and waiting analyses transition smoothly to aggregation.
      if (success && current.stage === "files" && current.progress && jobs.size > 0) {
        completedBatchProgress.push({ ...current.progress, phase: "aggregate" });
      }
      publish();
    },
  };
}
