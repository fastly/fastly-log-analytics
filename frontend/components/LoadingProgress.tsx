"use client";

import { useSyncExternalStore } from "react";
import { Check, Loader2 } from "lucide-react";
import { formatBytes, formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { cloudLoadingStore, type LoadingStage } from "@/lib/cloud-engine/loading-progress";
import type { RunProgress } from "@/lib/cloud-engine/dashboard/engine";

const STAGES = {
  session: { title: "Checking your session", detail: "Loading your account, permissions, and app configuration.", step: 0 },
  range: { title: "Finding available logs", detail: "Checking the earliest and latest logs for this service.", step: 1 },
  listing: { title: "Checking log coverage", detail: "Finding a complete set of files for the available time ranges.", step: 2 },
  files: { title: "Preparing local analysis", detail: "Starting the query and checking reusable data in your browser.", step: 3 },
};

export function LoadingProgress({
  stage, progress = null, refreshing = false,
}: {
  stage: LoadingStage;
  progress?: RunProgress | null;
  refreshing?: boolean;
}) {
  const active = STAGES[stage];
  const work = progress ? progress.toFetch + progress.toDecode : 0;
  const reading = stage === "files" && work > 0;
  const aggregating = stage === "files" && progress?.phase === "aggregate";
  const title = aggregating ? "Preparing dashboard results"
    : reading ? "Reading and aggregating logs"
      : stage === "files" && progress
        ? progress.phase === "cache" ? "Checking cached data" : "Merging cached summaries"
        : active.title;
  const detail = aggregating ? "Finishing the totals, rankings, and chart data."
    : reading ? "Reusing local data and downloading only what's missing." : active.detail;
  const percent = reading && !aggregating
    ? Math.max(0, Math.min(100, Math.round((progress!.done / work) * 100))) : null;

  return (
    <section role="region" aria-label="Dashboard loading progress" className="fixed bottom-4 right-4 left-4 z-40 overflow-hidden rounded-xl border border-primary/20 bg-popover text-popover-foreground shadow-xl sm:bottom-6 sm:right-6 sm:left-auto sm:w-96">
      <div className="border-b border-border/60 bg-primary/5 px-5 py-3">
        <div className="flex items-center gap-2 text-xs font-medium">
          <Loader2 aria-hidden="true" className="size-3.5 text-primary motion-safe:animate-spin" />
          {refreshing ? "Updating dashboard" : "Loading dashboard"}
          <span className="ml-auto font-mono text-[10px] text-muted-foreground">{active.step + 1} / 4 stages</span>
        </div>
      </div>
      <div className="space-y-4 p-5">
        <div role="status" aria-live="polite" aria-atomic="true">
          <p className="text-sm font-semibold">{title}</p>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{detail}</p>
        </div>
        <ol aria-label="Loading stages" className="grid grid-cols-4 gap-2">
          {["Account", "Log range", "File list", "Analysis"].map((label, index) => (
            <li key={label} aria-current={index === active.step ? "step" : undefined} className={cn("border-t-2 pt-2 text-[10px]", index <= active.step ? "border-primary text-foreground" : "border-muted text-muted-foreground")}>
              <span className="flex items-center gap-1">
                {index < active.step && <Check className="size-3 text-primary" aria-hidden="true" />}
                {label}
              </span>
            </li>
          ))}
        </ol>
        {stage === "files" && progress && (
          <div className="space-y-2 text-xs text-muted-foreground tabular-nums">
            {percent !== null && (
              <>
                <div className="flex justify-between">
                  <span>{formatCount(progress.done)} of {formatCount(work)} files</span>
                  <span>{percent}%</span>
                </div>
                <div role="progressbar" aria-label="Log files processed" aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent} className="h-1.5 overflow-hidden rounded-full bg-muted">
                  <div className="h-full rounded-full bg-primary motion-safe:transition-[width]" style={{ width: `${percent}%` }} />
                </div>
              </>
            )}
            <p>{formatBytes(progress.downloadedBytes)} downloaded · {formatBytes(progress.cachedBytes)} from cache</p>
          </div>
        )}
        {refreshing && <p className="text-xs text-muted-foreground">Your previous results stay visible while this finishes.</p>}
      </div>
    </section>
  );
}

export function CloudLoadingProgress() {
  const state = useSyncExternalStore(cloudLoadingStore.subscribe, cloudLoadingStore.getSnapshot, cloudLoadingStore.getServerSnapshot);
  return state ? <LoadingProgress {...state} /> : null;
}
