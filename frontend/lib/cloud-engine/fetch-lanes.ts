/**
 * Which reads go first:
 * - `summary`: small summary/bundle reads that answer whole sections.
 * - `raw`: small raw files (`<= LARGE_WHOLE_MIN_BYTES`) and HTTP byte ranges.
 * - `large`: multi-MiB whole-file downloads (`> LARGE_WHOLE_MIN_BYTES`), capped so
 *   concurrent transfers don't saturate HTTP/2 or starve tiny summary files.
 */
export type FetchLane = "summary" | "raw" | "large";

/** Whole-file downloads strictly above this size use the bandwidth-capped `"large"` lane. */
export const LARGE_WHOLE_MIN_BYTES = 2 * 1024 * 1024;

export function rawLaneForBytes(bytes: number): "raw" | "large" {
  return bytes > LARGE_WHOLE_MIN_BYTES ? "large" : "raw";
}

export type FetchLanes = {
  run<T>(lane: FetchLane, task: () => Promise<T>, signal?: AbortSignal): Promise<T>;
};

/**
 * Orders reads within one realm (page or worker). Reads queued in the same tick
 * start summary-first, then small raw/range reads, then multi-MiB whole-file
 * downloads. Summary reads run up to `summaryLimit` at once; while any summary
 * read is active or waiting, at most `rawWhileSummary` small raw reads and
 * `largeWhileSummary` multi-MiB whole-file reads run (default `0`, preventing
 * HTTP/2 head-of-line starvation of tiny summary files). Once summaries drain,
 * large whole-file reads remain capped at `largeLimit` so bandwidth contention
 * does not stretch per-file durations past the slow-API threshold.
 */
export function createFetchLanes({
  summaryLimit = 16,
  rawWhileSummary = 4,
  largeLimit = 2,
  largeWhileSummary = 0,
} = {}): FetchLanes {
  const waiting: Record<FetchLane, (() => void)[]> = { summary: [], raw: [], large: [] };
  const active: Record<FetchLane, number> = { summary: 0, raw: 0, large: 0 };
  let draining = false;

  const drain = () => {
    draining = false;
    while (waiting.summary.length && active.summary < summaryLimit) {
      active.summary++;
      waiting.summary.shift()!();
    }
    const summaryPending = active.summary + waiting.summary.length > 0;
    while (waiting.raw.length && (!summaryPending || active.raw < rawWhileSummary)) {
      active.raw++;
      waiting.raw.shift()!();
    }
    const currentLargeLimit = summaryPending ? largeWhileSummary : largeLimit;
    while (waiting.large.length && active.large < currentLargeLimit) {
      active.large++;
      waiting.large.shift()!();
    }
  };
  const schedule = () => {
    if (draining) return;
    draining = true;
    queueMicrotask(drain);
  };

  return {
    async run(lane, task, signal) {
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
      }
      await new Promise<void>((resolve, reject) => {
        let settled = false;
        const start = () => {
          if (settled) return;
          settled = true;
          signal?.removeEventListener("abort", onAbort);
          resolve();
        };
        const onAbort = () => {
          if (settled) return;
          settled = true;
          const idx = waiting[lane].indexOf(start);
          if (idx !== -1) waiting[lane].splice(idx, 1);
          schedule();
          reject(signal?.reason ?? new DOMException("The operation was aborted.", "AbortError"));
        };
        signal?.addEventListener("abort", onAbort, { once: true });
        waiting[lane].push(start);
        schedule();
      });
      try {
        return await task();
      } finally {
        active[lane]--;
        schedule();
      }
    },
  };
}

/** The lanes every range and whole-file read in this realm shares. */
export const fetchLanes = createFetchLanes();
