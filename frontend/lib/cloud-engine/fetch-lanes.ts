/** Which reads go first: summary projections answer whole sections, raw ranges only fill in. */
export type FetchLane = "summary" | "raw";

export type FetchLanes = {
  run<T>(lane: FetchLane, task: () => Promise<T>): Promise<T>;
};

/**
 * Orders range reads within one realm (page or worker). Reads queued in the
 * same tick start summary-first. Summary reads run up to `summaryLimit` at
 * once; raw reads are uncapped here (callers keep their own limit) except
 * that, while any summary read is running or waiting, at most
 * `rawWhileSummary` raw reads run, which leaves the rest of the browser's
 * connections to the summaries.
 */
export function createFetchLanes({ summaryLimit = 16, rawWhileSummary = 10 } = {}): FetchLanes {
  const waiting: Record<FetchLane, (() => void)[]> = { summary: [], raw: [] };
  const active: Record<FetchLane, number> = { summary: 0, raw: 0 };
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
  };
  const schedule = () => {
    if (draining) return;
    draining = true;
    queueMicrotask(drain);
  };

  return {
    async run(lane, task) {
      await new Promise<void>((resolve) => {
        waiting[lane].push(resolve);
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

/** The lanes every range read in this realm shares. */
export const fetchLanes = createFetchLanes();
