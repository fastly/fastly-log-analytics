export type ServerTimingEntry = { name: string; dur?: number; desc?: string };

/** Parses a `Server-Timing` header value into its metrics, in header order. */
export function parseServerTiming(
  value: string | null | undefined,
): ServerTimingEntry[] {
  if (!value) return [];
  const entries: ServerTimingEntry[] = [];
  for (const metric of value.split(",")) {
    const [rawName, ...params] = metric.split(";");
    const name = rawName.trim();
    if (!name) continue;
    const entry: ServerTimingEntry = { name };
    for (const param of params) {
      const eq = param.indexOf("=");
      if (eq < 0) continue;
      const key = param.slice(0, eq).trim().toLowerCase();
      const raw = param
        .slice(eq + 1)
        .trim()
        .replace(/^"(.*)"$/, "$1");
      if (key === "dur") {
        const dur = Number(raw);
        if (raw !== "" && Number.isFinite(dur)) entry.dur = dur;
      } else if (key === "desc") {
        entry.desc = raw;
      }
    }
    entries.push(entry);
  }
  return entries;
}

export type TimelineCall = {
  service: string;
  time_ms: number;
  /** Epoch milliseconds, comparable across the page and its workers. */
  start_ms?: number | null;
};

export type CallTimeline = {
  /** Sum of per-call durations; overstates elapsed time when calls overlap. */
  summedMs: number;
  /** First start to last end. */
  wallMs: number | null;
  /** Time with at least one call in flight. */
  busyMs: number | null;
  /** The longest chain of calls that ran one after another. */
  criticalPath: { calls: number; ms: number } | null;
  bySource: { source: string; count: number; ms: number }[];
};

// Starts and durations are rounded to whole milliseconds.
const SEQUENCE_SLACK_MS = 1;

export function summarizeCalls(calls: readonly TimelineCall[]): CallTimeline {
  const bySource = new Map<string, { count: number; ms: number }>();
  let summedMs = 0;
  for (const call of calls) {
    summedMs += call.time_ms;
    const source = bySource.get(call.service) ?? { count: 0, ms: 0 };
    source.count++;
    source.ms += call.time_ms;
    bySource.set(call.service, source);
  }
  const spans = calls
    .filter(
      (call): call is TimelineCall & { start_ms: number } =>
        typeof call.start_ms === "number" && Number.isFinite(call.start_ms),
    )
    .map((call) => ({
      start: call.start_ms,
      end: call.start_ms + Math.max(0, call.time_ms),
    }))
    .sort((a, b) => a.end - b.end);
  let timeline: Pick<CallTimeline, "wallMs" | "busyMs" | "criticalPath"> = {
    wallMs: null,
    busyMs: null,
    criticalPath: null,
  };
  if (spans.length > 0) {
    const byStart = [...spans].sort((a, b) => a.start - b.start);
    let busyMs = 0;
    let runStart = byStart[0].start;
    let runEnd = byStart[0].end;
    for (const span of byStart.slice(1)) {
      if (span.start > runEnd) {
        busyMs += runEnd - runStart;
        runStart = span.start;
      }
      runEnd = Math.max(runEnd, span.end);
    }
    busyMs += runEnd - runStart;
    // Weighted interval scheduling over spans sorted by end: best[i] is the
    // longest chain ending at or before spans[i].
    const best: { ms: number; calls: number }[] = [];
    for (let i = 0; i < spans.length; i++) {
      let lo = 0;
      let hi = i;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (spans[mid].end <= spans[i].start + SEQUENCE_SLACK_MS) lo = mid + 1;
        else hi = mid;
      }
      const prior = lo > 0 ? best[lo - 1] : { ms: 0, calls: 0 };
      const take = {
        ms: prior.ms + spans[i].end - spans[i].start,
        calls: prior.calls + 1,
      };
      const skip = i > 0 ? best[i - 1] : { ms: 0, calls: 0 };
      best.push(take.ms >= skip.ms ? take : skip);
    }
    timeline = {
      wallMs: Math.max(...spans.map((s) => s.end)) - byStart[0].start,
      busyMs,
      criticalPath: best[best.length - 1],
    };
  }
  return {
    summedMs,
    ...timeline,
    bySource: [...bySource].map(([source, { count, ms }]) => ({
      source,
      count,
      ms,
    })),
  };
}
