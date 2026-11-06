import {
  LIST_RATES,
  deriveFosUsageFromCronRuns,
  type UsageInput,
} from "./summary";

export type UsageBucketBy = "hour" | "day";

export interface UsagePoint {
  t: number;
  classA: number;
  classB: number;
  usd: number;
  kvClassA: number;
  kvClassB: number;
  kvUsd: number;
  bytesOut: number;
  requests: number;
  ingestedRows: number;
}

const STEP_MS: Record<UsageBucketBy, number> = {
  hour: 3_600_000,
  day: 86_400_000,
};

const floorTo = (ms: number, by: UsageBucketBy) =>
  Math.floor(ms / STEP_MS[by]) * STEP_MS[by];

export function bucketStarts(
  from: number,
  to: number,
  by: UsageBucketBy,
): number[] {
  const starts: number[] = [];
  for (let t = floorTo(from, by); t <= to; t += STEP_MS[by]) starts.push(t);
  return starts;
}

export function bucketUsage(
  rows: Pick<UsageInput["rows"], "fos_usage" | "api_telemetry"> &
    Partial<Pick<UsageInput["rows"], "cron_runs">>,
  range: { from: number; to: number; by: UsageBucketBy },
  cdnBandwidthByHour?: Array<{ t: number; bytes: number }>,
  kvByHour?: Array<{ t: number; classA: number; classB: number }>,
  kvRates: { classAPerMillion: number; classBPerMillion: number } = {
    classAPerMillion: LIST_RATES.kv_class_a_rate_per_million,
    classBPerMillion: LIST_RATES.kv_class_b_rate_per_million,
  },
): UsagePoint[] {
  const points = new Map<number, UsagePoint>(
    bucketStarts(range.from, range.to, range.by).map((t) => [
      t,
      {
        t,
        classA: 0,
        classB: 0,
        usd: 0,
        kvClassA: 0,
        kvClassB: 0,
        kvUsd: 0,
        bytesOut: 0,
        requests: 0,
        ingestedRows: 0,
      },
    ]),
  );
  const useCdnBw = Boolean(cdnBandwidthByHour && cdnBandwidthByHour.length > 0);
  const effectiveFosUsage =
    rows.fos_usage.length > 0 || !rows.cron_runs || rows.cron_runs.length === 0
      ? rows.fos_usage
      : deriveFosUsageFromCronRuns(rows.cron_runs);
  for (const row of effectiveFosUsage) {
    const point = points.get(floorTo(row.timestamp, range.by));
    if (!point || row.timestamp < range.from) continue;
    if (row.op_class === "A") point.classA += row.op_count;
    else if (row.op_class === "B") point.classB += row.op_count;
    point.usd += row.est_cost_usd;
    if (!useCdnBw) point.bytesOut += row.bytes_out;
  }
  if (useCdnBw && cdnBandwidthByHour) {
    for (const bucket of cdnBandwidthByHour) {
      if (bucket.t < range.from) continue;
      const point = points.get(floorTo(bucket.t, range.by));
      if (point) point.bytesOut += bucket.bytes;
    }
  }
  if (kvByHour) {
    for (const bucket of kvByHour) {
      if (bucket.t < range.from) continue;
      const point = points.get(floorTo(bucket.t, range.by));
      if (point) {
        point.kvClassA += bucket.classA;
        point.kvClassB += bucket.classB;
        point.kvUsd +=
          (bucket.classA / 1e6) * kvRates.classAPerMillion +
          (bucket.classB / 1e6) * kvRates.classBPerMillion;
      }
    }
  }
  for (const row of rows.api_telemetry) {
    if (row.source !== "compute_api" || row.timestamp < range.from) continue;
    const point = points.get(floorTo(row.timestamp, range.by));
    if (point) point.requests += row.files_fetched || 1;
  }
  const deriveRequestsFromCrons = rows.api_telemetry.length === 0;
  for (const run of rows.cron_runs ?? []) {
    if (run.started_at < range.from) continue;
    const point = points.get(floorTo(run.started_at, range.by));
    if (point) {
      point.ingestedRows += run.rows_written ?? 0;
      if (deriveRequestsFromCrons) {
        point.requests +=
          1 + (run.worker_executions ?? 0) + (run.idle_ticks ?? 0);
      }
    }
  }
  return [...points.values()];
}
