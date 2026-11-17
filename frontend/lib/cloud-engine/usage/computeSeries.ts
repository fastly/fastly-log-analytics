/** One hour (or day) of Fastly-billed Compute on the app and worker services. */
export interface ComputePoint {
  time: string;
  app_requests: number;
  worker_requests: number;
  app_vcpu_ms: number;
  worker_vcpu_ms: number;
  billed_ms: number;
}

/** `/api/usage/compute`: Fastly Stats API Compute figures for the page range. */
export type ComputeUsageResponse =
  | {
      source: "fastly";
      requests: number;
      vcpu_ms: number;
      billed_ms: number;
      app_requests: number;
      worker_requests: number;
      app_vcpu_ms: number;
      worker_vcpu_ms: number;
      covered_from: string | null;
      covered_to: string | null;
      /** Hourly, only for hours the Stats API returned. */
      data: ComputePoint[];
    }
  | { source: "unavailable"; reason: string };

const DAY_MS = 86_400_000;

/** Sums hourly points into UTC days, the same day boundaries as the other usage charts. */
export function rollupComputeByDay(
  points: readonly ComputePoint[],
): ComputePoint[] {
  const days = new Map<number, ComputePoint>();
  for (const point of points) {
    const t = Math.floor(Date.parse(point.time) / DAY_MS) * DAY_MS;
    const day = days.get(t) ?? {
      time: new Date(t).toISOString(),
      app_requests: 0,
      worker_requests: 0,
      app_vcpu_ms: 0,
      worker_vcpu_ms: 0,
      billed_ms: 0,
    };
    day.app_requests += point.app_requests;
    day.worker_requests += point.worker_requests;
    day.app_vcpu_ms += point.app_vcpu_ms;
    day.worker_vcpu_ms += point.worker_vcpu_ms;
    day.billed_ms += point.billed_ms;
    days.set(t, day);
  }
  return [...days.entries()].sort(([a], [b]) => a - b).map(([, day]) => day);
}
