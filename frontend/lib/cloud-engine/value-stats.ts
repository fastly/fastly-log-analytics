import type { components } from "@/types/api.generated";
import { dashboardCacheIdentity } from "./dashboard/sources";
import { roundTo } from "./engine-utils";
import { adminRequest } from "./telemetry/admin";
import { activeOpsUser } from "./telemetry/sources";
import type { paths } from "./cloud-api.generated";

export type ServiceStats =
  paths["/api/admin/usage/service-stats"]["get"]["responses"][200]["content"]["application/json"];
/** One logged UTC hour from the summaries: its start (Unix ms), requests and response bytes. */
export type LoggedHour = [hourMs: number, requests: number, bytes: number];
export type FastlyReported = components["schemas"]["FastlyReportedMetrics"];

const HOUR_MS = 3_600_000;
/** The Compute route's span limit, as for the compute billing read. */
const MAX_SPAN_MS = 31 * 86_400_000;
/** How long a Stats answer (or its failure) is reused; Fastly reports an hour about 15 minutes after it ends. */
const CACHE_TTL_MS = 300_000;
/** How long a summaries answer waits on a slower Stats read before it goes without the comparison. */
const STATS_GRACE_MS = 5_000;

let statsCacheEpoch = 0;
const cache = new Map<string, { at: number; stats: Promise<ServiceStats | null> }>();

export function clearServiceStatsCache(): void {
  statsCacheEpoch += 1;
  cache.clear();
}

const isoZ = (ms: number) => new Date(ms).toISOString().replace(".000Z", "Z");

/**
 * Fastly Platform Metrics hours for the logged service over the whole UTC hours of
 * the range, or null when it can't be read: an analyst (the route is
 * admin-only), no configured service, a Stats error or a range Stats doesn't
 * cover. Answers and failures are reused per range for a few minutes.
 */
function fetchServiceStats(
  fromMs: number,
  toMs: number,
  serviceId?: string,
): Promise<ServiceStats | null> | null {
  const from = Math.ceil(fromMs / HOUR_MS) * HOUR_MS;
  const to = Math.floor(Math.min(toMs, Date.now()) / HOUR_MS) * HOUR_MS;
  if (!(to > from) || to - from > MAX_SPAN_MS) return null;
  const startEpoch = statsCacheEpoch;
  const key = JSON.stringify([
    startEpoch,
    dashboardCacheIdentity(),
    activeOpsUser(),
    serviceId ?? "",
    from,
    to,
  ]);
  const now = Date.now();
  for (const [k, entry] of cache) if (now - entry.at > CACHE_TTL_MS) cache.delete(k);
  let entry = cache.get(key);
  if (!entry) {
    const stats = adminRequest<ServiceStats>(`/api/admin/usage/service-stats?from=${from}&to=${to}`)
      .then((res) => (startEpoch === statsCacheEpoch ? res : null))
      .catch(() => null);
    entry = { at: now, stats };
    if (startEpoch === statsCacheEpoch) {
      cache.set(key, entry);
    }
  }
  return entry.stats;
}

/**
 * Fastly-reported totals over the whole UTC hours from the range start through
 * the last hour Fastly reported before the logs' coverage ends, with the logged
 * totals over the same hours; the same shape the High-Scale backend returns.
 * Null without a reported hour in that window, or for another service.
 */
export function fastlyReportedFromStats(
  stats: ServiceStats,
  logged: readonly LoggedHour[],
  fromMs: number,
  throughMs: number,
  serviceId?: string,
): FastlyReported | null {
  if (!Array.isArray(stats?.hours) || (serviceId && serviceId !== stats.service_id)) return null;
  const start = Math.ceil(fromMs / HOUR_MS) * HOUR_MS;
  const limit = Math.floor(throughMs / HOUR_MS) * HOUR_MS;
  let requests = 0;
  let bandwidth = 0;
  let hits = 0;
  let last: number | null = null;
  for (const hour of stats.hours) {
    const at = hour.start_time * 1000;
    if (at < start || at + HOUR_MS > limit) continue;
    last = Math.max(last ?? at, at);
    requests += hour.requests;
    bandwidth += hour.bandwidth_bytes;
    hits += hour.hits;
  }
  if (last === null) return null;
  const end = last + HOUR_MS;
  let loggedRequests = 0;
  let loggedBytes = 0;
  for (const [at, n, bytes] of logged) {
    if (at < start || at >= end) continue;
    loggedRequests += n;
    loggedBytes += bytes;
  }
  return {
    window_start: isoZ(start),
    window_end: isoZ(end),
    requests,
    bandwidth_bytes: bandwidth,
    hit_requests: hits,
    origin_offload_pct: requests ? roundTo(hits * 100 / requests, 2) : null,
    sample_rate: stats.sample_rate,
    edge_only: stats.edge_only,
    logged_requests: loggedRequests,
    logged_bandwidth_bytes: loggedBytes,
  };
}

type RollupAnswer = { overview?: unknown; rollup?: { data_through?: string; logged_hours?: LoggedHour[] } };

/**
 * Starts the Stats read for an unfiltered /fastly-value overview alongside its
 * summaries, and returns how to finish their answer: without the logged hours
 * the summaries carry for it, and with `fastly_reported` when Stats answered
 * in time.
 */
export function valueStatsDecorator(start: string, end: string, params: Record<string, unknown>) {
  const fromMs = Date.parse(start);
  const sections = Array.isArray(params.sections) ? params.sections : [];
  const serviceId = typeof params.service_id === "string" ? params.service_id : undefined;
  const stats = sections.includes("overview")
    ? fetchServiceStats(fromMs, Date.parse(end), serviceId)
    : null;
  return async (answer: unknown): Promise<unknown> => {
    const { rollup, ...rest } = answer as RollupAnswer;
    if (!rollup?.logged_hours) return answer;
    const { logged_hours: logged, ...meta } = rollup;
    const out = { ...rest, rollup: meta };
    if (!stats || !rest.overview || !meta.data_through) return out;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const got = await Promise.race([
      stats,
      new Promise<null>((resolve) => { timer = setTimeout(() => resolve(null), STATS_GRACE_MS); }),
    ]);
    clearTimeout(timer);
    const reported = got && fastlyReportedFromStats(got, logged, fromMs, Date.parse(meta.data_through), serviceId);
    return reported ? { ...out, fastly_reported: reported } : out;
  };
}
