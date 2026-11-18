// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearServiceStatsCache,
  fastlyReportedFromStats,
  valueStatsDecorator,
  type LoggedHour,
  type ServiceStats,
} from "./value-stats";

const HOUR = 3_600_000;
const H0 = Date.UTC(2026, 10, 5, 10);

const stats = (hours: [number, number, number, number][], extra: Partial<ServiceStats> = {}): ServiceStats => ({
  service_id: "svc-1", from: H0 / 1000, to: (H0 + 6 * HOUR) / 1000, sample_rate: 100, edge_only: false,
  hours: hours.map(([at, requests, bandwidth_bytes, hits]) => ({ start_time: at / 1000, requests, bandwidth_bytes, hits })),
  ...extra,
});

const logged: LoggedHour[] = [
  [H0 - HOUR, 7, 70],
  [H0, 90, 900],
  [H0 + HOUR, 95, 950],
  [H0 + 2 * HOUR, 50, 500],
];

describe("fastlyReportedFromStats", () => {
  it("compares the whole hours Fastly reported, up to the logs' coverage, with the logs over the same hours", () => {
    const got = fastlyReportedFromStats(
      stats([[H0, 100, 1000, 80], [H0 + HOUR, 100, 1000, 90], [H0 + 2 * HOUR, 100, 1000, 90]]),
      logged, H0 - 30 * 60_000, H0 + 2 * HOUR + 59 * 60_000,
    );
    expect(got).toEqual({
      window_start: "2026-11-05T10:00:00Z",
      window_end: "2026-11-05T12:00:00Z",
      requests: 200,
      bandwidth_bytes: 2000,
      hit_requests: 170,
      origin_offload_pct: 85,
      sample_rate: 100,
      edge_only: false,
      logged_requests: 185,
      logged_bandwidth_bytes: 1850,
    });
  });

  it("is null without a reported hour in the window or for another service", () => {
    expect(fastlyReportedFromStats(stats([]), logged, H0, H0 + 3 * HOUR)).toBeNull();
    expect(fastlyReportedFromStats(stats([[H0 + 2 * HOUR, 1, 1, 1]]), logged, H0, H0 + 2 * HOUR)).toBeNull();
    expect(fastlyReportedFromStats(stats([[H0, 1, 1, 1]]), logged, H0, H0 + HOUR, "svc-2")).toBeNull();
    expect(fastlyReportedFromStats({ latest_log_at: "x" } as never, logged, H0, H0 + HOUR)).toBeNull();
  });

  it("keeps the sampling settings and leaves offload unknown without requests", () => {
    const got = fastlyReportedFromStats(stats([[H0, 0, 0, 0]], { sample_rate: 10, edge_only: true }), logged, H0, H0 + HOUR);
    expect(got).toMatchObject({ requests: 0, origin_offload_pct: null, sample_rate: 10, edge_only: true });
  });
});

describe("valueStatsDecorator", () => {
  const params = { sections: ["overview", "network"], service_id: "svc-1" };
  const start = new Date(H0).toISOString();
  const end = new Date(H0 + 2 * HOUR).toISOString();
  const answer = () => ({
    overview: { total_requests: 185 },
    network: { total_requests: 185 },
    rollup: {
      data_from: start, data_through: end, approximate: [], lower_bound: [],
      logged_hours: logged,
    },
  });
  const fetchMock = vi.fn();

  beforeEach(() => {
    clearServiceStatsCache();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("starts the Stats read before the summaries answer and attaches its comparison without the logged hours", async () => {
    fetchMock.mockResolvedValue(Response.json(stats([[H0, 100, 1000, 80], [H0 + HOUR, 100, 1000, 90]])));
    const finish = valueStatsDecorator(start, end, params);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toMatch(new RegExp(`/api/admin/usage/service-stats\\?from=${H0}&to=${H0 + 2 * HOUR}$`));
    const out = await finish(answer()) as Record<string, unknown>;
    expect(out.rollup).toEqual({ data_from: start, data_through: end, approximate: [], lower_bound: [] });
    expect(out.fastly_reported).toMatchObject({ requests: 200, logged_requests: 185, window_end: "2026-11-05T12:00:00Z" });
  });

  it("omits the comparison when Stats is refused, and doesn't ask again for the same range", async () => {
    fetchMock.mockResolvedValue(Response.json({ error: "forbidden" }, { status: 403 }));
    const out = await valueStatsDecorator(start, end, params)(answer()) as Record<string, unknown>;
    expect(out).not.toHaveProperty("fastly_reported");
    expect(out.rollup).not.toHaveProperty("logged_hours");
    await valueStatsDecorator(start, end, params)(answer());
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("waits up to 5s for a slower Compute Stats read before dropping the comparison", async () => {
    vi.useFakeTimers();
    fetchMock.mockImplementation(() => new Promise((resolve) => {
      setTimeout(() => resolve(Response.json(stats([[H0, 100, 1000, 80], [H0 + HOUR, 100, 1000, 90]]))), 2_500);
    }));
    const pending = valueStatsDecorator(start, end, params)(answer());
    await vi.advanceTimersByTimeAsync(2_500);
    const out = await pending as Record<string, unknown>;
    expect(out.fastly_reported).toMatchObject({ requests: 200, logged_requests: 185 });
  });

  it("doesn't hold the answer for a slow Stats read", async () => {
    vi.useFakeTimers();
    fetchMock.mockReturnValue(new Promise(() => {}));
    const pending = valueStatsDecorator(start, end, params)(answer());
    await vi.advanceTimersByTimeAsync(5_000);
    expect(await pending).not.toHaveProperty("fastly_reported");
  });

  it("reads nothing for the network section alone, beyond 31 days, or once the range is in the future", async () => {
    const out = await valueStatsDecorator(start, end, { sections: ["network"] })({ network: {}, rollup: { data_through: end } });
    expect(out).toEqual({ network: {}, rollup: { data_through: end } });
    valueStatsDecorator(new Date(H0 - 32 * 24 * HOUR).toISOString(), end, params);
    valueStatsDecorator(new Date(Date.now() + HOUR).toISOString(), new Date(Date.now() + 3 * HOUR).toISOString(), params);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
