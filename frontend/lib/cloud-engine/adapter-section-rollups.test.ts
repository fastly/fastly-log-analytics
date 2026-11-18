import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest } from "./adapter";
import { assetsFromRollup } from "./assets-rollup";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { ensureCloudRange } from "./range-loader";
import { valueFromRollup } from "./value-rollup";
import { clearServiceStatsCache } from "./value-stats";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

vi.mock("./assets-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./assets-rollup")>(),
  assetsFromRollup: vi.fn(async () => null),
}));

vi.mock("./value-rollup", async (importOriginal) => ({
  ...await importOriginal<typeof import("./value-rollup")>(),
  valueFromRollup: vi.fn(async () => null),
}));

// Without a service the raw scan skips the field catalog.
const post = (path: string, body: Record<string, unknown>, service?: string) =>
  handleCloudAnalyticalRequest(new Request(`http://localhost${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...service ? { "x-service-id": service } : {} },
    body: JSON.stringify(body),
  }));

describe("Cloud /assets and /fastly-value summary sections", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn().mockImplementation(async () => Response.json({ latest_log_at: "2026-11-06T00:00:00Z" })));
    const engine = getDuckDBEngine();
    vi.spyOn(engine, "executeSql").mockResolvedValue([]);
    vi.spyOn(engine, "getPresentColumns").mockResolvedValue(new Set(["url", "cache", "proto"]));
    vi.mocked(ensureCloudRange).mockClear();
    vi.mocked(assetsFromRollup).mockClear();
    vi.mocked(valueFromRollup).mockClear();
    clearServiceStatsCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("answers the default views' summary sections before any raw scan, over the scan's range and service", async () => {
    vi.mocked(assetsFromRollup).mockResolvedValueOnce({ served: "assets" } as never);
    const assets = await post("/api/assets/aggregates", { sections: ["summary"], filters: { cache: { mode: "include", values: ["HIT"] } } }, "svc-1");
    expect(await assets!.json()).toEqual({ served: "assets" });
    const [path, params, source] = vi.mocked(assetsFromRollup).mock.calls[0];
    expect(path).toBe("/api/assets/aggregates");
    expect(params).toMatchObject({ service_id: "svc-1", start_time: "2026-11-05T00:00:00.000Z", end_time: "2026-11-06T00:00:00Z" });
    expect(source.cacheScope).toContain("svc-1");

    vi.mocked(valueFromRollup).mockResolvedValueOnce({ served: "value" } as never);
    const value = await post("/api/value/summary", {
      sections: ["overview", "network"], filters: {}, custom_filters: {}, chart_interval: "1 day",
      start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z",
    }, "svc-1");
    expect(await value!.json()).toEqual({ served: "value" });
    expect(vi.mocked(valueFromRollup).mock.calls[0][1])
      .toMatchObject({ start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T18:00:00Z", chart_interval: "1 day" });
    expect(ensureCloudRange).not.toHaveBeenCalled();
  });

  it("adds Fastly's Stats API totals to an unfiltered overview answer, read alongside the summaries", async () => {
    const hour = Date.parse("2026-11-05T12:00:00Z");
    vi.mocked(fetch).mockImplementation(async (input) => String(input).includes("/api/admin/usage/service-stats")
      ? Response.json({
        service_id: "svc-1", from: hour / 1000, to: hour / 1000 + 7200, sample_rate: 100, edge_only: false,
        hours: [{ start_time: hour / 1000, requests: 120, bandwidth_bytes: 1200, hits: 60 }],
      })
      : Response.json({ latest_log_at: "2026-11-06T00:00:00Z" }));
    vi.mocked(valueFromRollup).mockResolvedValueOnce({
      overview: { total_requests: 100 },
      rollup: { data_from: "2026-11-05T12:00:00Z", data_through: "2026-11-05T14:00:00Z", approximate: [], lower_bound: [],
        logged_hours: [[hour, 100, 1000]] },
    } as never);
    const value = await post("/api/value/summary", {
      sections: ["overview", "network"], filters: {}, start_time: "2026-11-05T12:00:00Z", end_time: "2026-11-05T14:00:00Z",
    }, "svc-1");
    const json = await value!.json();
    expect(json.rollup).not.toHaveProperty("logged_hours");
    expect(json.fastly_reported).toMatchObject({ requests: 120, logged_requests: 100, window_end: "2026-11-05T13:00:00Z" });
  });

  it("scans raw, over only the requested sections' columns, when summaries can't answer or the request isn't theirs", async () => {
    expect((await post("/api/value/summary", { sections: ["overview", "network"] }))!.status).toBe(200);
    expect(valueFromRollup).toHaveBeenCalledTimes(1);
    expect(ensureCloudRange).toHaveBeenCalledTimes(1);

    vi.mocked(valueFromRollup).mockClear();
    vi.mocked(ensureCloudRange).mockClear();
    expect((await post("/api/value/summary", { sections: ["caching"] }))!.status).toBe(200);
    expect(valueFromRollup).not.toHaveBeenCalled();
    const columns = vi.mocked(ensureCloudRange).mock.calls[0][4]?.projection?.columns;
    expect(columns).toEqual(expect.arrayContaining(["cache", "resp_bytes", "edge"]));
    expect(columns).not.toContain("url");

    expect((await post("/api/assets/aggregates", { sections: ["low_ttl_assets"] }))!.status).toBe(200);
    expect((await post("/api/assets/aggregates", { sections: ["summary"], filters: { pop: { mode: "include", values: ["IAD"] } } }))!.status)
      .toBe(200);
    expect(assetsFromRollup).not.toHaveBeenCalled();
  });
});
