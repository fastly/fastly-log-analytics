// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, type RollupResult } from "./dashboard/rollup-query";
import { browserSource } from "./dashboard/sources";
import {
  FIXTURE_START,
  SUMMARY_FIXTURE_BUFFERS,
  fixtureFiles,
  fixturePage,
  loadSummaryFixtures,
  type SummaryFixture,
} from "./dashboard/summary-fixtures";
import { fixtureSource } from "./dashboard/test-fixtures";
import { insightsBaselineRollup, rollupInsightsBaseline } from "./insights-rollup";
import { networkFromRollup } from "./network-rollup";
import { originFromRollup } from "./origin-rollup";
import { performanceFromRollup } from "./performance-rollup";
import { answerFromRollup } from "./rollup-answer";
import { SECURITY_ROLLUP_SECTIONS } from "./rollup-sections";
import { securityFromRollup } from "./security-rollup";

vi.mock("./dashboard/sources", async (importOriginal) => ({
  ...await importOriginal<typeof import("./dashboard/sources")>(),
  browserSource: vi.fn(),
}));

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const ORIGIN = loadSummaryFixtures("origin-summaries");
const range = { start_time: new Date(T0).toISOString(), end_time: new Date(T1).toISOString() };
const health = { ...range, sections: ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets"],
  bucket_seconds: 900, top_n: 30 };
const origin = { ...range, bucket_minutes: 60, timeseries_metric: "ttlb", timeseries_percentile: "p95",
  ip_health_limit: 30, sections: ["ip_health", "timeseries"] };
const performance = { ...range, sections: ["ttl_dist", "waterfall"] };
const security = { ...range, bucket_seconds: 3600, sections: [...SECURITY_ROLLUP_SECTIONS] };
const insights = { service_id: "example", window_size_hrs: 1, baseline_hours: 1, clamp_end: range.end_time };

const source = (
  buffers: Record<string, ArrayBuffer> = SUMMARY_FIXTURE_BUFFERS,
  files: (kind: SummaryFixture["kind"]) => ManifestFile[] = (kind) => fixtureFiles(kind),
) => fixtureSource([fixturePage(files("raw"))], buffers, { json: [], files: [] }, [fixturePage(files("window"))]);
const originSource = () => source(ORIGIN.buffers, (kind) => ORIGIN.files(kind));

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("answerFromRollup", () => {
  it.each([
    ["/api/network-health", health, source, networkFromRollup],
    ["/api/network/pop-health", range, source, networkFromRollup],
    ["/api/network-health", { ...range, sections: ["buckets", "cities", "map_buckets"], map_asn: "all", bucket_seconds: 3600 },
      source, networkFromRollup],
    ["/api/network-quality", { ...range, region_country: "US" }, source, networkFromRollup],
    ["/api/network/bundle", { ...health, sections: ["summary", "pop_health"] }, source, networkFromRollup],
    ["/api/origin/aggregates", origin, originSource, originFromRollup],
    ["/api/performance/aggregates", performance, source, performanceFromRollup],
    ["/api/security/aggregates", security, source, securityFromRollup],
    ["/api/security/threat-intel", range, source, securityFromRollup],
  ] as const)("answers %s as the section reader does, unchanged by structured cloning", async (path, params, make, direct) => {
    const answer = await answerFromRollup(path, params, make());
    expect(answer).not.toBeNull();
    expect(answer).toStrictEqual(await direct(path, params as never, make()));
    expect(structuredClone(answer)).toStrictEqual(answer);
  });

  it("answers an Insights baseline with only what the detectors read, unchanged by structured cloning", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: T1 });
    const answer = await answerFromRollup("/api/insights", insights, source()) as RollupResult;
    expect(structuredClone(answer)).toStrictEqual(answer);

    const windows = fixtureFiles("window", (e) => e.end <= FIXTURE_START + 3600);
    const parts = await Promise.all(windows.map((f) => decodeRollup(SUMMARY_FIXTURE_BUFFERS[f.key], 300_000)));
    const full = { rollup: mergeRollups(parts.map((p) => p!)), dataFromMs: T0, dataThroughMs: T0 + HOUR };
    expect(answer).toStrictEqual(insightsBaselineRollup(full));
    const [city] = full.rollup.dims.get("city")!.keys();
    const [country, name] = JSON.parse(city) as [string, string];
    const rows = {
      city: [{ country, city: name, w_cnt: 3 }],
      dimension: [{ dimension: "ua", ua: "curl", w_cnt: 2 }, { dimension: "ua", ua: null, w_cnt: 1 }],
      asn: [{ asn: "13335", w_cnt: 1 }],
      scalar: [{ w_cnt: 3 }],
    };
    expect(rollupInsightsBaseline(answer, rows)).toStrictEqual(rollupInsightsBaseline(full, rows));
  });

  it("leaves paths summaries don't answer to the raw scan", async () => {
    expect(await answerFromRollup("/api/network-health", { ...health, sections: ["shielding_analysis"] }, source())).toBeNull();
    expect(await answerFromRollup("/api/network/bundle", { ...health, sections: ["quality"] }, source())).toBeNull();
  });
});

describe("dashboard engine worker rollup message", () => {
  it("posts the same answer the page computes in-thread, read under the page's cache scope and service", async () => {
    const fixture = source();
    const scoped = vi.fn(() => fixture);
    vi.mocked(browserSource).mockReturnValue({ ...fixture, withOptions: scoped, clearMemoryCache: vi.fn() } as never);
    const host = { onmessage: null as ((event: { data: unknown }) => void) | null, postMessage: vi.fn() };
    vi.stubGlobal("self", Object.assign(host, { location: { origin: "https://flp.example" } }));
    await import("./dashboard/engine.worker");

    host.onmessage!({ data: {
      id: 7, type: "rollup", path: "/api/network-health", params: health, serviceId: "svc-1", cacheScope: "page",
    } });
    await vi.waitFor(() => expect(host.postMessage).toHaveBeenCalled());
    const [reply] = host.postMessage.mock.calls[0] as [{ id: number; type: string; result: unknown }];
    expect(reply).toMatchObject({ id: 7, type: "rollupResult" });
    expect(structuredClone(reply.result)).toStrictEqual(await answerFromRollup("/api/network-health", health, source()));
    expect(fixture.log.files.length).toBeGreaterThan(0);
    expect(vi.mocked(browserSource).mock.calls.at(-1)?.[2]).toEqual({ cacheScope: "page" });
    expect(scoped).toHaveBeenCalledWith({ serviceId: "svc-1" });
  });
});
