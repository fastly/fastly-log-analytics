// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { parquetFixture } from "./dashboard/test-fixtures";
import {
  cloudScoringRead,
  queryCloudScoring,
  SCORING_CORE_COLUMNS,
  SCORING_EVENTS_COLUMNS,
  scoringRequiredAnyColumns,
  scoringScanColumns,
} from "./scoring-reads";
import { CloudBudgetError } from "./range-loader";

const settings = vi.hoisted(() => ({ ids: new Set(["edge_score"]), asked: [] as (string | undefined)[] }));
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: vi.fn(async (serviceId: string | undefined) => {
    settings.asked.push(serviceId);
    return settings.ids;
  }),
}));

const engine = getDuckDBEngine();
const end = "2026-11-02T00:00:00Z";
const at = Date.parse(end) - 1000;
const request = (route: string) => new Request(
  `http://localhost/api/services/fixture-service/scoring/${route}${route.includes("?") ? "&" : "?"}end_time=${end}`,
);

beforeEach(async () => {
  await engine.reset();
  settings.ids = new Set(["edge_score"]);
  settings.asked = [];
});
afterEach(() => { vi.unstubAllGlobals(); });

function scoringFixture(optional = false) {
  return parquetWriteBuffer({ columnData: [
    { name: "timestamp", type: "TIMESTAMP", data: [new Date(at), new Date(at - 1000), new Date(at - 2 * 86400000)] },
    { name: "edge", type: "BOOLEAN", data: [true, true, true] },
    { name: "edge_score", type: "INT32", data: [85, 15, 99] },
    { name: "edge_sid", type: "STRING", data: ["sid-a", "sid-b", "outside-range"] },
    { name: "ip", type: "STRING", data: ["192.0.2.1", "192.0.2.2", "192.0.2.3"] },
    { name: "edge_score_reason", type: "STRING", data: ["compute-unavailable,burst", "clean", "old"] },
    ...(optional ? [
      { name: "edge_score_rtt_us", type: "INT32" as const, data: [1000, 3000, 9000] },
      { name: "edge_score_exec_us", type: "INT32" as const, data: [100, 300, 900] },
      { name: "edge_score_l2", type: "INT32" as const, data: [80, 10, 95] },
      { name: "edge_cookie_compliance", type: "STRING" as const, data: ["ok", "missing", "old"] },
    ] : []),
  ] });
}

describe("Cloud scoring reads", () => {
  it("passes native scoring control-plane routes through to Compute while scanning Parquet locally", async () => {
    expect(await cloudScoringRead(request("config"))).toBeNull();
    expect(await cloudScoringRead(request("status"))).toBeNull();
    expect(await cloudScoringRead(request("labels"))).toBeNull();
    await engine.syncWindow([{ key: "scored", bytes: scoringFixture() }]);
    const preview = await queryCloudScoring(request("threshold-preview?threshold=50"), "threshold-preview");
    expect(preview).toMatchObject({
      threshold: 50,
      total_scored: 2,
      flagged_count: 1,
      flagged_pct: 50,
      total_scored_sessions: 2,
      flagged: { count: 1, pct: 50 },
      passed: { count: 1, pct: 50 },
    });
  });

  it("reports scoring off when the service's log field settings leave edge_score off, whatever the files hold", async () => {
    settings.ids = new Set(["status"]);
    await engine.syncWindow([{ key: "scored", bytes: scoringFixture() }]);
    expect(await queryCloudScoring(request("analytics"), "analytics")).toEqual({
      supported: false, available: false, reason: "scoring_fields_not_present",
    });
    expect(settings.asked).toEqual(["fixture-service"]);
  });

  it("does not turn enabled scoring with no scored rows yet into successful empty analytics", async () => {
    await engine.syncWindow([{ key: "unscored", bytes: parquetFixture([
      { timestamp: at, status: 200, cache: "HIT", resp_bytes: 100 },
    ]) }]);
    expect(await queryCloudScoring(request("analytics"), "analytics")).toEqual({
      supported: true, available: false, reason: "no_scored_rows",
    });
  });

  it("computes score buckets, health, ranked requests and bounded session events from actual Parquet", async () => {
    await engine.syncWindow([{ key: "scored", bytes: scoringFixture() }]);
    const result = await queryCloudScoring(request("analytics"), "analytics");
    expect(result).toMatchObject({
      available: true,
      top_flagged: { rows: [{ edge_sid: "sid-a", edge_score: 85 }, { edge_sid: "sid-b", edge_score: 15 }] },
      score_distribution: { rows: [{ bucket: "0-25", count: 1 }, { bucket: "75-100", count: 1 }] },
      health: { total_edge_rows: 2, scored_rows: 2, distinct_sids: 2, avg_score: 50,
        scorer_errors: 1, fail_open_rate_pct: 50, matrix_staleness: null },
      compliance_breakdown: { available: false, reason: "cookie_compliance_not_recorded" },
      latency_timeseries: { available: false, reason: "scorer_latency_not_recorded" },
    });
    const events = await queryCloudScoring(request("sessions/sid-a/events?since_days=1"), "sessions/sid-a/events");
    expect(events).toMatchObject({ sid: "sid-a", event_count: 1, events: [{ edge_score: 85 }] });
    const escaped = await queryCloudScoring(request("sessions/sid-a%27%20OR%201%3D1--/events?since_days=1"), "sessions/sid-a%27%20OR%201%3D1--/events");
    expect(escaped).toMatchObject({ event_count: 0 });
    const maskedRequest = new Request(request("top-flagged"), { headers: { "x-mask-ips": "1" } });
    expect(await queryCloudScoring(maskedRequest, "top-flagged")).toMatchObject({
      rows: [{ ip: "0.0.0.0" }, { ip: "0.0.0.0" }],
    });
  });

  it("uses recorded optional compliance, L2 and latency columns instead of placeholders", async () => {
    const lease = engine.lease([
      { id: "edge_score_rtt_us", type: "number" },
      { id: "edge_score_exec_us", type: "number" },
      { id: "edge_score_l2", type: "number" },
      { id: "edge_cookie_compliance", type: "string" },
    ]);
    try {
      await lease.ingestFile("optional", "optional", scoringFixture(true));
    } finally { lease.release(); }
    expect(await queryCloudScoring(request("analytics"), "analytics")).toMatchObject({
      compliance_breakdown: { rows: [{ compliance: "missing", count: 1 }, { compliance: "ok", count: 1 }] },
      latency_timeseries: { rows: [{ rtt_p50_us: 2000, rtt_p95_us: 2900, exec_p50_us: 200 }] },
      health: { latency: { available: true, rtt_p50_us: 2000 },
        matrix_staleness: { l2_evaluated: 2, l2_high_count: 1, l2_high_pct: 50, is_stale: false } },
    });
  });

  it("projects unified 11-column scoring core columns and requiredAnyColumns across overview routes", () => {
    for (const route of [
      "analytics", "top-flagged", "health", "score-distribution",
      "compliance-breakdown", "latency-timeseries", "threshold-preview",
      "evaluation", "evaluation/per-reason", "curves",
    ]) {
      const cols = scoringScanColumns(route);
      for (const heavy of ["url", "ua", "ip", "status", "country"]) {
        expect(cols, `${route} should not project ${heavy}`).not.toContain(heavy);
      }
      expect(cols, `${route} should project edge_score`).toContain("edge_score");
      expect(cols).toEqual([...SCORING_CORE_COLUMNS]);
      expect(cols).toHaveLength(11);
      expect(scoringRequiredAnyColumns(route)).toEqual(["edge_score"]);
    }

    const eventCols = scoringScanColumns("sessions/sid-a/events");
    expect(eventCols).toEqual([...SCORING_EVENTS_COLUMNS]);
    expect(eventCols).toHaveLength(10);
    expect(eventCols).toContain("url");
    expect(eventCols).toContain("status");
    for (const unused of ["ua", "ip", "country", "edge_score_rtt_us", "edge_score_exec_us"]) {
      expect(eventCols, `events should not project ${unused}`).not.toContain(unused);
    }
    expect(scoringRequiredAnyColumns("sessions/sid-a/events")).toEqual(["edge_score", "edge_sid"]);
    expect(scoringRequiredAnyColumns("config")).toEqual([]);
    expect(scoringRequiredAnyColumns()).toEqual([]);
  });

  it("enriches top-flagged rows via a bounded pass-2 range load and skips pass-2 when no scored rows match", async () => {
    // Pass-1 table has ONLY narrow scoring columns (no ip, ua, url, status, country).
    const narrowBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(at), new Date(at - 1000)] },
      { name: "edge", type: "BOOLEAN", data: [true, true] },
      { name: "edge_score", type: "INT32", data: [85, 15] },
      { name: "edge_sid", type: "STRING", data: ["sid-a", "sid-b"] },
      { name: "edge_score_reason", type: "STRING", data: ["burst", "clean"] },
    ] });
    const fullBytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(at), new Date(at - 1000)] },
      { name: "edge_score", type: "INT32", data: [85, 15] },
      { name: "edge_sid", type: "STRING", data: ["sid-a", "sid-b"] },
      { name: "ip", type: "STRING", data: ["198.51.100.1", "198.51.100.2"] },
      { name: "ua", type: "STRING", data: ["BotUA/1.0", "Mozilla/5.0"] },
      { name: "url", type: "STRING", data: ["/login", "/home"] },
      { name: "status", type: "INT32", data: [403, 200] },
      { name: "country", type: "STRING", data: ["US", "GB"] },
    ] });

    await engine.syncWindow([{ key: "narrow", bytes: narrowBytes }]);
    const loads: { fromMs: number; toMs: number; columns: readonly string[] | null }[] = [];
    const loadRange = vi.fn(async (fromMs: number, toMs: number, columns: readonly string[] | null) => {
      loads.push({ fromMs, toMs, columns });
      await engine.syncWindow([{ key: "enrich", bytes: fullBytes }]);
      await engine.retainFiles({ request: ["enrich"], vitals: [], errors: [] });
      return {
        runSql: (sql: string) => engine.executeSql<Record<string, unknown>>(sql),
        presentColumns: () => engine.getPresentColumns(),
      };
    });

    const result = await queryCloudScoring(request("analytics"), "analytics", { loadRange });
    expect(loadRange).toHaveBeenCalledTimes(1);
    expect(loads[0].fromMs).toBe(at - 1000);
    expect(loads[0].toMs).toBe(at + 2);
    expect(loads[0].columns).toEqual(expect.arrayContaining(["ip", "ua", "url", "status", "country"]));
    expect(result).toMatchObject({
      available: true,
      top_flagged: {
        rows: [
          { edge_sid: "sid-a", edge_score: 85, ip: "198.51.100.1", ua: "BotUA/1.0", url: "/login", status: 403, country: "US" },
          { edge_sid: "sid-b", edge_score: 15, ip: "198.51.100.2", ua: "Mozilla/5.0", url: "/home", status: 200, country: "GB" },
        ],
      },
    });

    // Masked request omits ip from pass-2 projection and returns 0.0.0.0.
    await engine.reset();
    await engine.syncWindow([{ key: "narrow", bytes: narrowBytes }]);
    loads.length = 0;
    loadRange.mockClear();
    const masked = await queryCloudScoring(
      new Request(request("top-flagged"), { headers: { "x-mask-ips": "1" } }),
      "top-flagged",
      { loadRange },
    );
    expect(loads[0].columns).not.toContain("ip");
    expect(masked).toMatchObject({
      rows: [
        { edge_sid: "sid-a", ip: "0.0.0.0", url: "/login" },
        { edge_sid: "sid-b", ip: "0.0.0.0", url: "/home" },
      ],
    });

    // Zero scored rows in window: pass-2 must never run.
    const emptyScored = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(at)] },
      { name: "edge", type: "BOOLEAN", data: [true] },
      { name: "edge_score", type: "INT32", data: [null] },
    ] });
    await engine.reset();
    await engine.syncWindow([{ key: "empty-scored", bytes: emptyScored }]);
    loadRange.mockClear();
    const emptyRes = await queryCloudScoring(request("analytics"), "analytics", { loadRange });
    expect(loadRange).not.toHaveBeenCalled();
    expect(emptyRes).toMatchObject({ available: true, top_flagged: { rows: [] } });

    // Budget error during pass-2 degrades gracefully instead of failing analytics.
    await engine.reset();
    await engine.syncWindow([{ key: "narrow", bytes: narrowBytes }]);
    const budgetFail = vi.fn(async () => {
      throw new CloudBudgetError("over", { unit: "bytes", needed: 80 * 1024 * 1024, limit: 64 * 1024 * 1024 });
    });
    const degraded = await queryCloudScoring(request("analytics"), "analytics", { loadRange: budgetFail });
    expect(degraded).toMatchObject({
      available: true,
      top_flagged: {
        rows: [
          { edge_sid: "sid-a", edge_score: 85, ip: null, ua: null, url: null, status: null, country: null },
          { edge_sid: "sid-b", edge_score: 15, ip: null, ua: null, url: null, status: null, country: null },
        ],
      },
    });
  });

  it("includes evaluation and evaluation_per_reason in analytics when labels are fetched", async () => {
    await engine.syncWindow([{ key: "scored", bytes: scoringFixture() }]);
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ labels: [] })));
    const result = await queryCloudScoring(request("analytics"), "analytics");
    expect(result).toMatchObject({
      available: true,
      evaluation: { n_good: 0, n_bad: 0, min_per_class: 5, has_min_samples: false, auc: null, passed: false },
      evaluation_per_reason: { rows: [], n_good: 0, n_bad: 0, has_min_samples: false },
    });
  });
});
