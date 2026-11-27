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
      since_hours: 24,
      total_scored_sessions: 2,
      flagged: { total: 1, good: 0, bad: 0, unlabeled: 1 },
      passed: { good: 0, bad: 0, unlabeled: 1 },
      precision: null,
      recall: null,
    });
    const zeroThresh = await queryCloudScoring(request("threshold-preview?threshold=0"), "threshold-preview");
    expect(zeroThresh).toMatchObject({ threshold: 0, total_scored_sessions: 2, flagged: { total: 2 } });
    const defaultThresh = await queryCloudScoring(request("threshold-preview"), "threshold-preview");
    expect(defaultThresh).toMatchObject({ threshold: 75 });
  });

  it("reports scoring off with valid empty OpenAPI shapes when edge_score is off", async () => {
    settings.ids = new Set(["status"]);
    await engine.syncWindow([{ key: "scored", bytes: scoringFixture() }]);
    expect(await queryCloudScoring(request("analytics"), "analytics")).toMatchObject({
      supported: false,
      available: false,
      reason: "scoring_fields_not_present",
      top_flagged: { rows: [] },
      score_distribution: { rows: [] },
      compliance_breakdown: { rows: [] },
      latency_timeseries: { rows: [], has_latency: false },
      evaluation: { min_per_class: 3, has_min_samples: false },
      evaluation_per_reason: { buckets: [], min_per_class: 3, has_min_samples_overall: false },
    });
    expect(await queryCloudScoring(request("curves"), "curves")).toMatchObject({
      supported: false,
      available: false,
      reason: "scoring_fields_not_present",
      roc: [],
      pr: [],
      min_per_class: 3,
      has_min_samples: false,
    });
    expect(settings.asked).toEqual(["fixture-service", "fixture-service"]);
  });

  it("returns valid empty OpenAPI shapes when scoring is enabled with no scored rows yet", async () => {
    await engine.syncWindow([{ key: "unscored", bytes: parquetFixture([
      { timestamp: at, status: 200, cache: "HIT", resp_bytes: 100 },
    ]) }]);
    expect(await queryCloudScoring(request("analytics"), "analytics")).toMatchObject({
      supported: true,
      available: false,
      reason: "no_scored_rows",
      top_flagged: { rows: [] },
      score_distribution: { rows: [] },
      evaluation_per_reason: { buckets: [], min_per_class: 3 },
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
      rows: [{ ip: "192.0.2.xxx" }, { ip: "192.0.2.xxx" }],
    });
    const blockedEvents = await cloudScoringRead(
      new Request(request("sessions/sid-a/events?since_days=1"), { headers: { "x-mask-ips": "1" } }),
    );
    expect(blockedEvents?.status).toBe(403);
    expect(await blockedEvents?.json()).toMatchObject({
      detail: { error: "pii_policy_violation", field: "sid" },
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

    // Masked request omits ip from pass-2 projection and masks missing/unloaded IP to [redacted].
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
        { edge_sid: "sid-a", ip: "[redacted]", url: "/login" },
        { edge_sid: "sid-b", ip: "[redacted]", url: "/home" },
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

  it("includes evaluation, evaluation_per_reason buckets, curves, and labeled threshold-preview", async () => {
    const labeledBytes = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: 6 }, (_, i) => new Date(at - i * 1000)) },
        { name: "edge", type: "BOOLEAN", data: [true, true, true, true, true, true] },
        { name: "edge_score", type: "INT32", data: [10, 20, 30, 80, 90, 95] },
        { name: "edge_sid", type: "STRING", data: ["g1", "g2", "g3", "b1", "b2", "b3"] },
        {
          name: "edge_score_reason",
          type: "STRING",
          data: [
            "cookie-missing",
            "cookie-missing",
            "cookie-missing",
            "cookie-missing,impossibly-fast",
            "cookie-missing",
            "cookie-missing",
          ],
        },
      ],
    });
    await engine.syncWindow([{ key: "labeled", bytes: labeledBytes }]);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          labels: [
            { sid: "g1", label: "good" },
            { sid: "g2", label: "good" },
            { sid: "g3", label: "good" },
            { sid: "b1", label: "bad" },
            { sid: "b2", label: "bad" },
            { sid: "b3", label: "bad" },
          ],
        }),
      ),
    );
    const result = await queryCloudScoring(request("analytics"), "analytics");
    expect(result).toMatchObject({
      available: true,
      evaluation: { n_good: 3, n_bad: 3, min_per_class: 3, has_min_samples: true, auc: 1, passed: true },
      evaluation_per_reason: {
        n_good: 3,
        n_bad: 3,
        min_per_class: 3,
        has_min_samples_overall: true,
      },
    });
    const perReason = await queryCloudScoring(request("evaluation/per-reason"), "evaluation/per-reason") as {
      buckets: Array<{ reason: string; n_good: number; n_bad: number; has_min_samples: boolean; auc?: number }>;
      has_min_samples_overall: boolean;
    };
    expect(perReason.has_min_samples_overall).toBe(true);
    const cookieMissing = perReason.buckets.find((b) => b.reason === "cookie-missing");
    expect(cookieMissing).toMatchObject({ n_good: 3, n_bad: 3, has_min_samples: true, auc: 1 });

    const curves = await queryCloudScoring(request("curves"), "curves") as {
      has_min_samples: boolean;
      min_per_class: number;
      auc: number;
      average_precision: number;
      roc: unknown[];
      pr: unknown[];
    };
    expect(curves).toMatchObject({ has_min_samples: true, min_per_class: 3, auc: 1, average_precision: 1 });
    expect(curves.roc).toHaveLength(101);
    expect(curves.pr).toHaveLength(101);

    const preview = await queryCloudScoring(request("threshold-preview?threshold=75"), "threshold-preview");
    expect(preview).toMatchObject({
      threshold: 75,
      total_scored_sessions: 6,
      flagged: { total: 3, good: 0, bad: 3, unlabeled: 0 },
      passed: { good: 3, bad: 0, unlabeled: 0 },
      precision: 1,
      recall: 1,
    });
  });
});
