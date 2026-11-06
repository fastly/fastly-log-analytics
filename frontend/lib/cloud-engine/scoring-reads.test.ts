// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { parquetFixture } from "./dashboard/test-fixtures";
import { cloudScoringRead, queryCloudScoring } from "./scoring-reads";

const engine = getDuckDBEngine();
const end = "2026-11-02T00:00:00Z";
const at = Date.parse(end) - 1000;
const request = (route: string) => new Request(
  `http://localhost/api/services/fixture-service/scoring/${route}${route.includes("?") ? "&" : "?"}end_time=${end}`,
);

beforeEach(async () => { await engine.reset(); });
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
  it("reports absent scorer control-plane capabilities without fabricated configuration or label counts", async () => {
    const fetcher = vi.fn(async () => Response.json({
      services: [{ service_id: "fixture-service", scoring: { enabled: false } }],
    }));
    vi.stubGlobal("fetch", fetcher);
    const config = await (await cloudScoringRead(request("config")))!.json();
    expect(config).toMatchObject({
      status: { enabled: false, drift_known: false, supported: false },
      capabilities: { historical_analytics: true, management: false, labels: false, thresholds: false },
    });
    expect(config).not.toHaveProperty("threshold");
    const labels = await (await cloudScoringRead(request("labels")))!.json();
    expect(labels).toMatchObject({ supported: false, reason: "cloud_scoring_control_plane_unavailable" });
    expect(labels).not.toHaveProperty("labels");
    expect(labels).not.toHaveProperty("counts");
    expect(fetcher).toHaveBeenCalledTimes(1);
    fetcher.mockImplementation(async () => Response.json({ error: "unauthorized" }, { status: 401 }));
    expect((await cloudScoringRead(request("status")))!.status).toBe(401);
  });

  it("does not turn a dataset with no scoring fields into successful empty analytics", async () => {
    await engine.syncWindow([{ key: "unscored", bytes: parquetFixture([
      { timestamp: at, status: 200, cache: "HIT", resp_bytes: 100 },
    ]) }]);
    expect(await queryCloudScoring(request("analytics"), "analytics")).toEqual({
      supported: false, available: false, reason: "scoring_fields_not_present",
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
});
