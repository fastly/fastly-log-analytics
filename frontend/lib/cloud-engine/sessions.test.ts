import { describe, expect, it } from "vitest";
import {
  SESSIONS_INACTIVITY_GAP_SECONDS,
  SESSIONS_MAX_WINDOW_DAYS,
  buildScoringSourceSql,
  buildSessionsCteSql,
  buildSessionsRollupStitchSql,
  handleSessionScoringRoute,
  handleSessionsDetail,
  handleSessionsList,
  openSessionToken,
  sealSessionToken,
} from "./sessions";

describe("cloud-engine sessions & session scoring module", () => {
  it("exports 30-minute inactivity gap (1800s) and 7-day window cap", () => {
    expect(SESSIONS_INACTIVITY_GAP_SECONDS).toBe(1800);
    expect(SESSIONS_MAX_WINDOW_DAYS).toBe(7);

    const cteSql = buildSessionsCteSql({
      startTime: "2026-11-01T00:00:00Z",
      endTime: "2026-11-02T00:00:00Z",
    });
    expect(cteSql).toContain("gap > 1800000");
    expect(cteSql).toContain("LAG(epoch_ms(ts)) OVER (PARTITION BY");

    const stitchSql = buildSessionsRollupStitchSql("sessions_rollup");
    expect(stitchSql).toContain("INTERVAL 30 MINUTES");
  });

  it("rejects session windows exceeding 7 days with HTTP 400", async () => {
    const res = await handleSessionsList({
      start_time: "2026-10-01T00:00:00Z",
      end_time: "2026-10-10T00:00:00Z",
    });
    expect(res.status).toBe(400);
    expect(res.body).toEqual({
      detail: {
        error: "invalid_window",
        message:
          "Sessions view is limited to 7 days. Please narrow your date range.",
      },
    });
  });

  it("seals session_token before masking IP to 0.0.0.0 for analyst-no-pii", async () => {
    const listRes = await handleSessionsList(
      {
        service_id: "svc-1",
        start_time: "2026-11-04T00:00:00Z",
        end_time: "2026-11-05T00:00:00Z",
      },
      {
        role: "analyst-no-pii",
        runSql: async () => [
          {
            ip: "198.51.100.42",
            ja4: "t13d1516h2_8daaf6152771_b0da82dd1658",
            session_start: "2026-11-04T10:00:00Z",
            session_end: "2026-11-04T10:15:00Z",
            req_count: 1200,
            reqs_4xx: 50,
            reqs_5xx: 0,
            edge_sid: "esid-99",
          },
        ],
      },
    );

    expect(listRes.status).toBe(200);
    if ("sessions" in listRes.body) {
      const row = listRes.body.sessions[0];
      expect(row.ip).toBe("0.0.0.0");
      expect(row.edge_sid).toBe("[redacted]");
      expect(row.flagged).toBe(true);

      const opened = openSessionToken(row.session_token);
      expect(opened?.ip).toBe("198.51.100.42");

      // Drill into session detail using the sealed token while masked
      const detailRes = await handleSessionsDetail(
        { session_token: row.session_token },
        {
          role: "analyst-no-pii",
          runSql: async (sql) => {
            expect(sql).toContain("198.51.100.42");
            return [
              {
                timestamp: "2026-11-04T10:01:00Z",
                ip: "198.51.100.42",
                url: "/checkout",
                status: 200,
              },
            ];
          },
        },
      );
      expect(detailRes.status).toBe(200);
      if ("data" in detailRes.body) {
        expect(detailRes.body.data[0].ip).toBe("0.0.0.0");
      }
    }
  });

  it("blocks un-tokenized /api/sessions/detail when maskIps is active", async () => {
    const res = await handleSessionsDetail(
      {
        ip: "198.51.100.42",
        start_time: "2026-11-04T00:00:00Z",
        end_time: "2026-11-05T00:00:00Z",
      },
      { maskIps: true },
    );
    expect(res.status).toBe(403);
    expect(res.body).toEqual({
      detail: {
        error: "pii_policy_violation",
        field: "ip",
      },
    });
  });

  it("synthesizes missing edge_score columns and serves /scoring/* routes", async () => {
    const sourceSql = buildScoringSourceSql(
      "requests",
      new Set(["timestamp", "ip", "url"]),
    );
    expect(sourceSql).toContain('CAST(NULL AS DOUBLE) AS "edge_score"');

    const statusRes = await handleSessionScoringRoute(
      "GET",
      "svc-1",
      "status",
    );
    expect(statusRes.status).toBe(200);

    const enableRes = await handleSessionScoringRoute(
      "POST",
      "svc-1",
      "enable",
    );
    expect(enableRes.body).toMatchObject({ enabled: true });

    const threshRes = await handleSessionScoringRoute(
      "PUT",
      "svc-1",
      "threshold",
      { threshold: 85, enforce_threshold: 95 },
    );
    expect(threshRes.body).toEqual({ threshold: 85, enforce_threshold: 95 });
  });

  it("round-trips session tokens cleanly", () => {
    const token = sealSessionToken({
      ip: "203.0.113.9",
      ja4: null,
      s: "2026-11-04T00:00:00Z",
      e: "2026-11-04T00:10:00Z",
      svc: "svc-test",
    });
    expect(openSessionToken(token)).toEqual({
      ip: "203.0.113.9",
      ja4: null,
      s: "2026-11-04T00:00:00Z",
      e: "2026-11-04T00:10:00Z",
      svc: "svc-test",
    });
    expect(openSessionToken("not-valid-base64-json")).toBeNull();
  });
});
