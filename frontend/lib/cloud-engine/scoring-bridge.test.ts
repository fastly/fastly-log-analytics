// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cloudScoringControlBridge } from "./scoring-bridge";
import * as scoringTrain from "./scoring-train";

describe("cloudScoringControlBridge", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns null for non-scoring paths, scan GET routes, and SSE job actions", async () => {
    expect(await cloudScoringControlBridge(new Request("http://localhost/api/sessions"))).toBeNull();
    expect(
      await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/analytics")),
    ).toBeNull();
    expect(
      await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/threshold-preview")),
    ).toBeNull();
    expect(
      await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/sessions/abc/events")),
    ).toBeNull();
    expect(
      await cloudScoringControlBridge(
        new Request("http://localhost/api/services/svc-1/scoring/enable", { method: "POST", body: "{}" }),
      ),
    ).toBeNull();
    expect(
      await cloudScoringControlBridge(
        new Request("http://localhost/api/services/svc-1/scoring/disable", { method: "POST", body: "{}" }),
      ),
    ).toBeNull();
    expect(
      await cloudScoringControlBridge(
        new Request("http://localhost/api/services/svc-1/scoring/teardown", { method: "POST", body: "{}" }),
      ),
    ).toBeNull();
  });

  it("flattens GET /status scoring fields onto top level", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          service_id: "svc-1",
          enabled: true,
          scoring: {
            scoring_service_id: "scorer-123",
            scoring_service_name: "flp-scorer-svc-1",
            scoring_domain: "scorer.example.com",
            matrix_version: "2026-11-27-01",
            enabled_at: "2026-11-27T10:00:00Z",
            scoring_active_version: 4,
            scoring_activated_at: "2026-11-27T10:01:00Z",
          },
        }),
      ),
    );

    const res = await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/status"));
    expect(res?.status).toBe(200);
    expect(await res?.json()).toMatchObject({
      enabled: true,
      scoring_service_id: "scorer-123",
      scoring_service_name: "flp-scorer-svc-1",
      scoring_domain: "scorer.example.com",
      matrix_version: "2026-11-27-01",
      enabled_at: "2026-11-27T10:00:00Z",
      scoring_active_version: 4,
      scoring_activated_at: "2026-11-27T10:01:00Z",
    });
  });

  it("translates GET /config, GET/PUT/validate exclude-regex, enforce-threshold, enforce-status-code, and rotate-key", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.endsWith("/scoring/config")) {
        return Response.json({
          status: {
            enabled: true,
            scoring: { scoring_service_id: "scorer-1", matrix_version: "v1" },
          },
          threshold: { threshold: 75 },
          enforce_threshold: { enforce_threshold: 80 },
          exclude_regex: { exclude_url_regex: "", effective_regex: "^/static" },
          enforce_status_code: { status_code: 403 },
        });
      }
      if (url.endsWith("/scoring/exclude-regex") && method === "GET") {
        return Response.json({
          exclude_url_regex: "^/health$",
          effective_regex: "^/health$",
          is_default: false,
        });
      }
      if (url.includes("/scoring/exclude-regex?") && method === "PUT") {
        expect(url).toContain("confirm=true");
        const parsed = JSON.parse(String(init?.body ?? "{}"));
        expect(parsed).toEqual({ exclude_url_regex: "^/api/" });
        return Response.json({ ok: true, exclude_url_regex: "^/api/", effective_regex: "^/api/" });
      }
      if (url.endsWith("/scoring/exclude-regex/validate") && method === "POST") {
        const parsed = JSON.parse(String(init?.body ?? "{}"));
        expect(parsed).toEqual({ exclude_url_regex: "^/ok" });
        return Response.json({ valid: true, cleaned: "^/ok", effective_regex: "^/ok" });
      }
      if (url.endsWith("/scoring/enforce-threshold") && method === "GET") {
        return Response.json({ enforce_threshold: 85 });
      }
      if (url.includes("/scoring/enforce-threshold?") && method === "PUT") {
        expect(url).toContain("confirm=true");
        const parsed = JSON.parse(String(init?.body ?? "{}"));
        expect(parsed).toEqual({ enforce_threshold: null });
        return Response.json({ ok: true, enforce_threshold: null });
      }
      if (url.includes("/scoring/enforce-status-code?") && method === "PUT") {
        expect(url).toContain("confirm=true");
        const parsed = JSON.parse(String(init?.body ?? "{}"));
        expect(parsed).toEqual({ status_code: 429 });
        return Response.json({ ok: true, status_code: 429 });
      }
      if (url.includes("/scoring/rotate-key?") && method === "POST") {
        expect(url).toContain("confirm=true");
        return Response.json({ ok: true, message: "AES-256 cookie key rotated." });
      }
      return Response.json({}, { status: 404 });
    });
    vi.stubGlobal("fetch", fetchMock);

    const cfgRes = await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/config"));
    expect(await cfgRes?.json()).toMatchObject({
      status: { scoring_service_id: "scorer-1", matrix_version: "v1" },
      exclude_regex: { current: "", effective: "^/static", is_default: true },
      enforce_status_code: { current: 403, default: 429, effective: 403, min: 400, max: 599, is_default: false },
      threshold: { threshold: 75, enforced: true },
    });

    const getRegex = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/exclude-regex"),
    );
    expect(await getRegex?.json()).toMatchObject({
      current: "^/health$",
      effective: "^/health$",
      is_default: false,
    });

    const putRegex = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/exclude-regex", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ regex: "^/api/" }),
      }),
    );
    expect(await putRegex?.json()).toMatchObject({
      ok: true,
      effective_regex: "^/api/",
      is_default: false,
      lint_warnings: [],
    });

    const valRegex = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/exclude-regex/validate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ regex: "^/ok" }),
      }),
    );
    expect(await valRegex?.json()).toEqual({
      ok: true,
      lint_warnings: [],
      error: null,
      reason: null,
      cleaned: "^/ok",
      effective_regex: "^/ok",
    });

    const getThresh = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/enforce-threshold"),
    );
    expect(await getThresh?.json()).toEqual({ threshold: 85, enforced: true, key: "enforce_threshold" });

    const putThresh = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/enforce-threshold", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ threshold: null }),
      }),
    );
    expect(await putThresh?.json()).toMatchObject({ ok: true, threshold: null, enforced: false });

    const putCode = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/enforce-status-code", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ status_code: null }),
      }),
    );
    expect(await putCode?.json()).toMatchObject({ ok: true, effective_status_code: 429, is_default: true });

    const rotRes = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/rotate-key", { method: "POST" }),
    );
    const rotBody = await rotRes?.json();
    expect(rotBody).toMatchObject({
      ok: true,
      message: "AES-256 cookie key rotated.",
      previous_key_grace: true,
    });
    expect(typeof rotBody.rotated_at).toBe("string");
  });

  it("maps labels (edge_sid <-> sid, created_by <-> flagged_by, neutral count) and audit entries", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        const method = (init?.method ?? "GET").toUpperCase();
        if (url.endsWith("/scoring/labels") && method === "GET") {
          return Response.json({
            labels: [
              { id: "l1", edge_sid: "sid-1", label: "good", created_by: "alice", notes: "" },
              { id: "l2", edge_sid: "sid-2", label: "neutral", created_by: "bob", notes: "hmm" },
            ],
            counts: { good: 1, bad: 0, total: 2 },
          });
        }
        if (url.endsWith("/scoring/labels") && method === "POST") {
          const sent = JSON.parse(String(init?.body ?? "{}"));
          expect(sent).toMatchObject({ edge_sid: "sid-3", label: "bad", created_by: "admin" });
          return Response.json({
            ok: true,
            label: { id: "l3", edge_sid: "sid-3", label: "bad", created_by: "admin", notes: "bot" },
          });
        }
        if (url.endsWith("/scoring/labels/l3") && method === "PATCH") {
          return Response.json({
            ok: true,
            label: { id: "l3", edge_sid: "sid-3", label: "good", created_by: "admin", notes: "ok" },
          });
        }
        if (url.endsWith("/scoring/labels/l3") && method === "DELETE") {
          return Response.json({ ok: true });
        }
        if (url.includes("/scoring/audit") && method === "GET") {
          return Response.json({
            entries: [{ timestamp: "2026-11-27T12:00:00Z", action: "key_rotated", actor: "admin", details: {} }],
          });
        }
        return Response.json({}, { status: 404 });
      }),
    );

    const listRes = await cloudScoringControlBridge(new Request("http://localhost/api/services/svc-1/scoring/labels"));
    expect(await listRes?.json()).toEqual({
      labels: [
        { id: "l1", service_id: "svc-1", sid: "sid-1", label: "good", flagged_by: "alice", notes: "" },
        { id: "l2", service_id: "svc-1", sid: "sid-2", label: "neutral", flagged_by: "bob", notes: "hmm" },
      ],
      counts: { good: 1, bad: 0, neutral: 1 },
    });

    const createRes = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/labels", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sid: "sid-3", label: "bad", notes: "bot", flagged_by: "admin" }),
      }),
    );
    expect(await createRes?.json()).toMatchObject({ id: "l3", sid: "sid-3", label: "bad", flagged_by: "admin" });

    const patchRes = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/labels/l3", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ label: "good", notes: "ok" }),
      }),
    );
    expect(await patchRes?.json()).toMatchObject({ id: "l3", sid: "sid-3", label: "good", notes: "ok" });

    const delRes = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/labels/l3", { method: "DELETE" }),
    );
    expect(await delRes?.json()).toEqual({ status: "deleted", id: "l3" });

    const auditRes = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/audit?limit=50"),
    );
    expect(await auditRes?.json()).toEqual({
      audit: [{ id: 1, timestamp: "2026-11-27T12:00:00Z", action: "key_rotated", actor: "admin", details: {} }],
      limit: 50,
    });
  });

  it("runs retrainCloudScoring for OpenAPI POST /retrain and returns ScoringRetrainResponse, while passing through matrix payloads", async () => {
    const spy = vi.spyOn(scoringTrain, "retrainCloudScoring").mockResolvedValue({
      ok: true,
      version: "2026-11-27-001",
      kv_pushed: true,
      auc_against_labels: 0.91,
      stats: {
        sessions_in: 100,
        sessions_kept: 80,
        sessions_dropped_short: 12,
        sessions_dropped_fast: 8,
        transitions: 350,
        routes_seen: 24,
      },
    });

    const res = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/retrain?since_days=14", { method: "POST" }),
    );
    expect(spy).toHaveBeenCalledWith("svc-1", expect.objectContaining({ sinceDays: 14 }));
    expect(await res?.json()).toMatchObject({
      ok: true,
      matrix_version: "2026-11-27-001",
      since_days: 14,
      sessions_trained_on: 80,
      transitions: 350,
      vocab_size: 24,
      rejected: { too_few_events: 12, too_fast: 8, kept: 80, routes_seen: 24 },
      auc_against_labels: { auc: 0.91, threshold: 0.85, passed: true },
      default_min_auc: 0.85,
      local_matrix_saved: false,
      fos_matrix_published: true,
      matrix_kv_written: true,
      deploy_hint: null,
    });

    const passthrough = await cloudScoringControlBridge(
      new Request("http://localhost/api/services/svc-1/scoring/retrain", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ matrix: { version: "v1" } }),
      }),
    );
    expect(passthrough).toBeNull();
  });
});
