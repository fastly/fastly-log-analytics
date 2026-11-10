import { afterEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch, handleCloudAnalyticalRequest } from "./adapter";
import { getDuckDBEngine } from "./dashboard/duckdb";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

const service = {
  service_id: "svc-1",
  fos_prefix: "logs/svc-1",
  log_period: 60,
  sample_rate: 100,
  edge_only: true,
  custom_condition: "",
  log_fields: { groups: ["core"], field_overrides: {}, field_limits: {} },
  cmcd: { enabled: false, mode: "query_string", version: 1 },
  last_activated_version: 41,
};

function nativeApi({ stepStatus = "complete" }: { stepStatus?: string } = {}) {
  let current = { ...service };
  let generation = "generation-7";
  const bodies = new Map<string, unknown>();
  const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
    const path = new URL(request.url).pathname;
    if (request.method !== "GET") bodies.set(`${request.method} ${path}`, await request.clone().json());
    if (path === "/api/admin/services/svc-1" && request.method === "GET") {
      return Response.json({ service: current, generation });
    }
    if (path === "/api/admin/services/svc-1" && request.method === "PATCH") {
      const body = await request.json() as Record<string, unknown>;
      if (body.generation !== generation) return Response.json({ error: "generation_conflict" }, { status: 409 });
      const { generation: _ignored, ...changes } = body;
      current = { ...current, ...changes } as typeof current;
      generation = "generation-8";
      return Response.json({ service: current, generation });
    }
    if (path === "/api/admin/services/svc-1/reconcile") {
      const body = await request.json() as { generation?: string };
      if (body.generation !== generation) return Response.json({ error: "generation_conflict" }, { status: 409 });
      return Response.json({ job: { id: "job-1", status: "pending" } });
    }
    if (path === "/api/admin/jobs/job-1/step") {
      return Response.json({ job: { id: "job-1", status: stepStatus, error: stepStatus === "failed" ? "activation_failed" : null } });
    }
    throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
  });
  vi.stubGlobal("fetch", fetch);
  return { fetch, patched: () => current, body: (path: string, method: string) => bodies.get(`${method} ${path}`) };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Cloud Log Settings bridge", () => {
  it("reads logging settings in the self-hosted shape from the native service record", async () => {
    nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/logging-settings"));
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      ok: true, period: 60, sample_rate: 100, edge_only: true, custom_condition: "", version: 41,
      cmcd: { enabled: false, mode: "query_string", version: 1 },
    });
  });

  it("reads log fields from the native service record", async () => {
    nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/log-fields"));
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ log_fields: service.log_fields, history: [] });
  });

  it("saves log fields through a generation-checked PATCH", async () => {
    const api = nativeApi();
    const log_fields = { groups: ["core", "tls"], field_overrides: { foo: true }, field_limits: {} };
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/log-fields", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ log_fields }),
    }));
    expect(response?.status).toBe(200);
    expect(api.body("/api/admin/services/svc-1", "PATCH")).toEqual({ generation: "generation-7", log_fields });
    expect(api.patched().log_fields).toEqual(log_fields);
  });

  it("rejects a malformed log-fields body before native dispatch", async () => {
    const api = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/log-fields", {
      method: "POST", body: '{"json-error-sentinel":',
    }));
    expect(response?.status).toBe(400);
    expect(api.fetch).not.toHaveBeenCalled();
  });

  it("applies the modal's update query through PATCH then a reconcile job stream", async () => {
    const api = nativeApi();
    const response = await cloudStreamFetch(
      "http://localhost/api/services/svc-1/logging-settings/update?update_format=true&period=10&sample_rate=50&edge_only=false"
      + "&custom_condition=req.url%20!~%20%22%5E%2Fhealth%22&cmcd_enabled=true&cmcd_mode=headers&cmcd_version=2",
    );
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    expect(await response.text()).toContain('"type":"done"');
    expect(api.body("/api/admin/services/svc-1", "PATCH")).toEqual({
      generation: "generation-7", log_period: 10, sample_rate: 50, edge_only: false,
      custom_condition: 'req.url !~ "^/health"', cmcd: { enabled: true, mode: "headers", version: 2 },
    });
    expect(api.body("/api/admin/services/svc-1/reconcile", "POST")).toEqual({ generation: "generation-8", activate: true, dry_run: false });
  });

  it("streams the native failure when activation fails", async () => {
    nativeApi({ stepStatus: "failed" });
    const response = await cloudStreamFetch(
      "http://localhost/api/services/svc-1/logging-settings/update?period=60&sample_rate=100&edge_only=true&custom_condition=",
    );
    const events = await response.text();
    expect(events).toContain('"type":"error"');
    expect(events).toContain("activation_failed");
  });

  it("rejects a non-numeric period without touching the native API", async () => {
    const api = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request(
      "http://localhost/api/services/svc-1/logging-settings/update?period=abc&sample_rate=100",
    ));
    expect(response?.status).toBe(400);
    expect(api.fetch).not.toHaveBeenCalled();
  });
});
