import { afterEach, describe, expect, it, vi } from "vitest";
import {
  cloudStreamFetch,
  handleCloudAnalyticalRequest,
  resetCloudMetadataCache,
} from "./adapter";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { cloudRumMutation } from "./rum-management";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

const rum = {
  enabled: true,
  endpoint_name: "Custom RUM Logs",
  log_period: 600,
  custom_condition: "",
  faro_version: "2.10.0",
  capture_vitals: true,
  capture_performance: true,
  capture_errors: true,
  capture_events: false,
  sample_rate: 0.5,
  error_sample_rate: 1,
};

function nativeApi({
  stepStatus = "complete",
  reconcileStatus = 200,
  missingJob = false,
}: { stepStatus?: string; reconcileStatus?: number; missingJob?: boolean } = {}) {
  const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
    if (request.method === "GET" && request.url.endsWith("/api/services")) {
      return Response.json({ services: [{ service_id: "svc-1" }] });
    }
    if (request.method === "GET" && request.url.endsWith("/api/admin/services/svc-1")) {
      return Response.json({ service: { rum }, generation: "generation-7" });
    }
    if (request.url.endsWith("/reconcile")) {
      return reconcileStatus === 200
        ? Response.json(missingJob ? {} : { job: { id: "job-1", status: "pending" } })
        : Response.json({ error: "generation_conflict" }, { status: reconcileStatus });
    }
    if (request.url.endsWith("/api/admin/jobs/job-1/step")) {
      return Response.json({ job: { id: "job-1", status: stepStatus, error: stepStatus === "failed" ? "activation_failed" : null } });
    }
    throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
  });
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

afterEach(() => {
  resetCloudMetadataCache();
  vi.unstubAllGlobals();
});

describe("Cloud RUM native lifecycle bridge", () => {
  it.each(["enable", "disable", "upgrade", "settings"])("rejects malformed %s JSON with a safe 400 before native dispatch", async (action) => {
    const fetch = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request(`http://localhost/api/services/svc-1/rum/${action}`, {
      method: "POST", body: '{"json-error-sentinel":',
    }));
    expect(response?.status).toBe(400);
    const body = await response!.text();
    expect(JSON.parse(body)).toEqual({ detail: { error: "invalid_json" } });
    expect(body).not.toContain("json-error-sentinel");
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["upgrade", "settings"])("rejects an empty required %s body before native dispatch", async (action) => {
    const fetch = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request(`http://localhost/api/services/svc-1/rum/${action}`, {
      method: "POST",
    }));
    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({ detail: { error: "invalid_json" } });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    ["enable", undefined], ["disable", undefined],
    ["enable", ""], ["disable", ""],
    ["enable", "null"], ["disable", "null"],
  ])("preserves the optional %s body contract for %j", async (action, body) => {
    const fetch = nativeApi();
    const response = await cloudStreamFetch(`http://localhost/api/services/svc-1/rum/${action}`, {
      method: "POST", body,
    });
    expect(await response.text()).toContain('"type":"done"');
    const call = fetch.mock.calls.find(([input]) => input instanceof Request && input.url.endsWith("/reconcile"));
    expect(await (call![0] as Request).json()).toMatchObject({
      generation: "generation-7", rum: { ...rum, enabled: action === "enable" },
    });
  });

  it("uses the public window origin for native RUM requests", async () => {
    vi.stubGlobal("window", { location: { origin: "https://rum.example.test" } });
    const fetch = nativeApi();
    const response = await cloudStreamFetch("/api/services/svc-1/rum/enable", {
      method: "POST",
      body: "{}",
    });
    await response.text();

    const nativeRequests = fetch.mock.calls
      .map(([input]) => input instanceof Request ? input.url : input instanceof URL ? input.toString() : null)
      .filter((input): input is string => input !== null);
    expect(nativeRequests).toHaveLength(2);
    expect(nativeRequests.every((url) => url.startsWith("https://rum.example.test/"))).toBe(true);
    expect(nativeRequests).toEqual([
      "https://rum.example.test/api/admin/services/svc-1",
      "https://rum.example.test/api/admin/services/svc-1/reconcile",
    ]);
  });

  it("does not convert body-read failures into JSON validation errors", async () => {
    const fetch = nativeApi();
    const request = new Request("http://localhost/api/services/svc-1/rum/settings", { method: "POST", body: "{}" });
    await request.text();
    await expect(cloudRumMutation(request, vi.fn())).rejects.toBeInstanceOf(TypeError);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not swallow SyntaxErrors from native I/O after valid JSON was parsed", async () => {
    const fetch = nativeApi();
    const error = new SyntaxError("native-response-parse-failed");
    fetch.mockRejectedValueOnce(error);
    const request = new Request("http://localhost/api/services/svc-1/rum/settings", { method: "POST", body: "{}" });
    await expect(cloudRumMutation(request, vi.fn())).rejects.toBe(error);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it.each([
    ["enable", { enabled: true }, {}],
    ["disable", { enabled: false }, {}],
    ["upgrade", { faro_version: "2.13.0" }, { version: "2.13.0", activate: true }],
  ])("executes %s through a generation-checked native reconcile job", async (action, changes, body) => {
    const fetch = nativeApi();
    const response = await cloudStreamFetch(`http://localhost/api/services/svc-1/rum/${action}`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    const events = await response.text();
    expect(events).toContain('"type":"done"');
    const call = fetch.mock.calls.find(([input]) => String(input instanceof Request ? input.url : input).endsWith("/reconcile"));
    expect(call).toBeDefined();
    const request = call![0] as Request;
    expect(await request.json()).toEqual({
      generation: "generation-7", activate: true, dry_run: false,
      rum: { ...rum, ...changes },
    });
  });

  it("does not cache stale bootstrap services while a streaming config mutation is running", async () => {
    let completeJob!: () => void;
    let jobStarted!: () => void;
    const jobStartedPromise = new Promise<void>((resolve) => { jobStarted = resolve; });
    const jobStep = new Promise<void>((resolve) => { completeJob = resolve; });
    let configured = false;
    const bootstrap = vi.fn(async () =>
      Response.json({ services: [{
        service_id: "svc-1",
        rum: { ...rum, enabled: configured },
        rum_enabled: configured,
      }] }),
    );
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request
        ? input
        : new Request(new URL(String(input), "http://localhost"), init);
      if (request.method === "GET" && request.url.endsWith("/api/bootstrap")) {
        const response = await bootstrap();
        const body = await response.json();
        return Response.json({
          runtime_mode: "cloud",
          user: { id: "fixture-user", role: "admin" },
          ...body,
          active_service_id: "svc-1",
          active_log_field_ids: [],
          log_fields_catalog: { groups: [], fields: [] },
          views: [],
          build_sha: "test-build",
          ngwaf_configured: false,
          fastly_bots_available: true,
          settings: {
            needs_login: false,
            is_remote_analyst: false,
            mask_ips: false,
            initializing: false,
          },
          debug_state: {
            query_debug_visibility: "disabled",
            api_call_debug_visibility: "disabled",
          },
        });
      }
      if (request.method === "GET" && request.url.endsWith("/api/admin/services/svc-1")) {
        return Response.json({ service: { rum: { ...rum, enabled: false } }, generation: "generation-7" });
      }
      if (request.method === "POST" && request.url.endsWith("/api/admin/services/svc-1/reconcile")) {
        return Response.json({ job: { id: "job-1", status: "pending" } });
      }
      if (request.method === "POST" && request.url.endsWith("/api/admin/jobs/job-1/step")) {
        jobStarted();
        await jobStep;
        configured = true;
        return Response.json({ job: { id: "job-1", status: "complete" } });
      }
      throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
    });
    vi.stubGlobal("fetch", fetch);

    const initial = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/bootstrap"),
    );
    expect(await initial?.json()).toMatchObject({
      services: [{ service_id: "svc-1", rum_enabled: false }],
    });

    const mutation = await cloudStreamFetch(
      "http://localhost/api/services/svc-1/rum/enable",
      { method: "POST", body: "{}" },
    );
    await jobStartedPromise;

    const during = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/bootstrap"),
    );
    expect(await during?.json()).toMatchObject({
      services: [{ service_id: "svc-1", rum_enabled: false }],
    });
    expect(bootstrap).toHaveBeenCalledTimes(2);

    completeJob();
    await mutation.text();

    const after = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/bootstrap"),
    );
    expect(await after?.json()).toMatchObject({
      services: [{ service_id: "svc-1", rum_enabled: true }],
    });
    expect(bootstrap).toHaveBeenCalledTimes(3);
  });

  it("does not report settings saved when native reconciliation fails", async () => {
    nativeApi({ stepStatus: "failed" });
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/settings", {
      method: "POST", body: JSON.stringify({ capture_errors: false, custom_condition: "req.url != \"/private\"" }),
    }));
    expect(response?.status).toBe(502);
    expect(await response?.json()).toEqual({ detail: { error: "activation_failed" } });
  });

  it("saves settings only after native reconciliation completes and preserves unedited configuration", async () => {
    const fetch = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/settings", {
      method: "POST", body: JSON.stringify({ capture_errors: false, custom_condition: "req.url != \"/private\"" }),
    }));
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({ ok: true });
    expect(fetch.mock.calls.some(([input]) => String(input).includes("/api/admin/jobs/job-1/step"))).toBe(true);
    const call = fetch.mock.calls.find(([input]) => input instanceof Request && input.url.endsWith("/reconcile"));
    expect(await (call![0] as Request).json()).toMatchObject({
      rum: { ...rum, capture_errors: false, custom_condition: "req.url != \"/private\"" },
    });
  });

  it("preserves native generation-conflict errors instead of claiming success", async () => {
    nativeApi({ reconcileStatus: 409 });
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/settings", {
      method: "POST", body: "{}",
    }));
    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({ error: "generation_conflict" });
  });

  it.each(["remove_cloud_files", "remove_bucket"])("rejects unsupported Cloud disable %s instead of silently ignoring it", async (option) => {
    const fetch = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/disable", {
      method: "POST", body: JSON.stringify({ [option]: true }),
    }));
    expect(response?.status).toBe(400);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not fabricate successful settings when the native response contains no job", async () => {
    nativeApi({ missingJob: true });
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/settings", {
      method: "POST", body: "{}",
    }));
    expect(response?.status).toBe(502);
    expect(await response?.json()).toEqual({ detail: { error: "Missing native RUM reconcile job" } });
  });

  it("honors native dry-run and non-activation requests", async () => {
    const fetch = nativeApi();
    const response = await cloudStreamFetch("http://localhost/api/services/svc-1/rum/upgrade", {
      method: "POST", body: JSON.stringify({ version: "2.13.0", dry_run: true, activate: false }),
    });
    await response.text();
    const call = fetch.mock.calls.find(([input]) => input instanceof Request && input.url.endsWith("/reconcile"));
    expect(await (call![0] as Request).json()).toMatchObject({ dry_run: true, activate: false });
  });

  it("reports failed native lifecycle jobs as SSE errors without a successful done frame and invalidates metadata cache", async () => {
    const fetchMock = nativeApi({ stepStatus: "failed" });
    await handleCloudAnalyticalRequest(new Request("http://localhost/api/services"));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const response = await cloudStreamFetch("http://localhost/api/services/svc-1/rum/enable", {
      method: "POST", body: "{}",
    });
    const events = await response.text();
    expect(events).toContain('"type":"error"');
    expect(events).toContain("activation_failed");
    expect(events).not.toContain('"type":"done"');
    await handleCloudAnalyticalRequest(new Request("http://localhost/api/services"));
    const servicesCalls = fetchMock.mock.calls.filter(([input]) =>
      String(input instanceof Request ? input.url : input).endsWith("/api/services"),
    );
    expect(servicesCalls).toHaveLength(2);
  });

  it("applies faro_version together with RUM capture settings in a single reconcile request", async () => {
    const fetchMock = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services/svc-1/rum/settings", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ capture_vitals: false, sample_rate: 0.5, faro_version: "2.14.0" }),
    }));
    expect(response?.status).toBe(200);
    const reconcileCall = fetchMock.mock.calls.find(([input]) =>
      input instanceof Request && input.url.endsWith("/reconcile"),
    );
    expect(reconcileCall).toBeDefined();
    expect(await (reconcileCall![0] as Request).json()).toMatchObject({
      generation: "generation-7",
      rum: { capture_vitals: false, sample_rate: 0.5, faro_version: "2.14.0" },
    });
  });
});
