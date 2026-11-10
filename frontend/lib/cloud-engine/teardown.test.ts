import { afterEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch, handleCloudAnalyticalRequest } from "./adapter";
import { cloudTeardownOptions } from "./teardown";
import { getDuckDBEngine } from "./dashboard/duckdb";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

function nativeApi({ stepStatus = "complete" }: { stepStatus?: string } = {}) {
  const bodies = new Map<string, unknown>();
  const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
    const path = new URL(request.url).pathname;
    if (request.method !== "GET") bodies.set(`${request.method} ${path}`, await request.clone().json());
    if (path === "/api/admin/services/svc-1" && request.method === "GET") {
      return Response.json({ service: { service_id: "svc-1" }, generation: "generation-5" });
    }
    if (path === "/api/provision/teardown") {
      const body = await request.json() as Record<string, unknown>;
      if (body.confirm_service_id !== body.service_id) return Response.json({ error: "typed_service_id_required" }, { status: 400 });
      if (body.generation !== "generation-5") return Response.json({ error: "generation_conflict" }, { status: 409 });
      return Response.json({ job: { id: "job-9", status: "pending" } });
    }
    if (path === "/api/admin/jobs/job-9/step") {
      return Response.json({ job: { id: "job-9", status: stepStatus, error: stepStatus === "failed" ? "fastly_api_error" : null } });
    }
    throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
  });
  vi.stubGlobal("fetch", fetch);
  return { fetch, body: (path: string) => bodies.get(`POST ${path}`) as Record<string, unknown> | undefined };
}

const dialogBody = {
  service_id: "svc-1", remove_logging: true, remove_rum: true, remove_cdn: true, remove_bucket: true,
  remove_cloud_files: true, remove_cache: true, token: "fastly-token-sentinel",
};

afterEach(() => vi.unstubAllGlobals());

describe("Cloud teardown bridge", () => {
  it("translates the dialog body into Cloud's options with generation and typed confirmation", async () => {
    const api = nativeApi();
    const response = await cloudStreamFetch("http://localhost/api/provision/teardown", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(dialogBody),
    });
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    expect(await response.text()).toContain('"type":"done"');
    expect(api.body("/api/provision/teardown")).toEqual({
      service_id: "svc-1", confirm_service_id: "svc-1", token: "fastly-token-sentinel", generation: "generation-5",
      remove_logging: true, remove_snippets: true, delete_fos_keys: true, delete_data: true, delete_fos_bucket: true,
    });
  });

  it("keeps credentials and data when the dialog keeps logging active", () => {
    expect(cloudTeardownOptions({ remove_logging: false, remove_rum: true, remove_bucket: true, remove_cloud_files: true })).toEqual({
      remove_logging: false, remove_snippets: true, delete_fos_keys: false, delete_data: false, delete_fos_bucket: false,
    });
  });

  it("streams the native failure", async () => {
    nativeApi({ stepStatus: "failed" });
    const events = await (await cloudStreamFetch("http://localhost/api/provision/teardown", {
      method: "POST", body: JSON.stringify(dialogBody),
    })).text();
    expect(events).toContain('"type":"error"');
    expect(events).toContain("fastly_api_error");
  });

  it("rejects a missing token before touching the native API", async () => {
    const api = nativeApi();
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/provision/teardown", {
      method: "POST", body: JSON.stringify({ ...dialogBody, token: "" }),
    }));
    expect(response?.status).toBe(400);
    expect(api.fetch).not.toHaveBeenCalled();
  });
});
