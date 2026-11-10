import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch, handleCloudAnalyticalRequest } from "./adapter";
import { setDeleteDataPollInterval } from "./delete-data";
import { getDuckDBEngine } from "./dashboard/duckdb";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

type Job = { status: string; error?: string | null; messages?: { at: number; text: string }[]; deleted?: Record<string, number> };

function nativeApi(timeline: Job[]) {
  const generation = "generation-3";
  let tick = 0;
  const bodies = new Map<string, unknown>();
  const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
    const path = new URL(request.url).pathname;
    if (request.method !== "GET") bodies.set(`${request.method} ${path}`, await request.clone().json());
    if (path === "/api/admin/services/svc-1" && request.method === "GET") {
      return Response.json({ service: { service_id: "svc-1" }, generation });
    }
    if (path === "/api/admin/services/svc-1/delete-data" && request.method === "POST") {
      const body = await request.json() as { generation?: string; confirm_service_id?: string };
      if (body.generation !== generation) return Response.json({ error: "generation_conflict" }, { status: 409 });
      if (body.confirm_service_id !== "svc-1") return Response.json({ error: "confirm_service_id_mismatch" }, { status: 400 });
      return Response.json({ job: timeline[0], service: { service_id: "svc-1" }, generation: "generation-4" });
    }
    if (path === "/api/admin/services/svc-1/delete-data" && request.method === "GET") {
      tick = Math.min(tick + 1, timeline.length - 1);
      return Response.json(timeline[tick]);
    }
    throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
  });
  vi.stubGlobal("fetch", fetch);
  return { fetch, body: (path: string, method: string) => bodies.get(`${method} ${path}`) };
}

function post(serviceId: string, confirm: string) {
  return cloudStreamFetch(`http://localhost/api/admin/reset-logs?service_id=${serviceId}`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ confirm }),
  });
}

beforeEach(() => setDeleteDataPollInterval(1));
afterEach(() => {
  vi.unstubAllGlobals();
  setDeleteDataPollInterval(5000);
});

describe("Cloud Delete Data bridge", () => {
  it("starts the native job with the current generation and streams it to completion", async () => {
    const api = nativeApi([
      { status: "fencing", messages: [{ at: 1, text: "Fencing ingest." }] },
      { status: "resetting", messages: [{ at: 1, text: "Fencing ingest." }, { at: 2, text: "Reset committed." }] },
      { status: "complete", messages: [{ at: 1, text: "Fencing ingest." }, { at: 2, text: "Reset committed." }], deleted: { objects: 12 } },
    ]);
    const response = await post("svc-1", "svc-1");
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    const events = await response.text();
    expect(api.body("/api/admin/services/svc-1/delete-data", "POST")).toEqual({ confirm_service_id: "svc-1", generation: "generation-3" });
    expect(events).toContain("Fencing ingest.");
    expect(events).toContain("Reset committed.");
    expect(events.match(/Fencing ingest\./g)).toHaveLength(1);
    expect(events).toContain('"type":"done"');
    expect(events).toContain("Deleted 12 objects.");
  });

  it("streams the native failure", async () => {
    nativeApi([{ status: "fencing" }, { status: "failed", error: "purge_failed" }]);
    const events = await (await post("svc-1", "svc-1")).text();
    expect(events).toContain('"type":"error"');
    expect(events).toContain("purge_failed");
  });

  it("rejects a mismatched confirmation before touching the native API", async () => {
    const api = nativeApi([{ status: "fencing" }]);
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/admin/reset-logs?service_id=svc-1", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ confirm: "svc-2" }),
    }));
    expect(response?.status).toBe(400);
    expect(api.fetch).not.toHaveBeenCalled();
  });
});
