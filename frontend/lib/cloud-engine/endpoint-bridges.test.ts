import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch, handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import contract from "./cloud-openapi.json";
import { cloudLoadingStore } from "./loading-progress";
import { NATIVE_ROUTES, isNativeCloudRoute } from "./native-routes";
import { ensureCloudRange } from "./range-loader";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { sealSessionToken } from "./sessions";
import { cloudServiceStatus } from "./service-bridges";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

function request(path: string, method = "GET", body?: unknown, serviceId?: string) {
  return new Request(`http://localhost${path}`, {
    method,
    headers: { "content-type": "application/json", ...(serviceId ? { "x-service-id": serviceId } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
}

beforeEach(() => {
  localStorage.clear();
  cloudLoadingStore.reset();
  vi.mocked(ensureCloudRange).mockImplementation(async () => getDuckDBEngine().readContext());
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); cloudLoadingStore.reset(); resetCloudMetadataCache(); });

describe("Cloud endpoint bridges", () => {
  it("uses the native RUM default version without claiming an up-to-date SDK needs updating", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) =>
      String(input) === "/api/services"
        ? Response.json({ services: [{ service_id: "fixture-a", rum: { enabled: true, faro_version: "2.13.0" } }] })
        : Response.json({ versions: ["2.13.0", "2.10.0"], default: "2.13.0" }),
    );
    vi.stubGlobal("fetch", fetcher);
    expect(await (await cloudServiceStatus(request("/api/services/fixture-a/rum/versions")))!.json()).toEqual({
      available: ["2.13.0", "2.10.0"], current: "2.13.0", latest: "2.13.0", update_available: false,
    });
    fetcher.mockImplementation(async (input: RequestInfo | URL) =>
      String(input) === "/api/services"
        ? Response.json({ services: [{ service_id: "fixture-a", rum: { enabled: true, faro_version: "2.10.0" } }] })
        : Response.json({ versions: ["2.13.0", "2.10.0"], default: "2.13.0" }),
    );
    expect(await (await cloudServiceStatus(request("/api/services/fixture-a/rum/versions")))!.json())
      .toMatchObject({ current: "2.10.0", latest: "2.13.0", update_available: true });
    fetcher.mockImplementation(async () => Response.json({ error: "unauthorized" }, { status: 401 }));
    expect((await cloudServiceStatus(request("/api/services/fixture-a/rum/versions")))!.status).toBe(401);
  });

  it("preserves native auth and rejects unsupported stream requests without network leakage", async () => {
    const fetcher = vi.fn(async () => Response.json({ user: { id: "fixture-user" } }));
    vi.stubGlobal("fetch", fetcher);
    expect(await (await handleCloudAnalyticalRequest(request("/api/auth/session")))!.json())
      .toEqual({ user: { id: "fixture-user" } });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect((await cloudStreamFetch("/api/auth/session")).status).toBe(200);
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect((await cloudStreamFetch("/api/admin/unsupported", { method: "POST" })).status).toBe(501);
    expect((await cloudStreamFetch("/api/admin/unsupported")).status).toBe(501);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("bridges native quarantine pagination and refuses false healthy accounting", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.includes("logged-traffic")) return Response.json({ from: 0, to: 86400, days: [] });
      return Response.json({
        complete: path.includes("cursor="), next_cursor: "fixture-next",
        items: [{ key: path.includes("cursor=") ? "raw/rum/failure" : "raw/request/failure",
          size: 12, status: "unreadable", reason: "invalid gzip", first_seen: "2026-01-01T00:00:00Z" }],
      });
    });
    vi.stubGlobal("fetch", fetcher);
    expect(await (await handleCloudAnalyticalRequest(request("/api/admin/quarantine/summary")))!.json())
      .toMatchObject({ total_items: 2, total_bytes: 24, request_items: 1, rum_items: 1 });
    expect(await (await handleCloudAnalyticalRequest(request("/api/admin/quarantine?offset=1&limit=1")))!.json())
      .toMatchObject({ total: 2, items: [{ source_type: "rum", evidence_metadata_available: false }] });
    expect(await (await handleCloudAnalyticalRequest(request("/api/admin/log-accounting")))!.json())
      .toMatchObject({ available: false, measurement_basis: "native_sample_adjusted_daily" });
    fetcher.mockImplementation(async () => Response.json({
      from: 0, to: 86400, days: [{ day: 0, expected_rows: 100, ingested_rows: 80 }],
    }));
    expect(await (await handleCloudAnalyticalRequest(request("/api/admin/log-accounting")))!.json())
      .toMatchObject({ by: "day", totals: { gap: 20, gap_pct: 0.2 } });
  });

  it("keeps the exact native passthrough allowlist in parity with the Compute contract", () => {
    const declared = Object.entries(contract.paths).flatMap(([path, operations]) =>
      Object.keys(operations).filter((key) => key !== "parameters").map((method) => `${method.toUpperCase()} ${path}`),
    ).sort();
    expect(NATIVE_ROUTES.flatMap(([methods, path]) => methods.split(",").map((method) => `${method} ${path}`)).sort()).toEqual(declared);
    expect(isNativeCloudRoute("POST", "/api/admin/invites/fixture/revoke")).toBe(true);
    expect(isNativeCloudRoute("DELETE", "/api/admin/invites/fixture/revoke")).toBe(false);
    expect(isNativeCloudRoute("POST", "/api/admin/services/fixture/unknown")).toBe(false);
  });

  it("persists actual saved-view wire shapes, scoped by service, through both URL contracts", async () => {
    const create = await handleCloudAnalyticalRequest(request("/api/views/", "POST", {
      service_id: "fixture-a", name: "Errors", filters_json: '{"status":["500"]}', page: "/dashboard",
    }));
    expect(create?.status).toBe(201);
    const saved = await create!.json();
    expect(saved).toEqual({ id: expect.any(String), status: "success" });
    const list = await handleCloudAnalyticalRequest(request("/api/views/fixture-a"));
    expect(await list!.json()).toEqual([expect.objectContaining({ id: saved.id, name: "Errors", service_id: "fixture-a" })]);
    expect(await (await handleCloudAnalyticalRequest(request("/api/views/fixture-b")))!.json()).toEqual([]);
    expect((await handleCloudAnalyticalRequest(request(`/api/views/fixture-b/${saved.id}`, "DELETE")))?.status).toBe(404);
    expect((await handleCloudAnalyticalRequest(request(`/api/views/fixture-a/${saved.id}`, "PUT", { name: "Updated" })))?.status).toBe(200);
    expect((await handleCloudAnalyticalRequest(request(`/api/views/${saved.id}`, "DELETE", undefined, "fixture-a")))?.status).toBe(204);
    expect(await (await handleCloudAnalyticalRequest(request("/api/views/fixture-a")))!.json()).toEqual([]);
  });

  it("validates malformed view writes and preserves POST upsert and partial PUT semantics", async () => {
    for (const body of [null, [], { name: 42, filters_json: "{}" }, { name: "View", filters_json: 42 },
      { name: "View", filters_json: "{}", start_time: 42 }]) {
      expect((await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", body)))?.status).toBe(422);
    }
    const create = await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", {
      name: "Original", filters_json: "{}", page: "/dashboard",
    }));
    const { id } = await create!.json();
    for (const body of [{ name: 42 }, { filters_json: [] }, { name: " " }]) {
      expect((await handleCloudAnalyticalRequest(request(`/api/views/fixture-a/${id}`, "PUT", body)))?.status).toBe(422);
    }
    await handleCloudAnalyticalRequest(request(`/api/views/fixture-a/${id}`, "PUT", { name: "Renamed" }));
    expect(await (await handleCloudAnalyticalRequest(request("/api/views/fixture-a")))!.json())
      .toEqual([expect.objectContaining({ id, name: "Renamed", filters_json: "{}", page: "/dashboard" })]);
    await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", {
      id, name: "Replaced", filters_json: "[]",
    }));
    expect(await (await handleCloudAnalyticalRequest(request("/api/views/fixture-a")))!.json())
      .toEqual([expect.objectContaining({ id, name: "Replaced", filters_json: "[]" })]);
  });

  it("accepts service-scoped POST and refuses unscoped writes and storage failures", async () => {
    expect((await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", { name: "View", filters_json: "{}" })))?.status).toBe(201);
    expect((await handleCloudAnalyticalRequest(request("/api/views/", "POST", { name: "View", filters_json: "{}" })))?.status).toBe(400);
    expect((await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", { service_id: "fixture-b", name: "View", filters_json: "{}" })))?.status).toBe(400);
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: () => { throw new Error("quota"); },
    });
    expect((await handleCloudAnalyticalRequest(request("/api/views/fixture-a", "POST", { name: "View", filters_json: "{}" })))?.status).toBe(503);
  });

  it("normalizes catalog SQL types and includes active custom fields without physical metrics", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).includes("/api/services")) return Response.json({ services: [{
        service_id: "fixture-a", log_fields: { custom_fields: [
          { name: "tenant", label: "Tenant", duckdb_type: "DOUBLE", enabled: true },
          { name: "disabled_field", label: "Disabled", enabled: false },
        ] },
      }] });
      return Response.json({ groups: [], fields: [
        { id: "status", label: "Status", description: "", group: null, duckdb_type: "BIGINT" },
        { id: "requests", label: "Requests", description: "", group: "METRICS", duckdb_type: "BIGINT" },
      ] });
    });
    vi.stubGlobal("fetch", fetcher);
    const catalog = await (await handleCloudAnalyticalRequest(request("/api/log-fields/catalog?service_id=fixture-a")))!.json();
    expect(catalog.fields.map((field: { id: string }) => field.id)).toEqual(["status", "requests", "tenant"]);
    expect(catalog.fields[0].sql_type).toBe("BIGINT");
    expect(catalog.fields.find((field: { id: string }) => field.id === "tenant"))
      .toMatchObject({ sql_type: "DOUBLE", duckdb_type: "DOUBLE" });
    const schema = await (await handleCloudAnalyticalRequest(request("/api/schema", "GET", undefined, "fixture-a")))!.json();
    expect(schema.table_name).toBe("logs");
    expect(schema.schema.map((column: { name: string }) => column.name)).toEqual(["status", "tenant"]);
    expect(schema.schema[0].type).toBe("BIGINT");
  });

  it.each([
    ["POST", "/api/admin/share/start"],
    ["PATCH", "/api/admin/share/settings"],
    ["POST", "/api/services/fixture/rum/not-supported"],
    ["PATCH", "/api/services/fixture/scoring/threshold"],
    ["GET", "/api/admin/queries/summary"],
    ["GET", "/api/not-a-compute-endpoint"],
    ["POST", "/api/log-extents"],
  ])("returns an explicit unsupported response without network leakage: %s %s", async (method, path) => {
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    const response = await handleCloudAnalyticalRequest(request(path, method));
    expect(response?.status).toBe(501);
    expect((await response!.json()).detail.error).toBe("cloud_endpoint_unsupported");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("tracks session/range/listing discovery through settlement and failure", async () => {
    let resolve!: (response: Response) => void;
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>((done) => { resolve = done; })));
    for (const [path, stage] of [["/api/auth/session", "session"], ["/api/log-extents", "range"], ["/api/files", "listing"]]) {
      const pending = handleCloudAnalyticalRequest(request(path));
      expect(cloudLoadingStore.getSnapshot()?.stage).toBe(stage);
      resolve(Response.json({}));
      await pending;
      expect(cloudLoadingStore.getSnapshot()).toBeNull();
    }
    resetCloudMetadataCache();
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
    await expect(handleCloudAnalyticalRequest(request("/api/log-extents"))).rejects.toThrow("offline");
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });

  it("loads the requested range before executing SQL and does not conceal scan errors", async () => {
    const engine = getDuckDBEngine();
    vi.mocked(ensureCloudRange).mockResolvedValue({
      ...engine.readContext(), executeSql: vi.fn().mockResolvedValue([{ requests: 7 }]),
    });
    const response = await handleCloudAnalyticalRequest(request("/api/query", "POST", {
      sql: "SELECT count(*) AS requests FROM logs", start_time: "2026-11-01T00:00:00Z", end_time: "2026-11-02T00:00:00Z",
    }));
    expect(ensureCloudRange).toHaveBeenCalledWith("2026-11-01T00:00:00Z", "2026-11-02T00:00:00.001Z", expect.any(AbortSignal), ["request"], { customFields: [], serviceId: undefined });
    expect((await response!.json()).data).toEqual([{ requests: 7 }]);
    vi.mocked(ensureCloudRange).mockRejectedValueOnce(new Error("incomplete listing"));
    await expect(handleCloudAnalyticalRequest(request("/api/query", "POST", {
      sql: "SELECT * FROM logs", start_time: "2026-11-01T00:00:00Z", end_time: "2026-11-02T00:00:00Z",
    }))).rejects.toThrow("incomplete listing");
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });

  it("loads singleton session tokens with an inclusive last-event scan bound", async () => {
    const at = "2026-11-01T00:00:00.000Z";
    const token = sealSessionToken({ ip: "192.0.2.1", ja4: null, s: at, e: at, svc: "fixture-service" });
    vi.spyOn(getDuckDBEngine(), "executeSql").mockResolvedValue([]);
    vi.stubGlobal("fetch", vi.fn(async (path: RequestInfo | URL) => Response.json(
      String(path).includes("/api/services") ? { services: [{ service_id: "fixture-service" }] } : { fields: [], groups: [] },
    )));
    const response = await handleCloudAnalyticalRequest(request("/api/sessions/detail", "POST", {
      service_id: "fixture-service", session_token: token,
    }));
    expect(response?.ok).toBe(true);
    expect(ensureCloudRange).toHaveBeenLastCalledWith(at, "2026-11-01T00:00:00.001Z",
      expect.any(AbortSignal), ["request"], { serviceId: "fixture-service", customFields: [] });
  });
});
