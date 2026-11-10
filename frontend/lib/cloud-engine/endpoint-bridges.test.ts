import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch, handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import contract from "./cloud-openapi.json";
import { cloudLoadingStore } from "./loading-progress";
import { NATIVE_ROUTES, isNativeCloudRoute } from "./native-routes";
import { ensureCloudRange } from "./range-loader";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { sealSessionToken } from "./sessions";
import { updateDashboardCacheIdentity } from "./dashboard/sources";
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

  it("passes saved views to the Compute origin after moving this browser's legacy local views there once", async () => {
    localStorage.setItem("flp-saved-views-v2:fixture-a", JSON.stringify([
      { id: "v1", service_id: "fixture-a", name: "Errors", filters_json: '{"status":["500"]}', page: "/dashboard" },
      { id: "other", service_id: "fixture-b", name: "Not mine", filters_json: "{}" },
      { id: "broken", service_id: "fixture-a", name: 42, filters_json: "{}" },
    ]));
    const fetcher = vi.fn(async () => Response.json({ id: "v1", status: "success" }, { status: 201 }));
    vi.stubGlobal("fetch", fetcher);
    expect(await handleCloudAnalyticalRequest(request("/api/views/fixture-a"))).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
    const [url, init] = fetcher.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/views/");
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toMatchObject({ id: "v1", service_id: "fixture-a", name: "Errors", page: "/dashboard" });
    expect(localStorage.getItem("flp-saved-views-v2:fixture-a")).toBeNull();
    expect(await handleCloudAnalyticalRequest(request("/api/views/fixture-a"))).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
    for (const [path, method] of [["/api/views/", "POST"], ["/api/views/v1", "DELETE"]] as const) {
      expect(await handleCloudAnalyticalRequest(request(path, method, method === "POST" ? { name: "x" } : undefined))).toBeNull();
    }
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("keeps legacy local views when the origin refuses them for a reason other than validation", async () => {
    localStorage.setItem("flp-saved-views-v2:fixture-a", JSON.stringify([
      { id: "v1", service_id: "fixture-a", name: "Errors", filters_json: "{}" },
    ]));
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: "down" }, { status: 503 })));
    expect(await handleCloudAnalyticalRequest(request("/api/views/fixture-a"))).toBeNull();
    expect(localStorage.getItem("flp-saved-views-v2:fixture-a")).not.toBeNull();
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

describe("Cloud RUM status and versions services lookup", () => {
  const services = { services: [{ service_id: "fixture-a", rum: { enabled: true, faro_version: "2.10.0" } }] };

  function heldServices() {
    const pending: Array<{ resolve: (response: Response) => void; signal?: AbortSignal | null }> = [];
    const fetcher = vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input) === "/api/services"
      ? new Promise<Response>((resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal!.reason), { once: true });
        pending.push({ resolve, signal: init?.signal });
      })
      : Promise.resolve(Response.json({ versions: ["2.13.0", "2.10.0"], default: "2.13.0" })));
    vi.stubGlobal("fetch", fetcher);
    const servicesCalls = () => fetcher.mock.calls.filter(([input]) => String(input) === "/api/services").length;
    return { fetcher, pending, servicesCalls };
  }

  it("shares one in-flight services read between the page's concurrent status and versions requests", async () => {
    const { pending, servicesCalls } = heldServices();
    const status = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    const versions = cloudServiceStatus(request("/api/services/fixture-a/rum/versions"));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    pending[0].resolve(Response.json(services));
    expect(await (await status)!.json()).toMatchObject({ enabled: true, faro_version: "2.10.0" });
    expect(await (await versions)!.json()).toMatchObject({ current: "2.10.0", update_available: true });
    expect(servicesCalls()).toBe(1);
  });

  it("lets one caller abort without cancelling the shared read for the others", async () => {
    const { pending, servicesCalls } = heldServices();
    const owner = new AbortController();
    const first = cloudServiceStatus(new Request("http://localhost/api/services/fixture-a/rum/status", { signal: owner.signal }));
    const second = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    owner.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });
    expect(pending[0].signal?.aborted).not.toBe(true);
    pending[0].resolve(Response.json(services));
    expect(await (await second)!.json()).toMatchObject({ enabled: true });
    expect(servicesCalls()).toBe(1);
  });

  it("cancels the shared read once every caller has aborted, and does not keep it", async () => {
    const { pending, servicesCalls } = heldServices();
    const owner = new AbortController();
    const aborted = cloudServiceStatus(new Request("http://localhost/api/services/fixture-a/rum/status", { signal: owner.signal }));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    owner.abort();
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
    expect(pending[0].signal?.aborted).toBe(true);
    const next = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    pending[1].resolve(Response.json(services));
    expect((await next)!.status).toBe(200);
    expect(servicesCalls()).toBe(2);
  });

  it("does not keep a failed read, so the next request asks again", async () => {
    const { pending, servicesCalls } = heldServices();
    const first = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    const second = cloudServiceStatus(request("/api/services/fixture-a/rum/versions"));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    pending[0].resolve(Response.json({ error: "busy" }, { status: 503 }));
    expect((await first)!.status).toBe(503);
    expect((await second)!.status).toBe(503);
    const retry = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    pending[1].resolve(Response.json(services));
    expect((await retry)!.status).toBe(200);
    expect(servicesCalls()).toBe(2);
  });

  it("does not share a read across a change of signed-in identity", async () => {
    const { pending, servicesCalls } = heldServices();
    const before = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    updateDashboardCacheIdentity("other-user:admin");
    const after = cloudServiceStatus(request("/api/services/fixture-a/rum/status"));
    await vi.waitFor(() => expect(pending).toHaveLength(2));
    pending.forEach(({ resolve }) => resolve(Response.json(services)));
    await Promise.all([before, after]);
    expect(servicesCalls()).toBe(2);
  });
});
