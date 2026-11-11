import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import { ensureCloudRange } from "./range-loader";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { setDashboardCacheIdentity } from "./dashboard/client";
import { dashboardCacheIdentity, dashboardCacheScope, updateDashboardCacheIdentity } from "./dashboard/sources";
import { cloudCacheIdentity } from "./cache-identity";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

function scan() {
  return new Request("http://localhost/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql: "SELECT 1 AS n FROM logs", service_id: "fixture-a" }),
  });
}

function metadataFetcher() {
  return vi.fn(async (input: RequestInfo | URL) => {
    const path = input instanceof Request ? new URL(input.url).pathname : String(input);
    if (path === "/api/log-extents") return Response.json({ latest_log_at: "2026-11-02T00:00:00Z" });
    if (path === "/api/services") return Response.json({ services: [{ service_id: "fixture-a" }] });
    if (path === "/api/auth/session") return Response.json({ user: { id: "u1", role: "admin" } });
    return Response.json({ groups: [], fields: [] });
  });
}

function calls(fetcher: ReturnType<typeof metadataFetcher>) {
  return fetcher.mock.calls.map(([input]) => input instanceof Request ? new URL(input.url).pathname : String(input)).sort();
}

beforeEach(() => {
  resetCloudMetadataCache();
  vi.spyOn(getDuckDBEngine(), "executeSql").mockResolvedValue([{ n: 1 }]);
  vi.mocked(ensureCloudRange).mockImplementation(async () => getDuckDBEngine().readContext());
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); resetCloudMetadataCache(); });

describe("Cloud scan metadata", () => {
  it("shares extents, catalog and services across the scans of one page load", async () => {
    const fetcher = metadataFetcher();
    vi.stubGlobal("fetch", fetcher);
    expect((await handleCloudAnalyticalRequest(scan()))?.ok).toBe(true);
    expect((await handleCloudAnalyticalRequest(scan()))?.ok).toBe(true);
    expect(calls(fetcher)).toEqual(["/api/log-extents", "/api/log-fields/catalog", "/api/services"]);
  });

  it("refetches after the short reuse window and after any write", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      const fetcher = metadataFetcher();
      vi.stubGlobal("fetch", fetcher);
      await handleCloudAnalyticalRequest(scan());
      vi.setSystemTime(Date.now() + 60_000);
      await handleCloudAnalyticalRequest(scan());
      expect(calls(fetcher)).toHaveLength(6);
      await handleCloudAnalyticalRequest(new Request("http://localhost/api/admin/share/start", { method: "POST" }));
      await handleCloudAnalyticalRequest(scan());
      expect(calls(fetcher)).toHaveLength(9);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps sharing one lookup across a page load whose scans run past the reuse window, up to a hard cap", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      const fetcher = metadataFetcher();
      vi.stubGlobal("fetch", fetcher);
      for (let scanIndex = 0; scanIndex < 6; scanIndex++) {
        await handleCloudAnalyticalRequest(scan());
        vi.setSystemTime(Date.now() + 10_000);
      }
      expect(calls(fetcher)).toHaveLength(3);
      // Back-to-back use never holds an answer past a minute from when it was fetched.
      await handleCloudAnalyticalRequest(scan());
      expect(calls(fetcher)).toHaveLength(6);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not keep a failed lookup", async () => {
    const fetcher = metadataFetcher();
    fetcher.mockResolvedValueOnce(new Response("busy", { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    await expect(handleCloudAnalyticalRequest(scan())).rejects.toThrow("Cloud range discovery failed (503)");
    expect((await handleCloudAnalyticalRequest(scan()))?.ok).toBe(true);
    expect(calls(fetcher).filter((path) => path === "/api/log-extents")).toHaveLength(2);
  });

  it("fixes the cache identity before bootstrap answers, so the page's first scans are not superseded", async () => {
    vi.stubGlobal("fetch", metadataFetcher());
    updateDashboardCacheIdentity(null);
    const scopeBefore = dashboardCacheScope();
    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap")))?.ok).toBe(true);
    expect(dashboardCacheIdentity()).toBe("u1:admin");
    const scope = dashboardCacheScope();
    expect(scope).not.toBe(scopeBefore);
    // The page's own identity sync after bootstrap must not rotate the scope under in-flight discovery.
    await setDashboardCacheIdentity(cloudCacheIdentity({ user: { id: "u1", role: "admin" } }));
    expect(dashboardCacheScope()).toBe(scope);
  });

  it("shares services with bootstrap and the page's own extents read with the scans", async () => {
    const fetcher = metadataFetcher();
    vi.stubGlobal("fetch", fetcher);
    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap")))?.ok).toBe(true);
    const extents = await handleCloudAnalyticalRequest(new Request("http://localhost/api/log-extents"));
    expect(await extents?.json()).toEqual({ latest_log_at: "2026-11-02T00:00:00Z" });
    expect((await handleCloudAnalyticalRequest(scan()))?.ok).toBe(true);
    const counts = calls(fetcher).reduce<Record<string, number>>((acc, path) => ({ ...acc, [path]: (acc[path] ?? 0) + 1 }), {});
    expect(counts["/api/services"]).toBe(1);
    expect(counts["/api/log-extents"]).toBe(1);
  });
});

describe("Cloud bootstrap", () => {
  it("reports NGWAF as not configured, so /security never draws the NGWAF row and then drops it", async () => {
    vi.stubGlobal("fetch", metadataFetcher());
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap"));
    expect(await response?.json()).toMatchObject({ ngwaf_configured: false });
  });
});
