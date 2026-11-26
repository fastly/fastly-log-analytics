import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import { fetchCloudMetadata } from "./metadata-cache";
import { useBootstrap } from "@/hooks/useBootstrap";
import { queryKeys } from "@/lib/api";
import { setCloudModeRuntime } from "./mode";
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

const nativeBootstrap = {
  runtime_mode: "cloud",
  user: { id: "u1", display_name: "User One", role: "admin" },
  services: [{ service_id: "fixture-a", name: "Fixture A", active_log_field_ids: ["status"] }],
  active_service_id: "fixture-a",
  active_log_field_ids: ["status"],
  log_fields_catalog: { groups: [], fields: [{ id: "status", label: "Status" }] },
  views: [{ view_id: "view-1", name: "Errors", service_id: "fixture-a", state: {} }],
  build_sha: "abc123",
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
};

function metadataFetcher() {
  return vi.fn(async (input: RequestInfo | URL) => {
    const path = input instanceof Request ? new URL(input.url).pathname : String(input);
    if (path === "/api/log-extents") return Response.json({ latest_log_at: "2026-11-02T00:00:00Z" });
    if (path === "/api/bootstrap") return Response.json(nativeBootstrap);
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

  it("refreshes bootstrap service metadata after a successful config mutation without evicting unrelated metadata", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = input instanceof Request ? new URL(input.url).pathname : String(input);
      if (path === "/api/bootstrap") return Response.json(nativeBootstrap);
      if (path === "/api/services") {
        return Response.json({ services: [{ service_id: "fixture-a" }] });
      }
      if (path === "/api/log-extents") {
        return Response.json({ latest_log_at: "2026-11-02T00:00:00Z" });
      }
      if (path === "/api/admin/services/fixture-a" && init?.method === "PATCH") {
        return Response.json({
          service: { log_fields: { custom_fields: [] } },
          generation: "generation-2",
        });
      }
      if (path === "/api/admin/services/fixture-a") {
        return Response.json({
          service: { log_fields: { custom_fields: [] } },
          generation: "generation-1",
        });
      }
      return Response.json({});
    });
    vi.stubGlobal("fetch", fetcher);

    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap")))?.ok).toBe(true);
    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/log-extents")))?.ok).toBe(true);

    const mutation = await handleCloudAnalyticalRequest(new Request(
      "http://localhost/api/services/fixture-a/log-fields",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ log_fields: { custom_fields: [] } }),
      },
    ));
    expect(mutation?.ok).toBe(true);

    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap")))?.ok).toBe(true);
    expect((await handleCloudAnalyticalRequest(new Request("http://localhost/api/log-extents")))?.ok).toBe(true);
    const paths = calls(fetcher);
    expect(paths.filter((path) => path === "/api/bootstrap")).toHaveLength(2);
    expect(paths.filter((path) => path === "/api/services")).toHaveLength(0);
    expect(paths.filter((path) => path === "/api/log-extents")).toHaveLength(1);
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
    const serve = fetcher.getMockImplementation()!;
    let failExtents = true;
    fetcher.mockImplementation(async (input) => {
      const path = input instanceof Request ? new URL(input.url).pathname : String(input);
      if (path !== "/api/log-extents" || !failExtents) return serve(input);
      failExtents = false;
      return new Response("busy", { status: 503 });
    });
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

  it("shares an anonymous metadata entry when promoted to a user identity, while isolating distinct or downgraded identities", async () => {
    const fetcher = metadataFetcher();
    vi.stubGlobal("fetch", fetcher);

    // 1. null -> "u1:admin" shares 1 underlying fetch and promotes the entry's identity.
    updateDashboardCacheIdentity(null);
    const anonRes = await fetchCloudMetadata("/api/services", new AbortController().signal);
    expect(anonRes.ok).toBe(true);
    updateDashboardCacheIdentity("u1:admin");
    const promotedRes = await fetchCloudMetadata("/api/services", new AbortController().signal);
    expect(promotedRes.ok).toBe(true);
    expect(calls(fetcher).filter((path) => path === "/api/services")).toHaveLength(1);

    // 2. "u1:admin" -> "u2:viewer" must issue a separate fetch.
    updateDashboardCacheIdentity("u2:viewer");
    const viewerRes = await fetchCloudMetadata("/api/services", new AbortController().signal);
    expect(viewerRes.ok).toBe(true);
    expect(calls(fetcher).filter((path) => path === "/api/services")).toHaveLength(2);

    // 3. "u1:admin" -> null must also issue a separate fetch.
    updateDashboardCacheIdentity("u1:admin");
    const adminExtents = await fetchCloudMetadata("/api/log-extents", new AbortController().signal);
    expect(adminExtents.ok).toBe(true);
    updateDashboardCacheIdentity(null);
    const anonExtents = await fetchCloudMetadata("/api/log-extents", new AbortController().signal);
    expect(anonExtents.ok).toBe(true);
    expect(calls(fetcher).filter((path) => path === "/api/log-extents")).toHaveLength(2);
  });
});

describe("Cloud bootstrap", () => {
  it("seeds services, catalog, and views from one native bootstrap request", async () => {
    setCloudModeRuntime(true);
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client: queryClient }, children);
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      expect(String(input)).toBe("/api/bootstrap");
      return Response.json(nativeBootstrap);
    });
    vi.stubGlobal("fetch", fetcher);

    const { result } = renderHook(() => useBootstrap(), { wrapper });

    await waitFor(() => expect(result.current.data).toEqual(nativeBootstrap));
    expect(queryClient.getQueryData(queryKeys.services())).toEqual({
      services: nativeBootstrap.services,
      _section_timings: [],
    });
    expect(queryClient.getQueryData(queryKeys.logFieldsCatalog("fixture-a")))
      .toEqual(nativeBootstrap.log_fields_catalog);
    expect(queryClient.getQueryData(queryKeys.views("fixture-a"))).toEqual(nativeBootstrap.views);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("reports NGWAF as not configured, so /security never draws the NGWAF row and then drops it", async () => {
    vi.stubGlobal("fetch", metadataFetcher());
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/bootstrap"));
    expect(await response?.json()).toMatchObject({ ngwaf_configured: false });
  });
});
