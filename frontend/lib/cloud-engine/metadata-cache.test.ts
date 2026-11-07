import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import { ensureCloudRange } from "./range-loader";
import { getDuckDBEngine } from "./dashboard/duckdb";

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
    const path = String(input);
    if (path === "/api/log-extents") return Response.json({ latest_log_at: "2026-11-02T00:00:00Z" });
    if (path === "/api/services") return Response.json({ services: [{ service_id: "fixture-a" }] });
    return Response.json({ groups: [], fields: [] });
  });
}

function calls(fetcher: ReturnType<typeof metadataFetcher>) {
  return fetcher.mock.calls.map(([input]) => String(input)).sort();
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

  it("does not keep a failed lookup", async () => {
    const fetcher = metadataFetcher();
    fetcher.mockResolvedValueOnce(new Response("busy", { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    await expect(handleCloudAnalyticalRequest(scan())).rejects.toThrow("Cloud range discovery failed (503)");
    expect((await handleCloudAnalyticalRequest(scan()))?.ok).toBe(true);
    expect(calls(fetcher).filter((path) => path === "/api/log-extents")).toHaveLength(2);
  });
});
