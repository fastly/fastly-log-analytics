// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { handleCloudAnalyticalRequest, resetCloudMetadataCache } from "./adapter";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { clearManifestCache } from "./dashboard/manifest";
import { resetRangeLoaderCache } from "./range-loader";

describe("RUM concurrent analytics + live-events manifest deduplication", () => {
  afterEach(() => {
    clearManifestCache();
    resetRangeLoaderCache();
    resetCloudMetadataCache();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("issues only one GET /api/files?dataset=rum&...&view=raw when analytics and live-events load concurrently", async () => {
    const engine = getDuckDBEngine();
    vi.spyOn(engine, "prewarm").mockResolvedValue();
    vi.spyOn(engine, "executeSql").mockResolvedValue([]);

    let releaseRawListing!: (res: Response) => void;
    const rawListingRequests: string[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        "http://localhost",
      );
      if (url.pathname === "/api/log-extents") {
        return Response.json({
          earliest_log_at: "2027-01-14T00:00:00.000Z",
          latest_log_at: "2027-01-15T10:00:00.000Z",
        });
      }
      if (url.pathname === "/api/files" && url.searchParams.get("dataset") === "rum") {
        const view = url.searchParams.get("view");
        if (view === "raw") {
          rawListingRequests.push(`${url.pathname}${url.search}`);
          return new Promise<Response>((resolve) => {
            releaseRawListing = resolve;
          });
        }
        return Response.json({ files: [], next_cursor: null, complete: true });
      }
      throw new Error(`Unexpected fetch: ${url.pathname}${url.search}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const analyticsPromise = handleCloudAnalyticalRequest(
      new Request(
        "http://localhost/api/services/svc-1/rum/analytics?start_time=2027-01-15T08:00:00.000Z&end_time=2027-01-15T09:00:00.000Z",
      ),
    );
    const liveEventsPromise = handleCloudAnalyticalRequest(
      new Request(
        "http://localhost/api/services/svc-1/rum/live-events?start_time=2027-01-15T08:00:00.000Z&end_time=2027-01-15T09:00:00.000Z",
      ),
    );

    // Wait for both concurrent reads to hit the shared main-thread rum/raw manifest lookup.
    await vi.waitFor(() => expect(rawListingRequests).toHaveLength(1));
    releaseRawListing(Response.json({ files: [], next_cursor: null, complete: true }));

    const [analyticsRes, liveEventsRes] = await Promise.all([
      analyticsPromise,
      liveEventsPromise,
    ]);
    expect(analyticsRes?.status).toBe(200);
    expect(liveEventsRes?.status).toBe(200);
    expect(rawListingRequests).toHaveLength(1);
    expect(new URL(rawListingRequests[0], "http://localhost").searchParams.get("service_id")).toBe("svc-1");
  });
});
