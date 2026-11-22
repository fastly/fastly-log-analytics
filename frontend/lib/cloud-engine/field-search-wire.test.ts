// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { handleCloudAnalyticalRequest } from "./adapter";
import { DashboardDuckDBClient } from "./dashboard/client";
import { searchFieldValues } from "./dashboard/engine";
import { fixtureSource, manifestEntry, parquetFixture } from "./dashboard/test-fixtures";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("Cloud field-search wire", () => {
  it("returns actual values in the requested service, time and other-filter context", async () => {
    const from = "2026-10-29T10:00:00Z";
    const to = "2026-10-29T10:01:00Z";
    const key = "search-service/processed/request/search.parquet";
    const bytes = await parquetFixture([
      { timestamp: Date.parse(from), status: 200, cache: "HIT", resp_bytes: 10, host: "alpha.example", country: "US" },
      { timestamp: Date.parse(from), status: 200, cache: "HIT", resp_bytes: 10, host: "alpha.example", country: "DE" },
      { timestamp: Date.parse(to), status: 200, cache: "HIT", resp_bytes: 10, host: "alpha.example", country: "US" },
    ]);
    const source = fixtureSource([{
      files: [manifestEntry(key, from, "2026-10-29T10:05:00Z", 3)],
      next_cursor: null, complete: true,
    }], { [key]: bytes });
    const search = vi.spyOn(DashboardDuckDBClient.prototype, "search")
      .mockImplementation((options) => searchFieldValues(source, options));
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/field-values", {
      method: "POST",
      headers: { "x-service-id": "search-service" },
      body: JSON.stringify({
        field: "host", search: "ALPHA", start_time: from, end_time: to, limit: 1,
        filters: {
          country: { mode: "include", values: ["US"] },
          host: { mode: "exclude", values: ["alpha.example"] },
        },
      }),
    }));
    expect(await response!.json()).toMatchObject({
      field: "host", values: [{ value: "alpha.example", count: 1 }],
    });
    expect(search.mock.calls[0][0]).toMatchObject({
      from, to, limit: 1, serviceId: "search-service",
    });
  });

  it("searches custom fields with their catalog types and other custom filters", async () => {
    const from = "2026-10-29T10:00:00Z";
    const to = "2026-10-29T10:05:00Z";
    const key = "pq/custom-field-search.parquet";
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(from), new Date(from)] },
      { name: "tenant", type: "STRING", data: ["alpha", "other"] },
      { name: "plan", type: "DOUBLE", data: [2, 3] },
    ] });
    const source = fixtureSource([{
      files: [manifestEntry(key, from, to, 2)], next_cursor: null, complete: true,
    }], { [key]: bytes });
    vi.spyOn(DashboardDuckDBClient.prototype, "search")
      .mockImplementation((options) => searchFieldValues(source, options));
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
      String(input) === "/api/services"
        ? Response.json({ services: [{ service_id: "search-service", log_fields: {
          custom_fields: [
            { name: "tenant", sql_type: "VARCHAR" },
            { name: "plan", sql_type: "DOUBLE" },
          ],
        } }] })
        : Response.json({ fields: [], groups: [] })));
    const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/dashboard/field-values", {
      method: "POST",
      headers: { "x-service-id": "search-service" },
      body: JSON.stringify({
        field: "tenant", search: "", start_time: from, end_time: to,
        filters: {}, custom_filters: { plan: { mode: "include", values: ["2.0"] } },
      }),
    }));
    expect(await response!.json()).toMatchObject({
      field: "tenant", values: [{ value: "alpha", count: 1 }],
    });
  });
});
