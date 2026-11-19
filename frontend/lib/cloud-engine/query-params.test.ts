import { describe, expect, it } from "vitest";
import {
  analyticsFiltersToSql,
  filtersPayloadToPredicates,
  queryRange,
  readRequestParams,
  rangeServiceSql,
} from "./query-params";

describe("Cloud query parameter helpers", () => {
  it("merges URL and JSON request parameters", async () => {
    const request = new Request("https://example.test/api/example?start=old&source=query", {
      method: "POST",
      body: JSON.stringify({ start: "new", filters: { status: { values: ["200"], mode: "include" } } }),
    });

    await expect(readRequestParams(request)).resolves.toEqual({
      start: "new",
      source: "query",
      filters: { status: { values: ["200"], mode: "include" } },
    });
  });

  it("builds deterministic filters, range and service predicates", () => {
    const filters = {
      status: { values: ["200", "201"], mode: "include" as const },
      status_1: { values: ["500"], mode: "exclude" as const },
    };

    expect(filtersPayloadToPredicates(filters)).toEqual([
      { field: "status", value: "200", mode: "include" },
      { field: "status", value: "201", mode: "include" },
      { field: "status", value: "500", mode: "exclude" },
    ]);
    expect(rangeServiceSql({
      start_time: "2026-01-01T00:00:00.000Z",
      end_time: "2026-01-01T01:00:00.000Z",
      service_id: "svc'one",
    })).toEqual([
      "1=1",
      "ts_ms >= 1767225600000",
      "ts_ms < 1767229200000",
      "(service_id = 'svc''one' OR service_id IS NULL)",
    ].join(" AND "));
    expect(analyticsFiltersToSql(filters)).toContain("status");
  });

  it("resolves the established range aliases and default lookback", () => {
    const range = queryRange(
      { start: "2026-01-01T00:00:00Z", end: "2026-01-01T02:00:00Z" },
      24,
    );
    expect(range).toEqual({
      start: "2026-01-01T00:00:00Z",
      end: "2026-01-01T02:00:00Z",
    });

    expect(queryRange({ end_time: "2026-01-02T00:00:00Z" }, 24)).toEqual({
      start: "2026-01-01T00:00:00.000Z",
      end: "2026-01-02T00:00:00Z",
    });
  });
});
