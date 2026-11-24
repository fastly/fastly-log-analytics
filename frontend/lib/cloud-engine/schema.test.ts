// @vitest-environment node
import { expect, it } from "vitest";
import { parquetFixture } from "./dashboard/test-fixtures";
import { parquetColumns, sampleStats, type SchemaColumn } from "./schema";

const field = (id: string, duckdb_type: string) => ({
  id,
  group: null,
  duckdb_type,
});

it("maps catalog types to the DuckDB types the converter writes", () => {
  const columns = parquetColumns({
    groups: [],
    fields: [
      field("timestamp", "TIMESTAMP"),
      field("status", "USMALLINT"),
      field("resp_bytes", "UBIGINT"),
      field("ttfb", "FLOAT"),
      field("lat", "DOUBLE"),
      field("edge", "BOOLEAN"),
      field("host", "VARCHAR"),
      { id: "untyped", group: null },
    ],
  });
  expect(columns.map((c) => [c.name, c.type, c.catalogType])).toEqual([
    ["timestamp", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP"],
    ["status", "BIGINT", "USMALLINT"],
    ["resp_bytes", "BIGINT", "UBIGINT"],
    ["ttfb", "DOUBLE", "FLOAT"],
    ["lat", "DOUBLE", "DOUBLE"],
    ["edge", "BOOLEAN", "BOOLEAN"],
    ["host", "VARCHAR", "VARCHAR"],
  ]);
});

it("computes populated share, distinct count and min/max from a sample file", async () => {
  const bytes = await parquetFixture(
    [
      {
        timestamp: Date.parse("2026-10-30T12:00:01Z"),
        status: 200,
        cache: "HIT",
        resp_bytes: 10,
        host: "a.example",
        edge: true,
      },
      {
        timestamp: Date.parse("2026-10-30T12:00:05Z"),
        status: 503,
        cache: null,
        resp_bytes: 9,
        host: "b.example",
        edge: false,
      },
      {
        timestamp: Date.parse("2026-10-30T12:00:03Z"),
        status: 200,
        cache: "MISS",
        resp_bytes: null,
        host: "a.example",
        edge: null,
      },
      {
        timestamp: Date.parse("2026-10-30T12:00:02Z"),
        status: null,
        cache: null,
        resp_bytes: null,
        host: null,
        edge: null,
      },
    ],
    ["country"],
  );
  const columns: SchemaColumn[] = parquetColumns({
    groups: [],
    fields: [
      field("timestamp", "TIMESTAMP"),
      field("status", "USMALLINT"),
      field("cache", "VARCHAR"),
      field("resp_bytes", "UBIGINT"),
      field("edge", "BOOLEAN"),
      field("country", "VARCHAR"),
    ],
  });
  const sample = await sampleStats(bytes, columns);
  expect(sample.rows).toBe(4);
  expect(sample.stats.get("timestamp")).toEqual({
    populated: 100,
    unique: 4,
    min: "2026-10-30T12:00:01.000Z",
    max: "2026-10-30T12:00:05.000Z",
  });
  expect(sample.stats.get("status")).toEqual({
    populated: 75,
    unique: 2,
    min: "200",
    max: "503",
  });
  expect(sample.stats.get("resp_bytes")).toEqual({
    populated: 50,
    unique: 2,
    min: "9",
    max: "10",
  });
  expect(sample.stats.get("cache")).toEqual({
    populated: 50,
    unique: 2,
    min: "HIT",
    max: "MISS",
  });
  expect(sample.stats.get("edge")).toEqual({
    populated: 50,
    unique: 2,
    min: "false",
    max: "true",
  });
  expect(sample.stats.has("country")).toBe(false);
});
