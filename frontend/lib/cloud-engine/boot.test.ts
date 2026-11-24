// @vitest-environment node
import { beforeEach, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ metadata: [] as string[], prewarms: 0 }));
vi.mock("./metadata-cache", () => ({
  fetchCloudMetadata: vi.fn(async (path: string) => {
    calls.metadata.push(path);
    return new Response("{}");
  }),
}));
vi.mock("./dashboard/duckdb", () => ({
  getDuckDBEngine: () => ({ prewarm: async () => { calls.prewarms++; } }),
}));

import { bootCloudEngine } from "./boot";

beforeEach(() => {
  calls.metadata = [];
  calls.prewarms = 0;
});

it("starts the range lookup and the page's DuckDB as soon as a raw-scan page boots", () => {
  bootCloudEngine("/insights");
  expect(calls.metadata).toEqual(["/api/log-extents"]);
  expect(calls.prewarms).toBe(1);
});

it("leaves DuckDB cold on pages that summaries answer", () => {
  bootCloudEngine("/network");
  expect(calls.metadata).toEqual(["/api/log-extents"]);
  expect(calls.prewarms).toBe(0);
});
