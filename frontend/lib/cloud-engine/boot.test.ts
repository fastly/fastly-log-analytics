// @vitest-environment node
import { beforeEach, expect, it, vi } from "vitest";

const calls = vi.hoisted(() => ({ metadata: [] as string[], prewarms: 0, workerPrewarms: 0 }));
vi.mock("./metadata-cache", () => ({
  fetchCloudMetadata: vi.fn(async (path: string) => {
    calls.metadata.push(path);
    return new Response("{}");
  }),
}));
vi.mock("./dashboard/duckdb", () => ({
  getDuckDBEngine: () => ({ prewarm: async () => { calls.prewarms++; } }),
}));
vi.mock("./dashboard/client", () => ({
  prewarmRollupWorker: () => { calls.workerPrewarms++; },
}));

import { bootCloudEngine } from "./boot";

beforeEach(() => {
  calls.metadata = [];
  calls.prewarms = 0;
  calls.workerPrewarms = 0;
});

it("starts the range lookup, rollup worker, and DuckDB as soon as a raw-scan page boots", () => {
  bootCloudEngine("/insights");
  expect(calls.metadata).toEqual(["/api/log-extents"]);
  expect(calls.prewarms).toBe(1);
  expect(calls.workerPrewarms).toBe(1);
});

it("leaves DuckDB cold on pages that summaries answer while still warming the rollup worker", () => {
  bootCloudEngine("/network");
  expect(calls.metadata).toEqual(["/api/log-extents"]);
  expect(calls.prewarms).toBe(0);
  expect(calls.workerPrewarms).toBe(1);
});
