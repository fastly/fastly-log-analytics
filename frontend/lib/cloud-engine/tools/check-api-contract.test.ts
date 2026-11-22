import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { expect, it } from "vitest";
import contract from "../cloud-openapi.json";
import { undeclaredApiCalls } from "./check-api-contract";

function sources(targetPath: string): string[] {
  const st = statSync(targetPath);
  if (st.isFile()) {
    return /\.[jt]sx?$/.test(targetPath) && !targetPath.includes(".test.")
      ? [targetPath]
      : [];
  }
  return readdirSync(targetPath, { withFileTypes: true }).flatMap((entry) => {
    const path = join(targetPath, entry.name);
    if (entry.isDirectory()) return sources(path);
    return /\.[jt]sx?$/.test(path) && !path.includes(".test.") ? [path] : [];
  });
}

it("kept cloud-engine modules call only declared Goal1 API paths and methods", () => {
  const violations = [
    "lib/cloud-engine/dashboard",
    "lib/cloud-engine/telemetry",
    "lib/cloud-engine/usage",
    "lib/cloud-engine/passkeys.ts",
  ].flatMap((directory) =>
    sources(directory).flatMap((path) =>
      undeclaredApiCalls(readFileSync(path, "utf8"), contract).map(
        (error) => `${path}: ${error}`,
      ),
    ),
  );
  expect(violations).toEqual([]);
});

it("rejects undeclared paths, wrong methods, and SSE instead of silently accepting them", () => {
  expect(
    undeclaredApiCalls(
      'client.GET("/api/dashboard"); client.POST("/api/log-extents"); new EventSource("/api/log-extents/stream")',
      contract,
    ),
  ).toEqual([
    "GET /api/dashboard",
    "/api/dashboard",
    "POST /api/log-extents",
    "EventSource",
    "/api/log-extents/stream",
  ]);
});
