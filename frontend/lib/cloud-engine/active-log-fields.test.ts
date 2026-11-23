// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";

const services = vi.hoisted(() => ({ body: {} as unknown, status: 200, calls: [] as string[] }));
vi.mock("./metadata-cache", () => ({
  fetchCloudMetadata: vi.fn(async (path: string) => {
    services.calls.push(path);
    return new Response(JSON.stringify(services.body), { status: services.status });
  }),
}));

import { cloudActiveLogFieldIds } from "./active-log-fields";

const signal = new AbortController().signal;

beforeEach(() => {
  services.calls = [];
  services.status = 200;
  services.body = {
    services: [
      { service_id: "a", active_log_field_ids: ["status", "ttl"] },
      { service_id: "b", active_log_field_ids: ["status", "edge_score"] },
    ],
  };
});

describe("cloudActiveLogFieldIds", () => {
  it("reads the named service's stored ids from the cached services listing", async () => {
    expect(await cloudActiveLogFieldIds("b", signal)).toEqual(new Set(["status", "edge_score"]));
    expect(services.calls).toEqual(["/api/services"]);
  });

  it("uses the first service when none is named, and an empty set for an unknown one", async () => {
    expect(await cloudActiveLogFieldIds(undefined, signal)).toEqual(new Set(["status", "ttl"]));
    expect(await cloudActiveLogFieldIds("zzz", signal)).toEqual(new Set());
  });

  it("fails loudly when the listing can't be read", async () => {
    services.status = 500;
    await expect(cloudActiveLogFieldIds("a", signal)).rejects.toThrow(/500/);
  });
});
