import { afterEach, describe, expect, it, vi } from "vitest";
import { cloudStreamFetch } from "./adapter";
import { cloudProvisionBody } from "./provision";
import { getDuckDBEngine } from "./dashboard/duckdb";

vi.mock("./range-loader", async (importOriginal) => ({
  ...await importOriginal<typeof import("./range-loader")>(),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

const wizardBody = {
  token: "fastly-token-sentinel", service_id: "svc-1", service_name: "Demo", endpoint_name: "fla-logs",
  fos_region: "us-east", fos_bucket_name: "demo-bucket", fos_prefix: "logs/", sample_rate: "100", edge_only: true,
  custom_condition: "", log_period: "60", rum_log_period: "300", cdn_service_name: "demo-cdn", cdn_shield: "",
  enable_cron_sync: true, delete_after: true, log_retention_days: 30, rum_retention_days: 30,
  log_fields: JSON.stringify({ groups: ["A"], field_overrides: {} }), cmcd_enabled: true, cmcd_mode: "headers",
  cmcd_version: 2, logging_enabled: true, rum_enabled: false, rum_custom_condition: "", faro_version: null,
  cdn_url: "https://demo.global.ssl.fastly.net",
};

afterEach(() => vi.unstubAllGlobals());

describe("Cloud provisioning bridge", () => {
  it("drops self-hosted-only keys and folds flat cmcd keys into Cloud's shape", () => {
    const body = cloudProvisionBody(wizardBody);
    for (const key of ["cdn_service_name", "cdn_shield", "cdn_url", "enable_cron_sync", "cmcd_enabled", "cmcd_mode", "cmcd_version"]) {
      expect(body).not.toHaveProperty(key);
    }
    expect(body).toMatchObject({
      token: "fastly-token-sentinel", service_id: "svc-1", fos_bucket_name: "demo-bucket", sample_rate: "100",
      rum_log_period: 300, log_fields: { groups: ["A"], field_overrides: {} },
      cmcd: { enabled: true, mode: "headers", version: 2 },
    });
  });

  it("posts the translated body natively and streams the provisioning job", async () => {
    let posted: Record<string, unknown> | undefined;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(new URL(String(input), "http://localhost"), init);
      const path = new URL(request.url).pathname;
      if (path === "/api/provision/execute") {
        posted = await request.json() as Record<string, unknown>;
        if ("cdn_service_name" in posted || "cmcd_enabled" in posted) return Response.json({ error: "invalid_input" }, { status: 400 });
        return Response.json({ job: { id: "job-2", status: "pending" } });
      }
      if (path === "/api/admin/jobs/job-2/step") return Response.json({ job: { id: "job-2", status: "complete" } });
      throw new Error(`Unexpected native request: ${request.method} ${request.url}`);
    }));
    const response = await cloudStreamFetch("http://localhost/api/provision/execute", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(wizardBody),
    });
    expect(await response.text()).toContain('"type":"done"');
    expect(posted?.cmcd).toEqual({ enabled: true, mode: "headers", version: 2 });
  });
});
