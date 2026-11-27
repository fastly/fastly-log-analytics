import { afterEach, describe, expect, it, vi } from "vitest";
import type { components } from "@/types/api.generated";
import { cloudLogAccounting } from "./admin-reads";
import { cloudCustomFields } from "./custom-fields";
import { cloudSchema } from "./log-fields";
import { isNativeCloudRoute } from "./native-routes";
import { buildCloudShareStatusPayload, shareAuditRows } from "./share-audit";

describe("Cloud contract parity fixes", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("wraps custom-fields 422 validation errors in ErrorEnvelope shape", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input instanceof Request ? input.url : input);
        if (url.includes("/api/admin/services/svc-1")) {
          return Response.json({
            service: { service_id: "svc-1", log_fields: { custom_fields: [] } },
            generation: "gen-1",
          });
        }
        if (url.includes("/api/admin/log-fields/custom/validate")) {
          return Response.json({ valid: false, errors: ["invalid VCL"] });
        }
        throw new Error(`Unexpected fetch: ${url}`);
      }),
    );

    const missingName = await cloudCustomFields(
      new Request("http://localhost/api/services/svc-1/custom-fields", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(missingName?.status).toBe(422);
    expect(await missingName?.json()).toMatchObject({
      detail: {
        error: "validation_error",
        messages: ["Field name is required"],
        errors: ["Field name is required"],
      },
    });

    const systemCreate = await cloudCustomFields(
      new Request("http://localhost/api/services/svc-1/custom-fields", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "fastly_req_id" }),
      }),
    );
    expect(systemCreate?.status).toBe(422);
    expect(await systemCreate?.json()).toMatchObject({
      detail: {
        error: "validation_error",
        messages: [expect.stringContaining("system-managed")],
      },
    });

    const invalidCreate = await cloudCustomFields(
      new Request("http://localhost/api/services/svc-1/custom-fields", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "x_custom", vcl_log_expression: "bad" }),
      }),
    );
    expect(invalidCreate?.status).toBe(422);
    expect(await invalidCreate?.json()).toMatchObject({
      detail: {
        error: "validation_error",
        messages: ["invalid VCL"],
        errors: ["invalid VCL"],
      },
    });
  });

  it("registers NGWAF workspace routes in NATIVE_ROUTES", () => {
    expect(isNativeCloudRoute("GET", "/api/provision/ngwaf-workspaces")).toBe(true);
    expect(isNativeCloudRoute("GET", "/api/admin/provision/ngwaf-workspaces")).toBe(true);
    expect(isNativeCloudRoute("PATCH", "/api/provision/services/svc-1/ngwaf-workspace")).toBe(true);
    expect(isNativeCloudRoute("PATCH", "/api/admin/provision/services/svc-1/ngwaf-workspace")).toBe(true);
  });

  it("emits LogAccountingResponse contract fields when populated and unconfigured", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json({ from: 0, to: 86400, days: [] })),
    );
    const unconfiguredRes = await cloudLogAccounting(
      new Request("http://localhost/api/admin/log-accounting"),
    );
    const unconfigured = await unconfiguredRes.json();
    expect(unconfigured).toMatchObject({
      available: false,
      by: "day",
      buckets: [],
      totals: { fastly_requests: 0, our_rows: 0, gap: 0, gap_pct: 0 },
      catchup: null,
      sustained_loss: null,
    });
    expect(typeof unconfigured.reason).toBe("string");

    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          from: 0,
          to: 86400,
          days: [{ day: 0, expected_rows: 100, ingested_rows: 80 }],
        }),
      ),
    );
    const okRes = await cloudLogAccounting(
      new Request("http://localhost/api/admin/log-accounting"),
    );
    const ok = await okRes.json();
    expect(ok.catchup).toBeNull();
    expect(ok.buckets[0]).toMatchObject({
      fastly_requests: 100,
      our_rows: 80,
      file_count: 0,
      gap: 20,
      gap_pct: 0.2,
    });
  });

  it("builds Cloud share status with ip_address em-dash and filters audit rows on actor, display_name, or role", () => {
    const payload = buildCloudShareStatusPayload(
      {
        invites: [
          {
            id: "inv-1",
            display_name: "Alice",
            role: "analyst-no-pii",
            created_at: 1_700_000_000,
            expires_at: 1_700_086_400,
            revoked: false,
          },
        ],
      },
      {
        users: [
          {
            id: "usr-1",
            display_name: "Bob",
            role: "analyst",
            created_at: 1_700_000_000,
            revoked: false,
            passkeys: [{ id: "pk-1" }, { id: "pk-2" }],
          },
        ],
      },
      { services: [{ service_id: "svc-1", name: "Primary" }] },
      "https://example.fastly.net",
    );

    expect(payload.invites[0]).toMatchObject({
      id: "inv-1",
      name: "Alice",
      role: "analyst-no-pii",
      auth_method: "passkey",
      pii_policy: { mask_ips: true },
    });
    expect(payload.sessions[0]).toMatchObject({
      session_id: "usr-1",
      name: "Bob",
      role: "analyst",
      ip_address: "—",
      auth_method: "Passkey (2)",
    });

    const rows = [
      {
        timestamp: 1000,
        action: "login",
        actor: "usr-1",
        display_name: "Bob Smith",
        role: "analyst",
        domain: "auth",
      },
      {
        timestamp: 2000,
        action: "login",
        actor: "usr-2",
        display_name: "Carol",
        role: "admin",
        domain: "auth",
      },
    ];
    expect(
      shareAuditRows(rows, { from: 0, to: 5000, eventType: null, email: "Bob Smith", limit: 10 }),
    ).toHaveLength(1);
    expect(
      shareAuditRows(rows, { from: 0, to: 5000, eventType: null, email: "admin", limit: 10 }),
    ).toHaveLength(1);
  });

  it("strips PII columns from cloudSchema when maskIps is true", () => {
    const catalog: components["schemas"]["LogFieldsCatalogResponse"] = {
      groups: [],
      fields: [
        { id: "timestamp", label: "Timestamp", description: "", sql_type: "TIMESTAMP", duckdb_type: "TIMESTAMP", group: "CORE" },
        { id: "url", label: "URL", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "CORE" },
        { id: "ip", label: "IP", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "CORE" },
        { id: "client_ip", label: "Client IP", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "CORE" },
        { id: "origin_ip", label: "Origin IP", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "CORE" },
        { id: "cookie_session", label: "Session", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "CORE" },
      ],
      presets: {},
      insights: [],
    };
    const masked = cloudSchema(catalog, undefined, true);
    expect(masked.schema.map((c) => c.name)).toEqual(["timestamp", "url"]);
    const unmasked = cloudSchema(catalog, undefined, false);
    expect(unmasked.schema.map((c) => c.name)).toContain("ip");
  });
});
