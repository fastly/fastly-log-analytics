import { expect, type Page } from "@playwright/test";

async function stepJobToCompletion(
  page: Page,
  origin: string,
  jobId: string,
): Promise<Record<string, unknown>> {
  let lastJob: Record<string, unknown> = {};
  for (let i = 0; i < 60; i++) {
    const res = await page.context().request.post(`/api/admin/jobs/${jobId}/step`, {
      headers: { origin },
      data: {},
    });
    expect(res.status()).toBe(200);
    const body = (await res.json()) as { job: Record<string, unknown> };
    lastJob = body.job;
    if (body.job.status === "complete") {
      return body.job;
    }
    expect(body.job.status).not.toBe("failed");
    if (typeof body.job.retry_at === "number" && body.job.retry_at > 0) {
      const waitMs = Math.max(100, body.job.retry_at * 1000 - Date.now() + 50);
      await new Promise((resolve) => setTimeout(resolve, Math.min(waitMs, 2000)));
    }
  }
  throw new Error(`Job ${jobId} did not complete within 60 steps: ${JSON.stringify(lastJob)}`);
}

export async function goal2Native(page: Page) {
  const origin = new URL(page.url()).origin;
  const req = page.context().request;

  // 1. Read-only token is rejected by native Rust token validation
  const roRes = await req.post("/api/provision/services", {
    headers: { origin },
    data: { token: "fixture-read-only" },
  });
  expect(roRes.status()).toBe(403);

  // 2. Valid fixture token lists VCL services in customer account
  const listRes = await req.post("/api/provision/services", {
    headers: { origin },
    data: { token: "fixture-token" },
  });
  expect(listRes.status()).toBe(200);
  const { services } = (await listRes.json()) as {
    services: { id: string; name: string }[];
  };
  expect(services.some((s) => s.id === "fixtureService")).toBe(true);

  // 3. Preview generated log format with CMCD v2 HTTP headers
  const catRes = await req.get("/api/log-fields/catalog");
  expect(catRes.status()).toBe(200);
  const { default_config } = (await catRes.json()) as {
    default_config: Record<string, unknown>;
  };

  const cmcdV2 = {
    enabled: true,
    version: 2,
    mode: "headers",
  };
  const previewRes = await req.post("/api/admin/log-fields/preview", {
    headers: { origin },
    data: { log_fields: default_config, cmcd: cmcdV2 },
  });
  expect(previewRes.status()).toBe(200);
  const previewBody = (await previewRes.json()) as {
    format: string;
    format_length: number;
  };
  expect(previewBody.format).toContain("cmcd_sid");
  expect(previewBody.format_length).toBeGreaterThan(0);

  // 4. Execute provisioning job and step to completion
  const execRes = await req.post("/api/provision/execute", {
    headers: { origin },
    data: {
      service_id: "fixtureService",
      token: "fixture-token",
      fos_bucket: "fos-fixture-logs",
      fos_region: "us-east-1",
      fos_prefix: "",
      logging_enabled: true,
      logging_endpoint_name: "flp-s3-logs",
      log_period: 60,
      sample_rate: 100,
      edge_only: false,
      custom_condition: "",
      log_fields: default_config,
      cmcd: cmcdV2,
    },
  });
  expect(execRes.status()).toBe(200);
  const execBody = (await execRes.json()) as {
    job: { id: string };
    generation: string;
  };
  await stepJobToCompletion(page, origin, execBody.job.id);

  // 5. Verify /admin renders the provisioned service state cleanly
  await page.reload();
  await expect(page.getByTestId("admin-overview-root")).toBeVisible();

  // 6. Validate custom field and patch sample_rate + custom_fields, then dry-run reconcile
  const customField = {
    name: "browser_fixture_value",
    label: "Browser Fixture Value",
    vcl: "%{req.http.X-Synthetic}V",
    vcl_log_expression: "%{req.http.X-Synthetic}V",
    parquet_type: "Utf8",
    duckdb_type: "VARCHAR",
    value_type: "string",
    bytes_estimate: 40,
  };
  const valFieldRes = await req.post("/api/admin/log-fields/custom/validate", {
    headers: { origin },
    data: { field: customField, existing_names: [] },
  });
  expect(valFieldRes.status()).toBe(200);
  expect((await valFieldRes.json()).valid).toBe(true);

  const svcGet = await req.get("/api/admin/services/fixtureService");
  expect(svcGet.status()).toBe(200);
  const svcState = (await svcGet.json()) as {
    generation: string;
    service: { log_fields: Record<string, unknown> };
  };
  const updatedFields = {
    ...svcState.service.log_fields,
    custom_fields: [
      ...((svcState.service.log_fields.custom_fields as unknown[]) ?? []),
      customField,
    ],
  };
  const patchRes = await req.patch("/api/admin/services/fixtureService", {
    headers: { origin },
    data: {
      generation: svcState.generation,
      sample_rate: 25,
      log_fields: updatedFields,
    },
  });
  expect(patchRes.status()).toBe(200);
  const patched = (await patchRes.json()) as { generation: string };

  const reconcileRes = await req.post(
    "/api/admin/services/fixtureService/reconcile",
    {
      headers: { origin },
      data: {
        generation: patched.generation,
        dry_run: true,
        activate: false,
      },
    },
  );
  expect(reconcileRes.status()).toBe(200);
  const recJob = (await reconcileRes.json()) as { job: { id: string } };
  await stepJobToCompletion(page, origin, recJob.job.id);

  // 7. Teardown service and verify token never leaked into browser storage
  const preTeardown = await req.get("/api/admin/services/fixtureService");
  const preTdBody = (await preTeardown.json()) as { generation: string };
  const tdRes = await req.post("/api/provision/teardown", {
    headers: { origin },
    data: {
      service_id: "fixtureService",
      confirm_service_id: "fixtureService",
      token: "fixture-token",
      generation: preTdBody.generation,
      remove_logging: true,
      remove_snippets: true,
      delete_fos_keys: true,
      delete_data: false,
      delete_fos_bucket: false,
    },
  });
  expect(tdRes.status()).toBe(200);
  const tdJob = (await tdRes.json()) as { job: { id: string } };
  await stepJobToCompletion(page, origin, tdJob.job.id);

  const stored = await page.evaluate(() =>
    JSON.stringify([
      Object.entries(localStorage),
      Object.entries(sessionStorage),
    ]),
  );
  expect(stored).not.toContain("fixture-token");
}
