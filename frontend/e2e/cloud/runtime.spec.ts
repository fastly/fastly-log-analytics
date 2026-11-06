import { test, expect } from "@playwright/test";
import { randomUUID } from "node:crypto";
import { virtualAuthenticator } from "./authenticator";
import { goal2Native } from "./goal2-native";

test.setTimeout(180_000);

test("direct requests without the front key are rejected", async ({
  request,
}) => {
  const direct =
    process.env.FLP_DIRECT_COMPUTE_URL ||
    (process.env.FLP_E2E_NATIVE === "1"
      ? `http://127.0.0.1:${process.env.FLP_E2E_NATIVE_BACKEND_PORT || "8787"}`
      : undefined);
  test.skip(
    !direct,
    "Set FLP_DIRECT_COMPUTE_URL to verify direct Compute denial",
  );
  const response = await request.get(new URL("/api/setup", direct!).toString());
  expect(response.status()).toBe(403);
});

test("native missing-store setup exposes only key names and CLI instructions and keeps public pages usable", async ({
  browser,
}) => {
  test.skip(
    process.env.FLP_E2E_NATIVE !== "1",
    "Requires the isolated missing-store runtime",
  );
  const context = await browser.newContext({
    baseURL: `https://localhost:${Number(process.env.FLP_E2E_NATIVE_TLS_PORT || "8443") + 1}`,
    ignoreHTTPSErrors: true,
  });
  try {
    const page = await context.newPage();
    const calls: string[] = [];
    page.on("request", (request) => calls.push(new URL(request.url()).pathname));
    await page.goto("/setup");
    await expect(page).toHaveURL(/\/share-login/);
    await expect(page.getByTestId("share-login-page")).toBeVisible();
    expect(calls).not.toContain("/api/admin/system-checks");

    const response = await context.request.get("/api/setup");
    expect(response.status()).toBe(200);
    const status = await response.json();
    expect(status.complete).toBe(false);
    expect(status.accepted).toBe(false);
    expect(status.missing).toHaveLength(1);
    expect(status.missing[0].name).toBe("fastly_token");
    expect(status.missing[0].command).toMatch(
      /fastly secret-store-entry create .*--name=fastly_token --stdin/,
    );
    expect(Object.keys(status.missing[0]).sort()).toEqual([
      "command",
      "name",
      "store",
    ]);
    expect(JSON.stringify(status)).not.toContain(
      "native-test-material-not-a-real-secret",
    );

    const sessionRes = await context.request.get("/api/auth/session");
    expect(sessionRes.status()).toBe(503);
    for (const path of ["/share-login.html", "/usage.html"]) {
      expect((await context.request.get(path)).status(), path).toBe(200);
    }
  } finally {
    await context.close();
  }
});

test("runtime-backed setup, passkeys, invites, role changes, revocation and logout", async ({
  browser,
  baseURL,
  request,
}) => {
  expect((await request.post("/__test/reset")).ok()).toBe(true);
  const runtimeExpect = expect.configure({ timeout: 15_000 });
  const runId = randomUUID().slice(0, 8);
  const adminName = `Browser acceptance admin ${runId}`;
  const analystName = `Browser acceptance analyst ${runId}`;
  const setupCode =
    process.env.FLP_E2E_SETUP_CODE ||
    process.env.FLP_TEST_SETUP_CODE ||
    (process.env.FLP_E2E_NATIVE === "1" ? "fixture-owner-code" : undefined);
  expect(
    setupCode,
    "Set FLP_E2E_SETUP_CODE for an isolated, uninitialized runtime",
  ).toBeTruthy();
  const ignoreHTTPSErrors =
    !!baseURL && new URL(baseURL).hostname === "localhost";
  const adminContext = await browser.newContext({ baseURL, ignoreHTTPSErrors });
  const analystContext = await browser.newContext({
    baseURL,
    ignoreHTTPSErrors,
  });
  const page = await virtualAuthenticator(adminContext);
  const analyst = await virtualAuthenticator(analystContext);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));

  await page.goto("/setup");
  await runtimeExpect(page).toHaveURL(/\/share-login/);
  await page
    .getByPlaceholder("Initial setup code (first-time admin)")
    .fill(setupCode!);
  await page.getByRole("button", { name: "Verify Setup Code" }).click();
  await page.getByPlaceholder("Admin display name").fill(adminName);
  await page.getByRole("button", { name: "Register Admin Passkey" }).click();
  await runtimeExpect(page).toHaveURL("/admin");
  await runtimeExpect(page.getByTestId("admin-overview-root")).toBeVisible();

  const checksResponse = await adminContext.request.get(
    "/api/admin/system-checks",
  );
  expect(checksResponse.status()).toBe(
    process.env.FLP_E2E_NATIVE === "1" ? 503 : 200,
  );

  if (process.env.FLP_E2E_NATIVE === "1") {
    await test.step("rendered Goal 2 provisioning, fields, reconcile and teardown against native Rust", () =>
      goal2Native(page));
  }

  const usageResponse = await adminContext.request.patch(
    "/api/admin/usage-logging",
    {
      headers: { origin: baseURL! },
      data: { enabled: true, class_a_rate_per_1k: 0.007 },
    },
  );
  expect(usageResponse.status()).toBe(200);
  expect(await usageResponse.json()).toMatchObject({
    enabled: true,
    class_a_rate_per_1k: 0.007,
    storage_rate_per_gb_month: 0.02,
  });
  const invalidUsage = await adminContext.request.patch(
    "/api/admin/usage-logging",
    {
      headers: { origin: baseURL! },
      data: { enabled: false, retention_days: 0 },
    },
  );
  expect(invalidUsage.status()).toBe(422);
  const persistedUsage = await adminContext.request.get(
    "/api/admin/usage-logging",
  );
  expect(persistedUsage.status()).toBe(200);
  expect(await persistedUsage.json()).toMatchObject({
    enabled: true,
    retention_days: 30,
    class_a_rate_per_1k: 0.007,
  });

  const usersResponse = await adminContext.request.get("/api/admin/users");
  expect(usersResponse.status()).toBe(200);
  const {
    users,
  }: {
    users: {
      id: string;
      display_name: string;
      role: string;
      revoked?: boolean;
      passkeys?: { id: string }[];
    }[];
  } = await usersResponse.json();
  const activeAdmins = users.filter(
    (user) => user.role === "admin" && !user.revoked && user.passkeys?.length,
  );
  expect(activeAdmins.length).toBe(1);
  const passkey = activeAdmins[0].passkeys?.[0];
  expect(passkey?.id).toBeTruthy();

  const lastAdminDenied = await adminContext.request.post(
    `/api/admin/users/${activeAdmins[0].id}/revoke`,
    { headers: { origin: baseURL! }, data: {} },
  );
  expect(lastAdminDenied.status()).toBe(403);
  const lastPasskeyDenied = await adminContext.request.post(
    `/api/admin/users/${activeAdmins[0].id}/passkeys/${passkey!.id}/revoke`,
    { headers: { origin: baseURL! }, data: {} },
  );
  expect(lastPasskeyDenied.status()).toBe(403);

  const createInviteRes = await adminContext.request.post("/api/admin/invites", {
    headers: { origin: baseURL! },
    data: { display_name: analystName, role: "analyst-no-pii" },
  });
  expect(createInviteRes.status()).toBe(200);
  const invitePayload = (await createInviteRes.json()) as {
    token: string;
    invite: { id: string; role: string };
  };
  expect(invitePayload.token).toBeTruthy();

  await analyst.goto(
    `/share-login?token=${encodeURIComponent(invitePayload.token)}`,
  );
  await analyst.getByLabel("Display Name").fill(analystName);
  await analyst
    .getByRole("button", { name: /Accept Invite & Register Passkey/i })
    .click();
  await runtimeExpect(analyst).toHaveURL(/\/dashboard/);

  const analystSession1 = await (
    await analystContext.request.get("/api/auth/session")
  ).json();
  expect(analystSession1.user?.role).toBe("analyst-no-pii");

  const analystAdminPage = await analyst.goto("/admin");
  expect(analystAdminPage?.status()).toBe(403);
  expect((await analystContext.request.get("/api/admin/users")).status()).toBe(
    403,
  );
  expect(
    (await analystContext.request.get("/api/admin/usage-logging")).status(),
  ).toBe(403);

  const analystUserId = analystSession1.user.id as string;
  const roleChangeRes = await adminContext.request.patch(
    `/api/admin/users/${analystUserId}`,
    {
      headers: { origin: baseURL! },
      data: { role: "analyst" },
    },
  );
  expect(roleChangeRes.status()).toBe(200);
  const analystSession2 = await (
    await analystContext.request.get("/api/auth/session")
  ).json();
  expect(analystSession2.user?.role).toBe("analyst");

  const logoutRes = await adminContext.request.post("/api/auth/logout", {
    headers: { origin: baseURL! },
    data: {},
  });
  expect(logoutRes.status()).toBe(200);
  await page.goto("/share-login?return=/admin");
  await page.getByTestId("cloud-passkey-signin-button").click();
  await runtimeExpect(page).toHaveURL(/\/admin/);

  const revokeAnalystRes = await adminContext.request.post(
    `/api/admin/users/${analystUserId}/revoke`,
    { headers: { origin: baseURL! }, data: {} },
  );
  expect(revokeAnalystRes.status()).toBe(200);
  const analystSessionAfterRevoke = await (
    await analystContext.request.get("/api/auth/session")
  ).json();
  expect(analystSessionAfterRevoke.user).toBeNull();

  await page.goto("/setup");
  await runtimeExpect(page).toHaveURL(/\/admin/);
  expect(errors).toEqual([]);
  await analystContext.close();
  await adminContext.close();
});
