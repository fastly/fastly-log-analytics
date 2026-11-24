import {
  expect,
  request as pwRequest,
  type APIRequestContext,
  type FullConfig,
} from "@playwright/test";

export async function assertNoUnexpectedCspReports(request: APIRequestContext) {
  const response = await request.get("/__test/csp-reports");
  expect(response.ok()).toBe(true);
  const { reports } = (await response.json()) as { reports: unknown[] };
  expect(reports).toEqual([]);
}

// Every page and worker in the suite reports CSP violations to the fixture server.
export default async function cspTeardown(config: FullConfig) {
  const baseURL = config.projects[0]?.use?.baseURL;
  if (!baseURL) return;
  const ctx = await pwRequest.newContext({
    baseURL,
    ignoreHTTPSErrors: new URL(baseURL).hostname === "localhost",
  });
  try {
    const response = await ctx.get("/__test/csp-reports");
    const { reports } = (await response.json()) as { reports: unknown[] };
    if (reports.length)
      throw new Error(
        `${reports.length} CSP violation(s) during the suite:\n${JSON.stringify(reports, null, 2)}`,
      );
  } finally {
    await ctx.dispose();
  }
}
