import { test, expect, type APIRequestContext } from "@playwright/test";
import { assertNoUnexpectedCspReports } from "./csp-teardown";

type Report = { "csp-report": Record<string, string> };

async function reports(request: APIRequestContext) {
  const response = await request.get("/__test/csp-reports");
  return ((await response.json()) as { reports: Report[] }).reports;
}

test("pages and scripts carry the enforced policy", async ({ request }) => {
  const html = await request.get("/dashboard");
  const csp = html.headers()["content-security-policy"];
  expect(csp).toContain("script-src 'self' 'wasm-unsafe-eval' 'sha256-");
  expect(csp).not.toContain("'unsafe-eval'");
  expect(csp).toContain("frame-ancestors 'none'");
  expect(html.headers()["content-security-policy-report-only"]).toBeUndefined();
  const page = await html.text();
  const script = page.match(/\/_next\/static\/chunks\/[^"]+\.js/)![0];
  expect((await request.get(script)).headers()["content-security-policy"]).toBe(
    csp,
  );
});

test("an unhashed inline script is blocked and reported", async ({
  page,
  request,
}) => {
  await page.goto("/share-login");
  await page.waitForLoadState("networkidle");
  expect(await reports(request)).toEqual([]);
  await expect(
    page.addScriptTag({ content: "window.cspProbe = true;" }),
  ).rejects.toThrow();
  expect(await page.evaluate(() => "cspProbe" in window)).toBe(false);
  await expect
    .poll(async () =>
      (await reports(request)).map((report) => [
        report["csp-report"]["effective-directive"],
        report["csp-report"]["blocked-uri"],
      ]),
    )
    .toEqual([["script-src-elem", "inline"]]);
  await request.delete("/__test/csp-reports");
  await assertNoUnexpectedCspReports(request);
});
