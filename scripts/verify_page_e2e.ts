/**
 * scripts/verify_page_e2e.ts
 *
 * Playwright E2E Verification with HAR recording and waterfall analysis.
 * Verifies all dashboard pages and modals across environments and roles:
 *  - Environments: local-standard, gce-standard, elevation-high-scale
 *  - Roles: admin, analyst-pii, analyst-no-pii
 *
 * Audits:
 *  1. Zero unexpected 4xx/5xx network errors.
 *  2. Slow query analysis and TTFB waterfall metrics.
 *  3. Strict PII redaction validation (mask_ips) in Analyst No-PII mode.
 *  4. Console error detection and hydration check.
 *  5. Saves HAR archive to artifacts/har/<env>-<role>.har
 *  6. Emits structured waterfall report to artifacts/har/<env>-<role>-waterfall.json
 *
 * Usage:
 *   npx tsx scripts/verify_page_e2e.ts --url https://example.global.ssl.fastly.net \
 *       --env elevation-high-scale --role analyst-no-pii --service-id <service-id>
 */

import { chromium, type Browser, type BrowserContext, type Page } from 'playwright';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { adminGatewayContextOptions } = require('./lib/admin_gateway_client.cjs');

function loadDeployEnv(): void {
  const envFile = path.join(os.homedir(), '.config/fastly-log-analytics/admin/deploy.env');
  if (!fs.existsSync(envFile)) return;
  const content = fs.readFileSync(envFile, 'utf8');
  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx <= 0) continue;
    const key = line.slice(0, eqIdx).trim();
    let val = line.slice(eqIdx + 1).trim();
    if ((val.startsWith("'") && val.endsWith("'")) || (val.startsWith('"') && val.endsWith('"'))) {
      val = val.slice(1, -1);
    }
    if (!process.env[key]) {
      process.env[key] = val;
    }
  }
}

function resolveOriginForEnv(envName: string): string | undefined {
  loadDeployEnv();
  const endpointsRaw = process.env.ADMIN_GATEWAY_ENDPOINTS;
  if (!endpointsRaw) return undefined;
  try {
    const endpoints = JSON.parse(endpointsRaw);
    const aliases: Record<string, string> = {
      'local-standard': 'local-standard',
      'gce-standard': 'remote-standard',
      'remote-standard': 'remote-standard',
      'elevation-high-scale': 'remote-high-scale',
      'remote-high-scale': 'remote-high-scale',
    };
    const key = aliases[envName] || envName;
    return endpoints[key]?.origin;
  } catch {
    return undefined;
  }
}

interface ScriptOptions {
  url: string;
  pagePath?: string;
  env: string;
  role: 'admin' | 'analyst-pii' | 'analyst-no-pii';
  serviceId?: string;
  passcode?: string;
  harDir: string;
  timeoutMs: number;
  headless: boolean;
  slowQueryThresholdMs: number;
}

interface NetworkRecord {
  url: string;
  method: string;
  status: number;
  durationMs: number;
  contentLength: number;
  contentType: string;
  isApi: boolean;
  piiViolations: string[];
}

interface WaterfallReport {
  timestamp: string;
  env: string;
  role: string;
  targetUrl: string;
  serviceId?: string;
  harPath: string;
  summary: {
    totalRequests: number;
    apiRequests: number;
    failedRequests: number;
    slowRequests: number;
    totalBytes: number;
    avgLatencyMs: number;
    p95LatencyMs: number;
    piiViolationsCount: number;
    consoleErrorsCount: number;
  };
  slowestQueries: Array<{
    url: string;
    method: string;
    durationMs: number;
    status: number;
  }>;
  errors: Array<{
    url: string;
    status: number;
    statusText: string;
  }>;
  consoleErrors: string[];
  piiAudit: {
    scannedResponses: number;
    violations: Array<{
      url: string;
      snippet: string;
    }>;
  };
}

// Parse command line arguments
function parseArgs(): ScriptOptions {
  const args = process.argv.slice(2);
  const options: Partial<ScriptOptions> = {
    harDir: path.resolve(process.cwd(), 'artifacts/har'),
    timeoutMs: 45_000,
    headless: true,
    slowQueryThresholdMs: 2_000,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--url' && args[i + 1]) options.url = args[++i];
    else if (arg === '--page' && args[i + 1]) options.pagePath = args[++i];
    else if (arg === '--env' && args[i + 1]) options.env = args[++i];
    else if (arg === '--role' && args[i + 1]) options.role = args[++i] as any;
    else if (arg === '--service-id' && args[i + 1]) options.serviceId = args[++i];
    else if (arg === '--passcode' && args[i + 1]) options.passcode = args[++i];
    else if (arg === '--har-dir' && args[i + 1]) options.harDir = path.resolve(args[++i]);
    else if (arg === '--timeout' && args[i + 1]) options.timeoutMs = parseInt(args[++i], 10);
    else if (arg === '--slow-threshold' && args[i + 1]) options.slowQueryThresholdMs = parseInt(args[++i], 10);
    else if (arg === '--headless') options.headless = args[++i] !== 'false';
  }

  if (!options.env) {
    options.env = 'local-standard';
  }
  if (!options.role) {
    options.role = 'admin';
  }
  if (!options.url) {
    const origin = resolveOriginForEnv(options.env);
    if (origin) {
      const p = options.pagePath ? (options.pagePath.startsWith('/') ? options.pagePath : `/${options.pagePath}`) : '/dashboard';
      options.url = `${origin.replace(/\/$/, '')}${p}`;
    } else {
      console.error('Error: --url is required (e.g. --url https://analytics.example.com) or configure ~/.config/fastly-log-analytics/admin/deploy.env');
      process.exit(1);
    }
  } else if (options.pagePath && !new URL(options.url).pathname.replace(/^\/$/, '')) {
    const p = options.pagePath.startsWith('/') ? options.pagePath : `/${options.pagePath}`;
    options.url = `${options.url.replace(/\/$/, '')}${p}`;
  }

  return options as ScriptOptions;
}

// Strict IPv4 unmasked pattern (excluding .xxx, localhost, RFC zeros, and slash-prefixed version strings like Chrome/131.0.0.0)
const UNMASKED_IPV4_RE = /(?<![/\w.])(?!(?:127\.0\.0\.1|0\.0\.0\.0)\b)(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b/g;

function scanForPii(text: string): string[] {
  const violations: string[] = [];
  const matches = text.match(UNMASKED_IPV4_RE);
  if (matches) {
    for (const match of matches) {
      if (!match.endsWith('.xxx') && !match.endsWith('.0.0.0')) {
        violations.push(`Unmasked IPv4 address detected: ${match}`);
      }
    }
  }
  return violations;
}

async function setupAnalystSession(
  browser: Browser,
  adminUrl: string,
  role: 'analyst-pii' | 'analyst-no-pii',
  mtlsOpts: Record<string, any>
): Promise<{ analystTargetUrl: string; cookies: any[]; extraHTTPHeaders?: Record<string, string> }> {
  const adminUrlObj = new URL(adminUrl);
  const adminOrigin = adminUrlObj.origin;
  const pagePathAndQuery = adminUrlObj.pathname + adminUrlObj.search;

  const adminCtx = await browser.newContext({
    ignoreHTTPSErrors: true,
    ...mtlsOpts,
  });

  try {
    let statusResp = await adminCtx.request.get(`${adminOrigin}/api/admin/share/status`);
    let status = await statusResp.json();
    if (!status.sharing_active) {
      await adminCtx.request.post(`${adminOrigin}/api/admin/share/start`, {
        data: { public_endpoint: 'https://127.0.0.1' },
      });
      statusResp = await adminCtx.request.get(`${adminOrigin}/api/admin/share/status`);
      status = await statusResp.json();
    }

    const pubEndpoint: string = (status.public_endpoint || 'https://127.0.0.1').replace(/\/$/, '');
    const isLocalLoopback = pubEndpoint.includes('127.0.0.1') || pubEndpoint.includes('localhost');
    const analystOrigin = isLocalLoopback ? 'http://127.0.0.1' : pubEndpoint;
    const extraHTTPHeaders = isLocalLoopback ? { 'X-Remote-Analyst': '1' } : undefined;

    let serviceIds: string[] = (status.services || []).map((s: any) => s.service_id).filter(Boolean);
    if (serviceIds.length === 0) {
      const bootResp = await adminCtx.request.get(`${adminOrigin}/api/bootstrap`);
      const boot = await bootResp.json();
      serviceIds = (boot.services || []).map((s: any) => s.service_id).filter(Boolean);
    }

    const ts = Date.now();
    const email = `e2e-${role}-${ts}@example.com`;
    const passcode = `E2ePass!${ts}#Xy9`;

    const invResp = await adminCtx.request.post(`${adminOrigin}/api/admin/share/invites`, {
      data: {
        name: `E2E ${role}`,
        email,
        passcode,
        duration_hours: 1,
        service_ids: serviceIds,
        pii_policy: { mask_ips: role === 'analyst-no-pii' },
        allow_concurrent_sessions: true,
      },
    });
    if (!invResp.ok()) {
      throw new Error(`Failed to create analyst invite: HTTP ${invResp.status()} ${await invResp.text()}`);
    }

    const authCtx = await browser.newContext({
      ignoreHTTPSErrors: true,
      ...(extraHTTPHeaders ? { extraHTTPHeaders } : {}),
    });
    try {
      const loginResp = await authCtx.request.post(`${analystOrigin}/api/share/login`, {
        headers: { Origin: analystOrigin },
        data: { email, passcode },
      });
      if (!loginResp.ok()) {
        throw new Error(`Analyst login failed: HTTP ${loginResp.status()} ${await loginResp.text()}`);
      }
      const loginBody = await loginResp.json();
      if (loginBody.tos_pending && loginBody.tos?.version) {
        const tosResp = await authCtx.request.post(`${analystOrigin}/api/share/acknowledge`, {
          headers: { Origin: analystOrigin },
          data: { version: loginBody.tos.version },
        });
        if (!tosResp.ok()) {
          throw new Error(`Analyst acknowledge-tos failed: HTTP ${tosResp.status()} ${await tosResp.text()}`);
        }
      }
      const cookies = await authCtx.cookies();
      return {
        analystTargetUrl: `${analystOrigin}${pagePathAndQuery}`,
        cookies,
        extraHTTPHeaders,
      };
    } finally {
      await authCtx.close();
    }
  } finally {
    await adminCtx.close();
  }
}

async function run() {
  const opts = parseArgs();
  console.log(`\n============================================================`);
  console.log(`Starting Playwright Verification & Waterfall Analysis`);
  console.log(`  Environment: ${opts.env}`);
  console.log(`  Role:        ${opts.role}`);
  console.log(`  Target URL:  ${opts.url}`);
  if (opts.serviceId) console.log(`  Service ID:  ${opts.serviceId}`);
  console.log(`============================================================\n`);

  fs.mkdirSync(opts.harDir, { recursive: true });
  const harPath = path.join(opts.harDir, `${opts.env}-${opts.role}.har`);
  const reportPath = path.join(opts.harDir, `${opts.env}-${opts.role}-waterfall.json`);

  const browser: Browser = await chromium.launch({
    headless: opts.headless,
  });

  loadDeployEnv();
  const mtlsOpts = adminGatewayContextOptions(opts.url, process.env);

  let contextOpts: Record<string, any> = {
    viewport: { width: 1440, height: 900 },
    ignoreHTTPSErrors: true,
    recordHar: {
      path: harPath,
      mode: 'full',
    },
  };
  let initialCookies: any[] = [];

  if (opts.role === 'admin') {
    contextOpts = { ...contextOpts, ...mtlsOpts };
  } else {
    console.log(`[Setup] Provisioning Analyst Path B (${opts.role}) session via Admin Gateway...`);
    const analystSetup = await setupAnalystSession(browser, opts.url, opts.role, mtlsOpts);
    opts.url = analystSetup.analystTargetUrl;
    initialCookies = analystSetup.cookies;
    if (analystSetup.extraHTTPHeaders) {
      contextOpts.extraHTTPHeaders = analystSetup.extraHTTPHeaders;
    }
    console.log(`[Setup] Analyst session ready -> ${opts.url}`);
  }

  const context: BrowserContext = await browser.newContext(contextOpts);
  if (initialCookies.length > 0) {
    await context.addCookies(initialCookies);
  }

  const networkRecords: NetworkRecord[] = [];
  const consoleErrors: string[] = [];
  const executionErrors: string[] = [];
  const piiViolations: Array<{ url: string; snippet: string }> = [];

  const page: Page = await context.newPage();

  // Monitor console errors
  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      const text = msg.text();
      // Ignore non-fatal resource errors if needed
      if (!text.includes('favicon.ico')) {
        consoleErrors.push(text);
        console.warn(`[Browser Console Error] ${text}`);
      }
    }
  });

  page.on('pageerror', (err) => {
    consoleErrors.push(err.message);
    console.error(`[Browser Uncaught PageError] ${err.message}`);
  });

  // Track request timings and responses
  const requestStartTimes = new WeakMap<object, number>();

  page.on('request', (req) => {
    requestStartTimes.set(req, performance.now());
  });

  page.on('response', async (res) => {
    const url = res.url();
    const start = requestStartTimes.get(res.request()) ?? performance.now();
    const durationMs = Math.round(performance.now() - start);
    const status = res.status();
    const contentType = res.headers()['content-type'] || '';
    const contentLength = parseInt(res.headers()['content-length'] || '0', 10);
    const isApi = url.includes('/api/');

    const violations: string[] = [];

    // Audit PII in responses for analyst-no-pii mode
    if (opts.role === 'analyst-no-pii' && isApi && contentType.includes('application/json')) {
      try {
        const bodyText = await res.text();
        const found = scanForPii(bodyText);
        if (found.length > 0) {
          for (const f of found) {
            violations.push(f);
            piiViolations.push({ url, snippet: f });
          }
          console.error(`[PII VIOLATION in ${url}] ${found.join('; ')}`);
        }
      } catch {
        // Body reading failed or streamed, skip
      }
    }

    networkRecords.push({
      url,
      method: res.request().method(),
      status,
      durationMs,
      contentLength,
      contentType,
      isApi,
      piiViolations: violations,
    });
  });

  try {
    let target = opts.url;
    if (opts.serviceId && !target.includes('service=')) {
      target += target.includes('?') ? `&service=${opts.serviceId}` : `?service=${opts.serviceId}`;
    }

    console.log(`[Step 1] Navigating to ${target}...`);
    const resp = await page.goto(target, { waitUntil: 'domcontentloaded', timeout: opts.timeoutMs });
    console.log(`Initial page response: ${resp ? resp.status() : 'null'}`);

    // Check if passcode entry is required (for analyst roles)
    if (opts.passcode) {
      console.log(`[Step 2] Attempting Passcode authentication...`);
      const passcodeInput = page.locator('input[type="password"], input[name="passcode"]');
      if (await passcodeInput.isVisible({ timeout: 5000 }).catch(() => false)) {
        await passcodeInput.fill(opts.passcode);
        const submitBtn = page.locator('button:has-text("Enter"), button:has-text("Join"), button[type="submit"]');
        await submitBtn.click();
        console.log(`Submitted passcode. Waiting for dashboard session...`);
        await page.waitForTimeout(2000);
      }
    }

    // Handle Terms of Service modal if displayed
    const tosAcceptBtn = page.locator('button:has-text("Accept"), button:has-text("I Agree")');
    if (await tosAcceptBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      console.log(`[TOS Modal] Accepting terms of service...`);
      await tosAcceptBtn.click();
      await page.waitForTimeout(1000);
    }

    // Wait for main dashboard container and primary API hydration
    console.log(`[Step 3] Waiting for dashboard UI shell...`);
    await page.waitForSelector('main, [data-testid="dashboard-content"]', { timeout: 15_000 });
    await page.waitForLoadState('networkidle', { timeout: 25_000 }).catch(() => {});
    await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 }).catch(() => {});
    await page.waitForTimeout(1500);

    const title = await page.title();
    console.log(`Page title confirmed: "${title}"`);

    if (!opts.pagePath) {
      // Verify Tab Navigation when no specific --page was requested
      const tabs = [
        { name: 'Dashboard', selector: 'button:has-text("Overview"), a:has-text("Overview"), button:has-text("Dashboard")' },
        { name: 'Security', selector: 'button:has-text("Security"), a:has-text("Security")' },
        { name: 'Network', selector: 'button:has-text("Network"), a:has-text("Network")' },
        { name: 'Origin', selector: 'button:has-text("Origin"), a:has-text("Origin")' },
        { name: 'Performance', selector: 'button:has-text("Performance"), a:has-text("Performance")' },
      ];

      console.log(`\n[Step 4] Walking dashboard tabs...`);
      for (const tab of tabs) {
        const tabLocator = page.locator(tab.selector).first();
        if (await tabLocator.isVisible().catch(() => false)) {
          console.log(`  -> Clicking tab: ${tab.name}`);
          await tabLocator.click().catch(() => {});
          await page.waitForTimeout(2000);
        }
      }
    } else {
      console.log(`\n[Step 4] Verified target page: ${opts.pagePath}`);
    }

    // Inspect Modals (Share / Analyst Invite if Admin)
    if (opts.role === 'admin') {
      console.log(`\n[Step 5] Testing Admin Modals (Live Share / Analyst Dialog)...`);
      const shareBtn = page.locator('button:has-text("Share"), button:has-text("Live Share")').first();
      if (await shareBtn.isVisible().catch(() => false)) {
        console.log(`  -> Opening Share modal...`);
        await shareBtn.click();
        await page.waitForTimeout(1500);
        const closeBtn = page.locator('button[aria-label="Close"], button:has-text("Cancel"), button:has-text("Close")').first();
        if (await closeBtn.isVisible().catch(() => false)) {
          await closeBtn.click();
        }
      }
    }

    // Audit DOM for PII in Analyst No-PII mode
    if (opts.role === 'analyst-no-pii') {
      console.log(`\n[Step 6] Auditing visible DOM for unmasked PII text...`);
      const bodyText = await page.evaluate(() => document.body.innerText);
      const domPii = scanForPii(bodyText);
      if (domPii.length > 0) {
        for (const v of domPii) {
          piiViolations.push({ url: 'DOM_VISIBLE_TEXT', snippet: v });
          console.error(`[PII VIOLATION in DOM TEXT] ${v}`);
        }
      } else {
        console.log(`  ✓ DOM text contains zero unmasked IP addresses.`);
      }
    }

  } catch (err: any) {
    const errMsg = err?.message || String(err);
    executionErrors.push(errMsg);
    consoleErrors.push(`EXECUTION_ERROR: ${errMsg}`);
    console.error(`Verification execution encountered an error: ${errMsg}`);
  } finally {
    // Finalize HAR and close
    await context.close();
    await browser.close();
  }

  // Waterfall Metrics Computation
  const apiRecords = networkRecords.filter((r) => r.isApi);
  const failedRequests = networkRecords.filter((r) => r.status >= 400);
  const slowRequests = networkRecords.filter((r) => r.durationMs >= opts.slowQueryThresholdMs);
  const totalBytes = networkRecords.reduce((acc, r) => acc + (r.contentLength || 0), 0);
  const latencies = networkRecords.map((r) => r.durationMs).sort((a, b) => a - b);
  const avgLatency = latencies.length ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) : 0;
  const p95Latency = latencies.length ? latencies[Math.floor(latencies.length * 0.95)] : 0;

  const slowest = [...networkRecords]
    .sort((a, b) => b.durationMs - a.durationMs)
    .slice(0, 8)
    .map((r) => ({
      url: r.url,
      method: r.method,
      durationMs: r.durationMs,
      status: r.status,
    }));

  const report: WaterfallReport = {
    timestamp: new Date().toISOString(),
    env: opts.env,
    role: opts.role,
    targetUrl: opts.url,
    serviceId: opts.serviceId,
    harPath,
    summary: {
      totalRequests: networkRecords.length,
      apiRequests: apiRecords.length,
      failedRequests: failedRequests.length,
      slowRequests: slowRequests.length,
      totalBytes,
      avgLatencyMs: avgLatency,
      p95LatencyMs: p95Latency,
      piiViolationsCount: piiViolations.length,
      consoleErrorsCount: consoleErrors.length,
    },
    slowestQueries: slowest,
    errors: failedRequests.map((r) => ({ url: r.url, status: r.status, statusText: `HTTP ${r.status}` })),
    consoleErrors,
    piiAudit: {
      scannedResponses: apiRecords.length,
      violations: piiViolations,
    },
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), 'utf8');

  console.log(`\n============================================================`);
  console.log(`WATERFALL & AUDIT SUMMARY [${opts.env} - ${opts.role}]`);
  console.log(`============================================================`);
  console.log(`Total Requests:      ${report.summary.totalRequests} (${report.summary.apiRequests} API)`);
  console.log(`Total Bytes:         ${(report.summary.totalBytes / 1024).toFixed(1)} KB`);
  console.log(`Latency (Avg / p95): ${avgLatency} ms / ${p95Latency} ms`);
  console.log(`Slow Requests (>${opts.slowQueryThresholdMs}ms): ${report.summary.slowRequests}`);
  console.log(`HTTP Errors (>=400): ${report.summary.failedRequests}`);
  console.log(`Console Errors:      ${report.summary.consoleErrorsCount}`);
  console.log(`PII Violations:      ${report.summary.piiViolationsCount}`);
  console.log(`HAR Saved:           ${harPath}`);
  console.log(`Report Saved:        ${reportPath}`);

  if (slowest.length > 0) {
    console.log(`\nSlowest Endpoints:`);
    for (const s of slowest) {
      console.log(`  - [${s.method}] ${s.durationMs}ms (HTTP ${s.status}): ${s.url}`);
    }
  }

  if (failedRequests.length > 0) {
    console.log(`\nFailed Requests:`);
    for (const f of failedRequests) {
      console.log(`  - [${f.method}] HTTP ${f.status} (${f.durationMs}ms): ${f.url}`);
    }
  }

  console.log(`============================================================\n`);

  if (opts.role === 'analyst-no-pii' && piiViolations.length > 0) {
    console.error(`FATAL: Analyst No-PII mode leaked unmasked PII!`);
    process.exit(1);
  }

  if (failedRequests.some((r) => r.status >= 500)) {
    console.error(`FATAL: 5xx server errors encountered!`);
    process.exit(1);
  }

  if (executionErrors.length > 0) {
    console.error(`FATAL: Verification execution failed: ${executionErrors.join('; ')}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error('Fatal execution error:', err);
  process.exit(1);
});
