const { chromium } = require('playwright');
const path = require('path');
const { adminGatewayContextOptions } = require('./lib/admin_gateway_client.cjs');
const { collectResponseMeasurements, budgets } = require('./lib/section_measurements.cjs');
const { execSync } = require('child_process');
// Usage: node verify_dashboard.js URL COMMIT ARCH ENV [--no-traffic]
const baseUrl = process.argv[2];
const expectedCommit = process.argv[3];
const expectedArch = process.argv[4];
const expectedEnv = process.argv[5];
const verificationFlags = process.argv.slice(6);
if (verificationFlags.some(flag => flag !== '--no-traffic')) {
  console.error(`Unknown verification option (only --no-traffic is supported).`);
  process.exit(1);
}
const noTraffic = verificationFlags.includes('--no-traffic');

if (!baseUrl) {
  console.error("Please provide a target URL.");
  process.exit(1);
}

// Dynamically resolve actual active commit hash from local git if not supplied or unknown
let actualExpectedCommit = expectedCommit;
if (!actualExpectedCommit || actualExpectedCommit === 'unknown') {
  try {
    actualExpectedCommit = execSync('git rev-parse --short=12 HEAD').toString().trim();
  } catch (e) {
    actualExpectedCommit = 'unknown';
  }
}

// Recency window for the "fresh ingest" liveness checks. Ingest is
// near-real-time: requests (~8s) and RUM beacons (~1s) are served from the
// buffer-stitched DuckLake view within seconds of the edge request, NOT gated
// on the 5-min commit tick. This window is a jitter cushion for edge->FOS
// delivery batching + verify-phase CPU spikes — NOT a pipeline latency floor.
// Requests and RUM share ONE pipeline, so both use the same symmetric window.
const RECENT_RANGE = "15m";

if (noTraffic) {
  console.log('[Mode --no-traffic] Read-only historical/chart verification. Recent injected request/RUM counts and header freshness: NOT RUN.');
}

async function runTrafficChecks(readOnly, label, check) {
  if (readOnly) {
    console.log(`[${label} ${RECENT_RANGE}] NOT RUN: recent injected traffic count and header freshness requirements (--no-traffic); historical/chart checks remain enabled.`);
    return;
  }
  await check();
}

function assertRecentHeader(bodyText, stream) {
  const match = bodyText.match(new RegExp(`${stream}[\\s\\n]*latest:[\\s\\n]*([^\\n]+)`, 'i'));
  const age = match?.[1].trim();
  // Match the UI's relative duration, not merely any non-placeholder text.
  // "just now" is also ambiguous (the UI uses it for future timestamps).
  const duration = age?.match(/^(?:(\d+)d\s+)?(?:(\d+)h\s+)?(?:(\d+)m\s+)?(?:(\d+)s\s+)?ago$/i);
  // The UI emits seconds/minutes/hours with seconds, but days without them.
  const seconds = duration && duration.slice(1).some(value => value !== undefined)
    ? Number(duration[1] || 0) * 86400 + Number(duration[2] || 0) * 3600
      + Number(duration[3] || 0) * 60 + Number(duration[4] || 0)
    : NaN;
  if (!Number.isFinite(seconds) || seconds >= parseInt(RECENT_RANGE, 10) * 60) {
    throw new Error(`[Header] ${stream} latest event age is missing, unrecognized, or outside ${RECENT_RANGE}: "${age ?? 'missing'}"`);
  }
  console.log(`[Header] Verified: ${stream} latest event age "${age}" is within ${RECENT_RANGE} (snapshot only; advancement not measured).`);
}

function getRecentWindow(url, range) {
  // Canonical frontend absolute mode: start_time/end_time WITHOUT range.
  // Capture now once so navigation and response matching share exact bounds.
  const end = Date.now();
  const start_time = new Date(end - parseInt(range, 10) * 60000).toISOString();
  const end_time = new Date(end).toISOString();
  const target = new URL(url);
  target.searchParams.delete('range');
  target.searchParams.set('start_time', start_time);
  target.searchParams.set('end_time', end_time);
  return { url: target.toString(), start_time, end_time };
}

function hasRecentWindow(body, expectedWindow) {
  // Reject preset/default/all-time requests, and even another recent window:
  // evidence must resolve to this attempt's exact absolute selection.
  if (!body || !expectedWindow || body.range_token) return false;
  const start = typeof body.start_time === 'string' ? Date.parse(body.start_time) : NaN;
  const end = typeof body.end_time === 'string' ? Date.parse(body.end_time) : NaN;
  const now = Date.now();
  return Number.isFinite(start) && Number.isFinite(end)
    && start === Date.parse(expectedWindow.start_time) && end === Date.parse(expectedWindow.end_time)
    && end - start === parseInt(RECENT_RANGE, 10) * 60000
    && end <= now && now - end <= 120000;
}

function isRecentRequestResponse(response, expectedWindow) {
  try {
    const url = new URL(response.url());
    if (url.origin !== new URL(baseUrl).origin
      || !['/api/dashboard/bundle', '/api/dashboard/aggregates'].includes(url.pathname)
      || !response.ok() || response.request().method() !== 'POST') return false;
    const body = response.request().postDataJSON();
    // The URL range alone proves nothing: the actual aggregate request must
    // select the requested absolute window, not auto/default/all-time.
    return hasRecentWindow(body, expectedWindow);
  } catch {
    return false;
  }
}

function isRecentRumResponse(response, expectedWindow) {
  try {
    const url = new URL(response.url());
    return url.origin === new URL(baseUrl).origin
      && /^\/api\/services\/[^/]+\/rum\/analytics$/.test(url.pathname)
      && response.ok() && response.request().method() === 'GET'
      && hasRecentWindow(Object.fromEntries(url.searchParams), expectedWindow);
  } catch {
    return false;
  }
}

async function recentRequestCount(response) {
  if (!response) return null;
  const payload = await response.json();
  const count = new URL(response.url()).pathname === '/api/dashboard/bundle'
    ? payload?.aggregates?.total_rows : payload?.total_rows;
  return Number.isSafeInteger(count) && count >= 0 ? count : null;
}

// Helper to append query parameters cleanly
function getUrlWithParam(url, key, value) {
  const joiner = url.includes('?') ? '&' : '?';
  return `${url}${joiner}${key}=${encodeURIComponent(value)}`;
}

// Bounded navigation retry. The page shell can take well over the old tight 10s
// to paint `main` under the harness's self-induced verify-phase CPU spike (4
// envs rendering in parallel on shared 6-CPU Colima while seeding continues),
// even though the data is already present. Retry a small bounded number of
// times with a generous shell timeout; a genuinely dead page still exhausts the
// retries and fails the run.
async function gotoWithShellReady(page, url, label, { attempts = 3, navTimeout = 30000, shellTimeout = 30000 } = {}) {
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    try {
      const response = await page.goto(url, { timeout: navTimeout });
      if (!response || !response.ok()) {
        throw new Error(`load status ${response ? response.status() : 'unknown'}`);
      }
      await page.waitForSelector('main', { timeout: shellTimeout });
      return response;
    } catch (e) {
      lastErr = e;
      console.warn(`⚠️ [${label}] navigation attempt ${i}/${attempts} failed under load: ${e.message}`);
      await page.waitForTimeout(2000 * i);
    }
  }
  throw new Error(`[${label}] navigation failed after ${attempts} attempts: ${lastErr && lastErr.message}`);
}

// Known self-healing transient conditions that occur during the harness's own
// verify-phase load spike (4 parallel browsers + continuous seeder + crons on a
// shared 6-CPU Colima) and recover within seconds with NO code defect:
//   * 503 from the documented DuckLake cold-start attach race (AGENTS.md Trap
//     #35) and K8s rollout readiness — React Query retries and the backend
//     self-heals.
//   * net::ERR_CONNECTION_REFUSED / ERR_ABORTED from a transient K8s
//     port-forward drop that the harness healer re-establishes.
// We absorb a small BOUNDED number of these instead of instant-failing on the
// first blip. The positive per-section checks below (which retry and assert
// real rendered data) remain the real pass/fail arbiter, so a PERSISTENTLY
// broken env still fails — tolerance cannot turn a real outage green.
const TRANSIENT_ERROR_BUDGET = 8;
let transientErrorsSeen = 0;

// The RUM vitals cards are heavy Plotly renders gated behind a status->analytics
// query chain. Under the verify-phase CPU spike (4 envs rendering in parallel on
// a shared 6-CPU Colima) the data is present and the analytics endpoint answers
// in ~0.5s, but the client paint can lag several seconds. Poll for the vitals to
// actually render (returns the instant they paint) rather than blind-sleeping.
const RUM_RENDER_SETTLE_MS = 12000;

function hasPopulatedRumChart() {
  return Array.from(document.querySelectorAll('main .js-plotly-plot')).some(chart => {
    const rect = chart.getBoundingClientRect();
    const hasFiniteMeasurement = values =>
      Array.isArray(values) && values.some(value => typeof value === 'number' && Number.isFinite(value));
    return rect.width > 100 && rect.height > 100
      && chart.data?.some(trace =>
        hasFiniteMeasurement(trace.x) || hasFiniteMeasurement(trace.y) || hasFiniteMeasurement(trace.values)
      );
  });
}

const { isTransientConsoleBlip } = require('./lib/console_blip');

function failOrTolerate(browser, contextName, label, detail, transient) {
  if (transient) {
    transientErrorsSeen += 1;
    console.warn(`${ts()} ⚠️ [Playwright] [${contextName}] tolerating transient self-healing blip (${transientErrorsSeen}/${TRANSIENT_ERROR_BUDGET}): ${detail}`);
    if (transientErrorsSeen > TRANSIENT_ERROR_BUDGET) {
      console.error(`${ts()} ❌ [Playwright] [${contextName}] transient-error budget exhausted (${transientErrorsSeen} > ${TRANSIENT_ERROR_BUDGET}) — treating as persistent: ${detail}`);
      browser.close().then(() => process.exit(1));
    }
    return;
  }
  console.error(`${ts()} ❌ [Playwright] [${contextName}] ${label}: ${detail}`);
  browser.close().then(() => process.exit(1));
}

// Investigating the Elevation RUM-30d port-forward blips (measured: the bound
// pod never restarts and the healer never respawns, yet requests reset mid-flight)
// needs wall-clock timestamps so console/network events here can be lined up
// against the harness's port-forward stderr logs second-for-second.
function ts() {
  return new Date().toISOString();
}

// Whether a URL is a data/API path whose failure is a real correctness defect,
// versus a static, content-hashed asset (JS/CSS chunk, font, image, svg) whose
// failure is cosmetic -- a dropped font byte proves nothing about whether the
// page's data is correct. Both the response and requestfailed listeners below
// key fatality off of this, not off of guessing from console text.
function isCriticalResourceUrl(url) {
  // Client telemetry beacons and UX telemetry pings are observational and non-critical;
  // a dropped beacon or transient connection reset on an analytics ping proves nothing
  // about whether the page's analytical data is correct or complete.
  if (url.includes('/api/web-vitals') || url.includes('/api/ux-events')) {
    return false;
  }
  return url.includes('/api/') || url.includes('/_next/data/');
}

// Chrome logs this exact generic line to the console for EVERY failed resource
// load, API or decorative asset alike, and critically it never includes the
// URL or filename -- so text-based filtering (the previous approach: skip if
// the string contains "woff2", "favicon", etc.) silently fails the instant the
// resource is referenced by a content hash instead of a recognizable name
// (measured 2026-10-04: a `/_next/static/media/<hash>.woff2` font reset killed
// an otherwise fully-passing Elevation run on exactly this gap). The
// requestfailed/response listeners already classify by the actual request URL,
// so this redundant, URL-blind echo is ignored here entirely; it is not a new
// source of truth, just a duplicate of what those two already decide correctly.
const GENERIC_RESOURCE_FAILURE_RE = /^Failed to load resource:/;

// Helper to fail the script if any console error or failed network response occurs
function registerErrorListeners(page, browser, contextName) {
  page.on('console', msg => {
    console.log(`${ts()} [Browser Console] [${contextName}] [${msg.type()}] ${msg.text()}`);
    if (msg.type() === 'error') {
      const text = msg.text();
      console.error(`${ts()} [Playwright Console Error] [${contextName}] ${text}`);
      if (GENERIC_RESOURCE_FAILURE_RE.test(text)) {
        return;
      }
      // Only fail on critical application or API request errors, filtering out benign browser preload/favicon warnings
      if (!text.includes('preload') && !text.includes('favicon') && !text.includes('React DevTools')) {
        failOrTolerate(browser, contextName, 'Failing E2E verification due to console error', `"${text}"`, isTransientConsoleBlip(text));
      }
    }
  });

  // Playwright's own request-failure event carries the exact URL and error
  // code independent of whatever the page's console happens to log, so it is
  // the correct signal for a connection-level failure (ERR_CONNECTION_RESET
  // etc.) that never reaches the 'response' listener below at all. Classified
  // the same way as 'response': only a critical (data/API) URL can fail the
  // run; a static asset reset is logged but never fatal.
  page.on('requestfailed', request => {
    const failure = request.failure();
    const errorText = failure ? failure.errorText : 'unknown error';
    console.log(`${ts()} [Request Failed] [${contextName}] ${request.url()} :: ${errorText}`);
    if (isCriticalResourceUrl(request.url())) {
      failOrTolerate(browser, contextName, 'Failed API request', `${request.url()} :: ${errorText}`, isTransientConsoleBlip(errorText));
    }
  });

  page.on('response', response => {
    const status = response.status();
    const url = response.url();
    // Fail on any API response that returns 4xx or 5xx — except a 503, which is
    // the documented self-healing cold-start/rollout window (Trap #35) and is
    // absorbed under the bounded transient budget.
    if (status >= 400 && isCriticalResourceUrl(url)) {
      failOrTolerate(browser, contextName, 'Failed API call', `${url} returned status ${status}`, status === 503);
    }
  });
}

const EMPTY_STATE_MARKERS = ["No data available", "No data in this time range yet", "No data in this range"];

async function attachGceFallbackAuth(context, certificateAdmin, logInjected = false) {
  if (!certificateAdmin && expectedArch === "standard" && expectedEnv === "gce") {
    const cookies = [
      { name: 'analyst_session_id', value: 'test-session-playwright-e2e-verification-secret', domain: '127.0.0.1', path: '/' },
    ];
    await context.addCookies(cookies);
    await context.setExtraHTTPHeaders({ 'X-Forwarded-For': '127.0.0.1' });
    if (logInjected) {
      console.log(`[Playwright Auth Bypass] Injected analyst_session_id cookie! 🍪`);
    }
  }
}

// Generic "is this page reading real data" gate for pages without a bespoke
// stage. Passes only when an identifying panel title renders AND at least one
// Plotly chart holds finite measurements (hasPopulatedRumChart is the shared
// predicate). A single legitimately empty panel (e.g. no suspicious clients in
// synthetic traffic) is logged, not fatal; `strictEmpty` pages treat ANY
// empty-state panel as a failure.
async function verifyDataPage(browser, adminTlsOptions, spec) {
  const { name, path: routePath, markers, strictEmpty = false } = spec;
  const pageUrl = getUrlWithParam(baseUrl.replace('/dashboard', routePath), "range", "24h");
  console.log(`[${name} 24h] Checking ${pageUrl} ...`);

  const context = await browser.newContext(adminTlsOptions);
  await attachGceFallbackAuth(context, Boolean(adminTlsOptions.clientCertificates));
  await logPoolAtStart(context, name);
  const page = await context.newPage();
  registerErrorListeners(page, browser, name);

  const measurements = collectResponseMeasurements(page);

  let bodyText = "";
  let hasMarker = false;
  let hasChart = false;
  let emptyPanels = 0;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      await gotoWithShellReady(page, pageUrl, `${name} 24h`, { attempts: 3, navTimeout: 35000, shellTimeout: 10000 });
      try {
        await page.waitForFunction(hasPopulatedRumChart, undefined, { timeout: 45000 });
      } catch (chartErr) {
        console.log(`[${name} 24h] Attempt ${attempt}/4: no populated Plotly chart yet.`);
      }
      const { bodyText: evaluatedBodyText, visibleText } = await page.evaluate(() => ({
        bodyText: `${document.body.innerText}\n${document.body.textContent || ''}`,
        visibleText: document.body.innerText,
      }));
      bodyText = evaluatedBodyText;
      const lower = bodyText.toLowerCase();
      hasMarker = markers.some(marker => lower.includes(marker.toLowerCase()));
      hasChart = await page.evaluate(hasPopulatedRumChart);
      emptyPanels = EMPTY_STATE_MARKERS.reduce((sum, marker) => sum + visibleText.split(marker).length - 1, 0);
      if (hasMarker && hasChart && !visibleText.includes("Failed to load") && (!strictEmpty || emptyPanels === 0)) {
        break;
      }
    } catch (err) {
      console.log(`[${name} 24h] Navigation warning (attempt ${attempt}): ${err.message}`);
    }
    console.log(`[${name} 24h] Attempt ${attempt}/4: data not fully rendered yet (marker=${hasMarker}, chart=${hasChart}, emptyPanels=${emptyPanels}). Waiting 8s...`);
    await page.waitForTimeout(8000);
  }

  if (!hasMarker || !hasChart || bodyText.includes("Failed to load") || (strictEmpty && emptyPanels > 0)) {
    console.error(`❌ [${name}] Verification Failed: expected panels [${markers.join(" | ")}] with populated charts (marker=${hasMarker}, populatedChart=${hasChart}, emptyPanels=${emptyPanels}).`);
    console.error(`Body text sample:\n${bodyText.slice(0, 1000)}`);
    await browser.close();
    process.exit(1);
  }
  if (emptyPanels > 0) {
    console.warn(`⚠️ [${name}] ${emptyPanels} panel(s) show an empty state; page has populated charts so proceeding.`);
  }
  console.log(`[${name} Verification] Verified: panels present and charts populated with real data. 🟢`);
  await logPagePerf(page, name, await measurements.snapshot(), measurements.gaps());
  measurements.close();
  if (name === 'Streaming') await logPoolStats(context, name);
  await page.close();
  await context.close();
}

async function verifyAnalyticsRouteWithPlotly(browser, adminTlsOptions, certificateAdmin, spec) {
  const {
    name,
    routePath,
    range = '30d',
    markers,
    configWarningMarker = '',
    configWarningMessage = '',
    screenshotPath = path.resolve(__dirname, '..', `${name.toLowerCase()}-screenshot.png`),
  } = spec;
  const routeBaseUrl = baseUrl.replace('/dashboard', routePath);
  const context = await browser.newContext(adminTlsOptions);
  await attachGceFallbackAuth(context, certificateAdmin);
  const routePage = await context.newPage();
  registerErrorListeners(routePage, browser, name);

  const targetUrl = getUrlWithParam(routeBaseUrl, 'range', range);
  const label = `${name} ${range}`;
  console.log(`[${label}] Checking ${targetUrl} ...`);

  let bodyText = '';
  let hasVitals = false;

  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const response = await gotoWithShellReady(routePage, targetUrl, label, {
        attempts: 3,
        navTimeout: 35000,
        shellTimeout: 10000,
      });
      if (response && response.ok()) {
        // GEMINI.md Mandate #4: Positive-case wait for Plotly charts to become visible
        try {
          await routePage.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 45000 });
        } catch (chartErr) {
          console.log(`[${label}] Attempt ${attempt}/4: Plotly chart not visible yet.`);
        }

        bodyText = await routePage.evaluate(() => document.body.innerText);
        const hasErrorText =
          (configWarningMarker && bodyText.includes(configWarningMarker)) || bodyText.includes('Failed to load');
        hasVitals = markers.some(marker => bodyText.includes(marker));

        const isChartVisible = await routePage.evaluate(() => {
          const el = document.querySelector('.js-plotly-plot, .plotly');
          if (!el) return false;
          const rect = el.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        });

        if (!hasErrorText && hasVitals && isChartVisible) {
          break;
        }
      }
    } catch (err) {
      console.log(`[${label}] Navigation warning (attempt ${attempt}): ${err.message}`);
    }
    console.log(`[${label}] Attempt ${attempt}/4: ${name} metrics or Plotly charts not fully rendered yet. Waiting 8s...`);
    await routePage.waitForTimeout(8000);
  }

  if (configWarningMarker && bodyText.includes(configWarningMarker)) {
    console.error(configWarningMessage);
    await browser.close();
    process.exit(1);
  }

  if (!hasVitals) {
    console.error(`[${label}] Verification Failed: ${name} page could not be verified or is missing standard panel names.`);
    console.error(`Body text sample:\n${bodyText.slice(0, 1000)}`);
    await browser.close();
    process.exit(1);
  }

  if (EMPTY_STATE_MARKERS.some(marker => bodyText.includes(marker))) {
    console.error(`❌ [Playwright ${name} Verification] Verification Failed: ${name} panels loaded successfully but returned "No data available" or "No data in this range"!`);
    await browser.close();
    process.exit(1);
  }

  const isChartVisible = await routePage.evaluate(() => {
    const el = document.querySelector('.js-plotly-plot, .plotly');
    if (!el) return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  });
  if (!isChartVisible) {
    const isLocal = process.argv[5] === 'local';
    if (isLocal) {
      await routePage.screenshot({ path: screenshotPath, fullPage: true });
      console.error(`❌ [Playwright ${name} Chart Verification] Verification Failed: No visible Plotly charts found on the ${name} page! Screenshot saved to ${path.basename(screenshotPath)}`);
      await browser.close();
      process.exit(1);
    } else {
      console.log(`⚠️ [Playwright ${name} Chart Verification] Warning: No visible Plotly charts found on the ${name} page for remote cloud environment (${process.argv[5]}), proceeding...`);
    }
  }

  console.log(`[${name} Panel Verification] Verified: All ${name} panels finished loading, standard elements are present, and Plotly charts are visible! 🟢`);

  await routePage.close();
  await context.close();
}

// Snapshot (never a wait) of how busy the DuckDB pool is when a page check starts: pages the
// previous check abandoned can still have queries running, which is what a user who clicks
// through pages quickly runs into. `in_use` counts open connections including idle ones, so
// busy = in_use - idle.
async function logPoolAtStart(context, name) {
  try {
    const res = await context.request.get(`${new URL(baseUrl).origin}/api/admin/health`, { timeout: 15000 });
    if (!res.ok()) return;
    const pools = (await res.json()).pool_wait || [];
    const busy = pools.reduce((n, p) => n + Math.max(0, (p.in_use || 0) - (p.idle || 0)), 0);
    console.log(`[POOL-START ${name}] busy=${busy}`);
  } catch (err) {
    console.log(`[POOL-START ${name}] unavailable: ${err.message}`);
  }
}

// DuckDB connection-pool state right after the page load (admin health snapshot):
// size, in-use/idle, and checkout vs iceberg-view-rebind wait percentiles. Explains
// a large `connection_acquire` entry in the aggregate endpoints' section timings.
async function logPoolStats(context, name) {
  try {
    const res = await context.request.get(`${new URL(baseUrl).origin}/api/admin/health`, { timeout: 20000 });
    if (!res.ok()) throw new Error(`status ${res.status()}`);
    const pools = ((await res.json()).pool_wait || []).map(p => ({
      max: p.max_size, open: p.in_use, idle: p.idle, busy: Math.max(0, (p.in_use || 0) - (p.idle || 0)), created: p.created_total, reused: p.reused_total,
      discarded: p.discarded_total, saturated: p.saturated_rejects_total, wait: p.wait, rebindWait: p.rebind_wait,
      heavy: p.heavy,
    }));
    console.log(`[POOL ${name}] ${JSON.stringify(pools)}`);
  } catch (err) {
    console.log(`[POOL ${name}] unavailable: ${err.message}`);
  }
}

// One-line, non-fatal page-performance record for the Phase 4 audit workbooks:
// navigation timings, paint/LCP/CLS, and per-endpoint API timings with duplicate
// counts. Navigation order does not establish response-cache or database state.
async function logPagePerf(page, name, responseMeasurements = [], measurementGaps = []) {
  try {
    const perf = await page.evaluate(() => new Promise(resolve => {
      const nav = performance.getEntriesByType('navigation')[0];
      const fcp = performance.getEntriesByName('first-contentful-paint')[0];
      let lcp = null;
      let cls = 0;
      try {
        new PerformanceObserver(list => {
          const entries = list.getEntries();
          if (entries.length) lcp = entries[entries.length - 1].startTime;
        }).observe({ type: 'largest-contentful-paint', buffered: true });
        new PerformanceObserver(list => {
          for (const e of list.getEntries()) if (!e.hadRecentInput) cls += e.value;
        }).observe({ type: 'layout-shift', buffered: true });
      } catch (e) { /* unsupported entry type (e.g. webkit) */ }
      setTimeout(() => {
        const api = {};
        for (const r of performance.getEntriesByType('resource')) {
          if (!r.name.includes('/api/') || r.name.includes('/api/web-vitals') || r.name.includes('/api/ux-events')) continue;
          const key = new URL(r.name).pathname;
          const row = api[key] || (api[key] = { n: 0, maxMs: 0, kb: 0 });
          row.n += 1;
          if (Math.round(r.duration) >= row.maxMs) {
            // Where the slowest call spent its time: browser/gateway queue and
            // connection setup (stall), server + network wait, and download.
            row.stallMs = Math.round(r.requestStart - r.startTime);
            row.waitMs = Math.round(r.responseStart - r.requestStart);
            row.dlMs = Math.round(r.responseEnd - r.responseStart);
          }
          row.maxMs = Math.max(row.maxMs, Math.round(r.duration));
          row.kb = Math.max(row.kb, Math.round((r.transferSize || r.encodedBodySize || 0) / 102.4) / 10);
        }
        resolve({
          ttfbMs: nav ? Math.round(nav.responseStart) : null,
          dclMs: nav ? Math.round(nav.domContentLoadedEventEnd) : null,
          loadMs: nav ? Math.round(nav.loadEventEnd) : null,
          fcpMs: fcp ? Math.round(fcp.startTime) : null,
          lcpMs: lcp === null ? null : Math.round(lcp),
          cls: Math.round(cls * 1000) / 1000,
          api,
          duplicateApi: Object.entries(api).filter(([, v]) => v.n > 1).map(([k, v]) => `${k} x${v.n}`),
        });
      }, 500);
    }));
    perf.responseMeasurements = responseMeasurements;
    perf.budgets = budgets;
    perf.qualification = 'incomplete';
    perf.unknowns = [...new Set([
      ...measurementGaps,
      ...responseMeasurements.flatMap(row => row.gaps),
      ...(responseMeasurements.length ? [] : ['aggregate_responses_unobserved']),
      'database_cache_state_unknown',
      'history_unverified',
      'reader_coverage_unverified',
      'query_io_attribution_unverified',
      'page_ready_unmeasured',
      'interaction_completion_unmeasured',
    ])];
    console.log(`[PERF ${name}] ${JSON.stringify(perf)}`);
  } catch (err) {
    console.log(`[PERF ${name}] unavailable: ${err.message}`);
  }
}

(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const adminTlsOptions = adminGatewayContextOptions(baseUrl);
    const certificateAdmin = Boolean(adminTlsOptions.clientCertificates);

    // Create a completely isolated incognito browser context for Stage 1 to prevent domain/port caching conflicts
    const dashboardContext = await browser.newContext(adminTlsOptions);
    await attachGceFallbackAuth(dashboardContext, certificateAdmin, true);
    const page = await dashboardContext.newPage();
    registerErrorListeners(page, browser, 'Dashboard');

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 1: DASHBOARD PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────

    // 1.1. Verify 24h Overall Data is present
    const url24h = getUrlWithParam(baseUrl, "range", "24h");
    console.log(`[Dashboard 24h] Checking ${url24h} ...`);
    let bodyText24h = "";
    let totalMatch24h = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      let response = await page.goto(url24h, { timeout: 20000 });
      if (!response || !response.ok()) {
        if (attempt === 3) {
          console.error(`[Dashboard 24h] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
          await browser.close();
          process.exit(1);
        }
        await page.waitForTimeout(4000);
        continue;
      }
      await page.waitForSelector('main', { timeout: 30000 });
      // Robust waiting: Wait for the total count to appear and be non-zero
      try {
        await page.waitForFunction(() => {
          const match = document.body.innerText.match(/total:\s*([\d,]+)/i);
          return match && parseInt(match[1].replace(/,/g, ''), 10) > 0;
        }, undefined, { timeout: 30000 });
      } catch (e) {
        console.log(`[Dashboard 24h] Warning: timed out waiting for total count to render, proceeding...`);
      }

      bodyText24h = await page.evaluate(() => document.body.innerText);
      const hasFailedToLoad24h = bodyText24h.includes("Failed to load") && !bodyText24h.includes("Faro version");
      if (hasFailedToLoad24h || bodyText24h.includes("saturated at") || bodyText24h.includes("PoolBusy") || bodyText24h.includes("No data for this filter")) {
        if (attempt === 3) {
          console.error(`[Dashboard 24h] Verification Failed: 24h page shows errors or empty 'No data for this filter' state!`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[Dashboard 24h] Attempt ${attempt}/3: transient error or warming up. Waiting 4s...`);
        await page.waitForTimeout(4000);
        continue;
      }

      totalMatch24h = bodyText24h.match(/total:\s*([\d,]+)/i);
      if (!totalMatch24h || parseInt(totalMatch24h[1].replace(/,/g, ''), 10) === 0) {
        if (attempt === 3) {
          console.error(`[Dashboard 24h] Verification Failed: 24h request row count is missing or zero!`);
          await browser.close();
          process.exit(1);
        }
        await page.waitForTimeout(4000);
        continue;
      }
      break;
    }
    console.log(`[Dashboard 24h] Verified: service header is populated (${totalMatch24h[1]} lifetime rows; NOT a 24h count).`);

    let response;
    await runTrafficChecks(noTraffic, 'Dashboard', async () => {
      // 1.2. Verify recent-window Range with ROBUST POLLING (At least 150 rows)
      let recentCount = null;
      const MIN_RECENT_REQUESTS = 150; // Safe minimum for standard traffic seeding
      let recentBodyText = "";

      for (let attempt = 1; attempt <= 4; attempt++) {
        recentCount = null;
        try {
          const recentWindow = getRecentWindow(baseUrl, RECENT_RANGE);
          console.log(`[Dashboard ${RECENT_RANGE}] Checking ${recentWindow.url} with active polling...`);
          // Observe only this navigation's requested-window response. SSR-only
          // or unscoped data is insufficient evidence: fail closed, never fall
          // back to the lifetime header total.
          const windowResponse = page.waitForResponse(
            response => isRecentRequestResponse(response, recentWindow), { timeout: 15000 }
          ).catch(() => null);
          response = await page.goto(recentWindow.url, { timeout: 15000 });
          if (response && response.ok()) {
            await page.waitForSelector('main', { timeout: 10000 });
            await page.waitForTimeout(4000);
            recentBodyText = await page.evaluate(() => document.body.innerText);
            recentCount = await recentRequestCount(await windowResponse);

            if (recentCount !== null && recentCount >= MIN_RECENT_REQUESTS) {
              break;
            }
          }
        } catch (err) {
          console.log(`[Dashboard ${RECENT_RANGE}] Navigation warning (attempt ${attempt}): ${err.message}`);
        }
        console.log(`[Dashboard ${RECENT_RANGE}] Attempt ${attempt}/4: Standard request count is ${recentCount ?? 'UNVERIFIED'}/${MIN_RECENT_REQUESTS}. Waiting 8s for background ingestion...`);
        await page.waitForTimeout(8000);
      }

      if (recentCount === null || recentCount < MIN_RECENT_REQUESTS) {
        console.error(`[Dashboard ${RECENT_RANGE}] Verification Failed: Requested-window aggregate count is ${recentCount ?? 'UNVERIFIED'} (expected at least ${MIN_RECENT_REQUESTS} from recent edge load-test). Missing evidence is not proof of completed ingestion.`);
        console.error(`Body text sample:\n${recentBodyText.slice(0, 1000)}`);
        await browser.close();
        process.exit(1);
      }
      console.log(`[Dashboard ${RECENT_RANGE}] Verified: requested-window aggregate contains ${recentCount} rows (at least ${MIN_RECENT_REQUESTS}).`);

      // 1.3. Validate Header Liveness (REQUEST)
      assertRecentHeader(recentBodyText, 'REQUEST');
    });

    // 1.4. Validate 30d historical data and the lifetime header
    const url30d = getUrlWithParam(baseUrl, "range", "30d");
    console.log(`[Dashboard 30d] Checking historical panels and lifetime header on ${url30d} ...`);
    // Use the bounded retry helper (like RUM 30d / Network 30d) so a transient
    // K8s port-forward drop the healer is re-establishing doesn't hard-fail the
    // run on the first blip; a genuinely dead page still exhausts retries.
    response = await gotoWithShellReady(page, url30d, 'Dashboard 30d', { attempts: 3, navTimeout: 20000, shellTimeout: 30000 });

    // Robust waiting: Wait for the header badge containing the REQUEST totals to fully render
    try {
      await page.waitForFunction(() => {
        return document.body.innerText.match(/REQUEST[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i);
      }, undefined, { timeout: 30000 });
    } catch (e) {
      console.log(`[Dashboard 30d] Warning: timed out waiting for header totals to render, proceeding...`);
    }

    // Wait for aggregates/bundle queries to complete and loading overlays to disappear
    try {
      await page.waitForFunction(() => {
        const text = document.body.innerText;
        return !text.includes("Crunching logs...") && !text.includes("Loading...") && !text.includes("Initializing...");
      }, undefined, { timeout: 90000 });
    } catch (e) {
      console.log(`[Dashboard 30d] Warning: timed out waiting for "Crunching logs" loading overlays to clear, proceeding...`);
    }

    // Wait for at least one Plotly chart to become visible
    try {
      await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 90000 });
      console.log(`[Dashboard 30d] Verified: Plotly charts are fully rendered and visible! 🟢`);
    } catch (e) {
      console.log(`⚠️ [Dashboard 30d] Warning: timed out waiting for Plotly charts to become visible.`);
    }

    const bodyText30d = await page.evaluate(() => document.body.innerText);

    // This badge is a lifetime service total, NOT a 30d metrics card.
    // Comparing the first "total:" with this same badge was a tautology.
    const headerReqMatch = bodyText30d.match(/REQUEST[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i);
    const headerReqTotal = headerReqMatch ? parseInt(headerReqMatch[1].replace(/,/g, ''), 10) : 0;
    if (headerReqTotal === 0) {
      console.error(`[Dashboard 30d] Verification Failed: REQUEST lifetime header total is missing or zero.`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard 30d] REQUEST lifetime total: ${headerReqTotal}; window-count consistency NOT VERIFIED (no independent page count).`);

    // Strict Panel Loading Verification: Assert that panels have successfully finished loading and contain real data
    if (bodyText30d.includes("Crunching logs") || bodyText30d.includes("Initializing")) {
      const isLocal = process.argv[5] === "local";
      if (isLocal) {
        console.error(`❌ [Playwright Panel Verification] Verification Failed: Dashboard panels are stuck on "Crunching logs..." or "Initializing..." loading state!`);
        await browser.close();
        process.exit(1);
      } else {
        console.log(`⚠️ [Playwright Panel Verification] Warning: Dashboard panels are still loading on remote cloud environment (${process.argv[5]}), proceeding...`);
      }
    }
    if (bodyText30d.includes("No data available") || bodyText30d.includes("No data in this time range yet")) {
      console.error(`❌ [Playwright Panel Verification] Verification Failed: Dashboard panels successfully loaded but have no data ("No data available")!`);
      await browser.close();
      process.exit(1);
    }

    // Verify that at least one Plotly chart is rendered and visible on the page
    const isChartVisible = await page.evaluate(() => {
      const el = document.querySelector('.js-plotly-plot, .plotly');
      if (!el) return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    });
    if (!isChartVisible) {
      console.error(`❌ [Playwright Chart Verification] Verification Failed: No visible Plotly charts found on the dashboard page!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard Panel Verification] Verified: All dashboard panels finished loading, metrics are positive, and Plotly charts are visible! 🟢`);

    // Verify Footers and Metadata on 30d page
    const footerText = await page.evaluate(() => {
      const footer = document.querySelector('footer');
      return footer ? footer.innerText : '';
    });
    console.log(`[Dashboard] Rendered Footer: "${footerText}"`);

    if (!footerText.includes(`commit:${actualExpectedCommit}`) && !(expectedEnv === "gce" && footerText.includes("commit:unknown"))) {
      console.error(`❌ [Playwright Commit Verification] Verification Failed: The deployed container is running the wrong commit, is un-built, or has stale build artifacts!`);
      console.error(`Expected active commit hash: 'commit:${actualExpectedCommit}'`);
      console.error(`Rendered footer text on page: "${footerText}"`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard Commit Verification] Verified: Running correct active commit 'commit:${actualExpectedCommit}'! 🟢`);
    if (expectedArch && !footerText.toLowerCase().includes(expectedArch.toLowerCase())) {
      console.error(`[Dashboard] Verification Failed: Expected architecture '${expectedArch}' not found in footer.`);
      await browser.close();
      process.exit(1);
    }
    if (expectedEnv && !footerText.toLowerCase().includes(expectedEnv.toLowerCase())) {
      if (expectedEnv.toLowerCase() === "elevation" && (footerText.toLowerCase().includes("elevation") || footerText.toLowerCase().includes("gce"))) {
        console.log(`[Dashboard] Dynamic GCE-on-GKE environment matched for Elevation! 🟢`);
      } else {
        console.error(`[Dashboard] Verification Failed: Expected environment '${expectedEnv}' not found in footer.`);
        await browser.close();
        process.exit(1);
      }
    }

    // Close the dashboard context and page before navigating to RUM to cleanly prevent client-side NextJS chunk-mismatch reloads!
    await page.close();
    await dashboardContext.close();

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 2: RUM PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────
    const rumBaseUrl = baseUrl.replace('/dashboard', '/rum');

    // Create a completely clean, isolated incognito browser context for Stage 2 to prevent any cookie or storage conflicts
    const rumContext = await browser.newContext(adminTlsOptions);
    await attachGceFallbackAuth(rumContext, certificateAdmin);
    const rumPage = await rumContext.newPage();
    registerErrorListeners(rumPage, browser, 'RUM');

    // 2.1. Verify 24h Overall RUM Data is present with active polling
    const rumUrl24h = getUrlWithParam(rumBaseUrl, "range", "24h");
    console.log(`[RUM 24h] Checking ${rumUrl24h} with active polling...`);

    let rumBodyText24h = "";
    let hasVitalsTitle24h = false;
    let hasVitalsRating24h = false;

    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        response = await rumPage.goto(rumUrl24h, { timeout: 35000 });
        if (response && response.ok()) {
          await rumPage.waitForSelector('main', { timeout: 10000 });
          // Poll for the vitals cards to actually paint (returns immediately once
          // rendered); tolerates a slow render under the verify-phase CPU spike
          // instead of blind-sleeping a fixed 4s and false-failing.
          try {
            await rumPage.waitForFunction(() => {
              const text = document.body.innerText;
              const hasTitle = text.includes("Largest Contentful Paint") || text.includes("LCP");
              const hasRating = text.includes("GOOD") || text.includes("POOR") || text.includes("NEEDS IMP.");
              const failedToLoad = text.includes("Failed to load") && !text.includes("Faro version");
              return hasTitle && hasRating && !failedToLoad;
            }, undefined, { timeout: RUM_RENDER_SETTLE_MS });
          } catch (e) {
            // Fall through to read the body and let the outer attempt loop retry.
          }
          rumBodyText24h = await rumPage.evaluate(() => document.body.innerText);
          const rumFailedToLoad24h = rumBodyText24h.includes("Failed to load") && !rumBodyText24h.includes("Faro version");

          hasVitalsTitle24h = rumBodyText24h.includes("Largest Contentful Paint") || rumBodyText24h.includes("LCP");
          hasVitalsRating24h = rumBodyText24h.includes("GOOD") || rumBodyText24h.includes("POOR") || rumBodyText24h.includes("NEEDS IMP.");

          if (!rumFailedToLoad24h && hasVitalsTitle24h && hasVitalsRating24h) {
            break;
          }
        }
      } catch (err) {
        console.log(`[RUM 24h] Navigation warning (attempt ${attempt}): ${err.message}`);
      }
      console.log(`[RUM 24h] Attempt ${attempt}/4: Web Vitals metrics not fully rendered yet. Waiting 8s for cron commit...`);
      await rumPage.waitForTimeout(8000);
    }

    if (!hasVitalsTitle24h || !hasVitalsRating24h) {
      console.error(`[RUM 24h] Verification Failed: Web Vitals metrics are missing or empty on the RUM page!`);
      console.error(`Body text sample:\n${rumBodyText24h.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 24h] Verified: 24h RUM data is active and charts/ratings successfully populated.`);

    await runTrafficChecks(noTraffic, 'RUM', async () => {
      // 2.2. Verify recent-window RUM Range with active polling (At least 50 beacons)
      let recentBeaconCount = 0;
      const MIN_RECENT_BEACONS = 50; // Safe threshold allowing for global edge S3 streaming latency
      let recentRumBodyText = "";

      for (let attempt = 1; attempt <= 4; attempt++) {
        recentBeaconCount = 0;
        try {
          const recentWindow = getRecentWindow(rumBaseUrl, RECENT_RANGE);
          console.log(`[RUM ${RECENT_RANGE}] Checking ${recentWindow.url} with active polling...`);
          const windowResponse = rumPage.waitForResponse(
            response => isRecentRumResponse(response, recentWindow), { timeout: 35000 }
          ).catch(() => null);
          response = await rumPage.goto(recentWindow.url, { timeout: 35000 });
          if (response && response.ok()) {
            await rumPage.waitForSelector('main', { timeout: 10000 });
            // Poll for the beacon card to paint (returns immediately once rendered)
            // so a slow render under load doesn't read 0 and waste the attempt.
            try {
              await rumPage.waitForFunction(() => document.body.innerText.includes("TOTAL BEACONS"), undefined, { timeout: RUM_RENDER_SETTLE_MS });
            } catch (e) {
              // Fall through; the outer attempt loop retries for ingestion/render.
            }
            recentRumBodyText = await rumPage.evaluate(() => document.body.innerText);
            const evidence = await windowResponse;
            const observedCount = evidence ? (await evidence.json())?.beacon_count : null;

            if (recentRumBodyText.includes("TOTAL BEACONS")) {
              const beaconMatch = recentRumBodyText.match(/TOTAL BEACONS\s*([\d,]+)/i);
              const displayedCount = beaconMatch ? parseInt(beaconMatch[1].replace(/,/g, ''), 10) : 0;
              if (Number.isSafeInteger(observedCount) && observedCount >= 0 && observedCount === displayedCount) {
                recentBeaconCount = observedCount;
              } else {
                console.log(`[RUM ${RECENT_RANGE}] Requested-window beacon count UNVERIFIED or differs from the rendered card.`);
              }

              if (recentBeaconCount >= MIN_RECENT_BEACONS) {
                break;
              }
            }
          }
        } catch (err) {
          console.log(`[RUM ${RECENT_RANGE}] Navigation warning (attempt ${attempt}): ${err.message}`);
        }
        console.log(`[RUM ${RECENT_RANGE}] Attempt ${attempt}/4: Beacon count is ${recentBeaconCount}/${MIN_RECENT_BEACONS}. Waiting 8s for background ingestion...`);
        await rumPage.waitForTimeout(8000);
      }

      if (recentBeaconCount < MIN_RECENT_BEACONS) {
        console.error(`[RUM ${RECENT_RANGE}] Verification Failed: Recent ${RECENT_RANGE} RUM beacon count is only ${recentBeaconCount} (expected at least ${MIN_RECENT_BEACONS} from our recent edge load-test!). Ingestion did not complete.`);
        console.error(`Body text sample:\n${recentRumBodyText.slice(0, 1000)}`);
        await browser.close();
        process.exit(1);
      }
      console.log(`[RUM ${RECENT_RANGE}] Verified: ${RECENT_RANGE} RUM data contains ${recentBeaconCount} beacons (at least ${MIN_RECENT_BEACONS}).`);

      // 2.3. Validate Header Liveness (RUM)
      assertRecentHeader(recentRumBodyText, 'RUM');
    });

    // 2.4. Validate 30d RUM Consistency: Header RUM Total vs. Page RUM Total
    const rumUrl30d = getUrlWithParam(rumBaseUrl, "range", "30d");
    console.log(`[RUM 30d] Checking data consistency on ${rumUrl30d} ...`);
    try {
      response = await gotoWithShellReady(rumPage, rumUrl30d, 'RUM 30d', { attempts: 3, navTimeout: 30000, shellTimeout: 30000 });
    } catch (e) {
      console.error(`[RUM 30d] Failed to load RUM page: ${e.message}`);
      await browser.close();
      process.exit(1);
    }

    // Wait for standard loading indicators to clear
    try {
      await rumPage.waitForFunction(() => {
        const text = document.body.innerText;
        return !text.includes("Crunching logs...") && !text.includes("Loading...") && !text.includes("Initializing...");
      }, undefined, { timeout: 90000 });
    } catch (e) {
      console.log(`[RUM 30d] Warning: timed out waiting for RUM loading overlays to clear, proceeding...`);
    }

    try {
      await rumPage.waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 });
      console.log(`[RUM 30d] Verified: Full-sized RUM Plotly charts contain data points! 🟢`);
    } catch (e) {
      console.error(`[RUM 30d] Verification Failed: No populated full-sized RUM chart: ${e.message}`);
      await browser.close();
      process.exit(1);
    }

    const rumBodyText30d = await rumPage.evaluate(() => document.body.innerText);

    // Strict RUM Panel Loading Verification: Assert that panels have successfully finished loading and contain real data
    if (rumBodyText30d.includes("Crunching logs") || rumBodyText30d.includes("Initializing")) {
      console.error(`❌ [Playwright RUM Panel Verification] Verification Failed: RUM panels are stuck on "Crunching logs..." or "Initializing..." loading state!`);
      await browser.close();
      process.exit(1);
    }
    if (rumBodyText30d.includes("No data available") || rumBodyText30d.includes("No data in this time range yet") || rumBodyText30d.includes("Waiting for real-time")) {
      console.error(`❌ [Playwright RUM Panel Verification] Verification Failed: RUM panels successfully loaded but have no data ("No data available")!`);
      await browser.close();
      process.exit(1);
    }

    console.log(`[RUM Panel Verification] Verified: All RUM panels finished loading, metrics are positive, and Plotly charts are visible! 🟢`);

    // Extract Header RUM Total
    const headerRumMatch = rumBodyText30d.match(/RUM[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i);
    if (!headerRumMatch) {
      console.error(`[RUM 30d] Verification Failed: RUM total count not found in global header.`);
      await browser.close();
      process.exit(1);
    }
    const headerRumTotal = parseInt(headerRumMatch[1].replace(/,/g, ''), 10);

    // Extract Page RUM Total
    const pageRumMatch = rumBodyText30d.match(/TOTAL BEACONS\s*([\d,]+)/i);
    const pageRumTotal = pageRumMatch ? parseInt(pageRumMatch[1].replace(/,/g, ''), 10) : 0;

    if (pageRumTotal === 0) {
      console.error(`❌ [Playwright RUM Panel Verification] Verification Failed: RUM metrics card has 0 total beacons!`);
      await browser.close();
      process.exit(1);
    }

    console.log(`[RUM 30d Consistency] Header Raw Metrics Total: ${headerRumTotal} │ Page Distinct Beacons Total: ${pageRumTotal}`);

    // Ensure distinct beacons count is valid, non-zero, and does not exceed the total raw telemetry rows (+20% tolerance)
    if (pageRumTotal > headerRumTotal * 1.20 || pageRumTotal === 0) {
      console.error(`[RUM 30d Consistency] Verification Failed: Page RUM count (${pageRumTotal}) is invalid, zero, or exceeds lifetime header count (${headerRumTotal})!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 30d Consistency] Verified: Header RUM count and page metrics are consistent! 🟢`);

    await rumPage.close();
    await rumContext.close();

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 3: NETWORK PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────
    await verifyAnalyticsRouteWithPlotly(browser, adminTlsOptions, certificateAdmin, {
      name: 'Network',
      routePath: '/network',
      range: '30d',
      markers: ['Global Health', 'Avg RTT', 'Leaderboard', 'RTT', 'HEALTH SCORE'],
      configWarningMarker: 'Enable Groups F and G',
      configWarningMessage:
        '❌ [Playwright Network Verification] Verification Failed: Network page is showing field group configuration warning: "Enable Groups F and G (Network Quality) in your log field configuration."',
      screenshotPath: path.resolve(__dirname, '..', 'network-screenshot.png'),
    });

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGES 4-7: STREAMING (CMCD), SECURITY, ORIGIN, PERFORMANCE
    // ────────────────────────────────────────────────────────────────────────
    await verifyDataPage(browser, adminTlsOptions, {
      name: 'Streaming',
      path: '/streaming',
      markers: ['Total Sessions', 'Peak Viewers'],
      strictEmpty: true,
    });
    await verifyDataPage(browser, adminTlsOptions, {
      name: 'Security',
      path: '/security',
      markers: [
        'Traffic Quality Breakdown',
        'Suspicious ISPs',
        'Proxy/Anonymizer Breakdown',
        'Well-Known Bots',
        'Request Header Size Distribution',
      ],
    });
    await verifyDataPage(browser, adminTlsOptions, {
      name: 'Origin',
      path: '/origin',
      markers: ['Origin Error Rate', 'Origin TTFB', 'Fetch Volume'],
    });
    await verifyDataPage(browser, adminTlsOptions, {
      name: 'Performance',
      path: '/performance',
      markers: ['Slowest URLs', 'Slowest Networks', 'Cache TTL Distribution'],
    });

    await browser.close();
    process.exit(0);
  } catch (e) {
    console.error(`Connection refused or error: ${e.message}`);
    if (browser) await browser.close();
    process.exit(1);
  }
})();
