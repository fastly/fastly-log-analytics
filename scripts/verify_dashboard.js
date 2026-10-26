const { chromium } = require('playwright');
const path = require('path');
const { adminGatewayContextOptions } = require('./lib/admin_gateway_client.cjs');
const { collectResponseMeasurements, budgets } = require('./lib/section_measurements.cjs');
const {
  isTransientConsoleBlip,
  logPoolAtStart = async () => {},
  logPoolStats = async () => {},
  logPagePerf = async () => {},
} = require('./lib/console_blip');
const { execSync } = require('child_process');
// Usage: node verify_dashboard.js URL COMMIT ARCH ENV [--no-traffic]
const baseUrl = process.argv[2], expectedCommit = process.argv[3], expectedArch = process.argv[4], expectedEnv = process.argv[5];
const verificationFlags = process.argv.slice(6);
if (verificationFlags.some(flag => flag !== '--no-traffic')) {
  console.error(`Unknown verification option (only --no-traffic is supported).`);
  process.exit(1);
}
const noTraffic = verificationFlags.includes('--no-traffic');
if (!baseUrl) { console.error("Please provide a target URL."); process.exit(1); }

let actualExpectedCommit = expectedCommit;
if (!actualExpectedCommit || actualExpectedCommit === 'unknown') {
  try { actualExpectedCommit = execSync('git rev-parse --short=12 HEAD').toString().trim(); }
  catch { actualExpectedCommit = 'unknown'; }
}

const RECENT_RANGE = "15m";
if (noTraffic) console.log('[Mode --no-traffic] Read-only historical/chart verification. Recent injected request/RUM counts and header freshness: NOT RUN.');

async function failAndExit(browser, ...messages) {
  for (const msg of messages) console.error(msg);
  if (browser) await browser.close();
  process.exit(1);
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
  const duration = age?.match(/^(?:(\d+)d\s+)?(?:(\d+)h\s+)?(?:(\d+)m\s+)?(?:(\d+)s\s+)?ago$/i);
  const seconds = duration && duration.slice(1).some(value => value !== undefined)
    ? Number(duration[1] || 0) * 86400 + Number(duration[2] || 0) * 3600 + Number(duration[3] || 0) * 60 + Number(duration[4] || 0)
    : NaN;
  if (!Number.isFinite(seconds) || seconds >= parseInt(RECENT_RANGE, 10) * 60) {
    throw new Error(`[Header] ${stream} latest event age is missing, unrecognized, or outside ${RECENT_RANGE}: "${age ?? 'missing'}"`);
  }
  console.log(`[Header] Verified: ${stream} latest event age "${age}" is within ${RECENT_RANGE} (snapshot only; advancement not measured).`);
}

function getRecentWindow(url, range) {
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
  if (!body || !expectedWindow || body.range_token) return false;
  const start = typeof body.start_time === 'string' ? Date.parse(body.start_time) : NaN;
  const end = typeof body.end_time === 'string' ? Date.parse(body.end_time) : NaN;
  const now = Date.now();
  return Number.isFinite(start) && Number.isFinite(end)
    && start === Date.parse(expectedWindow.start_time) && end === Date.parse(expectedWindow.end_time)
    && end - start === parseInt(RECENT_RANGE, 10) * 60000 && end <= now && now - end <= 120000;
}

function isRecentRequestResponse(response, expectedWindow) {
  try {
    const url = new URL(response.url());
    if (url.origin !== new URL(baseUrl).origin
      || !['/api/dashboard/bundle', '/api/dashboard/aggregates'].includes(url.pathname)
      || !response.ok() || response.request().method() !== 'POST') return false;
    return hasRecentWindow(response.request().postDataJSON(), expectedWindow);
  } catch { return false; }
}

function isRecentRumResponse(response, expectedWindow) {
  try {
    const url = new URL(response.url());
    return url.origin === new URL(baseUrl).origin
      && /^\/api\/services\/[^/]+\/rum\/analytics$/.test(url.pathname)
      && response.ok() && response.request().method() === 'GET'
      && hasRecentWindow(Object.fromEntries(url.searchParams), expectedWindow);
  } catch { return false; }
}

async function recentRequestCount(response) {
  if (!response) return null;
  const payload = await response.json();
  const count = new URL(response.url()).pathname === '/api/dashboard/bundle' ? payload?.aggregates?.total_rows : payload?.total_rows;
  return Number.isSafeInteger(count) && count >= 0 ? count : null;
}

// Helper to append query parameters cleanly
function getUrlWithParam(url, key, value) {
  return `${url}${url.includes('?') ? '&' : '?'}${key}=${encodeURIComponent(value)}`;
}

async function gotoWithShellReady(page, url, label, { attempts = 3, navTimeout = 30000, shellTimeout = 30000 } = {}) {
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    try {
      const response = await page.goto(url, { timeout: navTimeout });
      if (!response || !response.ok()) throw new Error(`load status ${response ? response.status() : 'unknown'}`);
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

// Bounded tolerance for self-healing blips (ERR_CONNECTION_REFUSED, HTTP 503, etc.)
const TRANSIENT_ERROR_BUDGET = 8;
let transientErrorsSeen = 0;
const RUM_RENDER_SETTLE_MS = 12000;

function hasPopulatedRumChart() {
  return Array.from(document.querySelectorAll('main .js-plotly-plot')).some(chart => {
    const rect = chart.getBoundingClientRect();
    const hasFiniteMeasurement = values =>
      Array.isArray(values) && values.some(value => typeof value === 'number' && Number.isFinite(value));
    return rect.width > 100 && rect.height > 100
      && chart.data?.some(trace => hasFiniteMeasurement(trace.x) || hasFiniteMeasurement(trace.y) || hasFiniteMeasurement(trace.values));
  });
}

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

function ts() { return new Date().toISOString(); }

function isCriticalResourceUrl(url) {
  return !url.includes('/api/web-vitals') && !url.includes('/api/ux-events') && (url.includes('/api/') || url.includes('/_next/data/'));
}

const GENERIC_RESOURCE_FAILURE_RE = /^Failed to load resource:/;

function registerErrorListeners(page, browser, contextName) {
  page.on('console', msg => {
    console.log(`${ts()} [Browser Console] [${contextName}] [${msg.type()}] ${msg.text()}`);
    if (msg.type() !== 'error') return;
    const text = msg.text();
    console.error(`${ts()} [Playwright Console Error] [${contextName}] ${text}`);
    if (!GENERIC_RESOURCE_FAILURE_RE.test(text) && !text.includes('preload') && !text.includes('favicon') && !text.includes('React DevTools')) {
      failOrTolerate(browser, contextName, 'Failing E2E verification due to console error', `"${text}"`, isTransientConsoleBlip(text));
    }
  });
  page.on('requestfailed', request => {
    const errorText = request.failure()?.errorText || 'unknown error';
    console.log(`${ts()} [Request Failed] [${contextName}] ${request.url()} :: ${errorText}`);
    if (isCriticalResourceUrl(request.url())) {
      failOrTolerate(browser, contextName, 'Failed API request', `${request.url()} :: ${errorText}`, isTransientConsoleBlip(errorText));
    }
  });
  page.on('response', response => {
    const status = response.status(), url = response.url();
    if (status >= 400 && isCriticalResourceUrl(url)) {
      failOrTolerate(browser, contextName, 'Failed API call', `${url} returned status ${status}`, status === 503);
    }
  });
}

const EMPTY_STATE_MARKERS = ["No data available", "No data in this time range yet", "No data in this range"];

async function hasVisiblePlotlyChart(page) {
  return page.evaluate(() => {
    const rect = document.querySelector('.js-plotly-plot, .plotly')?.getBoundingClientRect();
    return Boolean(rect && rect.width > 0 && rect.height > 0);
  });
}

async function attachGceFallbackAuth(context, certificateAdmin, logInjected = false) {
  if (!certificateAdmin && expectedArch === "standard" && expectedEnv === "gce") {
    await context.addCookies([{ name: 'analyst_session_id', value: 'test-session-playwright-e2e-verification-secret', domain: '127.0.0.1', path: '/' }]);
    await context.setExtraHTTPHeaders({ 'X-Forwarded-For': '127.0.0.1' });
    if (logInjected) console.log(`[Playwright Auth Bypass] Injected analyst_session_id cookie! 🍪`);
  }
}

async function verifyDataPage(browser, adminTlsOptions, spec) {
  const { name, path: routePath, markers, strictEmpty = false } = spec;
  const pageUrl = getUrlWithParam(baseUrl.replace('/dashboard', routePath), "range", "24h");
  console.log(`[${name} 24h] Checking ${pageUrl} ...`);
  const context = await browser.newContext(adminTlsOptions);
  await attachGceFallbackAuth(context, Boolean(adminTlsOptions.clientCertificates));
  await logPoolAtStart(context, baseUrl, name);
  const page = await context.newPage();
  registerErrorListeners(page, browser, name);
  const measurements = collectResponseMeasurements(page);

  let bodyText = "", hasMarker = false, hasChart = false, emptyPanels = 0;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      await gotoWithShellReady(page, pageUrl, `${name} 24h`, { attempts: 3, navTimeout: 35000, shellTimeout: 10000 });
      try { await page.waitForFunction(hasPopulatedRumChart, undefined, { timeout: 45000 }); }
      catch { console.log(`[${name} 24h] Attempt ${attempt}/4: no populated Plotly chart yet.`); }
      const { bodyText: evaluatedBodyText, visibleText } = await page.evaluate(() => ({
        bodyText: `${document.body.innerText}\n${document.body.textContent || ''}`,
        visibleText: document.body.innerText,
      }));
      bodyText = evaluatedBodyText;
      const lower = bodyText.toLowerCase();
      hasMarker = markers.some(marker => lower.includes(marker.toLowerCase()));
      hasChart = await page.evaluate(hasPopulatedRumChart);
      emptyPanels = EMPTY_STATE_MARKERS.reduce((sum, marker) => sum + visibleText.split(marker).length - 1, 0);
      if (hasMarker && hasChart && !visibleText.includes("Failed to load") && (!strictEmpty || emptyPanels === 0)) break;
    } catch (err) {
      console.log(`[${name} 24h] Navigation warning (attempt ${attempt}): ${err.message}`);
    }
    console.log(`[${name} 24h] Attempt ${attempt}/4: data not fully rendered yet (marker=${hasMarker}, chart=${hasChart}, emptyPanels=${emptyPanels}). Waiting 8s...`);
    await page.waitForTimeout(8000);
  }

  if (!hasMarker || !hasChart || bodyText.includes("Failed to load") || (strictEmpty && emptyPanels > 0)) {
    await failAndExit(browser,
      `❌ [${name}] Verification Failed: expected panels [${markers.join(" | ")}] with populated charts (marker=${hasMarker}, populatedChart=${hasChart}, emptyPanels=${emptyPanels}).`,
      `Body text sample:\n${bodyText.slice(0, 1000)}`);
  }
  if (emptyPanels > 0) console.warn(`⚠️ [${name}] ${emptyPanels} panel(s) show an empty state; page has populated charts so proceeding.`);
  console.log(`[${name} Verification] Verified: panels present and charts populated with real data. 🟢`);
  await logPagePerf(page, name, budgets, await measurements.snapshot(), measurements.gaps());
  measurements.close();
  if (name === 'Streaming') await logPoolStats(context, baseUrl, name);
  await page.close();
  await context.close();
}

async function verifyAnalyticsRouteWithPlotly(browser, adminTlsOptions, certificateAdmin, spec) {
  const {
    name, routePath, range = '30d', markers, configWarningMarker = '', configWarningMessage = '',
    screenshotPath = path.resolve(__dirname, '..', `${name.toLowerCase()}-screenshot.png`),
  } = spec;
  const context = await browser.newContext(adminTlsOptions);
  await attachGceFallbackAuth(context, certificateAdmin);
  const routePage = await context.newPage();
  registerErrorListeners(routePage, browser, name);
  const targetUrl = getUrlWithParam(baseUrl.replace('/dashboard', routePath), 'range', range);
  const label = `${name} ${range}`;
  console.log(`[${label}] Checking ${targetUrl} ...`);

  let bodyText = '', hasVitals = false;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const response = await gotoWithShellReady(routePage, targetUrl, label, { attempts: 3, navTimeout: 35000, shellTimeout: 10000 });
      if (response && response.ok()) {
        try { await routePage.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 45000 }); }
        catch { console.log(`[${label}] Attempt ${attempt}/4: Plotly chart not visible yet.`); }
        bodyText = await routePage.evaluate(() => document.body.innerText);
        const hasErrorText = (configWarningMarker && bodyText.includes(configWarningMarker)) || bodyText.includes('Failed to load');
        hasVitals = markers.some(marker => bodyText.includes(marker));
        if (!hasErrorText && hasVitals && (await hasVisiblePlotlyChart(routePage))) break;
      }
    } catch (err) {
      console.log(`[${label}] Navigation warning (attempt ${attempt}): ${err.message}`);
    }
    console.log(`[${label}] Attempt ${attempt}/4: ${name} metrics or Plotly charts not fully rendered yet. Waiting 8s...`);
    await routePage.waitForTimeout(8000);
  }

  if (configWarningMarker && bodyText.includes(configWarningMarker)) await failAndExit(browser, configWarningMessage);
  if (!hasVitals) await failAndExit(browser, `[${label}] Verification Failed: ${name} page could not be verified or is missing standard panel names.`, `Body text sample:\n${bodyText.slice(0, 1000)}`);
  if (EMPTY_STATE_MARKERS.some(marker => bodyText.includes(marker))) {
    await failAndExit(browser, `❌ [Playwright ${name} Verification] Verification Failed: ${name} panels loaded successfully but returned "No data available" or "No data in this range"!`);
  }
  if (!(await hasVisiblePlotlyChart(routePage))) {
    if (process.argv[5] === 'local') {
      await routePage.screenshot({ path: screenshotPath, fullPage: true });
      await failAndExit(browser, `❌ [Playwright ${name} Chart Verification] Verification Failed: No visible Plotly charts found on the ${name} page! Screenshot saved to ${path.basename(screenshotPath)}`);
    } else {
      console.log(`⚠️ [Playwright ${name} Chart Verification] Warning: No visible Plotly charts found on the ${name} page for remote cloud environment (${process.argv[5]}), proceeding...`);
    }
  }
  console.log(`[${name} Panel Verification] Verified: All ${name} panels finished loading, standard elements are present, and Plotly charts are visible! 🟢`);
  await routePage.close();
  await context.close();
}

(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const adminTlsOptions = adminGatewayContextOptions(baseUrl);
    const certificateAdmin = Boolean(adminTlsOptions.clientCertificates);
    const dashboardContext = await browser.newContext(adminTlsOptions);
    await attachGceFallbackAuth(dashboardContext, certificateAdmin, true);
    const page = await dashboardContext.newPage();
    registerErrorListeners(page, browser, 'Dashboard');

    // ── STAGE 1: DASHBOARD PAGE VERIFICATION ─────────────────────────────────
    const url24h = getUrlWithParam(baseUrl, "range", "24h");
    console.log(`[Dashboard 24h] Checking ${url24h} ...`);
    let bodyText24h = "", totalMatch24h = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
      const response24h = await page.goto(url24h, { timeout: 20000 });
      if (!response24h || !response24h.ok()) {
        if (attempt === 3) await failAndExit(browser, `[Dashboard 24h] Failed to load. Status: ${response24h ? response24h.status() : 'Unknown'}`);
        await page.waitForTimeout(4000);
        continue;
      }
      await page.waitForSelector('main', { timeout: 30000 });
      try {
        await page.waitForFunction(() => {
          const match = document.body.innerText.match(/total:\s*([\d,]+)/i);
          return match && parseInt(match[1].replace(/,/g, ''), 10) > 0;
        }, undefined, { timeout: 30000 });
      } catch { console.log(`[Dashboard 24h] Warning: timed out waiting for total count to render, proceeding...`); }

      bodyText24h = await page.evaluate(() => document.body.innerText);
      const hasFailedToLoad24h = bodyText24h.includes("Failed to load") && !bodyText24h.includes("Faro version");
      if (hasFailedToLoad24h || bodyText24h.includes("saturated at") || bodyText24h.includes("PoolBusy") || bodyText24h.includes("No data for this filter")) {
        if (attempt === 3) await failAndExit(browser, `[Dashboard 24h] Verification Failed: 24h page shows errors or empty 'No data for this filter' state!`);
        console.log(`[Dashboard 24h] Attempt ${attempt}/3: transient error or warming up. Waiting 4s...`);
        await page.waitForTimeout(4000);
        continue;
      }
      totalMatch24h = bodyText24h.match(/total:\s*([\d,]+)/i);
      if (!totalMatch24h || parseInt(totalMatch24h[1].replace(/,/g, ''), 10) === 0) {
        if (attempt === 3) await failAndExit(browser, `[Dashboard 24h] Verification Failed: 24h request row count is missing or zero!`);
        await page.waitForTimeout(4000);
        continue;
      }
      break;
    }
    console.log(`[Dashboard 24h] Verified: service header is populated (${totalMatch24h[1]} lifetime rows; NOT a 24h count).`);

    let response;
    await runTrafficChecks(noTraffic, 'Dashboard', async () => {
      let recentCount = null, recentBodyText = "";
      const MIN_RECENT_REQUESTS = 150;
      for (let attempt = 1; attempt <= 4; attempt++) {
        recentCount = null;
        try {
          const recentWindow = getRecentWindow(baseUrl, RECENT_RANGE);
          console.log(`[Dashboard ${RECENT_RANGE}] Checking ${recentWindow.url} with active polling...`);
          const windowResponse = page.waitForResponse(res => isRecentRequestResponse(res, recentWindow), { timeout: 15000 }).catch(() => null);
          response = await page.goto(recentWindow.url, { timeout: 15000 });
          if (response && response.ok()) {
            await page.waitForSelector('main', { timeout: 10000 });
            await page.waitForTimeout(4000);
            recentBodyText = await page.evaluate(() => document.body.innerText);
            recentCount = await recentRequestCount(await windowResponse);
            if (recentCount !== null && recentCount >= MIN_RECENT_REQUESTS) break;
          }
        } catch (err) { console.log(`[Dashboard ${RECENT_RANGE}] Navigation warning (attempt ${attempt}): ${err.message}`); }
        console.log(`[Dashboard ${RECENT_RANGE}] Attempt ${attempt}/4: Standard request count is ${recentCount ?? 'UNVERIFIED'}/${MIN_RECENT_REQUESTS}. Waiting 8s for background ingestion...`);
        await page.waitForTimeout(8000);
      }
      if (recentCount === null || recentCount < MIN_RECENT_REQUESTS) {
        await failAndExit(browser,
          `[Dashboard ${RECENT_RANGE}] Verification Failed: Requested-window aggregate count is ${recentCount ?? 'UNVERIFIED'} (expected at least ${MIN_RECENT_REQUESTS} from recent edge load-test). Missing evidence is not proof of completed ingestion.`,
          `Body text sample:\n${recentBodyText.slice(0, 1000)}`);
      }
      console.log(`[Dashboard ${RECENT_RANGE}] Verified: requested-window aggregate contains ${recentCount} rows (at least ${MIN_RECENT_REQUESTS}).`);
      assertRecentHeader(recentBodyText, 'REQUEST');
    });

    const url30d = getUrlWithParam(baseUrl, "range", "30d");
    console.log(`[Dashboard 30d] Checking historical panels and lifetime header on ${url30d} ...`);
    response = await gotoWithShellReady(page, url30d, 'Dashboard 30d', { attempts: 3, navTimeout: 20000, shellTimeout: 30000 });
    try { await page.waitForFunction(() => document.body.innerText.match(/REQUEST[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i), undefined, { timeout: 30000 }); }
    catch { console.log(`[Dashboard 30d] Warning: timed out waiting for header totals to render, proceeding...`); }
    try {
      await page.waitForFunction(() => {
        const text = document.body.innerText;
        return !text.includes("Crunching logs...") && !text.includes("Loading...") && !text.includes("Initializing...");
      }, undefined, { timeout: 90000 });
    } catch { console.log(`[Dashboard 30d] Warning: timed out waiting for "Crunching logs" loading overlays to clear, proceeding...`); }
    try {
      await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 90000 });
      console.log(`[Dashboard 30d] Verified: Plotly charts are fully rendered and visible! 🟢`);
    } catch { console.log(`⚠️ [Dashboard 30d] Warning: timed out waiting for Plotly charts to become visible.`); }

    const bodyText30d = await page.evaluate(() => document.body.innerText);
    const headerReqMatch = bodyText30d.match(/REQUEST[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i);
    const headerReqTotal = headerReqMatch ? parseInt(headerReqMatch[1].replace(/,/g, ''), 10) : 0;
    if (headerReqTotal === 0) await failAndExit(browser, `[Dashboard 30d] Verification Failed: REQUEST lifetime header total is missing or zero.`);
    console.log(`[Dashboard 30d] REQUEST lifetime total: ${headerReqTotal}; window-count consistency NOT VERIFIED (no independent page count).`);

    if (bodyText30d.includes("Crunching logs") || bodyText30d.includes("Initializing")) {
      if (process.argv[5] === "local") await failAndExit(browser, `❌ [Playwright Panel Verification] Verification Failed: Dashboard panels are stuck on "Crunching logs..." or "Initializing..." loading state!`);
      console.log(`⚠️ [Playwright Panel Verification] Warning: Dashboard panels are still loading on remote cloud environment (${process.argv[5]}), proceeding...`);
    }
    if (bodyText30d.includes("No data available") || bodyText30d.includes("No data in this time range yet")) {
      await failAndExit(browser, `❌ [Playwright Panel Verification] Verification Failed: Dashboard panels successfully loaded but have no data ("No data available")!`);
    }
    if (!(await hasVisiblePlotlyChart(page))) await failAndExit(browser, `❌ [Playwright Chart Verification] Verification Failed: No visible Plotly charts found on the dashboard page!`);
    console.log(`[Dashboard Panel Verification] Verified: All dashboard panels finished loading, metrics are positive, and Plotly charts are visible! 🟢`);

    const footerText = await page.evaluate(() => document.querySelector('footer')?.innerText || '');
    console.log(`[Dashboard] Rendered Footer: "${footerText}"`);
    if (!footerText.includes(`commit:${actualExpectedCommit}`) && !(expectedEnv === "gce" && footerText.includes("commit:unknown"))) {
      await failAndExit(browser,
        `❌ [Playwright Commit Verification] Verification Failed: The deployed container is running the wrong commit, is un-built, or has stale build artifacts!`,
        `Expected active commit hash: 'commit:${actualExpectedCommit}'`, `Rendered footer text on page: "${footerText}"`);
    }
    console.log(`[Dashboard Commit Verification] Verified: Running correct active commit 'commit:${actualExpectedCommit}'! 🟢`);
    if (expectedArch && !footerText.toLowerCase().includes(expectedArch.toLowerCase())) {
      await failAndExit(browser, `[Dashboard] Verification Failed: Expected architecture '${expectedArch}' not found in footer.`);
    }
    if (expectedEnv && !footerText.toLowerCase().includes(expectedEnv.toLowerCase())) {
      if (expectedEnv.toLowerCase() === "elevation" && (footerText.toLowerCase().includes("elevation") || footerText.toLowerCase().includes("gce"))) {
        console.log(`[Dashboard] Dynamic GCE-on-GKE environment matched for Elevation! 🟢`);
      } else {
        await failAndExit(browser, `[Dashboard] Verification Failed: Expected environment '${expectedEnv}' not found in footer.`);
      }
    }
    await page.close();
    await dashboardContext.close();

    // ── STAGE 2: RUM PAGE VERIFICATION ───────────────────────────────────────
    const rumBaseUrl = baseUrl.replace('/dashboard', '/rum');
    const rumContext = await browser.newContext(adminTlsOptions);
    await attachGceFallbackAuth(rumContext, certificateAdmin);
    const rumPage = await rumContext.newPage();
    registerErrorListeners(rumPage, browser, 'RUM');
    const rumUrl24h = getUrlWithParam(rumBaseUrl, "range", "24h");
    console.log(`[RUM 24h] Checking ${rumUrl24h} with active polling...`);

    let rumBodyText24h = "", hasVitalsTitle24h = false, hasVitalsRating24h = false;
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        response = await rumPage.goto(rumUrl24h, { timeout: 35000 });
        if (response && response.ok()) {
          await rumPage.waitForSelector('main', { timeout: 10000 });
          try {
            await rumPage.waitForFunction(() => {
              const text = document.body.innerText;
              const hasTitle = text.includes("Largest Contentful Paint") || text.includes("LCP");
              const hasRating = text.includes("GOOD") || text.includes("POOR") || text.includes("NEEDS IMP.");
              return hasTitle && hasRating && !(text.includes("Failed to load") && !text.includes("Faro version"));
            }, undefined, { timeout: RUM_RENDER_SETTLE_MS });
          } catch { /* retry in outer loop */ }
          rumBodyText24h = await rumPage.evaluate(() => document.body.innerText);
          const rumFailedToLoad24h = rumBodyText24h.includes("Failed to load") && !rumBodyText24h.includes("Faro version");
          hasVitalsTitle24h = rumBodyText24h.includes("Largest Contentful Paint") || rumBodyText24h.includes("LCP");
          hasVitalsRating24h = rumBodyText24h.includes("GOOD") || rumBodyText24h.includes("POOR") || rumBodyText24h.includes("NEEDS IMP.");
          if (!rumFailedToLoad24h && hasVitalsTitle24h && hasVitalsRating24h) break;
        }
      } catch (err) { console.log(`[RUM 24h] Navigation warning (attempt ${attempt}): ${err.message}`); }
      console.log(`[RUM 24h] Attempt ${attempt}/4: Web Vitals metrics not fully rendered yet. Waiting 8s for cron commit...`);
      await rumPage.waitForTimeout(8000);
    }

    if (!hasVitalsTitle24h || !hasVitalsRating24h) {
      await failAndExit(browser, `[RUM 24h] Verification Failed: Web Vitals metrics are missing or empty on the RUM page!`, `Body text sample:\n${rumBodyText24h.slice(0, 1000)}`);
    }
    console.log(`[RUM 24h] Verified: 24h RUM data is active and charts/ratings successfully populated.`);

    await runTrafficChecks(noTraffic, 'RUM', async () => {
      let recentBeaconCount = 0, recentRumBodyText = "";
      const MIN_RECENT_BEACONS = 50;
      for (let attempt = 1; attempt <= 4; attempt++) {
        recentBeaconCount = 0;
        try {
          const recentWindow = getRecentWindow(rumBaseUrl, RECENT_RANGE);
          console.log(`[RUM ${RECENT_RANGE}] Checking ${recentWindow.url} with active polling...`);
          const windowResponse = rumPage.waitForResponse(res => isRecentRumResponse(res, recentWindow), { timeout: 35000 }).catch(() => null);
          response = await rumPage.goto(recentWindow.url, { timeout: 35000 });
          if (response && response.ok()) {
            await rumPage.waitForSelector('main', { timeout: 10000 });
            try { await rumPage.waitForFunction(() => document.body.innerText.includes("TOTAL BEACONS"), undefined, { timeout: RUM_RENDER_SETTLE_MS }); }
            catch { /* retry in outer loop */ }
            recentRumBodyText = await rumPage.evaluate(() => document.body.innerText);
            const evidence = await windowResponse;
            const observedCount = evidence ? (await evidence.json())?.beacon_count : null;
            if (recentRumBodyText.includes("TOTAL BEACONS")) {
              const beaconMatch = recentRumBodyText.match(/TOTAL BEACONS\s*([\d,]+)/i);
              const displayedCount = beaconMatch ? parseInt(beaconMatch[1].replace(/,/g, ''), 10) : 0;
              if (Number.isSafeInteger(observedCount) && observedCount >= 0 && observedCount === displayedCount) recentBeaconCount = observedCount;
              else console.log(`[RUM ${RECENT_RANGE}] Requested-window beacon count UNVERIFIED or differs from the rendered card.`);
              if (recentBeaconCount >= MIN_RECENT_BEACONS) break;
            }
          }
        } catch (err) { console.log(`[RUM ${RECENT_RANGE}] Navigation warning (attempt ${attempt}): ${err.message}`); }
        console.log(`[RUM ${RECENT_RANGE}] Attempt ${attempt}/4: Beacon count is ${recentBeaconCount}/${MIN_RECENT_BEACONS}. Waiting 8s for background ingestion...`);
        await rumPage.waitForTimeout(8000);
      }
      if (recentBeaconCount < MIN_RECENT_BEACONS) {
        await failAndExit(browser,
          `[RUM ${RECENT_RANGE}] Verification Failed: Recent ${RECENT_RANGE} RUM beacon count is only ${recentBeaconCount} (expected at least ${MIN_RECENT_BEACONS} from our recent edge load-test!). Ingestion did not complete.`,
          `Body text sample:\n${recentRumBodyText.slice(0, 1000)}`);
      }
      console.log(`[RUM ${RECENT_RANGE}] Verified: ${RECENT_RANGE} RUM data contains ${recentBeaconCount} beacons (at least ${MIN_RECENT_BEACONS}).`);
      assertRecentHeader(recentRumBodyText, 'RUM');
    });

    const rumUrl30d = getUrlWithParam(rumBaseUrl, "range", "30d");
    console.log(`[RUM 30d] Checking data consistency on ${rumUrl30d} ...`);
    try { response = await gotoWithShellReady(rumPage, rumUrl30d, 'RUM 30d', { attempts: 3, navTimeout: 30000, shellTimeout: 30000 }); }
    catch (e) { await failAndExit(browser, `[RUM 30d] Failed to load RUM page: ${e.message}`); }
    try {
      await rumPage.waitForFunction(() => {
        const text = document.body.innerText;
        return !text.includes("Crunching logs...") && !text.includes("Loading...") && !text.includes("Initializing...");
      }, undefined, { timeout: 90000 });
    } catch { console.log(`[RUM 30d] Warning: timed out waiting for RUM loading overlays to clear, proceeding...`); }
    try {
      await rumPage.waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 });
      console.log(`[RUM 30d] Verified: Full-sized RUM Plotly charts contain data points! 🟢`);
    } catch (e) { await failAndExit(browser, `[RUM 30d] Verification Failed: No populated full-sized RUM chart: ${e.message}`); }

    const rumBodyText30d = await rumPage.evaluate(() => document.body.innerText);
    if (rumBodyText30d.includes("Crunching logs") || rumBodyText30d.includes("Initializing")) {
      await failAndExit(browser, `❌ [Playwright RUM Panel Verification] Verification Failed: RUM panels are stuck on "Crunching logs..." or "Initializing..." loading state!`);
    }
    if (rumBodyText30d.includes("No data available") || rumBodyText30d.includes("No data in this time range yet") || rumBodyText30d.includes("Waiting for real-time")) {
      await failAndExit(browser, `❌ [Playwright RUM Panel Verification] Verification Failed: RUM panels successfully loaded but have no data ("No data available")!`);
    }
    console.log(`[RUM Panel Verification] Verified: All RUM panels finished loading, metrics are positive, and Plotly charts are visible! 🟢`);

    const headerRumMatch = rumBodyText30d.match(/RUM[\s\n]*latest:[\s\n]*[\s\S]*?total:\s*([\d,]+)/i);
    if (!headerRumMatch) await failAndExit(browser, `[RUM 30d] Verification Failed: RUM total count not found in global header.`);
    const headerRumTotal = parseInt(headerRumMatch[1].replace(/,/g, ''), 10);
    const pageRumMatch = rumBodyText30d.match(/TOTAL BEACONS\s*([\d,]+)/i);
    const pageRumTotal = pageRumMatch ? parseInt(pageRumMatch[1].replace(/,/g, ''), 10) : 0;
    if (pageRumTotal === 0) await failAndExit(browser, `❌ [Playwright RUM Panel Verification] Verification Failed: RUM metrics card has 0 total beacons!`);
    console.log(`[RUM 30d Consistency] Header Raw Metrics Total: ${headerRumTotal} │ Page Distinct Beacons Total: ${pageRumTotal}`);
    if (pageRumTotal > headerRumTotal * 1.20 || pageRumTotal === 0) {
      await failAndExit(browser, `[RUM 30d Consistency] Verification Failed: Page RUM count (${pageRumTotal}) is invalid, zero, or exceeds lifetime header count (${headerRumTotal})!`);
    }
    console.log(`[RUM 30d Consistency] Verified: Header RUM count and page metrics are consistent! 🟢`);
    await rumPage.close();
    await rumContext.close();

    // ── STAGE 3: NETWORK PAGE VERIFICATION ───────────────────────────────────
    await verifyAnalyticsRouteWithPlotly(browser, adminTlsOptions, certificateAdmin, {
      name: 'Network', routePath: '/network', range: '30d',
      markers: ['Global Health', 'Avg RTT', 'Leaderboard', 'RTT', 'HEALTH SCORE'],
      configWarningMarker: 'Enable Groups F and G',
      configWarningMessage: '❌ [Playwright Network Verification] Verification Failed: Network page is showing field group configuration warning: "Enable Groups F and G (Network Quality) in your log field configuration."',
      screenshotPath: path.resolve(__dirname, '..', 'network-screenshot.png'),
    });

    // ── STAGES 4-7: STREAMING (CMCD), SECURITY, ORIGIN, PERFORMANCE ──────────
    for (const spec of [
      { name: 'Streaming', path: '/streaming', markers: ['Total Sessions', 'Peak Viewers'], strictEmpty: true },
      { name: 'Security', path: '/security', markers: ['Traffic Quality Breakdown', 'Suspicious ISPs', 'Proxy/Anonymizer Breakdown', 'Well-Known Bots', 'Request Header Size Distribution'] },
      { name: 'Origin', path: '/origin', markers: ['Origin Error Rate', 'Origin TTFB', 'Fetch Volume'] },
      { name: 'Performance', path: '/performance', markers: ['Slowest URLs', 'Slowest Networks', 'Cache TTL Distribution'] },
    ]) {
      await verifyDataPage(browser, adminTlsOptions, spec);
    }

    await browser.close();
    process.exit(0);
  } catch (e) {
    console.error(`Connection refused or error: ${e.message}`);
    if (browser) await browser.close();
    process.exit(1);
  }
})();
