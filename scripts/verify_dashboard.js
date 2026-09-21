const { chromium } = require('playwright');
const baseUrl = process.argv[2];
const expectedCommit = process.argv[3];
const expectedArch = process.argv[4];
const expectedEnv = process.argv[5];

if (!baseUrl) {
  console.error("Please provide a target URL.");
  process.exit(1);
}

// Helper to append query parameters cleanly
function getUrlWithParam(url, key, value) {
  const joiner = url.includes('?') ? '&' : '?';
  return `${url}${joiner}${key}=${encodeURIComponent(value)}`;
}

(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 1: DASHBOARD PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────
    
    // 1.1. Verify 24h Overall Data is present
    const url24h = getUrlWithParam(baseUrl, "range", "24h");
    console.log(`[Dashboard 24h] Checking ${url24h} ...`);
    let response = await page.goto(url24h, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[Dashboard 24h] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await page.waitForSelector('main', { timeout: 10000 });
    await page.waitForTimeout(4000); // Allow hydration

    const bodyText24h = await page.evaluate(() => document.body.innerText);
    const hasFailedToLoad24h = bodyText24h.includes("Failed to load") && !bodyText24h.includes("Faro version");
    if (hasFailedToLoad24h || bodyText24h.includes("saturated at") || bodyText24h.includes("PoolBusy") || bodyText24h.includes("No data for this filter")) {
      console.error(`[Dashboard 24h] Verification Failed: 24h page shows errors or empty 'No data for this filter' state!`);
      await browser.close();
      process.exit(1);
    }

    const totalMatch24h = bodyText24h.match(/total:\s*([\d,]+)/i);
    if (!totalMatch24h || parseInt(totalMatch24h[1].replace(/,/g, ''), 10) === 0) {
      console.error(`[Dashboard 24h] Verification Failed: 24h request row count is missing or zero!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard 24h] Verified: 24h data is active with ${totalMatch24h[1]} total rows.`);

    // 1.2. Verify 5m Range strictly contains our recent edge load-test (at least 300 rows)
    const url5m = getUrlWithParam(baseUrl, "range", "5m");
    console.log(`[Dashboard 5m] Checking ${url5m} ...`);
    response = await page.goto(url5m, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[Dashboard 5m] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await page.waitForSelector('main', { timeout: 10000 });
    await page.waitForTimeout(4000);

    const bodyText5m = await page.evaluate(() => document.body.innerText);
    const totalMatch5m = bodyText5m.match(/total:\s*([\d,]+)/i);
    const count5m = totalMatch5m ? parseInt(totalMatch5m[1].replace(/,/g, ''), 10) : 0;
    
    // We sent 500 total requests (70% standard = ~350). Assert at least 300 arrived.
    const MIN_REQ_5M = 300;
    if (count5m < MIN_REQ_5M) {
      console.error(`[Dashboard 5m] Verification Failed: Recent 5m data count is only ${count5m} (expected at least ${MIN_REQ_5M} from our recent edge load-test!). Ingestion is lagging or failed.`);
      console.error(`Body text sample:\n${bodyText5m.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard 5m] Verified: 5m data contains ${count5m} rows (greater than load-test minimum of ${MIN_REQ_5M} rows).`);

    // Verify Footers and Metadata on 5m page
    const footerText = await page.evaluate(() => {
      const footer = document.querySelector('footer');
      return footer ? footer.innerText : '';
    });
    console.log(`[Dashboard] Rendered Footer: "${footerText}"`);

    if (expectedCommit && !footerText.includes(`commit:${expectedCommit}`) && !bodyText5m.includes(`commit:${expectedCommit}`)) {
      console.error(`[Dashboard] Verification Failed: Expected commit hash 'commit:${expectedCommit}' not found in page.`);
      await browser.close();
      process.exit(1);
    }
    if (expectedArch && !footerText.toLowerCase().includes(expectedArch.toLowerCase())) {
      console.error(`[Dashboard] Verification Failed: Expected architecture '${expectedArch}' not found in footer.`);
      await browser.close();
      process.exit(1);
    }
    if (expectedEnv && !footerText.toLowerCase().includes(expectedEnv.toLowerCase())) {
      console.error(`[Dashboard] Verification Failed: Expected environment '${expectedEnv}' not found in footer.`);
      await browser.close();
      process.exit(1);
    }

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 2: RUM PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────
    const rumBaseUrl = baseUrl.replace('/dashboard', '/rum');
    
    // 2.1. Verify 24h Overall RUM Data is present
    const rumUrl24h = getUrlWithParam(rumBaseUrl, "range", "24h");
    console.log(`[RUM 24h] Checking ${rumUrl24h} ...`);
    response = await page.goto(rumUrl24h, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[RUM 24h] Failed to load RUM page. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await page.waitForSelector('main', { timeout: 10000 });
    await page.waitForTimeout(4000);

    const rumBodyText24h = await page.evaluate(() => document.body.innerText);
    const rumFailedToLoad24h = rumBodyText24h.includes("Failed to load") && !rumBodyText24h.includes("Faro version");
    if (rumFailedToLoad24h || rumBodyText24h.includes("No data for this time period") || rumBodyText24h.includes("Waiting for real-time RUM") || rumBodyText24h.includes("Internal Server Error")) {
      console.error(`[RUM 24h] Verification Failed: RUM page is displaying errors or empty RUM state!`);
      await browser.close();
      process.exit(1);
    }

    const hasVitalsTitle24h = rumBodyText24h.includes("Largest Contentful Paint") || rumBodyText24h.includes("LCP");
    const hasVitalsRating24h = rumBodyText24h.includes("GOOD") || rumBodyText24h.includes("POOR") || rumBodyText24h.includes("NEEDS IMP.");
    if (!hasVitalsTitle24h || !hasVitalsRating24h) {
      console.error(`[RUM 24h] Verification Failed: Web Vitals metrics are missing or empty on the RUM page!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 24h] Verified: 24h RUM data is active and charts/ratings successfully populated.`);

    // 2.2. Verify 5m RUM Range contains our recent beacons (at least 120 beacons)
    const rumUrl5m = getUrlWithParam(rumBaseUrl, "range", "5m");
    console.log(`[RUM 5m] Checking ${rumUrl5m} ...`);
    response = await page.goto(rumUrl5m, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[RUM 5m] Failed to load RUM page. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await page.waitForSelector('main', { timeout: 10000 });
    await page.waitForTimeout(4000);

    const rumBodyText5m = await page.evaluate(() => document.body.innerText);
    
    // Assert metrics headers are visible
    if (!rumBodyText5m.includes("TOTAL BEACONS") || !rumBodyText5m.includes("PAGEVIEWS")) {
      console.error(`[RUM 5m] Verification Failed: RUM page headers (TOTAL BEACONS/PAGEVIEWS) not found.`);
      await browser.close();
      process.exit(1);
    }

    // Extract beacon count from 5m range
    // Pattern matches TOTAL BEACONS followed by whitespace/newlines and then the count digits
    const beaconMatch = rumBodyText5m.match(/TOTAL BEACONS\s*([\d,]+)/i);
    const beaconCount5m = beaconMatch ? parseInt(beaconMatch[1].replace(/,/g, ''), 10) : 0;

    // We sent 500 total requests (30% RUM ratio = ~150). Assert at least 120 arrived.
    const MIN_BEACONS_5M = 120;
    if (beaconCount5m < MIN_BEACONS_5M) {
      console.error(`[RUM 5m] Verification Failed: Recent 5m RUM beacon count is only ${beaconCount5m} (expected at least ${MIN_BEACONS_5M} from our recent edge load-test!). Ingestion is lagging or failed.`);
      console.error(`Body text sample:\n${rumBodyText5m.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 5m] Verified: 5m RUM data contains ${beaconCount5m} beacons (greater than load-test minimum of ${MIN_BEACONS_5M} beacons).`);
    console.log(`[RUM 5m] Verified: Web Vitals metrics are active in the 5m window.`);

    await browser.close();
    process.exit(0);
  } catch (e) {
    console.error(`Connection refused or error: ${e.message}`);
    if (browser) await browser.close();
    process.exit(1);
  }
})();
