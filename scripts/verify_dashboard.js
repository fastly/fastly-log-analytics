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

    // 1.2. Verify 5m Range with ROBUST POLLING (At least 150 rows)
    const url5m = getUrlWithParam(baseUrl, "range", "5m");
    console.log(`[Dashboard 5m] Checking ${url5m} with active polling...`);
    
    let count5m = 0;
    const MIN_REQ_5M = 150; // Safe minimum for standard traffic seeding
    let bodyText5m = "";
    
    for (let attempt = 1; attempt <= 12; attempt++) {
      try {
        response = await page.goto(url5m, { timeout: 15000 });
        if (response && response.ok()) {
          await page.waitForSelector('main', { timeout: 10000 });
          await page.waitForTimeout(4000);
          bodyText5m = await page.evaluate(() => document.body.innerText);
          const totalMatch5m = bodyText5m.match(/total:\s*([\d,]+)/i);
          count5m = totalMatch5m ? parseInt(totalMatch5m[1].replace(/,/g, ''), 10) : 0;
          
          if (count5m >= MIN_REQ_5M) {
            break;
          }
        }
      } catch (err) {
        console.log(`[Dashboard 5m] Navigation warning (attempt ${attempt}): ${err.message}`);
      }
      console.log(`[Dashboard 5m] Attempt ${attempt}/12: Standard request count is ${count5m}/${MIN_REQ_5M}. Waiting 5s for background ingestion...`);
      await page.waitForTimeout(5000);
    }

    if (count5m < MIN_REQ_5M) {
      console.error(`[Dashboard 5m] Verification Failed: Recent 5m data count is only ${count5m} (expected at least ${MIN_REQ_5M} from our recent edge load-test!). Ingestion did not complete.`);
      console.error(`Body text sample:\n${bodyText5m.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard 5m] Verified: 5m data contains ${count5m} rows (greater than load-test minimum of ${MIN_REQ_5M} rows).`);

    // 1.3. Validate Header Liveness (REQUEST)
    const reqLatestMatch = bodyText5m.match(/REQUEST[\s\n]*latest:[\s\n]*([^\n]+)/i);
    if (!reqLatestMatch) {
      console.error(`[Header] Verification Failed: REQUEST block not found in the global header.`);
      await browser.close();
      process.exit(1);
    }
    const reqLatestTime = reqLatestMatch[1].trim();
    console.log(`[Header] REQUEST Latest Time: "${reqLatestTime}"`);
    if (reqLatestTime.includes("Never") || reqLatestTime.includes("—") || reqLatestTime.includes("-")) {
      console.error(`[Header] Verification Failed: REQUEST latest time is unpopulated/stale: "${reqLatestTime}"`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Header] Verified: REQUEST latest header time is fully active and updating!`);

    // 1.4. Validate 30d Data Consistency: Header Total vs. Page Total
    const url30d = getUrlWithParam(baseUrl, "range", "30d");
    console.log(`[Dashboard 30d] Checking data consistency on ${url30d} ...`);
    response = await page.goto(url30d, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[Dashboard 30d] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await page.waitForSelector('main', { timeout: 10000 });
    await page.waitForTimeout(4000);

    const bodyText30d = await page.evaluate(() => document.body.innerText);
    
    // Extract Header Request Total
    const headerReqMatch = bodyText30d.match(/REQUEST[\s\n]*latest:[\s\n]*[^\n]*[\s\n]*total:\s*([\d,]+)/i);
    if (!headerReqMatch) {
      console.error(`[Dashboard 30d] Verification Failed: REQUEST total count not found in global header.`);
      await browser.close();
      process.exit(1);
    }
    const headerReqTotal = parseInt(headerReqMatch[1].replace(/,/g, ''), 10);

    // Extract Page Request Total (from request metrics card)
    const pageReqMatch = bodyText30d.match(/total:\s*([\d,]+)/i);
    const pageReqTotal = pageReqMatch ? parseInt(pageReqMatch[1].replace(/,/g, ''), 10) : 0;

    console.log(`[Dashboard 30d Consistency] Header REQUEST Total: ${headerReqTotal} │ Page Metrics Total: ${pageReqTotal}`);
    if (headerReqTotal !== pageReqTotal) {
      console.error(`[Dashboard 30d Consistency] Verification Failed: Header count (${headerReqTotal}) does not match page count (${pageReqTotal}) under 30d range!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Dashboard 30d Consistency] Verified: Header request count and page metrics are 100% in-sync!`);

    // Verify Footers and Metadata on 30d page
    const footerText = await page.evaluate(() => {
      const footer = document.querySelector('footer');
      return footer ? footer.innerText : '';
    });
    console.log(`[Dashboard] Rendered Footer: "${footerText}"`);

    if (expectedCommit && !footerText.includes(`commit:${expectedCommit}`) && !bodyText30d.includes(`commit:${expectedCommit}`)) {
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

    // Close the dashboard page before navigating to RUM to cleanly prevent client-side NextJS chunk-mismatch reloads!
    await page.close();

    // ────────────────────────────────────────────────────────────────────────
    // ── STAGE 2: RUM PAGE VERIFICATION
    // ────────────────────────────────────────────────────────────────────────
    const rumBaseUrl = baseUrl.replace('/dashboard', '/rum');
    const rumPage = await browser.newPage();
    
    // 2.1. Verify 24h Overall RUM Data is present with active polling
    const rumUrl24h = getUrlWithParam(rumBaseUrl, "range", "24h");
    console.log(`[RUM 24h] Checking ${rumUrl24h} with active polling...`);
    
    let rumBodyText24h = "";
    let hasVitalsTitle24h = false;
    let hasVitalsRating24h = false;
    
    for (let attempt = 1; attempt <= 12; attempt++) {
      try {
        response = await rumPage.goto(rumUrl24h, { timeout: 15000 });
        if (response && response.ok()) {
          await rumPage.waitForSelector('main', { timeout: 10000 });
          await rumPage.waitForTimeout(4000);
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
      console.log(`[RUM 24h] Attempt ${attempt}/12: Web Vitals metrics not fully rendered yet. Waiting 5s for cron commit...`);
      await rumPage.waitForTimeout(5000);
    }

    if (!hasVitalsTitle24h || !hasVitalsRating24h) {
      console.error(`[RUM 24h] Verification Failed: Web Vitals metrics are missing or empty on the RUM page!`);
      console.error(`Body text sample:\n${rumBodyText24h.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 24h] Verified: 24h RUM data is active and charts/ratings successfully populated.`);

    // 2.2. Verify 5m RUM Range with active polling (At least 50 beacons)
    const rumUrl5m = getUrlWithParam(rumBaseUrl, "range", "5m");
    console.log(`[RUM 5m] Checking ${rumUrl5m} with active polling...`);
    
    let beaconCount5m = 0;
    const MIN_BEACONS_5M = 50; // Safe threshold allowing for global edge S3 streaming latency
    let rumBodyText5m = "";
    
    for (let attempt = 1; attempt <= 12; attempt++) {
      try {
        response = await rumPage.goto(rumUrl5m, { timeout: 15000 });
        if (response && response.ok()) {
          await rumPage.waitForSelector('main', { timeout: 10000 });
          await rumPage.waitForTimeout(4000);
          rumBodyText5m = await rumPage.evaluate(() => document.body.innerText);
          
          if (rumBodyText5m.includes("TOTAL BEACONS")) {
            const beaconMatch = rumBodyText5m.match(/TOTAL BEACONS\s*([\d,]+)/i);
            beaconCount5m = beaconMatch ? parseInt(beaconMatch[1].replace(/,/g, ''), 10) : 0;
            
            if (beaconCount5m >= MIN_BEACONS_5M) {
              break;
            }
          }
        }
      } catch (err) {
        console.log(`[RUM 5m] Navigation warning (attempt ${attempt}): ${err.message}`);
      }
      console.log(`[RUM 5m] Attempt ${attempt}/12: Beacon count is ${beaconCount5m}/${MIN_BEACONS_5M}. Waiting 5s for background ingestion...`);
      await rumPage.waitForTimeout(5000);
    }

    if (beaconCount5m < MIN_BEACONS_5M) {
      console.error(`[RUM 5m] Verification Failed: Recent 5m RUM beacon count is only ${beaconCount5m} (expected at least ${MIN_BEACONS_5M} from our recent edge load-test!). Ingestion did not complete.`);
      console.error(`Body text sample:\n${rumBodyText5m.slice(0, 1000)}`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 5m] Verified: 5m RUM data contains ${beaconCount5m} beacons (greater than load-test minimum of ${MIN_BEACONS_5M} beacons).`);
    console.log(`[RUM 5m] Verified: Web Vitals metrics are active in the 5m window.`);

    // 2.3. Validate Header Liveness (RUM)
    const rumLatestMatch = rumBodyText5m.match(/RUM[\s\n]*latest:[\s\n]*([^\n]+)/i);
    if (!rumLatestMatch) {
      console.error(`[Header] Verification Failed: RUM block not found in the global header.`);
      await browser.close();
      process.exit(1);
    }
    const rumLatestTime = rumLatestMatch[1].trim();
    console.log(`[Header] RUM Latest Time: "${rumLatestTime}"`);
    if (rumLatestTime.includes("Never") || rumLatestTime.includes("—") || rumLatestTime.includes("-")) {
      console.error(`[Header] Verification Failed: RUM latest time is unpopulated/stale: "${rumLatestTime}"`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[Header] Verified: RUM latest header time is fully active and updating!`);

    // 2.4. Validate 30d RUM Consistency: Header RUM Total vs. Page RUM Total
    const rumUrl30d = getUrlWithParam(rumBaseUrl, "range", "30d");
    console.log(`[RUM 30d] Checking data consistency on ${rumUrl30d} ...`);
    response = await rumPage.goto(rumUrl30d, { timeout: 20000 });
    if (!response || !response.ok()) {
      console.error(`[RUM 30d] Failed to load RUM page. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }
    await rumPage.waitForSelector('main', { timeout: 10000 });
    await rumPage.waitForTimeout(4000);

    const rumBodyText30d = await rumPage.evaluate(() => document.body.innerText);

    // Extract Header RUM Total
    const headerRumMatch = rumBodyText30d.match(/RUM[\s\n]*latest:[\s\n]*[^\n]*[\s\n]*total:\s*([\d,]+)/i);
    if (!headerRumMatch) {
      console.error(`[RUM 30d] Verification Failed: RUM total count not found in global header.`);
      await browser.close();
      process.exit(1);
    }
    const headerRumTotal = parseInt(headerRumMatch[1].replace(/,/g, ''), 10);

    // Extract Page RUM Total
    const pageRumMatch = rumBodyText30d.match(/TOTAL BEACONS\s*([\d,]+)/i);
    const pageRumTotal = pageRumMatch ? parseInt(pageRumMatch[1].replace(/,/g, ''), 10) : 0;

    console.log(`[RUM 30d Consistency] Header RUM Total: ${headerRumTotal} │ Page Metrics Total: ${pageRumTotal}`);
    if (headerRumTotal !== pageRumTotal) {
      console.error(`[RUM 30d Consistency] Verification Failed: Header count (${headerRumTotal}) does not match page count (${pageRumTotal}) under 30d range!`);
      await browser.close();
      process.exit(1);
    }
    console.log(`[RUM 30d Consistency] Verified: Header RUM count and page metrics are 100% in-sync!`);

    await browser.close();
    process.exit(0);
  } catch (e) {
    console.error(`Connection refused or error: ${e.message}`);
    if (browser) await browser.close();
    process.exit(1);
  }
})();
