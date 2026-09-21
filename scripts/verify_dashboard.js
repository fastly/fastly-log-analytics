const { chromium } = require('playwright');
const url = process.argv[2];
const expectedCommit = process.argv[3];
const expectedArch = process.argv[4];
const expectedEnv = process.argv[5];

if (!url) {
  console.error("Please provide a target URL.");
  process.exit(1);
}

(async () => {
  try {
    console.log(`Checking ${url} ...`);
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    const response = await page.goto(url, { timeout: 20000 });

    if (!response || !response.ok()) {
      console.error(`[${url}] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }

    try {
      await page.waitForSelector('main', { timeout: 10000 });
      await page.waitForTimeout(4000); // Allow NextJS client-side hydration to fully complete
      const title = await page.title();
      console.log(`[${url}] Success! Title: '${title}'`);

      const footerText = await page.evaluate(() => {
        const footer = document.querySelector('footer');
        return footer ? footer.innerText : '';
      });

      console.log(`[${url}] Rendered Footer: "${footerText}"`);

      // ── 1. Positive Validation: Dashboard Page ──
      const bodyText = await page.evaluate(() => document.body.innerText);
      
      // Assert that standard request metrics are present and non-zero
      if (!bodyText.includes("total:") && !bodyText.includes("REQUEST")) {
        console.error(`[${url}] Verification Failed: Request metrics section not found in page body.`);
        await browser.close();
        process.exit(1);
      }
      
      const totalMatch = bodyText.match(/total:\s*(\d+)/i);
      if (!totalMatch || parseInt(totalMatch[1], 10) === 0) {
        console.error(`[${url}] Verification Failed: Request row count is missing or zero! Ingestion did not populate.`);
        console.error(`Body text sample:\n${bodyText.slice(0, 1000)}`);
        await browser.close();
        process.exit(1);
      }
      console.log(`[${url}] Verified Dashboard request row count is populated: ${totalMatch[1]} total rows.`);

      // Validate Commit Hash
      if (expectedCommit) {
        if (!footerText.includes(`commit:${expectedCommit}`)) {
          if (!bodyText.includes(`commit:${expectedCommit}`)) {
            console.error(`[${url}] Verification Failed: Expected commit hash 'commit:${expectedCommit}' not found in page.`);
            await browser.close();
            process.exit(1);
          }
          console.log(`[${url}] Verified commit hash is active in page body: ${expectedCommit}`);
        } else {
          console.log(`[${url}] Verified commit hash is active in footer: ${expectedCommit}`);
        }
      }

      // Validate Architecture
      if (expectedArch) {
        if (!footerText.toLowerCase().includes(expectedArch.toLowerCase())) {
          console.error(`[${url}] Verification Failed: Expected architecture '${expectedArch}' not found in footer.`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[${url}] Verified architecture matches: ${expectedArch}`);
      }

      // Validate Environment
      if (expectedEnv) {
        if (!footerText.toLowerCase().includes(expectedEnv.toLowerCase())) {
          console.error(`[${url}] Verification Failed: Expected environment '${expectedEnv}' not found in footer.`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[${url}] Verified environment matches: ${expectedEnv}`);
      }

      // ── 2. Positive Validation: RUM Page ──
      if (url.includes('/dashboard')) {
        const rumUrl = url.replace('/dashboard', '/rum');
        console.log(`[${url}] Checking RUM page: ${rumUrl} ...`);
        const rumResponse = await page.goto(rumUrl, { timeout: 15000 });
        if (!rumResponse || !rumResponse.ok()) {
          console.error(`[${rumUrl}] Failed to load RUM page. Status: ${rumResponse ? rumResponse.status() : 'Unknown'}`);
          await browser.close();
          process.exit(1);
        }
        await page.waitForSelector('main', { timeout: 10000 });
        await page.waitForTimeout(4000);
        
        const rumBodyText = await page.evaluate(() => document.body.innerText);

        // Assert that the RUM Page components are rendered
        if (!rumBodyText.includes("TOTAL BEACONS") || !rumBodyText.includes("PAGEVIEWS")) {
          console.error(`[${rumUrl}] Verification Failed: RUM page metrics card headers (TOTAL BEACONS/PAGEVIEWS) not found.`);
          await browser.close();
          process.exit(1);
        }

        // Confirm Largest Contentful Paint chart and Good/Poor/Needs Imp rating exist, proving data is present
        const hasVitalsTitle = rumBodyText.includes("Largest Contentful Paint") || rumBodyText.includes("LCP");
        const hasVitalsRating = rumBodyText.includes("GOOD") || rumBodyText.includes("POOR") || rumBodyText.includes("NEEDS IMP.");
        
        if (!hasVitalsTitle || !hasVitalsRating) {
          console.error(`[${rumUrl}] Verification Failed: Web Vitals metrics are missing or empty on the RUM page! Ingestion failed or did not populate.`);
          console.error("Body text sample:");
          console.error(rumBodyText.slice(0, 1000));
          await browser.close();
          process.exit(1);
        }
        console.log(`[${rumUrl}] Verified RUM metrics successfully populated on page (Vitals & Ratings active).`);
      }

    } catch (e) {
      console.error(`[${url}] Timed out waiting for content. Error: ${e.message}`);
      await browser.close();
      process.exit(1);
    }

    await browser.close();
    process.exit(0);
  } catch (e) {
    console.error(`[${url}] Connection refused or error: ${e.message}`);
    process.exit(1);
  }
})();
