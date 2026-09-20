const { chromium } = require('playwright');
const url = process.argv[2];
const expectedCommit = process.argv[3];
const expectedArch = process.argv[4];
const expectedEnv = process.argv[5];

if (!url) {
  console.error("Please provide a URL to check.");
  process.exit(1);
}

(async () => {
  console.log(`Checking ${url} ...`);
  try {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    const response = await page.goto(url, { timeout: 15000 });
    if (!response || !response.ok()) {
      console.error(`[${url}] Failed to load. Status: ${response ? response.status() : 'Unknown'}`);
      await browser.close();
      process.exit(1);
    }

    try {
      await page.waitForSelector('main', { timeout: 10000 });
      await page.waitForTimeout(3000);
      const title = await page.title();
      console.log(`[${url}] Success! Title: '${title}'`);

      const footerText = await page.evaluate(() => {
        const footer = document.querySelector('footer');
        return footer ? footer.innerText : '';
      });

      console.log(`[${url}] Rendered Footer: "${footerText}"`);

      const bodyText = await page.evaluate(() => document.body.innerText);
      if (bodyText.includes("Failed to load") || bodyText.includes("saturated at") || bodyText.includes("PoolBusy")) {
        console.error(`[${url}] Verification Failed: Dashboard is displaying a 'Failed to load' or pool saturation error!`);
        console.error(`Body text sample:\n${bodyText.slice(0, 500)}`);
        await browser.close();
        process.exit(1);
      }

      if (expectedCommit) {
        if (!footerText.includes(`commit:${expectedCommit}`)) {
          // Fallback to body text in case footer selector missed
          const bodyText = await page.evaluate(() => document.body.innerText);
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

      if (expectedArch) {
        if (!footerText.toLowerCase().includes(expectedArch.toLowerCase())) {
          console.error(`[${url}] Verification Failed: Expected architecture '${expectedArch}' not found in footer.`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[${url}] Verified architecture matches: ${expectedArch}`);
      }

      if (expectedEnv) {
        if (!footerText.toLowerCase().includes(expectedEnv.toLowerCase())) {
          console.error(`[${url}] Verification Failed: Expected environment '${expectedEnv}' not found in footer.`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[${url}] Verified environment matches: ${expectedEnv}`);
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
