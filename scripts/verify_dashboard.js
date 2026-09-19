const { chromium } = require('playwright');
const url = process.argv[2];
const expectedCommit = process.argv[3];

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

      if (expectedCommit) {
        const bodyText = await page.evaluate(() => document.body.innerText);
        if (!bodyText.includes(`commit:${expectedCommit}`)) {
          console.error(`[${url}] Verification Failed: Expected commit hash 'commit:${expectedCommit}' not found in body text.`);
          console.error(`Body text sample:\n${bodyText.slice(-300)}`);
          await browser.close();
          process.exit(1);
        }
        console.log(`[${url}] Verified commit hash is active: ${expectedCommit}`);
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
