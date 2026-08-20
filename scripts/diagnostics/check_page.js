const { chromium } = require('@playwright/test');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  page.on('console', msg => {
    console.log(`[Browser Console] ${msg.type().toUpperCase()}: ${msg.text()}`);
  });

  page.on('pageerror', err => {
    console.log(`[Browser Exception] ${err.toString()}`);
  });

  const serviceId = process.env.SERVICE_ID || 'cVnu9mYB3Cvmob3lsqjQU3';
  console.log(`Navigating to http://localhost:3001/rum?service=${serviceId} ...`);
  await page.goto(`http://localhost:3001/rum?service=${serviceId}`);

  console.log('Waiting 5s for data fetching...');
  await page.waitForTimeout(5000);

  const bodyText = await page.evaluate(() => document.body.innerText);
  console.log('\n=== PAGE CONTENT ===\n');
  console.log(bodyText);
  console.log('\n=== END PAGE CONTENT ===\n');

  await page.screenshot({ path: 'scratch/rum_screenshot.png', fullPage: true });
  console.log('Screenshot captured at scratch/rum_screenshot.png');

  await browser.close();
})();
