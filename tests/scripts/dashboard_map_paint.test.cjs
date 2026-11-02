const assert = require('node:assert/strict');
const fs = require('node:fs');
const { after, before, test } = require('node:test');
const { chromium } = require('../../frontend/node_modules/@playwright/test');
const { measureCompositedResults } = require('../../scripts/lib/composited_results.cjs');
const {
  dashboardMapReference,
  mountDashboardMapFixture,
} = require('../../scripts/lib/dashboard_map_paint.cjs');

let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

const oldData = [{ country: 'US', count: 10 }, { country: 'BR', count: 5 }];
const currentData = [{ country: 'DE', count: 120 }, { country: 'US', count: 10 }];

async function observeMap({
  mode = 'update', overlay = false, theme = 'light', prepare, data = currentData,
  width = 700, height = 360,
} = {}) {
  const context = await browser.newContext({
    viewport: { width: 900, height: 700 },
    deviceScaleFactor: 1,
  });
  try {
    const page = await context.newPage();
    await mountDashboardMapFixture(page, {
      initialData: oldData,
      nextData: data,
      mode,
      overlay,
      theme,
      width,
      height,
    });
    if (prepare) await prepare(page);
    const result = await measureCompositedResults(page, {
      target: '#map-action',
      timeoutMs: 20000,
      reference: () => dashboardMapReference(page, data),
    }, async () => {
      await page.locator('#map-action').click();
      await page.waitForFunction(() => window.done > 0);
    });
    return { page, context, result };
  } catch (error) {
    await context.close();
    throw error;
  }
}

test('Dashboard map paint compares independent pixels generated from current country data', async () => {
  const { context, result } = await observeMap();
  try {
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
    assert.equal(result.results.source, 'compositor_frame');
    assert.equal(result.results.panels_required, 1);
    assert.equal(result.results.pixel_scope, 'opaque_reference_pixels');
  } finally { await context.close(); }
});

async function productionOverlayShadows(page) {
  await page.addStyleTag({ content: `
    #map-mount>[role="group"]>div[class~="top-3"]>button{
      box-sizing:border-box;width:126px;height:30px;border:1px solid #ddd;border-radius:6px;
      background:rgba(255,255,255,.95);backdrop-filter:blur(8px)}
    #map-mount>[role="group"]>div[class~="bottom-4"]{width:240px;height:60px;
      border:1px solid #ddd;border-radius:6px;background:rgba(255,255,255,.95);
      backdrop-filter:blur(8px)}
    #map-mount>[role="group"]>div[class~="top-3"]>button,
    #map-mount>[role="group"]>div[class~="bottom-4"]{
      box-shadow:0 4px 6px -1px #0000001a,0 2px 4px -2px #0000001a}
  ` });
}

test('production picker and legend shadows do not invalidate exact canonical map paint', async () => {
  const { context, result } = await observeMap({
    width: 521, height: 441, data: [{ country: 'CN', count: 100 }],
    prepare: productionOverlayShadows,
  });
  try {
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  } finally { await context.close(); }
});

test('production oklab legend ring mixed with transparent rgba shadows preserves exact map paint', async () => {
  const { context, page, result } = await observeMap({
    width: 521, height: 441, data: [{ country: 'CN', count: 100 }],
    prepare: async page => {
      await productionOverlayShadows(page);
      await page.addStyleTag({ content: `
        #map-mount>[role="group"]>div[class~="top-3"]>button,
        #map-mount>[role="group"]>div[class~="bottom-4"]{
          box-shadow:rgba(0,0,0,0) 0px 0px 0px 0px,rgba(0,0,0,0) 0px 0px 0px 0px,
            rgba(0,0,0,0) 0px 0px 0px 0px,rgba(0,0,0,0) 0px 0px 0px 0px,
            rgba(0,0,0,.1) 0px 4px 6px -1px,rgba(0,0,0,.1) 0px 2px 4px -2px}
        #map-mount div[class~="ring-1"]{
          box-shadow:rgba(0,0,0,0) 0px 0px 0px 0px,rgba(0,0,0,0) 0px 0px 0px 0px,
            rgba(0,0,0,0) 0px 0px 0px 0px,oklab(0 0 0 / .05) 0px 0px 0px 1px,
            rgba(0,0,0,0) 0px 0px 0px 0px}
      ` });
    },
  });
  try {
    assert.match(await page.locator('div[class~="ring-1"]').evaluate(element =>
      getComputedStyle(element).boxShadow), /oklab\(0 0 0 \/ 0\.05\)/);
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  } finally { await context.close(); }
});

test('shadow masking preserves stale-fill rejection outside known overlay paint', async () => {
  const { context, result } = await observeMap({
    width: 521, height: 441, data: [{ country: 'CN', count: 100 }],
    mode: 'stale', prepare: productionOverlayShadows,
  });
  try {
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
  } finally { await context.close(); }
});

test('oversized overlay shadows still fail the original masked-area cap', async () => {
  const context = await browser.newContext({
    viewport: { width: 900, height: 700 }, deviceScaleFactor: 1,
  });
  try {
    const page = await context.newPage();
    await mountDashboardMapFixture(page, {
      initialData: oldData, nextData: currentData, width: 521, height: 441,
    });
    await productionOverlayShadows(page);
    for (const color of ['black', 'oklab(0 0 0 / 0.05)']) {
      await page.locator('button[aria-haspopup="listbox"]').evaluate((element, color) => {
        element.style.boxShadow = `0px 0px 80px 0px ${color}`;
      }, color);
      await assert.rejects(dashboardMapReference(page, currentData), /dashboard_map_overlay_coverage_unsupported/);
    }
  } finally { await context.close(); }
});

test('the production Tailwind clip-path sr-only country table is not a visible overlay', async () => {
  const { context, page, result } = await observeMap({
    prepare: page => page.addStyleTag({ content: `
      #map-mount .sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;
        overflow:hidden;clip:auto;clip-path:inset(50%);white-space:nowrap;border-width:0}
    ` }),
  });
  try {
    assert.equal(await page.locator('table.sr-only').evaluate(element =>
      getComputedStyle(element).clipPath), 'inset(50%)');
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  } finally { await context.close(); }
});

test('a sr-only class alone cannot hide a visible country table from overlay validation', async () => {
  const context = await browser.newContext({
    viewport: { width: 900, height: 700 }, deviceScaleFactor: 1,
  });
  try {
    const page = await context.newPage();
    await mountDashboardMapFixture(page, { initialData: oldData, nextData: currentData });
    await page.addStyleTag({ content: '#map-mount .sr-only{clip:auto;clip-path:inset(49%)}' });
    await assert.rejects(dashboardMapReference(page, currentData), /dashboard_map_unknown_overlay/);
  } finally { await context.close(); }
});

test('current country rows cannot qualify while the map retains an older country fill', async () => {
  const { context, page, result } = await observeMap({ mode: 'stale' });
  try {
    assert.match(await page.locator('tbody').innerText(), /Germany/);
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
    assert.equal(result.results.elapsed_ms, null);
  } finally { await context.close(); }
});

test('current country rows cannot qualify when the map has no positive country fill', async () => {
  const { context, page, result } = await observeMap({ mode: 'missing' });
  try {
    assert.match(await page.locator('tbody').innerText(), /Germany/);
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
    assert.equal(result.results.elapsed_ms, null);
  } finally { await context.close(); }
});

test('an unrecognized overlay over map pixels is not hidden by the known-control mask', async () => {
  const { context, result } = await observeMap({ overlay: true });
  try {
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
    assert.equal(result.results.elapsed_ms, null);
  } finally { await context.close(); }
});

async function productionBackgrounds(page) {
  const css = fs.readFileSync(require.resolve('../../frontend/app/globals.css'), 'utf8');
  const variables = [css.match(/:root\s*\{[^}]*\}/)?.[0], css.match(/\.dark\s*\{[^}]*\}/)?.[0]];
  assert.ok(variables.every(Boolean));
  await page.addStyleTag({ content: `${variables.join('\n')}
    html,body{background:var(--background)!important}
    #map-mount{background:color-mix(in oklab,var(--muted) 20%,transparent)}
    #map-mount>[role="group"]{background:color-mix(in oklab,var(--muted) 10%,transparent)!important}
  ` });
}

for (const theme of ['light', 'dark']) {
  test(`canonical map pixels match production translucent ancestor backgrounds in ${theme} theme`, async () => {
    const { context, page, result } = await observeMap({ theme, prepare: productionBackgrounds });
    try {
      assert.match(await page.locator('[role="group"]').evaluate(element =>
        getComputedStyle(element).backgroundColor), /0\.1\)/);
      assert.equal(result.results.status, 'observed', JSON.stringify(result));
    } finally { await context.close(); }
  });
}

test('country coverage uses the production country resolver and normalization', async () => {
  const data = [{ country: 'us', count: 100 }, { country: 'AU', count: 60 }];
  const { context, page, result } = await observeMap({ data });
  try {
    assert.deepEqual(await page.evaluate(() => ['us', 'AU'].map(window.__dashboardCountryName)),
      ['United States of America', 'Australia']);
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  } finally { await context.close(); }
});

test('oversized map bounds are refused before reference rendering', async () => {
  const context = await browser.newContext({
    viewport: { width: 4000, height: 2200 }, deviceScaleFactor: 1,
  });
  try {
    const page = await context.newPage();
    await page.setContent(`<style>body{margin:0}div,canvas{width:4000px;height:2100px}</style>
      <div role="group" aria-label="World map of requests by country">
        <canvas class="maplibregl-canvas" width="4000" height="2100"></canvas>
      </div>`);
    await assert.rejects(dashboardMapReference(page, currentData), /dashboard_map_geometry_unsupported/);
    assert.equal(browser.contexts().length, 1);
  } finally { await context.close(); }
});

test('a gradient or faded ancestor fails closed instead of becoming an invented opaque backdrop', async () => {
  const context = await browser.newContext({
    viewport: { width: 900, height: 700 }, deviceScaleFactor: 1,
  });
  try {
    const page = await context.newPage();
    await mountDashboardMapFixture(page, { initialData: oldData, nextData: currentData });
    await productionBackgrounds(page);
    for (const style of ['opacity:0.5', 'background-image:linear-gradient(white,black)']) {
      await page.locator('#map-mount').evaluate((element, style) => { element.style.cssText = style; }, style);
      await assert.rejects(dashboardMapReference(page, currentData), /dashboard_map_background_unsupported/);
    }
  } finally { await context.close(); }
});
