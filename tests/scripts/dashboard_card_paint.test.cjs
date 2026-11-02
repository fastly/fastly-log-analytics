const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { createRequire } = require('node:module');
const { after, afterEach, before, beforeEach, describe, it } = require('node:test');

const frontend = path.resolve(__dirname, '../../frontend');
const frontendRequire = createRequire(path.join(frontend, 'package.json'));
const { chromium } = frontendRequire('@playwright/test');
const { dashboardCardReference, renderDashboardCardMarkup } = require('../../scripts/lib/dashboard_card_paint.cjs');
const { measureCompositedResults } = require('../../scripts/lib/composited_results.cjs');

const card = { id: 'host', label: 'Host', inActiveFormat: true };
const currentTop = [
  { value: 'edge-a', label: 'edge-a.example', count: 900 },
  { value: 'edge-b', label: 'edge-b.example', count: 450 },
];
const canonical = {
  aggregates: {
    data: {
      host: { total: 1350, top: currentTop },
    },
  },
};

let browser;
let context;
let page;
let css;

function bundle(top = currentTop) {
  return { aggregates: { data: { host: { total: 1350, top } } } };
}

function markup(data = canonical, selectedCard = card, options = {}) {
  return renderDashboardCardMarkup(data, { card: selectedCard, ...options });
}

async function authenticatedFontFixture(page, context, fontStatus = 200, initialData = canonical) {
  const font = fs.readFileSync(path.join(
    path.dirname(frontendRequire.resolve('next/package.json')),
    'dist/next-devtools/server/font/geist-latin.woff2',
  ));
  const html = fixtureDocument(await markup(initialData)).replace('</head>',
    '<link rel="stylesheet" href="/_next/static/css/font.css"></head>');
  const requests = [];
  const server = http.createServer((request, response) => {
    requests.push({ path: request.url, cookie: request.headers.cookie });
    if (request.headers.cookie !== 'font-fixture=allowed') {
      response.writeHead(403).end();
    } else if (request.url === '/_next/static/media/fixture.woff2') {
      if (fontStatus === 302) {
        response.writeHead(302, { location: 'http://127.0.0.1:1/forbidden' }).end();
      } else {
        response.writeHead(fontStatus, { 'content-type': 'font/woff2' }).end(font);
      }
    } else {
      response.writeHead(404).end();
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  await context.addCookies([{ name: 'font-fixture', value: 'allowed', url: origin }]);
  await page.route(`${origin}/dashboard`, route => route.fulfill({ contentType: 'text/html', body: html }));
  await page.route(`${origin}/_next/static/css/font.css`, route => route.fulfill({
    contentType: 'text/css',
    body: '@font-face{font-family:FixtureFont;src:url("../media/fixture.woff2") format("woff2")}body{font-family:FixtureFont}',
  }));
  await page.route(`${origin}/_next/static/media/fixture.woff2`, route =>
    route.fulfill({ contentType: 'font/woff2', body: font }));
  await page.goto(`${origin}/dashboard`);
  await page.evaluate(() => document.fonts.ready);
  return {
    requests,
    close: () => new Promise((resolve, reject) =>
      server.close(error => error ? reject(error) : resolve())),
  };
}

function fixtureDocument(cardMarkup, { mainHeight = 340, mainOverflow = 'auto', extra = '' } = {}) {
  return `<!doctype html>
    <html lang="en-US"><head><meta charset="utf-8"><style>${css}</style></head>
    <body class="antialiased">
      <button id="change" style="position:fixed;left:650px;top:20px">Update</button>
      <main style="position:fixed;left:40px;top:80px;width:420px;height:${mainHeight}px;overflow:${mainOverflow}">
        <div id="card-host" style="width:360px;height:300px">${cardMarkup}</div>${extra}
      </main>
    </body></html>`;
}

async function observe(action, options = {}, expected = canonical) {
  return measureCompositedResults(page, {
    target: '#change',
    timeoutMs: 5000,
    reference: target => dashboardCardReference(target, expected, {
      cards: [card],
      ...options,
    }),
  }, action);
}

describe('Dashboard card compositor reference', () => {
  before(async () => {
    const source = fs.readFileSync(path.join(frontend, 'app/globals.css'), 'utf8');
    const postcss = frontendRequire('postcss');
    const tailwind = frontendRequire('@tailwindcss/postcss');
    css = (await postcss([tailwind()]).process(source, {
      from: path.join(frontend, 'app/globals.css'),
    })).css;
    browser = await chromium.launch({ headless: true });
  });

  after(async () => {
    await browser?.close();
  });

  beforeEach(async () => {
    context = await browser.newContext({
      viewport: { width: 900, height: 700 },
      deviceScaleFactor: 1,
      locale: 'en-US',
    });
    page = await context.newPage();
  });

  afterEach(async () => {
    await context?.close();
  });

  it('observes a genuine update to canonical production-rendered rows', async () => {
    const stale = bundle([
      { value: 'old-a', label: 'old-a.example', count: 300 },
      { value: 'old-b', label: 'old-b.example', count: 100 },
    ]);
    const currentMarkup = await markup();
    await page.setContent(fixtureDocument(await markup(stale)));
    const result = await observe(async () => {
      await page.click('#change');
      await page.locator('#card-host').evaluate((host, html) => { host.innerHTML = html; }, currentMarkup);
    });
    assert.equal(result.results.status, 'observed');
    assert.equal(result.results.panels_required, 1);
    assert.equal(result.results.panels_observed, 1);
  });

  for (const { width, spacer, font } of [
    { width: 1132, spacer: 0, font: false },
    { width: 1098, spacer: 775, font: true },
  ]) {
    it(`matches five fractional grid cards at width ${width} with spacer ${spacer}`, async () => {
      await page.setViewportSize({ width: 1440, height: 1000 });
      const cards = ['ip', 'asn', 'host', 'url', 'ua'].map(id =>
        ({ id, label: id.toUpperCase(), inActiveFormat: true }));
      const data = { aggregates: { data: Object.fromEntries(cards.map(item =>
        [item.id, { total: 1350, top: currentTop }])) } };
      const stale = { aggregates: { data: Object.fromEntries(cards.map(item =>
        [item.id, { total: 400, top: [
          { value: 'old-a', label: 'old-a.example', count: 300 },
          { value: 'old-b', label: 'old-b.example', count: 100 },
        ] }])) } };
      const render = async value => {
        const rows = [];
        for (const item of cards) rows.push(`<div style="height:300px">${await markup(value, item)}</div>`);
        return rows.join('');
      };
      const currentMarkup = await render(data);
      const fixture = font ? await authenticatedFontFixture(page, context) : null;
      const htmlDocument = `<!doctype html><html lang="en-US" class="dark"><head>
        <style>${css}</style></head><body class="antialiased">
        <button id="change" style="position:fixed;left:650px;top:20px">Update</button>
        <main style="position:fixed;left:280px;top:80px;width:${width}px;height:920px;overflow:auto">
        <div style="height:${spacer}px"></div>
        <section class="rounded-lg border" style="background:rgb(59 130 246 / 0.03)">
        <div style="height:36px">Network</div>
        <div id="grid" class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4 px-4 pb-4 pt-1">
        ${await render(stale)}</div></section></main></body></html>`;
      try {
        if (fixture) {
          await page.evaluate(html => {
            const parsed = new DOMParser().parseFromString(html, 'text/html');
            document.documentElement.className = 'dark';
            document.body.innerHTML = parsed.body.innerHTML;
          }, htmlDocument);
        } else await page.setContent(htmlDocument);
        const result = await measureCompositedResults(page, {
          target: '#change', timeoutMs: 10000,
          reference: target => dashboardCardReference(target, data, { cards }),
        }, async () => {
          await page.click('#change');
          await page.locator('#grid').evaluate((grid, html) => { grid.innerHTML = html; }, currentMarkup);
        });
        const ref = await dashboardCardReference(page, data, { cards });
        const actualPng = (await page.screenshot()).toString('base64');
        const differences = await page.evaluate(async ({ ref, actualPng }) => {
          const read = async encoded => {
            const bitmap = await createImageBitmap(new Blob([
              Uint8Array.from(atob(encoded), character => character.charCodeAt(0)),
            ], { type: 'image/png' }));
            const canvas = new OffscreenCanvas(bitmap.width, bitmap.height);
            const context = canvas.getContext('2d');
            context.drawImage(bitmap, 0, 0);
            bitmap.close();
            return context;
          };
          const actual = await read(actualPng);
          const results = [];
          for (const region of ref.regions) {
            const expected = (await read(region.pngBase64)).getImageData(0, 0, region.width, region.height).data;
            const observed = actual.getImageData(region.x, region.y, region.width, region.height).data;
            let mismatch = 0;
            let maxDelta = 0;
            for (let index = 0; index < expected.length; index += 4) {
              if (expected.slice(index, index + 4).some((value, channel) => value !== observed[index + channel])) {
                mismatch++;
                for (let channel = 0; channel < 3; channel++) {
                  maxDelta = Math.max(maxDelta, Math.abs(expected[index + channel] - observed[index + channel]));
                }
              }
            }
            results.push({ x: region.x, width: region.width, mismatch, maxDelta });
          }
          return results;
        }, { ref, actualPng });
        assert.equal(result.results.status, 'observed', JSON.stringify(differences));
        assert.equal(result.results.panels_required, 5);
        assert.equal(result.results.panels_observed, 5);
        assert.ok(differences.every(region => region.mismatch === 0), JSON.stringify(differences));
        if (spacer) assert.ok(ref.regions.every(region => region.y === 953 && region.height === 34));
      } finally {
        await fixture?.close();
      }
    });
  }

  for (const [name, text] of [
    ['1/7 digit', '7,189'],
    ['8/9 digit', '1,199'],
    ['missing comma', '1189'],
    ['truncated count', '1,18'],
    ['missing count', ''],
  ]) {
    it(`rejects ${name} corruption at a fractional card origin`, async () => {
      const expected = bundle([{ value: 'edge-a', label: 'edge-a.example', count: 1189 }]);
      await page.setContent(fixtureDocument(await markup(expected)));
      await page.locator('main').evaluate(main => { main.style.left = '40.796875px'; });
      const count = page.locator('#card-host span.text-xs.font-mono').first();
      assert.equal(await count.textContent(), '1,189');
      await count.evaluate((element, value) => { element.textContent = value; }, text);
      const result = await observe(() => page.click('#change'), {}, expected);
      assert.equal(result.results.status, 'unobserved');
      assert.equal(result.results.panels_observed, 0);
    });
  }

  it('rejects stale visible labels', async () => {
    await page.setContent(fixtureDocument(await markup()));
    await page.locator('#card-host span.flex-1.truncate').first().evaluate(element => {
      element.textContent = 'stale-label.example';
    });
    const result = await observe(() => page.click('#change'));
    assert.equal(result.results.status, 'unobserved');
  });

  it('rejects a stale full-label title even when its visible prefix is unchanged', async () => {
    await page.setContent(fixtureDocument(await markup()));
    await page.locator('#card-host tbody tr button').first().evaluate(element => {
      element.title = 'stale-label.example';
    });
    await assert.rejects(
      dashboardCardReference(page, canonical, { cards: [card] }),
      /dashboard_card_reference_label_semantics_mismatch:host:0/,
    );
  });

  it('rejects stale visible counts', async () => {
    await page.setContent(fixtureDocument(await markup()));
    await page.locator('#card-host span.text-xs.font-mono').first().evaluate(element => {
      element.textContent = '777';
    });
    const result = await observe(() => page.click('#change'));
    assert.equal(result.results.status, 'unobserved');
  });

  it('rejects stale bar widths even when labels and counts remain current', async () => {
    await page.setContent(fixtureDocument(await markup()));
    await page.locator('#card-host button > div.absolute.inset-y-0.left-0').first()
      .evaluate(element => { element.style.width = '25%'; });
    const result = await observe(() => page.click('#change'));
    assert.equal(result.results.status, 'unobserved');
  });

  it('rejects a canonical row whose request count is missing', async () => {
    const missingCount = bundle([
      { value: 'edge-a', label: 'edge-a.example' },
    ]);
    await assert.rejects(
      renderDashboardCardMarkup(missingCount, { card }),
      /dashboard_card_reference_missing_count:host:0/,
    );
  });

  it('uses the production bot-card mapping and comparison delta renderer', async () => {
    const botMarkup = await renderDashboardCardMarkup({
      aggregates: { data: {} },
      top_bots: {
        bots: [{ id: 'bot-1', name: 'Crawler', request_count: 1200 }],
        ngwaf_bots: [],
      },
    }, { card: { id: '_bot_name', label: 'Bots', inActiveFormat: true } });
    assert.match(botMarkup, /Filter to Crawler/);
    assert.match(botMarkup, /1,200/);

    const comparedMarkup = await renderDashboardCardMarkup(canonical, {
      card,
      compareMode: true,
      compareAggregates: {
        data: { host: { top: [{ value: 'edge-a', count: 450 }, { value: 'edge-b', count: 450 }] } },
      },
    });
    assert.match(comparedMarkup, /\+100%/);
  });

  it('uses the real bootstrap cache to render masked IP rows as non-interactive', async () => {
    const ipCard = { id: 'ip', label: 'Client IP', inActiveFormat: true };
    const ipCanonical = {
      aggregates: {
        data: {
          ip: { total: 5, top: [{ value: '192.0.2.xxx', label: '192.0.2.xxx', count: 5 }] },
        },
      },
    };
    const maskedIpMarkup = await markup(ipCanonical, ipCard, { maskIps: true });
    assert.match(maskedIpMarkup, /192\.0\.2\.xxx/);
    assert.doesNotMatch(maskedIpMarkup, /aria-label="Filter to/);

    const stale = {
      aggregates: {
        data: {
          ip: { total: 1, top: [{ value: '192.0.2.yyy', label: '192.0.2.yyy', count: 1 }] },
        },
      },
    };
    await page.setContent(fixtureDocument(await markup(stale, ipCard, { maskIps: true })));
    const result = await observe(async () => {
      await page.click('#change');
      await page.locator('#card-host').evaluate((host, html) => { host.innerHTML = html; }, maskedIpMarkup);
    }, { cards: [ipCard], maskIps: true }, ipCanonical);
    assert.equal(result.results.status, 'observed');
  });

  it('rejects an opaque overlay covering the card data', async () => {
    await page.setContent(fixtureDocument(await markup(), {
      extra: '<div style="position:fixed;left:40px;top:80px;width:360px;height:300px;background:white;z-index:99"></div>',
    }));
    const result = await observe(() => page.click('#change'));
    assert.equal(result.results.status, 'unobserved');
  });

  it('matches fully visible rows inside an ancestor clipping boundary', async () => {
    const stale = bundle([
      { value: 'old-a', label: 'old-a.example', count: 300 },
      { value: 'old-b', label: 'old-b.example', count: 100 },
    ]);
    const currentMarkup = await markup();
    await page.setContent(fixtureDocument(await markup(stale), { mainHeight: 180, mainOverflow: 'hidden' }));
    const result = await observe(async () => {
      await page.click('#change');
      await page.locator('#card-host').evaluate((host, html) => { host.innerHTML = html; }, currentMarkup);
    });
    assert.equal(result.results.status, 'observed');
  });

  it('ignores lazy/offscreen cards and hidden a11y-only tables', async () => {
    const hidden = `<div style="margin-top:900px;width:360px;height:300px">${await markup()}</div>
      <div class="sr-only"><h3>Host (Top 10)</h3><table><tbody><tr><td>hidden</td></tr></tbody></table></div>`;
    await page.setContent(fixtureDocument('', { extra: hidden }));
    await assert.rejects(
      dashboardCardReference(page, canonical, { cards: [card] }),
      /dashboard_card_reference_no_visible_rows/,
    );
  });

  it('fails closed when comparison mode lacks canonical comparison aggregates', async () => {
    await page.setContent(fixtureDocument(await markup()));
    await assert.rejects(
      dashboardCardReference(page, canonical, { cards: [card], compareMode: true }),
      /dashboard_card_reference_comparison_unsupported/,
    );
  });

  it('fetches stylesheet-relative static fonts through the authenticated context and renders them offline', async () => {
    const stale = bundle([
      { value: 'old-a', label: 'old-a.example', count: 300 },
      { value: 'old-b', label: 'old-b.example', count: 100 },
    ]);
    const currentMarkup = await markup();
    const fixture = await authenticatedFontFixture(page, context, 200, stale);
    try {
      const result = await observe(async () => {
        await page.click('#change');
        await page.locator('#card-host').evaluate((host, html) => { host.innerHTML = html; }, currentMarkup);
      });
      assert.equal(result.results.status, 'observed');
      assert.equal(result.results.panels_required, 1);
      assert.equal(result.results.panels_observed, 1);
      assert.deepEqual(fixture.requests, [{
        path: '/_next/static/media/fixture.woff2',
        cookie: 'font-fixture=allowed',
      }]);
    } finally {
      await fixture.close();
    }
  });

  for (const status of [302, 403]) {
    it(`rejects static font HTTP ${status} without following redirects or using an unauthenticated fallback`, async () => {
      const fixture = await authenticatedFontFixture(page, context, status);
      try {
        await assert.rejects(
          dashboardCardReference(page, canonical, { cards: [card] }),
          new RegExp(`dashboard_card_reference_font_http_${status}`),
        );
        assert.equal(fixture.requests.length, 1);
      } finally {
        await fixture.close();
      }
    });
  }

  it('rejects a font transport failure explicitly', async () => {
    const fixture = await authenticatedFontFixture(page, context);
    await fixture.close();
    await assert.rejects(
      dashboardCardReference(page, canonical, { cards: [card] }),
      /dashboard_card_reference_font_transport_failed/,
    );
    assert.equal(fixture.requests.length, 0);
  });

  for (const source of [
    'https://external.invalid/_next/static/media/fixture.woff2',
    '/api/private.woff2',
    '/_next/static/media/fixture.woff2?private=true',
  ]) {
    it(`rejects nonallowlisted font metadata before authenticated transport: ${source}`, async () => {
      const fixture = await authenticatedFontFixture(page, context);
      try {
        await page.addStyleTag({ content: `@font-face{font-family:UnusedFont;src:url("${source}")}` });
        await assert.rejects(
          dashboardCardReference(page, canonical, { cards: [card] }),
          /dashboard_card_reference_font_origin_or_path_unsupported/,
        );
        assert.equal(fixture.requests.length, 0);
      } finally {
        await fixture.close();
      }
    });
  }
});
