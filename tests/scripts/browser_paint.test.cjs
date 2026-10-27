const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { chromium } = require('../../frontend/node_modules/playwright');
const { measurePaintedAction } = require('../../scripts/lib/section_measurements.cjs');

async function fixture(t, body) {
  const browser = await chromium.launch({ headless: true });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.route('**/*', route => route.fulfill({ contentType: 'text/html', body }));
  await page.goto('https://example.com/');
  return page;
}

const options = {
  target: '#apply',
  feedback: { selector: '#pending', text: 'Applying filter' },
  results: [
    { selector: '#count', text: 'Matched requests: 42' },
    { selector: '#series', text: 'Requested series ready' },
  ],
  timeoutMs: 2000,
};

test('the pilot scrolls a lazy-card ancestor before arming painted-action observation', async t => {
  const page = await fixture(t, `<section id="lazy-card" style="margin-top:2000px;height:250px"></section>
    <script>
      const card = document.querySelector('#lazy-card');
      const observer = new IntersectionObserver(entries => {
        if (!entries.some(entry => entry.isIntersecting)) return;
        observer.disconnect();
        card.innerHTML = '<button id="search">Search methods</button><div id="results" style="display:flex;flex-direction:column"></div>';
        document.querySelector('#search').onclick = () => {
          document.querySelector('#results').innerHTML = '<h2 id="feedback">Search methods</h2>';
          setTimeout(() => {
            document.querySelector('#results').insertAdjacentHTML('beforeend', '<span id="method">GET</span>');
          }, 180);
        };
      });
      observer.observe(card);
    </script>`);
  assert.equal(await page.locator('#search').count(), 0);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'analyst-paint-cli-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const script = require.resolve('../../scripts/dev/check_analyst_lifecycle.cjs');
  const load = createRequire(script);
  const sandbox = {
    module: { exports: {} },
    process: { stdout: { write() {} } },
    require(name) {
      if (name === '../lib/analyst_fixture.cjs') {
        return { async withAnalystUsers(_options, work) { await work([{ page }]); } };
      }
      return load(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(script, 'utf8'), sandbox, { filename: script });
  const output = path.join(directory, 'evidence.json');
  await sandbox.module.exports.main([
    '--environment', 'remote-standard', '--admin-origin', 'https://admin.example.com',
    '--analyst-origin', 'https://analyst.example.com', '--service-id', 'service-test',
    '--role', 'masked', '--users', '1', '--output', output,
    '--paint-scroll-selector', '#lazy-card', '--paint-target', '#search',
    '--paint-feedback-selector', '#feedback', '--paint-feedback-text', 'Search methods',
    '--paint-result-selector', '#method', '--paint-result-text', 'GET', '--paint-timeout-ms', '2000',
  ]);
  const report = JSON.parse(fs.readFileSync(output, 'utf8'));
  assert.equal(report.painted_action.feedback.status, 'observed', JSON.stringify(report.painted_action));
  assert.equal(report.painted_action.results.status, 'observed', JSON.stringify(report.painted_action));
  assert.equal(report.painted_action.qualification, 'incomplete');
  assert.equal(await page.locator('[elementtiming]').count(), 0);
});

test('meaningful text paint precedes delayed results, including every requested panel', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML = '<p id="pending">Applying filter</p>';
        setTimeout(() => {
          document.querySelector('#content').insertAdjacentHTML('beforeend', '<p id="count">Matched requests: 42</p>');
        }, 180);
        setTimeout(() => {
          document.querySelector('#content').insertAdjacentHTML('beforeend', '<p id="series">Requested series ready</p>');
        }, 360);
      };
    </script>`);
  const report = await measurePaintedAction(page, options, () => page.locator('#apply').click());
  assert.equal(report.feedback.source, 'element_timing');
  assert.equal(report.feedback.status, 'observed');
  assert.equal(report.results.status, 'observed');
  assert.ok(report.feedback.elapsed_ms >= 0);
  assert.ok(report.results.elapsed_ms >= 300);
  assert.ok(report.feedback.elapsed_ms < report.results.elapsed_ms);
  assert.equal(report.results.panels_observed, 2);
  assert.equal(report.qualification, 'incomplete');
  assert.ok(report.gaps.includes('repeated_samples_unverified'));
  assert.equal(report.whole_request_budget_policy, 'diagnostic_only');
  assert.equal(JSON.stringify(report).includes('Matched requests'), false);
  assert.equal(JSON.stringify(report).includes('example.com'), false);
  assert.equal(await page.locator('[elementtiming]').count(), 0);
});

test('hidden DOM acknowledgments and canvas presence cannot establish paint', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending" style="visibility:hidden">Applying filter</p><canvas id="count"></canvas>';
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 250 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'unobserved');
  assert.equal(report.feedback.elapsed_ms, null);
  assert.equal(report.results.status, 'unobserved');
  assert.ok(report.gaps.includes('feedback_paint_unobserved'));
  assert.ok(report.gaps.includes('visible_results_paint_unobserved'));
});

test('already visible markers are refused rather than counted as new results', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button>
    <p id="pending">Applying filter</p><p id="count">Matched requests: 42</p>
    <p id="series">Requested series ready</p>`);
  let acted = false;
  await assert.rejects(measurePaintedAction(page, options, async () => { acted = true; }), /already visible/);
  assert.equal(acted, false);
});

test('synthetic dispatched events do not establish browser event arrival', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML = '<p id="pending">Applying filter</p>';
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 250 },
    () => page.locator('#apply').dispatchEvent('click'));
  assert.equal(report.event_arrival_ms, null);
  assert.equal(report.feedback.elapsed_ms, null);
  assert.ok(report.gaps.includes('trusted_event_unobserved'));
});

test('failed actions clean up observers and never become success-shaped observations', async t => {
  const page = await fixture(t, '<button id="apply">Apply</button>');
  await assert.rejects(measurePaintedAction(page, options, async () => { throw new Error('Action failed'); }), /Action failed/);
  assert.equal(await page.evaluate(() => Object.keys(window).some(key => key.startsWith('__phase4bPaint'))), false);
  assert.equal(await page.locator('[elementtiming]').count(), 0);
});

test('event arrival includes time queued behind a blocked main thread', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onkeydown = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending">Applying filter</p><p id="count">Matched requests: 42</p><p id="series">Requested series ready</p>';
      };
    </script>`);
  await page.locator('#apply').focus();
  let blocked;
  const blocking = new Promise(resolve => { blocked = resolve; });
  await page.exposeFunction('blocked', blocked);
  const report = await measurePaintedAction(page, { ...options, eventType: 'keydown' }, async () => {
    await page.evaluate(() => {
      setTimeout(() => {
        window.blocked();
        const until = performance.now() + 250;
        while (performance.now() < until) { /* Deliberate input queue delay. */ }
      }, 0);
    });
    await blocking;
    await page.keyboard.press('Enter');
  });
  assert.equal(report.feedback.status, 'observed');
  assert.ok(report.input_delay_ms >= 100, JSON.stringify(report));
  assert.ok(report.feedback.elapsed_ms >= report.input_delay_ms);
  assert.ok(report.results.elapsed_ms >= report.input_delay_ms);
});

test('a DOM state removed before paint is not a meaningful painted acknowledgment', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML = '<p id="pending">Applying filter</p>';
        requestAnimationFrame(() => document.querySelector('#pending').remove());
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 300 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'unobserved');
  assert.equal(report.feedback.elapsed_ms, null);
});

test('panels painted at different times must still be usable together', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending">Applying filter</p><p id="count">Matched requests: 42</p>';
        setTimeout(() => {
          document.querySelector('#count').remove();
          document.querySelector('#content').insertAdjacentHTML('beforeend', '<p id="series">Requested series ready</p>');
        }, 150);
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 400 }, () => page.locator('#apply').click());
  assert.equal(report.results.status, 'unobserved');
  assert.equal(report.results.elapsed_ms, null);
  assert.equal(report.results.panels_observed, 1);
});

for (const pointerEvents of ['auto', 'none']) test(`opaque ${pointerEvents}-pointer overlay does not establish visible paint`, async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending">Applying filter</p><p id="count">Matched requests: 42</p><p id="series">Requested series ready</p>' +
          '<div style="position:fixed;inset:0;background:white;z-index:999;pointer-events:${pointerEvents}"></div>';
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 300 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'unobserved');
  assert.equal(report.results.status, 'unobserved');
});

test('a previously painted panel cannot be covered while later panels finish', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending">Applying filter</p><div id="first" style="position:relative;display:inline-block">' +
          '<p id="count">Matched requests: 42</p></div>';
        setTimeout(() => {
          document.querySelector('#first').insertAdjacentHTML('beforeend',
            '<div style="position:absolute;inset:0;background:white;z-index:999;pointer-events:none"></div>');
        }, 100);
        setTimeout(() => {
          document.querySelector('#content').insertAdjacentHTML('beforeend', '<p id="series">Requested series ready</p>');
        }, 300);
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 650 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'observed');
  assert.equal(report.results.status, 'unobserved');
  assert.equal(report.results.panels_observed, 1);
});

test('obscuring descendants cannot masquerade as text-only markers', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending" style="position:relative">Applying filter' +
          '<span style="position:absolute;inset:0;background:white"></span></p>';
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 300 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'unobserved');
});

test('missing browser visibility support stays an explicit evidence gap', async t => {
  const page = await fixture(t, `<button id="apply">Apply</button><div id="content"></div>
    <script>
      delete IntersectionObserverEntry.prototype.isVisible;
      document.querySelector('#apply').onclick = () => {
        document.querySelector('#content').innerHTML =
          '<p id="pending">Applying filter</p><p id="count">Matched requests: 42</p><p id="series">Requested series ready</p>';
      };
    </script>`);
  const report = await measurePaintedAction(page, { ...options, timeoutMs: 300 }, () => page.locator('#apply').click());
  assert.equal(report.feedback.status, 'unobserved');
  assert.equal(report.results.status, 'unobserved');
  assert.ok(report.gaps.includes('paint_visibility_unavailable'));
});
