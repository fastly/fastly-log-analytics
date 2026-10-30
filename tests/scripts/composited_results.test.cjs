const assert = require('node:assert/strict');
const { before, after, test } = require('node:test');
const { chromium } = require('../../frontend/node_modules/@playwright/test');
const { measureCompositedResults } = require('../../scripts/lib/composited_results.cjs');
const { expectedTrafficTraces, dashboardChartScene, dashboardChartReference } =
  require('../../scripts/lib/dashboard_paint.cjs');

let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

async function fixture(work, { overlay = false, disjoint = false } = {}) {
  const context = await browser.newContext({ viewport: { width: 240, height: 160 }, deviceScaleFactor: 1 });
  try {
    await context.route('**/*', route => route.abort());
    const page = await context.newPage();
    await page.setContent(`
      <style>body{margin:0}canvas{position:absolute;top:40px}
      #first{left:0}#second{left:80px}#cover{position:absolute;top:40px;left:0;width:64px;height:64px;background:white}</style>
      <button id="action">Change</button>
      <canvas id="first" width="64" height="64"></canvas>
      <canvas id="second" width="64" height="64"></canvas>
      ${overlay ? '<div id="cover"></div>' : ''}
      <script>
        const fill = (id, color) => {
          const ctx = document.getElementById(id).getContext('2d');
          ctx.fillStyle = color; ctx.fillRect(0, 0, 64, 64);
        };
        fill('first', 'red'); fill('second', 'blue');
        document.getElementById('action').onclick = () => {
          window.arrival = performance.now();
          setTimeout(() => {
            fill('first', 'green');
            setTimeout(() => {
              ${disjoint ? "fill('first', 'red');" : ''}
              fill('second', 'yellow'); window.done = performance.now();
            }, 120);
          }, 120);
        };
      </script>
    `);
    const reference = await page.evaluate(async () => {
      const regions = [];
      for (const [x, color] of [[0, 'green'], [80, 'yellow']]) {
        const canvas = new OffscreenCanvas(64, 64);
        const ctx = canvas.getContext('2d');
        ctx.fillStyle = color;
        ctx.fillRect(0, 0, 64, 64);
        const bytes = new Uint8Array(await (await canvas.convertToBlob({ type: 'image/png' })).arrayBuffer());
        regions.push({ x, y: 40, width: 64, height: 64,
          pngBase64: btoa(String.fromCharCode(...bytes)) });
      }
      return { source: 'independent_render', regions };
    });
    await work(page, reference);
  } finally {
    await context.close();
  }
}

function options(reference, extra = {}) {
  return { target: '#action', reference: async () => reference, timeoutMs: 3000, ...extra };
}

async function change(page) {
  await page.locator('#action').click();
  await page.waitForFunction(() => window.done > 0);
}

function interceptFrames(context, transform) {
  const original = context.newCDPSession.bind(context);
  context.newCDPSession = async target => {
    const session = await original(target);
    const on = session.on.bind(session);
    const off = session.off.bind(session);
    const listeners = new Map();
    session.on = (name, listener) => {
      if (name !== 'Page.screencastFrame') return on(name, listener);
      const wrapped = event => transform(event).forEach(listener);
      listeners.set(listener, wrapped);
      return on(name, wrapped);
    };
    session.off = (name, listener) => off(name, listeners.get(listener) ?? listener);
    return session;
  };
}

test('compositor timing requires every requested panel in the same actual frame', async () => {
  await fixture(async (page, reference) => {
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    const changeAt = await page.evaluate(() => window.done - window.arrival);
    assert.equal(result.qualification, 'incomplete');
    assert.equal(result.results.status, 'observed');
    assert.equal(result.results.source, 'compositor_frame');
    assert.equal(result.results.panels_required, 2);
    assert.equal(result.results.panels_observed, 2);
    assert.ok(result.results.elapsed_ms >= changeAt - 2, JSON.stringify(result));
    assert.ok(result.results.elapsed_ms < 1500, JSON.stringify(result));
    assert.equal(result.results.precision, 'first_matching_captured_frame_upper_bound');
    assert.equal(result.clock.status, 'verified');
    assert.deepEqual(result.clock.rejection_reasons, []);
    assert.ok(result.frames_observed >= 2);
    assert.equal(JSON.stringify(result).includes('pngBase64'), false);
    assert.equal(JSON.stringify(result).includes('#action'), false);
    assert.deepEqual(await page.evaluate(() =>
      Object.keys(window).filter(key => key.startsWith('__flaCompositor'))), []);
  });
});

test('a correct backing-store bitmap hidden behind an overlay is not painted evidence', async () => {
  await fixture(async (page, reference) => {
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    assert.equal(result.results.status, 'unobserved');
    assert.equal(result.results.elapsed_ms, null);
    assert.equal(result.results.panels_observed, 1);
    assert.ok(result.gaps.includes('visible_results_paint_unobserved'));
  }, { overlay: true });
});

test('panels painted in different frames never combine into a completed result', async () => {
  await fixture(async (page, reference) => {
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    assert.equal(result.results.status, 'unobserved');
    assert.equal(result.results.panels_observed, 1);
  }, { disjoint: true });
});

test('a synthetic input cannot establish event-arrival paint latency', async () => {
  await fixture(async (page, reference) => {
    const result = await measureCompositedResults(page, options(reference), async () => {
      await page.locator('#action').dispatchEvent('click');
      await page.waitForFunction(() => window.done > 0);
    });
    assert.equal(result.event_arrival_ms, null);
    assert.equal(result.results.status, 'unobserved');
    assert.ok(result.gaps.includes('trusted_event_unobserved'));
  });
});

test('a visual reference already visible before the input is not a new paint', async () => {
  await fixture(async (page, reference) => {
    await page.evaluate(() => {
      document.getElementById('first').getContext('2d').fillStyle = 'green';
      document.getElementById('first').getContext('2d').fillRect(0, 0, 64, 64);
      document.getElementById('second').getContext('2d').fillStyle = 'yellow';
      document.getElementById('second').getContext('2d').fillRect(0, 0, 64, 64);
    });
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    assert.equal(result.results.status, 'unobserved');
    assert.ok(result.gaps.includes('reference_visible_before_event'));
  });
});

test('missing monotonic frame clocks never fall back to epoch timestamps', async () => {
  await fixture(async (page, reference) => {
    interceptFrames(page.context(), event => [{
      ...event, metadata: { ...event.metadata, monotonicTimestamp: null },
    }]);
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    assert.equal(result.clock.status, 'unobserved');
    assert.equal(result.results.status, 'unobserved');
    assert.ok(result.gaps.includes('compositor_clock_unverified'));
    assert.ok(result.clock.rejection_reasons.includes('capture_timestamp_invalid_or_reordered'));
    assert.ok(result.clock.rejection_reasons.includes('retained_frame_clock_invalid'));
  });
});

for (const stage of ['before', 'after']) {
  test(`clock diagnostics identify ${stage}-action calibration failure without accepting paint`, async () => {
    await fixture(async (page, reference) => {
      const context = page.context();
      const original = context.newCDPSession.bind(context);
      context.newCDPSession = async target => {
        const session = await original(target);
        const send = session.send.bind(session);
        let samples = 0;
        session.send = async (method, params) => {
          const response = await send(method, params);
          if (method === 'Performance.getMetrics' && ++samples === (stage === 'before' ? 1 : 2)) {
            return { ...response, metrics: response.metrics.map(metric =>
              metric.name === 'Timestamp' ? { ...metric, value: metric.value + 60 } : metric) };
          }
          return response;
        };
        return session;
      };
      const result = await measureCompositedResults(page, options(reference), () => change(page));
      assert.equal(result.clock.status, 'unobserved');
      assert.equal(result.results.elapsed_ms, null);
      assert.ok(result.clock.rejection_reasons.includes(`${stage}_calibration_failed`));
      assert.equal(result.clock.calibration[stage].verified, false);
      assert.equal(result.clock.calibration[stage === 'before' ? 'after' : 'before'].verified, true);
      assert.equal(JSON.stringify(result.clock).includes('pngBase64'), false);
    });
  });
}

test('capture pressure retains recent real frames and reports only a conservative completion upper bound', async () => {
  await fixture(async (page, reference) => {
    let clicked = false;
    interceptFrames(page.context(), frame => Array(clicked ? 150 : 1).fill(frame));
    const result = await measureCompositedResults(page, options(reference), async () => {
      await page.locator('#action').click();
      await page.waitForFunction(() => window.done > 0);
      clicked = true;
      await page.evaluate(() => { document.body.style.backgroundColor = 'rgb(1,2,3)'; });
    });
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
    assert.equal(result.results.precision, 'first_matching_retained_frame_upper_bound');
    assert.ok(result.frames_dropped > 0);
    assert.ok(result.frames_observed <= 120);
    assert.ok(result.gaps.includes('compositor_capture_window_reduced'));
    assert.equal(result.gaps.includes('pre_input_frame_coverage_incomplete'), false);
  });
});

test('evicting pre-input frames cannot establish that the expected output was newly visible', async () => {
  await fixture(async (page, reference) => {
    interceptFrames(page.context(), frame => Array(150).fill(frame));
    const result = await measureCompositedResults(page, options(reference), () => change(page));
    assert.equal(result.results.status, 'unobserved');
    assert.equal(result.results.elapsed_ms, null);
    assert.ok(result.gaps.includes('pre_input_frame_coverage_incomplete'));
  });
});

test('document changes invalidate the frame-to-event clock mapping', async () => {
  await fixture(async (page, reference) => {
    const result = await measureCompositedResults(page, options(reference), async () => {
      await page.locator('#action').click();
      await page.goto('data:text/html,<p>Different document</p>');
    });
    assert.equal(result.clock.status, 'unobserved');
    assert.equal(result.results.status, 'unobserved');
    assert.ok(result.gaps.includes('observation_document_changed'));
  });
});

test('an action failure is preserved and its capture listeners are removed', async () => {
  await fixture(async (page, reference) => {
    const failure = new Error('deliberate-action-failure');
    await assert.rejects(measureCompositedResults(page, options(reference), async () => {
      throw failure;
    }), error => error === failure);
    assert.deepEqual(await page.evaluate(() =>
      Object.keys(window).filter(key => key.startsWith('__flaCompositor'))), []);
  });
});

test('a session acquired after the observation deadline is still explicitly detached', async () => {
  await fixture(async (page, reference) => {
    const context = page.context();
    const original = context.newCDPSession.bind(context);
    let detached = 0;
    context.newCDPSession = async target => {
      const session = await original(target);
      const detach = session.detach.bind(session);
      session.detach = async () => { detached++; await detach(); };
      await new Promise(resolve => setTimeout(resolve, 100));
      return session;
    };
    await assert.rejects(measureCompositedResults(page, options(reference, { timeoutMs: 20 }),
      () => change(page)), /compositor_observation_timeout/);
    assert.equal(detached, 1);
  });
});

test('screenshot/baseline references and invalid bounds are refused', async () => {
  await fixture(async (page, reference) => {
    await assert.rejects(measureCompositedResults(page,
      options({ ...reference, source: 'screenshot' }), () => change(page)), /independent reference/);
    const regions = [{ ...reference.regions[0], x: -1 }];
    await assert.rejects(measureCompositedResults(page,
      options({ ...reference, regions }), () => change(page)), /reference bounds/);
  });
});

async function dashboardFixture(work, {
  values = [10, 20, 5], interactive = false, background = '#ffffff',
} = {}) {
  const context = await browser.newContext({ viewport: { width: 640, height: 480 }, deviceScaleFactor: 1 });
  try {
    const page = await context.newPage();
    await context.route('**/*', route => route.abort());
    await page.setContent(`<style>body{margin:0;background:${background}}</style><button id="action">Filter</button>`
      + '<main><div id="chart" style="width:600px;height:400px"></div></main>');
    await page.addScriptTag({ path: require.resolve('../../frontend/node_modules/plotly.js-cartesian-dist-min') });
    const request = { start_time: '2020-01-01T00:00:00Z', end_time: '2020-01-01T03:00:00Z',
      chart_metric: 'requests', chart_interval: '1h' };
    const aggregate = values => ({ metric: 'requests', interval: '1h', time_series: values.map((value, hour) =>
      ({ time: `2020-01-01T0${hour}:00:00Z`, value })) });
    const initial = expectedTrafficTraces(aggregate([30, 30, 30]), request);
    const expected = expectedTrafficTraces(aggregate(values), request);
    await page.evaluate(async ({ initial, expected, interactive }) => {
      await window.Plotly.newPlot('chart', initial, {
        width: 600, height: 400, margin: { l: 50, r: 20, t: 20, b: 40 },
        paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
        xaxis: { type: 'date', range: ['2020-01-01 00:00:00', '2020-01-01 03:00:00'] },
        yaxis: { range: [0, Math.max(40, ...expected[0].y.map(value => value * 1.1))] },
      }, { staticPlot: !interactive, displayModeBar: false });
      window.expected = expected;
      document.getElementById('action').onclick = async () => {
        if (window.stale) document.getElementById('chart').data = expected;
        else await window.Plotly.react('chart', expected, document.getElementById('chart').layout);
        window.done = true;
      };
    }, { initial, expected, interactive });
    await work(page, expected);
  } finally { await context.close(); }
}

test('Dashboard paint compares independently rebuilt canonical chart pixels, not a screen baseline', async () => {
  await dashboardFixture(async (page, expected) => {
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
    assert.equal(result.results.pixel_scope, 'opaque_reference_pixels');
    assert.equal(result.qualification, 'incomplete');
  });
});

test('Dashboard rejects incorrect bucket vectors even when the displayed total is unchanged', async () => {
  await dashboardFixture(async (page, expected) => {
    await change(page);
    await page.evaluate(() => {
      const values = document.getElementById('chart').data[0].y;
      [values[0], values[1]] = [values[1], values[0]];
    });
    assert.equal(await dashboardChartScene(page, expected), null);
    await assert.rejects(dashboardChartReference(page, expected), /exact_chart_vectors/);
  });
});

test('interactive Dashboard paint includes bars shorter than one mathematical pixel', async () => {
  await dashboardFixture(async (page, expected) => {
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  }, { values: [12, 8000, 4000], interactive: true });
});

test('a missing tiny Dashboard bar cannot pass with current vectors', async () => {
  await dashboardFixture(async (page, expected) => {
    await page.evaluate(() => {
      document.getElementById('action').onclick = async () => {
        await window.Plotly.react('chart', window.expected, document.getElementById('chart').layout);
        document.querySelector('#chart .barlayer .point path').style.visibility = 'hidden';
        window.done = true;
      };
    });
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
  }, { values: [12, 8000, 4000], interactive: true });
});

test('tiny Dashboard bars remain observable on fractional CSS backgrounds', async () => {
  await dashboardFixture(async (page, expected) => {
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'observed', JSON.stringify(result));
  }, { values: [12, 8000, 4000], interactive: true, background: 'lab(98.8197 -0.809044 -3.66713)' });
});

test('a stale extra pixel above a tiny Dashboard bar cannot pass with current vectors', async () => {
  await dashboardFixture(async (page, expected) => {
    await page.evaluate(() => {
      document.getElementById('action').onclick = async () => {
        await window.Plotly.react('chart', window.expected, document.getElementById('chart').layout);
        const path = document.querySelector('#chart .barlayer .point path');
        path.setAttribute('d', path.getAttribute('d').replace(/V(-?[\d.]+)/,
          (_, top) => `V${Number(top) - 1}`));
        window.done = true;
      };
    });
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'unobserved', JSON.stringify(result));
  }, { values: [12, 8000, 4000], interactive: true });
});

test('new Dashboard data with stale taller SVG bars cannot pass by covering the expected bar interiors', async () => {
  await dashboardFixture(async (page, expected) => {
    await page.evaluate(() => { window.stale = true; });
    const result = await measureCompositedResults(page, {
      target: '#action', timeoutMs: 10000, reference: () => dashboardChartReference(page, expected),
    }, () => change(page));
    assert.equal(result.results.status, 'unobserved');
    assert.ok(result.gaps.includes('visible_results_paint_unobserved'));
  });
});

test('binary masks cannot suppress every reference pixel', async () => {
  await fixture(async (page, reference) => {
    const pngBase64 = await page.evaluate(async () => {
      const canvas = new OffscreenCanvas(64, 64);
      canvas.getContext('2d');
      const blob = await canvas.convertToBlob({ type: 'image/png' });
      const bytes = new Uint8Array(await blob.arrayBuffer());
      return btoa(String.fromCharCode(...bytes));
    });
    await assert.rejects(measureCompositedResults(page, options({
      ...reference, mask: 'opaque_pixels', regions: [{ ...reference.regions[0], pngBase64 }],
    }), () => change(page)), /no required pixels/);
  });
});
