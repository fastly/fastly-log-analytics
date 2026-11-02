const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');

const frontend = path.resolve(__dirname, '../../frontend');
const frontendRequire = createRequire(path.join(frontend, 'package.json'));
let trafficBuilder;

function expectedTrafficTraces(aggregates, request) {
  if (!trafficBuilder) {
    const ts = frontendRequire('typescript');
    const modules = new Map();
    const load = filename => {
      if (modules.has(filename)) return modules.get(filename).exports;
      const module = { exports: {} };
      modules.set(filename, module);
      const source = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
        fileName: filename,
      }).outputText;
      vm.runInNewContext(source, {
        module, exports: module.exports,
        require: name => name.startsWith('./')
          ? load(path.resolve(path.dirname(filename), `${name}.ts`)) : frontendRequire(name),
      }, { filename });
      return module.exports;
    };
    trafficBuilder = load(path.join(frontend, 'lib/chartUtils.ts')).buildTrafficData;
  }
  if (!request.start_time || !request.end_time || request.chart_metric !== 'requests'
    || aggregates.metric !== 'requests') throw new Error('dashboard_paint_requires_absolute_request_counts');
  return JSON.parse(JSON.stringify(trafficBuilder({
    aggregates, compareAggregates: null, compareMode: false, compareStartTime: null,
    startTime: request.start_time, endTime: request.end_time, timezone: 'UTC',
    metric: 'requests', effectiveInterval: request.chart_interval,
    hiddenCategories: new Set(), trend: 'off', catalog: null,
  })));
}

async function dashboardChartScene(page, expected) {
  return page.evaluate(expected => {
    if (expected.length !== 1 || expected[0].type !== 'bar'
      || !Number.isFinite(expected[0].width) || expected[0].width <= 0) return null;
    const equal = (actual, wanted) => Array.isArray(actual) && actual.length === wanted.length
      && actual.every((value, index) => value === wanted[index]);
    const charts = [...document.querySelectorAll('main .js-plotly-plot')].filter(element => {
      const traces = element.data;
      return Array.isArray(traces) && traces.length === expected.length
        && traces.every((trace, index) => trace.type === expected[index].type
          && equal(trace.x, expected[index].x) && equal(trace.y, expected[index].y)
          && trace.visible !== false && trace.visible !== 'legendonly');
    });
    if (charts.length !== 1) return null;
    const chart = charts[0];
    const box = chart.querySelector('.main-svg')?.getBoundingClientRect();
    const layout = chart._fullLayout;
    if (!box || !layout || layout.xaxis?.type !== 'date' || layout.yaxis?.type !== 'linear'
      || layout.width !== box.width || layout.height !== box.height
      || box.x < 0 || box.y < 0 || box.right > innerWidth || box.bottom > innerHeight) return null;
    const backgrounds = [];
    const probe = new OffscreenCanvas(1, 1).getContext('2d');
    let opaque = false;
    for (let ancestor = chart; ancestor; ancestor = ancestor.parentElement) {
      const style = getComputedStyle(ancestor);
      if (style.backgroundImage !== 'none') return null;
      backgrounds.unshift(style.backgroundColor);
      probe.clearRect(0, 0, 1, 1);
      probe.fillStyle = style.backgroundColor;
      probe.fillRect(0, 0, 1, 1);
      if (probe.getImageData(0, 0, 1, 1).data[3] === 255) { opaque = true; break; }
    }
    if (!opaque) return null;
    const bars = expected[0].x.flatMap((time, index) => {
      if (expected[0].y[index] <= 0) return [];
      const center = layout.xaxis.d2p(time);
      const halfWidth = Math.abs(layout.xaxis._m * expected[0].width) / 2;
      return [{ left: Math.max(0, center - halfWidth), right: Math.min(layout.xaxis._length, center + halfWidth),
        top: Math.max(0, layout.yaxis.d2p(expected[0].y[index])),
        bottom: Math.min(layout.yaxis._length, layout.yaxis.d2p(0)) }];
    }).filter(bar => bar.right > bar.left && bar.bottom > bar.top);
    if (!bars.length) return null;
    return {
      x: box.x, y: box.y, width: layout.width, height: layout.height,
      viewport: { width: innerWidth, height: innerHeight },
      staticPlot: chart._context.staticPlot,
      backgrounds, bars,
      plot: { x: layout.xaxis._offset, y: layout.yaxis._offset,
        width: layout.xaxis._length, height: layout.yaxis._length },
      layout: {
        ...JSON.parse(JSON.stringify(chart.layout)),
        width: layout.width, height: layout.height,
        margin: { l: layout.xaxis._offset, t: layout.yaxis._offset,
          r: layout.width - layout.xaxis._offset - layout.xaxis._length,
          b: layout.height - layout.yaxis._offset - layout.yaxis._length, pad: 0 },
        xaxis: { ...JSON.parse(JSON.stringify(chart.layout.xaxis)),
          range: [...layout.xaxis.range], autorange: false, automargin: false },
        yaxis: { ...JSON.parse(JSON.stringify(chart.layout.yaxis)),
          range: [...layout.yaxis.range], autorange: false, automargin: false },
      },
    };
  }, expected);
}

async function dashboardChartReference(page, expected) {
  const scene = await dashboardChartScene(page, expected);
  if (!scene) throw new Error('dashboard_exact_chart_vectors_or_geometry_unverified');
  const browser = page.context().browser();
  if (!browser) throw new Error('dashboard_reference_browser_unavailable');
  const context = await browser.newContext({ viewport: scene.viewport, deviceScaleFactor: 1 });
  try {
    await context.route('**/*', route => route.abort());
    const reference = await context.newPage();
    await reference.setContent('<style>body{margin:0}</style><div id="chart"></div>');
    await reference.addScriptTag({ path: frontendRequire.resolve('plotly.js-cartesian-dist-min') });
    await reference.evaluate(async ({ scene, expected }) => {
      const paint = new OffscreenCanvas(1, 1).getContext('2d');
      for (const color of scene.backgrounds) {
        paint.fillStyle = color;
        paint.fillRect(0, 0, 1, 1);
      }
      const [r, g, b, alpha] = paint.getImageData(0, 0, 1, 1).data;
      if (alpha !== 255) throw new Error('dashboard_reference_background_unverified');
      document.body.style.backgroundColor = `rgb(${r},${g},${b})`;
      const chart = document.getElementById('chart');
      Object.assign(chart.style, { position: 'absolute', left: `${scene.x}px`, top: `${scene.y}px`,
        width: `${scene.width}px`, height: `${scene.height}px` });
      // Static exports suppress the interactive renderer's minimum-pixel bars.
      await window.Plotly.newPlot(chart, expected, scene.layout,
        { staticPlot: scene.staticPlot, displayModeBar: false });
    }, { scene, expected });
    const x = Math.floor(scene.x + scene.plot.x);
    const y = Math.floor(scene.y + scene.plot.y);
    const width = Math.ceil(scene.x + scene.plot.x + scene.plot.width) - x;
    const height = Math.ceil(scene.y + scene.plot.y + scene.plot.height) - y;
    const png = await reference.screenshot({ clip: { x, y, width, height } });
    const pngBase64 = await reference.evaluate(async ({ encoded, scene, expected, x, y, width, height }) => {
      const bytes = Uint8Array.from(atob(encoded), character => character.charCodeAt(0));
      const image = await createImageBitmap(new Blob([bytes], { type: 'image/png' }));
      try {
        const canvas = new OffscreenCanvas(width, height);
        const context = canvas.getContext('2d', { willReadFrequently: true });
        for (const color of scene.backgrounds) {
          context.fillStyle = color;
          context.fillRect(0, 0, width, height);
        }
        const backdrop = context.getImageData(0, 0, 1, 1).data;
        context.drawImage(image, 0, 0);
        const pixels = context.getImageData(0, 0, width, height);
        const original = new Uint8ClampedArray(pixels.data);
        const color = expected[0].marker.color.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
        if (!color) throw new Error('dashboard_reference_bar_color_unsupported');
        const rgb = color.slice(1).map(channel => parseInt(channel, 16));
        let required = 0;
        for (let row = 0; row < height; row++) {
          for (let column = 0; column < width; column++) {
            const offset = (row * width + column) * 4;
            let flat = row > 0 && column > 0 && row < height - 1 && column < width - 1;
            for (let dy = -1; flat && dy <= 1; dy++) {
              for (let dx = -1; flat && dx <= 1; dx++) {
                const neighbor = ((row + dy) * width + column + dx) * 4;
                flat = [0, 1, 2, 3].every(channel => original[offset + channel] === original[neighbor + channel]);
              }
            }
            // Exact fill pixels include one-pixel bars; exact background rejects extra stale rows.
            const fill = rgb.every((channel, index) => channel === original[offset + index]);
            const background = [0, 1, 2, 3].every(channel => backdrop[channel] === original[offset + channel]);
            const keep = original[offset + 3] === 255 && (flat || fill || background);
            pixels.data[offset + 3] = keep ? 255 : 0;
            if (keep) required++;
          }
        }
        if (required < width * height / 2) throw new Error('dashboard_reference_pixel_coverage_insufficient');
        for (const bar of scene.bars) {
          const column = Math.floor(scene.x + scene.plot.x + (bar.left + bar.right) / 2) - x;
          const row = Math.ceil(scene.y + scene.plot.y + bar.bottom) - 1 - y;
          const offset = (row * width + column) * 4;
          if (column < 0 || row < 0 || column >= width || row >= height
            || pixels.data[offset + 3] !== 255
            || !rgb.every((channel, index) => channel === pixels.data[offset + index])) {
            throw new Error('dashboard_reference_bar_coverage_insufficient');
          }
        }
        context.putImageData(pixels, 0, 0);
        const blob = await canvas.convertToBlob({ type: 'image/png' });
        const output = new Uint8Array(await blob.arrayBuffer());
        let binary = '';
        for (const byte of output) binary += String.fromCharCode(byte);
        return btoa(binary);
      } finally { image.close(); }
    }, { encoded: png.toString('base64'), scene, expected, x, y, width, height });
    return { source: 'independent_render', mask: 'opaque_pixels',
      regions: [{ x, y, width, height, pngBase64 }] };
  } finally {
    await context.close();
  }
}

module.exports = { expectedTrafficTraces, dashboardChartScene, dashboardChartReference };
