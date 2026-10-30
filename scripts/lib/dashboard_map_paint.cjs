const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const FRONTEND = path.resolve(__dirname, '../../frontend');
const frontendRequire = createRequire(path.join(FRONTEND, 'package.json'));
const MAP_COMPONENT = path.join(FRONTEND, 'components/Map/NetworkMap/ChoroplethMap.tsx');
const MAPLIBRE = path.join(FRONTEND, 'node_modules/maplibre-gl/dist');
const MAP_ORIGIN = 'http://127.0.0.1:41732';
const MAP_ROOT = '/__dashboard_map_reference__';
const STATIC_ASSETS = new Map([
  ['/geo/world.topo.json', path.join(FRONTEND, 'public/geo/world.topo.json')],
  ['/maplibre-gl.mjs', path.join(MAPLIBRE, 'maplibre-gl.mjs')],
  ['/maplibre-gl-worker.mjs', path.join(MAPLIBRE, 'maplibre-gl-worker.mjs')],
  ['/maplibre-gl-shared.mjs', path.join(MAPLIBRE, 'maplibre-gl-shared.mjs')],
]);
const MAX_BUNDLE_BYTES = 12 * 1024 * 1024;
const MAX_REFERENCE_PIXELS = 8_000_000;
let bundlePromise;

function mapModuleLoader(source) {
  const ts = frontendRequire('typescript');
  const output = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ES2022,
      jsx: ts.JsxEmit.ReactJSX,
      esModuleInterop: true,
    },
    fileName: this.resourcePath,
  }).outputText;
  if (this.resourcePath !== MAP_COMPONENT) return output;
  return `${output}
import ReactRuntime from 'react';
import { createRoot } from 'react-dom/client';
import { ThemeProvider } from 'next-themes';
import * as maplibreRuntime from 'maplibre-gl';
const originalAddControl = maplibreRuntime.Map.prototype.addControl;
maplibreRuntime.Map.prototype.addControl = function (...args) {
  window.__dashboardMap = this;
  return originalAddControl.apply(this, args);
};
window.__mountDashboardMap = (node, data, theme) => {
  const root = createRoot(node);
  const render = value => root.render(ReactRuntime.createElement(ThemeProvider,
    { attribute: 'class', forcedTheme: theme },
    ReactRuntime.createElement(ChoroplethMap, { data: value })));
  render(data);
  window.__updateDashboardMap = render;
  return root;
};
window.__dashboardCountryName = country => {
  const name = resolveCountryName(country);
  return NORMALIZE_COUNTRY[name] || name;
};`;
}

function buildBundle() {
  if (!bundlePromise) {
    bundlePromise = new Promise((resolve, reject) => {
      const webpack = frontendRequire('next/dist/compiled/webpack/webpack').webpack;
      const captured = new Map();
      let compiler;
      try {
        compiler = webpack({
          mode: 'production',
          context: FRONTEND,
          entry: MAP_COMPONENT,
          output: {
            path: __dirname,
            filename: 'map.js',
            chunkFilename: '[name].js',
            publicPath: `${MAP_ROOT}/`,
            module: true,
          },
          experiments: { outputModule: true },
          externalsType: 'module',
          externals: { 'maplibre-gl': `module ${MAP_ORIGIN}/maplibre-gl.mjs` },
          resolve: {
            alias: { '@': FRONTEND },
            extensions: ['.tsx', '.ts', '.js', '.mjs', '.json'],
          },
          module: {
            rules: [{
              test: /\.tsx?$/,
              exclude: /node_modules/,
              use: [{ loader: __filename }],
            }],
          },
          optimization: { minimize: false, splitChunks: false, runtimeChunk: false },
          cache: false,
          devtool: false,
          stats: 'errors-warnings',
          infrastructureLogging: { level: 'error' },
        });
        compiler.hooks.shouldEmit.tap('dashboard-map-reference-capture', compilation => {
          for (const asset of compilation.getAssets()) {
            captured.set(asset.name, Buffer.from(asset.source.source()));
          }
          return false;
        });
      } catch (error) {
        bundlePromise = undefined;
        reject(error);
        return;
      }

      compiler.run((error, stats) => {
        compiler.close(closeError => {
          if (error || closeError || stats?.hasErrors()) {
            bundlePromise = undefined;
            const details = stats?.toString({ all: false, errors: true, warnings: true });
            reject(error || closeError || new Error(details || 'dashboard_map_bundle_failed'));
            return;
          }
          const bytes = [...captured.values()].reduce((total, asset) => total + asset.length, 0);
          if (!captured.has('map.js') || bytes > MAX_BUNDLE_BYTES || captured.size > 16) {
            bundlePromise = undefined;
            reject(new Error('dashboard_map_bundle_size_or_assets_unsupported'));
            return;
          }
          resolve(captured);
        });
      });
    });
  }
  return bundlePromise;
}

function validateData(data) {
  if (!Array.isArray(data) || data.length > 300) throw new Error('dashboard_map_data_unsupported');
  const countries = new Set();
  for (const row of data) {
    if (!row || typeof row.country !== 'string' || !/^[A-Za-z]{2}$/.test(row.country)
      || !Number.isFinite(row.count) || row.count < 0 || countries.has(row.country.toUpperCase())) {
      throw new Error('dashboard_map_data_unsupported');
    }
    countries.add(row.country.toUpperCase());
  }
}

function cssForMap({ width, height, background, hideOverlays }) {
  return `
    html,body{margin:0;width:100%;height:100%;overflow:hidden;background:${background}}
    #map-mount{position:relative;width:${width}px;height:${height}px}
    #map-mount>[role="group"]{position:relative;width:100%;height:100%;min-height:300px;
      overflow:hidden;border-radius:8px;background:${background}}
    #map-mount>[role="group"]>[aria-hidden="true"]{position:absolute!important;inset:0!important;
      width:100%!important;height:100%!important;min-height:300px!important}
    #map-mount>[role="group"]>div[class~="top-3"]{position:absolute;top:12px;left:12px;z-index:20}
    #map-mount>[role="group"]>div[class~="bottom-4"]{position:absolute;bottom:16px;left:16px;
      z-index:10;width:240px;height:60px;padding:12px;box-sizing:border-box}
    #map-mount .sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;
      clip:rect(0,0,0,0);white-space:nowrap;border:0}
    ${hideOverlays ? '#map-mount>[role="group"]>:not([aria-hidden="true"]){display:none!important}.maplibregl-ctrl-top-right{display:none!important}' : ''}
  `;
}

async function applyMapBackgrounds(page, backgrounds) {
  await page.evaluate(backgrounds => {
    document.documentElement.style.backgroundColor = backgrounds[0].color;
    document.body.style.backgroundColor = backgrounds[0].color;
    const mount = document.getElementById('map-mount');
    let parent = mount.parentElement;
    for (const background of backgrounds.slice(1, -1)) {
      const layer = document.createElement('div');
      Object.assign(layer.style, {
        position: 'relative', width: `${mount.clientWidth}px`, height: `${mount.clientHeight}px`,
        backgroundColor: background.color, borderRadius: background.radius,
      });
      parent.replaceChild(layer, mount);
      layer.append(mount);
      parent = layer;
    }
    window.__dashboardMapBackgrounds = backgrounds;
  }, backgrounds);
}

async function routeMapAssets(context, assets, denied) {
  await context.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.origin !== MAP_ORIGIN) {
      denied.push(url.origin);
      await route.abort();
      return;
    }
    if (url.pathname === `${MAP_ROOT}/` || url.pathname === '/fixture') {
      await route.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><html><body><div id="map-mount"></div></body></html>' });
      return;
    }
    if (url.pathname === '/favicon.ico') {
      await route.fulfill({ status: 204, body: '' });
      return;
    }
    if (STATIC_ASSETS.has(url.pathname)) {
      const assetPath = STATIC_ASSETS.get(url.pathname);
      if (!fs.existsSync(assetPath)) {
        await route.abort();
        return;
      }
      const contentType = url.pathname.endsWith('.json') ? 'application/json' : 'text/javascript';
      await route.fulfill({ status: 200, contentType, path: assetPath });
      return;
    }
    if (url.pathname.startsWith(`${MAP_ROOT}/`)) {
      const body = assets.get(url.pathname.slice(MAP_ROOT.length + 1));
      if (body) {
        await route.fulfill({ status: 200, contentType: 'text/javascript', body });
        return;
      }
    }
    denied.push(url.pathname);
    await route.abort();
  });
}

async function mountMap(page, {
  data, theme = 'light', width, height,
  backgrounds = [{ color: 'rgb(255, 255, 255)', radius: '0px' }], hideOverlays = false,
}) {
  const assets = await buildBundle();
  const denied = [];
  await routeMapAssets(page.context(), assets, denied);
  await page.goto(`${MAP_ORIGIN}/fixture`);
  const css = fs.readFileSync(path.join(MAPLIBRE, 'maplibre-gl.css'), 'utf8');
  await page.addStyleTag({
    content: `${css}\n${cssForMap({ width, height, background: backgrounds.at(-1).color, hideOverlays })}`,
  });
  await applyMapBackgrounds(page, backgrounds);
  await page.addScriptTag({ url: `${MAP_ORIGIN}${MAP_ROOT}/map.js`, type: 'module' });
  await page.evaluate(({ data, theme }) => {
    const mount = document.getElementById('map-mount');
    window.__mapRoot = window.__mountDashboardMap(mount, data, theme);
  }, { data, theme });
  await page.waitForFunction(() => {
    const map = window.__dashboardMap;
    return map && map.getLayer('countries') && map.isStyleLoaded();
  }, null, { timeout: 10000 });
  await page.evaluate(async () => {
    const map = window.__dashboardMap;
    await new Promise(resolve => {
      map.once('idle', resolve);
      map.triggerRepaint();
    });
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  });
  if (denied.length) throw new Error('dashboard_map_reference_external_request_blocked');
  return { assets, denied };
}

async function mapScene(page) {
  return page.evaluate(maximumPixels => {
    const groups = [...document.querySelectorAll('[role="group"][aria-label^="World map of requests by country"]')];
    if (groups.length !== 1) throw new Error('dashboard_map_surface_unavailable');
    const group = groups[0];
    const canvases = [...group.querySelectorAll('canvas.maplibregl-canvas')];
    if (canvases.length !== 1) throw new Error('dashboard_map_canvas_unavailable');
    const canvas = canvases[0];
    const rect = canvas.getBoundingClientRect();
    const groupRect = group.getBoundingClientRect();
    const viewport = { width: innerWidth, height: innerHeight, scale: devicePixelRatio, scrollX, scrollY };
    if (viewport.scale !== 1 || scrollX !== 0 || scrollY !== 0
      || ![rect.x, rect.y, rect.width, rect.height].every(Number.isInteger)
      || rect.width < 160 || rect.height < 160 || rect.width * rect.height > maximumPixels
      || canvas.width !== rect.width || canvas.height !== rect.height
      || groupRect.x !== rect.x || groupRect.y !== rect.y
      || groupRect.width !== rect.width || groupRect.height !== rect.height
      || rect.x < 0 || rect.y < 0 || rect.right > innerWidth || rect.bottom > innerHeight) {
      throw new Error('dashboard_map_geometry_unsupported');
    }
    const backgrounds = [];
    const colorProbe = new OffscreenCanvas(1, 1).getContext('2d');
    let opaque = false;
    for (let ancestor = canvas; ancestor; ancestor = ancestor.parentElement) {
      const style = getComputedStyle(ancestor);
      if (style.backgroundImage !== 'none' || style.filter !== 'none'
        || style.backdropFilter !== 'none' || style.mixBlendMode !== 'normal'
        || style.transform !== 'none' || Number(style.opacity) !== 1) {
        throw new Error('dashboard_map_background_unsupported');
      }
      if (!opaque) {
        const bounds = ancestor.getBoundingClientRect();
        const exactBounds = bounds.x === rect.x && bounds.y === rect.y
          && bounds.width === rect.width && bounds.height === rect.height;
        if (style.backgroundColor !== 'rgba(0, 0, 0, 0)'
          && (bounds.left > rect.left || bounds.top > rect.top
            || bounds.right < rect.right || bounds.bottom < rect.bottom)) {
          throw new Error('dashboard_map_background_unsupported');
        }
        backgrounds.unshift({ color: style.backgroundColor, radius: exactBounds ? style.borderRadius : '0px' });
        colorProbe.clearRect(0, 0, 1, 1);
        colorProbe.fillStyle = style.backgroundColor;
        colorProbe.fillRect(0, 0, 1, 1);
        opaque = colorProbe.getImageData(0, 0, 1, 1).data[3] === 255;
      }
    }
    if (!opaque) throw new Error('dashboard_map_background_unsupported');

    const overlays = [];
    const includeOverlay = (element, kind, expectedPosition) => {
      if (!element) throw new Error(`dashboard_map_${kind}_overlay_unavailable`);
      const box = element.getBoundingClientRect();
      if (![box.x, box.y, box.width, box.height].every(Number.isFinite)
        || box.width <= 0 || box.height <= 0 || box.x < rect.x || box.y < rect.y
        || box.right > rect.right || box.bottom > rect.bottom
        || box.width * box.height > rect.width * rect.height * 0.08) {
        throw new Error(`dashboard_map_${kind}_overlay_unsupported`);
      }
      const [edgeX, edgeY] = expectedPosition;
      if (Math.abs(edgeX === 'left' ? box.left - rect.left : rect.right - box.right) > 24
        || Math.abs(edgeY === 'top' ? box.top - rect.top : rect.bottom - box.bottom) > 24) {
        throw new Error(`dashboard_map_${kind}_overlay_position_unsupported`);
      }
      const paint = {
        left: box.left - 3, top: box.top - 3, right: box.right + 3, bottom: box.bottom + 3,
      };
      for (const descendant of [element, ...element.querySelectorAll('*')]) {
        const style = getComputedStyle(descendant);
        if (style.display === 'none' || style.visibility === 'hidden') continue;
        if (style.filter !== 'none') throw new Error('dashboard_map_overlay_shadow_unsupported');
        if (style.boxShadow === 'none') continue;
        for (const shadow of style.boxShadow.split(/,(?![^(]*\))/)) {
          const parsed = shadow.trim().match(
            /^(rgba?\([^)]*\)|color\([^)]*\)|oklab\([^)]*\))\s+(-?[\d.]+)px\s+(-?[\d.]+)px(?:\s+([\d.]+)px)?(?:\s+(-?[\d.]+)px)?(\s+inset)?$/,
          );
          if (!parsed) throw new Error('dashboard_map_overlay_shadow_unsupported');
          const [, color, offsetX, offsetY, blur = '0', spread = '0', inset] = parsed;
          if (!CSS.supports('color', color)) throw new Error('dashboard_map_overlay_shadow_unsupported');
          colorProbe.clearRect(0, 0, 1, 1);
          colorProbe.fillStyle = color;
          colorProbe.fillRect(0, 0, 1, 1);
          if (inset || colorProbe.getImageData(0, 0, 1, 1).data[3] === 0) continue;
          const shadowBox = descendant.getBoundingClientRect();
          // CSS blur uses sigma = blur / 2; Chromium's finite kernel spans three sigma.
          const reach = Math.ceil(Number(blur) * 1.5) + Number(spread);
          paint.left = Math.min(paint.left, shadowBox.left + Number(offsetX) - reach);
          paint.top = Math.min(paint.top, shadowBox.top + Number(offsetY) - reach);
          paint.right = Math.max(paint.right, shadowBox.right + Number(offsetX) + reach);
          paint.bottom = Math.max(paint.bottom, shadowBox.bottom + Number(offsetY) + reach);
        }
      }
      overlays.push({
        x: box.x - rect.x, y: box.y - rect.y, width: box.width, height: box.height,
        paint: {
          x: paint.left - rect.x, y: paint.top - rect.y,
          width: paint.right - paint.left, height: paint.bottom - paint.top,
        },
      });
      return element;
    };

    const button = group.querySelector('button[aria-haspopup="listbox"]');
    const legend = [...group.children].find(element => element.textContent.includes('Traffic Intensity'));
    if (button) {
      if (button.getAttribute('aria-expanded') !== 'false') throw new Error('dashboard_map_picker_open');
      includeOverlay(button.parentElement, 'picker', ['left', 'top']);
    }
    if (legend) includeOverlay(legend, 'legend', ['left', 'bottom']);

    const mapElement = canvas.closest('.maplibregl-map');
    const controls = mapElement?.querySelector('.maplibregl-ctrl-top-right');
    includeOverlay(controls, 'controls', ['right', 'top']);

    const known = [button?.parentElement, legend, controls].filter(Boolean);
    const visible = element => {
      const style = getComputedStyle(element);
      const screenReaderOnly = element.closest('.sr-only');
      const srStyle = screenReaderOnly && getComputedStyle(screenReaderOnly);
      if (srStyle?.overflow === 'hidden'
        && (/^rect\(0px,\s*0px,\s*0px,\s*0px\)$/.test(srStyle.clip)
          || srStyle.clipPath === 'inset(50%)')) return false;
      return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) > 0;
    };
    for (const element of group.querySelectorAll('*')) {
      if (!visible(element) || known.some(container => container === element || container.contains(element))) continue;
      if (mapElement?.contains(element)) continue;
      const style = getComputedStyle(element);
      if (style.position !== 'absolute' && style.position !== 'fixed') continue;
      const box = element.getBoundingClientRect();
      if (box.width > 0 && box.height > 0 && box.right > rect.left && box.left < rect.right
        && box.bottom > rect.top && box.top < rect.bottom) {
        throw new Error('dashboard_map_unknown_overlay');
      }
    }
    const totalOverlayArea = overlays.reduce((sum, overlay) => sum + overlay.width * overlay.height, 0);
    const maskedOverlayArea = overlays.reduce((sum, overlay) =>
      sum + overlay.paint.width * overlay.paint.height, 0);
    if (totalOverlayArea > rect.width * rect.height * 0.12
      || maskedOverlayArea > rect.width * rect.height * 0.14) {
      throw new Error('dashboard_map_overlay_coverage_unsupported');
    }
    return {
      x: rect.x, y: rect.y, width: rect.width, height: rect.height, backgrounds, overlays, viewport,
      theme: document.documentElement.classList.contains('dark') ? 'dark' : 'light',
    };
  }, MAX_REFERENCE_PIXELS);
}

async function maskReference(page, screenshot, data, overlays) {
  return page.evaluate(async ({ encoded, data, overlays }) => {
    const bytes = Uint8Array.from(atob(encoded), character => character.charCodeAt(0));
    const image = await createImageBitmap(new Blob([bytes], { type: 'image/png' }));
    try {
      const { width, height } = image;
      const canvas = new OffscreenCanvas(width, height);
      const context = canvas.getContext('2d', { willReadFrequently: true });
      context.drawImage(image, 0, 0);
      const pixels = context.getImageData(0, 0, width, height);
      const map = window.__dashboardMap;
      const layer = map.getLayer('countries');
      if (!map.loaded() || !layer || !map.getSource('world')) {
        throw new Error('dashboard_map_reference_style_unloaded');
      }
      const expression = map.getPaintProperty('countries', 'fill-color');
      if (!Array.isArray(expression) || expression[0] !== 'match' || expression[1]?.[1] !== 'name'
        || expression.length < 5 || (expression.length - 3) % 2 !== 0) {
        throw new Error('dashboard_map_reference_fill_expression_unsupported');
      }
      const countryFill = new Map();
      for (let index = 2; index < expression.length - 1; index += 2) {
        if (typeof expression[index] !== 'string' || typeof expression[index + 1] !== 'string') {
          throw new Error('dashboard_map_reference_fill_expression_unsupported');
        }
        countryFill.set(expression[index], expression[index + 1]);
      }
      const baseFill = expression.at(-1);
      if (typeof baseFill !== 'string') throw new Error('dashboard_map_reference_base_fill_unsupported');

      if (typeof window.__dashboardCountryName !== 'function') {
        throw new Error('dashboard_map_country_resolver_unavailable');
      }
      const features = map.queryRenderedFeatures({ layers: ['countries'] });
      const checkedCountries = [];
      const isOverlay = (x, y) => overlays.some(({ paint }) =>
        x + 0.5 >= paint.x && x + 0.5 < paint.x + paint.width
        && y + 0.5 >= paint.y && y + 0.5 < paint.y + paint.height);
      for (const row of data) {
        if (row.count <= 0) continue;
        const name = window.__dashboardCountryName(row.country);
        const color = countryFill.get(name);
        const countryFeatures = features.filter(feature => feature.properties?.name === name);
        if (!color || !countryFeatures.length) throw new Error('dashboard_map_positive_country_not_rendered');

        const path = new Path2D();
        const addRing = ring => {
          if (!Array.isArray(ring) || ring.length < 4) throw new Error('dashboard_map_geometry_unsupported');
          ring.forEach((coordinate, index) => {
            const point = map.project(coordinate);
            if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) {
              throw new Error('dashboard_map_geometry_unsupported');
            }
            if (index === 0) path.moveTo(point.x, point.y);
            else path.lineTo(point.x, point.y);
          });
          path.closePath();
        };
        for (const feature of countryFeatures) {
          const geometry = feature.geometry;
          if (geometry?.type === 'Polygon') geometry.coordinates.forEach(addRing);
          else if (geometry?.type === 'MultiPolygon') geometry.coordinates.forEach(polygon => polygon.forEach(addRing));
          else throw new Error('dashboard_map_geometry_unsupported');
        }
        const mask = new OffscreenCanvas(width, height);
        const maskContext = mask.getContext('2d', { willReadFrequently: true });
        maskContext.fillStyle = '#fff';
        maskContext.fill(path, 'evenodd');
        const maskPixels = maskContext.getImageData(0, 0, width, height).data;
        const backdrop = new OffscreenCanvas(1, 1).getContext('2d');
        for (const background of window.__dashboardMapBackgrounds) {
          backdrop.fillStyle = background.color;
          backdrop.fillRect(0, 0, 1, 1);
        }
        backdrop.fillStyle = color;
        backdrop.fillRect(0, 0, 1, 1);
        const expected = backdrop.getImageData(0, 0, 1, 1).data;
        let countryPixels = 0;
        let matchingPixels = 0;
        for (let y = 1; y < height - 1; y++) {
          for (let x = 1; x < width - 1; x++) {
            const offset = (y * width + x) * 4;
            if (maskPixels[offset + 3] < 250 || isOverlay(x, y)) continue;
            let interior = true;
            for (let dy = -1; interior && dy <= 1; dy++) {
              for (let dx = -1; interior && dx <= 1; dx++) {
                if (maskPixels[((y + dy) * width + x + dx) * 4 + 3] < 250) interior = false;
              }
            }
            if (!interior) continue;
            countryPixels++;
            if ([0, 1, 2].every(channel => Math.abs(pixels.data[offset + channel] - expected[channel]) <= 1)
              && pixels.data[offset + 3] === 255) matchingPixels++;
          }
        }
        const minimum = Math.max(32, Math.ceil(countryPixels * 0.65));
        if (countryPixels < 64 || matchingPixels < minimum) {
          throw new Error('dashboard_map_country_fill_coverage_insufficient');
        }
        checkedCountries.push({ country: row.country.toUpperCase(), countryPixels, matchingPixels });
      }

      const original = new Uint8ClampedArray(pixels.data);
      let opaque = 0;
      for (let y = 0; y < height; y++) {
        for (let x = 0; x < width; x++) {
          const offset = (y * width + x) * 4;
          let edge = false;
          for (let dy = -2; !edge && dy <= 2; dy++) {
            for (let dx = -2; !edge && dx <= 2; dx++) {
              const nx = x + dx;
              const ny = y + dy;
              if (nx < 0 || ny < 0 || nx >= width || ny >= height) continue;
              const neighbor = (ny * width + nx) * 4;
              edge = [0, 1, 2, 3].some(channel => original[offset + channel] !== original[neighbor + channel]);
            }
          }
          const keep = !edge && !isOverlay(x, y) && original[offset + 3] === 255;
          pixels.data[offset + 3] = keep ? 255 : 0;
          if (keep) opaque++;
        }
      }
      if (opaque < Math.max(100, width * height * 0.1)) {
        throw new Error('dashboard_map_reference_pixel_coverage_insufficient');
      }
      context.putImageData(pixels, 0, 0);
      const blob = await canvas.convertToBlob({ type: 'image/png' });
      const output = new Uint8Array(await blob.arrayBuffer());
      let binary = '';
      for (const byte of output) binary += String.fromCharCode(byte);
      return { pngBase64: btoa(binary), opaque, checkedCountries };
    } finally { image.close(); }
  }, { encoded: screenshot.toString('base64'), data, overlays });
}

async function dashboardMapReference(page, data) {
  validateData(data);
  const scene = await mapScene(page);
  const browser = page.context().browser();
  if (!browser) throw new Error('dashboard_map_reference_browser_unavailable');
  const context = await browser.newContext({
    viewport: { width: scene.viewport.width, height: scene.viewport.height },
    deviceScaleFactor: 1,
    serviceWorkers: 'block',
  });
  try {
    const reference = await context.newPage();
    const { denied } = await mountMap(reference, {
      data, theme: scene.theme, width: scene.width, height: scene.height,
      backgrounds: scene.backgrounds, hideOverlays: true,
    });
    const screenshot = await reference.screenshot({ clip: { x: 0, y: 0, width: scene.width, height: scene.height } });
    const masked = await maskReference(reference, screenshot, data, scene.overlays);
    if (denied.length || masked.opaque < 100 || masked.checkedCountries.length !== data.filter(row => row.count > 0).length) {
      throw new Error('dashboard_map_reference_validation_failed');
    }
    return {
      source: 'independent_render',
      mask: 'opaque_pixels',
      regions: [{
        x: scene.x, y: scene.y, width: scene.width, height: scene.height,
        pngBase64: masked.pngBase64,
      }],
    };
  } finally {
    await context.close();
  }
}

async function mountDashboardMapFixture(page, {
  initialData, nextData, mode = 'update', width = 700, height = 360,
  theme = 'light', background = 'rgb(255, 255, 255)', overlay = false,
}) {
  validateData(initialData);
  validateData(nextData);
  const assets = await buildBundle();
  const denied = [];
  await routeMapAssets(page.context(), assets, denied);
  await page.goto(`${MAP_ORIGIN}/fixture`);
  const css = fs.readFileSync(path.join(MAPLIBRE, 'maplibre-gl.css'), 'utf8');
  await page.addStyleTag({ content: `${css}\n${cssForMap({ width, height, background, hideOverlays: false })}` });
  await page.evaluate(({ overlay }) => {
    const action = document.createElement('button');
    action.id = 'map-action';
    action.textContent = 'Update map';
    document.body.prepend(action);
    if (overlay) {
      const cover = document.createElement('div');
      cover.id = 'unknown-map-overlay';
      cover.style.cssText = 'position:fixed;left:260px;top:170px;width:120px;height:120px;background:white;z-index:9999';
      document.body.append(cover);
    }
  }, { overlay });
  await page.addScriptTag({ url: `${MAP_ORIGIN}${MAP_ROOT}/map.js`, type: 'module' });
  await page.evaluate(({ initialData, nextData, theme, mode }) => {
    const root = document.getElementById('map-mount');
    window.__mapRoot = window.__mountDashboardMap(root, initialData, theme);
    document.getElementById('map-action').addEventListener('click', async () => {
      const map = window.__dashboardMap;
      if (mode !== 'update') {
        const original = map.setPaintProperty.bind(map);
        const base = map.getPaintProperty('countries', 'fill-color').at(-1);
        map.setPaintProperty = function (layer, property, value, ...rest) {
          if (property === 'fill-color') {
            if (mode === 'stale') return this;
            value = base;
          }
          return original(layer, property, value, ...rest);
        };
      }
      window.__updateDashboardMap(nextData);
      await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
      map.once('idle', () => { window.done = performance.now(); });
      map.triggerRepaint();
    });
  }, { initialData, nextData, theme, mode });
  await page.waitForFunction(() => {
    const map = window.__dashboardMap;
    return map && map.getLayer('countries') && map.isStyleLoaded();
  }, null, { timeout: 10000 });
  await page.evaluate(async () => {
    const map = window.__dashboardMap;
    await new Promise(resolve => {
      map.once('idle', resolve);
      map.triggerRepaint();
    });
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  });
  if (denied.length) throw new Error('dashboard_map_fixture_external_request_blocked');
}

module.exports = mapModuleLoader;
module.exports.dashboardMapReference = dashboardMapReference;
module.exports.mountDashboardMapFixture = mountDashboardMapFixture;
