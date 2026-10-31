const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { createRequire } = require('node:module');

const frontend = path.resolve(__dirname, '../../frontend');
const frontendRequire = createRequire(path.join(frontend, 'package.json'));
const { JSDOM } = frontendRequire('jsdom');
const modules = new Map();
let productionRenderer;

function resolveFrontendModule(parent, request) {
  const base = request.startsWith('@/') ? path.join(frontend, request.slice(2))
    : path.resolve(path.dirname(parent), request);
  const candidates = [
    base,
    ...['.tsx', '.ts', '.jsx', '.js', '.json'].map(extension => `${base}${extension}`),
    ...['index.tsx', 'index.ts', 'index.jsx', 'index.js'].map(file => path.join(base, file)),
  ];
  return candidates.find(filename => fs.existsSync(filename) && fs.statSync(filename).isFile());
}

function loadFrontendModule(filename) {
  filename = path.resolve(filename);
  if (modules.has(filename)) return modules.get(filename).exports;
  if (filename.endsWith('.json')) return JSON.parse(fs.readFileSync(filename, 'utf8'));

  const module = new Module(filename);
  module.filename = filename;
  module.paths = Module._nodeModulePaths(path.dirname(filename));
  modules.set(filename, module);
  module.require = request => {
    if (request.endsWith('.css')) return {};
    const local = resolveFrontendModule(filename, request);
    if (local) return loadFrontendModule(local);
    if (request.startsWith('@/')) throw new Error(`dashboard_card_reference_module_unavailable:${request}`);
    return frontendRequire(request);
  };

  const source = fs.readFileSync(filename, 'utf8');
  const output = /\.(?:tsx?|jsx?)$/.test(filename)
    ? frontendRequire('typescript').transpileModule(source, {
        fileName: filename,
        compilerOptions: {
          module: frontendRequire('typescript').ModuleKind.CommonJS,
          target: frontendRequire('typescript').ScriptTarget.ES2022,
          jsx: frontendRequire('typescript').JsxEmit.ReactJSX,
          esModuleInterop: true,
        },
      }).outputText
    : source;
  module._compile(output, filename);
  return module.exports;
}

function getProductionRenderer() {
  if (productionRenderer) return productionRenderer;
  const React = frontendRequire('react');
  const { QueryClient, QueryClientProvider } = frontendRequire('@tanstack/react-query');
  const { SearchParamsContext } = frontendRequire('next/dist/shared/lib/hooks-client-context.shared-runtime');
  const { queryKeys } = loadFrontendModule(path.join(frontend, 'lib/api.ts'));
  const { TopTenTable } = loadFrontendModule(path.join(frontend, 'components/Dashboard/TopTenTable.tsx'));
  const { Bot } = frontendRequire('lucide-react');
  const { usePopGeoStore } = loadFrontendModule(path.join(frontend, 'stores/filterStore.ts'));
  const { isIpFamilyField } = loadFrontendModule(path.join(frontend, 'lib/utils.ts'));
  productionRenderer = {
    React, QueryClient, QueryClientProvider, SearchParamsContext, queryKeys, TopTenTable, Bot,
    usePopGeoStore, isIpFamilyField, formatValue: loadFrontendModule(path.join(frontend, 'lib/utils.ts')).formatValue,
  };
  return productionRenderer;
}

function normalizeAggregates(value) {
  const aggregates = value?.aggregates ?? value;
  if (!aggregates || !aggregates.data || typeof aggregates.data !== 'object') {
    throw new Error('dashboard_card_reference_aggregates_unsupported');
  }
  return aggregates;
}

function normalizeTopBots(value) {
  return value?.top_bots ?? value?.topBots ?? value?.top_bots_data ?? value?.topBotsData;
}

function validateTopItems(data, cardId) {
  if (!data || !Array.isArray(data.top) || data.top.length === 0) {
    throw new Error(`dashboard_card_reference_rows_unavailable:${cardId}`);
  }
  const top = data.top.slice(0, 10);
  for (const [index, item] of top.entries()) {
    if (!item || !Number.isFinite(item.count) || item.count < 0) {
      throw new Error(`dashboard_card_reference_missing_count:${cardId}:${index}`);
    }
    if (item.value !== null && !['string', 'number'].includes(typeof item.value)) {
      throw new Error(`dashboard_card_reference_value_unsupported:${cardId}:${index}`);
    }
    if (item.label != null && typeof item.label !== 'string') {
      throw new Error(`dashboard_card_reference_label_unsupported:${cardId}:${index}`);
    }
  }
  return { ...data, top };
}

function cardData(card, canonical) {
  const cardId = card.id;
  if (cardId === '_bot_name' || cardId === '_ngwaf_bot_name') {
    const bots = normalizeTopBots(canonical);
    if (!bots) throw new Error(`dashboard_card_reference_bot_data_unsupported:${cardId}`);
    const items = cardId === '_bot_name' ? bots.bots : bots.ngwaf_bots;
    if (!Array.isArray(items) || items.length === 0) {
      throw new Error(`dashboard_card_reference_rows_unavailable:${cardId}`);
    }
    const top = items.map((item, index) => {
      if (!item || !Number.isFinite(item.request_count) || item.request_count < 0) {
        throw new Error(`dashboard_card_reference_missing_count:${cardId}:${index}`);
      }
      if (typeof item.name !== 'string') {
        throw new Error(`dashboard_card_reference_label_unsupported:${cardId}:${index}`);
      }
      return {
        value: cardId === '_bot_name' && item.id ? item.id : item.name,
        label: item.name,
        count: item.request_count,
      };
    });
    return validateTopItems({ total: top.reduce((sum, item) => sum + item.count, 0), top }, cardId);
  }

  const aggregates = normalizeAggregates(canonical);
  return validateTopItems(aggregates.data[cardId], cardId);
}

function compareCardData(card, options) {
  if (!options.compareMode || card.id === '_bot_name' || card.id === '_ngwaf_bot_name') return undefined;
  if (!options.compareAggregates) {
    throw new Error('dashboard_card_reference_comparison_unsupported');
  }
  const aggregates = normalizeAggregates(options.compareAggregates);
  const data = aggregates.data[card.id];
  if (!data || !Array.isArray(data.top)) {
    throw new Error(`dashboard_card_reference_comparison_unsupported:${card.id}`);
  }
  return validateTopItems(data, card.id);
}

async function renderDashboardCardMarkup(canonical, options) {
  if (!options || !options.card || typeof options.card.id !== 'string'
    || typeof options.card.label !== 'string') {
    throw new Error('dashboard_card_reference_card_metadata_required');
  }
  const renderer = getProductionRenderer();
  const card = options.card;
  const data = cardData(card, canonical);
  const compareData = compareCardData(card, options);
  const bootstrap = options.bootstrap && typeof options.bootstrap === 'object' ? options.bootstrap : {};
  const serviceId = options.serviceId ?? bootstrap.active_service_id ?? 'dashboard-card-reference';
  const maskIps = options.maskIps ?? bootstrap.settings?.mask_ips ?? false;
  if (typeof maskIps !== 'boolean') throw new Error('dashboard_card_reference_mask_mode_unsupported');

  const queryClient = new renderer.QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity, gcTime: Infinity } },
  });
  queryClient.setQueryData(renderer.queryKeys.bootstrap(), {
    ...bootstrap,
    active_service_id: serviceId,
    settings: { ...bootstrap.settings, mask_ips: maskIps },
  });
  queryClient.setQueryData(
    renderer.queryKeys.logFieldsCatalog(serviceId),
    options.catalog ?? { fields: [], groups: [] },
  );

  const popGeo = options.popGeo ?? bootstrap.pop_geo;
  if (card.id === 'pop' && (!popGeo || typeof popGeo !== 'object')) {
    throw new Error('dashboard_card_reference_pop_geo_unsupported');
  }
  const previousPopGeo = renderer.usePopGeoStore.getState().map;
  const keys = [
    'window', 'document', 'navigator', 'HTMLElement', 'Element', 'Node',
    'MutationObserver', 'getComputedStyle', 'IS_REACT_ACT_ENVIRONMENT',
  ];
  const previousGlobals = new Map(keys.map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const dom = new JSDOM('<!doctype html><div id="dashboard-card-reference-root"></div>', {
    url: `http://dashboard-card-reference.invalid/dashboard${serviceId ? `?service=${encodeURIComponent(serviceId)}` : ''}`,
    pretendToBeVisual: true,
  });
  let reactRoot;
  let mounted = false;
  try {
    for (const key of keys) {
      const value = key === 'IS_REACT_ACT_ENVIRONMENT' ? true
        : key === 'getComputedStyle' ? dom.window.getComputedStyle.bind(dom.window)
          : dom.window[key];
      Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
    }
    if (popGeo) renderer.usePopGeoStore.getState().setMap(popGeo);
    const { createRoot } = frontendRequire('react-dom/client');
    const query = serviceId ? `?service=${encodeURIComponent(serviceId)}` : '';
    const icon = card.id === '_bot_name'
      ? renderer.React.createElement(renderer.Bot, { className: 'h-4 w-4' })
      : undefined;
    const component = renderer.React.createElement(renderer.TopTenTable, {
      title: card.label,
      icon,
      field: card.id,
      inActiveFormat: card.inActiveFormat ?? true,
      data,
      compareData,
    });
    const providers = renderer.React.createElement(
      renderer.QueryClientProvider,
      { client: queryClient },
      renderer.React.createElement(
        renderer.SearchParamsContext.Provider,
        { value: new URLSearchParams(query) },
        component,
      ),
    );
    reactRoot = createRoot(document.getElementById('dashboard-card-reference-root'));
    mounted = true;
    await renderer.React.act(async () => { reactRoot.render(providers); });
    const markup = document.getElementById('dashboard-card-reference-root').innerHTML;
    await renderer.React.act(async () => { reactRoot.unmount(); });
    mounted = false;
    return markup;
  } finally {
    try {
      if (mounted) {
        try {
          await renderer.React.act(async () => { reactRoot.unmount(); });
        } finally {
          mounted = false;
        }
      }
    } finally {
      renderer.usePopGeoStore.getState().setMap(previousPopGeo);
      queryClient.clear();
      await new Promise(resolve => setImmediate(resolve));
      dom.window.close();
      for (const key of keys) {
        const descriptor = previousGlobals.get(key);
        if (descriptor) Object.defineProperty(globalThis, key, descriptor);
        else delete globalThis[key];
      }
    }
  }
}

function validateCards(cards) {
  if (!Array.isArray(cards) || !cards.length || cards.length > 256
    || cards.some(card => !card || typeof card.id !== 'string' || typeof card.label !== 'string')) {
    throw new Error('dashboard_card_reference_cards_required');
  }
  const ids = new Set(cards.map(card => card.id));
  if (ids.size !== cards.length) throw new Error('dashboard_card_reference_duplicate_card');
}

async function dashboardCardScene(page, cards, canonical) {
  const renderer = getProductionRenderer();
  const expectedRows = cards.map(card => {
    try {
      return {
        id: card.id,
        labels: cardData(card, canonical).top.map(item =>
          String(item.label || renderer.formatValue(card.id, item.value))),
      };
    } catch (error) {
      return { id: card.id, error: error.message };
    }
  });
  const ipFields = cards.filter(card => renderer.isIpFamilyField(card.id)).map(card => card.id);
  return page.evaluate(({ cards, expectedRows, ipFields }) => {
    const normalizeText = value => value.replace(/\s+/g, ' ').trim();
    const intersection = (a, b) => ({
      left: Math.max(a.left, b.left),
      top: Math.max(a.top, b.top),
      right: Math.min(a.right, b.right),
      bottom: Math.min(a.bottom, b.bottom),
    });
    const ancestorsVisibleRect = element => {
      const bounds = element.getBoundingClientRect();
      let visible = intersection(
        { left: bounds.left, top: bounds.top, right: bounds.right, bottom: bounds.bottom },
        { left: 0, top: 0, right: innerWidth, bottom: innerHeight },
      );
      for (let ancestor = element; ancestor && visible.right > visible.left && visible.bottom > visible.top;
        ancestor = ancestor.parentElement) {
        const style = getComputedStyle(ancestor);
        if (style.display === 'none' || style.visibility !== 'visible' || Number(style.opacity) !== 1
          || ancestor.getAttribute('aria-hidden') === 'true') return null;
        if (style.clipPath !== 'none' || style.filter !== 'none' || style.perspective !== 'none'
          || style.transform !== 'none' || style.mixBlendMode !== 'normal') {
          throw new Error('dashboard_card_reference_ancestor_effect_unsupported');
        }
        const clip = /(?:hidden|clip|scroll|auto)/;
        const containPaint = style.contain.split(/\s+/).some(value =>
          ['paint', 'strict', 'content'].includes(value));
        if (clip.test(style.overflowX) || clip.test(style.overflowY) || containPaint) {
          const rect = ancestor.getBoundingClientRect();
          const left = rect.left + ancestor.clientLeft;
          const top = rect.top + ancestor.clientTop;
          visible = intersection(visible, {
            left, top,
            right: left + ancestor.clientWidth,
            bottom: top + ancestor.clientHeight,
          });
        }
      }
      if (typeof element.checkVisibility === 'function'
        && !element.checkVisibility({
          checkOpacity: true, checkVisibilityCSS: true, contentVisibilityAuto: true,
        })) return null;
      return visible.right > visible.left && visible.bottom > visible.top ? visible : null;
    };

    const main = document.querySelector('main');
    if (!main) throw new Error('dashboard_card_reference_main_missing');
    const styles = [];
    const fontUrls = new Set();
    const absoluteUrls = (text, base, isFont) => text.replace(
      /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)]*))\s*\)/gi,
      (match, quoted, singleQuoted, unquoted) => {
        const source = quoted ?? singleQuoted ?? unquoted.trim();
        if (source.includes('\\')) throw new Error('dashboard_card_reference_css_url_unsupported');
        let url;
        try { url = new URL(source, base); } catch {
          throw new Error('dashboard_card_reference_css_url_unsupported');
        }
        if (isFont) fontUrls.add(url.href);
        return `url("${url.href.replaceAll('"', '%22')}")`;
      },
    );
    const serializeRule = (rule, base) => {
      if (rule.type === CSSRule.IMPORT_RULE) {
        throw new Error('dashboard_card_reference_css_import_unsupported');
      }
      if (rule.cssRules) {
        for (const child of rule.cssRules) serializeRule(child, base);
      }
      return absoluteUrls(rule.cssText, base, rule.type === CSSRule.FONT_FACE_RULE);
    };
    for (const sheet of document.styleSheets) {
      let rules;
      try { rules = [...sheet.cssRules]; } catch {
        throw new Error('dashboard_card_reference_stylesheet_unreadable');
      }
      styles.push(rules.map(rule => serializeRule(rule, sheet.href ?? document.baseURI)).join('\n'));
    }

    const candidates = [];
    for (const heading of main.querySelectorAll('h3')) {
      const titleText = normalizeText(heading.textContent ?? '');
      if (!titleText.endsWith('(Top 10)')) continue;
      let root = heading.parentElement;
      while (root && !(root.tagName === 'DIV' && root.classList.contains('[content-visibility:auto]')
        && root.querySelector('table tbody'))) root = root.parentElement;
      if (!root) continue;

      const visibleRoot = ancestorsVisibleRect(root);
      if (!visibleRoot) continue;
      const cardLabel = titleText.slice(0, -'(Top 10)'.length).trim();
      const cardMatches = cards.filter(item => item.label === cardLabel);
      if (cardMatches.length !== 1) {
        throw new Error(`dashboard_card_reference_card_catalog_mismatch:${cardLabel}`);
      }
      const [card] = cardMatches;
      const expected = expectedRows.find(item => item.id === card.id);
      const rootRect = root.getBoundingClientRect();
      const rows = [...root.querySelectorAll('table tbody tr')];
      const fullRows = rows.map((row, index) => {
        const bounds = row.getBoundingClientRect();
        const visible = ancestorsVisibleRect(row);
        if (!visible || bounds.width <= 0 || bounds.height <= 0) return null;
        const epsilon = 0.5;
        const fullyVisible = visible.left <= bounds.left + epsilon
          && visible.right >= bounds.right - epsilon
          && visible.top <= bounds.top + epsilon
          && visible.bottom >= bounds.bottom - epsilon;
        return fullyVisible ? {
          index, x: bounds.x, y: bounds.y, width: bounds.width, height: bounds.height,
        } : null;
      }).filter(Boolean);
      if (!fullRows.length) continue;
      for (const { index } of fullRows) {
        const row = rows[index];
        const value = row.querySelector('td > button[title], td > div[title]');
        if (expected?.error) throw new Error(expected.error);
        if (!expected || value?.getAttribute('title') !== expected.labels[index]) {
          throw new Error(`dashboard_card_reference_label_semantics_mismatch:${card.id}:${index}`);
        }
      }
      const first = fullRows[0];
      const last = fullRows[fullRows.length - 1];
      const x = Math.ceil(first.x);
      const y = Math.ceil(first.y);
      const right = Math.floor(first.x + first.width);
      const bottom = Math.floor(last.y + last.height);
      const rowElements = rows.filter((_, index) =>
        fullRows.some(row => row.index === index));
      const masked = rendererMaskMode(card.id, rowElements);
      candidates.push({
        card,
        x, y, width: right - x, height: bottom - y,
        root: { x: rootRect.x, y: rootRect.y, width: rootRect.width, height: rootRect.height },
        rows: rows.map(row => {
          const rect = row.getBoundingClientRect();
          return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
        }),
        visibleRows: fullRows.map(row => row.index),
        masked,
      });
    }

    function rendererMaskMode(cardId, rows) {
      if (!ipFields.includes(cardId)) return null;
      const staticRows = rows.filter(row => row.querySelector('td > div'));
      const buttonRows = rows.filter(row => row.querySelector('td > button'));
      if (staticRows.length && buttonRows.length) {
        throw new Error(`dashboard_card_reference_mixed_mask_mode:${cardId}`);
      }
      return staticRows.length > 0;
    }

    if (!candidates.length) throw new Error('dashboard_card_reference_no_visible_rows');
    if (candidates.length > 32) throw new Error('dashboard_card_reference_too_many_visible_cards');
    const language = navigator.language;
    if (!language || devicePixelRatio !== 1) {
      throw new Error('dashboard_card_reference_viewport_unsupported');
    }
    return {
      cards: candidates,
      css: styles.join('\n'),
      fontUrls: [...fontUrls],
      baseUrl: document.baseURI,
      origin: location.origin,
      language,
      viewport: { width: innerWidth, height: innerHeight },
      htmlClass: document.documentElement.className,
      htmlStyle: document.documentElement.getAttribute('style') ?? '',
      bodyClass: document.body.className,
      bodyStyle: document.body.getAttribute('style') ?? '',
      serviceId: new URLSearchParams(location.search).get('service'),
    };
  }, { cards, expectedRows, ipFields });
}

function escapeAttribute(value) {
  return String(value).replaceAll('&', '&amp;').replaceAll('"', '&quot;')
    .replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

async function buildReferenceDocument(scene, canonical, options) {
  const cards = [];
  for (const { card, root, masked } of scene.cards) {
    const configuredMask = options.maskIps ?? options.bootstrap?.settings?.mask_ips;
    if (masked !== null && configuredMask !== undefined && configuredMask !== masked) {
      throw new Error(`dashboard_card_reference_mask_mode_unverified:${card.id}`);
    }
    const cardOptions = {
      ...options,
      card,
      serviceId: options.serviceId ?? scene.serviceId ?? options.bootstrap?.active_service_id,
      maskIps: configuredMask ?? (masked === null ? false : masked),
    };
    const markup = await renderDashboardCardMarkup(canonical, cardOptions);
    // Fixed wrappers create fractional compositor surfaces and alter glyph/bar rasterization.
    cards.push(`<div data-card-reference="${escapeAttribute(card.id)}" style="position:absolute;left:${root.x}px;top:${root.y}px;width:${root.width}px;height:${root.height}px">${markup}</div>`);
  }
  const cardMarkup = cards.join('');
  const base = scene.origin && scene.origin !== 'null'
    ? `<base href="${escapeAttribute(scene.baseUrl)}">`
    : '';
  return `<!doctype html><html lang="${escapeAttribute(scene.language)}" class="${escapeAttribute(scene.htmlClass)}" style="${escapeAttribute(scene.htmlStyle)}"><head>${base}<meta charset="utf-8"><style>${scene.css}</style></head><body class="${escapeAttribute(scene.bodyClass)}" style="margin:0;${escapeAttribute(scene.bodyStyle)}">${cardMarkup}</body></html>`;
}

async function verifyReferenceGeometry(page, scene) {
  return page.evaluate(cards => {
    const close = (actual, expected) => Number.isFinite(actual)
      && Math.abs(actual - expected) <= 0.5;
    return cards.every(card => {
      const wrapper = document.querySelector(`[data-card-reference="${CSS.escape(card.card.id)}"]`);
      const root = wrapper?.firstElementChild;
      if (!root) return false;
      const rect = root.getBoundingClientRect();
      if (!close(rect.x, card.root.x) || !close(rect.y, card.root.y)
        || !close(rect.width, card.root.width) || !close(rect.height, card.root.height)) return false;
      const rows = [...root.querySelectorAll('table tbody tr')].map(row => {
        const bounds = row.getBoundingClientRect();
        return { x: bounds.x, y: bounds.y, width: bounds.width, height: bounds.height };
      });
      if (rows.length !== card.rows.length) return false;
      return rows.every((row, index) => ['x', 'y', 'width', 'height']
        .every(key => close(row[key], card.rows[index][key])));
    });
  }, scene.cards);
}

async function fetchReferenceFonts(page, scene) {
  const fonts = new Map();
  if (scene.fontUrls.length > 32) throw new Error('dashboard_card_reference_font_budget_exceeded');
  const urls = scene.fontUrls.map(href => {
    const url = new URL(href);
    if (!['http:', 'https:'].includes(url.protocol) || url.origin !== scene.origin
      || url.username || url.password || url.search || url.hash
      || !/^\/_next\/static\/media\/[A-Za-z0-9._-]+\.(?:woff2?|ttf|otf)$/.test(url.pathname)) {
      throw new Error('dashboard_card_reference_font_origin_or_path_unsupported');
    }
    return url;
  });
  let total = 0;
  for (const url of urls) {
    let response;
    try {
      try {
        response = await page.context().request.get(url.href, {
          maxRedirects: 0, failOnStatusCode: false, timeout: 5000,
        });
      } catch {
        throw new Error('dashboard_card_reference_font_transport_failed');
      }
      if (response.status() !== 200) {
        throw new Error(`dashboard_card_reference_font_http_${response.status()}`);
      }
      if (response.url() !== url.href) {
        throw new Error('dashboard_card_reference_font_response_origin_unverified');
      }
      let body;
      try { body = await response.body(); } catch {
        throw new Error('dashboard_card_reference_font_read_failed');
      }
      total += body.length;
      if (!body.length || body.length > 2_000_000 || total > 16_000_000) {
        throw new Error('dashboard_card_reference_font_budget_exceeded');
      }
      const signature = body.subarray(0, 4).toString('hex');
      const contentType = {
        '774f4632': 'font/woff2', '774f4646': 'font/woff',
        '00010000': 'font/ttf', '4f54544f': 'font/otf',
      }[signature];
      if (!contentType) throw new Error('dashboard_card_reference_font_bytes_invalid');
      fonts.set(url.href, { body, contentType });
    } finally {
      if (response) {
        await response.dispose().catch(() => {
          throw new Error('dashboard_card_reference_font_dispose_failed');
        });
      }
    }
  }
  return fonts;
}

/**
 * Renders visible canonical Dashboard cards with TopTenTable in a network-blocked context.
 * Pass the current DashboardCard[] as options.cards; comparison, masking and PoP state are
 * accepted as options.compareAggregates/compareMode, maskIps/bootstrap and popGeo/bootstrap.
 */
async function dashboardCardReference(page, canonicalBundleOrAggregates, options = {}) {
  validateCards(options.cards);
  normalizeAggregates(canonicalBundleOrAggregates);
  if (options.compareMode && !options.compareAggregates) {
    throw new Error('dashboard_card_reference_comparison_unsupported');
  }
  const scene = await dashboardCardScene(page, options.cards, canonicalBundleOrAggregates);
  const browser = page.context().browser();
  if (!browser) throw new Error('dashboard_card_reference_browser_unavailable');
  if (new Intl.NumberFormat(scene.language).resolvedOptions().locale
    !== new Intl.NumberFormat().resolvedOptions().locale) {
    throw new Error('dashboard_card_reference_locale_unsupported');
  }
  const fonts = await fetchReferenceFonts(page, scene);
  const context = await browser.newContext({
    viewport: scene.viewport,
    deviceScaleFactor: 1,
    locale: scene.language,
    serviceWorkers: 'block',
  });
  const denied = [];
  try {
    await context.route('**/*', async route => {
      const request = route.request();
      const font = fonts.get(request.url());
      if (request.method() !== 'GET' || request.resourceType() !== 'font' || !font) {
        denied.push(request.url());
        await route.abort();
        return;
      }
      await route.fulfill({
        status: 200, ...font,
        headers: { 'access-control-allow-origin': '*', 'cache-control': 'no-store' },
      });
    });
    const reference = await context.newPage();
    await reference.setContent(await buildReferenceDocument(scene, canonicalBundleOrAggregates, options));
    await reference.evaluate(async () => {
      await document.fonts.ready;
      await new Promise(resolve => requestAnimationFrame(() =>
        requestAnimationFrame(resolve)));
      const failedFonts = [...document.fonts].filter(font => font.status === 'error');
      if (failedFonts.length) throw new Error('dashboard_card_reference_font_load_failed');
    });
    if (denied.length) throw new Error('dashboard_card_reference_external_request_blocked');
    if (!await verifyReferenceGeometry(reference, scene)) {
      throw new Error('dashboard_card_reference_geometry_unverified');
    }

    const regions = [];
    for (const card of scene.cards) {
      const png = await reference.screenshot({
        clip: { x: card.x, y: card.y, width: card.width, height: card.height },
      });
      regions.push({
        x: card.x, y: card.y, width: card.width, height: card.height,
        pngBase64: png.toString('base64'),
      });
    }
    return { source: 'independent_render', mask: 'opaque_pixels', regions };
  } finally {
    await context.close();
  }
}

module.exports = {
  dashboardCardReference,
  dashboardCardScene,
  renderDashboardCardMarkup,
};
