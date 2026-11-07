function installCloudReadiness() {
  const queries = new Map();
  let observed = false;
  let worker = null;
  let workerRoute = null;
  let selectedService = null;
  const key = (route, search, serviceId) => {
    const params = new URLSearchParams(search);
    return JSON.stringify([route, serviceId ?? null, params.get('range') || '24h', params.get('start_time'),
      params.get('end_time'), params.get('filters') || '']);
  };
  const current = () => key(location.pathname, location.search, selectedService);
  const rows = value => Number.isSafeInteger(value) && value >= 0;
  const message = value => typeof value === 'string' && value.trim() ? value.trim().slice(0, 500) : null;
  window.addEventListener('fla:cloud-service', ({ detail }) => {
    if (!detail || !(detail.serviceId === null || typeof detail.serviceId === 'string')) return;
    observed = true;
    if (selectedService === detail.serviceId) return;
    selectedService = detail.serviceId;
    queries.clear();
    worker = null;
    workerRoute = null;
  });
  window.addEventListener('fla:cloud-query', ({ detail }) => {
    if (!detail || !Number.isSafeInteger(detail.id) || typeof detail.section !== 'string') return;
    if (!['pending', 'success', 'error'].includes(detail.state)) return;
    observed = true;
    const route = key(detail.route, detail.search, detail.serviceId);
    const identity = `${route}:${detail.section}`;
    if ((queries.get(identity)?.id ?? -1) > detail.id) return;
    queries.set(identity, { id: detail.id, route, section: detail.section, state: detail.state,
      totalRows: rows(detail.totalRows) ? detail.totalRows : null,
      error: detail.state === 'error' ? message(detail.error) ?? (rows(detail.status) ? `HTTP ${detail.status}` : null) : null });
  });
  window.addEventListener('fla:cloud-worker', ({ detail }) => {
    if (!detail || key(detail.route, detail.search, detail.serviceId) !== current()) return;
    worker = {};
    workerRoute = current();
    for (const field of ['filesListed', 'filesFetched', 'filesCached', 'cdnHits', 'rowsScanned',
      'bytesScanned', 'clientElapsedMs', 'vcpuMs']) {
      const value = detail[field];
      worker[field] = typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
    }
  });
  window.__flaCloudReadiness = {
    snapshot() {
      const active = [...queries.values()].filter(query => query.route === current());
      const dashboard = active.find(query => query.section === 'dashboard.bundle');
      const pending = active.filter(query => query.state === 'pending').length;
      const errors = active.filter(query => query.state === 'error').length;
      const complete = active.filter(query => query.state === 'success').length;
      const dashboardRoute = location.pathname.replace(/\/$/, '') === '/dashboard';
      const namespace = ({ 'fastly-value': 'value', 'streaming': 'cmcd', 'assets-shield': 'assets' })[location.pathname.split('/')[1]]
        ?? location.pathname.split('/')[1];
      // Pages whose analytical reads are not named `<namespace>.*` or `api./api/<namespace>/*`.
      const routeScope = {
        '/rum': /^(?:rum-analytics|api\.\/api\/services\/[^/]+\/rum\/analytics)$/,
        '/admin/session-scoring': /^api\.\/api\/services\/[^/]+\/scoring(?:\/|$)/,
        '/admin/rum': /^api\.\/api\/services\/[^/]+\/rum\/(?:status|versions)$/,
        '/admin/share': /^api\.\/api\/admin\/share(?:\/|$)/,
        '/admin/usage-log': /^api\.\/api\/admin\/usage-log(?:\/|$)/,
      }[location.pathname.replace(/\/$/, '')];
      const analytical = active.filter(query => routeScope ? routeScope.test(query.section) : query.section.startsWith(`${namespace}.`)
        || query.section === `api./api/${namespace}`
        || query.section.startsWith(`api./api/${namespace}/`));
      const ready = pending === 0 && errors === 0 && (dashboardRoute
        ? dashboard?.state === 'success' && rows(dashboard.totalRows)
        : analytical.some(query => query.state === 'success'));
      const failures = active.filter(query => query.state === 'error').slice(0, 5)
        .map(query => ({ section: query.section, error: query.error }));
      return { source: 'cloud-query', observed, ready: Boolean(ready), pending, errors,
        complete, failures, totalRows: dashboard?.totalRows ?? null, worker: workerRoute === current() ? worker : null };
    },
  };
}

const CLOUD_READINESS_INIT_SCRIPT = `(${installCloudReadiness.toString()})()`;

function dashboardSnapshot() {
  const cloud = window.__flaCloudReadiness?.snapshot();
  if (cloud?.observed) return cloud;
  const total = document.querySelector('[data-testid="total-requests"][data-fresh="true"]');
  const value = total ? Number(total.textContent.replace(/,/g, '').trim()) : NaN;
  return { source: 'legacy', ready: Number.isSafeInteger(value) && value >= 0,
    totalRows: Number.isSafeInteger(value) && value >= 0 ? value : null,
    pending: 0, errors: 0, complete: total ? 1 : 0 };
}

const SECRET_PARAM = /((?:^|[?&;,\s])[\w.-]*(?:token|secret|key|passw(?:or)?d|cookie|session|auth|signature|credential)[\w.-]*=)[^&\s;,'"]+/gi;

// Failure text can echo request URLs or headers; secret-looking values never leave the browser.
function redactFailure(text) {
  return String(text).replace(/\bBearer\s+[^\s,;'"]+/gi, 'Bearer [redacted]')
    .replace(SECRET_PARAM, '$1[redacted]').slice(0, 500);
}

function routeLabel(page, label) {
  if (label) return redactFailure(label);
  try {
    return new URL(page.url()).pathname;
  } catch {
    return 'unknown route';
  }
}

function queryFailure(page, result, label) {
  const details = (Array.isArray(result.failures) ? result.failures : [])
    .map(failure => `${redactFailure(failure?.section ?? 'unknown section')}: ${redactFailure(failure?.error ?? 'no error message')}`);
  return new Error(`Current analytical query failed on ${routeLabel(page, label)}${details.length ? ` (${details.join('; ')})` : ''}`);
}

async function waitForSnapshot(page, snapshot, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let last;
  const timeoutError = () => {
    const count = value => (Number.isSafeInteger(value) ? value : 'unknown');
    const counts = last
      ? `pending=${count(last.pending)} complete=${count(last.complete)} missing=${last.ready ? 0 : 1}`
      : 'no snapshot';
    return new Error(`Timed out after ${timeoutMs}ms waiting for current analytical results on ${routeLabel(page, label)} (${counts})`);
  };
  for (;;) {
    let remaining = deadline - Date.now();
    if (remaining <= 0) throw timeoutError();
    let timer;
    let result;
    try {
      result = await Promise.race([
        page.evaluate(snapshot),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(timeoutError()), remaining);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
    last = result;
    remaining = deadline - Date.now();
    if (result?.errors > 0) throw queryFailure(page, result, label);
    if (result?.ready && remaining >= 0) return result;
    if (remaining <= 0) throw timeoutError();
    await new Promise(resolve => setTimeout(resolve, Math.min(100, remaining)));
  }
}

async function waitForDashboardResult(page, timeoutMs = 120_000, label) {
  return waitForSnapshot(page, dashboardSnapshot, timeoutMs, label);
}

async function waitForCloudReport(page, timeoutMs = 120_000, label) {
  return waitForSnapshot(page, () => window.__flaCloudReadiness?.snapshot(), timeoutMs, label);
}

async function waitForCloudIdle(page, timeoutMs = 120_000, label) {
  return waitForSnapshot(page, () => {
    const snapshot = window.__flaCloudReadiness?.snapshot();
    return { ...snapshot, ready: !snapshot?.pending };
  }, timeoutMs, label);
}

const CLOUD_ANALYTICAL_ROUTES = Object.freeze(['/dashboard', '/origin', '/security', '/performance',
  '/network', '/streaming', '/rum', '/sessions', '/usage', '/fastly-value', '/assets-shield', '/insights']);
const CLOUD_ADMIN_DATA_ROUTES = Object.freeze(['/admin/session-scoring', '/admin/rum',
  '/admin/share', '/admin/usage-log']);

async function waitForPageReadiness(page, route, timeoutMs = 120_000) {
  const pathname = new URL(route, 'https://redacted.invalid').pathname;
  if (pathname === '/dashboard') return waitForDashboardResult(page, timeoutMs);
  if (CLOUD_ANALYTICAL_ROUTES.includes(pathname) || CLOUD_ADMIN_DATA_ROUTES.includes(pathname)) return waitForCloudReport(page, timeoutMs);
  return null;
}

module.exports = {
  CLOUD_READINESS_INIT_SCRIPT, CLOUD_ANALYTICAL_ROUTES, CLOUD_ADMIN_DATA_ROUTES, dashboardSnapshot,
  waitForDashboardResult, waitForCloudReport, waitForCloudIdle, waitForPageReadiness, redactFailure,
};
