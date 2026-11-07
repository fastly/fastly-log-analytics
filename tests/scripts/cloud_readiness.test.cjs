const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const { CLOUD_READINESS_INIT_SCRIPT, waitForDashboardResult, waitForCloudReport, waitForCloudIdle } = require('../../scripts/lib/cloud_readiness.cjs');

function documentState(search = '?range=1h') {
  const listeners = {};
  const window = { addEventListener: (name, callback) => { listeners[name] = callback; } };
  const location = { pathname: '/dashboard', search };
  vm.runInNewContext(CLOUD_READINESS_INIT_SCRIPT, { window, location, URLSearchParams, Map, Number });
  return {
    state: () => JSON.parse(JSON.stringify(window.__flaCloudReadiness.snapshot())),
    location,
    selectService: serviceId => listeners['fla:cloud-service']?.({ detail: { serviceId } }),
    worker: detail => listeners['fla:cloud-worker']({ detail: {
      route: '/dashboard', search, ...detail,
    } }),
    emit: (detail) => listeners['fla:cloud-query']({ detail: {
      section: 'dashboard.bundle', route: '/dashboard', search, id: 1, totalRows: null, ...detail,
    } }),
  };
}

test('a same-route service switch rejects old-service results and worker statistics', () => {
  const doc = documentState();
  doc.selectService('service-a');
  doc.emit({ serviceId: 'service-a', state: 'success', totalRows: 42 });
  assert.equal(doc.state().ready, true);
  doc.worker({ serviceId: 'service-a', filesFetched: 3 });
  assert.equal(doc.state().worker.filesFetched, 3);
  doc.selectService('service-b');
  assert.equal(doc.state().ready, false);
  assert.equal(doc.state().worker, null);
  doc.emit({ id: 2, serviceId: 'service-a', state: 'success', totalRows: 42 });
  doc.worker({ serviceId: 'service-a', filesFetched: 5 });
  assert.equal(doc.state().ready, false);
  assert.equal(doc.state().worker, null);
  doc.emit({ id: 3, serviceId: 'service-b', state: 'success', totalRows: 0 });
  doc.worker({ serviceId: 'service-b', filesFetched: 0, filesCached: 3 });
  assert.equal(doc.state().ready, true);
  assert.equal(doc.state().worker.filesCached, 3);
});

test('requires a current completed query, never cached DOM or pending/error/stale range results', () => {
  const doc = documentState();
  assert.equal(doc.state().ready, false);
  doc.emit({ state: 'pending' });
  assert.equal(doc.state().pending, 1);
  doc.emit({ state: 'error' });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 2, state: 'pending' });
  doc.emit({ id: 1, state: 'success', totalRows: 999 });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 2, state: 'success', totalRows: 0 });
  assert.equal(doc.state().ready, true);
  assert.equal(doc.state().totalRows, 0);
  doc.location.search = '?range=6h';
  assert.equal(doc.state().ready, false);
});

test('missing row counts and malformed counts fail closed', () => {
  for (const totalRows of [null, undefined, -1, NaN, '500']) {
    const doc = documentState();
    doc.emit({ state: 'success', totalRows });
    assert.equal(doc.state().ready, false);
  }
});

test('a different filter result and an unfinished local API cannot certify a dashboard', () => {
  const doc = documentState('?range=1h&filters=%7B%22status%22%3A%7B%22mode%22%3A%22include%22%2C%22values%22%3A%5B%22200%22%5D%7D%7D');
  doc.emit({ state: 'success', totalRows: 123, search: '?range=1h' });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 2, state: 'success', totalRows: 10 });
  assert.equal(doc.state().ready, true);
  doc.emit({ id: 3, section: 'api./api/dashboard/bundle', state: 'pending' });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 3, section: 'api./api/dashboard/bundle', state: 'error' });
  assert.equal(doc.state().ready, false);
});

test('non-dashboard routes need their own successful analytical response, not bootstrap alone', () => {
  const doc = documentState();
  doc.location.pathname = '/sessions';
  doc.emit({ route: '/sessions', section: 'api./api/bootstrap', state: 'success' });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 2, route: '/sessions', section: 'api./api/sessions/list', state: 'pending' });
  assert.equal(doc.state().ready, false);
  doc.emit({ id: 2, route: '/sessions', section: 'api./api/sessions/list', state: 'success' });
  assert.equal(doc.state().ready, true);
});

test('nested admin pages require their own completed data namespace, not bootstrap or unrelated admin reads', () => {
  for (const [route, section] of [
    ['/admin/session-scoring', 'api./api/services/svc/scoring/analytics'],
    ['/admin/rum', 'api./api/services/svc/rum/status'],
    ['/admin/share', 'api./api/admin/share/status'],
    ['/admin/usage-log', 'api./api/admin/usage-log'],
  ]) {
    const doc = documentState('');
    doc.location.pathname = route;
    doc.emit({ route, section: 'api./api/admin/debug-settings', state: 'success' });
    assert.equal(doc.state().ready, false, route);
    doc.emit({ route, id: 2, section, state: 'pending' });
    assert.equal(doc.state().pending, 1, route);
    assert.equal(doc.state().ready, false, route);
    doc.emit({ route, id: 2, section, state: 'success' });
    assert.equal(doc.state().ready, true, route);
  }
});

function cspPage(snapshots, legacy = false) {
  let index = 0;
  return {
    waitForFunction: async () => {
      throw new EvalError('Content Security Policy blocks unsafe-eval');
    },
    evaluate: async callback => {
      const state = snapshots[Math.min(index++, snapshots.length - 1)];
      const marker = '[data-testid="total-requests"][data-fresh="true"]';
      const document = {
        querySelector: selector => selector === marker && state.ready
          ? { textContent: String(state.totalRows) } : null,
      };
      const window = legacy ? {} : { __flaCloudReadiness: { snapshot: () => state } };
      const result = vm.runInNewContext(`(${callback.toString()})()`, { window, document }, {
        contextCodeGeneration: { strings: false, wasm: false },
      });
      return result == null ? result : JSON.parse(JSON.stringify(result));
    },
  };
}

for (const [name, wait] of [['dashboard', waitForDashboardResult], ['report', waitForCloudReport]]) {
  test(`${name} query failures name the route and the redacted query error`, async () => {
    const document = documentState('?range=24h');
    document.emit({ state: 'pending', section: 'api./api/origin/aggregates' });
    document.emit({ state: 'error', section: 'api./api/origin/aggregates', status: 413,
      error: 'HTTP 413: query exceeds the browser scan budget; /api/file?token=sentinel-query-token Bearer sentinel-bearer' });
    const page = { ...cspPage([document.state()]), url: () => 'https://app.example/dashboard?session=sentinel-url-session' };
    await assert.rejects(wait(page, 1000), error => {
      assert.match(error.message, /query failed on \/dashboard \(api\.\/api\/origin\/aggregates: HTTP 413: query exceeds the browser scan budget/);
      assert.doesNotMatch(error.message, /sentinel/);
      return true;
    });
    await assert.rejects(wait(page, 1000, 'Origin (24h)'), /query failed on Origin \(24h\)/);
  });
}

test('configuration-only readiness does not require an invented analytical query', async () => {
  const snapshot = await waitForCloudIdle(cspPage([
    { observed: false, ready: false, pending: 0, errors: 0, complete: 0 },
  ]), 1000);
  assert.equal(snapshot.ready, true);
  assert.equal(snapshot.observed, false);
  assert.equal(snapshot.complete, 0);
});

for (const [name, wait] of [['dashboard', waitForDashboardResult], ['report', waitForCloudReport], ['configuration', waitForCloudIdle]]) {
  test(`CSP-safe ${name} readiness waits for real completion and accepts zero rows`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
    const page = cspPage([
      { observed: true, ready: false, pending: 1, totalRows: null },
      { observed: true, ready: true, pending: 0, totalRows: 0 },
    ]);
    const result = wait(page, 200);
    await new Promise(setImmediate);
    t.mock.timers.tick(100);
    const snapshot = await result;
    assert.equal(snapshot.ready, true);
    assert.equal(snapshot.totalRows, 0);
    assert.equal(snapshot.pending, 0);
  });

  test(`CSP-safe ${name} readiness times out without inventing success`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
    const page = cspPage([{ observed: true, ready: false, pending: 1, errors: 0, totalRows: 42 }]);
    const rejected = assert.rejects(wait(page, 40), /[Tt]imed out.*40/);
    await new Promise(setImmediate);
    t.mock.timers.tick(40);
    await rejected;
  });

  test(`CSP-safe ${name} readiness rejects a completed failed query without waiting for a timeout`, async () => {
    const page = cspPage([{ observed: true, ready: false, pending: 0, errors: 1 }]);
    await assert.rejects(wait(page, 1000), /query failed/i);
  });

  test(`CSP-safe ${name} readiness rejects completion arriving after its deadline`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
    const page = cspPage([{ observed: true, ready: true, totalRows: 42 }]);
    const evaluate = page.evaluate;
    page.evaluate = async callback => {
      await new Promise(resolve => setTimeout(resolve, 41));
      return evaluate(callback);
    };
    const rejected = assert.rejects(wait(page, 40), /[Tt]imed out.*40/);
    t.mock.timers.tick(41);
    await rejected;
  });

  test(`CSP-safe ${name} readiness bounds an unresponsive browser evaluation`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
    let finish;
    let outcome;
    const page = { evaluate: () => new Promise(resolve => { finish = resolve; }) };
    const result = wait(page, 40).then(
      () => { outcome = 'ready'; },
      error => { outcome = error.message; },
    );
    await new Promise(setImmediate);
    t.mock.timers.tick(40);
    await new Promise(setImmediate);
    try {
      assert.match(outcome ?? 'still waiting', /[Tt]imed out.*40/);
    } finally {
      finish({ observed: true, ready: true, totalRows: 42 });
      await result;
    }
  });
}

test('legacy dashboards still wait for their genuine fresh total marker under CSP', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
  const page = cspPage([{ ready: false, totalRows: 42 }, { ready: true, totalRows: 42 }], true);
  const result = waitForDashboardResult(page, 200);
  await new Promise(setImmediate);
  t.mock.timers.tick(100);
  const snapshot = await result;
  assert.equal(snapshot.source, 'legacy');
  assert.equal(snapshot.totalRows, 42);
});
