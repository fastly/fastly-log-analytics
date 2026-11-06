const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const { CLOUD_READINESS_INIT_SCRIPT, waitForDashboardResult } = require('../../scripts/lib/cloud_readiness.cjs');

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

test('legacy dashboards still require their genuine fresh total marker', async () => {
  let waited = false;
  const page = {
    waitForFunction: async () => { waited = true; },
    evaluate: async () => ({ source: 'legacy', ready: true, totalRows: 42 }),
  };
  assert.equal((await waitForDashboardResult(page, 25)).totalRows, 42);
  assert.equal(waited, true);
});
