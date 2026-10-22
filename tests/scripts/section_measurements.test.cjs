const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const { describeResponse, collectResponseMeasurements, budgets } = require('../../scripts/lib/section_measurements.cjs');

test('default false does not establish miss and missing timing is explicit', () => {
  const report = describeResponse({ _is_cached: false });
  assert.equal(report.components[0].cache_state, 'unknown');
  assert.ok(report.gaps.includes('response_cache_unknown'));
  assert.ok(report.gaps.includes('section_timings_missing'));
});

test('component hits cannot replay historical work as current execution', () => {
  const report = describeResponse({
    aggregates: { _is_cached: true, _section_timings: [{ section: 'top_n_rollup', time_ms: 1700 }] },
    top_bots: { _response_cache: { state: 'miss', prewarmer: false }, _section_timings: [{ section: 'bots', elapsed_ms: 33, source: 'fact', current_execution: true }] },
  });
  const aggregate = report.components.find(row => row.component === 'aggregates');
  const bots = report.components.find(row => row.component === 'top_bots');
  assert.equal(aggregate.cache_state, 'hit');
  assert.equal(aggregate.sections[0].current_execution, false);
  assert.equal(bots.cache_state, 'miss');
  assert.equal(bots.sections[0].source, 'fact');
  assert.equal(bots.sections[0].ms, 33);
  assert.equal(bots.prewarmer, false);
});

test('cache activity is not rollup source and invalid durations are not zero', () => {
  const report = describeResponse({ _section_timings: [
    { section: 'summary_cache_hit', time_ms: 1 },
    { section: 'connection_acquire', time_ms: 9 },
    ...[null, true, -1, NaN, Infinity, '12'].map(value => ({ section: 'scan', time_ms: value })),
  ] });
  const sections = report.components[0].sections;
  assert.equal(sections[0].source, 'unknown');
  assert.equal(sections[0].activity, 'cache');
  assert.equal(sections[1].activity, 'queue');
  for (const row of sections.slice(2)) assert.equal(row.ms, null);
  assert.equal('totals_ms' in report, false);
});

test('response output excludes queries credentials and data', () => {
  const report = describeResponse({ _debug_queries: [{ sql: 'SENSITIVE' }], headers: { Cookie: 'BEARER' }, data: { ip: '192.0.2.1' } });
  const text = JSON.stringify(report);
  for (const secret of ['SENSITIVE', 'BEARER', '192.0.2.1']) assert.equal(text.includes(secret), false);
});

test('approved budgets are shared with the sweep and not mislabeled as request budgets', () => {
  assert.equal(budgets.visible_results_p95_ms['24h'].unfiltered, 2000);
  assert.equal(budgets.visible_results_p95_ms['30d'].filtered, 30000);
  assert.equal(budgets.whole_request_p95_ms, null);
  assert.equal(budgets.interaction_feedback_p95_ms, null);
});

function response(path, payload, status = 200) {
  return { url: () => `https://example.com${path}`, status: () => status, json: async () => payload };
}

test('collector captures bundle and aggregate responses, preserves repeats and drains pending JSON', async () => {
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page);
  let complete;
  const parsed = new Promise(resolve => { complete = resolve; });
  page.emit('response', { ...response('/api/dashboard/bundle?token=SECRET', null), json: () => parsed });
  page.emit('response', response('/api/network/bundle', {}));
  page.emit('response', response('/api/cmcd/aggregates', {}));
  page.emit('response', response('/api/cmcd/aggregates', {}));
  page.emit('response', response('/api/bootstrap', {}));
  complete({ _is_cached: true });
  const rows = await collector.snapshot();
  assert.equal(rows.length, 4);
  assert.equal(rows.filter(row => row.endpoint === '/api/cmcd/aggregates').length, 2);
  assert.equal(rows.find(row => row.endpoint === '/api/dashboard/bundle').response.components[0].cache_state, 'hit');
  assert.equal(new Set(rows.map(row => row.sample_id)).size, 4);
  assert.equal(JSON.stringify(rows).includes('SECRET'), false);
  collector.close();
  assert.equal(page.listenerCount('response'), 0);
});

test('collector preserves HTTP and JSON failures as evidence gaps without leaking errors', async () => {
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page);
  page.emit('response', response('/api/origin/aggregates', { detail: 'SECRET' }, 503));
  page.emit('response', { ...response('/api/network/bundle', {}), json: async () => { throw new Error('PRIVATE'); } });
  const rows = await collector.snapshot();
  assert.equal(rows[0].status, 503);
  assert.ok(rows[0].gaps.includes('http_error'));
  assert.ok(rows[1].gaps.includes('response_json_unavailable'));
  assert.equal(JSON.stringify(rows).includes('PRIVATE'), false);
  assert.equal(JSON.stringify(rows).includes('SECRET'), false);
  collector.close();
});

test('snapshot freezes its response cohort before waiting', async () => {
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page);
  let complete;
  page.emit('response', { ...response('/api/dashboard/bundle', null), json: () => new Promise(resolve => { complete = resolve; }) });
  const snapshot = collector.snapshot();
  page.emit('response', { ...response('/api/network/bundle', null), json: () => new Promise(() => {}) });
  complete({});
  const rows = await snapshot;
  assert.equal(rows.length, 1);
  assert.notEqual(rows[0].response, null);
  collector.close();
});

test('a stalled response body cannot hang reporting or look complete', async () => {
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page);
  page.emit('response', { ...response('/api/dashboard/bundle', null), json: () => new Promise(() => {}) });
  const rows = await collector.snapshot({ timeoutMs: 0 });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].response, null);
  assert.ok(rows[0].gaps.includes('response_body_pending'));
  collector.close();
});

test('an ancestor cache hit suppresses all historical child execution', () => {
  const report = describeResponse({ _is_cached: true, aggregates: { _response_cache: { state: 'miss' }, _section_timings: [{ section: 'scan', time_ms: 900, current_execution: true }] } });
  assert.equal(report.components[1].sections[0].current_execution, false);
  const conflict = describeResponse({ _is_cached: true, _response_cache: { state: 'miss' }, _section_timings: [{ section: 'scan', time_ms: 900, current_execution: true }] });
  assert.equal(conflict.components[0].sections[0].current_execution, false);
});

test('collector bounds records and explicitly reports truncation', async () => {
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page, { maxRecords: 1 });
  page.emit('response', response('/api/cmcd/aggregates', {}));
  page.emit('response', response('/api/cmcd/aggregates', {}));
  const rows = await collector.snapshot();
  assert.equal(rows.length, 1);
  assert.ok(collector.gaps().includes('response_records_truncated'));
  collector.close();
});
