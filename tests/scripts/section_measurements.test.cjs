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

test('list-shaped components remain explicit unknowns without exposing rows', () => {
  for (const state of ['bypass', 'hit']) {
    for (const rows of [[], [{ pop: 'PRIVATE_ROW_VALUE', requests: 7 }]]) {
      const report = describeResponse({ _response_cache: { state }, pop_health: rows });
      const pop = report.components.find(row => row.component === 'pop_health');
      assert.ok(pop);
      assert.equal(pop.cache_state, 'unknown');
      assert.deepEqual(pop.sections, []);
      assert.deepEqual(pop.reader_coverage, []);
      assert.equal(pop.history, null);
      assert.ok(pop.gaps.includes('component_observation_envelope_missing'));
      assert.ok(pop.gaps.includes('response_cache_unknown'));
      assert.equal(pop.execution_reused, state === 'hit');
      assert.equal(JSON.stringify(report).includes('PRIVATE_ROW_VALUE'), false);
    }
  }
});

test('absent and unrequested list components are not fabricated', () => {
  for (const payload of [{}, { pop_health: null }]) {
    assert.deepEqual(describeResponse(payload).components.map(row => row.component), ['response']);
  }
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

test('reader observations stay component-owned and inherited hits are historical', () => {
  const observation = { reader: 'dashboard.conn_requests_histogram', minimum_window_hours: 2, outcome: 'served', live_top_up: 'failed', attempts: [{ bundle: 'security_conn_reuse', reason: 'planned', closed_hours: 4, covered_hours: 2, path: 'PRIVATE' }], sql: 'PRIVATE' };
  const report = describeResponse({
    _response_cache: { state: 'hit' },
    aggregates: { _response_cache: { state: 'miss' }, _reader_coverage: [observation] },
    top_bots: { _reader_coverage: [{ reader: 'dashboard.ngwaf_bots', minimum_window_hours: 2, outcome: 'served' }] },
  });
  const aggregate = report.components.find(row => row.component === 'aggregates');
  assert.equal(aggregate.reader_coverage[0].current_observation, false);
  assert.equal(aggregate.reader_coverage[0].observation.attempts[0].covered_hours, 2);
  assert.ok(report.gaps.includes('reader_live_top_up_failed'));
  assert.ok(report.gaps.includes('history_evidence_missing'));
  assert.equal(report.gaps.includes('reader_coverage_missing'), false);
  assert.equal(JSON.stringify(report).includes('PRIVATE'), false);
  assert.equal(observation.attempts[0].path, 'PRIVATE');
});

test('history report flags an uncovered raw prefix without certifying continuity', () => {
  const report = describeResponse({
    _response_cache: { state: 'miss' },
    _history: {
      extent_source: 'live_query',
      extent_rows: 5,
      window_rows: 5,
      window_count_source: 'rollup',
      window_start: '2026-09-25T05:00:00+00:00',
      earliest_event_at: '2026-09-25T05:38:42+00:00',
      raw_extent_reaches_window_start: false,
    },
  });
  assert.ok(report.gaps.includes('history_raw_prefix_uncovered'));
  assert.ok(report.gaps.includes('history_continuity_unverified'));
  assert.equal(report.components[0].history.observation.raw_extent_reaches_window_start, false);
});

test('malformed reader observations cannot become current coverage or leak arbitrary timestamps', () => {
  for (const raw of [
    [{ reader: 'dashboard.ngwaf_bots', minimum_window_hours: true }],
    [{ reader: 'dashboard.ngwaf_bots', minimum_window_hours: 2, window_start: 'PRIVATE' }],
    [{ reader: 'dashboard.ngwaf_bots', minimum_window_hours: 2, attempts: [{ bundle: 'ngwaf_bots', covered_hours: -1 }] }],
  ]) {
    const report = describeResponse({ _response_cache: { state: 'miss' }, _reader_coverage: raw });
    assert.deepEqual(report.components[0].reader_coverage, []);
    assert.ok(report.gaps.includes('reader_coverage_invalid'));
    assert.equal(JSON.stringify(report).includes('PRIVATE'), false);
  }
});

test('approved budgets are shared with the sweep and not mislabeled as request budgets', () => {
  assert.equal(budgets.visible_results_p95_ms['24h'].unfiltered, 2000);
  assert.equal(budgets.visible_results_p95_ms['30d'].filtered, 30000);
  assert.equal(budgets.whole_request_p95_ms, null);
  assert.equal(budgets.whole_request_budget_policy, 'diagnostic_only');
  assert.equal(budgets.interaction_feedback_p95_ms, 200);
  assert.equal(budgets.interaction_feedback_target_p95_ms, 100);
  assert.equal(budgets.interaction_feedback_measure, 'event_arrival_to_meaningful_paint');
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
