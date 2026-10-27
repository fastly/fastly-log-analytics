const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const fs = require('node:fs');
const { describeResponse, collectResponseMeasurements, measurePaintedAction, budgets } = require('../../scripts/lib/section_measurements.cjs');
const { summarizeRequests, metricDelta, summarizeSample, renderReport, ledger, finishAll, journey } = require('../../scripts/dev/profile_dashboard.cjs');

test('analyst journeys retain the fixture-owned, origin-guarded page even on failure', async () => {
  const directory = fs.mkdtempSync(require('node:path').join(require('node:os').tmpdir(), 'dashboard-journey-'));
  let closed = false;
  let detached = false;
  const page = {
    setDefaultTimeout() {}, setDefaultNavigationTimeout() {},
    async setViewportSize() {}, async addInitScript() {},
    async waitForResponse() {}, async goto() { throw new Error('navigation-stopped'); },
    async close() { closed = true; },
  };
  const context = Object.assign(new EventEmitter(), {
    async newPage() { throw new Error('unguarded-page-created'); },
    async addCookies() {}, async setExtraHTTPHeaders() {},
    async newCDPSession(target) {
      assert.equal(target, page);
      return { async send() { return { metrics: [] }; }, async detach() { detached = true; } };
    },
  });
  try {
    await assert.rejects(journey({}, context, 'https://example.test', '', directory,
      'masked-analyst', false, page), /navigation-stopped/);
    assert.equal(closed, false);
    assert.equal(detached, true);
    assert.equal(context.listenerCount('request'), 0);
  } finally {
    fs.rmSync(directory, { recursive: true });
  }
});

test('dashboard pilot retains failed requests, redirects and streams at cutoff', async () => {
  const context = new EventEmitter();
  const record = ledger(context);
  const first = { url: () => 'https://example.test/first', method: () => 'GET',
    resourceType: () => 'fetch', redirectedFrom: () => null, headers: () => ({}),
    timing: () => ({ responseEnd: 10 }), sizes: async () => ({ responseBodySize: 64 }) };
  const redirected = { ...first, url: () => 'https://example.test/redirected', redirectedFrom: () => first,
    failure: () => ({ errorText: 'net::ERR_ABORTED' }) };
  const stream = { ...first, url: () => 'https://example.test/api/log-extents/stream' };
  context.emit('request', first);
  context.emit('response', { request: () => first, status: () => 302, headers: () => ({}) });
  context.emit('requestfinished', first);
  record.setPhase('filter');
  context.emit('request', redirected);
  context.emit('requestfailed', redirected);
  context.emit('request', stream);
  await record.stop();
  const summary = summarizeRequests(record.rows);
  assert.deepEqual([summary.count, summary.finished, summary.failed, summary.open, summary.redirects], [3, 1, 1, 1, 1]);
  assert.equal(record.rows[1].phase, 'filter');
  assert.equal(record.rows[1].failure, 'net::ERR_ABORTED');
  assert.equal(record.rows[0].response_bytes, 64);
  assert.equal(context.listenerCount('request'), 0);
});

test('renderer measurements preserve separate durations and reject missing evidence', () => {
  const before = { TaskDuration: 1, ScriptDuration: 0.1, LayoutDuration: 0.2, RecalcStyleDuration: 0.3 };
  const after = { TaskDuration: 2, ScriptDuration: 0.6, LayoutDuration: 0.45, RecalcStyleDuration: 0.425 };
  assert.deepEqual(metricDelta(before, after), {
    TaskDuration: 1000, ScriptDuration: 500, LayoutDuration: 250, RecalcStyleDuration: 125,
  });
  assert.throws(() => metricDelta({}, after), /renderer_metric_missing/);
});

test('pilot report flags cross-path count mismatches without exporting raw private evidence', () => {
  const sample = {
    role: 'admin', traced: true, load_asserted_ms: 100, filter_asserted_ms: 200,
    deployed_footer_commit: 'abc1234', assertions: { country_count_consistent: false },
    initial: { country: 'PRIVATE_COUNTRY', count: 40 }, filtered: { count: 30 },
    load_renderer_ms: {}, filter_renderer_ms: {},
    browser_performance: { navigation: [{ name: 'PRIVATE_ORIGIN' }], paint: [],
      entries: [{ type: 'longtask', duration: 60 }, { type: 'event', start: 10, processingStart: 30 }] },
    requests: { count: 2, finished: 1, failed: 0, open: 1, redirects: 0, http_errors: 0,
      endpoints: [{ endpoint: '/PRIVATE_SERVICE', url: 'PRIVATE_ORIGIN' }] },
    diagnostics: [{ phase: 'load', raw_sql: 'PRIVATE_SQL',
      components: [
        { component: 'aggregates', cache_state: 'hit', execution_reused: true,
          sections: [{ section: 'wide_temp_create', ms: 987654321, current_execution: false }] },
        { component: 'top_bots', cache_state: 'hit', execution_reused: true },
        { component: 'response', cache_state: 'bypass', execution_reused: false,
          sections: [{ section: 'bundle:primary_checkout', ms: 420, current_execution: null }] },
      ] }],
  };
  const summary = summarizeSample(sample);
  const report = renderReport([summary], { checked_out_commit: 'def5678' });
  assert.equal(summary.browser.maximum_observed_input_delay_ms, 20);
  assert.equal(summary.browser.long_task_total_ms, 60);
  assert.equal(summary.requests.open_at_cutoff, 1);
  assert.equal(summary.components[0].execution_reused, true);
  assert.match(report, /FAILED or unverified/);
  assert.match(report, /not an overhead estimate or a before\/after improvement/);
  assert.match(report, /\| admin \| yes \| load \| 420 \| unknown \| unknown \| unknown \|/);
  assert.equal(report.includes('987654321'), false);
  for (const secret of ['PRIVATE_COUNTRY', 'PRIVATE_ORIGIN', 'PRIVATE_SERVICE', 'PRIVATE_SQL']) {
    assert.equal(JSON.stringify(summary).includes(secret), false);
    assert.equal(report.includes(secret), false);
  }
});

test('pilot cleanup continues after an individual artifact or browser cleanup failure', async () => {
  const seen = [];
  await assert.rejects(finishAll([
    async () => { seen.push('trace'); throw new Error('trace_write_failed'); },
    async () => { seen.push('browser'); },
    async () => { seen.push('ledger'); },
  ]), AggregateError);
  assert.deepEqual(seen, ['trace', 'browser', 'ledger']);
});

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

test('list sidecars retain owner stamps but never promote ancestor reuse', () => {
  for (const reuse of ['none', 'hit', 'follower']) {
    const metadata = { state: 'bypass', prewarmer: false };
    if (reuse === 'hit') metadata.state = 'hit';
    if (reuse === 'follower') Object.assign(metadata, { state: 'unknown', single_flight: 'follower' });
    const report = describeResponse({
      _response_cache: metadata,
      pop_health: [{ pop: 'PRIVATE_ROW_VALUE' }],
      _component_observations: {
        pop_health: {
          _response_cache: { state: 'bypass', prewarmer: false },
          _section_timings: [{ section: 'pop_health', time_ms: 3, source: 'live', current_execution: true }],
        },
      },
    });
    const pop = report.components.find(row => row.component === 'pop_health');
    assert.equal(pop.cache_state, 'bypass');
    assert.equal(pop.lookup_current, reuse === 'none');
    assert.equal(pop.sections[0].current_execution, reuse === 'none');
    assert.equal(pop.gaps.includes('component_observation_envelope_missing'), false);
    assert.ok(pop.gaps.includes('reader_coverage_missing'));
    assert.ok(pop.gaps.includes('history_evidence_missing'));
    assert.equal(JSON.stringify(report).includes('PRIVATE_ROW_VALUE'), false);
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

test('collector regex stays coupled to every section_sweep ENDPOINTS path', async () => {
  // Guards against a silent selector regression: if section_sweep adds or
  // renames a swept endpoint whose path no longer ends in /aggregates or
  // /bundle, the collector would stop capturing it and the pilot would
  // produce zero rows for that section. cwd is the repo root under node --test.
  const sweepSrc = fs.readFileSync('scripts/dev/section_sweep.py', 'utf8');
  const endpointsBlock = sweepSrc.slice(
    sweepSrc.indexOf('ENDPOINTS'),
    sweepSrc.indexOf('ENDPOINTS') + 2000,
  );
  const paths = [...endpointsBlock.matchAll(/"(\/api\/[^"]+)"/g)]
    .map((match) => match[1].split('?')[0])
    .filter((path) => path.startsWith('/api/'));
  assert.ok(paths.length >= 6, `expected >=6 swept endpoints, got ${paths.length}`);

  for (const path of paths) {
    const page = new EventEmitter();
    const collector = collectResponseMeasurements(page);
    page.emit('response', response(path, {}));
    const rows = await collector.snapshot();
    assert.equal(
      rows.length,
      1,
      `collector regex dropped swept endpoint ${path}`,
    );
    assert.equal(rows[0].endpoint, path);
    collector.close();
  }

  // Control: a non-swept endpoint must still be excluded.
  const page = new EventEmitter();
  const collector = collectResponseMeasurements(page);
  page.emit('response', response('/api/bootstrap', {}));
  const rows = await collector.snapshot();
  assert.equal(rows.length, 0, 'collector must not capture /api/bootstrap');
  collector.close();
});

// --- measurePaintedAction: synchronous argument-validation guard ------------

const GUARD_MESSAGE =
  'Explicit action target, meaningful text markers and bounded timeout are required';

function guardPage() {
  // Any evaluate/page interaction means the guard failed to fire first.
  return {
    evaluate: async () => {
      throw new Error('page.evaluate must not run when validation fails');
    },
    waitForFunction: async () => {
      throw new Error('page.waitForFunction must not run when validation fails');
    },
    isClosed: () => false,
  };
}

function validOptions(overrides = {}) {
  return {
    target: 'button.apply-filter',
    eventType: 'click',
    feedback: { selector: '.toast', text: 'Filter applied' },
    results: [{ selector: 'table tbody tr', text: '42' }],
    timeoutMs: 5000,
    ...overrides,
  };
}

const noopAction = async () => {};

test('measurePaintedAction rejects an empty or non-string action target', async () => {
  for (const target of ['', null, undefined, 42, {}]) {
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ target }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
  }
});

test('measurePaintedAction rejects an unsupported event type', async () => {
  for (const eventType of ['mouseover', 'focus', '', null]) {
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ eventType }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
  }
});

test('measurePaintedAction rejects missing, empty or oversized results markers', async () => {
  const oversized = Array.from({ length: 33 }, (_, i) => ({
    selector: `tr:nth-child(${i + 1})`,
    text: String(i),
  }));
  for (const results of [undefined, null, 'rows', [], oversized]) {
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ results }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
  }
});

test('measurePaintedAction rejects markers with an empty selector or blank text', async () => {
  const badMarkers = [
    { selector: '', text: 'ok' },
    { selector: 'tr', text: '   ' },
    { selector: 'tr', text: '' },
    { selector: 42, text: 'ok' },
    { selector: 'tr' },
    'not-an-object',
  ];
  for (const marker of badMarkers) {
    // Invalid as a results marker.
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ results: [marker] }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
    // Invalid as the feedback marker (prepended into the marker list).
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ feedback: marker }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
  }
});

test('measurePaintedAction rejects a non-finite or out-of-range timeout', async () => {
  for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 60001, '5000']) {
    await assert.rejects(
      () => measurePaintedAction(guardPage(), validOptions({ timeoutMs }), noopAction),
      new RegExp(GUARD_MESSAGE),
    );
  }
});

// --- measurePaintedAction: fake-page post-evaluate tail --------------------

// Builds a fake Puppeteer-like page. The first string-argument evaluate call
// returns the canned snapshot; the second returns undefined (cleanup). Object
// -argument evaluate calls (registration) return undefined.
function paintPage(state) {
  let snapshotServed = false;
  return {
    evaluate: async (_fn, arg) => {
      if (typeof arg === 'string') {
        if (!snapshotServed) {
          snapshotServed = true;
          return state;
        }
        return undefined; // cleanup
      }
      return undefined; // registration
    },
    waitForFunction: async () => true,
    isClosed: () => false,
  };
}

test('measurePaintedAction reports observed results when every marker paints', async () => {
  const state = {
    event: 100,
    paints: [140, 180],
    supported: true,
    visibilitySupported: true,
    inputDelay: 5,
    finished: true,
  };
  let actionRan = false;
  const result = await measurePaintedAction(
    paintPage(state),
    validOptions(),
    async () => {
      actionRan = true;
    },
  );
  assert.equal(actionRan, true);
  assert.equal(result.qualification, 'incomplete');
  assert.equal(result.feedback.status, 'observed');
  assert.equal(result.feedback.elapsed_ms, 40);
  assert.equal(result.results.status, 'observed');
  assert.equal(result.results.elapsed_ms, 80);
  assert.equal(result.results.panels_observed, 1);
  assert.equal(result.results.panels_required, 1);
  assert.ok(!result.gaps.includes('visible_results_paint_unobserved'));
});

test('measurePaintedAction flags unobserved results when a marker never paints', async () => {
  const state = {
    event: 100,
    paints: [140, null],
    supported: true,
    visibilitySupported: true,
    inputDelay: 5,
    finished: true,
  };
  const result = await measurePaintedAction(paintPage(state), validOptions(), noopAction);
  assert.equal(result.qualification, 'incomplete');
  assert.equal(result.results.status, 'unobserved');
  assert.equal(result.results.elapsed_ms, null);
  assert.equal(result.results.panels_observed, 0);
  assert.equal(result.results.panels_required, 1);
  assert.ok(result.gaps.includes('visible_results_paint_unobserved'));
});

test('measurePaintedAction never leaks private markers through serialization', async () => {
  const state = {
    event: 100,
    paints: [140, 180],
    supported: true,
    visibilitySupported: true,
    inputDelay: 5,
    finished: true,
  };
  const options = validOptions({
    target: 'button.PRIVATE_TARGET',
    feedback: { selector: '.PRIVATE_FEEDBACK', text: 'PRIVATE_FEEDBACK_TEXT' },
    results: [{ selector: 'table.PRIVATE_RESULT', text: 'PRIVATE_RESULT_TEXT' }],
  });
  const result = await measurePaintedAction(paintPage(state), options, noopAction);
  const serialized = JSON.stringify(result);
  for (const secret of [
    'PRIVATE_TARGET',
    'PRIVATE_FEEDBACK',
    'PRIVATE_FEEDBACK_TEXT',
    'PRIVATE_RESULT',
    'PRIVATE_RESULT_TEXT',
  ]) {
    assert.equal(serialized.includes(secret), false, `must not leak ${secret}`);
  }
});
