const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const { isIP } = require('node:net');
const { chromium, request, expect } = require('../../frontend/node_modules/@playwright/test');
const { adminGatewayContextOptions } = require('../lib/admin_gateway_client.cjs');
const { withAnalystUsers } = require('../lib/analyst_fixture.cjs');
const { describeResponse } = require('../lib/section_measurements.cjs');

function check(value, code) {
  if (!value) throw new Error(code);
}

function write(file, value) {
  fs.writeFileSync(file, JSON.stringify(value, null, 2), { flag: 'wx', mode: 0o600 });
}

function summarizeRequests(rows) {
  return {
    count: rows.length,
    finished: rows.filter(row => row.state === 'finished').length,
    failed: rows.filter(row => row.state === 'failed').length,
    open: rows.filter(row => row.state === 'open').length,
    redirects: rows.filter(row => row.redirected_from !== null).length,
    http_errors: rows.filter(row => row.status >= 400).length,
    phases: [...new Set(rows.map(row => row.phase))].map(phase =>
      ({ phase, count: rows.filter(row => row.phase === phase).length })),
    resources: [...new Set(rows.map(row => row.resource))].map(resource => {
      const cohort = rows.filter(row => row.resource === resource);
      const known = cohort.filter(row => Number.isFinite(row.response_bytes) && row.response_bytes >= 0);
      return { resource, count: cohort.length, known_bytes: known.reduce((sum, row) => sum + row.response_bytes, 0),
        unknown_bytes: cohort.length - known.length };
    }),
    endpoints: [...new Set(rows.map(row => row.endpoint))].map(endpoint => {
      const cohort = rows.filter(row => row.endpoint === endpoint);
      return { endpoint, count: cohort.length, durations_ms: cohort.map(row => row.duration_ms),
        response_bytes: cohort.map(row => row.response_bytes) };
    }),
  };
}

function ledger(context) {
  const rows = [];
  const requests = new Map();
  const pending = new Set();
  let phase = 'load';
  const started = performance.now();
  const onRequest = req => {
    const url = new URL(req.url());
    const row = {
      id: rows.length + 1, phase, endpoint: url.pathname, url: req.url(), method: req.method(),
      resource: req.resourceType(), start_ms: performance.now() - started,
      redirected_from: requests.get(req.redirectedFrom())?.id ?? null,
      state: 'open', status: null, duration_ms: null, response_bytes: null,
      page_load_id: req.headers()['x-page-load-id'] ?? null,
      request_data: /^\/api\/dashboard\//.test(url.pathname) && req.method() === 'POST'
        ? req.postDataJSON() : null,
    };
    requests.set(req, row);
    rows.push(row);
  };
  const onResponse = response => {
    const row = requests.get(response.request());
    if (!row) return;
    row.status = response.status();
    row.headers_ms = performance.now() - started;
    const headers = response.headers();
    row.request_id = headers['x-request-id'] ?? null;
    row.traceparent = headers.traceparent ?? null;
    if (/^\/api\/dashboard\/(?:bundle|aggregates)$/.test(row.endpoint)) {
      const task = response.json().then(body => {
        row.diagnostics = describeResponse(body);
        row.debug_counts = Object.fromEntries(
          ['_debug_queries', '_debug_calls', '_debug_postgres', '_debug_sqlite']
            .map(key => [key, Array.isArray(body[key]) ? body[key].length : null]),
        );
        row.result = body;
      }, () => { row.body_unavailable = true; });
      pending.add(task);
      task.finally(() => pending.delete(task));
    }
  };
  const onFinished = req => {
    const row = requests.get(req);
    if (!row) return;
    row.state = 'finished';
    row.duration_ms = performance.now() - started - row.start_ms;
    row.timing = req.timing();
    const task = req.sizes().then(sizes => {
      if (Number.isFinite(sizes.responseBodySize) && sizes.responseBodySize >= 0) {
        row.response_bytes = sizes.responseBodySize;
      } else {
        row.sizes_invalid = true;
      }
    },
      () => { row.sizes_unavailable = true; });
    pending.add(task);
    task.finally(() => pending.delete(task));
  };
  const onFailed = req => {
    const row = requests.get(req);
    if (!row) return;
    row.state = 'failed';
    row.failure = req.failure()?.errorText;
    row.duration_ms = performance.now() - started - row.start_ms;
  };
  context.on('request', onRequest);
  context.on('response', onResponse);
  context.on('requestfinished', onFinished);
  context.on('requestfailed', onFailed);
  return {
    rows, setPhase(value) { phase = value; },
    async stop() {
      context.off('request', onRequest);
      context.off('response', onResponse);
      context.off('requestfinished', onFinished);
      context.off('requestfailed', onFailed);
      await Promise.all([...pending]);
    },
  };
}

async function browserMetrics(cdp) {
  return Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(row => [row.name, row.value]));
}

function metricDelta(before, after) {
  for (const key of ['TaskDuration', 'ScriptDuration', 'LayoutDuration', 'RecalcStyleDuration']) {
    check(Number.isFinite(before[key]) && Number.isFinite(after[key]), 'renderer_metric_missing');
  }
  return Object.fromEntries(
    ['TaskDuration', 'ScriptDuration', 'LayoutDuration', 'RecalcStyleDuration']
      .map(key => [key, (after[key] - before[key]) * 1000]),
  );
}

function summarizeSample(sample) {
  const entries = sample.browser_performance.entries;
  const tasks = entries.filter(entry => entry.type === 'longtask');
  const events = entries.filter(entry => entry.type === 'event');
  return {
    role: sample.role, traced: sample.traced,
    load_asserted_ms: sample.load_asserted_ms, filter_asserted_ms: sample.filter_asserted_ms,
    deployed_footer_commit: sample.deployed_footer_commit,
    assertions: sample.assertions,
    country_counts: { initial: sample.initial.count, filtered: sample.filtered.count },
    renderer_ms: { load: sample.load_renderer_ms, filter: sample.filter_renderer_ms },
    browser: {
      fcp_ms: sample.browser_performance.paint.find(entry => entry.name === 'first-contentful-paint')?.startTime ?? null,
      long_tasks: tasks.length, long_task_total_ms: tasks.reduce((sum, entry) => sum + entry.duration, 0),
      longest_task_ms: tasks.length ? Math.max(...tasks.map(entry => entry.duration)) : null,
      observed_events: events.length,
      maximum_observed_input_delay_ms: events.length
        ? Math.max(...events.map(entry => entry.processingStart - entry.start)) : null,
      layout_shift_sum: entries.filter(entry => entry.type === 'layout-shift' && !entry.recentInput)
        .reduce((sum, entry) => sum + entry.value, 0),
    },
    requests: {
      count: sample.requests.count, finished: sample.requests.finished, failed: sample.requests.failed,
      open_at_cutoff: sample.requests.open, redirects: sample.requests.redirects,
      http_errors: sample.requests.http_errors,
      phases: sample.requests.phases,
      resources: sample.requests.resources,
      dashboard: sample.requests.endpoints.filter(entry => entry.endpoint === '/api/dashboard/bundle'),
    },
    diagnostics: sample.diagnostics.map(response => ({
      phase: response.phase, records: response.debug_counts,
      components: response.components.map(component => ({
        component: component.component, cache_state: component.cache_state,
        execution_reused: component.execution_reused,
        sections: (component.sections ?? []).filter(section => [
          'bundle:primary_checkout', 'bundle:second_checkout', 'bundle:aggregates', 'bundle:top_bots',
          'wide_temp_create', 'waf_sig_ind_explode', 'edge_score_reason_ind_explode',
          'top_bots:temp_table_create',
        ].includes(section.section)).map(section => ({
          section: section.section, ms: section.ms, current_execution: section.current_execution,
        })),
      })),
    })),
    components: sample.diagnostics.flatMap(response => response.components.map(component => ({
      phase: response.phase, component: component.component, cache_state: component.cache_state,
      execution_reused: component.execution_reused,
    }))),
  };
}

function renderReport(samples, conditions) {
  const countsAgree = samples.length > 0
    && samples.every(sample => sample.assertions.country_count_consistent);
  const ms = value => Number.isFinite(value) ? Math.round(value) : 'unknown';
  const cachedCheckouts = samples.flatMap(sample => sample.diagnostics.flatMap(response => {
    if (!['aggregates', 'top_bots'].every(name => response.components.some(component =>
      component.component === name && component.cache_state === 'hit'))) return [];
    const sections = response.components.find(component => component.component === 'response')?.sections ?? [];
    const wall = name => ms(sections.find(section => section.section === name)?.ms);
    return [`| ${sample.role} | ${sample.traced ? 'yes' : 'no'} | ${response.phase} | ${wall('bundle:primary_checkout')} | ${wall('bundle:second_checkout')} | ${wall('bundle:aggregates')} | ${wall('bundle:top_bots')} |`];
  }));
  return `# Remote-standard dashboard pilot

Collection: ${samples.length} samples. Country-count consistency: ${countsAgree ? 'passed' : 'FAILED or unverified'}.
Checkout: ${conditions?.checked_out_commit ?? 'unavailable'}; deployed administrator commit: ${samples[0]?.deployed_footer_commit ?? 'unavailable'}.
Viewport: 1440 x 1000; no CPU/network throttling; one fixed hour.
Window: ${conditions?.window?.start ?? 'unavailable'} to ${conditions?.window?.end ?? 'unavailable'}.

| Role | Renderer trace | Load assertion ms | Filter assertion ms | Requests | Country: initial / filtered |
|---|---|---:|---:|---:|---:|
${samples.map(sample => `| ${sample.role} | ${sample.traced ? 'yes' : 'no'} | ${Math.round(sample.load_asserted_ms)} | ${Math.round(sample.filter_asserted_ms)} | ${sample.requests.count} | ${sample.country_counts.initial} / ${sample.country_counts.filtered} |`).join('\n')}

| Role | Renderer trace | FCP ms | Long tasks | Total long-task ms | Longest ms | Maximum observed input delay ms |
|---|---|---:|---:|---:|---:|---:|
${samples.map(sample => `| ${sample.role} | ${sample.traced ? 'yes' : 'no'} | ${ms(sample.browser.fcp_ms)} | ${sample.browser.long_tasks} | ${ms(sample.browser.long_task_total_ms)} | ${ms(sample.browser.longest_task_ms)} | ${ms(sample.browser.maximum_observed_input_delay_ms)} |`).join('\n')}

Responses with both component owners reporting cache hits:

| Role | Renderer trace | Phase | Primary checkout ms | Second checkout ms | Aggregates branch ms | Bots branch ms |
|---|---|---|---:|---:|---:|---:|
${cachedCheckouts.join('\n') || '| No confirmed paired cache hits | - | - | unknown | unknown | unknown | unknown |'}

Checkout and branch sections are observed wall times, not server CPU. Missing
current-execution owner stamps remain unknown; historical child work is excluded
from this table. Do not sum overlapping branch durations.

## Prioritized evidence

1. **Correctness first (high confidence observation, cause unverified).**
   Compare the initial country's count with the exact same country after the
   filtered query under identical bounds. Counts are in the table above.
   The rollup/raw paths and retained derivatives need investigation; no repair
   or data deletion is authorized by this harness.
2. **Backend checkout and filtered materialization (measured costs, proposed fixes).**
   Per-response owner stamps and selected wall sections are in summary.json.
   Filtered wide-temp creation and independent bot-temp creation are candidates;
   preserve filtered semantics rather than replacing them with unscoped rollups.
   Cache peeking before bundle checkout is another narrow candidate.
   Relevant code: backend/routers/dashboard.py:153,
   backend/repositories/security.py:669 and
   backend/repositories/dashboard.py:293.
3. **Browser execution (observed, stacks require DevTools inspection).**
   Long tasks, renderer task/script/layout/style durations, FCP, observed input
   delay and known resource bytes are in summary.json. Do not assign a long
   task to React, Plotly or MapLibre without examining the native trace stack.

## Exercised coverage and limits

Each journey records document load, the Add Custom Filter dialog, Country
selection, application of the filter and three seconds of bounded observation.
Every measured-context browser request is retained, including status, redirects,
failure/cancellation events, pending streams, timings and available body sizes.
Negative/unavailable size values are unknown, not zero-byte transfers. The full
ledger permits request-shape and waterfall inspection; no broad duplicate-fetch
or oversized-response cause is established merely from request counts.
Navigation away, hidden-tab behavior, field suggestions and other controls
remain untested. Login/TOS, tenant/admin denials and owned analyst teardown use
the existing fixture; this is not a full RBAC or PII audit.

API/SQL debug envelopes and component cache stamps are observed live, not merely
present in source. Record counts are not current-operation counts: cached SQL,
overlapping sections and synthesized usage entries need owner evidence.
Browser page IDs are captured; request-ID/traceparent response headers, emitted
OTel span delivery, server logs, external-I/O completeness and background-job
attribution are not established by this capture. Use only separately authorized
existing observability access to close those gaps.

These are result-assertion wall times, not paint timings or INP. Plotly data totals
were checked against response series; neither visibility nor data assertions prove
correct pixels were painted. Event Timing observes entries at least 16 ms only.
Layout-shift sums are not session-window CLS. Renderer duration metrics overlap;
do not sum them or infer backend CPU.

Each condition has n=1. Traced then control is confounded by order, cache and
network conditions, not an overhead estimate or a before/after improvement.
Owner cache stamps are retained per component; cached child timings are historical.
Open streams at cutoff are not completed requests or failures. Authentication,
ownership probes and application-diagnostic reads are outside the measured journey.

Load the *.chrome.json files in Chrome DevTools Performance for renderer stacks,
layout and paint inspection. The *.playwright.zip files are separate action traces.
Raw requests, bodies and detailed measurements are owner-only *.private.json files.
No telemetry-service query, deployment, cache clear, retained-data change or traffic
seeding is performed. Exported OTel, server logs, external-I/O completeness and
background-job attribution remain unverified; missing evidence is not zero work.

Next: reconcile the same-window country-count mismatch before claiming correctness
or changing rollup behavior. Separately, cached bundle connection checkout is a
measured candidate. Local regression results and deployment status must be recorded
separately; this capture does not establish a live optimization gain.

Repeat from the owned checkout using the existing operator authentication
environment (never copy credentials into this report). Set PILOT_OUTPUT to a new
absolute directory below an owner-only parent before running:

\`\`\`sh
PILOT_END_TIME="${conditions?.window?.end ?? ''}" \\
node scripts/dev/profile_dashboard.cjs
\`\`\`

Exit 2 means collection and ownership cleanup completed but the cross-path
correctness assertion failed. Exit 1 means collection or cleanup failed; inspect
private failure evidence. Check conditions.json and the ownership journal.
`;
}

async function finishAll(actions) {
  const errors = [];
  for (const action of actions) {
    try { await action(); } catch (error) { errors.push(error); }
  }
  if (errors.length) throw new AggregateError(errors, 'pilot_cleanup_failed');
}

async function assertResult(page, response, filter) {
  check(response.ok(), 'dashboard_http_error');
  const body = await response.json();
  const aggregates = body.aggregates ?? body;
  check(Array.isArray(aggregates.time_series) && aggregates.time_series.length > 0, 'series_missing');
  check(aggregates.data && typeof aggregates.data === 'object', 'dimensions_missing');
  const countries = aggregates.data.country?.top;
  check(Array.isArray(countries) && countries.length > 0, 'country_rows_missing');
  check(countries.every(row => Number.isFinite(row.count) && row.count > 0), 'country_counts_invalid');
  if (filter) check(countries.every(row => row.value === filter), 'filter_result_mismatch');
  const country = filter ?? String(countries[0].value);
  const expectedTotal = aggregates.time_series.reduce((sum, point) => sum + point.value, 0);
  check(expectedTotal > 0, 'series_counts_invalid');
  await expect.poll(() => page.locator('.js-plotly-plot').evaluateAll((elements, total) => {
    return elements.some(element => {
      const bars = element.data?.filter(trace => trace.type === 'bar') ?? [];
      return bars.length > 0 && bars.reduce((sum, trace) =>
        sum + trace.y.reduce((subtotal, value) => subtotal + value, 0), 0) === total;
    });
  }, expectedTotal), { timeout: 60000 }).toBe(true);
  return { country, count: countries.find(row => String(row.value) === country).count,
    series_points: aggregates.time_series.length };
}

async function journey(browser, context, origin, target, directory, role, traced, fixturePage) {
  const page = fixturePage ?? await context.newPage();
  page.setDefaultTimeout(60000);
  page.setDefaultNavigationTimeout(60000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await context.addCookies([{ name: 'fla.debugResponses', value: '1', url: origin, secure: true }]);
  await context.setExtraHTTPHeaders({ 'x-debug-responses': '1' });
  await page.addInitScript(() => {
    if (window.__pilotEntries) return;
    window.__pilotEntries = [];
    for (const type of ['longtask', 'event', 'layout-shift', 'largest-contentful-paint']) {
      if (!PerformanceObserver.supportedEntryTypes.includes(type)) continue;
      new PerformanceObserver(list => {
        for (const entry of list.getEntries()) {
          window.__pilotEntries.push({
            type: entry.entryType, start: entry.startTime, duration: entry.duration,
            processingStart: entry.processingStart, processingEnd: entry.processingEnd,
            interactionId: entry.interactionId, value: entry.value, recentInput: entry.hadRecentInput,
          });
        }
      }).observe({ type, buffered: true, ...(type === 'event' ? { durationThreshold: 16 } : {}) });
    }
  });
  const cdp = await context.newCDPSession(page);
  await cdp.send('Performance.enable');
  const record = ledger(context);
  const prefix = `${role}-${traced ? 'traced' : 'control'}`;
  let tracing = false;
  let actions = false;
  let result;
  try {
    if (traced) {
      await context.tracing.start({ screenshots: true, snapshots: true, sources: false });
      actions = true;
      await browser.startTracing(page, { screenshots: true });
      tracing = true;
    }
    const before = await browserMetrics(cdp);
    const start = performance.now();
    const responsePromise = page.waitForResponse(response =>
      new URL(response.url()).pathname === '/api/dashboard/bundle' && response.request().method() === 'POST');
    await page.goto(`${origin}/dashboard?${target}`, { waitUntil: 'domcontentloaded' });
    await page.evaluate(() => performance.mark('pilot:document-domcontentloaded'));
    const response = await responsePromise;
    const initial = await assertResult(page, response);
    const loaded = performance.now();
    await page.evaluate(() => performance.mark('pilot:load-result-asserted-not-paint-proof'));
    const loadMetrics = await browserMetrics(cdp);
    record.setPhase('filter');
    const filterStart = performance.now();
    await page.evaluate(() => performance.mark('pilot:filter-start'));
    const filteredPromise = page.waitForResponse(response => {
      if (new URL(response.url()).pathname !== '/api/dashboard/bundle') return false;
      return Object.keys(response.request().postDataJSON()?.filters ?? {}).length > 0;
    });
    await page.getByTestId('add-filter-stub').click();
    const dialog = page.getByRole('dialog');
    await expect(dialog.getByRole('heading', { name: 'Add Custom Filter' })).toBeVisible();
    await dialog.getByRole('combobox').click();
    await page.getByRole('option', { name: 'Country', exact: true }).click();
    await dialog.getByPlaceholder('Type a country value...').fill(initial.country);
    await dialog.getByRole('button', { name: 'Apply Filter' }).click();
    const filteredResponse = await filteredPromise;
    const filtered = await assertResult(page, filteredResponse, initial.country);
    if (role === 'masked-analyst') {
      const body = await filteredResponse.json();
      const ips = body.aggregates?.data?.ip?.top;
      check(Array.isArray(ips) && ips.length > 0 && ips.every(row => isIP(String(row.value)) === 0),
        'masked_ip_result_unverified');
    }
    await expect.poll(() => new URL(page.url()).searchParams.get('filters')).toBe(
      JSON.stringify(filteredResponse.request().postDataJSON().filters),
    );
    const filteredAt = performance.now();
    await page.evaluate(() => performance.mark('pilot:filter-result-asserted-not-paint-proof'));
    const filterMetrics = await browserMetrics(cdp);
    record.setPhase('observation');
    await page.waitForTimeout(3000);
    const entries = await page.evaluate(() => ({
      navigation: performance.getEntriesByType('navigation').map(entry => entry.toJSON()),
      paint: performance.getEntriesByType('paint').map(entry => entry.toJSON()),
      entries: window.__pilotEntries,
      marks: performance.getEntriesByType('mark').filter(entry => entry.name.startsWith('pilot:')).map(entry => entry.toJSON()),
    }));
    const footers = page.locator('footer');
    check(role !== 'masked-analyst' || await footers.count() === 0, 'analyst_admin_footer_visible');
    const footer = await footers.count() ? await footers.first().innerText() : '';
    const commit = footer.match(/\b[0-9a-f]{7,40}\b/)?.[0] ?? null;
    result = {
      role, traced, load_asserted_ms: loaded - start, filter_asserted_ms: filteredAt - filterStart,
      load_renderer_ms: metricDelta(before, loadMetrics),
      filter_renderer_ms: metricDelta(loadMetrics, filterMetrics),
      deployed_footer_commit: commit, assertions: { country_filter: true, nonzero_country_rows: true,
        chart_total_matches_response: true, masking: role === 'masked-analyst' ? 'checked' : 'not_applicable',
        country_count_consistent: initial.count === filtered.count, result_painted: 'not_proven' },
      initial, filtered, browser_performance: entries,
    };
  } finally {
    await finishAll([
      async () => { if (tracing) fs.writeFileSync(path.join(directory, `${prefix}.chrome.json`), await browser.stopTracing(), { mode: 0o600 }); },
      async () => { if (actions) await context.tracing.stop({ path: path.join(directory, `${prefix}.playwright.zip`) }); },
      () => cdp.detach(),
      async () => { if (!fixturePage) await page.close(); },
      () => record.stop(),
      () => write(path.join(directory, `${prefix}.requests.private.json`), record.rows),
    ]);
  }
  result.requests = summarizeRequests(record.rows);
  result.diagnostics = record.rows.filter(row => row.diagnostics).map(row =>
    ({ phase: row.phase, endpoint: row.endpoint, debug_counts: row.debug_counts, ...row.diagnostics }));
  return result;
}

async function main() {
  process.umask(0o077);
  const output = process.env.PILOT_OUTPUT;
  check(output && path.isAbsolute(output), 'absolute_private_output_required');
  const parent = fs.lstatSync(path.dirname(output));
  check(parent.isDirectory() && !parent.isSymbolicLink() && parent.uid === process.getuid()
    && !(parent.mode & 0o077), 'owner_only_artifact_parent_required');
  fs.mkdirSync(output, { mode: 0o700 });
  const config = JSON.parse(process.env.ADMIN_GATEWAY_ENDPOINTS ?? '{}')['remote-standard'];
  check(config, 'remote_standard_identity_required');
  const adminOrigin = new URL(config.origin).origin;
  const adminOptions = adminGatewayContextOptions(adminOrigin);
  const api = await request.newContext({ ...adminOptions, timeout: 30000 });
  let browser;
  const samples = [];
  let failure;
  let conditions;
  try {
    const bootstrapResponse = await api.get(`${adminOrigin}/api/bootstrap`);
    check(bootstrapResponse.ok(), 'bootstrap_failed');
    const bootstrap = await bootstrapResponse.json();
    const statusResponse = await api.get(`${adminOrigin}/api/admin/share/status`);
    check(statusResponse.ok(), 'share_status_failed');
    const status = await statusResponse.json();
    check(status.sharing_active === true, 'active_sharing_required');
    const analystOrigin = new URL(status.public_endpoint).origin;
    const serviceId = bootstrap.active_service_id;
    const latest = Date.parse(bootstrap.log_extents?.latest_log_at);
    const end = new Date(process.env.PILOT_END_TIME ?? Math.floor(latest / 60000) * 60000 - 120000);
    check(Number.isFinite(end.getTime()), 'event_extent_required');
    check(end.getTime() <= latest && end.getTime() % 60000 === 0, 'retained_minute_aligned_end_required');
    const start = new Date(end.getTime() - 3600000);
    const target = new URLSearchParams({ service: serviceId, start_time: start.toISOString(), end_time: end.toISOString() });
    browser = await chromium.launch({ headless: true });
    for (const traced of [true, false]) {
      const context = await browser.newContext({ ...adminOptions, viewport: { width: 1440, height: 1000 },
        serviceWorkers: 'block' });
      try { samples.push(await journey(browser, context, adminOrigin, target, output, 'admin', traced)); }
      finally { await context.close(); }
    }
    await withAnalystUsers({ browser, request, adminOrigin, analystOrigin, serviceId, maskIps: true, users: 1,
      journalPath: path.join(output, 'analyst.ownership.private.jsonl') }, async users => {
      for (const traced of [true, false]) {
        samples.push(await journey(browser, users[0].context, analystOrigin, target, output,
          'masked-analyst', traced, users[0].page));
      }
    });
    conditions = {
      environment: 'remote-standard', window: { start: start.toISOString(), end: end.toISOString() },
      viewport: { width: 1440, height: 1000 }, browser: browser.version(),
      playwright: require('../../frontend/node_modules/@playwright/test/package.json').version,
      checked_out_commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
      checked_out_dirty: execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim().length > 0,
      cleanup: 'verified_by_owned_fixture', cpu_throttle: 'none',
      network_throttle: 'none', cache: 'uncontrolled; owner evidence in diagnostics',
      browser_cache: 'admin contexts fresh; analyst login warms a shared context; not role-comparable',
      overhead: 'traced then control; single samples confounded by order/cache/network; not an overhead estimate',
    };
    write(path.join(output, 'conditions.json'), conditions);
  } catch (error) {
    failure = error;
    write(path.join(output, 'failure.private.json'), { message: error.message, stack: error.stack, cleanup: error.cleanup });
  } finally {
    await finishAll([
      async () => { if (browser) await browser.close(); },
      () => api.dispose(),
      () => write(path.join(output, 'measurements.private.json'), samples),
      () => write(path.join(output, 'summary.json'), samples.map(summarizeSample)),
      () => fs.writeFileSync(path.join(output, 'report.md'),
        renderReport(samples.map(summarizeSample), conditions), { flag: 'wx', mode: 0o600 }),
    ]);
  }
  if (failure) throw failure;
  const countsAgree = samples.every(sample => sample.assertions.country_count_consistent);
  process.stdout.write(`${JSON.stringify({ status: countsAgree ? 'captured' : 'correctness_findings', samples: samples.map(sample => ({
    role: sample.role, traced: sample.traced, load_ms: Math.round(sample.load_asserted_ms),
    filter_ms: Math.round(sample.filter_asserted_ms), requests: sample.requests.count,
  })), cleanup: 'verified' })}\n`);
  if (!countsAgree) process.exitCode = 2;
}

if (require.main === module) main().catch(() => {
  process.stderr.write('dashboard_pilot_failed; inspect owner-only artifacts; no private exception printed\n');
  process.exitCode = 1;
});

module.exports = { summarizeRequests, metricDelta, summarizeSample, renderReport, ledger, finishAll, journey };
