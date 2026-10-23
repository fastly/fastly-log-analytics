const budgets = require('./performance_budgets.json');

const cacheStates = new Set(['hit', 'miss', 'bypass', 'unknown']);
const sources = new Set(['rollup', 'live', 'projection', 'fact', 'projected_fact', 'unknown']);
const componentNames = ['aggregates', 'top_bots', 'health', 'quality', 'pop_health', 'transport', 'shielding_analysis'];
const queueSections = new Set(['connection_acquire', 'executor_wait', 'admission_wait', 'single_flight_wait']);
const overheadSections = new Set(['get_schema_cols', 'build_where_clause', 'request_setup', 'response_assembly']);
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const duration = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;

function describeSection(row, cacheState) {
  const name = row.section;
  const ms = duration(Object.hasOwn(row, 'time_ms') ? row.time_ms : row.elapsed_ms);
  let source = sources.has(row.source) ? row.source : 'unknown';
  let sourceEvidence = source !== 'unknown' ? 'explicit' : 'unknown';
  if (source === 'unknown' && name.includes('rollup') && !name.endsWith('_cache_hit')) {
    source = 'rollup';
    sourceEvidence = 'section_name';
  }
  let activity = row.activity;
  if (!['compute', 'queue', 'overhead', 'cache', 'skipped', 'unknown'].includes(activity)) {
    if (queueSections.has(name)) activity = 'queue';
    else if (overheadSections.has(name)) activity = 'overhead';
    else if (name.endsWith('_cache_hit')) activity = 'cache';
    else if (name.includes('temp_skipped') || name.endsWith('_skipped')) activity = 'skipped';
    else activity = 'unknown';
  }
  let current = typeof row.current_execution === 'boolean' ? row.current_execution : null;
  if (cacheState === 'hit') current = false;
  const gaps = [];
  if (ms === null) gaps.push('duration_unknown');
  if (sourceEvidence !== 'explicit' && !['queue', 'overhead', 'cache', 'skipped'].includes(activity)) gaps.push('source_unverified');
  if (current === null) gaps.push('execution_freshness_unknown');
  return { section: name, ms, source, source_evidence: sourceEvidence, activity, current_execution: current, gaps };
}

const coverageTimestamp = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/;
const enumValue = values => value => values.includes(value);
const count = value => Number.isInteger(value) && value >= 0;
const fraction = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
const nullable = check => value => value === null || check(value);
const timestamp = nullable(value => typeof value === 'string' && coverageTimestamp.test(value));
const pathFields = {
  bundle: enumValue(['security_conn_reuse', 'all_fields', 'ngwaf_bots']),
  reason: enumValue(['unknown', 'durable_unready', 'no_bundled_root', 'no_paths', 'below_coverage_floor', 'planned']),
  minimum_coverage_fraction: fraction, active_hour_start: timestamp,
  closed_hours: nullable(count), covered_hours: nullable(count), coverage_fraction: nullable(fraction),
  partial_start_skipped: nullable(value => typeof value === 'boolean'),
  day_files: nullable(count), hour_files: nullable(count), done_markers: nullable(count),
};
const readerFields = {
  reader: enumValue(['dashboard.conn_requests_histogram', 'dashboard.ngwaf_bots']),
  minimum_window_hours: count, maximum_window_days: value => Number.isInteger(value),
  eligible: nullable(value => typeof value === 'boolean'),
  eligibility_reason: enumValue(['unknown', 'filtered', 'bounds_missing', 'top_asns_empty', 'invalid_window', 'window_too_short', 'window_too_long', 'eligible']),
  window_start: timestamp, window_end: timestamp, live_start: timestamp, live_end: timestamp,
  outcome: enumValue(['unknown', 'ineligible', 'no_coverage', 'read_failed', 'empty_result', 'served']),
  live_top_up: enumValue(['not_attempted', 'outside_window', 'missing_column', 'unavailable', 'empty', 'used', 'failed']),
};

function allowlistedObject(row, fields, required) {
  if (!isObject(row) || required.some(key => !Object.hasOwn(row, key))) return null;
  const result = {};
  for (const [key, check] of Object.entries(fields)) {
    if (!Object.hasOwn(row, key)) continue;
    if (!check(row[key])) return null;
    result[key] = row[key];
  }
  return result;
}

function describeReaderCoverage(raw, reused, state) {
  if (!Array.isArray(raw) || !raw.length) return { observations: [], gaps: ['reader_coverage_missing', 'history_evidence_missing'] };
  const observations = [];
  const gaps = ['history_evidence_missing'];
  for (const row of raw.slice(0, 2)) {
    const parsed = allowlistedObject(row, readerFields, ['reader', 'minimum_window_hours']);
    if (parsed && Object.hasOwn(row, 'attempts')) {
      if (!Array.isArray(row.attempts) || row.attempts.length > 2) {
        gaps.push('reader_coverage_invalid');
        continue;
      }
      parsed.attempts = row.attempts.map(attempt => allowlistedObject(attempt, pathFields, ['bundle']));
      if (parsed.attempts.some(attempt => attempt === null)) {
        gaps.push('reader_coverage_invalid');
        continue;
      }
    }
    if (!parsed) {
      gaps.push('reader_coverage_invalid');
      continue;
    }
    const current = reused ? false : ['miss', 'bypass'].includes(state) ? true : null;
    observations.push({ observation: parsed, current_observation: current });
    if (current !== true) gaps.push('reader_coverage_historical_or_unknown');
    if (['failed', 'unavailable', 'missing_column'].includes(parsed.live_top_up)) gaps.push(`reader_live_top_up_${parsed.live_top_up}`);
    if (!parsed.outcome || ['unknown', 'read_failed'].includes(parsed.outcome)) gaps.push('reader_outcome_unverified');
  }
  if (raw.length > 2) gaps.push('reader_coverage_truncated');
  return { observations, gaps };
}

function describeResponse(payload) {
  if (!isObject(payload)) return { components: [], gaps: ['response_not_object'] };
  const components = [];
  function visit(body, path, enclosingHit = false) {
    const metadata = isObject(body._response_cache) ? body._response_cache : {};
    const hitMarker = body._is_cached === true || body.is_cached === true;
    const conflict = ['miss', 'bypass'].includes(metadata.state) && hitMarker;
    const state = conflict ? 'unknown' : cacheStates.has(metadata.state) ? metadata.state : hitMarker ? 'hit' : 'unknown';
    const singleFlight = ['leader', 'follower'].includes(metadata.single_flight) ? metadata.single_flight : null;
    const shared = singleFlight === 'follower';
    const cachedExecution = enclosingHit || hitMarker || state === 'hit' || shared;
    const raw = Object.hasOwn(body, '_section_timings') ? body._section_timings : body.section_timings;
    const sections = (Array.isArray(raw) ? raw : []).filter(row => isObject(row) && typeof row.section === 'string').map(row => describeSection(row, cachedExecution ? 'hit' : state));
    const prewarmer = typeof metadata.prewarmer === 'boolean' ? metadata.prewarmer : null;
    const rawReaders = Object.hasOwn(body, '_reader_coverage') ? body._reader_coverage : body.reader_coverage;
    const readers = rawReaders == null && componentNames.some(name => isObject(body[name]))
      ? { observations: [], gaps: ['history_evidence_missing'] }
      : describeReaderCoverage(rawReaders, cachedExecution, state);
    const gaps = conflict ? ['response_cache_conflict'] : [];
    gaps.push(...readers.gaps);
    if (state === 'unknown') gaps.push('response_cache_unknown');
    if (shared) gaps.push('shared_execution');
    if (prewarmer === null) gaps.push('prewarmer_unknown');
    if (!sections.length) gaps.push('section_timings_missing');
    if (cachedExecution && sections.length) gaps.push('historical_or_unverified_timings');
    components.push({ component: path, cache_state: state, prewarmer, ttl_seconds: duration(metadata.ttl_seconds), single_flight: singleFlight, wait_ms: duration(metadata.wait_ms), execution_reused: cachedExecution, lookup_current: !enclosingHit && !shared, sections, reader_coverage: readers.observations, gaps });
    for (const name of componentNames) {
      const inherit = enclosingHit || (cachedExecution && metadata.scope !== 'self');
      if (isObject(body[name])) visit(body[name], path === 'response' ? name : `${path}.${name}`, inherit);
    }
  }
  visit(payload, 'response');
  return { components, gaps: [...new Set(components.flatMap(row => [...row.gaps, ...row.sections.flatMap(section => section.gaps)]))].sort() };
}

function collectResponseMeasurements(page, { maxRecords = 256 } = {}) {
  if (!Number.isInteger(maxRecords) || maxRecords < 1) throw new Error('maxRecords must be a positive integer');
  const records = [];
  const pending = new Set();
  let sequence = 0;
  let truncated = false;
  const listener = response => {
    const endpoint = new URL(response.url()).pathname;
    if (!/^\/api\/.*\/(?:aggregates|bundle)$/.test(endpoint)) return;
    if (sequence >= maxRecords) {
      truncated = true;
      return;
    }
    const row = { sample_id: `response-${++sequence}`, endpoint, status: response.status(), response: null, gaps: [] };
    records.push(row);
    if (row.status !== 200) {
      row.gaps.push('http_error');
      return;
    }
    const work = (async () => {
      try {
        row.response = describeResponse(await response.json());
        row.gaps = [...row.response.gaps];
      } catch {
        row.gaps.push('response_json_unavailable');
      }
    })();
    pending.add(work);
    work.finally(() => pending.delete(work));
  };
  page.on('response', listener);
  return {
    async snapshot({ timeoutMs = 2000 } = {}) {
      if (!Number.isFinite(timeoutMs) || timeoutMs < 0) throw new Error('timeoutMs must be finite and nonnegative');
      const cohort = records.slice();
      let timer;
      try {
        await Promise.race([
          Promise.all([...pending]),
          new Promise(resolve => { timer = setTimeout(resolve, timeoutMs); }),
        ]);
      } finally {
        clearTimeout(timer);
      }
      const snapshot = structuredClone(cohort);
      for (const row of snapshot) {
        if (row.status === 200 && row.response === null && !row.gaps.length) row.gaps.push('response_body_pending');
      }
      return snapshot;
    },
    gaps: () => truncated ? ['response_records_truncated'] : [],
    close: () => page.off('response', listener),
  };
}

module.exports = { budgets, describeResponse, collectResponseMeasurements };
