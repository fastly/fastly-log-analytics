const budgets = require('./performance_budgets.json');

const cacheStates = new Set(['hit', 'miss', 'bypass', 'unknown']);
const sources = new Set(['rollup', 'live', 'projection', 'fact', 'projected_fact', 'mixed', 'unknown']);
const componentNames = ['aggregates', 'top_bots', 'health', 'quality', 'pop_health', 'transport', 'shielding_analysis'];
const queueSections = new Set(['connection_acquire', 'executor_wait', 'admission_wait', 'single_flight_wait']);
const overheadSections = new Set(['get_schema_cols', 'build_where_clause', 'request_setup', 'response_assembly']);
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const duration = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
const correlationFields = ['query_id', 'request_id', 'page_load_id', 'service_id', 'component', 'section_id', 'parent_section_id'];
const correlationValue = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/.test(value);

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
  const correlation = Object.fromEntries(correlationFields.filter(key => correlationValue(row[key])).map(key => [key, row[key]]));
  const timingKind = ['client_section_wall', 'client_operation_wall'].includes(row.timing_kind) ? row.timing_kind : 'unknown';
  const executorDispatch = ['inline', 'thread_pool'].includes(row.executor_dispatch) ? row.executor_dispatch : 'unknown';
  return { section: name, ms, source, source_evidence: sourceEvidence, activity, executor_dispatch: executorDispatch, executor_wait_ms: duration(row.executor_wait_ms), timing_kind: timingKind, current_execution: current, ...correlation, gaps };
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
  reader: enumValue(['dashboard.conn_requests_histogram', 'dashboard.ngwaf_bots', 'dashboard.count', 'dashboard.time_series', 'dashboard.top_n', 'dashboard.ua_bots']),
  minimum_window_hours: nullable(count), maximum_window_days: nullable(count),
  closed_hour_heal: enumValue(['not_attempted', 'planned', 'used', 'unavailable', 'failed']),
  eligible: nullable(value => typeof value === 'boolean'),
  eligibility_reason: enumValue(['unknown', 'filtered', 'bounds_missing', 'top_asns_empty', 'invalid_window', 'window_too_short', 'window_too_long', 'eligible', 'unsupported_metric', 'unsupported_interval', 'unsafe_fields', 'unsafe_service', 'durable_unready', 'rollups_missing']),
  window_start: timestamp, window_end: timestamp, live_start: timestamp, live_end: timestamp,
  outcome: enumValue(['unknown', 'ineligible', 'no_coverage', 'read_failed', 'empty_result', 'served']),
  live_top_up: enumValue(['not_attempted', 'outside_window', 'missing_column', 'unavailable', 'empty', 'used', 'failed', 'view_fallback', 'unverified']),
};

const hourlyFields = {
  reason: enumValue(['unknown', 'no_bundled_root', 'durable_unready', 'writer_behind', 'no_paths', 'old_gaps', 'planned']),
  window_start: timestamp, window_end: timestamp, active_hour_start: timestamp,
  ...Object.fromEntries(['closed_hours', 'bundle_hours', 'verified_empty_hours', 'missing_hours', 'covered_hours', 'heal_horizon_hours'].map(key => [key, nullable(count)])),
  coverage_fraction: nullable(fraction),
};
const scalarFields = {
  reason: enumValue(['unknown', 'trailing_unavailable', 'boundary_changed', 'planned']),
  window_start: timestamp, window_end: timestamp, active_hour_start: timestamp, trailing_start: timestamp,
  closed_hours: nullable(count), covered_hours: nullable(count), coverage_fraction: nullable(fraction),
  prefix_hours: count, day_time_series_files: count, day_overview_files: count,
};
const topNFields = {
  coverage_scope: enumValue(['any_field_file_presence']), active_hour_start: timestamp,
  ...Object.fromEntries(['closed_hours', 'any_field_covered_hours', 'field_complete_hours', 'heal_candidates', 'heal_missing_hours', 'compacted_hours_skipped', 'outside_heal_horizon_hours'].map(key => [key, nullable(count)])),
  ...Object.fromEntries(['bundled_day_files', 'bundled_hour_files', 'per_field_day_files', 'per_field_hour_files', 'heal_horizon_hours'].map(key => [key, count])),
  rolled_read: enumValue(['not_attempted', 'used', 'failed']),
};
const historyFields = {
  extent_source: enumValue(['unknown', 'status_cache', 'live_query', 'count_only_query', 'unavailable']),
  extent_rows: nullable(count), earliest_event_at: timestamp, latest_event_at: timestamp,
  window_start: timestamp, window_end: timestamp, filters_requested: nullable(value => typeof value === 'boolean'),
  window_rows: nullable(count), window_count_source: enumValue(['unknown', 'rollup', 'raw', 'filtered_temp']),
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
  if (!Array.isArray(raw) || !raw.length) return { observations: [], gaps: ['reader_coverage_missing'] };
  const observations = [];
  const gaps = [];
  for (const row of raw.slice(0, 32)) {
    let parsed = allowlistedObject(row, readerFields, ['reader']);
    if (parsed && Object.hasOwn(row, 'scalar_plan')) {
      parsed.scalar_plan = row.scalar_plan === null ? null : allowlistedObject(row.scalar_plan, scalarFields, []);
      if (row.scalar_plan !== null && !parsed.scalar_plan) parsed = null;
      if (parsed?.scalar_plan && Object.hasOwn(row.scalar_plan, 'trailing')) {
        parsed.scalar_plan.trailing = row.scalar_plan.trailing === null ? null : allowlistedObject(row.scalar_plan.trailing, hourlyFields, []);
        if (row.scalar_plan.trailing !== null && !parsed.scalar_plan.trailing) parsed = null;
      }
    }
    if (parsed && Object.hasOwn(row, 'top_n_plan')) {
      parsed.top_n_plan = row.top_n_plan === null ? null : allowlistedObject(row.top_n_plan, topNFields, []);
      if (row.top_n_plan !== null && !parsed.top_n_plan) parsed = null;
    }
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
    if (['failed', 'unavailable', 'missing_column', 'unverified'].includes(parsed.live_top_up)) gaps.push(`reader_live_top_up_${parsed.live_top_up}`);
    if (!parsed.outcome || ['unknown', 'read_failed'].includes(parsed.outcome)) gaps.push('reader_outcome_unverified');
    if (['failed', 'unavailable'].includes(parsed.closed_hour_heal)) gaps.push('reader_closed_hour_heal_incomplete');
    if ((parsed.attempts || []).some(attempt => attempt.reason === 'planned' && attempt.coverage_fraction != null && attempt.coverage_fraction < 1)) gaps.push('reader_closed_hours_uncovered');
    if (parsed.scalar_plan?.reason === 'boundary_changed') gaps.push('reader_coverage_boundary_changed');
    if (parsed.scalar_plan?.trailing?.missing_hours) gaps.push('reader_closed_hours_uncovered');
    if (parsed.top_n_plan && Object.keys(parsed.top_n_plan).length) {
      gaps.push('reader_field_complete_coverage_unverified');
      if (parsed.top_n_plan.rolled_read === 'failed') gaps.push('reader_rolled_read_failed');
      if (parsed.top_n_plan.outside_heal_horizon_hours) gaps.push('reader_older_hours_not_heal_checked');
    }
  }
  if (raw.length > 32) gaps.push('reader_coverage_truncated');
  return { observations, gaps };
}

function describeHistory(raw, reused, state) {
  if (raw == null) return { observation: null, gaps: ['history_evidence_missing'] };
  const parsed = allowlistedObject(raw, historyFields, []);
  if (!parsed) return { observation: null, gaps: ['history_evidence_invalid'] };
  const current = reused ? false : ['miss', 'bypass'].includes(state) ? true : null;
  const gaps = ['history_continuity_unverified'];
  if (current !== true) gaps.push('history_historical_or_unknown');
  if (parsed.extent_source !== 'live_query' || !parsed.earliest_event_at || !parsed.latest_event_at) gaps.push('history_extent_unverified');
  if (!['raw', 'filtered_temp'].includes(parsed.window_count_source) || parsed.window_rows == null) gaps.push('history_window_rows_unverified');
  if (parsed.window_rows === 0) gaps.push('history_empty_window');
  return { observation: { observation: parsed, current_observation: current }, gaps };
}

const queryFields = {
  ...Object.fromEntries(correlationFields.map(key => [key, nullable(correlationValue)])),
  query_id: correlationValue, section: nullable(correlationValue),
  source: enumValue(['fact', 'projection', 'projected_fact', 'mixed', 'unknown']),
  outcome: enumValue(['success', 'error', 'timeout']),
  client_elapsed_ms: value => duration(value) !== null,
  ...Object.fromEntries(['admission_wait_ms', 'network_ms', 'decode_ms'].map(key => [key, nullable(value => duration(value) !== null)])),
  server_elapsed_ms: nullable(value => duration(value) !== null),
  server_elapsed_source: nullable(enumValue(['statistics.elapsed'])),
  server_cpu_ms: value => value === null,
  timing_kind: enumValue(['client_operation_wall']),
};

function describeQueryOperations(raw, reused, state) {
  const operations = [];
  const gaps = [];
  if (!Array.isArray(raw)) return { operations, gaps };
  for (const row of raw.slice(0, 256)) {
    if (!isObject(row) || !isObject(row.clickhouse)) continue;
    const parsed = allowlistedObject(row.clickhouse, queryFields, ['query_id', 'outcome', 'client_elapsed_ms']);
    if (!parsed) {
      gaps.push('query_operation_invalid');
      continue;
    }
    const current = reused || row.is_cached === true ? false : ['miss', 'bypass'].includes(state) ? true : null;
    operations.push({ observation: parsed, current_observation: current });
    gaps.push('server_cpu_unobserved');
    if (parsed.server_elapsed_ms == null || parsed.server_elapsed_source !== 'statistics.elapsed') gaps.push('server_wall_time_unobserved');
    if (current !== true) gaps.push('query_operation_historical_or_unknown');
    if (['request_id', 'service_id', 'section_id', 'section'].some(key => !parsed[key])) gaps.push('query_correlation_incomplete');
    if (parsed.outcome !== 'success') gaps.push('query_operation_failed');
    else if (['admission_wait_ms', 'network_ms', 'decode_ms'].some(key => parsed[key] == null)) gaps.push('query_phase_unverified');
  }
  if (raw.length > 256) gaps.push('query_operations_truncated');
  return { operations, gaps };
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
    const wrapper = componentNames.some(name => isObject(body[name]));
    const rawReaders = Object.hasOwn(body, '_reader_coverage') ? body._reader_coverage : body.reader_coverage;
    const readers = rawReaders == null && wrapper
      ? { observations: [], gaps: [] }
      : describeReaderCoverage(rawReaders, cachedExecution, state);
    const rawHistory = Object.hasOwn(body, '_history') ? body._history : body.history;
    const history = rawHistory == null && wrapper ? { observation: null, gaps: [] } : describeHistory(rawHistory, cachedExecution, state);
    const gaps = conflict ? ['response_cache_conflict'] : [];
    gaps.push(...readers.gaps, ...history.gaps);
    const queries = describeQueryOperations(body._debug_queries ?? body.debug_queries, cachedExecution, state);
    gaps.push(...queries.gaps);
    if (state === 'unknown') gaps.push('response_cache_unknown');
    if (shared) gaps.push('shared_execution');
    if (prewarmer === null) gaps.push('prewarmer_unknown');
    if (!sections.length) gaps.push('section_timings_missing');
    if (cachedExecution && sections.length) gaps.push('historical_or_unverified_timings');
    components.push({ component: path, cache_state: state, prewarmer, ttl_seconds: duration(metadata.ttl_seconds), single_flight: singleFlight, wait_ms: duration(metadata.wait_ms), execution_reused: cachedExecution, lookup_current: !enclosingHit && !shared, sections, reader_coverage: readers.observations, history: history.observation, query_operations: queries.operations, gaps });
    for (const name of componentNames) {
      const inherit = enclosingHit || (cachedExecution && metadata.scope !== 'self');
      if (isObject(body[name])) visit(body[name], path === 'response' ? name : `${path}.${name}`, inherit);
    }
  }
  visit(payload, 'response');
  const gaps = new Set(components.flatMap(row => [...row.gaps, ...row.sections.flatMap(section => section.gaps)]));
  const querySections = new Set(components.flatMap(row => row.query_operations.filter(query => query.current_observation === true).map(query => query.observation.section_id).filter(Boolean)));
  for (const component of components) {
    for (const section of component.sections) {
      if (['fact', 'projection', 'projected_fact', 'mixed'].includes(section.source) && section.current_execution === true && !querySections.has(section.section_id)) gaps.add('section_query_correlation_missing');
    }
  }
  return { components, gaps: [...gaps].sort() };
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
