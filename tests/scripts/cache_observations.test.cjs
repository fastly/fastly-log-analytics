const test = require('node:test');
const assert = require('node:assert/strict');
const { describeResponse } = require('../../scripts/lib/section_measurements.cjs');

test('followers retain wait metadata but cannot claim current computation', () => {
  const report = describeResponse({
    _response_cache: { state: 'unknown', prewarmer: false, ttl_seconds: 15, single_flight: 'follower', wait_ms: 8 },
    _section_timings: [{ section: 'query', time_ms: 5, current_execution: true, source: 'fact' }],
  });
  const row = report.components[0];
  assert.equal(row.single_flight, 'follower');
  assert.equal(row.wait_ms, 8);
  assert.equal(row.ttl_seconds, 15);
  assert.equal(row.lookup_current, false);
  assert.equal(row.sections[0].current_execution, false);
  assert.ok(row.gaps.includes('shared_execution'));
});

test('independent shielding is not historical merely because health hit its own cache', () => {
  const child = {
    _response_cache: { state: 'miss', prewarmer: false },
    _section_timings: [{ section: 'shielding', time_ms: 10, current_execution: true, source: 'live' }],
  };
  const health = { _response_cache: { state: 'hit', scope: 'self', prewarmer: false }, shielding_analysis: child };
  const independent = describeResponse(health).components[1];
  assert.equal(independent.sections[0].current_execution, true);
  assert.equal(independent.lookup_current, true);
  const historical = describeResponse({ _is_cached: true, health }).components[2];
  assert.equal(historical.sections[0].current_execution, false);
  assert.equal(historical.lookup_current, false);
});

test('malformed optional observations remain unknown and do not disclose extra fields', () => {
  const report = describeResponse({
    _response_cache: { state: 'hit', ttl_seconds: true, wait_ms: -1, single_flight: ['follower'], token: 'private' },
  });
  assert.equal(report.components[0].ttl_seconds, null);
  assert.equal(report.components[0].wait_ms, null);
  assert.equal(report.components[0].single_flight, null);
  assert.ok(!JSON.stringify(report).includes('private'));
});
