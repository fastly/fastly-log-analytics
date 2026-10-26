const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { test } = require('node:test');
const { parameters, writeEvidence, parsePaintOptions, INVALID_PAINT } = require('../../scripts/dev/check_analyst_lifecycle.cjs');

const args = ['--environment', 'gce-standard', '--admin-origin', 'https://admin.example.com', '--analyst-origin', 'https://analyst.example.com', '--service-id', 'service-test'];

const paintArgs = ['--role', 'masked',
  '--paint-target', 'button.apply-filter',
  '--paint-feedback-selector', '.toast', '--paint-feedback-text', 'Filter applied',
  '--paint-result-selector', 'table tbody tr', '--paint-result-text', '42'];

test('roles are explicit and normal/stress user counts remain bounded', () => {
  assert.throws(() => parameters(args), /invalid_arguments/);
  assert.throws(() => parameters([...args, '--role', 'masked', '--users', '100']), /invalid_arguments/);
  const masked = parameters([...args, '--role', 'masked']);
  assert.equal(masked.maskIps, true);
  assert.equal(masked.users, 2);
  assert.equal(masked.serviceId, 'service-test');
  const unmasked = parameters([...args, '--role', 'unmasked', '--users', '5']);
  assert.equal(unmasked.maskIps, false);
  assert.equal(unmasked.users, 5);
  assert.equal(unmasked.recover, false);
});

test('recovery requires its ownership journal and never chooses an implicit role', () => {
  assert.throws(() => parameters([...args, '--recover']), /invalid_arguments/);
  const recovery = parameters([...args, '--recover', '--journal', '/private/ownership.jsonl']);
  assert.equal(recovery.recover, true);
  assert.equal(recovery.role, undefined);
});

test('evidence is owner-only and cannot overwrite files or follow symlinks', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'phase4b-lifecycle-cli-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'evidence.json');
  writeEvidence(file, { kind: 'analyst_lifecycle_scope', qualification: 'incomplete' });
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.throws(() => writeEvidence(file, { different: true }));
  assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).qualification, 'incomplete');
  const link = path.join(dir, 'link.json');
  fs.symlinkSync(file, link);
  assert.throws(() => writeEvidence(link, {}));
});

test('invalid CLI arguments fail before browser or transport setup with safe output', () => {
  const result = spawnSync(process.execPath, [path.join(__dirname, '../../scripts/dev/check_analyst_lifecycle.cjs')], { encoding: 'utf8' });
  assert.equal(result.status, 2);
  assert.deepEqual(JSON.parse(result.stderr), { status: 'blocked', error: 'invalid_arguments', qualification: 'incomplete' });
  assert.equal(result.stdout, '');
});

test('parsePaintOptions returns null when no painted-action target is supplied', () => {
  assert.equal(parsePaintOptions({}), null);
});

test('parsePaintOptions rejects paint flags supplied without a target', () => {
  assert.equal(parsePaintOptions({ 'paint-result-selector': ['tr'] }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ 'paint-result-text': ['42'] }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ 'paint-feedback-selector': '.toast' }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ 'paint-feedback-text': 'ok' }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ 'paint-event-type': 'click' }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ 'paint-timeout-ms': '5000' }), INVALID_PAINT);
});

test('parsePaintOptions requires feedback and matched result pairs when a target is present', () => {
  const base = {
    'paint-target': 'button.apply-filter',
    'paint-feedback-selector': '.toast', 'paint-feedback-text': 'Filter applied',
    'paint-result-selector': ['tbody tr'], 'paint-result-text': ['42'],
  };
  assert.deepEqual(parsePaintOptions(base), {
    target: 'button.apply-filter',
    feedback: { selector: '.toast', text: 'Filter applied' },
    results: [{ selector: 'tbody tr', text: '42' }],
    eventType: 'click', timeoutMs: 5000,
  });
  assert.equal(parsePaintOptions({ ...base, 'paint-feedback-selector': undefined }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ ...base, 'paint-feedback-text': undefined }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ ...base, 'paint-result-selector': [] }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ ...base, 'paint-result-text': ['42', 'extra'] }), INVALID_PAINT);
});

test('parsePaintOptions maps multiple required result panels in order', () => {
  const paint = parsePaintOptions({
    'paint-target': 'button.apply-filter',
    'paint-feedback-selector': '.toast', 'paint-feedback-text': 'Filter applied',
    'paint-result-selector': ['tbody tr', '.summary .count'], 'paint-result-text': ['42', '7 shown'],
  });
  assert.deepEqual(paint.results, [
    { selector: 'tbody tr', text: '42' },
    { selector: '.summary .count', text: '7 shown' },
  ]);
});

test('parsePaintOptions validates event type and timeout bounds', () => {
  const base = {
    'paint-target': 'button.apply-filter',
    'paint-feedback-selector': '.toast', 'paint-feedback-text': 'Filter applied',
    'paint-result-selector': ['tbody tr'], 'paint-result-text': ['42'],
  };
  assert.equal(parsePaintOptions({ ...base, 'paint-event-type': 'keydown' }).eventType, 'keydown');
  assert.equal(parsePaintOptions({ ...base, 'paint-event-type': 'hover' }), INVALID_PAINT);
  assert.equal(parsePaintOptions({ ...base, 'paint-timeout-ms': '1000' }).timeoutMs, 1000);
  for (const bad of ['0', '-1', '60001', 'abc']) {
    assert.equal(parsePaintOptions({ ...base, 'paint-timeout-ms': bad }), INVALID_PAINT);
  }
});

test('parameters folds a valid painted-action request into options and rejects a malformed one', () => {
  const resolved = parameters([...args, ...paintArgs]);
  assert.deepEqual(resolved.paint, {
    target: 'button.apply-filter',
    feedback: { selector: '.toast', text: 'Filter applied' },
    results: [{ selector: 'table tbody tr', text: '42' }],
    eventType: 'click', timeoutMs: 5000,
  });
  assert.equal(parameters([...args, '--role', 'masked']).paint, null);
  assert.throws(() => parameters([...args, '--role', 'masked', '--paint-target', 'button.apply-filter']), /invalid_arguments/);
});
