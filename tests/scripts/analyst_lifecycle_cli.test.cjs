const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { test } = require('node:test');
const { parameters, writeEvidence } = require('../../scripts/dev/check_analyst_lifecycle.cjs');

const args = ['--environment', 'gce-standard', '--admin-origin', 'https://admin.example.com', '--analyst-origin', 'https://analyst.example.com', '--service-id', 'service-test'];

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
