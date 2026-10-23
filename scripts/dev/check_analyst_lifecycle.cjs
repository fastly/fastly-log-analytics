const fs = require('node:fs');
const path = require('node:path');
const { parseArgs } = require('node:util');
const { withAnalystUsers, recoverOwnedInvites } = require('../lib/analyst_fixture.cjs');

function parameters(argv) {
  const { values } = parseArgs({ args: argv, strict: true, options: {
    environment: { type: 'string' }, 'admin-origin': { type: 'string' }, 'analyst-origin': { type: 'string' },
    'service-id': { type: 'string' }, role: { type: 'string' }, users: { type: 'string', default: '2' },
    journal: { type: 'string' }, output: { type: 'string' }, recover: { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  } });
  if (values.help) return { help: true };
  if (!['gce-standard', 'local-standard', 'elevation-high-scale'].includes(values.environment)
    || !values['admin-origin'] || !values['analyst-origin'] || !values['service-id']
    || (!values.recover && !['masked', 'unmasked'].includes(values.role))
    || (values.recover && !values.journal) || ![1, 2, 5].includes(Number(values.users))) {
    const error = new Error('invalid_arguments');
    error.code = 'invalid_arguments';
    throw error;
  }
  const stem = `analyst-lifecycle-${values.environment}-${values.recover ? 'recovery' : values.role}-${Date.now()}`;
  return {
    environment: values.environment, adminOrigin: values['admin-origin'], analystOrigin: values['analyst-origin'],
    serviceId: values['service-id'], maskIps: values.role === 'masked', role: values.role,
    users: Number(values.users), recover: values.recover,
    journalPath: values.journal || path.join('performance-report', `${stem}.ownership.jsonl`),
    output: values.output || path.join('performance-report', `${stem}.json`),
  };
}

function writeEvidence(file, evidence) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const fd = fs.openSync(file, 'wx', 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(evidence, null, 2)}\n`);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

async function main(argv = process.argv.slice(2)) {
  const options = parameters(argv);
  if (options.help) {
    process.stdout.write('Usage: node scripts/dev/check_analyst_lifecycle.cjs --environment gce-standard|local-standard|elevation-high-scale --admin-origin HTTPS_ORIGIN --analyst-origin HTTPS_ORIGIN --service-id SERVICE_ID --role masked|unmasked [--users 1|2|5] [--journal PRIVATE_JSONL] [--output PRIVATE_JSON]\nRecovery: supply the same origins/service/environment with --recover --journal PRIVATE_JSONL\nThis checks disposable analyst lifecycle and scope only; performance qualification remains incomplete.\n');
    return;
  }
  const { chromium, request } = require('../../frontend/node_modules/@playwright/test');
  let browser;
  let result;
  let failure;
  try {
    if (options.recover) {
      await recoverOwnedInvites({ ...options, request });
      result = { resource_cleanup: 'verified' };
    } else {
      const lazyBrowser = { async newContext(contextOptions) {
        browser ??= await chromium.launch({ headless: true, timeout: 30000 });
        return browser.newContext(contextOptions);
      } };
      await withAnalystUsers({ ...options, request, browser: lazyBrowser }, async users => {
        result = { scope_status: 'checked', users: users.length, role: options.role };
      });
    }
  } catch (error) {
    failure = error;
  } finally {
    if (browser) {
      try { await browser.close(); } catch {
        if (!failure) {
          failure = new Error('browser_close_failed');
          failure.code = 'browser_close_failed';
        }
        failure.cleanup = [...(failure.cleanup || []), 'browser_close_failed'];
      }
    }
  }
  if (failure) throw failure;
  const evidence = {
    kind: options.recover ? 'analyst_lifecycle_recovery' : 'analyst_lifecycle_scope',
    qualification: 'incomplete', environment: options.environment, service_id: options.serviceId,
    admin_origin: options.adminOrigin, analyst_origin: options.analystOrigin, observed_at: new Date().toISOString(),
    ...result,
    gaps: ['ssr_scope_unverified', 'real_data_masking_unverified', 'filter_scenarios_unverified', 'history_unverified', 'live_traffic_unverified', 'browser_completion_unmeasured'],
  };
  writeEvidence(options.output, evidence);
  process.stdout.write(`${JSON.stringify({ environment: options.environment, ...result, qualification: 'incomplete', evidence: path.basename(options.output) })}\n`);
}

if (require.main === module) main().catch(error => {
  const code = typeof error.code === 'string' && /^[A-Za-z_]{1,80}$/.test(error.code) ? error.code : 'verification_failed';
  const cleanup = Array.isArray(error.cleanup) ? error.cleanup.filter(value => typeof value === 'string' && /^[a-z_]{1,80}$/.test(value)) : [];
  process.stderr.write(`${JSON.stringify({ status: 'blocked', error: code, qualification: 'incomplete', ...(cleanup.length ? { cleanup } : {}) })}\n`);
  process.exitCode = 2;
});

module.exports = { main, parameters, writeEvidence };
