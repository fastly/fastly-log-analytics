const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { adminGatewayContextOptions } = require('./admin_gateway_client.cjs');

class FixtureFailure extends Error {
  constructor(code, cleanup = []) {
    super(`Analyst fixture blocked: ${code}${cleanup.length ? `; cleanup=${cleanup.join(',')}` : ''}`);
    this.code = code;
    this.cleanup = cleanup;
  }
}

function requireThat(condition, code) {
  if (!condition) throw new FixtureFailure(code);
}

function httpsOrigin(value) {
  const url = new URL(value);
  requireThat(url.protocol === 'https:' && !url.username && !url.password && ['/', ''].includes(url.pathname) && !url.search && !url.hash, 'https_origin_required');
  return url.origin;
}

function configuration(options) {
  const adminOrigin = httpsOrigin(options.adminOrigin);
  const analystOrigin = httpsOrigin(options.analystOrigin);
  requireThat(typeof options.serviceId === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(options.serviceId), 'service_required');
  const tls = adminGatewayContextOptions(adminOrigin, options.env || process.env);
  requireThat(tls.clientCertificates?.length === 1 && tls.clientCertificates[0].origin === adminOrigin, 'configured_admin_identity_required');
  return { adminOrigin, analystOrigin, tls };
}

async function api(context, origin, endpoint, { method = 'GET', data, headers, statuses = [200], json = true } = {}) {
  const response = await context.fetch(`${origin}${endpoint}`, { method, data, headers, maxRedirects: 0, failOnStatusCode: false, timeout: 15000 });
  try {
    requireThat(new URL(response.url()).origin === origin && statuses.includes(response.status()), 'unexpected_api_response');
    return json ? await response.json() : null;
  } finally {
    await response.dispose();
  }
}

function ownedInvite(invite, intent, future = false) {
  const expiry = Date.parse(invite?.expires_at);
  requireThat(invite && typeof invite.id === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(invite.id), 'invalid_invite_identity');
  requireThat(invite.email === intent.email && invite.name === intent.name && invite.auth_method === 'passcode'
    && Array.isArray(invite.service_ids) && invite.service_ids.length === 1 && invite.service_ids[0] === intent.service_id
    && invite.pii_policy?.mask_ips === intent.mask_ips && invite.allow_concurrent_sessions === false
    && invite.query_window_hours == null && invite.query_start_time == null && invite.query_end_time == null,
  'invite_ownership_changed');
  requireThat(Number.isFinite(expiry) && expiry > intent.started_at && expiry <= intent.started_at + 7200000 && (!future || expiry > Date.now()), 'invalid_invite_expiry');
  return { id: invite.id, expires_at: invite.expires_at };
}

function acquireJournalLock(journalPath) {
  const lockPath = `${journalPath}.lock`;
  let fd;
  try { fd = fs.openSync(lockPath, 'wx', 0o600); } catch (error) {
    if (error.code === 'EEXIST') throw new FixtureFailure('journal_lock_held');
    throw error;
  }
  try {
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: Date.now() }));
    fs.fsyncSync(fd);
    return { fd, path: lockPath, inode: fs.fstatSync(fd).ino };
  } catch (error) {
    fs.closeSync(fd);
    fs.unlinkSync(lockPath);
    throw error;
  }
}

function releaseJournalLock(lock, cleanup) {
  if (!lock) return;
  try {
    requireThat(fs.lstatSync(lock.path).ino === lock.inode, 'journal_lock_changed');
    fs.unlinkSync(lock.path);
  } catch { cleanup.push('journal_lock_release_failed'); }
  try { fs.closeSync(lock.fd); } catch { cleanup.push('journal_lock_close_failed'); }
}

function appendJournal(fd, event) {
  fs.writeFileSync(fd, `${JSON.stringify(event)}\n`);
  fs.fsyncSync(fd);
}

async function removeOwned(admin, origin, intents, ids, fd, cleanup) {
  for (const intent of intents) {
    try {
      const listed = await api(admin, origin, '/api/admin/share/invites');
      requireThat(Array.isArray(listed.invites), 'invite_list_invalid');
      const matches = listed.invites.filter(invite => invite.email === intent.email && invite.name === intent.name);
      requireThat(matches.length <= 1, 'ambiguous_invite_ownership');
      if (!matches.length) continue;
      const owned = ownedInvite(matches[0], intent);
      intent.settled = true;
      appendJournal(fd, { event: 'invite_reconciled', email: intent.email, id: owned.id });
      ids.add(owned.id);
      await api(admin, origin, `/api/admin/share/invites/${encodeURIComponent(owned.id)}`, { method: 'DELETE', statuses: [200, 204, 404], json: false });
      appendJournal(fd, { event: 'invite_deleted', id: owned.id });
    } catch {
      cleanup.push('owned_invite_cleanup_failed');
    }
  }
  const unresolved = intents.some(intent => !intent.settled);
  if (unresolved) cleanup.push('create_completion_unresolved');
  try {
    const status = await api(admin, origin, '/api/admin/share/status');
    requireThat(Array.isArray(status.invites) && Array.isArray(status.sessions), 'cleanup_status_invalid');
    requireThat(!status.invites.some(invite => ids.has(invite.id) || intents.some(intent => invite.email === intent.email && invite.name === intent.name))
      && !status.sessions.some(session => ids.has(session.invite_id)), 'owned_state_remains');
    appendJournal(fd, unresolved ? { event: 'cleanup_incomplete', unresolved_create: true } : { event: 'cleanup_verified', remaining: 0 });
  } catch {
    cleanup.push('owned_state_verification_failed');
  }
}

function scopedBootstrap(body, serviceId, maskIps) {
  requireThat(body?.settings?.is_remote_analyst === true
    && body.settings.mask_ips === maskIps
    && body.active_service_id === serviceId && Array.isArray(body.services) && body.services.length === 1
    && body.services[0].service_id === serviceId, 'analyst_bootstrap_scope_failed');
}

async function login(context, page, origin, serviceId, intent, passcode) {
  await page.goto(`${origin}/share-login?return=${encodeURIComponent(`/dashboard?service=${encodeURIComponent(serviceId)}`)}`);
  await page.getByTestId('share-login-form').waitFor({ state: 'visible' });
  requireThat(new URL(page.url()).origin === origin, 'credential_document_origin_invalid');
  await page.getByLabel('Email', { exact: true }).fill(intent.email);
  requireThat(new URL(page.url()).origin === origin, 'credential_document_origin_invalid');
  await page.getByLabel('Passcode', { exact: true }).fill(passcode);
  const [response] = await Promise.all([
    page.waitForResponse(value => value.url() === `${origin}/api/share/login` && value.request().method() === 'POST'),
    Promise.resolve().then(() => page.getByTestId('share-login-form').locator('button[type="submit"]').click()),
  ]);
  requireThat(response.status() === 200, 'analyst_login_failed');
  await page.waitForURL(url => url.origin === origin && ['/share-login/acknowledge', '/dashboard'].includes(url.pathname));
  let pending = null;
  if (new URL(page.url()).pathname === '/share-login/acknowledge') {
    const cookies = await context.cookies(origin);
    pending = cookies.find(cookie => cookie.name === 'analyst_pending_session_id');
    requireThat(pending?.secure === true && pending.httpOnly === true && !cookies.some(cookie => cookie.name === 'analyst_session_id'), 'pending_cookie_invalid');
    await page.getByTestId('share-acknowledge-page').waitFor({ state: 'visible' });
    requireThat(new URL(page.url()).origin === origin, 'credential_document_origin_invalid');
    const [acknowledged] = await Promise.all([
      page.waitForResponse(value => value.url() === `${origin}/api/share/acknowledge` && value.request().method() === 'POST'),
      Promise.resolve().then(() => page.getByTestId('share-acknowledge-submit').click()),
    ]);
    requireThat(acknowledged.status() === 200, 'tos_acknowledgment_failed');
  }
  await page.waitForURL(url => url.origin === origin && url.pathname === '/dashboard');
  const cookies = await context.cookies(origin);
  const session = cookies.find(cookie => cookie.name === 'analyst_session_id');
  requireThat(session?.secure === true && session.httpOnly === true && session.sameSite === 'Strict'
    && !cookies.some(cookie => cookie.name === 'analyst_pending_session_id') && (!pending || session.value !== pending.value), 'session_cookie_invalid');
}

async function withAnalystUsers(options, work) {
  let stage = 'configuration';
  let primary = null;
  let result;
  let admin;
  let anonymous;
  let fd;
  let lock;
  const contexts = [];
  const intents = [];
  const ids = new Set();
  const cleanup = [];
  let config;
  try {
    config = configuration(options);
    requireThat(!(process.env.DEBUG || '').split(/[\s,]+/).some(value => value === '*' || value.startsWith('pw:'))
      && (!process.env.PWDEBUG || process.env.PWDEBUG === '0'), 'credential_debug_logging_forbidden');
    const users = options.users ?? 1;
    requireThat(Number.isInteger(users) && users >= 1 && users <= 5 && typeof options.maskIps === 'boolean', 'user_scenario_invalid');
    stage = 'admin_preflight';
    admin = await options.request.newContext({ ...config.tls, ignoreHTTPSErrors: false });
    const status = await api(admin, config.adminOrigin, '/api/admin/share/status');
    requireThat(status.sharing_active === true && httpsOrigin(status.public_endpoint) === config.analystOrigin
      && Array.isArray(status.services) && status.services.some(service => service.service_id === options.serviceId), 'active_target_sharing_required');
    requireThat(Number.isInteger(status.max_concurrent_sessions) && Number.isInteger(status.active_session_count)
      && status.active_session_count >= 0 && status.active_session_count + users <= status.max_concurrent_sessions, 'analyst_capacity_unavailable');
    const deniedService = status.services.find(service => service.service_id !== options.serviceId)?.service_id || `phase4b-denied-${crypto.randomUUID()}`;
    anonymous = await options.request.newContext({ ignoreHTTPSErrors: false });
    const auth = await api(anonymous, config.analystOrigin, '/api/share/auth-config');
    requireThat(auth.passcode_enabled === true, 'passcode_login_disabled');
    stage = 'ownership_journal';
    requireThat(typeof options.journalPath === 'string', 'ownership_journal_required');
    fs.mkdirSync(path.dirname(options.journalPath), { recursive: true });
    lock = acquireJournalLock(options.journalPath);
    fd = fs.openSync(options.journalPath, 'ax', 0o600);
    const runId = crypto.randomUUID();
    appendJournal(fd, { event: 'run', run_id: runId, admin_origin: config.adminOrigin, analyst_origin: config.analystOrigin, service_id: options.serviceId });
    const ready = [];
    for (let index = 0; index < users; index++) {
      const intent = { event: 'invite_intent', run_id: runId, name: `Phase4b ${runId} ${index}`, email: `phase4b-${runId}-${index}@example.com`, service_id: options.serviceId, mask_ips: options.maskIps, started_at: Date.now() };
      intents.push(intent);
      appendJournal(fd, intent);
      const passcode = crypto.randomBytes(24).toString('base64url');
      stage = 'invite_create';
      const invite = await api(admin, config.adminOrigin, '/api/admin/share/invites', { method: 'POST', data: { name: intent.name, email: intent.email, passcode, duration_hours: 1, service_ids: [options.serviceId], pii_policy: { mask_ips: options.maskIps }, allow_concurrent_sessions: false, auth_method: 'passcode' } });
      const owned = ownedInvite(invite, intent, true);
      ids.add(owned.id);
      intent.settled = true;
      appendJournal(fd, { event: 'invite_created', email: intent.email, id: owned.id, expires_at: owned.expires_at });
      stage = 'analyst_context';
      const context = await options.browser.newContext({ clientCertificates: [], ignoreHTTPSErrors: false, serviceWorkers: 'block', acceptDownloads: false });
      contexts.push(context);
      context.setDefaultTimeout(15000);
      context.setDefaultNavigationTimeout(30000);
      let originViolations = 0;
      let checkedRequests = 0;
      let interceptionFailures = 0;
      const failureCategories = {};
      const failureEvents = [];
      const rejectedSchemes = {};
      const pendingInterceptions = new Set();
      const page = await context.newPage();
      const interception = await context.newCDPSession(page);
      interception.on('Fetch.requestPaused', event => {
        const pausedAt = Date.now();
        const handling = (async () => {
          checkedRequests++;
          let allowed = false;
          let scheme = 'invalid';
          try {
            const target = new URL(event.request.url);
            scheme = ['https:', 'http:', 'data:', 'blob:', 'about:'].includes(target.protocol) ? target.protocol : 'other';
            allowed = target.protocol === 'https:' && !target.username && !target.password && target.origin === config.analystOrigin;
          } catch {
            scheme = 'invalid';
          }
          if (!allowed) {
            originViolations++;
            rejectedSchemes[scheme] = (rejectedSchemes[scheme] || 0) + 1;
          }
          try {
            await interception.send(allowed ? 'Fetch.continueRequest' : 'Fetch.failRequest', {
              requestId: event.requestId, ...(allowed ? {} : { errorReason: 'BlockedByClient' }),
            });
          } catch (error) {
            interceptionFailures++;
            const message = String(error?.message || '');
            const category = /invalid interceptionid|invalid requestid/i.test(message) ? 'request_gone'
              : /session closed|target closed|has been closed/i.test(message) ? 'session_closed' : 'other';
            failureCategories[category] = (failureCategories[category] || 0) + 1;
            if (failureEvents.length < 10) failureEvents.push({
              category, timestamp_ms: Date.now(), action: allowed ? 'continue' : 'block',
              paused_at_ms: pausedAt,
              url_sha256: crypto.createHash('sha256').update(event.request.url).digest('hex'),
              has_network_id: typeof event.networkId === 'string',
              network_id: typeof event.networkId === 'string' && /^(?:\d+\.\d+|[a-f0-9]{32})$/i.test(event.networkId) ? event.networkId : null,
              resource_type: ['Document', 'Stylesheet', 'Image', 'Media', 'Font', 'Script', 'XHR', 'Fetch', 'EventSource', 'Other'].includes(event.resourceType) ? event.resourceType : 'Other',
            });
          }
        })().finally(() => pendingInterceptions.delete(handling));
        pendingInterceptions.add(handling);
        return handling;
      });
      const checkOrigin = async phase => {
        while (pendingInterceptions.size) await Promise.all([...pendingInterceptions]);
        appendJournal(fd, { event: 'origin_guard', user: index, phase, checked_requests: checkedRequests,
          rejected_requests: originViolations, rejected_schemes: rejectedSchemes,
          interception_failures: interceptionFailures, failure_categories: failureCategories, failure_events: failureEvents });
        requireThat(originViolations === 0, 'analyst_origin_violation');
        requireThat(interceptionFailures === 0, 'analyst_interception_failed');
      };
      await interception.send('Fetch.enable', { patterns: [{ urlPattern: '*', requestStage: 'Request' }] });
      stage = 'normal_login';
      await login(context, page, config.analystOrigin, options.serviceId, intent, passcode);
      stage = 'scope_verification';
      scopedBootstrap(await api(context.request, config.analystOrigin, '/api/bootstrap'), options.serviceId, options.maskIps);
      await api(context.request, config.analystOrigin, '/api/admin/share/status', { statuses: [403], json: false });
      await api(context.request, config.analystOrigin, `/api/dashboard/aggregates?service_id=${encodeURIComponent(deniedService)}`, { method: 'POST', headers: { Origin: config.analystOrigin }, data: { filters: {}, sections: ['core'] }, statuses: [403], json: false });
      await checkOrigin('scope_verification');
      ready.push({ context, page, serviceId: options.serviceId, maskIps: options.maskIps, user: index, checkOrigin });
    }
    stage = 'work';
    result = await work(ready.map(user => ({ context: user.context, page: user.page, serviceId: user.serviceId, maskIps: user.maskIps, user: user.user })));
    for (const user of ready) await user.checkOrigin('work');
  } catch (error) {
    primary = error instanceof FixtureFailure ? error.code : stage;
  } finally {
    for (const context of contexts) {
      try {
        await api(context.request, config.analystOrigin, '/api/share/logout', { method: 'POST', headers: { Origin: config.analystOrigin }, json: false });
      } catch {
        cleanup.push('logout_failed');
      }
    }
    if (admin && intents.length && fd !== undefined) await removeOwned(admin, config.adminOrigin, intents, ids, fd, cleanup);
    for (const context of contexts) {
      try { await context.close(); } catch { cleanup.push('context_close_failed'); }
    }
    for (const request of [anonymous, admin]) {
      if (request) {
        try { await request.dispose(); } catch { cleanup.push('request_dispose_failed'); }
      }
    }
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch { cleanup.push('journal_close_failed'); }
    }
    releaseJournalLock(lock, cleanup);
  }
  if (primary || cleanup.length) throw new FixtureFailure(primary || 'cleanup_failed', cleanup);
  return result;
}

async function recoverOwnedInvites(options) {
  let fd;
  let lock;
  let admin;
  let primary = null;
  const cleanup = [];
  try {
    const config = configuration(options);
    lock = acquireJournalLock(options.journalPath);
    fd = fs.openSync(options.journalPath, fs.constants.O_RDWR | fs.constants.O_APPEND | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(fd);
    requireThat(stat.isFile() && !(stat.mode & 0o077) && stat.size <= 1048576
      && (process.getuid === undefined || stat.uid === process.getuid()), 'unsafe_ownership_journal');
    const text = fs.readFileSync(fd, 'utf8');
    const complete = text.endsWith('\n') ? text : text.slice(0, text.lastIndexOf('\n') + 1);
    const events = complete.trim().split('\n').map(line => JSON.parse(line));
    const run = events[0];
    requireThat(run?.event === 'run' && /^[0-9a-f-]{36}$/.test(run.run_id) && run.admin_origin === config.adminOrigin
      && run.analyst_origin === config.analystOrigin && run.service_id === options.serviceId, 'journal_scope_mismatch');
    const settled = new Set(events.filter(event => ['invite_created', 'invite_reconciled'].includes(event.event)).map(event => event.email));
    const intents = events.filter(event => event.event === 'invite_intent').map(intent => ({ ...intent, settled: settled.has(intent.email) }));
    requireThat(intents.length <= 5 && intents.every(intent => intent.run_id === run.run_id && intent.service_id === run.service_id
      && typeof intent.mask_ips === 'boolean' && Number.isFinite(intent.started_at)
      && typeof intent.email === 'string' && intent.email.startsWith(`phase4b-${run.run_id}-`) && intent.email.endsWith('@example.com')
      && typeof intent.name === 'string' && intent.name.startsWith(`Phase4b ${run.run_id} `)), 'journal_ownership_invalid');
    const ids = new Set(events.filter(event => event.event === 'invite_created').map(event => event.id));
    requireThat([...ids].every(id => typeof id === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(id)), 'journal_identity_invalid');
    fs.ftruncateSync(fd, Buffer.byteLength(complete));
    admin = await options.request.newContext({ ...config.tls, ignoreHTTPSErrors: false });
    await removeOwned(admin, config.adminOrigin, intents, ids, fd, cleanup);
  } catch (error) {
    primary = error instanceof FixtureFailure ? error.code : 'recovery_failed';
  } finally {
    if (admin) {
      try { await admin.dispose(); } catch { cleanup.push('request_dispose_failed'); }
    }
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch { cleanup.push('journal_close_failed'); }
    }
    releaseJournalLock(lock, cleanup);
  }
  if (primary || cleanup.length) throw new FixtureFailure(primary || 'recovery_failed', cleanup);
  return { remaining: 0 };
}

module.exports = { withAnalystUsers, recoverOwnedInvites };
