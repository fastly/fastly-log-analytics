const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');
const { withAnalystUsers, recoverOwnedInvites } = require('../../scripts/lib/analyst_fixture.cjs');

const adminOrigin = 'https://admin.example.com';
const analystOrigin = 'https://analyst.example.com';
const serviceId = 'service-test';

function fixture(t, overrides = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'phase4b-analyst-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  for (const name of ['cert', 'key', 'ca']) fs.writeFileSync(path.join(dir, name), 'nonsecret test fixture', { mode: 0o600 });
  const state = { invites: [], sessions: [], adminOptions: [], analystOptions: [], contexts: [], mutations: [], closed: 0, passcodes: [], cookieValues: [], ...overrides };
  const env = { ADMIN_GATEWAY_ENDPOINTS: JSON.stringify({ test: { origin: adminOrigin, cert: path.join(dir, 'cert'), key: path.join(dir, 'key'), ca: path.join(dir, 'ca') } }) };
  function response(url, status, body) {
    return { url: () => url, status: () => status, json: async () => body, dispose: async () => {} };
  }
  function api(isAdmin, context) {
    return {
      dispose: async () => { state.closed++; },
      async fetch(url, options) {
        assert.equal(options.maxRedirects, 0);
        const endpoint = new URL(url).pathname;
        if (isAdmin && endpoint === '/api/admin/share/status') return response(url, 200, { sharing_active: state.active !== false, public_endpoint: analystOrigin, services: [{ service_id: serviceId }, { service_id: 'service-other' }], invites: state.invites, sessions: state.sessions, max_concurrent_sessions: 10, active_session_count: state.sessions.length });
        if (isAdmin && endpoint === '/api/admin/share/invites' && options.method === 'POST') {
          const payload = options.data;
          state.mutations.push('create');
          state.passcodes.push(payload.passcode);
          const invite = { ...payload, id: `invite-${state.invites.length}`, expires_at: new Date(Date.now() + 3600000).toISOString(), passcode_hash: 'PRIVATE_HASH' };
          if (state.delayedCreate) { state.delayed = invite; throw new Error('private create timeout'); }
          state.invites.push(invite);
          if (state.ambiguousCreate) throw new Error('private upstream failure');
          return response(url, 200, invite);
        }
        if (isAdmin && endpoint === '/api/admin/share/invites') return response(url, 200, { invites: state.invites });
        if (isAdmin && endpoint.startsWith('/api/admin/share/invites/') && options.method === 'DELETE') {
          const id = decodeURIComponent(endpoint.split('/').at(-1));
          state.mutations.push(`delete:${id}`);
          state.invites = state.invites.filter(row => row.id !== id);
          state.sessions = state.sessions.filter(row => row.invite_id !== id);
          return response(url, 200, { ok: true });
        }
        if (!isAdmin && endpoint === '/api/share/auth-config') return response(url, 200, { passcode_enabled: state.passcodesEnabled !== false });
        if (!isAdmin && endpoint === '/api/bootstrap') return response(url, 200, { active_service_id: serviceId, services: [{ service_id: state.badScope ? 'service-other' : serviceId }], settings: { is_remote_analyst: !state.badBootstrap, mask_ips: state.badMask ? !context.invite.pii_policy.mask_ips : context.invite.pii_policy.mask_ips } });
        if (!isAdmin && endpoint === '/api/admin/share/status') return response(url, 403, {});
        if (!isAdmin && endpoint === '/api/dashboard/aggregates') {
          assert.equal(options.headers?.Origin, analystOrigin);
          return response(url, 403, {});
        }
        if (!isAdmin && endpoint === '/api/share/logout') {
          if (options.headers?.Origin !== analystOrigin) return response(url, 403, { error: 'origin_not_allowed' });
          state.logoutOrigins ??= [];
          state.logoutOrigins.push(options.headers.Origin);
          if (state.failLogout) throw new Error('private logout failure');
          return response(url, 200, { ok: true });
        }
        throw new Error(`unexpected fixture endpoint ${endpoint}`);
      },
    };
  }
  const request = { async newContext(options) { state.adminOptions.push(options); return api(Boolean(options.clientCertificates?.length), null); } };
  const browser = { async newContext(options) {
    state.analystOptions.push(options);
    const context = { currentUrl: `${analystOrigin}/share-login`, values: {}, cookiesList: [], async close() { state.closed++; }, async route(pattern, handler) { this.routeGuard = handler; }, async cookies() { return this.cookiesList; }, setDefaultTimeout() {}, setDefaultNavigationTimeout() {} };
    context.request = api(false, context);
    context.newCDPSession = async () => ({
      on(event, handler) { assert.equal(event, 'Fetch.requestPaused'); context.requestGuard = handler; },
      async send(method, args) {
        context.guardCommand = { method, args };
        if (method === 'Fetch.continueRequest' && state.continueError) {
          if (state.delayContinue) await new Promise(resolve => setImmediate(resolve));
          throw new Error(state.continueError);
        }
      },
    });
    const page = {
      goto: async url => { context.currentUrl = url; }, url: () => context.currentUrl,
      waitForURL: async predicate => { assert.ok(typeof predicate === 'function' ? predicate(new URL(context.currentUrl)) : context.currentUrl.includes(predicate)); },
      waitForResponse: predicate => new Promise(resolve => { context.pending = { predicate, resolve }; }),
      getByLabel: label => ({ fill: async value => { context.values[label] = value; } }),
      getByTestId: testId => ({ locator: () => ({ click: () => { assert.equal(testId, 'share-login-form'); return login(); } }), click: () => acknowledge(), waitFor: async () => {} }),
      locator: () => ({ click: () => login() }),
    };
    function cookie(name, value) { state.cookieValues.push(value); return { name, value, secure: true, httpOnly: true, sameSite: 'Strict' }; }
    function complete(endpoint, body) { const res = { ...response(`${analystOrigin}${endpoint}`, 200, body), request: () => ({ method: () => 'POST' }) }; assert.ok(context.pending.predicate(res)); context.pending.resolve(res); }
    async function login() {
      context.invite = state.invites.find(row => row.email === context.values.Email);
      assert.equal(context.values.Passcode, context.invite.passcode);
      const pending = state.tosPending === true;
      context.cookiesList = [cookie(pending ? 'analyst_pending_session_id' : 'analyst_session_id', `cookie-${context.invite.id}`)];
      state.sessions.push({ invite_id: context.invite.id });
      context.currentUrl = pending ? `${analystOrigin}/share-login/acknowledge` : `${analystOrigin}/dashboard?service=${serviceId}`;
      complete('/api/share/login', { tos_pending: pending });
    }
    async function acknowledge() {
      context.cookiesList = [cookie('analyst_session_id', state.noRotation ? `cookie-${context.invite.id}` : `rotated-${context.invite.id}`)];
      context.currentUrl = `${analystOrigin}/dashboard?service=${serviceId}`;
      complete('/api/share/acknowledge', { ok: true });
    }
    context.newPage = async () => page;
    state.contexts.push(context);
    return context;
  } };
  return { state, api, options: { browser, request, adminOrigin, analystOrigin, serviceId, env, users: 1, maskIps: true, journalPath: path.join(dir, 'ownership.jsonl') } };
}

for (const tosPending of [false, true]) test(`normal login${tosPending ? ' and TOS rotation' : ''} stays isolated and cleans owned state`, async t => {
  const { state, options } = fixture(t, { tosPending });
  const result = await withAnalystUsers(options, async users => { assert.equal(users.length, 1); assert.equal(users[0].maskIps, true); return { checked: true }; });
  assert.deepEqual(result, { checked: true });
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
  assert.equal(state.mutations.length, 2);
  assert.deepEqual(state.logoutOrigins, [analystOrigin]);
  assert.ok(state.adminOptions[0].clientCertificates.length === 1);
  assert.ok(state.analystOptions.every(row => row.ignoreHTTPSErrors === false && row.clientCertificates.length === 0 && row.serviceWorkers === 'block'));
  const journal = fs.readFileSync(options.journalPath, 'utf8');
  for (const secret of [...state.passcodes, ...state.cookieValues, 'PRIVATE_HASH']) assert.equal(journal.includes(secret), false);
  assert.ok(journal.includes('cleanup_verified'));
});

for (const failure of ['active', 'passcodesEnabled', 'badBootstrap', 'ambiguousCreate', 'failLogout']) test(`failure ${failure} never widens sharing and preserves cleanup`, async t => {
  const value = ['active', 'passcodesEnabled'].includes(failure) ? false : true;
  const { state, options } = fixture(t, { [failure]: value });
  await assert.rejects(withAnalystUsers(options, async () => {}), error => !error.message.includes('private'));
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
  if (failure === 'active' || failure === 'passcodesEnabled') assert.equal(state.mutations.length, 0);
  assert.ok(state.mutations.every(row => row === 'create' || row.startsWith('delete:')));
});

for (const failure of ['badMask', 'badScope', 'noRotation']) test(`invalid analyst boundary ${failure} fails and cleans owned state`, async t => {
  const { state, options } = fixture(t, { [failure]: true, tosPending: failure === 'noRotation' });
  await assert.rejects(withAnalystUsers(options, async () => {}));
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
});

test('recovery reconciles an interrupted create by exact run ownership only', async t => {
  const { state, options } = fixture(t);
  await withAnalystUsers(options, async () => {});
  const intent = fs.readFileSync(options.journalPath, 'utf8').trim().split('\n').map(line => JSON.parse(line)).find(row => row.event === 'invite_intent');
  state.invites.push({ id: 'owned-orphan', name: intent.name, email: intent.email, service_ids: [serviceId], pii_policy: { mask_ips: intent.mask_ips }, allow_concurrent_sessions: false, auth_method: 'passcode', expires_at: new Date(intent.started_at + 3600000).toISOString() });
  state.sessions.push({ invite_id: 'owned-orphan' });
  state.invites.push({ id: 'unrelated', name: 'operator', email: 'operator@example.com', service_ids: [serviceId] });
  fs.appendFileSync(options.journalPath, '{"event":"invite_created","id":');
  const result = await recoverOwnedInvites(options);
  assert.equal(result.remaining, 0);
  assert.equal(state.invites.length, 1);
  assert.equal(state.invites[0].id, 'unrelated');
  assert.equal(state.sessions.length, 0);
});

test('ownership intents remain complete when a filesystem write is short', async t => {
  const { options } = fixture(t);
  const write = fs.writeSync;
  const writeFile = fs.writeFileSync;
  let injected = false;
  let delegated = false;
  fs.writeFileSync = (fd, data, ...rest) => {
    if (data.toString().includes('"invite_intent"')) delegated = true;
    return writeFile(fd, data, ...rest);
  };
  fs.writeSync = (fd, data, ...rest) => {
    const bytes = Buffer.isBuffer(data) ? data.subarray(rest[0] || 0, (rest[0] || 0) + (rest[1] ?? data.length)) : Buffer.from(data);
    if (!injected && bytes.toString().includes('"invite_intent"')) {
      injected = true;
      return Buffer.isBuffer(data) ? write(fd, data, rest[0] || 0, 10, rest[2]) : write(fd, data.slice(0, 10), ...rest);
    }
    return write(fd, data, ...rest);
  };
  t.after(() => { fs.writeSync = write; fs.writeFileSync = writeFile; });
  await withAnalystUsers(options, async () => {});
  assert.ok(injected || delegated);
  const events = fs.readFileSync(options.journalPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(events.filter(row => row.event === 'invite_intent').length, 1);
  assert.equal(events.at(-1).event, 'cleanup_verified');
});

test('an absent timed-out create remains unresolved until its durable row is observed', async t => {
  const { state, options } = fixture(t, { delayedCreate: true });
  await assert.rejects(withAnalystUsers(options, async () => {}), error => error.cleanup.includes('create_completion_unresolved'));
  const before = fs.readFileSync(options.journalPath, 'utf8');
  assert.equal(before.includes('cleanup_verified'), false);
  state.invites.push(state.delayed);
  await recoverOwnedInvites(options);
  assert.equal(state.invites.length, 0);
  assert.ok(fs.readFileSync(options.journalPath, 'utf8').includes('cleanup_verified'));
});

test('recovery cannot mutate a journal while its fixture is running', async t => {
  const { options } = fixture(t);
  await withAnalystUsers(options, async () => {
    const before = fs.readFileSync(options.journalPath, 'utf8');
    await assert.rejects(recoverOwnedInvites(options), /journal_lock_held/);
    assert.equal(fs.readFileSync(options.journalPath, 'utf8'), before);
  });
});

test('two unmasked users get separate invites and cleanup after callback failure', async t => {
  const { state, options } = fixture(t);
  await assert.rejects(withAnalystUsers({ ...options, users: 2, maskIps: false }, async users => {
    assert.equal(users.length, 2);
    assert.ok(users.every(user => user.maskIps === false));
    assert.notEqual(users[0].context, users[1].context);
    throw new Error(state.passcodes[0]);
  }), error => !error.message.includes(state.passcodes[0]));
  assert.equal(new Set(state.passcodes).size, 2);
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
  assert.equal(state.contexts.length, 2);
});

test('cross-origin browser requests abort and invalidate the lifecycle', async t => {
  const { state, options } = fixture(t);
  let aborted = false;
  await assert.rejects(withAnalystUsers(options, async users => {
    await users[0].context.requestGuard({ requestId: 'request-test', request: { url: 'https://unexpected.example.com/' } });
    aborted = users[0].context.guardCommand.method === 'Fetch.failRequest';
  }), /analyst_origin_violation/);
  assert.equal(aborted, true);
  assert.equal(state.invites.length, 0);
  const journal = fs.readFileSync(options.journalPath, 'utf8');
  const observation = journal.trim().split('\n').map(line => JSON.parse(line)).find(row => row.event === 'origin_guard' && row.phase === 'work');
  assert.equal(observation.rejected_requests, 1);
  assert.deepEqual(observation.rejected_schemes, { 'https:': 1 });
  assert.equal(observation.interception_failures, 0);
  assert.equal(journal.includes('unexpected.example.com'), false);
});

for (const delayContinue of [false, true]) test(`interception failures ${delayContinue ? 'pending at callback completion' : 'already observed'} fail closed without claiming an origin violation`, async t => {
  const { state, options } = fixture(t, { continueError: 'Invalid InterceptionId. PRIVATE_CDP_DETAIL', delayContinue });
  await assert.rejects(withAnalystUsers(options, async users => {
    const handling = users[0].context.requestGuard({ requestId: 'request-test', networkId: delayContinue ? 'PRIVATE_CDP_DETAIL' : '42.7', resourceType: delayContinue ? 'PRIVATE_CDP_DETAIL' : 'Fetch', request: { url: `${analystOrigin}/dashboard?private=value` } });
    if (!delayContinue) await handling;
  }), error => error.code === 'analyst_interception_failed' && !error.message.includes('PRIVATE'));
  const journal = fs.readFileSync(options.journalPath, 'utf8');
  const observation = journal.trim().split('\n').map(line => JSON.parse(line)).find(row => row.event === 'origin_guard' && row.phase === 'work');
  assert.equal(observation.rejected_requests, 0);
  assert.equal(observation.interception_failures, 1);
  assert.deepEqual(observation.failure_categories, { request_gone: 1 });
  assert.equal(observation.failure_events.length, 1);
  assert.deepEqual({ ...observation.failure_events[0], timestamp_ms: 0, paused_at_ms: 0 }, {
    category: 'request_gone', network_id: delayContinue ? null : '42.7', resource_type: delayContinue ? 'Other' : 'Fetch',
    has_network_id: true, action: 'continue', timestamp_ms: 0, paused_at_ms: 0,
    url_sha256: crypto.createHash('sha256').update(`${analystOrigin}/dashboard?private=value`).digest('hex'),
  });
  assert.ok(Number.isFinite(observation.failure_events[0].timestamp_ms));
  assert.ok(observation.failure_events[0].paused_at_ms <= observation.failure_events[0].timestamp_ms);
  assert.equal(journal.includes('PRIVATE_CDP_DETAIL'), false);
  assert.equal(journal.includes('private=value'), false);
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
  assert.ok(journal.includes('cleanup_verified'));
});

test('malformed recovery input cannot disclose its contents through errors', async t => {
  const { options } = fixture(t);
  fs.writeFileSync(options.journalPath, 'PRIVATE\n', { mode: 0o600 });
  await assert.rejects(recoverOwnedInvites(options), error => error.code === 'recovery_failed' && !error.message.includes('PRIVATE'));
});

for (const scenario of ['normal', 'redirect', 'profile', 'profile-redirect']) test(`real Chromium ${scenario === 'redirect' ? 'blocks cross-origin redirects before credentials' : scenario === 'profile-redirect' ? 'blocks cross-origin navigation after profiling-session detach' : scenario === 'profile' ? 'keeps the origin guard through traced inline assets and reloads' : 'completes the normal form flow'}`, async t => {
  const redirectLogin = scenario === 'redirect';
  const { chromium } = require('../../frontend/node_modules/@playwright/test');
  const browser = await chromium.launch({ headless: true });
  t.after(() => browser.close());
  const { state, api, options } = fixture(t, { redirectLogin });
  const realBrowser = { async newContext(contextOptions) {
    state.analystOptions.push(contextOptions);
    const context = await browser.newContext(contextOptions);
    const identity = {};
    context.on('requestfailed', request => {
      if (new URL(request.url()).origin === 'https://unexpected.invalid') state.redirectFailure = request.failure().errorText;
    });
    await context.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.pathname === '/api/share/login') {
        const payload = route.request().postDataJSON();
        identity.invite = state.invites.find(invite => invite.email === payload.email && invite.passcode === payload.passcode);
        assert.ok(identity.invite);
        state.sessions.push({ invite_id: identity.invite.id });
        await route.fulfill({ status: 200, headers: { 'content-type': 'application/json', 'set-cookie': 'analyst_session_id=browser-fixture; Path=/; Secure; HttpOnly; SameSite=Strict' }, body: JSON.stringify({ tos_pending: false }) });
      } else if (url.pathname === '/share-login' && redirectLogin) {
        await route.fulfill({ status: 302, headers: { location: 'https://unexpected.invalid/' } });
      } else if (url.pathname === '/share-login') {
        await route.fulfill({ contentType: 'text/html', body: `<form data-testid="share-login-form"><label for="email">Email</label><input id="email"><label for="passcode">Passcode</label><input id="passcode" type="password"><button type="submit">Sign in</button></form><script>document.querySelector('form').addEventListener('submit', async event => { event.preventDefault(); const response = await fetch('/api/share/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: document.querySelector('#email').value, passcode: document.querySelector('#passcode').value }) }); if (response.ok) location.assign('/dashboard?service=service-test'); });</script>` });
      } else {
        await route.fulfill({ contentType: 'text/html', body: '<h1>Analyst fixture</h1><img alt="Inline chart" src="data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%221%22 height=%221%22%3E%3C/svg%3E">' });
      }
    });
    const requests = api(false, identity);
    return new Proxy(context, { get(target, key) {
      if (key === 'request') return requests;
      if (key === 'newCDPSession') return async page => {
        const session = await target.newCDPSession(page);
        session.on('Fetch.requestPaused', event => { (state.pausedOrigins ??= []).push(new URL(event.request.url).origin); });
        const send = session.send.bind(session);
        session.send = async (method, args) => {
          let result;
          try { result = await send(method, args); }
          catch (error) { (state.guardErrors ??= []).push({ method, message: error.message }); throw error; }
          if (method === 'Fetch.failRequest' && args.errorReason === 'BlockedByClient') state.redirectBlocked = true;
          return result;
        };
        return session;
      };
      const value = Reflect.get(target, key, target); return typeof value === 'function' ? value.bind(target) : value;
    } });
  } };
  const run = withAnalystUsers({ ...options, browser: realBrowser }, async users => {
    assert.equal(redirectLogin, false);
    assert.equal(await users[0].page.locator('h1').textContent(), 'Analyst fixture');
    const cookies = await users[0].context.cookies(analystOrigin);
    assert.equal(cookies.find(cookie => cookie.name === 'analyst_session_id').secure, true);
    if (scenario.startsWith('profile')) {
      for (const traced of [true, false]) {
        const cdp = await users[0].context.newCDPSession(users[0].page);
        await cdp.send('Performance.enable');
        if (traced) {
          await users[0].context.tracing.start({ screenshots: true, snapshots: true, sources: false });
          await browser.startTracing(users[0].page, { screenshots: true });
        }
        try {
          await users[0].page.reload({ waitUntil: 'load' });
          await users[0].page.getByRole('img', { name: 'Inline chart' }).evaluate(image => image.decode());
        } finally {
          if (traced) {
            await browser.stopTracing();
            await users[0].context.tracing.stop();
          }
          await cdp.detach();
        }
      }
      assert.deepEqual(state.guardErrors ?? [], []);
    }
    if (scenario === 'profile-redirect') await users[0].page.goto('https://unexpected.invalid/after-profile');
  });
  if (redirectLogin || scenario === 'profile-redirect') {
    await assert.rejects(run);
    assert.equal(state.redirectBlocked, true);
    assert.ok(state.pausedOrigins.includes('https://unexpected.invalid'));
    assert.ok(['net::ERR_ABORTED', 'net::ERR_BLOCKED_BY_CLIENT'].includes(state.redirectFailure));
  } else {
    await run;
    const observations = fs.readFileSync(options.journalPath, 'utf8').trim().split('\n').map(line => JSON.parse(line)).filter(row => row.event === 'origin_guard');
    assert.deepEqual(observations.map(row => row.phase), ['scope_verification', 'work']);
    assert.ok(observations.every(row => row.checked_requests > 0 && row.rejected_requests === 0 && row.interception_failures === 0));
  }
  assert.equal(state.invites.length, 0);
  assert.equal(state.sessions.length, 0);
});

for (const variable of ['DEBUG', 'PWDEBUG']) test(`credential logging mode ${variable} fails before invite creation`, async t => {
  const previous = process.env[variable];
  process.env[variable] = variable === 'DEBUG' ? 'pw:api' : '1';
  t.after(() => { if (previous === undefined) delete process.env[variable]; else process.env[variable] = previous; });
  const { state, options } = fixture(t);
  await assert.rejects(withAnalystUsers(options, async () => {}), /credential_debug_logging_forbidden/);
  assert.equal(state.mutations.length, 0);
});

test('HTTP and unconfigured administrator origins fail before invite creation', async t => {
  const { state, options } = fixture(t);
  for (const changes of [{ analystOrigin: 'http://analyst.example.com' }, { adminOrigin: 'https://unknown.example.com' }]) await assert.rejects(withAnalystUsers({ ...options, ...changes }, async () => {}));
  assert.equal(state.mutations.length, 0);
});
