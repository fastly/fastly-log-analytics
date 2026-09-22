/**
 * R-3a: spawn the FastAPI backend for the Playwright suite.
 *
 * Uses scripts/run_contract_backend.py (already-existing launcher
 * shared with the R-13 backend-contract suite) so the sandboxing
 * is centralised — CONTRACT_CONFIGS_DIR / CONTRACT_DATA_DIR redirect
 * backend.config.CONFIGS_DIR / DATA_DIR away from the dev workstation's
 * real configs. FASTLY_MOCK_MODE=1 short-circuits Fastly + NGWAF API
 * calls per R-3b.
 *
 * The Playwright backend runs on port 18004 — distinct from dev (18002)
 * and the contract suite (13003) so all three can run side-by-side
 * during local iteration.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { E2E_BACKEND_PORT, E2E_FRONTEND_PORT } from '../playwright.config'
import { backendTestEnvironment } from '../tests/backend-environment'

const HOST = '127.0.0.1'

async function poll(url: string, timeoutMs = 180_000): Promise<void> {
  // Default 180s: the playwright config webServer timeout is 120s for the
  // FRONTEND, but the BACKEND boot cost (DuckDB init + httpfs load + Iceberg
  // catalog open + every router import) is the lion's share of cold-start.
  // The prior 60s default failed on slow CI runners with the misleading
  // 'backend never returned 200' before the frontend even got a chance to
  // start. 180s clears realistic CI variance with margin.
  const deadline = Date.now() + timeoutMs
  let lastErr: unknown = null
  while (Date.now() < deadline) {
    try {
      const r = await fetch(url)
      if (r.ok) return
    } catch (e) {
      lastErr = e
    }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(
    `E2E backend never returned 200 from ${url} within ${timeoutMs}ms ` +
      `(global-setup.ts::poll timeout — separate from playwright.config.ts ` +
      `webServer.timeout). Last fetch error: ${String(lastErr)}`,
  )
}

// Stash state for global-teardown via env on the parent process.
let proc: ChildProcess | null = null
let sandbox: string | null = null

function _seedDefaultServiceConfig(configsDir: string): void {
  // Write one mock service config so /api/bootstrap surfaces a service
  // and the dashboard journeys have an active selection on first paint.
  // All fields use safe placeholder values — never any real Fastly IDs.
  //
  // `fastly_api_key` / `cdn_service_id` are deliberately left EMPTY rather
  // than fabricated placeholder strings: they were previously truthy fakes
  // ('mock-fastly-key' / 'mock-cdn-svc'), which passed `config.get_fastly_api_key`
  // / `get_fastly_logging_service_id`'s truthiness checks and caused Control
  // Room's RT poller (`backend/core/realtime/poller.py::_fetch_realtime`) to
  // make real HTTPS calls to rt.fastly.com with garbage credentials — a
  // 403-retry storm across every parallel WebKit worker. Leaving them empty
  // hits the poller's existing honest `if not api_key or not fastly_service_id:
  // return None` short-circuit, same as any real service with no Fastly
  // credentials configured yet. No spec in this suite depends on these two
  // fields being truthy for `svc-playwright-e2e`.
  const sid = 'svc-playwright-e2e'
  const config = {
    service_id: sid,
    service_name: 'Playwright E2E Service',
    fos_bucket: 'mock-bucket',
    fos_region: 'us-east-1',
    fos_access_key_id: 'AKIA_MOCK',
    fos_secret_access_key: 'SECRET_MOCK',
    fastly_api_key: '',
    cdn_service_id: '',
    cdn_secret: 'mock-cdn-secret',
    access_level: 'read_write',
    provisioning: { endpoint_name: 'Mock Logger' },
  }
  writeFileSync(join(configsDir, `${sid}.json`), JSON.stringify(config, null, 2))
}

/**
 * Optional second service config that points Control Room's RT poller at a
 * REAL Fastly service instead of synthesizing fake tick data. Opt-in only,
 * via env vars the operator exports locally before `make e2e` — never
 * committed, never a CI secret (E2E does not run in GitHub Actions).
 *
 * `get_fastly_api_key` / `get_fastly_logging_service_id` (backend/config.py)
 * read the config's own `service_id` field as the Fastly service ID sent to
 * rt.fastly.com, so this seeds a config file whose `service_id` IS the real
 * Fastly service ID — separate from the shared `svc-playwright-e2e` fixture
 * used by every other spec, so this stays purely additive and opt-in.
 *
 * When unset, `control-room.spec.ts` falls back to `svc-playwright-e2e`,
 * whose fake credentials are absent from the poller's config lookup path —
 * see `_fetch_realtime`'s `if not api_key or not fastly_service_id: return
 * None` short-circuit — so it never calls rt.fastly.com with garbage
 * credentials (the prior 403-retry-storm bug) and never fabricates data.
 */
function _seedRealRtServiceConfig(configsDir: string): void {
  const apiKey = process.env.E2E_REAL_RT_FASTLY_API_KEY
  const serviceId = process.env.E2E_REAL_RT_FASTLY_SERVICE_ID
  if (!apiKey || !serviceId) return

  const config = {
    service_id: serviceId,
    service_name: 'Playwright E2E Control Room (real RT)',
    fos_bucket: 'mock-bucket',
    fos_region: 'us-east-1',
    fos_access_key_id: 'AKIA_MOCK',
    fos_secret_access_key: 'SECRET_MOCK',
    fastly_api_key: apiKey,
    cdn_service_id: serviceId,
    cdn_secret: 'mock-cdn-secret',
    access_level: 'read_write',
    provisioning: { endpoint_name: 'Mock Logger' },
  }
  writeFileSync(join(configsDir, `${serviceId}.json`), JSON.stringify(config, null, 2))
  console.log(`[e2e] seeded real-RT service config for ${serviceId} (E2E_REAL_RT_FASTLY_* set)`)
}

async function globalSetup() {
  const repoRoot = join(__dirname, '..', '..')
  sandbox = mkdtempSync(join(tmpdir(), 'fla-playwright-'))
  const configsDir = join(sandbox, 'configs')
  const dataDir = join(sandbox, 'data')
  // mkdir is handled by run_contract_backend.py, but we need to seed
  // the configs file before the backend boots and reads them.
  // node:fs.mkdirSync would be one way; the launcher creates the dir
  // first via mkdir(parents=True), so we mirror that here.
  const fs = await import('node:fs')
  fs.mkdirSync(configsDir, { recursive: true })
  fs.mkdirSync(dataDir, { recursive: true })
  _seedDefaultServiceConfig(configsDir)
  _seedRealRtServiceConfig(configsDir)

  // Wire the analyst-OAuth feature against the in-process mock IdP (all on
  // 127.0.0.1, no network). The registry points discovery at the backend's own
  // /mock-idp routes; the browser's callback lands on the frontend proxy origin.
  const oauthRegistryPath = join(sandbox, 'oauth_providers.json')
  writeFileSync(
    oauthRegistryPath,
    JSON.stringify({
      google: {
        display_name: 'Google Workspace',
        discovery_url: `http://${HOST}:${E2E_BACKEND_PORT}/mock-idp/.well-known/openid-configuration`,
        scopes: 'openid email',
        enabled: true,
      },
    }),
  )

  proc = spawn(
    'uv',
    [
      'run',
      'python',
      'scripts/run_contract_backend.py',
      '--host',
      HOST,
      '--port',
      String(E2E_BACKEND_PORT),
    ],
    {
      cwd: repoRoot,
      env: backendTestEnvironment(sandbox, {
        backendPort: E2E_BACKEND_PORT,
        frontendPort: E2E_FRONTEND_PORT,
        registryPath: oauthRegistryPath,
      }),
      stdio: ['ignore', 'pipe', 'pipe'],
      // Detach so the child doesn't share our TTY signal group.
      detached: false,
    },
  )

  proc.stdout?.on('data', (chunk) =>
    process.stdout.write(`[e2e-backend] ${chunk}`),
  )
  proc.stderr?.on('data', (chunk) =>
    process.stderr.write(`[e2e-backend] ${chunk}`),
  )

  await poll(`http://${HOST}:${E2E_BACKEND_PORT}/api/health`, 180_000)

  ;(globalThis as { __PLAYWRIGHT_E2E_PROC?: ChildProcess }).__PLAYWRIGHT_E2E_PROC = proc
  ;(globalThis as { __PLAYWRIGHT_E2E_SANDBOX?: string }).__PLAYWRIGHT_E2E_SANDBOX = sandbox
  // Echo the frontend URL so the operator can copy/paste during --ui.
  console.log(`[e2e] backend ready on ${HOST}:${E2E_BACKEND_PORT}; frontend on ${HOST}:${E2E_FRONTEND_PORT}`)
}

export default globalSetup
