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
  const sid = 'svc-playwright-e2e'
  const config = {
    service_id: sid,
    service_name: 'Playwright E2E Service',
    fos_bucket: 'mock-bucket',
    fos_region: 'us-east-1',
    fos_access_key_id: 'AKIA_MOCK',
    fos_secret_access_key: 'SECRET_MOCK',
    fastly_api_key: 'mock-fastly-key',
    cdn_service_id: 'mock-cdn-svc',
    cdn_secret: 'mock-cdn-secret',
    access_level: 'read_write',
    provisioning: { endpoint_name: 'Mock Logger' },
  }
  writeFileSync(join(configsDir, `${sid}.json`), JSON.stringify(config, null, 2))
}

async function globalSetup() {
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
  // The share DB resolves its dir from REMOTE_SHARE_DB_DIR (default literal
  // ``data/system``) — it does NOT honor CONTRACT_DATA_DIR's svcconfig patch —
  // so without this the e2e backend would read/write the developer's REAL
  // remote_share.db. Sandbox it explicitly.
  const shareDbDir = join(dataDir, 'system')
  fs.mkdirSync(shareDbDir, { recursive: true })
  _seedDefaultServiceConfig(configsDir)

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

  const repoRoot = join(__dirname, '..', '..')

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
      env: {
        ...process.env,
        DEBUG_RESPONSES: 'true',
        FASTLY_MOCK_MODE: '1',
        CONTRACT_CONFIGS_DIR: configsDir,
        CONTRACT_DATA_DIR: dataDir,
        // Isolate the share DB from the developer's real data/system (see above).
        REMOTE_SHARE_DB_DIR: shareDbDir,
        // Analyst OAuth against the in-process mock IdP.
        OAUTH_MOCK_IDP: '1',
        OAUTH_MOCK_IDP_ISSUER: `http://${HOST}:${E2E_BACKEND_PORT}/mock-idp`,
        OAUTH_PROVIDERS_CONFIG_PATH: oauthRegistryPath,
        OAUTH_FLOW_STATE_SECRET: 'e2e-oauth-flow-state-secret-0123456789',
        OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
        OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
        OAUTH_REDIRECT_BASE: `http://${HOST}:${E2E_FRONTEND_PORT}`,
      },
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
