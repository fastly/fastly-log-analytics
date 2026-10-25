/**
 * R-13: spawn a real uvicorn backend for the contract-test suite.
 *
 * The boot wires:
 *   - SERVICES_DATA_DIR / CONFIGS_DIR / NGWAF_DATA_DIR / CACHE_DATA_DIR
 *     / SYSTEM_DATA_DIR and CWD → a per-suite sandbox so the test process
 *     doesn't write into the dev workstation's real data/configs.
 *   - DEBUG_RESPONSES=true so the contract assertions can also pin
 *     the telemetry envelope shape if a future test wants to.
 *   - FASTLY_MOCK_MODE=1 (lands in Phase 3 R-3b). The contract tests
 *     only hit endpoints that don't talk to Fastly today, but the
 *     env var stays set defensively so the day Phase 3 lands the
 *     contract suite picks up the mock layer automatically.
 *
 * Lifecycle: `startBackend()` resolves once /api/health returns 200,
 * `stopBackend()` SIGTERMs the process and waits for exit. Bind to
 * 127.0.0.1:13003 (the audit-prescribed contract-test port — distinct
 * from dev 18002 and Playwright 18004 so concurrent invocations don't
 * collide).
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const PORT = 13003
const HOST = '127.0.0.1'
export const CONTRACT_API_BASE = `http://${HOST}:${PORT}`

// Do not copy process.env: deployment DSNs, credentials and uv/Python env-file
// loaders must be removed BEFORE uv starts, not after Python imports config.
const HOST_ENV_KEYS = [
  'PATH', 'HOME', 'USER', 'LOGNAME',
  'LANG', 'LC_ALL', 'LC_CTYPE', 'TZ',
  'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'PATHEXT',
] as const

interface MockOAuth {
  backendPort: number
  frontendPort: number
  registryPath: string
}

export function backendTestEnvironment(
  sandbox: string,
  oauth?: MockOAuth,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { NODE_ENV: 'test' }
  for (const key of HOST_ENV_KEYS) {
    if (process.env[key] !== undefined) env[key] = process.env[key]
  }
  const metaDsn = process.env.TEST_METADATA_DSN || (process.env.METADATA_DSN && !process.env.METADATA_DSN.includes('example.invalid') ? process.env.METADATA_DSN : 'postgresql://fla:fla_test_password@localhost:5432/ducklake_test')
  const duckCatalog = process.env.TEST_DUCKLAKE_CATALOG || (process.env.DUCKLAKE_CATALOG && !process.env.DUCKLAKE_CATALOG.includes('example.invalid') ? process.env.DUCKLAKE_CATALOG : metaDsn)

  Object.assign(env, {
    UV_NO_ENV_FILE: '1',
    UV_NO_CONFIG: '1',
    PYTHON_DOTENV_DISABLED: '1',
    DEBUG_RESPONSES: 'true',
    FASTLY_MOCK_MODE: '1',
    FLA_DEV_NO_CRONS: '1',
    FLA_DEV_LOCAL_CRONS: '0',
    DEPLOYMENT_MODE: 'standard',
    SSE_BACKPLANE: 'local',
    OTEL_EXPORTER: 'none',
    DUCKDB_POOL_MAX_SIZE: '16',
    CONTRACT_CONFIGS_DIR: join(sandbox, 'configs'),
    CONTRACT_DATA_DIR: join(sandbox, 'data'),
    METADATA_DSN: metaDsn,
    DUCKLAKE_CATALOG: duckCatalog,
  })
  if (oauth) {
    Object.assign(env, {
      OAUTH_MOCK_IDP: '1',
      OAUTH_MOCK_IDP_ISSUER: `http://127.0.0.1:${oauth.backendPort}/mock-idp`,
      OAUTH_PROVIDERS_CONFIG_PATH: oauth.registryPath,
      OAUTH_FLOW_STATE_SECRET: 'e2e-oauth-flow-state-secret-0123456789',
      OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
      OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
      OAUTH_REDIRECT_BASE: `http://127.0.0.1:${oauth.frontendPort}`,
    })
  }
  return env
}

let proc: ChildProcess | null = null
let sandbox: string | null = null

async function poll(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastErr: unknown = null
  while (Date.now() < deadline) {
    try {
      const r = await fetch(url)
      if (r.ok) return
    } catch (e) {
      lastErr = e
    }
    await new Promise((r) => setTimeout(r, 250))
  }
  throw new Error(`Backend never returned 200 from ${url}: ${String(lastErr)}`)
}

export async function startBackend(): Promise<void> {
  if (proc) return
  const repoRoot = join(__dirname, '..', '..', '..')
  sandbox = mkdtempSync(join(tmpdir(), 'fla-contract-'))

  proc = spawn(
    'uv',
    [
      'run',
      '--project',
      repoRoot,
      'python',
      'scripts/run_contract_backend.py',
      '--host',
      HOST,
      '--port',
      String(PORT),
    ],
    {
      cwd: repoRoot,
      env: backendTestEnvironment(sandbox),
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )

  // Pipe child output to the parent so a boot failure shows up.
  proc.stdout?.on('data', (chunk) => process.stdout.write(`[backend] ${chunk}`))
  proc.stderr?.on('data', (chunk) => process.stderr.write(`[backend] ${chunk}`))

  await poll(`${CONTRACT_API_BASE}/api/health`, 120_000)
}

export async function stopBackend(): Promise<void> {
  if (!proc) return
  const p = proc
  proc = null
  p.kill('SIGTERM')
  await new Promise<void>((resolve) => {
    const t = setTimeout(() => {
      p.kill('SIGKILL')
      resolve()
    }, 5_000)
    p.once('exit', () => {
      clearTimeout(t)
      resolve()
    })
  })
  if (sandbox) {
    try {
      rmSync(sandbox, { recursive: true, force: true })
    } catch {
      /* Sandbox cleanup is best-effort. */
    }
    sandbox = null
  }
}
