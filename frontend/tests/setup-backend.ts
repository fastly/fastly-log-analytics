/**
 * R-13: spawn a real uvicorn backend for the contract-test suite.
 *
 * The boot wires:
 *   - SERVICES_DATA_DIR / CONFIGS_DIR / NGWAF_DATA_DIR / CACHE_DATA_DIR
 *     / SYSTEM_DATA_DIR → a per-suite tmp tree so the live process
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
  sandbox = mkdtempSync(join(tmpdir(), 'fla-contract-'))
  const dataDir = join(sandbox, 'data')
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
      String(PORT),
    ],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        DEBUG_RESPONSES: 'true',
        FASTLY_MOCK_MODE: '1',
        // run_contract_backend.py reads these and patches
        // backend.config.CONFIGS_DIR / DATA_DIR (+ sub-dirs) BEFORE
        // any router loads — same pattern conftest.py uses for the
        // in-process tests, but adapted for a fresh Python process.
        CONTRACT_CONFIGS_DIR: join(sandbox, 'configs'),
        CONTRACT_DATA_DIR: dataDir,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )

  // Pipe child output to the parent so a boot failure shows up.
  proc.stdout?.on('data', (chunk) => process.stdout.write(`[backend] ${chunk}`))
  proc.stderr?.on('data', (chunk) => process.stderr.write(`[backend] ${chunk}`))

  await poll(`${CONTRACT_API_BASE}/api/health`, 30_000)
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
      /* tmpdir cleanup is best-effort */
    }
    sandbox = null
  }
}
