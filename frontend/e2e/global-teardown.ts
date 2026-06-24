/**
 * R-3a: kill the spawned backend + remove the tmp configs/data sandbox.
 */
import type { ChildProcess } from 'node:child_process'
import { rmSync } from 'node:fs'

async function globalTeardown() {
  const proc = (globalThis as { __PLAYWRIGHT_E2E_PROC?: ChildProcess }).__PLAYWRIGHT_E2E_PROC
  if (proc) {
    proc.kill('SIGTERM')
    await new Promise<void>((resolve) => {
      const t = setTimeout(() => {
        proc.kill('SIGKILL')
        resolve()
      }, 5_000)
      proc.once('exit', () => {
        clearTimeout(t)
        resolve()
      })
    })
  }
  const sandbox = (globalThis as { __PLAYWRIGHT_E2E_SANDBOX?: string }).__PLAYWRIGHT_E2E_SANDBOX
  if (sandbox) {
    try {
      rmSync(sandbox, { recursive: true, force: true })
    } catch {
      /* best-effort */
    }
  }
}

export default globalTeardown
