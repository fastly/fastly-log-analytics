import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

type Suite = { file?: string; suites?: Suite[] }

function discover(env: Record<string, string>, project = 'chromium'): string[] {
  const root = resolve(__dirname, '../..')
  const report = JSON.parse(
    execFileSync(
      process.execPath,
      ['node_modules/playwright/cli.js', 'test', '--list', '--reporter=json', `--project=${project}`],
      {
        cwd: root,
        env: {
          ...process.env,
          FLP_E2E_NATIVE: '0',
          PLAYWRIGHT_BASE_URL: '',
          PROJECTS: '',
          ...env,
        },
        encoding: 'utf8',
        timeout: 30_000,
      },
    ),
  ) as { suites: Suite[] }
  const files = new Set<string>()
  function collect(suites: Suite[]) {
    for (const suite of suites) {
      if (suite.file) files.add(suite.file.replaceAll('\\', '/'))
      collect(suite.suites ?? [])
    }
  }
  collect(report.suites)
  return [...files]
}

describe.each(['', 'https://fixture.example.test'])('Playwright routing (base URL: %s)', (baseURL) => {
  it('keeps Cloud-only contracts out of Standard discovery', () => {
    const files = discover({ PLAYWRIGHT_BASE_URL: baseURL })
    expect(files.length).toBeGreaterThan(0)
    expect(files.filter((file) => file.includes('cloud/'))).toEqual([])
  }, 45_000)

  it('keeps all dedicated Cloud contracts in native discovery, without Standard journeys', () => {
    const files = discover({ FLP_E2E_NATIVE: '1', PLAYWRIGHT_BASE_URL: baseURL })
    expect(files.map((file) => file.split('/').slice(-2).join('/')).sort()).toEqual([
      'cloud/csp.spec.ts',
      'cloud/engine.spec.ts',
      'cloud/runtime.spec.ts',
    ])
  }, 45_000)
})

it.each(['firefox', 'webkit'])('discovers the nightly %s project', (browser) => {
  const files = discover({ PROJECTS: browser }, browser)
  expect(files.length).toBeGreaterThan(0)
  expect(files.filter((file) => file.includes('cloud/'))).toEqual([])
}, 45_000)
