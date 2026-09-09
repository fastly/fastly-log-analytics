// @vitest-environment node
import { spawn, spawnSync } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('node:child_process', async (importOriginal) => ({
  ...await importOriginal<typeof import('node:child_process')>(),
  spawn: vi.fn(),
}))
vi.mock('node:fs', async (importOriginal) => ({
  ...await importOriginal<typeof import('node:fs')>(),
  mkdtempSync: vi.fn((prefix: string) => `${prefix}sandbox`),
  mkdirSync: vi.fn(),
  writeFileSync: vi.fn(),
  rmSync: vi.fn(),
}))
vi.mock('../../playwright.config', () => ({
  E2E_BACKEND_PORT: 18004,
  E2E_FRONTEND_PORT: 13004,
}))

const rejected = {
  METADATA_DSN: 'postgresql://example.invalid/operator',
  DUCKLAKE_CATALOG: 'postgres:example.invalid/operator',
  DUCKLAKE_DATA_PATH: 's3://operator-placeholder/',
  CELERY_BROKER_URL: 'redis://example.invalid/0',
  CELERY_RESULT_BACKEND: 'redis://example.invalid/1',
  CLICKHOUSE_PASSWORD: 'operator-placeholder',
  HOT_S3_SECRET: 'operator-placeholder',
  FASTLY_API_KEY: 'operator-placeholder',
  AWS_SECRET_ACCESS_KEY: 'operator-placeholder',
  REMOTE_SHARE_DB_DIR: '/operator-placeholder',
  OAUTH_PROVIDERS_CONFIG_PATH: '/operator-placeholder/oauth.json',
  OAUTH_FLOW_STATE_SECRET: 'operator-placeholder',
  OAUTH_GOOGLE_CLIENT_SECRET: 'operator-placeholder',
  UV_ENV_FILE: '/operator-placeholder/.env',
  UV_CONFIG_FILE: '/operator-placeholder/uv.toml',
  UV_PROJECT_ENVIRONMENT: '/operator-placeholder/venv',
  PYTHONPATH: '/operator-placeholder',
  PYTHONSTARTUP: '/operator-placeholder/start.py',
  NODE_OPTIONS: '--env-file=/operator-placeholder/.env',
  ENV: '/operator-placeholder/.env',
  BASH_ENV: '/operator-placeholder/.env',
  DOTENV_CONFIG_PATH: '/operator-placeholder/.env',
  HTTP_PROXY: 'http://example.invalid',
  UNKNOWN_DEPLOYMENT_SECRET: 'operator-placeholder',
}

beforeEach(() => {
  vi.resetModules()
  vi.clearAllMocks()
  for (const [key, value] of Object.entries(rejected)) vi.stubEnv(key, value)
  for (const key of ['FLA_DEV_NO_CRONS', 'FASTLY_MOCK_MODE']) vi.stubEnv(key, '0')
  vi.stubEnv('SCHEDULER_MODE', 'external')
  vi.stubEnv('INGEST_MODE', 'celery')
  vi.stubEnv('SERVING_MODE', 'durable')
  vi.stubEnv('SSE_BACKPLANE', 'valkey')
  vi.stubEnv('OTEL_EXPORTER', 'otlp')
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true }))
  vi.spyOn(console, 'log').mockImplementation(() => {})
  const child = Object.assign(new EventEmitter(), {
    stdout: new EventEmitter(),
    stderr: new EventEmitter(),
    kill: vi.fn(() => {
      queueMicrotask(() => child.emit('exit', 0))
      return true
    }),
  })
  vi.mocked(spawn).mockReturnValue(child as unknown as ReturnType<typeof spawn>)
})

afterEach(() => {
  vi.unstubAllEnvs()
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
  Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_PROC')
  Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_SANDBOX')
})

describe('backend harness child isolation', () => {
  it.each(['contract', 'e2e'])('%s rejects deployment env before spawning uv', async (harness) => {
    const contract = await import('../../tests/setup-backend')
    try {
      if (harness === 'contract') {
        await contract.startBackend()
      } else {
        const { default: globalSetup } = await import('../../e2e/global-setup')
        await globalSetup()
      }
      expect(spawn).toHaveBeenCalledOnce()
      const [command, args, options] = vi.mocked(spawn).mock.calls[0]
      expect(command).toBe('uv')
      expect(args).toContain('scripts/run_contract_backend.py')
      const env = options!.env!
      for (const [key, value] of Object.entries(rejected)) {
        expect(env[key], key).not.toBe(value)
      }
      expect(env).toMatchObject({
        PATH: process.env.PATH,
        DEBUG_RESPONSES: 'true',
        FASTLY_MOCK_MODE: '1',
        FLA_DEV_NO_CRONS: '1',
        SCHEDULER_MODE: 'inprocess',
        INGEST_MODE: 'sync',
        SERVING_MODE: 'file',
        SSE_BACKPLANE: 'local',
        OTEL_EXPORTER: 'none',
        UV_NO_ENV_FILE: '1',
        UV_NO_CONFIG: '1',
        PYTHON_DOTENV_DISABLED: '1',
      })
      expect(env.CONTRACT_CONFIGS_DIR).toBe(join(env.CONTRACT_DATA_DIR!, '..', 'configs'))
      if (harness === 'e2e') {
        expect(env).toMatchObject({
          OAUTH_MOCK_IDP: '1',
          OAUTH_MOCK_IDP_ISSUER: 'http://127.0.0.1:18004/mock-idp',
          OAUTH_REDIRECT_BASE: 'http://127.0.0.1:13004',
          OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
          OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
        })
      } else {
        expect(Object.keys(env).filter((key) => key.startsWith('OAUTH_'))).toEqual([])
      }
      // Verify the actual environment seen by a fresh process, without a server.
      const child = spawnSync(process.execPath, ['-e', 'console.log(JSON.stringify(process.env))'], {
        env,
        encoding: 'utf8',
      })
      expect(child.status, child.stderr).toBe(0)
      expect(JSON.parse(child.stdout)).toMatchObject(env)
    } finally {
      await contract.stopBackend()
    }
  })
})
