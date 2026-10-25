/**
 * @vitest-environment jsdom
 *
 * Error-path coverage (TESTING_PLAN_3 item 14).
 *
 * Until now every test mocked 2xx responses. That meant the openapi-fetch
 * ``onResponse`` middleware (extractApiError + throw on !response.ok),
 * extractApiError's FastAPI-422 shape unwrapping, and react-query's
 * ``isError`` propagation into the UI were all dead code under test.
 *
 * This file exercises three concrete scenarios end-to-end against MSW:
 *
 *   - openapi-fetch error middleware extracts a plain ``detail`` string
 *     from a 500.
 *   - openapi-fetch error middleware unwraps the FastAPI 422 array shape
 *     (``[{loc, msg}]``) into a human-readable string.
 *   - A ``useQuery`` consumer's ``isError`` flips to ``true`` on 401 —
 *     no silent failure, no infinite loading state.
 *
 * IMPLEMENTATION NOTE: imports of ``@/lib/api`` are dynamic (inside each
 * test body), not static at module load. openapi-fetch captures
 * ``globalThis.fetch`` at ``createClient`` time, and MSW patches
 * ``globalThis.fetch`` from ``beforeAll`` (in vitest.setup.ts). A static
 * import would race the setup and the client would hold an unpatched
 * fetch reference — silently letting all requests fall through to the
 * real ``http://127.0.0.1:8000`` if a dev server is running. Same pattern
 * as ``__tests__/hooks/useBootstrap.test.ts``.
 */
import { renderHook, waitFor } from '@testing-library/react'
import { useQuery } from '@tanstack/react-query'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import { http, HttpResponse } from 'msw'
import childProcess, { ChildProcess, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import { syncBuiltinESMExports } from 'node:module'
import { EventEmitter } from 'node:events'
import { join } from 'node:path'
import { afterEach, describe, it, expect, beforeEach, vi } from 'vitest'

import { server } from '@/__tests__/helpers/msw/server'

vi.mock('../../playwright.config', () => ({
  E2E_BACKEND_PORT: 18004,
  E2E_FRONTEND_PORT: 13004,
}))

vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'test-svc' }
  const useServiceStore = Object.assign(
    vi.fn((selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state)),
    { getState: () => state },
  )
  return { useServiceStore }
})

const API_BASE = 'http://127.0.0.1:8000'

function wrapper() {
  const qc = createTestQueryClient({ queries: { gcTime: 0 } })
  return makeQueryWrapper(qc)
}

describe('extractApiError — unit coverage of error shapes', () => {
  it('returns the string for {detail: "..."}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: 'bucket not found' })).toBe('bucket not found')
  })

  it('joins FastAPI 422 array shape {detail: [{loc, msg}]}', async () => {
    const { extractApiError } = await import('@/lib/api')
    const result = extractApiError({
      detail: [
        { loc: ['body', 'name'], msg: 'String must not be empty' },
        { loc: ['body', 'region'], msg: 'String must match pattern' },
      ],
    })
    expect(result).toBe('name: String must not be empty, region: String must match pattern')
  })

  it('joins {detail: {errors: [...]}}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: { errors: ['e1', 'e2'] } })).toBe('e1, e2')
  })

  it('returns {error: "..."} from nested detail', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ detail: { error: 'Token expired' } })).toBe('Token expired')
  })

  it('handles the no-detail fallback {error: "..."}', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError({ error: 'Direct error message' })).toBe('Direct error message')
  })

  it('returns "Unknown error" for falsy input', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError(null)).toBe('Unknown error')
    expect(extractApiError(undefined)).toBe('Unknown error')
  })

  it('passes through a raw string', async () => {
    const { extractApiError } = await import('@/lib/api')
    expect(extractApiError('plain message')).toBe('plain message')
  })
})

describe('openapi-fetch onResponse middleware — error path coverage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('throws extracted message on 500 with {detail: "..."}', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ detail: 'Background scheduler offline' }, { status: 500 }),
      ),
    )

    const { client } = await import('@/lib/api')
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow(
      'Background scheduler offline',
    )
  })

  it('throws joined message on 422 with FastAPI array shape', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json(
          {
            detail: [
              { loc: ['query', 'service_id'], msg: 'field required' },
            ],
          },
          { status: 422 },
        ),
      ),
    )

    const { client } = await import('@/lib/api')
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow(
      /service_id: field required/,
    )
  })

  it('throws a generic message when the error body is not JSON', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        new HttpResponse('not json — gateway timeout', { status: 504 }),
      ),
    )

    const { client } = await import('@/lib/api')
    // .catch(() => ({ message: ... })) → extractApiError sees {message: ...} → JSON.stringify
    await expect(client.GET('/api/admin/system-jobs')).rejects.toThrow()
  })
})

describe('useQuery isError flag — react-query error propagation', () => {
  it('isError === true when handler returns 401', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ detail: 'Unauthorized' }, { status: 401 }),
      ),
    )

    const { client } = await import('@/lib/api')
    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['system-jobs-401'],
          queryFn: async () => {
            const { data } = await client.GET('/api/admin/system-jobs')
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isError).toBe(true))
    expect(result.current.error).toBeInstanceOf(Error)
    expect((result.current.error as Error).message).toMatch(/Unauthorized/i)
  })

  it('isError === false and data populated on 2xx success', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/system-jobs`, () =>
        HttpResponse.json({ jobs: [] }),
      ),
    )

    const { client } = await import('@/lib/api')
    const { result } = renderHook(
      () =>
        useQuery({
          queryKey: ['system-jobs-ok'],
          queryFn: async () => {
            const { data } = await client.GET('/api/admin/system-jobs')
            return data
          },
        }),
      { wrapper: wrapper() },
    )

    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.isError).toBe(false)
    expect(result.current.data).toEqual({ jobs: [] })
  })
})

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

describe('backend harness child isolation', () => {
  let spawnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    vi.resetModules()
    vi.clearAllMocks()
    for (const [key, value] of Object.entries(rejected)) vi.stubEnv(key, value)
    for (const key of ['FLA_DEV_NO_CRONS', 'FASTLY_MOCK_MODE']) vi.stubEnv(key, '0')
    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    vi.stubEnv('SSE_BACKPLANE', 'valkey')
    vi.stubEnv('OTEL_EXPORTER', 'otlp')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true }))
    vi.spyOn(console, 'log').mockImplementation(() => {})
    const child = Object.assign(new ChildProcess(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      kill: vi.fn(() => {
        queueMicrotask(() => child.emit('exit', 0))
        return true
      }),
    })
    spawnSpy = vi.spyOn(childProcess, 'spawn').mockReturnValue(child)
    vi.spyOn(childProcess, 'execFileSync').mockReturnValue(Buffer.from(''))
    vi.spyOn(fs, 'mkdtempSync').mockImplementation(((prefix: string) => `${prefix}sandbox`) as typeof fs.mkdtempSync)
    vi.spyOn(fs, 'mkdirSync').mockImplementation((() => undefined) as typeof fs.mkdirSync)
    vi.spyOn(fs, 'writeFileSync').mockImplementation((() => undefined) as typeof fs.writeFileSync)
    vi.spyOn(fs, 'rmSync').mockImplementation((() => undefined) as typeof fs.rmSync)
    syncBuiltinESMExports()
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
    syncBuiltinESMExports()
    Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_PROC')
    Reflect.deleteProperty(globalThis, '__PLAYWRIGHT_E2E_SANDBOX')
  })

  it.each(['contract', 'e2e'])('%s rejects deployment env before spawning uv', async (harness) => {
    const contract = await import('../helpers/setup-backend')
    try {
      if (harness === 'contract') {
        await contract.startBackend()
      } else {
        const { default: globalSetup } = await import('../../e2e/global-setup')
        await globalSetup()
      }
      expect(spawnSpy).toHaveBeenCalledOnce()
      const [command, args, options] = spawnSpy.mock.calls[0]
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
        DEPLOYMENT_MODE: 'standard',
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
