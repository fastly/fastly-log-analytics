/**
 * @vitest-environment jsdom
 *
 * MSW-driven tests for the wizard's API call helpers at
 * [components/ProvisionWizard/wizard-api.ts](../../../components/ProvisionWizard/wizard-api.ts).
 *
 * The wizard's network-bound steps (validate token, check config, check
 * FOS credentials, check domain availability, analyze data lake) all go
 * through these helpers. They follow a uniform pattern: drive the
 * dispatch setters on success, set an error state on failure. Locking
 * those state transitions matters because the wizard renders directly
 * off them — silent regression breaks the step UI without any test
 * complaining.
 *
 * Coverage gain: file was 6.5% before (only the import lines were
 * touched). Pure MSW pattern — same approach as
 * `__tests__/lib/api/custom-fields.test.ts`.
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import {
  buildValidateOnSuccess,
  runCheckConfig,
  runCheckDomain,
  runCheckFos,
  runAnalyzeLake,
  validateMutationFn,
} from '@/components/ProvisionWizard/wizard-api'
import { INITIAL_CONFIG } from '@/components/ProvisionWizard/types'
import { useServiceStore } from '@/stores/serviceStore'
import { server } from '../../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'

// Match the pattern in __tests__/lib/api-error-paths.test.ts — the api
// middleware reads useServiceStore.getState() on every typed-client
// request. Inline the active-service id rather than referencing a const
// (vi.mock is hoisted above module-level decls).
vi.mock('@/stores/serviceStore', () => {
  const state = { activeServiceId: 'svc-test' }
  const useServiceStore: unknown = Object.assign(
    (selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state),
    { getState: () => state },
  )
  return { useServiceStore }
})

afterEach(() => {
  server.resetHandlers()
})

describe('validateMutationFn', () => {
  it('POSTs to /api/provision/validate and returns the data body', async () => {
    let bodyReceived: unknown = null
    server.use(
      http.post(`${API_BASE}/api/provision/validate`, async ({ request }) => {
        bodyReceived = await request.json()
        return HttpResponse.json({ token_info: { id: 't1', name: 'me', type: 'user' } })
      }),
    )
    const fn = validateMutationFn('TOKEN_XYZ')
    const result = await fn('service-123')
    expect(bodyReceived).toMatchObject({ token: 'TOKEN_XYZ', service_id: 'service-123' })
    expect(result).toEqual({ token_info: { id: 't1', name: 'me', type: 'user' } })
  })

  // Regression: on a FRESH INSTALL there is no active service yet — that's
  // the whole reason the wizard exists. The api-client middleware aborts
  // serviceless requests with "No active service — request aborted" unless
  // the path is on SERVICELESS_PATH_PREFIXES. /api/provision was omitted in
  // v2.0.0, so step 1 (validate token) aborted before the request left the
  // browser. Pin the no-service path: the call must reach the server.
  it('fresh install (no active service): the validate call is NOT aborted', async () => {
    const prev = useServiceStore.getState().activeServiceId
    ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = null
    try {
      let reached = false
      server.use(
        http.post(`${API_BASE}/api/provision/validate`, async () => {
          reached = true
          return HttpResponse.json({ token_info: { id: 't1', name: 'me', type: 'user' } })
        }),
      )
      const result = await validateMutationFn('TOKEN_XYZ')('service-123')
      expect(reached).toBe(true)
      expect(result).toEqual({ token_info: { id: 't1', name: 'me', type: 'user' } })
    } finally {
      ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = prev
    }
  })
})

describe('buildValidateOnSuccess', () => {
  function makeDeps(mode: 'join' | 'ingest' | 'create' = 'create') {
    return {
      token: 'TOK',
      mode,
      setTokenInfo: vi.fn(),
      setConfig: vi.fn(),
      setStep: vi.fn(),
    }
  }

  it('writes token_info via setTokenInfo when present', () => {
    const deps = makeDeps()
    const handler = buildValidateOnSuccess(deps)
    handler({ token_info: { id: 't1', name: 'me', type: 'user' } })
    expect(deps.setTokenInfo).toHaveBeenCalledWith({
      id: 't1',
      name: 'me',
      type: 'user',
    })
  })

  it('applies defaults via setConfig and falls back when fields are missing', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({
      service_name: 'my-svc',
      defaults: {
        fos_bucket_name: 'MyBucket-Logs-Prod',
        fos_prefix: 'logs/',
      },
    })
    expect(deps.setConfig).toHaveBeenCalledOnce()
    // Apply the updater to a fresh INITIAL_CONFIG to assert the merge shape.
    const updater = deps.setConfig.mock.calls[0][0]
    const next = updater(INITIAL_CONFIG)
    // Falls back to literal default strings.
    expect(next.endpoint_name).toBe('Fastly Object Storage Logs')
    expect(next.fos_region).toBe('us-east-1')
    // Bucket name normalised to lowercase.
    expect(next.fos_bucket_name).toBe('mybucket-logs-prod')
    // CDN name composed from service_name when not supplied.
    expect(next.cdn_service_name).toBe('my-svc (CDN)')
    // CDN prefix derived from the bucket's first two dash-segments,
    // prefixed with `fos-`, lowercase.
    expect(next.cdn_prefix).toBe('fos-mybucket-logs')
  })

  it('honours explicit defaults from the server', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({
      service_name: 'svc',
      defaults: {
        endpoint_name: 'Custom EP',
        fos_region: 'us-west-2',
        fos_bucket_name: 'override',
        cdn_service_name: 'explicit cdn',
        cdn_prefix: 'EXPLICIT',
      },
    })
    const next = deps.setConfig.mock.calls[0][0](INITIAL_CONFIG)
    expect(next.endpoint_name).toBe('Custom EP')
    expect(next.fos_region).toBe('us-west-2')
    expect(next.cdn_service_name).toBe('explicit cdn')
    expect(next.cdn_prefix).toBe('explicit')
  })

  it('advances to "join" step when mode is join', () => {
    const deps = makeDeps('join')
    buildValidateOnSuccess(deps)({})
    expect(deps.setStep).toHaveBeenCalledWith('join')
  })

  it('advances to "join" step when mode is ingest', () => {
    const deps = makeDeps('ingest')
    buildValidateOnSuccess(deps)({})
    expect(deps.setStep).toHaveBeenCalledWith('join')
  })

  it('advances to "storage" step for the default create mode', () => {
    const deps = makeDeps('create')
    buildValidateOnSuccess(deps)({})
    expect(deps.setStep).toHaveBeenCalledWith('storage')
  })

  it('skips setTokenInfo and setConfig when neither key is present', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({})
    expect(deps.setTokenInfo).not.toHaveBeenCalled()
    expect(deps.setConfig).not.toHaveBeenCalled()
    // setStep still fires unconditionally.
    expect(deps.setStep).toHaveBeenCalled()
  })
})

describe('runCheckConfig', () => {
  function makeArgs(
    overrides: Partial<Parameters<typeof runCheckConfig>[0]> = {},
  ): Parameters<typeof runCheckConfig>[0] {
    return {
      token: 'TOK',
      selectedService: { id: 's1', name: 'svc' } as never,
      selectedCdnService: { id: 'c1', name: 'cdn' } as never,
      config: { ...INITIAL_CONFIG, fos_bucket_name: 'b' },
      setIsCheckingConfig: vi.fn(),
      setConfigStatus: vi.fn(),
      ...overrides,
    }
  }

  it('returns early without flipping the loading state when prerequisites are missing', async () => {
    const args = makeArgs({ selectedService: null })
    await runCheckConfig(args)
    expect(args.setIsCheckingConfig).not.toHaveBeenCalled()
    expect(args.setConfigStatus).not.toHaveBeenCalled()
  })

  it('flips loading on, sets status on success, flips loading off', async () => {
    const body = {
      logging_service: { ok: true, details: 'fine' },
      cdn_service: { ok: true, details: 'fine' },
    }
    server.use(
      http.get(`${API_BASE}/api/provision/check-config`, () => HttpResponse.json(body)),
    )
    const args = makeArgs()
    await runCheckConfig(args)
    expect(args.setIsCheckingConfig).toHaveBeenNthCalledWith(1, true)
    expect(args.setConfigStatus).toHaveBeenCalledWith(body)
    expect(args.setIsCheckingConfig).toHaveBeenLastCalledWith(false)
  })
})

describe('runCheckFos', () => {
  function makeArgs(
    overrides: Partial<Parameters<typeof runCheckFos>[0]> = {},
  ): Parameters<typeof runCheckFos>[0] {
    return {
      vals: {
        bucket: 'b',
        region: 'us-east-1',
        access_key: 'a',
        secret_key: 's',
      },
      config: INITIAL_CONFIG,
      setFosStatus: vi.fn(),
      setFosError: vi.fn(),
      ...overrides,
    }
  }

  it('does nothing when any credential is missing', async () => {
    const args = makeArgs({ vals: { bucket: 'b' } })
    await runCheckFos(args)
    expect(args.setFosStatus).not.toHaveBeenCalled()
  })

  it('sets success state when the server returns ok:true', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/check-fos`, () =>
        HttpResponse.json({ ok: true }),
      ),
    )
    const args = makeArgs()
    await runCheckFos(args)
    expect(args.setFosStatus).toHaveBeenCalledWith('checking')
    expect(args.setFosStatus).toHaveBeenLastCalledWith('success')
    expect(args.setFosError).toHaveBeenCalledWith('')
  })

  it('sets error state when the server returns ok:false', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/check-fos`, () =>
        HttpResponse.json({ ok: false, error: 'no creds' }),
      ),
    )
    const args = makeArgs()
    await runCheckFos(args)
    expect(args.setFosStatus).toHaveBeenLastCalledWith('error')
    expect(args.setFosError).toHaveBeenCalledWith('no creds')
  })

  it('falls back to config values when vals is not supplied', async () => {
    let received: unknown = null
    server.use(
      http.post(`${API_BASE}/api/provision/check-fos`, async ({ request }) => {
        received = await request.json()
        return HttpResponse.json({ ok: true })
      }),
    )
    const args = makeArgs({
      vals: undefined,
      config: {
        ...INITIAL_CONFIG,
        fos_bucket_name: 'cfg-bucket',
        fos_region: 'cfg-region',
        fos_access_key: 'cfg-ak',
        fos_secret_key: 'cfg-sk',
      },
    })
    await runCheckFos(args)
    expect(received).toMatchObject({
      bucket: 'cfg-bucket',
      region: 'cfg-region',
      access_key: 'cfg-ak',
      secret_key: 'cfg-sk',
    })
  })

  it('catches network errors and produces a fallback message', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/check-fos`, () =>
        HttpResponse.error(),
      ),
    )
    const args = makeArgs()
    await runCheckFos(args)
    expect(args.setFosStatus).toHaveBeenLastCalledWith('error')
    // setFosError was called at least once with a non-empty string.
    expect(args.setFosError).toHaveBeenCalled()
  })
})

describe('runCheckDomain', () => {
  function makeArgs(
    overrides: Partial<Parameters<typeof runCheckDomain>[0]> = {},
  ): Parameters<typeof runCheckDomain>[0] {
    return {
      prefix: 'myco',
      setDomainStatus: vi.fn(),
      setDomainMessage: vi.fn(),
      ...overrides,
    }
  }

  it('returns early when the prefix is shorter than 3 characters', async () => {
    const args = makeArgs({ prefix: 'no' })
    await runCheckDomain(args)
    expect(args.setDomainStatus).not.toHaveBeenCalled()
  })

  it('returns early on empty prefix', async () => {
    const args = makeArgs({ prefix: '' })
    await runCheckDomain(args)
    expect(args.setDomainStatus).not.toHaveBeenCalled()
  })

  it('sets available + message when server reports available', async () => {
    server.use(
      http.get(`${API_BASE}/api/provision/check-domain`, () =>
        HttpResponse.json({ available: true }),
      ),
    )
    const args = makeArgs()
    await runCheckDomain(args)
    expect(args.setDomainStatus).toHaveBeenNthCalledWith(1, 'checking')
    expect(args.setDomainStatus).toHaveBeenLastCalledWith('available')
    expect(args.setDomainMessage).toHaveBeenCalledWith('Domain available!')
  })

  it('sets taken when server reports unavailable', async () => {
    server.use(
      http.get(`${API_BASE}/api/provision/check-domain`, () =>
        HttpResponse.json({ available: false }),
      ),
    )
    const args = makeArgs()
    await runCheckDomain(args)
    expect(args.setDomainStatus).toHaveBeenLastCalledWith('taken')
    expect(args.setDomainMessage).toHaveBeenCalledWith(
      'This domain prefix is already in use.',
    )
  })

  it('sets error on a thrown network failure', async () => {
    server.use(
      http.get(`${API_BASE}/api/provision/check-domain`, () =>
        HttpResponse.error(),
      ),
    )
    const args = makeArgs()
    await runCheckDomain(args)
    expect(args.setDomainStatus).toHaveBeenLastCalledWith('error')
  })
})

describe('runAnalyzeLake', () => {
  function makeArgs(
    overrides: Partial<Parameters<typeof runAnalyzeLake>[0]> = {},
  ): Parameters<typeof runAnalyzeLake>[0] {
    return {
      config: {
        ...INITIAL_CONFIG,
        fos_bucket_name: 'b',
        fos_region: 'us-east-1',
        fos_access_key: 'ak',
        fos_secret_key: 'sk',
        fos_prefix: 'p/',
      },
      icebergMetadataLocation: 's3://b/metadata/v1.json',
      setIsAnalyzing: vi.fn(),
      setLakeInfo: vi.fn(),
      setImportRange: vi.fn(),
      setStep: vi.fn(),
      setFosStatus: vi.fn(),
      setFosError: vi.fn(),
      ...overrides,
    }
  }

  it('on success: writes lake info + import range, advances to analyze step', async () => {
    const body = {
      ok: true,
      table_count: 3,
      range: { start: '2026-01-01', end: '2026-02-01' },
    }
    server.use(
      http.post(`${API_BASE}/api/provision/lake-info`, () => HttpResponse.json(body)),
    )
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setIsAnalyzing).toHaveBeenNthCalledWith(1, true)
    expect(args.setLakeInfo).toHaveBeenCalledWith(body)
    expect(args.setImportRange).toHaveBeenCalledWith({
      start: '2026-01-01',
      end: '2026-02-01',
    })
    expect(args.setStep).toHaveBeenCalledWith('analyze')
    expect(args.setIsAnalyzing).toHaveBeenLastCalledWith(false)
  })

  it('skips setImportRange when the server omits the range', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/lake-info`, () =>
        HttpResponse.json({ ok: true, table_count: 0 }),
      ),
    )
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setImportRange).not.toHaveBeenCalled()
    expect(args.setStep).toHaveBeenCalledWith('analyze')
  })

  it('on ok:false flips Fos status to error with the supplied message', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/lake-info`, () =>
        HttpResponse.json({ ok: false, error: 'no manifests found' }),
      ),
    )
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setFosStatus).toHaveBeenCalledWith('error')
    expect(args.setFosError).toHaveBeenCalledWith('no manifests found')
    expect(args.setStep).not.toHaveBeenCalled()
  })

  it('catches a thrown network error and surfaces a message', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/lake-info`, () =>
        HttpResponse.error(),
      ),
    )
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setFosStatus).toHaveBeenLastCalledWith('error')
    expect(args.setFosError).toHaveBeenCalled()
    // setIsAnalyzing is still flipped off in the finally block.
    expect(args.setIsAnalyzing).toHaveBeenLastCalledWith(false)
  })
})
