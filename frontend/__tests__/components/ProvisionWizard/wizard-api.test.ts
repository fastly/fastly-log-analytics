/**
 * @vitest-environment jsdom
 *
 * Consolidated MSW-driven tests for the ProvisionWizard API and deploy/join/ingest helpers.
 */
import { API_BASE, FULL_CONFIG, serviceStoreMockFactory, without } from '@/__tests__/helpers/page-smoke'
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, afterEach } from 'vitest'

vi.mock('@/stores/filterStore', () => serviceStoreMockFactory())

import {
  buildHandleModalClose,
  buildValidateOnSuccess,
  runAdminIngest,
  runAnalyzeLake,
  runCheckConfig,
  runCheckDomain,
  runCheckFos,
  runDeploy,
  runFetchTerraformPreview,
  runJoin,
  validateMutationFn,
} from '@/components/ProvisionWizard/useWizardState'
import { INITIAL_CONFIG } from '@/components/ProvisionWizard/ProvisionWizard'
import { useServiceStore } from '@/stores/filterStore'
import { server } from '@/__tests__/helpers/msw/handlers'

afterEach(() => {
  server.resetHandlers()
  vi.useRealTimers()
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
    const result = await validateMutationFn('TOKEN_XYZ')('service-123')
    expect(bodyReceived).toMatchObject({ token: 'TOKEN_XYZ', service_id: 'service-123' })
    expect(result).toEqual({ token_info: { id: 't1', name: 'me', type: 'user' } })
  })

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

  it('writes token_info via setTokenInfo when present and skips when absent', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({ token_info: { id: 't1', name: 'me', type: 'user' } })
    expect(deps.setTokenInfo).toHaveBeenCalledWith({ id: 't1', name: 'me', type: 'user' })

    const emptyDeps = makeDeps()
    buildValidateOnSuccess(emptyDeps)({})
    expect(emptyDeps.setTokenInfo).not.toHaveBeenCalled()
    expect(emptyDeps.setConfig).not.toHaveBeenCalled()
    expect(emptyDeps.setStep).toHaveBeenCalled()
  })

  it('applies defaults via setConfig and honours fallback vs explicit values', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({
      service_name: 'my-svc',
      defaults: { fos_bucket_name: 'MyBucket-Logs-Prod', fos_prefix: 'logs/' },
    })
    const nextFallback = deps.setConfig.mock.calls[0][0](INITIAL_CONFIG)
    expect(nextFallback).toMatchObject({
      endpoint_name: 'Fastly Object Storage Logs',
      fos_region: 'us-east-1',
      fos_bucket_name: 'mybucket-logs-prod',
      cdn_service_name: 'my-svc (CDN)',
      cdn_prefix: 'fos-mybucket-logs',
    })

    const explicitDeps = makeDeps()
    buildValidateOnSuccess(explicitDeps)({
      service_name: 'svc',
      defaults: {
        endpoint_name: 'Custom EP',
        fos_region: 'us-west-2',
        fos_bucket_name: 'override',
        cdn_service_name: 'explicit cdn',
        cdn_prefix: 'EXPLICIT',
      },
    })
    const nextExplicit = explicitDeps.setConfig.mock.calls[0][0](INITIAL_CONFIG)
    expect(nextExplicit).toMatchObject({
      endpoint_name: 'Custom EP',
      fos_region: 'us-west-2',
      cdn_service_name: 'explicit cdn',
      cdn_prefix: 'explicit',
    })
  })

  it.each([
    ['join' as const, 'join'],
    ['ingest' as const, 'join'],
    ['create' as const, 'features'],
  ])('advances to %s step when mode is %s', (mode, expectedStep) => {
    const deps = makeDeps(mode)
    buildValidateOnSuccess(deps)({})
    expect(deps.setStep).toHaveBeenCalledWith(expectedStep)
  })
})

describe('runCheckConfig', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runCheckConfig>[0]> = {}): Parameters<typeof runCheckConfig>[0] {
    return {
      token: 'TOK',
      selectedService: { id: 's1', name: 'svc', provisioned: false },
      selectedCdnService: { id: 'c1', name: 'cdn', provisioned: false },
      config: { ...INITIAL_CONFIG, fos_bucket_name: 'b' },
      setIsCheckingConfig: vi.fn(),
      setConfigStatus: vi.fn(),
      ...overrides,
    }
  }

  it('returns early when prerequisites are missing, and updates status when present', async () => {
    const missing = makeArgs({ selectedService: null })
    await runCheckConfig(missing)
    expect(missing.setIsCheckingConfig).not.toHaveBeenCalled()

    const body = { logging_service: { ok: true, details: 'fine' }, cdn_service: { ok: true, details: 'fine' } }
    server.use(http.get(`${API_BASE}/api/provision/check-config`, () => HttpResponse.json(body)))
    const args = makeArgs()
    await runCheckConfig(args)
    expect(args.setIsCheckingConfig).toHaveBeenNthCalledWith(1, true)
    expect(args.setConfigStatus).toHaveBeenCalledWith(body)
    expect(args.setIsCheckingConfig).toHaveBeenLastCalledWith(false)
  })
})

describe('runCheckFos', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runCheckFos>[0]> = {}): Parameters<typeof runCheckFos>[0] {
    return {
      vals: { bucket: 'b', region: 'us-east-1', access_key: 'a', secret_key: 's' },
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

  it.each([
    ['ok:true', () => HttpResponse.json({ ok: true }), 'success', ''],
    ['ok:false', () => HttpResponse.json({ ok: false, error: 'no creds' }), 'error', 'no creds'],
    ['network error', () => HttpResponse.error(), 'error', null],
  ])('sets status on %s', async (_label, respFn, expectedStatus, expectedErr) => {
    server.use(http.post(`${API_BASE}/api/provision/check-fos`, respFn))
    const args = makeArgs()
    await runCheckFos(args)
    expect(args.setFosStatus).toHaveBeenCalledWith('checking')
    expect(args.setFosStatus).toHaveBeenLastCalledWith(expectedStatus)
    if (expectedErr !== null) {
      expect(args.setFosError).toHaveBeenCalledWith(expectedErr)
    } else {
      expect(args.setFosError).toHaveBeenCalled()
    }
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
      config: { ...INITIAL_CONFIG, fos_bucket_name: 'cfg-bucket', fos_region: 'cfg-region', fos_access_key: 'cfg-ak', fos_secret_key: 'cfg-sk' },
    })
    await runCheckFos(args)
    expect(received).toMatchObject({ bucket: 'cfg-bucket', region: 'cfg-region', access_key: 'cfg-ak', secret_key: 'cfg-sk' })
  })
})

describe('runCheckDomain', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runCheckDomain>[0]> = {}): Parameters<typeof runCheckDomain>[0] {
    return { prefix: 'myco', setDomainStatus: vi.fn(), setDomainMessage: vi.fn(), ...overrides }
  }

  it.each(['no', ''])('returns early for short/empty prefix (%j)', async (prefix) => {
    const args = makeArgs({ prefix })
    await runCheckDomain(args)
    expect(args.setDomainStatus).not.toHaveBeenCalled()
  })

  it.each([
    ['available', () => HttpResponse.json({ available: true }), 'available', 'Domain available!'],
    ['taken', () => HttpResponse.json({ available: false }), 'taken', 'This domain prefix is already in use.'],
    ['error', () => HttpResponse.error(), 'error', null],
  ])('handles %s domain response', async (_label, respFn, expectedStatus, expectedMsg) => {
    server.use(http.get(`${API_BASE}/api/provision/check-domain`, respFn))
    const args = makeArgs()
    await runCheckDomain(args)
    expect(args.setDomainStatus).toHaveBeenNthCalledWith(1, 'checking')
    expect(args.setDomainStatus).toHaveBeenLastCalledWith(expectedStatus)
    if (expectedMsg) expect(args.setDomainMessage).toHaveBeenCalledWith(expectedMsg)
  })
})

describe('runAnalyzeLake', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runAnalyzeLake>[0]> = {}): Parameters<typeof runAnalyzeLake>[0] {
    return {
      config: { ...INITIAL_CONFIG, fos_bucket_name: 'b', fos_region: 'us-east-1', fos_access_key: 'ak', fos_secret_key: 'sk', fos_prefix: 'p/' },
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

  it('writes lake info + import range (when present) and advances to analyze step', async () => {
    const body = { ok: true, table_count: 3, range: { start: '2026-01-01', end: '2026-02-01' } }
    server.use(http.post(`${API_BASE}/api/provision/lake-info`, () => HttpResponse.json(body)))
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setIsAnalyzing).toHaveBeenNthCalledWith(1, true)
    expect(args.setLakeInfo).toHaveBeenCalledWith(body)
    expect(args.setImportRange).toHaveBeenCalledWith({ start: '2026-01-01', end: '2026-02-01' })
    expect(args.setStep).toHaveBeenCalledWith('analyze')
    expect(args.setIsAnalyzing).toHaveBeenLastCalledWith(false)

    server.use(http.post(`${API_BASE}/api/provision/lake-info`, () => HttpResponse.json({ ok: true, table_count: 0 })))
    const noRangeArgs = makeArgs()
    await runAnalyzeLake(noRangeArgs)
    expect(noRangeArgs.setImportRange).not.toHaveBeenCalled()
    expect(noRangeArgs.setStep).toHaveBeenCalledWith('analyze')
  })

  it.each([
    ['ok:false', () => HttpResponse.json({ ok: false, error: 'no manifests found' }), 'no manifests found'],
    ['network error', () => HttpResponse.error(), null],
  ])('surfaces error on %s', async (_label, respFn, expectedMsg) => {
    server.use(http.post(`${API_BASE}/api/provision/lake-info`, respFn))
    const args = makeArgs()
    await runAnalyzeLake(args)
    expect(args.setFosStatus).toHaveBeenLastCalledWith('error')
    if (expectedMsg) expect(args.setFosError).toHaveBeenCalledWith(expectedMsg)
    else expect(args.setFosError).toHaveBeenCalled()
    expect(args.setStep).not.toHaveBeenCalled()
    expect(args.setIsAnalyzing).toHaveBeenLastCalledWith(false)
  })
})

describe('runFetchTerraformPreview', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runFetchTerraformPreview>[0]> = {}): Parameters<typeof runFetchTerraformPreview>[0] {
    return {
      token: 'TOK',
      selectedService: { id: 'svc-1', name: 'svc', provisioned: false },
      config: FULL_CONFIG,
      setIsFetchingTerraform: vi.fn(),
      setTerraformFiles: vi.fn(),
      setSelectedTfFile: vi.fn(),
      ...overrides,
    }
  }

  it('returns early when no service is selected', async () => {
    const args = makeArgs({ selectedService: null })
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).not.toHaveBeenCalled()
  })

  it.each([
    ['selects main.tf when present', { 'main.tf': 'resource "..." {}', 'variables.tf': 'variable "..." {}' }, 'main.tf'],
    ['falls back to first file when main.tf is absent', { 'logging.tf': '...', 'cdn.tf': '...' }, 'logging.tf'],
  ])('%s', async (_label, files, expectedFile) => {
    server.use(http.post(`${API_BASE}/api/provision/terraform/preview`, () => HttpResponse.json(files)))
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).toHaveBeenNthCalledWith(1, true)
    expect(args.setTerraformFiles).toHaveBeenCalledWith(files)
    expect(args.setSelectedTfFile).toHaveBeenCalledWith(expectedFile)
    expect(args.setIsFetchingTerraform).toHaveBeenLastCalledWith(false)
  })

  it('still flips loading off if the request errors', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(http.post(`${API_BASE}/api/provision/terraform/preview`, () => HttpResponse.error()))
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).toHaveBeenLastCalledWith(false)
    expect(args.setTerraformFiles).not.toHaveBeenCalled()
    expect(errSpy).toHaveBeenCalled()
    errSpy.mockRestore()
  })
})

describe('buildHandleModalClose', () => {
  function makeDeps(overrides: Partial<Parameters<typeof buildHandleModalClose>[0]> = {}) {
    return {
      status: 'idle',
      isDone: false,
      onOpenChange: vi.fn(),
      selectedService: null,
      setActiveServiceId: vi.fn(),
      queryClient: { invalidateQueries: vi.fn() },
      setStep: vi.fn(),
      setMode: vi.fn(),
      setSearch: vi.fn(),
      setSelectedService: vi.fn(),
      setIsDeploying: vi.fn(),
      setFosStatus: vi.fn(),
      setFosError: vi.fn(),
      setLakeInfo: vi.fn(),
      setIsAnalyzing: vi.fn(),
      setImportMode: vi.fn(),
      setSyncEnabled: vi.fn(),
      reset: vi.fn(),
      resetConfig: vi.fn(),
      setNgwafWorkspaces: vi.fn(),
      setNgwafFetching: vi.fn(),
      setNgwafFetchError: vi.fn(),
      ...overrides,
    }
  }

  it('refuses to close while streaming and propagates open=true unchanged', () => {
    const streaming = makeDeps({ status: 'streaming' })
    buildHandleModalClose(streaming)(false)
    expect(streaming.onOpenChange).not.toHaveBeenCalled()

    const opening = makeDeps()
    buildHandleModalClose(opening)(true)
    expect(opening.onOpenChange).toHaveBeenCalledWith(true)
    expect(opening.setStep).not.toHaveBeenCalled()
  })

  it('on close after done/isDone: switches active service + invalidates bootstrap', () => {
    const reload = vi.fn()
    Object.defineProperty(window, 'location', { configurable: true, value: { reload } })
    const deps = makeDeps({
      status: 'done',
      selectedService: { id: 'svc-new', name: 'new', provisioned: false },
    })
    buildHandleModalClose(deps)(false)
    expect(deps.onOpenChange).toHaveBeenCalledWith(false)
    expect(deps.setActiveServiceId).toHaveBeenCalledWith('svc-new')
    expect(deps.queryClient.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['bootstrap'] })
    expect(reload).toHaveBeenCalled()
  })

  it('on close mid-flight: defers a reset chain via setTimeout(300)', () => {
    vi.useFakeTimers()
    const deps = makeDeps({ status: 'idle' })
    buildHandleModalClose(deps)(false)
    expect(deps.setStep).not.toHaveBeenCalled()
    vi.advanceTimersByTime(300)
    expect(deps.setStep).toHaveBeenCalledWith('mode')
    expect(deps.setMode).toHaveBeenCalledWith(null)
    expect(deps.setFosStatus).toHaveBeenCalledWith('idle')
    expect(deps.reset).toHaveBeenCalled()
    expect(deps.resetConfig).toHaveBeenCalled()
  })
})

describe('runDeploy', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runDeploy>[0]> = {}) {
    return {
      token: 'TOK',
      selectedService: { id: 'svc-1', name: 'svc', provisioned: false },
      config: FULL_CONFIG,
      setIsDeploying: vi.fn(),
      start: vi.fn(),
      ...overrides,
    }
  }

  it('skips when no service is selected', () => {
    const args = makeArgs({ selectedService: null })
    runDeploy(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
    expect(args.start).not.toHaveBeenCalled()
  })

  it('starts the execute SSE with the full deploy body and handles optional config variations', () => {
    const args = makeArgs()
    runDeploy(args)
    expect(args.setIsDeploying).toHaveBeenCalledWith(true)
    const [path, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(path).toBe('/api/provision/execute')
    expect(body).toMatchObject({
      token: 'TOK',
      service_id: 'svc-1',
      service_name: 'svc',
      endpoint_name: 'My EP',
      fos_bucket_name: 'my-bucket',
      sample_rate: '1',
      log_period: '60',
      commit_interval_mins: 5,
      cdn_url: 'https://mycdn.global.ssl.fastly.net',
    })

    const noCdn = makeArgs({ config: { ...FULL_CONFIG, cdn_prefix: '', faro_version: '1.9.0', log_fields: { groups: ['core', 'http'] } } })
    runDeploy(noCdn)
    const [, bodyNoCdn] = (noCdn.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(bodyNoCdn).not.toHaveProperty('cdn_url')
    expect(bodyNoCdn.faro_version).toBe('1.9.0')
    expect(bodyNoCdn.log_fields).toBe('{"groups":["core","http"]}')

    const nullFields = makeArgs({ config: { ...(without(FULL_CONFIG, 'log_fields') as typeof FULL_CONFIG), faro_version: null } })
    runDeploy(nullFields)
    const [, bodyNull] = (nullFields.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(bodyNull.faro_version).toBeNull()
    expect(bodyNull.log_fields).toBeNull()
  })
})

describe('runJoin', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runJoin>[0]> = {}) {
    return {
      config: FULL_CONFIG,
      analystPathASupported: true,
      syncIntervalMins: '15',
      syncEnabled: true,
      icebergMetadataLocation: 's3://b/meta.json',
      importMode: 'all' as 'all' | 'range',
      importRange: { start: '', end: '' },
      setIsDeploying: vi.fn(),
      setJoinPhase: vi.fn(),
      setStep: vi.fn(),
      reset: vi.fn(),
      start: vi.fn(),
      ...overrides,
    }
  }

  it.each([
    ['missing credential', { config: { ...FULL_CONFIG, fos_bucket_name: '' } }],
    ['unsupported independent analyst join', { analystPathASupported: false }],
  ])('skips when %s', (_label, overrides) => {
    const args = makeArgs(overrides)
    runJoin(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
    expect(args.start).not.toHaveBeenCalled()
  })

  it('starts the join SSE with query string for all/range modes', () => {
    const allArgs = makeArgs()
    runJoin(allArgs)
    expect(allArgs.setIsDeploying).toHaveBeenCalledWith(true)
    expect(allArgs.setJoinPhase).toHaveBeenCalledWith('connecting')
    expect(allArgs.setStep).toHaveBeenCalledWith('join')
    expect(allArgs.reset).toHaveBeenCalled()
    const [urlAll] = (allArgs.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(urlAll).toMatch(/^\/api\/provision\/join\?/)
    expect(urlAll).toContain('fos_bucket_name=my-bucket')
    expect(urlAll).toContain('sync_interval_mins=15')
    expect(urlAll).toContain('sync_enabled=true')
    expect(urlAll).toContain('iceberg_metadata_location=s3%3A%2F%2Fb%2Fmeta.json')
    expect(urlAll).not.toContain('start_time')

    const rangeArgs = makeArgs({ importMode: 'range', importRange: { start: '2026-01-01', end: '2026-02-01' } })
    runJoin(rangeArgs)
    const [urlRange] = (rangeArgs.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(urlRange).toContain('start_time=2026-01-01')
    expect(urlRange).toContain('end_time=2026-02-01')

    const blankRangeArgs = makeArgs({ importMode: 'range', importRange: { start: '', end: '' } })
    runJoin(blankRangeArgs)
    const [urlBlank] = (blankRangeArgs.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(urlBlank).not.toContain('start_time')
  })
})

describe('runAdminIngest', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runAdminIngest>[0]> = {}) {
    return {
      token: 'TOK',
      selectedService: { id: 'svc-1', name: 'svc', provisioned: false },
      selectedCdnService: { id: 'cdn-1', name: 'cdn', provisioned: false },
      config: FULL_CONFIG,
      services: [{ id: 'svc-other', name: 'other', accessLevel: 'read_write' as const }],
      setIsDeploying: vi.fn(),
      setJoinedServiceId: vi.fn(),
      setActiveServiceId: vi.fn(),
      setServices: vi.fn(),
      queryClient: { invalidateQueries: vi.fn() },
      setJoinPhase: vi.fn(),
      setStep: vi.fn(),
      ...overrides,
    }
  }

  it('skips when no service is selected', async () => {
    const args = makeArgs({ selectedService: null })
    await runAdminIngest(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
  })

  it('on success adds the joined service (deduping existing) and advances phase', async () => {
    server.use(http.post(`${API_BASE}/api/provision/ingest`, () => HttpResponse.json({ ok: true })))
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setIsDeploying).toHaveBeenNthCalledWith(1, true)
    expect(args.setJoinedServiceId).toHaveBeenCalledWith('svc-1')
    expect(args.setActiveServiceId).toHaveBeenCalledWith('svc-1')
    expect(args.setServices).toHaveBeenCalledOnce()
    const newServices = (args.setServices as ReturnType<typeof vi.fn>).mock.calls[0][0]
    expect(newServices).toHaveLength(2)
    expect(newServices[1]).toMatchObject({ id: 'svc-1', accessLevel: 'read_write' })
    expect(args.queryClient.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['bootstrap'] })
    expect(args.setJoinPhase).toHaveBeenCalledWith('done')
    expect(args.setStep).toHaveBeenCalledWith('join')
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)

    const existingArgs = makeArgs({ services: [{ id: 'svc-1', name: 'svc', accessLevel: 'read_write' as const }] })
    await runAdminIngest(existingArgs)
    expect(existingArgs.setServices).not.toHaveBeenCalled()
    expect(existingArgs.setJoinPhase).toHaveBeenCalledWith('done')
  })

  it.each([
    ['ok:false', () => HttpResponse.json({ ok: false, error: 'nope' }), false],
    ['network error', () => HttpResponse.error(), true],
  ])('does not advance phase on %s and still flips loading off', async (_label, respFn, logsErr) => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(http.post(`${API_BASE}/api/provision/ingest`, respFn))
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setJoinPhase).not.toHaveBeenCalled()
    expect(args.setStep).not.toHaveBeenCalled()
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)
    if (logsErr) expect(errSpy).toHaveBeenCalled()
    errSpy.mockRestore()
  })
})
