/**
 * @vitest-environment jsdom
 *
 * Tests for the wizard's deploy + join + ingest helpers at
 * [components/ProvisionWizard/wizard-deploy.ts](../../../components/ProvisionWizard/wizard-deploy.ts).
 *
 * Same coverage rationale as wizard-api: these helpers own the state
 * transitions that drive the deploy / join / admin-ingest UI steps. The
 * body construction is especially worth pinning — every key has to match
 * what the backend expects, and the wrapper is what serializes wizard
 * state into the request payload.
 *
 * `runExportTerraform` is skipped here — it calls `downloadBlob` which
 * exercises browser-only `URL.createObjectURL` plumbing not present in
 * jsdom. The other 5 exports are covered.
 */
import { http, HttpResponse } from 'msw'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import {
  buildHandleModalClose,
  runAdminIngest,
  runDeploy,
  runFetchTerraformPreview,
  runJoin,
} from '@/components/ProvisionWizard/wizard-deploy'
import { INITIAL_CONFIG } from '@/components/ProvisionWizard/types'
import { server } from '../../../tests/msw/server'

const API_BASE = 'http://127.0.0.1:8000'

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
  vi.useRealTimers()
})

const FULL_CONFIG = {
  ...INITIAL_CONFIG,
  endpoint_name: 'My EP',
  fos_region: 'us-east-1',
  fos_bucket_name: 'my-bucket',
  fos_prefix: 'logs/',
  fos_access_key: 'AK',
  fos_secret_key: 'SK',
  sample_rate: 1,
  edge_only: true,
  custom_condition: '',
  log_period: 60,
  cdn_service_name: 'My CDN',
  cdn_prefix: 'mycdn',
  cdn_shield: 'iad-va-us',
  cdn_url: '',
  cdn_secret: '',
  enable_cron_sync: true,
  delete_after: false,
  commit_interval_mins: 5,
  enable_cron_compact: true,
}

describe('runFetchTerraformPreview', () => {
  function makeArgs(
    overrides: Partial<Parameters<typeof runFetchTerraformPreview>[0]> = {},
  ): Parameters<typeof runFetchTerraformPreview>[0] {
    return {
      token: 'TOK',
      selectedService: { id: 'svc-1', name: 'svc' } as never,
      config: FULL_CONFIG,
      setIsFetchingTerraform: vi.fn(),
      setTerraformFiles: vi.fn(),
      setSelectedTfFile: vi.fn(),
      ...overrides,
    }
  }

  it('returns early without flipping the loading state when no service is selected', async () => {
    const args = makeArgs({ selectedService: null })
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).not.toHaveBeenCalled()
  })

  it('on success selects main.tf when present', async () => {
    const files = {
      'main.tf': 'resource "..." {}',
      'variables.tf': 'variable "..." {}',
    }
    server.use(
      http.post(`${API_BASE}/api/provision/terraform/preview`, () =>
        HttpResponse.json(files),
      ),
    )
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).toHaveBeenNthCalledWith(1, true)
    expect(args.setTerraformFiles).toHaveBeenCalledWith(files)
    expect(args.setSelectedTfFile).toHaveBeenCalledWith('main.tf')
    expect(args.setIsFetchingTerraform).toHaveBeenLastCalledWith(false)
  })

  it('falls back to the first file when main.tf is absent', async () => {
    const files = { 'logging.tf': '...', 'cdn.tf': '...' }
    server.use(
      http.post(`${API_BASE}/api/provision/terraform/preview`, () =>
        HttpResponse.json(files),
      ),
    )
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setSelectedTfFile).toHaveBeenCalledWith('logging.tf')
  })

  it('still flips loading off if the request errors', async () => {
    // The helper logs the failure via console.error on the catch path —
    // expected here (we forced a network error), so silence it and assert
    // it fired instead of letting the raw error noise the test output.
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(
      http.post(`${API_BASE}/api/provision/terraform/preview`, () =>
        HttpResponse.error(),
      ),
    )
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

  it('refuses to close while streaming', () => {
    const deps = makeDeps({ status: 'streaming' })
    const handler = buildHandleModalClose(deps)
    handler(false)
    expect(deps.onOpenChange).not.toHaveBeenCalled()
  })

  it('propagates open=true unchanged (modal-open path)', () => {
    const deps = makeDeps()
    buildHandleModalClose(deps)(true)
    expect(deps.onOpenChange).toHaveBeenCalledWith(true)
    expect(deps.setStep).not.toHaveBeenCalled()
  })

  it('on close after done/isDone: switches active service + invalidates bootstrap', () => {
    const reload = vi.fn()
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { reload },
    })
    const deps = makeDeps({
      status: 'done',
      selectedService: { id: 'svc-new', name: 'new' } as never,
    })
    buildHandleModalClose(deps)(false)
    expect(deps.onOpenChange).toHaveBeenCalledWith(false)
    expect(deps.setActiveServiceId).toHaveBeenCalledWith('svc-new')
    expect(deps.queryClient.invalidateQueries).toHaveBeenCalledWith({
      queryKey: ['bootstrap'],
    })
    expect(reload).toHaveBeenCalled()
  })

  it('on close mid-flight: defers a reset chain via setTimeout(300)', () => {
    vi.useFakeTimers()
    const deps = makeDeps({ status: 'idle' })
    buildHandleModalClose(deps)(false)
    // None of the reset setters should fire synchronously — only after the
    // 300ms delay that gives the modal time to finish its close animation.
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
      selectedService: { id: 'svc-1', name: 'svc' } as never,
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

  it('starts the execute SSE with the full deploy body', () => {
    const args = makeArgs()
    runDeploy(args)
    expect(args.setIsDeploying).toHaveBeenCalledWith(true)
    expect(args.start).toHaveBeenCalledOnce()
    const [path, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(path).toBe('/api/provision/execute')
    expect(body).toMatchObject({
      token: 'TOK',
      service_id: 'svc-1',
      service_name: 'svc',
      endpoint_name: 'My EP',
      fos_bucket_name: 'my-bucket',
      // numeric fields get stringified per the API contract.
      sample_rate: '1',
      log_period: '60',
      commit_interval_mins: 5,
      // cdn_prefix → derived cdn_url.
      cdn_url: 'https://mycdn.global.ssl.fastly.net',
    })
  })

  it('omits cdn_url when cdn_prefix is empty', () => {
    const args = makeArgs({ config: { ...FULL_CONFIG, cdn_prefix: '' } })
    runDeploy(args)
    const [, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(body).not.toHaveProperty('cdn_url')
  })

  it('threads the picked faro_version into the deploy body', () => {
    const args = makeArgs({
      config: { ...FULL_CONFIG, faro_version: '1.9.0' },
    })
    runDeploy(args)
    const [, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(body.faro_version).toBe('1.9.0')
  })

  it('sends faro_version: null when the operator never pinned one (registry outage or skipped)', () => {
    const args = makeArgs({
      config: { ...FULL_CONFIG, faro_version: null },
    })
    runDeploy(args)
    const [, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(body.faro_version).toBeNull()
  })

  it('serializes log_fields to a JSON string', () => {
    const args = makeArgs({
      config: {
        ...FULL_CONFIG,
        log_fields: { groups: ['core', 'http'] } as never,
      },
    })
    runDeploy(args)
    const [, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(body.log_fields).toBe('{"groups":["core","http"]}')
  })

  it('passes log_fields=null when missing', () => {
    const args = makeArgs({
      config: { ...FULL_CONFIG, log_fields: undefined as unknown as never },
    })
    runDeploy(args)
    const [, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(body.log_fields).toBeNull()
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

  it('skips when any required credential is missing', () => {
    const args = makeArgs({
      config: { ...FULL_CONFIG, fos_bucket_name: '' },
    })
    runJoin(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
    expect(args.start).not.toHaveBeenCalled()
  })

  it('skips unsupported independent analyst joins before starting SSE', () => {
    const args = makeArgs({ analystPathASupported: false })
    runJoin(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
    expect(args.setJoinPhase).not.toHaveBeenCalled()
    expect(args.setStep).not.toHaveBeenCalled()
    expect(args.reset).not.toHaveBeenCalled()
    expect(args.start).not.toHaveBeenCalled()
  })

  it('starts the join SSE with a query string built from config', () => {
    const args = makeArgs()
    runJoin(args)
    expect(args.setIsDeploying).toHaveBeenCalledWith(true)
    expect(args.setJoinPhase).toHaveBeenCalledWith('connecting')
    expect(args.setStep).toHaveBeenCalledWith('join')
    expect(args.reset).toHaveBeenCalled()
    expect(args.start).toHaveBeenCalledOnce()
    const [url] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(url).toMatch(/^\/api\/provision\/join\?/)
    expect(url).toContain('fos_bucket_name=my-bucket')
    expect(url).toContain('sync_interval_mins=15')
    expect(url).toContain('sync_enabled=true')
    expect(url).toContain('iceberg_metadata_location=s3%3A%2F%2Fb%2Fmeta.json')
    // importMode='all' → no start_time/end_time on the URL.
    expect(url).not.toContain('start_time')
    expect(url).not.toContain('end_time')
  })

  it('adds start_time + end_time when importMode is "range"', () => {
    const args = makeArgs({
      importMode: 'range',
      importRange: { start: '2026-01-01', end: '2026-02-01' },
    })
    runJoin(args)
    const [url] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(url).toContain('start_time=2026-01-01')
    expect(url).toContain('end_time=2026-02-01')
  })

  it('omits range params when range mode but endpoints are blank', () => {
    const args = makeArgs({
      importMode: 'range',
      importRange: { start: '', end: '' },
    })
    runJoin(args)
    const [url] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(url).not.toContain('start_time')
    expect(url).not.toContain('end_time')
  })
})

describe('runAdminIngest', () => {
  function makeArgs(overrides: Partial<Parameters<typeof runAdminIngest>[0]> = {}) {
    return {
      token: 'TOK',
      selectedService: { id: 'svc-1', name: 'svc' } as never,
      selectedCdnService: { id: 'cdn-1', name: 'cdn' } as never,
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

  it('on success adds the joined service to the active services list', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/ingest`, () =>
        HttpResponse.json({ ok: true }),
      ),
    )
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setIsDeploying).toHaveBeenNthCalledWith(1, true)
    expect(args.setJoinedServiceId).toHaveBeenCalledWith('svc-1')
    expect(args.setActiveServiceId).toHaveBeenCalledWith('svc-1')
    expect(args.setServices).toHaveBeenCalledOnce()
    const newServices = (args.setServices as ReturnType<typeof vi.fn>).mock.calls[0][0]
    expect(newServices).toHaveLength(2)
    expect(newServices[1]).toMatchObject({ id: 'svc-1', accessLevel: 'read_write' })
    expect(args.queryClient.invalidateQueries).toHaveBeenCalledWith({
      queryKey: ['bootstrap'],
    })
    expect(args.setJoinPhase).toHaveBeenCalledWith('done')
    expect(args.setStep).toHaveBeenCalledWith('join')
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)
  })

  it('does not re-add a service that is already in the list', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/ingest`, () =>
        HttpResponse.json({ ok: true }),
      ),
    )
    const args = makeArgs({
      services: [
        { id: 'svc-1', name: 'svc', accessLevel: 'read_write' as const },
      ],
    })
    await runAdminIngest(args)
    expect(args.setServices).not.toHaveBeenCalled()
    // Other state transitions still happen.
    expect(args.setJoinPhase).toHaveBeenCalledWith('done')
  })

  it('does not advance phase when ok:false', async () => {
    server.use(
      http.post(`${API_BASE}/api/provision/ingest`, () =>
        HttpResponse.json({ ok: false, error: 'nope' }),
      ),
    )
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setJoinPhase).not.toHaveBeenCalled()
    expect(args.setStep).not.toHaveBeenCalled()
    // setIsDeploying still flips off in the finally block.
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)
  })

  it('swallows network errors but still flips loading off', async () => {
    // runAdminIngest logs "Ingest failed" via console.error on the catch
    // path — expected here (forced network error), so silence it and assert
    // it fired instead of letting the raw error noise the test output.
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(
      http.post(`${API_BASE}/api/provision/ingest`, () =>
        HttpResponse.error(),
      ),
    )
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setJoinPhase).not.toHaveBeenCalled()
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)
    expect(errSpy).toHaveBeenCalled()
    errSpy.mockRestore()
  })
})
