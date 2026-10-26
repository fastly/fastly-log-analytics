/**
 * @vitest-environment jsdom
 *
 * Consolidated ProvisionWizard component tests: TerraformStep,
 * topology capability (ModeStep + JoinStep), WizardFooter, RumVersionPicker, and ResumeBanner.
 */
import * as React from 'react'
import { FULL_CONFIG, without } from '@/__tests__/helpers/page-smoke'
import { toggleLogFieldGroup, toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit, getApiBase } from "@/lib/api";
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'

import { ProvisionWizard, TerraformStep, ModeStep, ResumeBanner, WizardFooter } from '@/components/ProvisionWizard/ProvisionWizard'
import { JoinStep } from '@/components/ProvisionWizard/steps/JoinStep'
import { RumVersionPicker } from '@/components/ProvisionWizard/steps/StorageStep'
import {
  WIZARD_DRAFT_KEY,
  buildHandleModalClose,
  buildValidateOnSuccess,
  clearDraft,
  loadDraft,
  mergePersistedConfig,
  runAdminIngest,
  runAnalyzeLake,
  runCheckConfig,
  runCheckDomain,
  runCheckFos,
  runDeploy,
  runFetchTerraformPreview,
  runJoin,
  saveDraft,
  stripSecretsFromConfig,
  validateMutationFn,
} from '@/components/ProvisionWizard/useWizardState'
import {
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type LogFieldsCatalog,
  type WizardDraft,
  type WizardState,
} from '@/components/ProvisionWizard/ProvisionWizard'
import { useServiceStore } from '@/stores/filterStore'
import { createTestQueryClient, renderWithQuery } from '../../helpers/page-smoke'
import { createUseSSEMock } from '../../helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useTimezoneStore: () => ({
    timezone: 'UTC',
  }),
}))

const { state: sseState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()), useSSE: () => sseState }))

const API_BASE = getApiBase()
const SVC = 'svc-new-1'

afterEach(() => {
  server.resetHandlers()
  vi.useRealTimers()
})

describe('TerraformStep', () => {
  const tfState = {
    terraformFiles: { 'main.tf': 'resource "x" "y" {}' },
    selectedTfFile: 'main.tf',
    setSelectedTfFile: vi.fn(),
    isFetchingTerraform: false,
    handleExportTerraform: vi.fn(),
  } as unknown as WizardState

  test('copy button writes the selected file to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
    })
    expect(writeText).toHaveBeenCalledWith('resource "x" "y" {}')
  })

  test('a rejected clipboard write is swallowed instead of surfacing as an unhandled rejection', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
    })
    await new Promise((r) => setTimeout(r, 0))
    expect(writeText).toHaveBeenCalledTimes(1)
  })
})

function makeTopologyState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'mode',
    setStep: vi.fn(),
    mode: 'join',
    setMode: vi.fn(),
    analystPathASupported: true,
    analystPathAReason: null,
    joinPhase: 'form',
    config: INITIAL_CONFIG,
    setConfig: vi.fn(),
    isAnalyzing: false,
    lines: [],
    status: 'idle',
    sseError: null,
    icebergMetadataLocation: '',
    setIcebergMetadataLocation: vi.fn(),
    handleCheckFos: vi.fn(),
    fosStatus: 'idle',
    fosError: '',
    selectedService: null,
    selectedCdnService: null,
    servicesData: [],
    configStatus: null,
    isCheckingConfig: false,
    handleCheckConfig: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('ProvisionWizard topology capability', () => {
  it('keeps Analyst: Join enabled for supported sync topology', () => {
    render(<ModeStep s={makeTopologyState({ analystPathASupported: true })} />)
    expect(screen.getByRole('button', { name: /analyst: join/i })).toBeEnabled()
  })

  it('disables Analyst: Join for unsupported scalable topology', () => {
    render(
      <ModeStep
        s={makeTopologyState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    )
    expect(screen.getByRole('button', { name: /analyst: join/i })).toBeDisabled()
    expect(screen.getByText(ANALYST_PATH_A_UNSUPPORTED_REASON)).toBeVisible()
  })

  it('warns when a stale invite or manual entry reaches JoinStep', () => {
    render(
      <JoinStep
        s={makeTopologyState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    )
    expect(screen.getByText(/independent analyst join is unavailable/i)).toBeVisible()
    expect(screen.getByText(/old invite or entered credentials manually/i)).toBeVisible()
    expect(screen.getByText(/Path B/i)).toBeVisible()
  })
})

function makeFooterState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'terraform',
    setStep: vi.fn(),
    mode: 'provision',
    isDeploying: false,
    status: 'idle',
    isDone: false,
    handleModalClose: vi.fn(),
    isAnalyzing: false,
    token: 'tok',
    isLoadingServices: false,
    handleTokenSubmit: vi.fn(),
    selectedService: { id: 'svc', name: 'Svc' },
    validateMutation: { isPending: false },
    domainStatus: 'available',
    config: { fos_bucket_name: 'bucket', ngwaf_workspace_id: '' },
    fetchTerraformPreview: vi.fn(),
    handleDeploy: vi.fn(),
    handleAdminIngest: vi.fn(),
    fosStatus: 'idle',
    handleAnalyzeLake: vi.fn(),
    importMode: 'all',
    importRange: { start: '', end: '' },
    handleJoin: vi.fn(),
    joinPhase: 'form',
    stop: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('WizardFooter — terraform (Terraform & VCL preview) step', () => {
  it("provision and ingest modes: 'Back to Review' returns to Review without deploying or ingesting", async () => {
    for (const mode of ['provision', 'ingest'] as const) {
      const setStep = vi.fn()
      const handleDeploy = vi.fn()
      const handleAdminIngest = vi.fn()
      const s = makeFooterState({ mode, setStep, handleDeploy, handleAdminIngest })
      const { unmount } = render(<WizardFooter s={s} />)

      expect(screen.queryByRole('button', { name: /deploy to fastly/i })).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /complete setup/i })).not.toBeInTheDocument()

      await userEvent.click(screen.getByRole('button', { name: /back to review/i }))
      expect(setStep).toHaveBeenCalledWith('execute')
      expect(handleDeploy).not.toHaveBeenCalled()
      expect(handleAdminIngest).not.toHaveBeenCalled()
      unmount()
    }
  })
})

function renderPicker(onChange: (v: string | null) => void, value: string | null = null) {
  return renderWithQuery(<RumVersionPicker serviceId={SVC} value={value} onChange={onChange} />)
}

describe('RumVersionPicker', () => {
  it('renders the loading state without crashing while the fetch is in flight', () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, async () => {
        await new Promise(() => {})
        return HttpResponse.json({ available: [] })
      }),
    )
    const { container } = renderPicker(() => {})
    expect(screen.getByText(/faro web sdk version/i)).toBeInTheDocument()
    expect(container.querySelector('[data-slot="skeleton"]')).toBeTruthy()
  })

  it('lists available versions, marks latest, and reports a selection', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({
          available: ['1.9.0', '1.8.0', '1.7.0'],
          current: null,
          latest: '1.9.0',
          update_available: false,
        }),
      ),
    )
    const seen: (string | null)[] = []
    const user = userEvent.setup()
    renderPicker((v) => seen.push(v))

    await waitFor(() => expect(seen).toContain('1.9.0'))

    await user.click(screen.getByRole('combobox', { name: /faro web sdk version/i }))
    await user.click(await screen.findByRole('option', { name: /1\.7\.0/ }))
    expect(seen).toContain('1.7.0')
  })

  it('degrades to an inline message + retry on a registry 503 or empty version list', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(
          { error: 'faro_registry_unavailable', message: 'registry down' },
          { status: 503 },
        ),
      ),
    )
    const { unmount } = renderPicker(() => {
      throw new Error('onChange must not fire on a degraded fetch')
    })

    expect(await screen.findByText(/couldn't reach the npm registry/i)).toBeInTheDocument()
    expect(screen.getByText(/default self-hosted version/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeEnabled()
    unmount()

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({ available: [], current: null, latest: null, update_available: false }),
      ),
    )
    renderPicker(() => {
      throw new Error('onChange must not fire when there is nothing to default to')
    })
    expect(await screen.findByText(/no versions available/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
  })

  it('retry re-fires the request after a registry outage clears', async () => {
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        calls += 1
        if (calls === 1) {
          return HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 })
        }
        return HttpResponse.json({
          available: ['2.0.0'],
          current: null,
          latest: '2.0.0',
          update_available: false,
        })
      }),
    )
    const seen: (string | null)[] = []
    const user = userEvent.setup()
    renderPicker((v) => seen.push(v))

    await screen.findByText(/couldn't reach the npm registry/i)
    await user.click(screen.getByRole('button', { name: /retry/i }))

    await waitFor(() => expect(seen).toContain('2.0.0'))
    expect(calls).toBe(2)
  })
})

const draftQueryClient = createTestQueryClient()

function makeDraft(overrides: Partial<WizardDraft> = {}): WizardDraft {
  return {
    version: WIZARD_DRAFT_VERSION,
    draftId: 'draft-1',
    mode: 'provision',
    step: 'fields',
    currentStep: 'fields',
    selectedServiceId: 'svc-1',
    selectedServiceName: 'My Service',
    selectedCdnServiceId: null,
    selectedCdnServiceName: null,
    tokenInfo: { id: 'tok-1', name: 'tok', type: 'user' },
    config: stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      endpoint_name: 'My EP',
      fos_bucket_name: 'b',
      fos_access_key: 'AK-LEAK',
      fos_secret_key: 'SK-LEAK',
      cdn_secret: 'CDN-LEAK',
    }),
    importMode: 'all',
    importRange: { start: '', end: '' },
    syncEnabled: true,
    syncIntervalMins: '2',
    icebergMetadataLocation: '',
    updatedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
    ...overrides,
  }
}

describe('ResumeBanner', () => {
  beforeEach(() => {
    window.localStorage.removeItem(WIZARD_DRAFT_KEY)
  })

  afterEach(() => {
    window.localStorage.removeItem(WIZARD_DRAFT_KEY)
  })

  it('renders the step label from the saved draft and fires the action callbacks', async () => {
    const user = userEvent.setup()
    const onResume = vi.fn()
    const onStartFresh = vi.fn()
    const draft = makeDraft({ currentStep: 'fields' })
    render(
      <ResumeBanner
        draft={draft}
        onResume={onResume}
        onStartFresh={onStartFresh}
      />,
    )
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument()
    expect(
      screen.getByTestId('resume-banner-resume'),
    ).toHaveTextContent(/Resume from Log Fields/i)
    await user.click(screen.getByTestId('resume-banner-resume'))
    expect(onResume).toHaveBeenCalledOnce()
    await user.click(screen.getByTestId('resume-banner-start-fresh'))
    expect(onStartFresh).toHaveBeenCalledOnce()
  })

  it('hydrates the saved draft from localStorage when the wizard opens', () => {
    saveDraft(makeDraft({ currentStep: 'fields' }))
    renderWithQuery(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, {
      queryClient: draftQueryClient,
    })
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument()
  })
})


const buildToggleGroup = (catalog: LogFieldsCatalog | null | undefined) => (
  prev: typeof INITIAL_CONFIG,
  groupId: string,
  checked: boolean,
) => ({
  ...prev,
  log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked),
})

const applyToggleField = (
  prev: typeof INITIAL_CONFIG,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
) => ({
  ...prev,
  log_fields: toggleLogFieldOverride(prev.log_fields, fieldId, checked, defaultEnabledByGroup),
})

const applyUpdateFieldLimit = (
  prev: typeof INITIAL_CONFIG,
  fieldId: string,
  limit?: number,
) => ({
  ...prev,
  log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit),
})

const buildTogglePreset = (
  catalog: LogFieldsCatalog | null | undefined,
  isPresetActive: (groups: string[]) => boolean,
) => (prev: typeof INITIAL_CONFIG, presetGroups: string[]) => ({
  ...prev,
  log_fields: toggleLogFieldPreset(prev.log_fields, catalog, presetGroups, isPresetActive),
})

const group = (id: string, label: string, requires: string | null = null) => ({
  id,
  label,
  description: '',
  locked: false,
  requires,
  note: null,
  recommended_with: null,
  total_bytes: 0,
  fields: [],
})

const PRESETS = {
  minimal: { label: 'Minimal', description: '', groups: ['core'] },
  standard: { label: 'Standard', description: '', groups: ['core', 'http'] },
  full: { label: 'Full', description: '', groups: ['core', 'http', 'tls', 'security'] },
}

const CATALOG: LogFieldsCatalog = {
  groups: [
    group('core', 'Core'),
    group('http', 'HTTP', 'core'),
    group('tls', 'TLS', 'core'),
    group('security', 'Security', 'http'),
    group('detail', 'Detail', 'http'),
  ],
  presets: PRESETS,
}

function configWithGroups(groups: string[]) {
  return {
    ...INITIAL_CONFIG,
    log_fields: { ...INITIAL_CONFIG.log_fields, groups },
  }
}

describe('buildToggleGroup', () => {
  it('adds a group with no dependencies and auto-adds required ancestor groups', () => {
    const toggle = buildToggleGroup(CATALOG)
    expect(toggle(configWithGroups([]), 'core', true).log_fields.groups).toEqual(['core'])
    expect(new Set(toggle(configWithGroups([]), 'security', true).log_fields.groups)).toEqual(
      new Set(['security', 'http', 'core']),
    )
    expect(new Set(toggle(configWithGroups(['http']), 'security', true).log_fields.groups)).toEqual(
      new Set(['http', 'core', 'security']),
    )
  })

  it('removes a group without touching dependencies of others and is idempotent', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups(['core', 'http', 'security']), 'security', false)
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['core', 'http']))
    expect(toggle(configWithGroups(['core']), 'core', true).log_fields.groups).toEqual(['core'])
  })

  it('does not mutate input config and tolerates null catalog or missing groups', () => {
    const toggle = buildToggleGroup(CATALOG)
    const input = configWithGroups(['core'])
    const inputGroupsRef = input.log_fields.groups
    toggle(input, 'http', true)
    expect(input.log_fields.groups).toBe(inputGroupsRef)

    expect(buildToggleGroup(null)(configWithGroups([]), 'core', true).log_fields.groups).toEqual(['core'])
    const config: typeof INITIAL_CONFIG = {
      ...INITIAL_CONFIG,
      log_fields: without(INITIAL_CONFIG.log_fields, 'groups') as typeof INITIAL_CONFIG.log_fields,
    }
    expect(toggle(config, 'core', true).log_fields.groups).toEqual(['core'])
  })
})

describe('applyToggleField & applyUpdateFieldLimit', () => {
  it('sets or clears field overrides based on group default', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: {} } }
    expect(applyToggleField(c1, 'cookie_id', true, false).log_fields.field_overrides).toEqual({ cookie_id: true })

    const c2 = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: { cookie_id: true, ip: false } },
    }
    expect(applyToggleField(c2, 'cookie_id', true, true).log_fields.field_overrides).toEqual({ ip: false })

    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_overrides') } }
    expect(applyToggleField(c3, 'cookie_id', false, true).log_fields.field_overrides).toEqual({ cookie_id: false })
  })

  it('sets, overwrites, and removes numeric field limits', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: {} } }
    expect(applyUpdateFieldLimit(c1, 'url', 2048).log_fields.field_limits).toEqual({ url: 2048 })

    const c2 = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: { url: 2048, ua: 512 } },
    }
    expect(applyUpdateFieldLimit(c2, 'url', undefined).log_fields.field_limits).toEqual({ ua: 512 })
    expect(applyUpdateFieldLimit(c2, 'url', 1024).log_fields.field_limits).toEqual({ url: 1024, ua: 512 })

    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_limits') } }
    expect(applyUpdateFieldLimit(c3, 'url', 4096).log_fields.field_limits).toEqual({ url: 4096 })
  })
})

describe('buildTogglePreset', () => {
  function isPresetActiveFor(groups: string[]) {
    const set = new Set(groups)
    return (presetGroups: string[]) => presetGroups.every((g) => set.has(g))
  }

  it('adds preset groups, removes unprotected groups on toggle-off, and cascades dependents', () => {
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor([]))
    expect(new Set(toggle(configWithGroups([]), PRESETS.full.groups).log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )

    const isolatedCatalog = { ...CATALOG, presets: { standard: PRESETS.standard } }
    const startingGroups = ['core', 'http', 'detail']
    const toggleOff = buildTogglePreset(isolatedCatalog, isPresetActiveFor(startingGroups))
    expect(toggleOff(configWithGroups(startingGroups), PRESETS.standard.groups).log_fields.groups).toEqual([])
  })

  it('keeps groups protected by other active presets and tolerates missing/null catalog', () => {
    const startingStd = ['core', 'http']
    const toggleStd = buildTogglePreset(CATALOG, isPresetActiveFor(startingStd))
    expect(toggleStd(configWithGroups(startingStd), PRESETS.standard.groups).log_fields.groups).toEqual(['core'])

    const startingFull = ['core', 'http', 'tls', 'security']
    const toggleFull = buildTogglePreset(CATALOG, isPresetActiveFor(startingFull))
    expect(new Set(toggleFull(configWithGroups(startingFull), PRESETS.standard.groups).log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )

    expect(buildTogglePreset({ groups: CATALOG.groups }, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
    expect(buildTogglePreset(null, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
  })
})

describe('wizard-draft persistence', () => {
  beforeEach(() => window.localStorage.clear())
  afterEach(() => window.localStorage.clear())

  it('strips secrets and mergePersistedConfig always returns blank secret fields', () => {
    const stripped = stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      endpoint_name: 'My EP',
      sample_rate: 50,
      fos_access_key: 'AK',
      fos_secret_key: 'SK',
      cdn_secret: 'CDN',
    })
    expect(stripped).not.toHaveProperty('fos_access_key')
    expect(stripped).not.toHaveProperty('fos_secret_key')
    expect(stripped).not.toHaveProperty('cdn_secret')
    expect(stripped).toMatchObject({ endpoint_name: 'My EP', sample_rate: 50 })

    const dirty = {
      ...stripSecretsFromConfig(INITIAL_CONFIG),
      fos_access_key: 'LEAK',
      fos_secret_key: 'LEAK',
      cdn_secret: 'LEAK',
    }
    const merged = mergePersistedConfig(dirty)
    expect(merged.fos_access_key).toBe('')
    expect(merged.fos_secret_key).toBe('')
    expect(merged.cdn_secret).toBe('')
  })

  it('roundtrips a non-secret draft with zero secret keys or values in localStorage', () => {
    const draft = makeDraft()
    saveDraft(draft)
    const loaded = loadDraft()
    expect(loaded).not.toBeNull()
    expect(loaded!.draftId).toBe('draft-1')
    expect(loaded!.config.endpoint_name).toBe('My EP')

    const raw = window.localStorage.getItem(WIZARD_DRAFT_KEY)!
    expect(raw).not.toContain('AK-LEAK')
    expect(raw).not.toContain('SK-LEAK')
    expect(raw).not.toContain('CDN-LEAK')
  })

  it('handles version mismatch, mode step, invalid JSON, clearDraft, and broken localStorage', () => {
    saveDraft(makeDraft({ version: WIZARD_DRAFT_VERSION + 1 }))
    expect(loadDraft()).toBeNull()
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).toBeNull()

    saveDraft(makeDraft({ step: 'mode', currentStep: 'mode' }))
    expect(loadDraft()).toBeNull()

    window.localStorage.setItem(WIZARD_DRAFT_KEY, '{not json')
    expect(loadDraft()).toBeNull()

    clearDraft()
    expect(loadDraft()).toBeNull()

    const origSet = window.localStorage.setItem
    window.localStorage.setItem = () => { throw new Error('quota') }
    try {
      expect(() => saveDraft(makeDraft())).not.toThrow()
    } finally {
      window.localStorage.setItem = origSet
    }
  })
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
