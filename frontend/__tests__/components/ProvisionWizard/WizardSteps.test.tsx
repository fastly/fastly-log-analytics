/**
 * @vitest-environment jsdom
 *
 * Consolidated ProvisionWizard component tests: TerraformStep,
 * topology capability (ModeStep + JoinStep), WizardFooter, RumVersionPicker, and ResumeBanner.
 */
import * as React from 'react'
import { without } from '@/__tests__/helpers/page-smoke'
import {
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/query-keys'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'

import { ProvisionWizard, TerraformStep, ModeStep } from '@/components/ProvisionWizard/ProvisionWizard'
import { JoinStep } from '@/components/ProvisionWizard/steps/JoinStep'
import { ResumeBanner, WizardFooter } from '@/components/ProvisionWizard/WizardFooter'
import { RumVersionPicker } from '@/components/ProvisionWizard/steps/StorageStep'
import {
  WIZARD_DRAFT_KEY,
  clearDraft,
  loadDraft,
  mergePersistedConfig,
  saveDraft,
  stripSecretsFromConfig,
} from '@/components/ProvisionWizard/useWizardState'
import {
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type LogFieldsCatalog,
  type WizardDraft,
  type WizardState,
} from '@/components/ProvisionWizard/types'
import { createTestQueryClient, renderWithQuery } from '../../helpers/page-smoke'
import { createUseSSEMock } from '../../helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { getApiBase } from '@/lib/api'

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useServiceStore: () => ({
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    services: [],
  }),
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
