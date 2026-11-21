/**
 * @vitest-environment jsdom
 */
import * as React from 'react'
import { renderHook, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { createTestQueryClient, makeQueryWrapper, renderWithQuery, stubMatchMedia, stubPointerCaptureAndScroll } from '@/__tests__/helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import {
  ExecuteStep,
  INITIAL_CONFIG,
  type LogFieldsCatalog,
  type WizardState,
} from '@/components/ProvisionWizard/ProvisionWizard'
import { useWizardState } from '@/components/ProvisionWizard/useWizardState'

stubMatchMedia()
stubPointerCaptureAndScroll()

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useTimezoneStore: () => ({ timezone: 'UTC' }),
}))

const SAMPLE_CATALOG: LogFieldsCatalog = {
  groups: [
    {
      id: null,
      label: 'Core Request',
      description: 'Base request fields',
      locked: true,
      requires: null,
      note: null,
      recommended_with: null,
      total_bytes: 120,
      fields: ['timestamp', 'status'],
    },
    {
      id: 'A',
      label: 'Geo & Network',
      description: 'Client IP and geo',
      locked: false,
      requires: null,
      note: null,
      recommended_with: null,
      total_bytes: 80,
      fields: ['client_ip', 'geo_country'],
    },
    {
      id: 'B',
      label: 'Cache Performance',
      description: 'Cache status and age',
      locked: false,
      requires: null,
      note: null,
      recommended_with: null,
      total_bytes: 60,
      fields: ['fastly_info'],
    },
    {
      id: 'C',
      label: 'TLS & Security',
      description: 'TLS protocol and cipher',
      locked: false,
      requires: null,
      note: null,
      recommended_with: null,
      total_bytes: 40,
      fields: ['tls_protocol'],
    },
  ],
  fields: [
    { id: 'timestamp', group: null, label: 'Timestamp', description: '', sql_type: 'TIMESTAMP', typical_bytes: 60 },
    { id: 'status', group: null, label: 'Status', description: '', sql_type: 'INT', typical_bytes: 60 },
    { id: 'client_ip', group: 'A', label: 'Client IP', description: '', sql_type: 'VARCHAR', typical_bytes: 40 },
    { id: 'geo_country', group: 'A', label: 'Geo Country', description: '', sql_type: 'VARCHAR', typical_bytes: 40 },
    { id: 'fastly_info', group: 'B', label: 'Fastly Info', description: '', sql_type: 'VARCHAR', typical_bytes: 60 },
    { id: 'tls_protocol', group: 'C', label: 'TLS Protocol', description: '', sql_type: 'VARCHAR', typical_bytes: 40 },
  ],
  insights: [
    {
      id: 'geo_insights',
      category: 'traffic',
      title: 'Geographic Traffic',
      description: 'Country and region distribution',
      required_fields: ['geo_country'],
      required_groups: ['A'],
    },
    {
      id: 'cache_insights',
      category: 'performance',
      title: 'Cache Efficiency',
      description: 'Hit ratio and shield analysis',
      required_fields: ['fastly_info'],
      required_groups: ['B'],
    },
  ],
  presets: {
    standard: {
      label: 'Standard',
      description: 'Core + Geo + Cache',
      groups: ['A', 'B'],
    },
  },
}

afterEach(() => {
  setCloudModeRuntime(false)
  server.resetHandlers()
  vi.restoreAllMocks()
})

function makeReviewState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    config: {
      ...INITIAL_CONFIG,
      log_fields: { groups: ['A', 'B'], field_overrides: {} },
    },
    catalog: SAMPLE_CATALOG,
    isLoadingCatalog: false,
    estimatedBytes: 260,
    selectedService: { id: 'svc-1', name: 'SE Demo Site', provisioned: false, version: 1 },
    isDeploying: false,
    lines: [],
    status: 'idle',
    sseError: null,
    ...overrides,
  } as unknown as WizardState
}

describe('ReviewStep (ExecuteStep)', () => {
  it('shows all enabled group pills and field counts even when a preset matches', () => {
    renderWithQuery(<ExecuteStep s={makeReviewState()} />)

    expect(screen.getByText('Standard Preset')).toBeInTheDocument()
    expect(screen.getByText(/Core Request/)).toBeInTheDocument()
    expect(screen.getByText(/Geo & Network/)).toBeInTheDocument()
    expect(screen.getByText(/Cache Performance/)).toBeInTheDocument()
    expect(screen.getByText(/5 fields/)).toBeInTheDocument()
    expect(screen.getByText(/~260 B \/ line/)).toBeInTheDocument()
    expect(screen.getByText('Geographic Traffic')).toBeInTheDocument()
    expect(screen.getByText('Cache Efficiency')).toBeInTheDocument()
  })

  it('computes log line size from catalog even if s.estimatedBytes is 0', () => {
    renderWithQuery(<ExecuteStep s={makeReviewState({ estimatedBytes: 0 })} />)
    expect(screen.getByText(/~260 B \/ line/)).toBeInTheDocument()
  })

  it('shows loading indicator for log line size while catalog is still loading', () => {
    renderWithQuery(<ExecuteStep s={makeReviewState({ catalog: undefined, isLoadingCatalog: true, estimatedBytes: 0 })} />)
    expect(screen.getByText(/Loading fields/i)).toBeInTheDocument()
  })

  it('shows Cloud Snapshot Interval in High-Scale mode and omits it in Cloud mode', () => {
    const { rerender } = renderWithQuery(
      <ExecuteStep s={makeReviewState({ config: { ...INITIAL_CONFIG, snapshot_interval_minutes: 60 } })} />,
    )
    expect(screen.getByText('Cloud Snapshot Interval')).toBeInTheDocument()
    expect(screen.getByText('1h')).toBeInTheDocument()

    setCloudModeRuntime(true)
    rerender(<ExecuteStep s={makeReviewState({ config: { ...INITIAL_CONFIG, snapshot_interval_minutes: 60 } })} />)
    expect(screen.queryByText('Cloud Snapshot Interval')).not.toBeInTheDocument()
  })
})

describe('useWizardState catalog fetching', () => {
  it('fetches catalog on mount in Cloud mode even when fields step is not active (e.g. attach mode)', async () => {
    setCloudModeRuntime(true)
    server.use(
      http.get('*/api/log-fields/catalog', () => HttpResponse.json(SAMPLE_CATALOG)),
    )

    const { result } = renderHook(() => useWizardState(true, vi.fn()), {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })

    await waitFor(() => {
      expect(result.current.catalog?.groups?.length).toBe(4)
    })
    expect(result.current.step).toBe('token')
  })
})
