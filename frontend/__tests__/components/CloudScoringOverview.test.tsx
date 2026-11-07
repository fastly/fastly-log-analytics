import { fireEvent, render, renderHook, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { PropsWithChildren } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { CloudScoringOverview } from '@/components/SessionScoring/CloudScoringOverview'
import { useScoringLabels } from '@/components/SessionScoring/LabelsTab'
import { FlagSessionPopover } from '@/components/SessionScoring/SessionFlagAndEvents'
import { adminFetch, client } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'

vi.mock('@/lib/api', async (original) => ({
  ...await original<typeof import('@/lib/api')>(),
  adminFetch: vi.fn(),
}))
vi.mock('@/components/ReportLayout', () => ({ BackToAdminLink: () => <a href="/admin">Admin</a> }))
vi.mock('@/components/SessionScoring/TopFlaggedTable', () => ({
  TopFlaggedTable: () => <div>Recorded scored requests</div>,
}))
vi.mock('@/components/SessionScoring/ScoringOverviewTab', () => ({
  ScoreDistChart: () => <div>Recorded distribution</div>,
  ComplianceChart: () => <div>Recorded compliance</div>,
  ScorerLatencyChart: () => <div>Recorded latency</div>,
}))

function wrapper({ children }: PropsWithChildren) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

beforeEach(() => {
  setCloudModeRuntime(true)
  vi.mocked(adminFetch).mockImplementation(async (input) => {
    if (String(input) === '/api/services') return Response.json({ services: [{ service_id: 'fixture-service' }] })
    return Response.json({ available: false, reason: 'scoring_fields_not_present' })
  })
})
afterEach(() => {
  setCloudModeRuntime(false)
  vi.restoreAllMocks()
})

describe('Cloud scoring capability views', () => {
  it('shows not-configured analytics without mounting control-plane reads or actions', async () => {
    render(<CloudScoringOverview />, { wrapper })
    await screen.findByText(/Session scoring is not configured in the retained logs/)
    expect(screen.getByText(/Labels, trained matrices, evaluation curves/)).toBeInTheDocument()
    expect(screen.getByText(/Labels, trained matrices, evaluation curves/).closest('[role]')).toHaveAttribute('role', 'status')
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByRole('tab', { name: 'Labels' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Retrain|Rotate key|Enable scoring/ })).not.toBeInTheDocument()
    expect(vi.mocked(adminFetch).mock.calls.map(([path]) => String(path))).toEqual([
      '/api/services', '/api/services/fixture-service/scoring/analytics?since_hours=24',
    ])
  })

  it('renders recorded metrics while leaving missing optional fields explicitly unavailable', async () => {
    vi.mocked(adminFetch).mockImplementation(async (input) => String(input) === '/api/services'
      ? Response.json({ services: [{ service_id: 'fixture-service' }] })
      : Response.json({
        available: true, health: { scored_rows: 2, distinct_sids: 2, avg_score: 50, p95_score: 81.5 },
        top_flagged: { rows: [] }, score_distribution: { rows: [] },
        compliance_breakdown: { available: false }, latency_timeseries: { available: false },
      }))
    render(<CloudScoringOverview />, { wrapper })
    await screen.findByText('Recorded scored requests')
    expect(screen.getByText('81.5')).toBeInTheDocument()
    expect(screen.getByText('Recorded distribution')).toBeInTheDocument()
    expect(screen.getByText(/Cookie compliance was not recorded/)).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByText('Recorded compliance')).not.toBeInTheDocument()
    fireEvent.change(screen.getByRole('combobox', { name: 'Scoring time range' }), { target: { value: '6' } })
    await waitFor(() => expect(adminFetch).toHaveBeenCalledWith(
      '/api/services/fixture-service/scoring/analytics?since_hours=6', expect.anything(),
    ))
  })

  it('keeps failed scoring reads as alerts', async () => {
    vi.mocked(adminFetch).mockResolvedValue(Response.json({ detail: 'unavailable' }, { status: 500 }))
    render(<CloudScoringOverview />, { wrapper })
    expect(await screen.findByRole('alert')).toHaveTextContent('Session scoring read failed (500)')
  })

  it('suppresses session label reads and disables flags in Cloud', () => {
    const get = vi.spyOn(client, 'GET')
    renderHook(() => useScoringLabels('fixture-service'), { wrapper })
    render(<FlagSessionPopover serviceId="fixture-service" sid="sid-a" />, { wrapper })
    const flag = screen.getByRole('button', { name: 'Session labels are unavailable in Cloud' })
    expect(flag).toBeDisabled()
    expect(get).not.toHaveBeenCalled()
  })
})
