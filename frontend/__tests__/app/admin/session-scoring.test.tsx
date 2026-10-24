/**
 * Unit & integration smoke suite for `/admin/session-scoring`.
 *
 * Validates:
 *   1. Page mounts without unhandled requests and fires `/scoring/analytics`
 *      and `/scoring/config` composite requests.
 *   2. Zero-CLS reservation containers (`admin-session-scoring-status-reservation`,
 *      `admin-session-scoring-overview-reservation`) carry `[contain-intrinsic-size:auto_*]`
 *      and `[content-visibility:auto]`.
 *   3. Tab triggers (`Overview`, `Labels`, `Matrix history`, `Audit`) and manual
 *      `Refresh` (`admin-session-scoring-refresh-btn`) are wired and interactive.
 *   4. Direct navigation when `activeServiceId` is empty falls back to `/api/services`.
 */
import { describe, expect, test, vi, beforeEach } from 'vitest'
import { fireEvent, screen, waitFor } from '@testing-library/react'
import { renderWithQuery } from '../../helpers/query'
import { http, HttpResponse } from 'msw'
import React from 'react'

import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import SessionScoringPage from '@/app/admin/session-scoring/page'

vi.mock('next/navigation', async () => (await import('../../helpers/page-smoke')).navigationModuleMock('/admin/session-scoring'))

vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: () => () => null,
}))

vi.mock('@/components/SessionScoring/RocPrCurves', () => ({ RocPrCurves: () => null }))
vi.mock('@/components/SessionScoring/ScoreDistChart', () => ({ ScoreDistChart: () => null }))
vi.mock('@/components/SessionScoring/ScorerLatencyChart', () => ({ ScorerLatencyChart: () => null }))
vi.mock('@/components/SessionScoring/ScorerErrorsChart', () => ({ ScorerErrorsChart: () => null }))
vi.mock('@/components/SessionScoring/ScorerFailOpenBreakdownCard', () => ({
  ScorerFailOpenBreakdownCard: () => null,
}))
vi.mock('@/components/SessionScoring/ComplianceChart', () => ({ ComplianceChart: () => null }))
vi.mock('@/components/SessionScoring/ScoringHealthCard', () => ({ ScoringHealthCard: () => null }))
vi.mock('@/components/SessionScoring/StatusPanel', () => ({
  StatusPanel: ({ serviceId }: { serviceId: string }) => (
    <div data-testid="admin-session-scoring-status-card">Status for {serviceId}</div>
  ),
}))
vi.mock('@/components/SessionScoring/ThresholdSlider', () => ({ ThresholdSlider: () => null }))
vi.mock('@/components/SessionScoring/L2EnforcementCard', () => ({ L2EnforcementCard: () => null }))
vi.mock('@/components/SessionScoring/TopFlaggedTable', () => ({ TopFlaggedTable: () => null }))
vi.mock('@/components/SessionScoring/PerReasonAucCard', () => ({ PerReasonAucCard: () => null }))
vi.mock('@/components/SessionScoring/MatrixVersionsCard', () => ({ MatrixVersionsCard: () => null }))
vi.mock('@/components/SessionScoring/ExcludeRegexCard', () => ({ ExcludeRegexCard: () => null }))
vi.mock('@/components/SessionScoring/RetrainButton', () => ({ RetrainButton: () => null }))
vi.mock('@/components/SessionScoring/RotateKeyButton', () => ({ RotateKeyButton: () => null }))
vi.mock('@/components/SessionScoring/SinceHoursPicker', () => ({ SinceHoursPicker: () => null }))

const API_BASE = getApiBase()

beforeEach(() => {
  useServiceStore.setState({
    activeServiceId: 'svc-default',
    isInitialized: true,
  })
})

describe('admin/session-scoring page', () => {
  test('renders without unhandled requests (default MSW handlers cover the composites)', async () => {
    const analyticsHits: string[] = []
    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, ({ params }) => {
        analyticsHits.push(String(params.service_id))
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({
          status: { enabled: false },
          threshold: { value: 50 },
          exclude_regex: { pattern: '' },
          enforce_status_code: { code: 403 },
          matrix_versions: { versions: [] },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(analyticsHits, 'scoring/analytics composite must have been called').not.toEqual([])
      expect(configHits, 'scoring/config composite must have been called').not.toEqual([])
    })

    expect(analyticsHits[0]).toBe('svc-default')
    expect(configHits[0]).toBe('svc-default')

    // Zero-CLS layout reservation containers and tabs must be present
    const root = screen.getByTestId('admin-session-scoring-root')
    const statusRes = screen.getByTestId('admin-session-scoring-status-reservation')
    const overviewRes = screen.getByTestId('admin-session-scoring-overview-reservation')
    expect(root).toBeTruthy()
    expect(statusRes.className).toContain('[contain-intrinsic-size:auto_220px]')
    expect(statusRes.className).toContain('[content-visibility:auto]')
    expect(overviewRes.className).toContain('[contain-intrinsic-size:auto_900px]')
    expect(overviewRes.className).toContain('[content-visibility:auto]')

    expect(screen.getByTestId('admin-session-scoring-tab-overview')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-labels')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-matrix')).toBeTruthy()
    expect(screen.getByTestId('admin-session-scoring-tab-audit')).toBeTruthy()
  })

  test('manual refresh button invalidates and refetches composites', async () => {
    let configCalls = 0
    let analyticsCalls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () => {
        analyticsCalls += 1
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, () => {
        configCalls += 1
        return HttpResponse.json({
          status: { enabled: true },
          threshold: { threshold: 75 },
          exclude_regex: { pattern: '' },
          enforce_status_code: { effective: 429 },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(configCalls).toBe(1)
      expect(analyticsCalls).toBe(1)
    })

    const refreshBtn = screen.getByTestId('admin-session-scoring-refresh-btn')
    fireEvent.click(refreshBtn)

    await waitFor(() => {
      expect(configCalls).toBe(2)
      expect(analyticsCalls).toBe(2)
    })
  })

  test('falls back to /api/services when activeServiceId is initially empty', async () => {
    useServiceStore.setState({
      activeServiceId: null,
      isInitialized: false,
    })

    const configHits: string[] = []
    server.use(
      http.get(`${API_BASE}/api/services`, () => {
        return HttpResponse.json({
          services: [{ service_id: 'svc-fallback', name: 'Fallback Service' }],
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () => {
        return HttpResponse.json({
          health: { ok: true },
          top_flagged: { rows: [] },
          score_distribution: { buckets: [] },
          compliance_breakdown: { categories: [] },
          evaluation_per_reason: { rows: [] },
          evaluation: { mean_score: 0 },
        })
      }),
      http.get(`${API_BASE}/api/services/:service_id/scoring/config`, ({ params }) => {
        configHits.push(String(params.service_id))
        return HttpResponse.json({
          status: { enabled: false },
          threshold: { threshold: null },
          exclude_regex: { pattern: '' },
          enforce_status_code: { effective: 429 },
        })
      }),
    )

    renderWithQuery(<SessionScoringPage />)

    await waitFor(() => {
      expect(configHits).toContain('svc-fallback')
    })
  })
})
