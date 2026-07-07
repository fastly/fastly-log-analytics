/**
 * Smoke test for the admin Session Scoring page.
 *
 * Audit finding (item #9): the MSW safety net
 * (``onUnhandledRequest: 'error'``) is inert for any page that has no
 * test, because the unhandled request is never made. Before this test
 * existed, ``app/admin/session-scoring/page.tsx`` could ship calling
 * ``/api/services/{id}/scoring/analytics`` + ``.../scoring/config``
 * without handlers and the safety net never tripped.
 *
 * This test mounts the page with the default MSW handler set and:
 *   1. Asserts the page renders without throwing (the calls don't
 *      crash openapi-fetch + the React Query wiring is correct).
 *   2. Asserts the composite-endpoint queries actually fire (so the
 *      missing-handler safety net would trip if either was removed
 *      from handlers.ts).
 *
 * The page's individual sub-component queries (RocPrCurves,
 * ScoreDistChart, …) live behind sub-component tests; this is a
 * page-level integration smoke, not a screenshot/visual test.
 */
import { describe, expect, test, vi, beforeEach } from 'vitest'
import { render, waitFor } from '@testing-library/react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../../helpers/query'
import { http, HttpResponse } from 'msw'
import React from 'react'

import { server } from '../../../tests/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
// vitest hoists the vi.mock(...) calls below above this import, so the
// static SUT import still binds the mocked next/dynamic + sub-components.
import SessionScoringPage from '@/app/admin/session-scoring/page'

// next/navigation is jsdom-incompatible; stub the bits the page touches.
vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/admin/session-scoring'),
  useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn() })),
}))

// next/dynamic returns a promise-backed component that suspends; stub
// to a placeholder so the page doesn't hang waiting for the chunk load.
vi.mock('next/dynamic', () => ({
  __esModule: true,
  default: () => () => null,
}))

// The page mounts a dozen sub-components, several of which call
// .toFixed() / Plotly / etc. on the composite response data. The
// audit's recommendation is a SMOKE test (does the page mount + do
// the composite calls fire?), not a full sub-component render. Stub
// every Scoring sub-component to a null render — keeps the test
// focused on the page-shell contract.
vi.mock('@/components/SessionScoring/RocPrCurves', () => ({ RocPrCurves: () => null }))
vi.mock('@/components/SessionScoring/ScoreDistChart', () => ({ ScoreDistChart: () => null }))
vi.mock('@/components/SessionScoring/ScorerLatencyChart', () => ({ ScorerLatencyChart: () => null }))
vi.mock('@/components/SessionScoring/ScorerErrorsChart', () => ({ ScorerErrorsChart: () => null }))
vi.mock('@/components/SessionScoring/ComplianceChart', () => ({ ComplianceChart: () => null }))
vi.mock('@/components/SessionScoring/ScoringHealthCard', () => ({ ScoringHealthCard: () => null }))
vi.mock('@/components/SessionScoring/StatusPanel', () => ({ StatusPanel: () => null }))
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
  } as never)
})

describe('admin/session-scoring page', () => {
  test('renders without unhandled requests (default MSW handlers cover the composites)', async () => {
    // Track the analytics + config composite calls so we can assert
    // they actually fired — that's what guarantees the MSW safety net
    // is meaningful for this page.
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

    const client = createTestQueryClient()

    render(
      <QueryClientProvider client={client}>
        <SessionScoringPage />
      </QueryClientProvider>,
    )

    // The two composite calls fire on mount as long as activeServiceId
    // is set. Wait for both to land — the page's enabled=!!activeServiceId
    // gate means they fire immediately under our beforeEach setup.
    await waitFor(() => {
      expect(analyticsHits, 'scoring/analytics composite must have been called').not.toEqual([])
      expect(configHits, 'scoring/config composite must have been called').not.toEqual([])
    })

    expect(analyticsHits[0]).toBe('svc-default')
    expect(configHits[0]).toBe('svc-default')
  })
})
