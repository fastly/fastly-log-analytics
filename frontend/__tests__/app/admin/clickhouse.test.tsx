import React from 'react'
import { afterEach, describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { http, HttpResponse } from 'msw'

import { renderWithQueryClient } from '@/__tests__/helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { useServiceStore } from '@/stores/serviceStore'
import Loading from '@/app/admin/clickhouse/loading'
import ClickHouseAdminPage from '@/app/admin/clickhouse/page'
import ClickHouseAdminClient from '@/app/admin/clickhouse/_sections/ClickHouseAdminClient'
import { ClickHouseStatusNotice } from '@/app/admin/clickhouse/_sections/ClickHouseStatusNotice'

const notFoundMock = vi.fn(() => {
  throw new Error('NEXT_HTTP_ERROR_FALLBACK;404')
})

vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams('service=svc-test'),
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  usePathname: () => '/admin/clickhouse',
  notFound: () => notFoundMock(),
}))

describe('/admin/clickhouse page & ClickHouseAdminClient', () => {
  beforeEach(() => {
    notFoundMock.mockClear()
    vi.unstubAllEnvs()
    useServiceStore.setState({
      activeServiceId: 'svc-test',
      services: [{ id: 'svc-test', name: 'Test Service' }],
    })
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('renders route-level loading skeleton with pre-allocated zero-CLS reservation containers', () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    render(<Loading />)
    expect(screen.getByTestId('admin-clickhouse-loading')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-toolbar')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-vitals-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-generation-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-replay-reservation')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toHaveAttribute('aria-hidden', 'true')
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toHaveAttribute('aria-hidden', 'true')
  })

  it('triggers notFound() for the route-level loading shell in Standard deployment mode', () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'standard')
    expect(() => render(<Loading />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')
    expect(notFoundMock).toHaveBeenCalled()
  })

  it('keeps notice reservations mounted while status changes and exposes only the applicable notice', () => {
    const { rerender } = render(<ClickHouseStatusNotice />)
    const disabled = screen.getByTestId('admin-clickhouse-disabled-banner')
    const unavailable = screen.getByTestId('admin-clickhouse-unavailable-banner')
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    expect(unavailable).toHaveAttribute('aria-hidden', 'true')

    rerender(<ClickHouseStatusNotice enabled={false} health="disabled" />)
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toBe(disabled)
    expect(disabled).toHaveAttribute('aria-hidden', 'false')
    expect(disabled).not.toHaveClass('invisible')
    expect(unavailable).toHaveClass('invisible')

    rerender(<ClickHouseStatusNotice enabled health="unavailable" />)
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toBe(unavailable)
    expect(unavailable).toHaveAttribute('aria-hidden', 'false')
    expect(unavailable).not.toHaveClass('invisible')
    expect(disabled).toHaveClass('invisible')

    rerender(<ClickHouseStatusNotice enabled health="ok" />)
    expect(disabled).toHaveAttribute('aria-hidden', 'true')
    expect(unavailable).toHaveAttribute('aria-hidden', 'true')
    expect(disabled).toHaveClass('invisible')
    expect(unavailable).toHaveClass('invisible')
  })

  it('triggers notFound() on /admin/clickhouse in Standard deployment mode and renders disabled client banner when mounted in High-Scale mode', async () => {
    vi.stubEnv('DEPLOYMENT_MODE', 'standard')
    expect(() => renderWithQueryClient(<ClickHouseAdminPage />)).toThrow('NEXT_HTTP_ERROR_FALLBACK;404')
    expect(notFoundMock).toHaveBeenCalled()

    vi.stubEnv('DEPLOYMENT_MODE', 'high_scale')
    renderWithQueryClient(<ClickHouseAdminPage />)

    expect(screen.getByTestId('admin-clickhouse-root')).toBeInTheDocument()
    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('disabled')
    })
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-disabled-banner')).toHaveAttribute('aria-hidden', 'false')
    expect(screen.getByTestId('admin-clickhouse-vital-enabled')).toHaveTextContent('Disabled')
    expect(screen.getByTestId('admin-clickhouse-replay-disabled-hint')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-replay-submit-btn')).toBeDisabled()
  })

  it('renders High-Scale mode ok state, active generation, pre-fill, and dry-run replay workflow', async () => {
    server.use(
      http.get('*/api/admin/clickhouse/status', () =>
        HttpResponse.json({
          service_id: 'svc-test',
          enabled: true,
          health: 'ok',
          schema_version: 2,
          publication_counts: { pending: 2, claimed: 1, failed: 0, published: 14 },
          oldest_pending_age_seconds: 18.4,
          last_published_at: '2026-10-20T12:00:00Z',
          active_generation: {
            generation: 'gen-active-01',
            dataset_id: 'ds-active-01',
            coverage_start: '2026-10-20T10:00:00Z',
            coverage_end: '2026-10-20T12:00:00Z',
            expires_at: '2026-10-21T12:00:00Z',
            expired: false,
            target_matches: true,
            coverage_age_seconds: 120,
          },
          expired_dataset_count: 3,
        }),
      ),
      http.post('*/api/admin/clickhouse/replay', async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({
          service_id: body.service_id,
          dry_run: body.dry_run,
          operation: body.dataset_id ? 'rebuild' : 'resume',
          dataset_id: (body.dataset_id as string) || 'ds-active-01',
          generation: (body.generation as string) || 'gen-active-01',
          artifact_count: 5,
          planned_artifacts: 5,
          attempted: body.dry_run ? 0 : 5,
          published: body.dry_run ? 0 : 5,
          activated: !body.dry_run,
          limit: body.limit ?? 100,
        })
      }),
    )

    renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('ok')
    })

    expect(screen.getByTestId('admin-clickhouse-vital-enabled')).toHaveTextContent('High-Scale (Enabled)')
    expect(screen.getByTestId('admin-clickhouse-vital-counts')).toHaveTextContent('14 / 2')
    expect(screen.getByTestId('admin-clickhouse-gen-id')).toHaveTextContent('gen-active-01')
    expect(screen.getByTestId('admin-clickhouse-dataset-id')).toHaveTextContent('ds-active-01')

    // Pre-fill reference from active generation
    fireEvent.click(screen.getByTestId('admin-clickhouse-prefill-btn'))
    const refInput = screen.getByTestId('admin-clickhouse-reference-input') as HTMLInputElement
    expect(refInput.value).toBe('ds-active-01')

    // Submit dry-run preview
    fireEvent.click(screen.getByTestId('admin-clickhouse-replay-submit-btn'))

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toBeInTheDocument()
    })
    expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toHaveTextContent('Dry-Run Replay Preview')
    expect(screen.getByTestId('admin-clickhouse-replay-outcome')).toHaveTextContent('operation: rebuild')
  })

  it('renders unavailable 503 status payload gracefully without crashing', async () => {
    server.use(
      http.get('*/api/admin/clickhouse/status', () =>
        HttpResponse.json(
          {
            service_id: 'svc-test',
            enabled: true,
            health: 'unavailable',
            schema_version: 1,
            publication_counts: { pending: 0, claimed: 0, failed: 1, published: 4 },
            oldest_pending_age_seconds: null,
            last_published_at: null,
            active_generation: null,
            expired_dataset_count: 0,
          },
          { status: 503 },
        ),
      ),
    )

    renderWithQueryClient(<ClickHouseAdminClient initialServiceId="svc-test" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-clickhouse-health-badge')).toHaveTextContent('unavailable')
    })
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toBeInTheDocument()
    expect(screen.getByTestId('admin-clickhouse-unavailable-banner')).toHaveAttribute('aria-hidden', 'false')
  })
})
