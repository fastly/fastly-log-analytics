/**
 * @vitest-environment jsdom
 */
import React from 'react'
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { client } from '@/lib/api'
import { StatusPanel } from '@/components/SessionScoring/StatusPanel'

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('StatusPanel in Cloud Attached capture mode', () => {
  beforeEach(() => {
    setCloudModeRuntime(true)
  })

  afterEach(() => {
    setCloudModeRuntime(false)
    vi.restoreAllMocks()
  })

  it('shows Log Fields Off (Attached) and Configure Log Fields CTA instead of Managed Enable', async () => {
    vi.spyOn(client, 'GET').mockImplementation(async (path: string) => {
      if (path.includes('scoring/status')) {
        return {
          data: {
            enabled: false,
            capture: 'attached',
            scoring_fields_enabled: false,
          },
          response: new Response('{}', { status: 200 }),
        } as never
      }
      return {
        data: {},
        response: new Response('{}', { status: 200 }),
      } as never
    })

    renderWithQuery(<StatusPanel serviceId="svc-attach" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-session-scoring-status-badge')).toHaveTextContent(
        'Log Fields Off (Attached)',
      )
    })
    expect(screen.getByTestId('admin-session-scoring-attached-fields-warning')).toBeInTheDocument()
    const cta = screen.getByTestId('admin-session-scoring-configure-fields-btn')
    expect(cta).toHaveAttribute('href', '/admin?service=svc-attach')
    expect(screen.queryByTestId('admin-session-scoring-enable-btn')).not.toBeInTheDocument()
  })

  it('shows Enabled (Attached) when edge_score log fields are active', async () => {
    vi.spyOn(client, 'GET').mockImplementation(async (path: string) => {
      if (path.includes('scoring/status')) {
        return {
          data: {
            enabled: false,
            capture: 'attached',
            scoring_fields_enabled: true,
          },
          response: new Response('{}', { status: 200 }),
        } as never
      }
      return {
        data: { n_good: 0, n_bad: 0, min_per_class: 5, has_min_samples: false },
        response: new Response('{}', { status: 200 }),
      } as never
    })

    renderWithQuery(<StatusPanel serviceId="svc-attach" />)

    await waitFor(() => {
      expect(screen.getByTestId('admin-session-scoring-status-badge')).toHaveTextContent(
        'Enabled (Attached)',
      )
    })
    expect(screen.getByTestId('admin-session-scoring-configure-fields-btn')).toHaveAttribute(
      'href',
      '/admin?service=svc-attach',
    )
    expect(screen.queryByTestId('admin-session-scoring-disable-btn')).not.toBeInTheDocument()
  })
})
