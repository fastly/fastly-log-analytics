/**
 * StatusPanel contract for Session Scoring prerequisite pragmas.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubGetAnimations, stubPointerCaptureAndScroll } from '@/__tests__/helpers/browser-stubs'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach } from 'vitest'
import { act, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'

import { renderWithQuery } from '../../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { StatusPanel } from '@/components/SessionScoring/StatusPanel'

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
  stubGetAnimations()
})

const API_BASE = getApiBase()
const SVC = 'svc-pragma-test'

function renderPanel() {
  return renderWithQuery(<StatusPanel serviceId={SVC} />)
}

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: SVC, isInitialized: true })
})

describe('StatusPanel preflight pragma gate', () => {
  it('opens the prerequisite modal instead of SSEModal when preflight_pragmas_ready is false, without exposing internal pragma names', async () => {
    let statusCalls = 0
    let readyState = false

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () => {
        statusCalls += 1
        return HttpResponse.json({
          enabled: false,
          preflight_pragmas_ready: readyState,
        })
      }),
    )

    const user = userEvent.setup()
    renderPanel()

    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()

    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))

    const modal = await screen.findByTestId('admin-session-scoring-pragma-modal')
    expect(modal).toBeInTheDocument()
    expect(screen.getByText(/Preflight Request Body Preservation Required/i)).toBeInTheDocument()

    const msgEl = screen.getByTestId('admin-session-scoring-pragma-message')
    expect(msgEl.textContent).toContain(SVC)
    expect(msgEl.textContent).toContain('Fastly team')
    // Must never leak internal pragma names or hctl CLI syntax
    expect(modal.textContent).not.toContain('fix_unsent_body_drain')
    expect(modal.textContent).not.toContain('no_body_if_bereq_is_get_or_head')
    expect(modal.textContent).not.toContain('hctl')

    // Flip readyState to true and click "Check Again" -> modal closes automatically
    readyState = true
    await user.click(screen.getByTestId('admin-session-scoring-pragma-recheck-btn'))

    await waitFor(() => expect(statusCalls).toBeGreaterThanOrEqual(2))
    await waitFor(() =>
      expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument(),
    )
  })

  it('keeps a resolved prerequisite dialog closed if readiness later becomes missing again', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () =>
        HttpResponse.json({ enabled: false, preflight_pragmas_ready: false }),
      ),
    )
    const user = userEvent.setup()
    const { queryClient } = renderPanel()
    await screen.findByTestId('admin-session-scoring-pragma-warning')
    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))
    expect(await screen.findByTestId('admin-session-scoring-pragma-modal')).toBeInTheDocument()

    act(() => {
      queryClient.setQueryData(['scoring-status', SVC], { enabled: false, preflight_pragmas_ready: true })
    })
    await waitFor(() => expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument())
    act(() => {
      queryClient.setQueryData(['scoring-status', SVC], { enabled: false, preflight_pragmas_ready: false })
    })
    expect(await screen.findByTestId('admin-session-scoring-pragma-warning')).toBeInTheDocument()
    expect(screen.queryByTestId('admin-session-scoring-pragma-modal')).not.toBeInTheDocument()
    await user.click(screen.getByTestId('admin-session-scoring-enable-btn'))
    expect(await screen.findByTestId('admin-session-scoring-pragma-modal')).toBeInTheDocument()
  })

  it('opens the Enable Session Scoring SSEModal when preflight_pragmas_ready is true', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/scoring/status`, () =>
        HttpResponse.json({
          enabled: false,
          preflight_pragmas_ready: true,
        }),
      ),
    )

    const user = userEvent.setup()
    renderPanel()

    const enableBtn = await screen.findByTestId('admin-session-scoring-enable-btn')
    expect(screen.queryByTestId('admin-session-scoring-pragma-warning')).not.toBeInTheDocument()

    await user.click(enableBtn)
    expect(await screen.findByRole('heading', { name: 'Enable Session Scoring' })).toBeInTheDocument()
  })
})
