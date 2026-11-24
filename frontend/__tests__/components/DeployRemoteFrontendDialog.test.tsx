/**
 * @vitest-environment jsdom
 */
import * as React from 'react'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { DeployRemoteFrontendDialog } from '@/components/DeployRemoteFrontendDialog/DeployRemoteFrontendDialog'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/filterStore'
import { renderWithQuery, stubMatchMedia, stubPointerCaptureAndScroll } from '@/__tests__/helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'

vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams('service=svc-secret-999'),
}))

stubMatchMedia()
stubPointerCaptureAndScroll()

const API_BASE = getApiBase()
const OPAQUE_RE = /^fla-[0-9a-f]{24}$/

afterEach(() => {
  server.resetHandlers()
  vi.restoreAllMocks()
})

describe('DeployRemoteFrontendDialog opaque naming', () => {
  it('initializes with a CSPRNG opaque subdomain prefix uncorrelated with service_id and distinct across opens', async () => {
    const user = userEvent.setup()
    const first = renderWithQuery(<DeployRemoteFrontendDialog />)
    await user.click(screen.getByRole('button', { name: /deploy remote frontend/i }))

    const input1 = await screen.findByLabelText(/subdomain prefix/i) as HTMLInputElement
    const prefix1 = input1.value
    expect(prefix1).toMatch(OPAQUE_RE)
    expect(prefix1.length).toBeLessThanOrEqual(63)
    expect(prefix1).not.toContain('svc-secret-999')
    expect(prefix1).not.toContain('999')
    expect(prefix1.startsWith('fos-')).toBe(false)
    first.unmount()

    const second = renderWithQuery(<DeployRemoteFrontendDialog />)
    await user.click(screen.getByRole('button', { name: /deploy remote frontend/i }))
    const input2 = await screen.findByLabelText(/subdomain prefix/i) as HTMLInputElement
    const prefix2 = input2.value
    expect(prefix2).toMatch(OPAQUE_RE)
    expect(prefix2).not.toBe(prefix1)
    second.unmount()
  })

  it('preserves explicit user-supplied custom/share domain untouched through check and deploy', async () => {
    const prevActive = useServiceStore.getState().activeServiceId
    useServiceStore.setState({ activeServiceId: 'svc-secret-999' })
    const user = userEvent.setup()
    const captured: {
      checkQuery: URLSearchParams | null
      deployBody: Record<string, unknown> | null
    } = { checkQuery: null, deployBody: null }

    server.use(
      http.get(`${API_BASE}/api/provision/check-domain`, ({ request }) => {
        captured.checkQuery = new URL(request.url).searchParams
        return HttpResponse.json({ available: true })
      }),
      http.post(`${API_BASE}/api/sharing/deploy-frontend`, async ({ request }) => {
        captured.deployBody = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({ ok: true, service_id: 'rf-1', domain: 'share.acme.example' })
      }),
    )

    try {
      renderWithQuery(<DeployRemoteFrontendDialog />)
      await user.click(screen.getByRole('button', { name: /deploy remote frontend/i }))

      // Switch to Custom Domain
      await user.click(screen.getByRole('radio', { name: /custom domain/i }))
      const customInput = await screen.findByLabelText(/custom domain name/i) as HTMLInputElement
      expect(customInput.value).toBe('')

      await user.type(customInput, 'share.acme.example')
      expect(customInput.value).toBe('share.acme.example')

      // Re-clicking already-selected Custom Domain must not clobber the user-supplied domain
      await user.click(screen.getByRole('radio', { name: /custom domain/i }))
      expect(customInput.value).toBe('share.acme.example')

      await user.click(screen.getByRole('button', { name: /check domain availability/i }))
      await waitFor(() => expect(screen.getByText(/domain is available/i)).toBeInTheDocument())
      expect(captured.checkQuery?.get('prefix')).toBe('share.acme.example')
      expect(captured.checkQuery?.get('is_custom')).toBe('true')

      await user.click(screen.getByRole('button', { name: /next/i }))
      await user.click(screen.getByRole('button', { name: /next/i }))
      await user.click(screen.getByRole('button', { name: /deploy service/i }))

      await waitFor(() => expect(captured.deployBody).not.toBeNull())
      expect(captured.deployBody?.domain_name).toBe('share.acme.example')
    } finally {
      useServiceStore.setState({ activeServiceId: prevActive })
    }
  })
})
