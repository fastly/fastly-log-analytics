import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/tests/msw/server'
import AcknowledgePage from '@/app/share-login/acknowledge/page'

const replaceSpy = vi.fn()
const locationAssignSpy = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ replace: replaceSpy, push: replaceSpy }),
}))

beforeEach(() => {
  replaceSpy.mockReset()
  locationAssignSpy.mockReset()
  // jsdom's window.location is mostly non-configurable. Replace the
  // entire object on `window` so the acknowledge page's
  // window.location.assign('/dashboard') is observable. We keep the
  // origin/pathname/host fields populated so jsdom-driven fetches and
  // MSW handlers still resolve URLs correctly.
  Object.defineProperty(window, 'location', {
    configurable: true,
    writable: true,
    value: {
      ...window.location,
      assign: locationAssignSpy,
      origin: 'http://localhost',
      href: 'http://localhost/',
    },
  })
})

describe('AcknowledgePage', () => {
  it('redirects to /share-login when heartbeat returns 401', async () => {
    server.use(
      http.get('/api/share/heartbeat', () =>
        HttpResponse.json({ detail: 'unauthenticated' }, { status: 401 }),
      ),
    )

    render(<AcknowledgePage />)
    await waitFor(() => expect(replaceSpy).toHaveBeenCalledWith('/share-login'))
  })

  it('renders TOS text and acknowledges → hard-reload to /dashboard', async () => {
    server.use(
      http.get('/api/share/heartbeat', () =>
        HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com' }),
      ),
      http.post('/api/share/acknowledge', () =>
        HttpResponse.json({ ok: true }),
      ),
    )

    const user = userEvent.setup()
    render(<AcknowledgePage />)

    expect(
      await screen.findByText(/i acknowledge that i am viewing/i),
    ).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })

  it('shows server error if acknowledge fails', async () => {
    server.use(
      http.get('/api/share/heartbeat', () =>
        HttpResponse.json({ ok: true }),
      ),
      http.post('/api/share/acknowledge', () =>
        HttpResponse.json(
          { detail: { error: 'invalid_version' } },
          { status: 400 },
        ),
      ),
    )

    const user = userEvent.setup()
    render(<AcknowledgePage />)

    await screen.findByText(/i acknowledge that i am viewing/i)
    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
    expect(replaceSpy).not.toHaveBeenCalledWith('/dashboard')
  })
})
