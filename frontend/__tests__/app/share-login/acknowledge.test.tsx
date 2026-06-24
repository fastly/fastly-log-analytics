import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/tests/msw/server'
import { AcknowledgeButton } from '@/app/share-login/acknowledge/AcknowledgeButton'
import { AcknowledgeFallback } from '@/app/share-login/acknowledge/AcknowledgeFallback'

const replaceSpy = vi.fn()
const locationAssignSpy = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ replace: replaceSpy, push: replaceSpy }),
}))

beforeEach(() => {
  replaceSpy.mockReset()
  locationAssignSpy.mockReset()
  // jsdom's window.location is mostly non-configurable. Replace the
  // entire object on `window` so the AcknowledgeButton's
  // window.location.assign('/dashboard') is observable.
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

const TOS_TEXT =
  'I acknowledge that I am viewing third-party operational log data, that my access is logged, and that I will not retain, redistribute, or use this data outside the scope of my engagement.'

// AcknowledgeFallback runs when the SSR-side TOS fetch failed (backend
// hiccup) — it does the original useEffect-based TOS fetch + auth-check
// + render path. The page-level happy path is now SSR'd into static HTML
// and exercised through AcknowledgeButton instead.
describe('AcknowledgeFallback (SSR-failure path)', () => {
  it('redirects to /share-login when tos fetch returns 401', async () => {
    server.use(
      http.get('/api/share/tos', () =>
        HttpResponse.json({ detail: 'unauthenticated' }, { status: 401 }),
      ),
    )

    render(<AcknowledgeFallback />)
    await waitFor(() => expect(replaceSpy).toHaveBeenCalledWith('/share-login'))
  })

  it('renders TOS text and acknowledges → hard-reload to /dashboard', async () => {
    const ackBody = vi.fn()
    server.use(
      http.get('/api/share/tos', () =>
        HttpResponse.json({ version: 'v1', text: TOS_TEXT }),
      ),
      http.post('/api/share/acknowledge', async ({ request }) => {
        ackBody(await request.json())
        return HttpResponse.json({ ok: true })
      }),
    )

    const user = userEvent.setup()
    render(<AcknowledgeFallback />)

    expect(
      await screen.findByText(/i acknowledge that i am viewing/i),
    ).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    // The version POSTed must be the one /tos returned — not a sentinel.
    expect(ackBody).toHaveBeenCalledWith({ version: 'v1' })
  })

  it('shows server error if acknowledge fails', async () => {
    server.use(
      http.get('/api/share/tos', () =>
        HttpResponse.json({ version: 'v1', text: TOS_TEXT }),
      ),
      http.post('/api/share/acknowledge', () =>
        HttpResponse.json(
          { detail: { error: 'invalid_version' } },
          { status: 400 },
        ),
      ),
    )

    const user = userEvent.setup()
    render(<AcknowledgeFallback />)

    await screen.findByText(/i acknowledge that i am viewing/i)
    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
    expect(replaceSpy).not.toHaveBeenCalledWith('/dashboard')
  })
})

// AcknowledgeButton is the SSR-happy-path island: page RSC fetched the
// TOS and passes its version straight in. No mount-time TOS fetch.
describe('AcknowledgeButton (SSR-happy-path island)', () => {
  it('acknowledges with the supplied version → hard-reload to /dashboard', async () => {
    const ackBody = vi.fn()
    server.use(
      http.post('/api/share/acknowledge', async ({ request }) => {
        ackBody(await request.json())
        return HttpResponse.json({ ok: true })
      }),
    )

    const user = userEvent.setup()
    render(<AcknowledgeButton version="v2" />)

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    expect(ackBody).toHaveBeenCalledWith({ version: 'v2' })
  })

  it('shows server error if acknowledge fails', async () => {
    server.use(
      http.post('/api/share/acknowledge', () =>
        HttpResponse.json(
          { detail: { error: 'invalid_version' } },
          { status: 400 },
        ),
      ),
    )

    const user = userEvent.setup()
    render(<AcknowledgeButton version="v2" />)

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
    expect(locationAssignSpy).not.toHaveBeenCalled()
  })
})
