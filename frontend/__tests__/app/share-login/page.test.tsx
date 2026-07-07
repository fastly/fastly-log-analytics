import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/tests/msw/server'
import ShareLoginPage from '@/app/share-login/page'

const pushSpy = vi.fn()
const locationAssignSpy = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: pushSpy, replace: pushSpy }),
  // ShareLoginForm started reading useSearchParams (commit 8c6374d /
  // d180c4c era). Return a static empty params object — the spec
  // doesn't deep-link via search params.
  useSearchParams: () => new URLSearchParams(),
}))

beforeEach(() => {
  pushSpy.mockReset()
  locationAssignSpy.mockReset()
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

describe('ShareLoginPage', () => {
  it('hard-reloads to /dashboard on successful login when tos already accepted', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({
          ok: true,
          session_id: 'sid-1',
          name: 'Jane',
          email: 'jane@example.com',
          tos_pending: false,
          redirect: '/dashboard',
        }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })

  it('routes to acknowledge when tos_pending', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({
          ok: true,
          session_id: 'sid-1',
          name: 'Jane',
          email: 'jane@example.com',
          tos_pending: true,
          redirect: '/dashboard',
        }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    await waitFor(() =>
      expect(pushSpy).toHaveBeenCalledWith('/share-login/acknowledge'),
    )
  })

  it('shows invalid-credentials on 401', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({ detail: { error: 'invalid' } }, { status: 401 }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'wrong@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'nope')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    expect(await screen.findByText(/invalid email or passcode/i)).toBeInTheDocument()
    expect(pushSpy).not.toHaveBeenCalled()
  })

  it('shows lockout countdown on 429', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json(
          { detail: { error: 'locked', retry_after_s: 30 } },
          { status: 429 },
        ),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'x')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    expect(await screen.findByText(/locked out/i)).toBeInTheDocument()
    expect(screen.getByText(/retry in 30s/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /sign in/i })).toBeDisabled()
  })

  it('shows ip whitelist message on 403', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json(
          { detail: { error: 'ip_not_whitelisted' } },
          { status: 403 },
        ),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'x')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    expect(
      await screen.findByText(/ip address is not on the approved list/i),
    ).toBeInTheDocument()
  })

  it('toggles passcode visibility', async () => {
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    const input = (await screen.findByLabelText('Passcode')) as HTMLInputElement
    expect(input.type).toBe('password')
    await user.click(screen.getByRole('button', { name: /reveal passcode/i }))
    expect(input.type).toBe('text')
  })
})
