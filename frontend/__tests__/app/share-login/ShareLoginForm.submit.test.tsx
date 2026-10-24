/**
 * ShareLoginForm submit contract that must survive the react-hook-form
 * migration: the exact JSON body sent to /api/share/login, that a blank
 * form never submits, that the email field is auto-focused, and that a
 * second click while a login is in flight does not double-submit.
 */
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/__tests__/helpers/msw/server'
import { renderWithQuery as render } from '@/__tests__/helpers/renderWithQuery'
import ShareLoginPage from '@/app/share-login/page'

const locationAssignSpy = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
}))

beforeEach(() => {
  locationAssignSpy.mockReset()
  Object.defineProperty(window, 'location', {
    configurable: true,
    writable: true,
    value: { ...window.location, assign: locationAssignSpy, origin: 'http://localhost', href: 'http://localhost/' },
  })
})

describe('ShareLoginForm submit', () => {
  it('posts exactly {email, passcode} as JSON', async () => {
    let body: unknown = null
    let contentType: string | null = null
    server.use(
      http.post('/api/share/login', async ({ request }) => {
        body = await request.json()
        contentType = request.headers.get('content-type')
        return HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' })
      }),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')
    await user.click(screen.getByRole('button', { name: /sign in/i }))
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    expect(body).toEqual({ email: 'jane@example.com', passcode: 'ocean-cabin-42' })
    expect(contentType).toContain('application/json')
  })

  it('does not submit a blank form', async () => {
    let calls = 0
    server.use(
      http.post('/api/share/login', () => {
        calls += 1
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.click(await screen.findByRole('button', { name: /sign in/i }))
    await new Promise((r) => setTimeout(r, 50))
    expect(calls).toBe(0)
  })

  it('focuses the email field once the passcode form shows', async () => {
    render(<ShareLoginPage />)
    const email = await screen.findByLabelText('Email')
    await waitFor(() => expect(document.activeElement).toBe(email))
  })

  it('ignores a second submit while a login is in flight', async () => {
    let calls = 0
    let release!: () => void
    server.use(
      http.post('/api/share/login', async () => {
        calls += 1
        await new Promise<void>((r) => {
          release = r
        })
        return HttpResponse.json({ ok: true, name: 'J', email: 'j@example.com', tos_pending: false, redirect: '/dashboard' })
      }),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'j@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'pw-1')
    const submit = screen.getByRole('button', { name: /sign in/i })
    await user.click(submit)
    await waitFor(() => expect(calls).toBe(1))
    await user.click(submit)
    release()
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalled())
    expect(calls).toBe(1)
  })
})
