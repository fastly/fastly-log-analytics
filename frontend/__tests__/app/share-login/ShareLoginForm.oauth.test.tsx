import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/tests/msw/server'
import { ShareLoginForm } from '@/app/share-login/ShareLoginForm'

// Mutable search params so each test can set ?oauth_error / ?return.
let mockSearch = new URLSearchParams()
const assignSpy = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  useSearchParams: () => mockSearch,
}))

beforeEach(() => {
  mockSearch = new URLSearchParams()
  assignSpy.mockReset()
  Object.defineProperty(window, 'location', {
    configurable: true,
    writable: true,
    value: { ...window.location, assign: assignSpy, origin: 'http://localhost', href: 'http://localhost/' },
  })
})

afterEach(() => vi.restoreAllMocks())

function useAuthConfig(passcode_enabled: boolean, providers: Array<{ id: string; display_name: string }>) {
  server.use(http.get('/api/share/auth-config', () => HttpResponse.json({ passcode_enabled, providers })))
}

describe('ShareLoginForm OAuth', () => {
  it('renders SSO buttons + passcode form when both are enabled', async () => {
    useAuthConfig(true, [{ id: 'google', display_name: 'Google Workspace' }])
    render(<ShareLoginForm />)
    expect(await screen.findByRole('button', { name: /sign in with google workspace/i })).toBeInTheDocument()
    expect(await screen.findByLabelText('Email')).toBeInTheDocument()
    expect(screen.getByText('OR')).toBeInTheDocument()
  })

  it('clicking an SSO button navigates to the authorize endpoint', async () => {
    useAuthConfig(false, [{ id: 'google', display_name: 'Google Workspace' }])
    const user = userEvent.setup()
    render(<ShareLoginForm />)
    await user.click(await screen.findByRole('button', { name: /sign in with google workspace/i }))
    expect(assignSpy).toHaveBeenCalledWith('/api/share/oauth/authorize?provider=google')
  })

  it('SSO-only config hides the passcode form', async () => {
    useAuthConfig(false, [{ id: 'google', display_name: 'Google Workspace' }])
    render(<ShareLoginForm />)
    await screen.findByRole('button', { name: /sign in with google workspace/i })
    expect(screen.queryByLabelText('Email')).toBeNull()
    expect(screen.queryByText('OR')).toBeNull()
  })

  it('shows the misconfiguration lockout when nothing is enabled', async () => {
    useAuthConfig(false, [])
    render(<ShareLoginForm />)
    expect(await screen.findByText(/sign-in is not configured/i)).toBeInTheDocument()
    expect(screen.queryByLabelText('Email')).toBeNull()
  })

  it('fails open to the passcode form when auth-config fetch fails', async () => {
    server.use(http.get('/api/share/auth-config', () => HttpResponse.json({ error: 'boom' }, { status: 500 })))
    render(<ShareLoginForm />)
    expect(await screen.findByLabelText('Email')).toBeInTheDocument()
  })

  it('maps a known oauth_error code to friendly copy and never reflects the raw param', async () => {
    mockSearch = new URLSearchParams('oauth_error=not_invited')
    useAuthConfig(true, [])
    render(<ShareLoginForm />)
    expect(await screen.findByText(/isn't invited to a dashboard/i)).toBeInTheDocument()
    expect(screen.queryByText(/not_invited/)).toBeNull()
  })

  it('maps an unknown oauth_error code to the generic fallback', async () => {
    mockSearch = new URLSearchParams('oauth_error=some_unknown_code_xyz')
    useAuthConfig(true, [])
    render(<ShareLoginForm />)
    expect(await screen.findByText(/sign-in failed\. please try again\./i)).toBeInTheDocument()
    expect(screen.queryByText(/some_unknown_code_xyz/)).toBeNull()
  })
})
