import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw/http'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderWithQuery } from '../helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { ShareLoginForm } from '@/app/share-login/page'

vi.mock('@/lib/cloud-engine/mode', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/cloud-engine/mode')>()),
  IS_CLOUD_MODE: true,
}))

vi.mock('next/navigation', async () => ({
  ...(await import('../helpers/page-smoke')).navigationModuleMock('/share-login'),
  useSearchParams: vi.fn(() => new URLSearchParams()),
  useRouter: vi.fn(() => ({
    push: vi.fn(),
    replace: vi.fn(),
    back: vi.fn(),
    forward: vi.fn(),
    refresh: vi.fn(),
    prefetch: vi.fn(),
  })),
}))

describe('ShareLoginForm Cloud Passkey Setup', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('hides Sign in with Passkey after verifying the setup code (step 2)', async () => {
    const user = userEvent.setup()
    server.use(
      http.get('/api/setup', () => HttpResponse.json({ complete: false, accepted: false })),
      http.post('/api/setup/accept', () => HttpResponse.json({ ok: true })),
    )

    renderWithQuery(<ShareLoginForm />)

    expect(await screen.findByPlaceholderText('Initial setup code (first-time admin)')).toBeInTheDocument()
    expect(screen.getByTestId('cloud-passkey-signin-button')).toBeInTheDocument()

    await user.type(screen.getByPlaceholderText('Initial setup code (first-time admin)'), 'setup-secret-123')
    await user.click(screen.getByRole('button', { name: 'Verify Setup Code' }))

    expect(await screen.findByRole('button', { name: 'Register Admin Passkey' })).toBeInTheDocument()
    expect(screen.getByPlaceholderText('Admin display name')).toBeInTheDocument()
    expect(screen.queryByTestId('cloud-passkey-signin-button')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Sign in with Passkey/i })).not.toBeInTheDocument()
  })

  it('hides Sign in with Passkey when /api/setup already reports accepted=true and complete=false', async () => {
    server.use(
      http.get('/api/setup', () => HttpResponse.json({ complete: false, accepted: true })),
    )

    renderWithQuery(<ShareLoginForm />)

    expect(await screen.findByRole('button', { name: 'Register Admin Passkey' })).toBeInTheDocument()
    expect(screen.getByPlaceholderText('Admin display name')).toBeInTheDocument()
    expect(screen.queryByTestId('cloud-passkey-signin-button')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Sign in with Passkey/i })).not.toBeInTheDocument()
  })

  it('shows Sign in with Passkey when setup is already complete', async () => {
    server.use(
      http.get('/api/setup', () => HttpResponse.json({ complete: true, accepted: true })),
    )

    renderWithQuery(<ShareLoginForm />)

    await waitFor(() => {
      expect(screen.queryByRole('button', { name: 'Verify Setup Code' })).not.toBeInTheDocument()
    })
    expect(screen.getByTestId('cloud-passkey-signin-button')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Register Admin Passkey' })).not.toBeInTheDocument()
  })
})
