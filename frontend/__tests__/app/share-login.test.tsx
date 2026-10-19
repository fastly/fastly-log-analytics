import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/tests/msw/server'
import ShareLoginPage from '@/app/share-login/page'
import ShareLoginLoading from '@/app/share-login/loading'
import { safeReturnTarget } from '@/app/share-login/ShareLoginForm'
import { AcknowledgeButton } from '@/app/share-login/acknowledge/AcknowledgeButton'
import { AcknowledgeFallback } from '@/app/share-login/acknowledge/AcknowledgeFallback'

const pushSpy = vi.fn()
const replaceSpy = vi.fn()
const locationAssignSpy = vi.fn()
let mockSearchParams = new URLSearchParams()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: pushSpy, replace: replaceSpy }),
  useSearchParams: () => mockSearchParams,
}))

beforeEach(() => {
  pushSpy.mockReset()
  replaceSpy.mockReset()
  locationAssignSpy.mockReset()
  mockSearchParams = new URLSearchParams()
  Object.defineProperty(window, 'location', {
    configurable: true,
    writable: true,
    value: {
      ...window.location,
      assign: locationAssignSpy,
      origin: 'http://localhost',
      href: 'http://localhost/share-login',
    },
  })
})

describe('ShareLoginPage — Zero-CLS Layout Reservations & Loading Skeletons', () => {
  it('renders pre-allocated zero-CLS card and content reservations on ShareLoginPage', async () => {
    render(<ShareLoginPage />)
    const card = await screen.findByTestId('share-login-card')
    const content = screen.getByTestId('share-login-card-content')
    const formContainer = await screen.findByTestId('share-login-form-container')

    expect(card.className).toContain('[contain-intrinsic-size:auto_380px]')
    expect(card.className).toContain('[content-visibility:auto]')
    expect(content.className).toContain('[contain-intrinsic-size:auto_240px]')
    expect(content.className).toContain('[content-visibility:auto]')
    expect(formContainer.className).toContain('[contain-intrinsic-size:auto_240px]')
    expect(formContainer.className).toContain('[content-visibility:auto]')
  })

  it('renders ShareLoginLoading with zero-CLS reservations and contextual status copy', () => {
    render(<ShareLoginLoading />)
    expect(
      screen.getByText(/verifying share session & security policies/i),
    ).toBeInTheDocument()
  })

  it('shows contextual in-place loading skeleton while auth-config is pending', async () => {
    let resolveConfig!: () => void
    server.use(
      http.get('/api/share/auth-config', async () => {
        await new Promise<void>((r) => {
          resolveConfig = r
        })
        return HttpResponse.json({
          passcode_enabled: true,
          oauth_enabled: false,
          oauth_provider_label: null,
        })
      }),
    )

    render(<ShareLoginPage />)
    expect(screen.getByTestId('share-login-loading-skeleton')).toBeInTheDocument()
    expect(
      screen.getByText(/verifying share session & security policies/i),
    ).toBeInTheDocument()

    await waitFor(() => expect(resolveConfig).toBeTypeOf('function'))
    resolveConfig()
    await waitFor(() =>
      expect(screen.queryByTestId('share-login-loading-skeleton')).not.toBeInTheDocument(),
    )
  })

  it('applies dimmed transition (opacity-40 pointer-events-none) while login submission is in-flight', async () => {
    let resolveLogin!: () => void
    server.use(
      http.post('/api/share/login', async () => {
        await new Promise<void>((r) => {
          resolveLogin = r
        })
        return HttpResponse.json({
          ok: true,
          name: 'Jane',
          email: 'jane@example.com',
          tos_pending: false,
          redirect: '/dashboard',
        })
      }),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')

    const fieldsContainer = screen.getByTestId('share-login-fields')
    expect(fieldsContainer.className).not.toContain('opacity-40')

    await user.click(screen.getByRole('button', { name: /sign in/i }))

    await waitFor(() => {
      expect(fieldsContainer.className).toContain('opacity-40')
      expect(fieldsContainer.className).toContain('pointer-events-none')
    })

    resolveLogin()
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })
})

describe('ShareLoginPage — Invite Token Claim & Credential Validation', () => {
  it('claims a valid ?token= query parameter and pre-fills the analyst email', async () => {
    mockSearchParams = new URLSearchParams('token=valid-claim-tok')
    server.use(
      http.post('/api/share/claim/valid-claim-tok', () =>
        HttpResponse.json({
          name: 'Analyst Bob',
          email: 'bob@example.com',
          passcode: 'secret-words-99',
          share_url: 'https://share.example.com',
          expires_at: null,
        }),
      ),
    )

    render(<ShareLoginPage />)
    expect(await screen.findByTestId('share-login-invite-banner')).toHaveTextContent(
      /invitation verified for analyst bob/i,
    )
    const emailInput = screen.getByLabelText('Email') as HTMLInputElement
    expect(emailInput.value).toBe('bob@example.com')
  })

  it('shows expired invite error when ?token= returns 404 or 410', async () => {
    mockSearchParams = new URLSearchParams('token=expired-claim-tok')
    server.use(
      http.post('/api/share/claim/expired-claim-tok', () =>
        HttpResponse.json({ detail: { error: 'invite_gone' } }, { status: 410 }),
      ),
    )

    render(<ShareLoginPage />)
    expect(
      await screen.findByText(/this invitation link has expired or is no longer valid/i),
    ).toBeInTheDocument()
  })

  it('displays error feedback for 503 capacity cap', async () => {
    const user = userEvent.setup()

    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({ detail: { error: 'capacity_exceeded' } }, { status: 503 }),
      ),
    )

    render(<ShareLoginPage />)
    await user.type(await screen.findByLabelText('Email'), 'jane@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    expect(
      await screen.findByText(/the dashboard is at capacity/i),
    ).toBeInTheDocument()
  })
})

describe('ShareLoginPage — TOS Gate & Safe Redirect Validation', () => {
  it('supports ?next= redirect parameter when routing through TOS acknowledgment', async () => {
    mockSearchParams = new URLSearchParams('next=/performance')
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({
          ok: true,
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
      expect(pushSpy).toHaveBeenCalledWith(
        '/share-login/acknowledge?return=%2Fperformance',
      ),
    )
  })

  it('blocks open-redirect targets on ?next= and ?return= via safeReturnTarget', () => {
    expect(safeReturnTarget('//evil.example/phish')).toBeNull()
    expect(safeReturnTarget('https://evil.example/phish')).toBeNull()
    expect(safeReturnTarget('/\t/evil.example')).toBeNull()
    expect(safeReturnTarget('/\\evil.example')).toBeNull()
    expect(safeReturnTarget('/share-login/acknowledge')).toBeNull()
    expect(safeReturnTarget('/query?service=svc-1')).toBe('/query?service=svc-1')
  })

  it('acknowledges TOS via AcknowledgeButton with custom safe redirect target and dimmed busy state', async () => {
    server.use(
      http.post('/api/share/acknowledge', () => HttpResponse.json({ ok: true })),
    )

    const user = userEvent.setup()
    render(<AcknowledgeButton version="v1" returnTo="//evil.example" />)
    await user.click(screen.getByTestId('share-acknowledge-submit'))

    // Malicious returnTo is sanitized to /dashboard
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })

  it('renders zero-CLS container reservations in AcknowledgeFallback', async () => {
    server.use(
      http.get('/api/share/tos', () =>
        HttpResponse.json({ version: 'v1', text: 'Terms of Service body text' }),
      ),
    )

    render(<AcknowledgeFallback />)
    const container = await screen.findByTestId('share-acknowledge-fallback')
    expect(container.className).toContain('[contain-intrinsic-size:auto_140px]')
    expect(container.className).toContain('[content-visibility:auto]')
  })
})
