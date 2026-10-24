import { render as rtlRender, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { server } from '@/__tests__/helpers/msw/server'
import { renderWithQuery as render } from '@/__tests__/helpers/query'
import { stubWindowLocationAssign } from '@/__tests__/helpers/browser-stubs'
import ShareLoginPage from '@/app/share-login/page'
import ShareLoginLoading from '@/app/share-login/loading'
import { ShareLoginForm, safeReturnTarget } from '@/app/share-login/ShareLoginForm'
import { AcknowledgeButton, AcknowledgeFallback } from '@/app/share-login/acknowledge/AcknowledgeButton'

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
  stubWindowLocationAssign(locationAssignSpy, 'http://localhost/share-login')
})

afterEach(() => vi.restoreAllMocks())

function useAuthConfig(
  passcode_enabled: boolean,
  providers: Array<{ id: string; display_name: string }>,
) {
  server.use(
    http.get('/api/share/auth-config', () =>
      HttpResponse.json({ passcode_enabled, providers }),
    ),
  )
}

const TOS_TEXT =
  'I acknowledge that I am viewing third-party operational log data, that my access is logged, and that I will not retain, redistribute, or use this data outside the scope of my engagement.'

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

  it.each([404, 410])('shows expired invite error when ?token= returns %s', async (status) => {
    mockSearchParams = new URLSearchParams('token=expired-claim-tok')
    server.use(
      http.post('/api/share/claim/expired-claim-tok', () =>
        HttpResponse.json({ detail: { error: 'invite_gone' } }, { status }),
      ),
    )

    render(<ShareLoginPage />)
    expect(
      await screen.findByText(/this invitation link has expired or is no longer valid/i),
    ).toBeInTheDocument()
  })

  it('clears an expired invitation banner on submission and replaces it with the login error', async () => {
    mockSearchParams = new URLSearchParams('token=expired-claim-tok')
    let resolveLogin!: () => void
    server.use(
      http.post('/api/share/claim/expired-claim-tok', () =>
        HttpResponse.json({ detail: { error: 'invite_gone' } }, { status: 410 }),
      ),
      http.post('/api/share/login', async () => {
        await new Promise<void>((resolve) => { resolveLogin = resolve })
        return HttpResponse.json({ detail: { error: 'invalid_credentials' } }, { status: 401 })
      }),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    const inviteError = await screen.findByText(/this invitation link has expired/i)
    await user.type(await screen.findByLabelText('Email'), 'you@example.com')
    await user.type(screen.getByLabelText('Passcode'), 'ocean-cabin-42')
    await user.click(screen.getByRole('button', { name: /sign in/i }))

    await waitFor(() => expect(resolveLogin).toBeTypeOf('function'))
    expect(inviteError).not.toBeInTheDocument()
    resolveLogin()
    const loginError = await screen.findByText('Invalid email or passcode.')
    expect(loginError.closest('[role="alert"]')).toHaveFocus()
    expect(screen.queryByText(/this invitation link has expired/i)).not.toBeInTheDocument()
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

describe('ShareLoginForm submit', () => {
  it('posts exactly {email, passcode} as JSON', async () => {
    let body: unknown = null
    let contentType: string | null = null
    server.use(
      http.post('/api/share/login', async ({ request }) => {
        body = await request.json()
        contentType = request.headers.get('content-type')
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
        return HttpResponse.json({
          ok: true,
          name: 'J',
          email: 'j@example.com',
          tos_pending: false,
          redirect: '/dashboard',
        })
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

describe('ShareLoginForm OAuth', () => {
  it('renders SSO buttons + passcode form when both are enabled', async () => {
    useAuthConfig(true, [{ id: 'google', display_name: 'Google Workspace' }])
    render(<ShareLoginForm />)
    expect(
      await screen.findByRole('button', { name: /sign in with google workspace/i }),
    ).toBeInTheDocument()
    expect(await screen.findByLabelText('Email')).toBeInTheDocument()
    expect(screen.getByText('OR')).toBeInTheDocument()
  })

  it('clicking an SSO button navigates to the authorize endpoint', async () => {
    useAuthConfig(false, [{ id: 'google', display_name: 'Google Workspace' }])
    const user = userEvent.setup()
    render(<ShareLoginForm />)
    await user.click(
      await screen.findByRole('button', { name: /sign in with google workspace/i }),
    )
    expect(locationAssignSpy).toHaveBeenCalledWith(
      '/api/share/oauth/authorize?provider=google',
    )
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
    server.use(
      http.get('/api/share/auth-config', () =>
        HttpResponse.json({ error: 'boom' }, { status: 500 }),
      ),
    )
    render(<ShareLoginForm />)
    expect(await screen.findByLabelText('Email')).toBeInTheDocument()
  })

  it('maps a known oauth_error code to friendly copy and never reflects the raw param', async () => {
    mockSearchParams = new URLSearchParams('oauth_error=not_invited')
    useAuthConfig(true, [])
    render(<ShareLoginForm />)
    expect(await screen.findByText(/isn't invited to a dashboard/i)).toBeInTheDocument()
    expect(screen.queryByText(/not_invited/)).toBeNull()
  })

  it('maps an unknown oauth_error code to the generic fallback', async () => {
    mockSearchParams = new URLSearchParams('oauth_error=some_unknown_code_xyz')
    useAuthConfig(true, [])
    render(<ShareLoginForm />)
    expect(
      await screen.findByText(/sign-in failed\. please try again\./i),
    ).toBeInTheDocument()
    expect(screen.queryByText(/some_unknown_code_xyz/)).toBeNull()
  })
})

describe('AcknowledgeFallback (SSR-failure path)', () => {
  it('redirects to /share-login when tos fetch returns 401', async () => {
    server.use(
      http.get('/api/share/tos', () =>
        HttpResponse.json({ detail: 'unauthenticated' }, { status: 401 }),
      ),
    )

    rtlRender(<AcknowledgeFallback />)
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
    rtlRender(<AcknowledgeFallback />)

    expect(
      await screen.findByText(/i acknowledge that i am viewing/i),
    ).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
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
    rtlRender(<AcknowledgeFallback />)

    await screen.findByText(/i acknowledge that i am viewing/i)
    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
    expect(replaceSpy).not.toHaveBeenCalledWith('/dashboard')
  })
})

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
    rtlRender(<AcknowledgeButton version="v2" />)

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
    rtlRender(<AcknowledgeButton version="v2" />)

    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))

    expect(await screen.findByText(/invalid_version/i)).toBeInTheDocument()
    expect(locationAssignSpy).not.toHaveBeenCalled()
  })
})

describe('safeReturnTarget (post-login open-redirect guard, finding 008)', () => {
  it('accepts in-app absolute paths', () => {
    expect(safeReturnTarget('/dashboard')).toBe('/dashboard')
    expect(safeReturnTarget('/sessions?x=1')).toBe('/sessions?x=1')
  })

  it('rejects protocol-relative, backslash, and absolute external targets', () => {
    expect(safeReturnTarget('//evil.com')).toBeNull()
    expect(safeReturnTarget('/\\evil.com')).toBeNull()
    expect(safeReturnTarget('https://evil.com')).toBeNull()
    expect(safeReturnTarget('evil.com')).toBeNull()
  })

  it('rejects whitespace-smuggled protocol-relative targets (the finding-008 bypass)', () => {
    expect(safeReturnTarget('/\t/evil.com')).toBeNull()
    expect(safeReturnTarget('/\n/evil.com')).toBeNull()
    expect(safeReturnTarget('/ /evil.com')).toBeNull()
    expect(safeReturnTarget('/\t\\evil.com')).toBeNull()
  })

  it('returns null for empty / nullish input', () => {
    expect(safeReturnTarget(null)).toBeNull()
    expect(safeReturnTarget(undefined)).toBeNull()
    expect(safeReturnTarget('')).toBeNull()
  })
})
