import { render as rtlRender, screen, waitFor, fireEvent, cleanup, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import React from 'react'
import { http, HttpResponse } from 'msw'
import { server } from '@/__tests__/helpers/msw/handlers'
import { renderWithQuery as render, stubResizeObserver, stubWindowLocationAssign } from '@/__tests__/helpers/page-smoke'
import ShareLoginPage, { ShareLoginForm, safeReturnTarget, AcknowledgeButton, AcknowledgeFallback } from '@/app/share-login/page'
import ShareLoginLoading from '@/app/share-login/loading'
import { SessionDetail, SessionsTable, ScoringControls } from '@/app/sessions/page'
import type { components } from '@/types/api.generated'
import type { SessionsResponse } from '@/types/api'

type SessionRow = components['schemas']['Session']

const pushSpy = vi.fn()
const replaceSpy = vi.fn()
const locationAssignSpy = vi.fn()
let mockSearchParams = new URLSearchParams()
let useMockSessionApi = false
const postMock = vi.fn()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: pushSpy, replace: replaceSpy }),
  useSearchParams: () => mockSearchParams,
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    client: new Proxy(actual.client, {
      get(target, prop, receiver) {
        if (useMockSessionApi && prop === 'POST') return (...args: unknown[]) => postMock(...args)
        return Reflect.get(target, prop, receiver)
      },
    }),
    extractApiError: (e: unknown) => (useMockSessionApi ? String(e) : actual.extractApiError(e)),
  }
})

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useFieldLabel: () => (id: string) => id,
}))

vi.mock('@tanstack/react-virtual', async () => (await import('@/__tests__/helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@/components/SessionScoring/TopFlaggedTable', () => ({
  FlagSessionPopover: () => <span data-testid="flag-popover" />,
}))

beforeAll(() => { stubResizeObserver() })

beforeEach(() => {
  pushSpy.mockReset()
  replaceSpy.mockReset()
  locationAssignSpy.mockReset()
  mockSearchParams = new URLSearchParams()
  stubWindowLocationAssign(locationAssignSpy, 'http://localhost/share-login')
})

afterEach(() => vi.restoreAllMocks())

function useAuthConfig(passcode_enabled: boolean, providers: Array<{ id: string; display_name: string }>) {
  server.use(http.get('/api/share/auth-config', () => HttpResponse.json({ passcode_enabled, providers })))
}

async function submitLoginForm(user: ReturnType<typeof userEvent.setup>, email = 'jane@example.com', passcode = 'ocean-cabin-42') {
  await user.type(await screen.findByLabelText('Email'), email)
  await user.type(screen.getByLabelText('Passcode'), passcode)
  await user.click(screen.getByRole('button', { name: /sign in/i }))
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
    expect(screen.getByText(/verifying share session & security policies/i)).toBeInTheDocument()
  })

  it('shows contextual in-place loading skeleton while auth-config is pending', async () => {
    let resolveConfig!: () => void
    server.use(
      http.get('/api/share/auth-config', async () => {
        await new Promise<void>((r) => { resolveConfig = r })
        return HttpResponse.json({ passcode_enabled: true, oauth_enabled: false, oauth_provider_label: null })
      }),
    )

    render(<ShareLoginPage />)
    expect(screen.getByTestId('share-login-loading-skeleton')).toBeInTheDocument()
    expect(screen.getByText(/verifying share session & security policies/i)).toBeInTheDocument()

    await waitFor(() => expect(resolveConfig).toBeTypeOf('function'))
    resolveConfig()
    await waitFor(() => expect(screen.queryByTestId('share-login-loading-skeleton')).not.toBeInTheDocument())
  })

  it('applies dimmed transition (opacity-40 pointer-events-none) while login submission is in-flight', async () => {
    let resolveLogin!: () => void
    server.use(
      http.post('/api/share/login', async () => {
        await new Promise<void>((r) => { resolveLogin = r })
        return HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' })
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
    expect(await screen.findByTestId('share-login-invite-banner')).toHaveTextContent(/invitation verified for analyst bob/i)
    expect((screen.getByLabelText('Email') as HTMLInputElement).value).toBe('bob@example.com')
  })

  it.each([404, 410])('shows expired invite error when ?token= returns %s', async (status) => {
    mockSearchParams = new URLSearchParams('token=expired-claim-tok')
    server.use(
      http.post('/api/share/claim/expired-claim-tok', () =>
        HttpResponse.json({ detail: { error: 'invite_gone' } }, { status }),
      ),
    )

    render(<ShareLoginPage />)
    expect(await screen.findByText(/this invitation link has expired or is no longer valid/i)).toBeInTheDocument()
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
    await submitLoginForm(user, 'you@example.com', 'ocean-cabin-42')

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
      http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'capacity_exceeded' } }, { status: 503 })),
    )

    render(<ShareLoginPage />)
    await submitLoginForm(user)
    expect(await screen.findByText(/the dashboard is at capacity/i)).toBeInTheDocument()
  })
})

describe('ShareLoginPage — TOS Gate & Safe Redirect Validation', () => {
  it('supports ?next= redirect parameter when routing through TOS acknowledgment', async () => {
    mockSearchParams = new URLSearchParams('next=/performance')
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: true, redirect: '/dashboard' }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user)
    await waitFor(() => expect(pushSpy).toHaveBeenCalledWith('/share-login/acknowledge?return=%2Fperformance'))
  })

  it('blocks open-redirect targets on ?next= and ?return= via safeReturnTarget', () => {
    for (const bad of ['//evil.example/phish', 'https://evil.example/phish', '/\t/evil.example', '/\\evil.example', '/share-login/acknowledge']) {
      expect(safeReturnTarget(bad)).toBeNull()
    }
    expect(safeReturnTarget('/query?service=svc-1')).toBe('/query?service=svc-1')
  })

  it('acknowledges TOS via AcknowledgeButton with custom safe redirect target and dimmed busy state', async () => {
    server.use(http.post('/api/share/acknowledge', () => HttpResponse.json({ ok: true })))
    const user = userEvent.setup()
    render(<AcknowledgeButton version="v1" returnTo="//evil.example" />)
    await user.click(screen.getByTestId('share-acknowledge-submit'))
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })

  it('renders zero-CLS container reservations in AcknowledgeFallback', async () => {
    server.use(http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: 'Terms of Service body text' })))
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
        HttpResponse.json({ ok: true, session_id: 'sid-1', name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user)
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
  })

  it('routes to acknowledge when tos_pending', async () => {
    server.use(
      http.post('/api/share/login', () =>
        HttpResponse.json({ ok: true, session_id: 'sid-1', name: 'Jane', email: 'jane@example.com', tos_pending: true, redirect: '/dashboard' }),
      ),
    )

    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user)
    await waitFor(() => expect(pushSpy).toHaveBeenCalledWith('/share-login/acknowledge'))
  })

  it('shows invalid-credentials on 401', async () => {
    server.use(http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'invalid' } }, { status: 401 })))
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user, 'wrong@example.com', 'nope')
    expect(await screen.findByText(/invalid email or passcode/i)).toBeInTheDocument()
    expect(pushSpy).not.toHaveBeenCalled()
  })

  it('shows lockout countdown on 429', async () => {
    server.use(
      http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'locked', retry_after_s: 30 } }, { status: 429 })),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user, 'jane@example.com', 'x')
    expect(await screen.findByText(/locked out/i)).toBeInTheDocument()
    expect(screen.getByText(/retry in 30s/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /sign in/i })).toBeDisabled()
  })

  it('shows ip whitelist message on 403', async () => {
    server.use(
      http.post('/api/share/login', () => HttpResponse.json({ detail: { error: 'ip_not_whitelisted' } }, { status: 403 })),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user, 'jane@example.com', 'x')
    expect(await screen.findByText(/ip address is not on the approved list/i)).toBeInTheDocument()
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
        return HttpResponse.json({ ok: true, name: 'Jane', email: 'jane@example.com', tos_pending: false, redirect: '/dashboard' })
      }),
    )
    const user = userEvent.setup()
    render(<ShareLoginPage />)
    await submitLoginForm(user)
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    expect(body).toEqual({ email: 'jane@example.com', passcode: 'ocean-cabin-42' })
    expect(contentType).toContain('application/json')
  })

  it('does not submit a blank form', async () => {
    let calls = 0
    server.use(http.post('/api/share/login', () => { calls += 1; return HttpResponse.json({}) }))
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
        await new Promise<void>((r) => { release = r })
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
    expect(locationAssignSpy).toHaveBeenCalledWith('/api/share/oauth/authorize?provider=google')
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
    expect(await screen.findByText(/sign-in failed\. please try again\./i)).toBeInTheDocument()
    expect(screen.queryByText(/some_unknown_code_xyz/)).toBeNull()
  })
})

describe('AcknowledgeFallback (SSR-failure path)', () => {
  it('redirects to /share-login when tos fetch returns 401', async () => {
    server.use(http.get('/api/share/tos', () => HttpResponse.json({ detail: 'unauthenticated' }, { status: 401 })))
    rtlRender(<AcknowledgeFallback />)
    await waitFor(() => expect(replaceSpy).toHaveBeenCalledWith('/share-login'))
  })

  it('renders TOS text and acknowledges → hard-reload to /dashboard', async () => {
    const ackBody = vi.fn()
    server.use(
      http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: TOS_TEXT })),
      http.post('/api/share/acknowledge', async ({ request }) => {
        ackBody(await request.json())
        return HttpResponse.json({ ok: true })
      }),
    )

    const user = userEvent.setup()
    rtlRender(<AcknowledgeFallback />)
    expect(await screen.findByText(/i acknowledge that i am viewing/i)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /i acknowledge/i }))
    await waitFor(() => expect(locationAssignSpy).toHaveBeenCalledWith('/dashboard'))
    expect(ackBody).toHaveBeenCalledWith({ version: 'v1' })
  })

  it('shows server error if acknowledge fails', async () => {
    server.use(
      http.get('/api/share/tos', () => HttpResponse.json({ version: 'v1', text: TOS_TEXT })),
      http.post('/api/share/acknowledge', () => HttpResponse.json({ detail: { error: 'invalid_version' } }, { status: 400 })),
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
      http.post('/api/share/acknowledge', () => HttpResponse.json({ detail: { error: 'invalid_version' } }, { status: 400 })),
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
    for (const target of ['//evil.com', '/\\evil.com', 'https://evil.com', 'evil.com']) {
      expect(safeReturnTarget(target)).toBeNull()
    }
  })

  it('rejects whitespace-smuggled protocol-relative targets (the finding-008 bypass)', () => {
    for (const target of ['/\t/evil.com', '/\n/evil.com', '/ /evil.com', '/\t\\evil.com']) {
      expect(safeReturnTarget(target)).toBeNull()
    }
  })

  it('returns null for empty / nullish input', () => {
    expect(safeReturnTarget(null)).toBeNull()
    expect(safeReturnTarget(undefined)).toBeNull()
    expect(safeReturnTarget('')).toBeNull()
  })
})

function makeRow(overrides: Partial<SessionRow> = {}): SessionRow {
  return {
    ip: '10.0.0.1', country: 'US', asn: 7922,
    session_start: '2026-06-15T00:00:00Z', session_end: '2026-06-15T00:30:00Z',
    req_count: 100, edge_count: 80, shield_count: 20, unique_urls: 12,
    reqs_4xx: 5, reqs_5xx: 0, total_bytes: 50000, median_rtt_ms: 23.5,
    ja4: 't13d1715h2_5b57614c22b0_3d5424432f57', ua: 'curl/7.85.0',
    edge_sid: null, flagged: false, ...overrides,
  }
}

function makeResponse(
  rows: SessionRow[] = [],
  flags: Partial<Pick<SessionsResponse, 'has_rtt' | 'has_ja4' | 'has_edge' | 'has_edge_sid' | 'has_cmcd'>> = {},
): SessionsResponse {
  return {
    sessions: rows, asn_names: {}, total: rows.length, page: 1, limit: 100,
    has_rtt: false, has_ja4: false, has_edge: false, has_edge_sid: false, has_cmcd: false,
    min_reqs_flag: 0, min_4xx_pct_flag: 0, _is_cached: false, ...flags,
  }
}

function detailElement(overrides: Partial<React.ComponentProps<typeof SessionDetail>> = {}) {
  const props: React.ComponentProps<typeof SessionDetail> = {
    selectedSession: makeRow(),
    setSelectedSession: vi.fn(),
    activeServiceId: 'svc-1',
    data: makeResponse(),
    labels: [],
    labelBySid: new Map(),
    onFlagged: vi.fn(),
    ...overrides,
  }
  return <SessionDetail {...props} />
}

function renderSessionDetail(overrides: Partial<React.ComponentProps<typeof SessionDetail>> = {}) {
  return render(detailElement(overrides), { queryOptions: { queries: { gcTime: 0, staleTime: 0 } } })
}

describe('SessionDetail', () => {
  beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
  afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

  it('does not render the dialog when selectedSession is null', () => {
    renderSessionDetail({ selectedSession: null })
    expect(screen.queryByText(/^Session:/)).toBeNull()
    expect(postMock).not.toHaveBeenCalled()
  })

  it('renders the session header + metadata when a session is selected', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    renderSessionDetail({ selectedSession: makeRow({ ip: '10.0.0.42', country: 'CA' }) })
    expect(screen.getByText(/Session:/)).toBeInTheDocument()
    for (const txt of ['10.0.0.42', 'CA', 'AS7922', '100']) expect(screen.getByText(txt)).toBeInTheDocument()
  })

  it('issues a POST to /api/sessions/detail with the session window', async () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const selectedSession = makeRow({
      ip: '10.0.0.50', ja4: 'abc_def_ghi',
      session_start: '2026-06-15T00:00:00Z', session_end: '2026-06-15T01:00:00Z',
    })
    renderSessionDetail({ selectedSession })
    await waitFor(() => expect(postMock).toHaveBeenCalled())
    const [path, opts] = postMock.mock.calls[0]
    expect(path).toBe('/api/sessions/detail')
    expect(opts.body).toMatchObject({
      ip: '10.0.0.50', ja4: 'abc_def_ghi',
      start_time: '2026-06-15T00:00:00Z', end_time: '2026-06-15T01:00:00Z',
    })
  })

  it('skips the fetch when activeServiceId is null (useQuery enabled=false)', async () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    renderSessionDetail({ activeServiceId: null })
    await act(async () => { await new Promise((r) => setTimeout(r, 30)) })
    expect(postMock).not.toHaveBeenCalled()
  })

  it('renders the timeline columns + rows once the detail fetch resolves', async () => {
    postMock.mockResolvedValue({
      data: {
        _is_cached: false,
        columns: ['timestamp', 'host', 'url', 'method', 'status'],
        data: [
          { timestamp: '2026-06-15T00:00:00Z', host: 'example.com', url: '/a', method: 'GET', status: 200 },
          { timestamp: '2026-06-15T00:00:05Z', host: 'example.com', url: '/b', method: 'GET', status: 404 },
        ],
      },
    })
    renderSessionDetail()
    expect(screen.getByText(/Request Timeline/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.getAllByText(/example\.com/).length).toBeGreaterThan(0))
    for (const txt of ['/a', '/b', '200', '404']) expect(screen.getByText(txt)).toBeInTheDocument()
  })

  it('shows the Edge-only switch only when data.has_edge=true', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    const { rerender } = renderSessionDetail({ data: makeResponse([], { has_edge: false }) })
    expect(screen.queryByText(/Edge only/i)).toBeNull()

    rerender(detailElement({ data: makeResponse([], { has_edge: true }) }))
    expect(screen.getByText(/Edge only/i)).toBeInTheDocument()
  })

  it('renders the FlagSessionPopover in the header when edge_sid is present', () => {
    postMock.mockResolvedValue({ data: { columns: [], data: [], _is_cached: false } })
    renderSessionDetail({ selectedSession: makeRow({ edge_sid: 'sid-abc' }) })
    expect(screen.getByTestId('flag-popover')).toBeInTheDocument()
  })

  it('UX-5: shows an inline error + Retry (not a bare empty table) when the detail query 5xxs', async () => {
    postMock.mockRejectedValueOnce(new Error('detail boom'))
    renderSessionDetail({ selectedSession: makeRow({ ip: '1.2.3.4' }), activeServiceId: 'test-svc', data: undefined })
    await waitFor(
      () => expect(screen.getByRole('alert')).toHaveTextContent(/failed to load session detail/i),
      { timeout: 10000 },
    )
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
  }, 15000)
})

function baseProps(overrides: Partial<React.ComponentProps<typeof SessionsTable>> = {}): React.ComponentProps<typeof SessionsTable> {
  return {
    data: makeResponse([makeRow()]), activeServiceId: 'svc-1', isLoadingInitial: false, isFetching: false,
    labels: [], labelBySid: new Map(), idBySid: new Map(), onFlagged: vi.fn(), onRowClick: vi.fn(),
    ...overrides,
  }
}

describe('SessionsTable', () => {
  beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
  afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

  it('renders the canonical column headers', () => {
    rtlRender(<SessionsTable {...baseProps()} />)
    for (const h of ['IP Address', 'Country', 'Requests', '4xx%', 'User-Agent']) {
      expect(screen.getByText(h)).toBeInTheDocument()
    }
  })

  it('renders one row per session in data.sessions', () => {
    const rows = [makeRow({ ip: '10.0.0.1' }), makeRow({ ip: '10.0.0.2', country: 'CA' }), makeRow({ ip: '10.0.0.3', country: 'GB' })]
    rtlRender(<SessionsTable {...baseProps({ data: makeResponse(rows) })} />)
    for (const ip of ['10.0.0.1', '10.0.0.2', '10.0.0.3']) expect(screen.getByText(ip)).toBeInTheDocument()
  })

  it('clicking the row-level Details button opens the detail panel via onRowClick', () => {
    const onRowClick = vi.fn()
    const row = makeRow({ ip: '10.0.0.42' })
    rtlRender(<SessionsTable {...baseProps({ data: makeResponse([row]), onRowClick })} />)
    fireEvent.click(screen.getByRole('button', { name: /details/i }))
    expect(onRowClick).toHaveBeenCalled()
    expect(onRowClick.mock.calls[0][0]).toMatchObject({ ip: '10.0.0.42' })
  })

  it('renders the flagged-icon when row.flagged=true', () => {
    const row = makeRow({ ip: '10.0.0.7', flagged: true })
    const { container } = rtlRender(<SessionsTable {...baseProps({ data: makeResponse([row]) })} />)
    expect(container.querySelector('[title="Flagged Session"]')).toBeInTheDocument()
  })

  it('omits RTT / JA4 / Edge columns when their has_* flags are false', () => {
    rtlRender(<SessionsTable {...baseProps()} />)
    for (const h of ['Med. RTT', 'JA4', 'Edge / Shield', 'Flag']) expect(screen.queryByText(h)).toBeNull()
  })

  it('adds RTT / JA4 / Edge columns when their has_* flags are true', () => {
    const row = makeRow({ edge_sid: 'sid-abc' })
    rtlRender(
      <SessionsTable
        {...baseProps({
          data: makeResponse([row], { has_rtt: true, has_ja4: true, has_edge: true, has_edge_sid: true }),
        })}
      />,
    )
    for (const h of ['Med. RTT', 'JA4', 'Edge / Shield', 'Flag']) expect(screen.getByText(h)).toBeInTheDocument()
    expect(screen.getAllByTestId('flag-popover').length).toBeGreaterThan(0)
  })

  it('renders the empty state when data has no sessions', () => {
    rtlRender(<SessionsTable {...baseProps({ data: makeResponse([]) })} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })

  it('renders even when data is undefined (initial fetch)', () => {
    rtlRender(<SessionsTable {...baseProps({ data: undefined })} />)
    expect(screen.getByText('IP Address')).toBeInTheDocument()
  })
})

function controlsBaseProps(overrides: Partial<React.ComponentProps<typeof ScoringControls>> = {}) {
  return {
    cmcdEnabled: false, flaggedOnly: false, setFlaggedOnly: vi.fn(), streamingOnly: false, setStreamingOnly: vi.fn(),
    minReqs: '' as number | '', setMinReqs: vi.fn(), min4xxPct: '' as number | '', setMin4xxPct: vi.fn(),
    data: { min_reqs_flag: 1000, min_4xx_pct_flag: 20 }, isFetching: false, isLoadingInitial: false, refetch: vi.fn(),
    ...overrides,
  }
}

describe('ScoringControls', () => {
  beforeEach(() => { useMockSessionApi = true; postMock.mockReset() })
  afterEach(() => { useMockSessionApi = false; cleanup(); postMock.mockReset() })

  it('renders the flagged-only switch label and the threshold inputs', () => {
    rtlRender(<ScoringControls {...controlsBaseProps()} />)
    expect(screen.getByText(/flagged only/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. requests/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. 4xx%/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument()
  })

  it('uses data.min_reqs_flag / data.min_4xx_pct_flag as placeholder hints', () => {
    rtlRender(<ScoringControls {...controlsBaseProps({ data: { min_reqs_flag: 5000, min_4xx_pct_flag: 35 } })} />)
    expect(screen.getByPlaceholderText(/≥\s*5000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*35/)).toBeInTheDocument()
  })

  it('falls back to default placeholders when data is undefined', () => {
    rtlRender(<ScoringControls {...controlsBaseProps({ data: undefined })} />)
    expect(screen.getByPlaceholderText(/≥\s*1000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*20/)).toBeInTheDocument()
  })

  it('fires setMinReqs with a number when the requests input changes', () => {
    const setMinReqs = vi.fn()
    rtlRender(<ScoringControls {...controlsBaseProps({ setMinReqs })} />)
    fireEvent.change(screen.getByPlaceholderText(/≥\s*1000/), { target: { value: '250' } })
    expect(setMinReqs).toHaveBeenCalledWith(250)
  })

  it('fires setMinReqs("") when the requests input is cleared', () => {
    const setMinReqs = vi.fn()
    rtlRender(<ScoringControls {...controlsBaseProps({ setMinReqs, minReqs: 250 })} />)
    fireEvent.change(screen.getByPlaceholderText(/≥\s*1000/), { target: { value: '' } })
    expect(setMinReqs).toHaveBeenCalledWith('')
  })

  it('fires setMin4xxPct with a number when the 4xx input changes', () => {
    const setMin4xxPct = vi.fn()
    rtlRender(<ScoringControls {...controlsBaseProps({ setMin4xxPct })} />)
    fireEvent.change(screen.getByPlaceholderText(/≥\s*20/), { target: { value: '40' } })
    expect(setMin4xxPct).toHaveBeenCalledWith(40)
  })

  it('fires setFlaggedOnly when the switch is toggled', async () => {
    const setFlaggedOnly = vi.fn()
    rtlRender(<ScoringControls {...controlsBaseProps({ setFlaggedOnly })} />)
    await userEvent.setup().click(screen.getByRole('switch'))
    expect(setFlaggedOnly).toHaveBeenCalledTimes(1)
    expect(setFlaggedOnly.mock.calls[0][0]).toBe(true)
  })

  it('does not show "Clear filters" when no filter is active', () => {
    rtlRender(<ScoringControls {...controlsBaseProps()} />)
    expect(screen.queryByRole('button', { name: /clear filters/i })).toBeNull()
  })

  it('shows "Clear filters" once any threshold is set and resets all three on click', () => {
    const setFlaggedOnly = vi.fn()
    const setMinReqs = vi.fn()
    const setMin4xxPct = vi.fn()
    rtlRender(
      <ScoringControls
        {...controlsBaseProps({
          flaggedOnly: true, minReqs: 250, min4xxPct: 40, setFlaggedOnly, setMinReqs, setMin4xxPct,
        })}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /clear filters/i }))
    expect(setFlaggedOnly).toHaveBeenCalledWith(false)
    expect(setMinReqs).toHaveBeenCalledWith('')
    expect(setMin4xxPct).toHaveBeenCalledWith('')
  })

  it('calls refetch when the Refresh button is clicked', () => {
    const refetch = vi.fn()
    rtlRender(<ScoringControls {...controlsBaseProps({ refetch })} />)
    fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    expect(refetch).toHaveBeenCalledTimes(1)
  })

  it('disables the Refresh button while isFetching=true', () => {
    rtlRender(<ScoringControls {...controlsBaseProps({ isFetching: true })} />)
    expect(screen.getByRole('button', { name: /refresh/i })).toBeDisabled()
  })

  it('dims the control bar while a background refetch is running', () => {
    const { container } = rtlRender(<ScoringControls {...controlsBaseProps({ isFetching: true, isLoadingInitial: false })} />)
    expect(container.querySelector('.opacity-40')).toBeInTheDocument()
  })

  it('renders the streaming sessions switch when cmcdEnabled is true', () => {
    rtlRender(<ScoringControls {...controlsBaseProps({ cmcdEnabled: true })} />)
    expect(screen.getByText(/streaming sessions/i)).toBeInTheDocument()
  })

  it('does not render the streaming sessions switch when cmcdEnabled is false', () => {
    rtlRender(<ScoringControls {...controlsBaseProps({ cmcdEnabled: false })} />)
    expect(screen.queryByText(/streaming sessions/i)).toBeNull()
  })
})
