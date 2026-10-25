import { act, fireEvent, render, renderHook, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import React from 'react'

import { server } from '@/__tests__/helpers/msw/handlers'
import { createTestQueryClient, makeQueryWrapper } from '@/__tests__/helpers/page-smoke'
import { useServiceStore } from '@/stores/serviceStore'
import { InviteAnalystDialog } from '@/components/InviteAnalystDialog'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { CreateInviteDialog } from '@/components/share-dashboard/CreateInviteDialog'
import { useTableSort, type ShareStatus, type SortAccessors } from '@/components/share-dashboard/SharingControlPanel'
import type { ServiceConfig } from '@/types/api'
import { stubMatchMedia } from '@/__tests__/helpers/page-smoke'

// Radix primitives (Popover/Dialog/Select/Checkbox) need these in jsdom.
stubMatchMedia()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

const status = {
  services: [{ service_id: 'svcA', name: 'Service A' }],
  invites: [
    {
      id: 'i1',
      name: 'Alice',
      email: 'alice@corp.com',
      service_ids: ['svcA'],
      expires_at: null,
      revoked: 0,
      auth_method: 'passcode',
      last_login_at: '2026-06-05T09:30:00Z',
    },
    {
      id: 'i2',
      name: 'Bob',
      email: 'bob@corp.com',
      service_ids: [],
      expires_at: null,
      revoked: 0,
      auth_method: 'passcode',
      last_login_at: null,
    },
  ],
  sessions: [
    {
      session_id: 's1',
      invite_id: 'i1',
      email: 'alice@corp.com',
      ip_address: '1.2.3.4',
      last_active_time: '2026-06-05T10:00:00Z',
    },
  ],
} as ShareStatus

// Data rows only (drop the header row).
const bodyRows = () => screen.getAllByRole('row').slice(1)

test('renders last-login (with Never fallback), online dot, and service name + id', () => {
  render(<InvitationsPanel status={status} onRefresh={vi.fn()} onError={vi.fn()} />)

  expect(screen.getByText('Never')).toBeInTheDocument() // Bob never logged in
  // One online indicator — Alice has a live session matching her invite_id.
  expect(screen.getAllByLabelText('Online now')).toHaveLength(1)
  // Services column shows the friendly name AND the raw id.
  expect(screen.getByTitle('Service A (svcA)')).toBeInTheDocument()
  expect(screen.getByText('(svcA)')).toBeInTheDocument()
})

test('defaults to last-login desc: the recently-active invite sorts above "Never"', () => {
  render(<InvitationsPanel status={status} onRefresh={vi.fn()} onError={vi.fn()} />)
  expect(bodyRows()[0]).toHaveTextContent('Alice')
  expect(bodyRows()[1]).toHaveTextContent('Bob')
})

test('clicking a column header re-sorts the rows', async () => {
  const user = userEvent.setup()
  render(<InvitationsPanel status={status} onRefresh={vi.fn()} onError={vi.fn()} />)

  // Click "Name" -> new key resets to desc -> Bob before Alice.
  await user.click(screen.getByRole('button', { name: 'Name' }))
  expect(bodyRows()[0]).toHaveTextContent('Bob')
  expect(bodyRows()[1]).toHaveTextContent('Alice')

  // Click again -> asc -> Alice before Bob.
  await user.click(screen.getByRole('button', { name: 'Name' }))
  expect(bodyRows()[0]).toHaveTextContent('Alice')
})

const stubClipboard = (writeText: () => Promise<void>) =>
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
const hasCheck = (btn: HTMLElement) => btn.querySelector('svg.lucide-check') !== null

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

test('share-card copy flips the icon for the copied row only, then resets', async () => {
  vi.useFakeTimers()
  const writeText = vi.fn().mockResolvedValue(undefined)
  stubClipboard(writeText)
  render(<InvitationsPanel status={status} onRefresh={vi.fn()} onError={vi.fn()} />)
  const [alice, bob] = screen.getAllByTitle('Copy share card')

  await act(async () => {
    fireEvent.click(alice)
  })
  expect(writeText).toHaveBeenCalledTimes(1)
  expect(hasCheck(alice)).toBe(true)
  expect(hasCheck(bob)).toBe(false)

  await act(async () => {
    vi.advanceTimersByTime(1500)
  })
  expect(hasCheck(alice)).toBe(false)
})

test('share-card copy does not show success when the clipboard write is rejected', async () => {
  const writeText = vi.fn().mockRejectedValue(new Error('denied'))
  stubClipboard(writeText)
  render(<InvitationsPanel status={status} onRefresh={vi.fn()} onError={vi.fn()} />)
  const [alice] = screen.getAllByTitle('Copy share card')
  await act(async () => {
    fireEvent.click(alice)
  })
  expect(hasCheck(alice)).toBe(false)
})

// --- CreateInviteDialog --------------------------------------------------

const services: ShareStatus['services'] = [{ service_id: 'svcA', name: 'Service A' }]

interface InviteBody {
  allow_concurrent_sessions?: boolean
}

function captureInviteBody(): { current: InviteBody | undefined } {
  const captured: { current: InviteBody | undefined } = { current: undefined }
  server.use(
    http.post('*/api/admin/share/invites', async ({ request }) => {
      captured.current = (await request.json()) as InviteBody
      return HttpResponse.json({ id: 'i1' })
    }),
  )
  return captured
}

async function fillRequiredFields(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Name'), 'Jane')
  await user.type(screen.getByLabelText('Email'), 'jane@example.com')
  await user.type(screen.getByLabelText('Passcode'), 'ocean-breeze-cabin-42')
  await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
}

describe('CreateInviteDialog', () => {
  beforeEach(() => {
    // The api-client middleware aborts requests when no service is active.
    useServiceStore.setState({ activeServiceId: 'svcA' })
  })

  test('CreateInviteDialog sends allow_concurrent_sessions=true when the shared-logins box is checked', async () => {
    const user = userEvent.setup()
    const posted = captureInviteBody()

    render(
      <CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />,
    )

    await fillRequiredFields(user)
    await user.click(screen.getByRole('checkbox', { name: /multiple people to use this link/i }))
    await user.click(screen.getByRole('button', { name: /create invite/i }))

    await waitFor(() => expect(posted.current).toBeDefined())
    expect(posted.current?.allow_concurrent_sessions).toBe(true)
  })

  test('CreateInviteDialog defaults allow_concurrent_sessions to false', async () => {
    const user = userEvent.setup()
    const posted = captureInviteBody()

    render(
      <CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />,
    )

    await fillRequiredFields(user)
    await user.click(screen.getByRole('button', { name: /create invite/i }))

    await waitFor(() => expect(posted.current).toBeDefined())
    expect(posted.current?.allow_concurrent_sessions).toBe(false)
  })

  test('CreateInviteDialog OAuth mode hides the passcode field and shows the provider selector', async () => {
    server.use(
      http.get('/api/admin/share/oauth-providers', () =>
        HttpResponse.json({ providers: [{ id: 'google', display_name: 'Google Workspace', enabled: true }] }),
      ),
    )
    const user = userEvent.setup()
    render(<CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />)

    expect(screen.getByLabelText('Passcode')).toBeInTheDocument()

    await user.click(screen.getByRole('radio', { name: /SSO/i }))
    expect(screen.queryByLabelText('Passcode')).toBeNull()
    expect(await screen.findByLabelText('Identity provider')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Name'), 'Jane')
    await user.type(screen.getByLabelText('Email'), 'jane@example.com')
    await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
    expect(screen.getByRole('button', { name: /create invite/i })).toBeDisabled()
  })

  test('CreateInviteDialog OAuth mode with no configured providers stays unsubmittable', async () => {
    server.use(http.get('/api/admin/share/oauth-providers', () => HttpResponse.json({ providers: [] })))
    const user = userEvent.setup()
    render(<CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />)

    await user.type(screen.getByLabelText('Name'), 'Jane')
    await user.type(screen.getByLabelText('Email'), 'jane@example.com')
    await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
    await user.click(screen.getByRole('radio', { name: /SSO/i }))

    expect(await screen.findByText(/add one to the registry first/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create invite/i })).toBeDisabled()
  })
})

// --- useTableSort --------------------------------------------------------

type SortRow = { name: string; n: number | null }

const sortRows: SortRow[] = [
  { name: 'bravo', n: 2 },
  { name: 'alpha', n: null },
  { name: 'charlie', n: 1 },
]

const sortAccessors: SortAccessors<SortRow> = {
  name: (r) => r.name,
  n: (r) => r.n,
}

const names = (rs: SortRow[]) => rs.map((r) => r.name)

describe('useTableSort', () => {
  test('default sorts by the given key/direction', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    expect(names(result.current.sorted)).toEqual(['alpha', 'bravo', 'charlie'])
    expect(result.current.sortKey).toBe('name')
    expect(result.current.sortDir).toBe('asc')
  })

  test('toggling the active key flips asc <-> desc', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('name'))
    expect(result.current.sortDir).toBe('desc')
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('nulls always sort last, regardless of direction', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'n', defaultDir: 'desc' }),
    )
    expect(names(result.current.sorted)).toEqual(['bravo', 'charlie', 'alpha'])
    act(() => result.current.toggle('n')) // -> asc
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('switching to a new key resets direction to desc', () => {
    const { result } = renderHook(() =>
      useTableSort(sortRows, sortAccessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('n'))
    expect(result.current.sortKey).toBe('n')
    expect(result.current.sortDir).toBe('desc')
  })
})

// --- InviteAnalystDialog -------------------------------------------------

const analystService = { service_id: 'svc-copy', name: 'Copy Svc' } as unknown as ServiceConfig

const analystInvite = {
  name: 'analyst',
  service_id: 'svc-copy',
  fos_bucket: 'b',
  fos_region: 'r',
  fos_endpoint: 'e',
  fos_prefix: 'p',
  access_key_id: 'AKIAEXAMPLE',
  secret_key: 'not-a-real-secret',
}

const openAnalystResult = async () => {
  useServiceStore.setState({ activeServiceId: 'svc-copy' })
  server.use(
    http.post('*/api/services/:service_id/generate-viewer-key', () =>
      HttpResponse.json(analystInvite),
    ),
  )
  render(<InviteAnalystDialog service={analystService} open onOpenChange={vi.fn()} />, {
    wrapper: makeQueryWrapper(createTestQueryClient()),
  })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /generate invite/i }))
  })
  return screen.findByRole('button', { name: /copy json/i })
}

describe('InviteAnalystDialog', () => {
  test('Copy JSON flips to Copied after a successful write and resets', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const btn = await openAnalystResult()
    vi.useFakeTimers()
    await act(async () => {
      fireEvent.click(btn)
    })
    expect(writeText).toHaveBeenCalledTimes(1)
    expect(JSON.parse(writeText.mock.calls[0][0]).service_id).toBe('svc-copy')
    expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument()
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(screen.getByRole('button', { name: /copy json/i })).toBeInTheDocument()
  })

  test('Copy JSON does not claim success when the clipboard write is rejected', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockRejectedValue(new Error('denied')) },
      configurable: true,
    })
    const btn = await openAnalystResult()
    await act(async () => {
      fireEvent.click(btn)
    })
    expect(screen.queryByRole('button', { name: /copied!/i })).toBeNull()
  })
})
