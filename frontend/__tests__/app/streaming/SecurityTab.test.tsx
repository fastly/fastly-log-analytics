import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import React from 'react'
import SecurityTab, { coarsenEnforcement } from '@/app/streaming/_sections/tabs/SecurityTab'
import { recencyOf } from '@/app/streaming/_sections/tabs/SubscriberIpsDialog'
import { client } from '@/lib/api'
import { showToastWithAction } from '@/lib/toast'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { createTestQueryClient } from '../../helpers/query'
import { spyOnConsoleError } from '../../helpers/page-smoke'

vi.mock('@/stores/serviceStore', async () => (await import('../../helpers/page-smoke')).serviceStoreModuleMock())

vi.mock('@/stores/filterStore', async () => (await import('../../helpers/page-smoke')).filterStoreModuleMock())

vi.mock('next/navigation', async () => (await import('../../helpers/page-smoke')).navigationModuleMock('/streaming'))

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn(), DELETE: vi.fn(), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

vi.mock('@/lib/toast', () => ({ showToast: vi.fn(), showToastWithAction: vi.fn() }))

vi.mock('@/hooks/useIsAnalyst', () => ({ useIsAnalyst: vi.fn(() => false) }))

vi.mock('@/components/charts/TimeSeriesChart', () => ({
  TimeSeriesChart: ({ data }: { data: { name: string; y: number[] }[] }) => (
    <div data-testid={data[0]?.name.startsWith('Edge') ? 'bandwidth-chart' : 'enforcement-chart'}>
      {data.map((t) => (
        <span key={t.name}>{`${t.name}: ${t.y.join(',')}`}</span>
      ))}
    </div>
  ),
}))

const FULL_RESPONSE = {
  available: true,
  fields: { country: true, asn: true, referer: true, host: true, resp_bytes: true, edge: true, cs_content_id: true, ip: true, cs_sub_id: true, p_type: true, p_desc: true, cs_status: true, cs_mode: true, cs_rl_mode: true },
  content_id: null,
  has_shield_split: true,
  content_ids: [
    { content_id: 'movie-1', requests: 3 },
    { content_id: 'movie-2', requests: 2 },
  ],
  top_countries: [{ value: 'US', users: 2, requests: 3, bytes: 3072 }],
  top_asns: [{ value: '7922', label: 'Comcast Cable (7922)', users: 2, requests: 3, bytes: 3072 }],
  top_referers: [{ value: 'referring.example', users: 2, requests: 3, bytes: 3072 }],
  top_hosts: [{ value: 'pirate.example', users: 987, requests: 1234, bytes: 1048576 }],
  top_subscribers: [
    { subscriber_id: 'acct-shared', distinct_ips: 14, distinct_countries: 5, requests: 90, bytes: 2097152, last_seen: '2026-01-01T00:55:00+00:00' },
    { subscriber_id: 'acct-home', distinct_ips: 1, distinct_countries: 1, requests: 300, bytes: 1024, last_seen: '2026-01-01T00:40:00+00:00' },
  ],
  proxy_types: [
    { value: 'hosting', users: 25, requests: 300, bytes: 524288 },
    { value: 'anonymous', users: 4, requests: 20, bytes: 8192 },
  ],
  proxy_descriptions: [
    { value: 'vpn', users: 40, requests: 500, bytes: 2097152 },
    { value: 'tor-exit', users: 3, requests: 12, bytes: 4096 },
  ],
  subscribers_masked: false,
  bandwidth_ts: [
    { bucket: '2026-01-01 00:00:00', edge_bytes: 3000, shield_bytes: 300 },
    { bucket: '2026-01-01 00:05:00', edge_bytes: 6000, shield_bytes: 0 },
  ],
  enforcement: [
    { status: 'allowed', requests: 900, enforced: 0, users: 80, bytes: 9000 },
    { status: 'blocked', requests: 40, enforced: 25, users: 3, bytes: 400 },
    { status: 'rate_limited', requests: 12, enforced: 12, users: 2, bytes: null },
  ],
  enforcement_ts: [
    { bucket: '2026-01-01 00:00:00', status: 'allowed', requests: 500 },
    { bucket: '2026-01-01 00:00:00', status: 'blocked', requests: 10 },
    { bucket: '2026-01-01 00:05:00', status: 'allowed', requests: 400 },
    { bucket: '2026-01-01 00:05:00', status: 'rate_limited', requests: 12 },
  ],
}

const postMock = vi.mocked(client.POST)
const getMock = vi.mocked(client.GET)
const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

beforeEach(() => {
  // cmdk (the content picker's search list) observes its own size.
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  queryClient.clear()
  postMock.mockReset()
  postMock.mockResolvedValue({ data: FULL_RESPONSE } as never)
  getMock.mockReset()
  routeGets()
  vi.mocked(client.DELETE).mockReset()
  vi.mocked(showToastWithAction).mockReset()
  vi.mocked(useIsAnalyst).mockReturnValue(false)
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  vi.unstubAllGlobals()
  errorSpy.mockRestore()
})

function renderTab(searchParams = '', onUrlUpdate?: (e: UrlUpdateEvent) => void) {
  return render(
    <NuqsTestingAdapter searchParams={searchParams} onUrlUpdate={onUrlUpdate} hasMemory>
      <QueryClientProvider client={queryClient}>
        <SecurityTab
          activeServiceId="test-svc"
          filterPayload={{}}
          startTime="2026-01-01T00:00:00Z"
          endTime="2026-01-01T01:00:00Z"
          relativeRange={null}
          isAutoRange={false}
          anchor="2026-01-01T01:00:00Z"
          timezone="UTC"
          bucketSeconds={300}
        />
      </QueryClientProvider>
    </NuqsTestingAdapter>,
  )
}

function lastBody() {
  const call = postMock.mock.calls.at(-1)
  return (call?.[1] as unknown as { body: Record<string, unknown> }).body
}

test('renders the three leaderboards and the edge vs shield chart', async () => {
  renderTab()

  expect(await screen.findByText('pirate.example')).toBeInTheDocument()
  expect(screen.getByText('1,234')).toBeInTheDocument()
  expect(screen.getByText('1 MB')).toBeInTheDocument()
  expect(screen.getByText('referring.example')).toBeInTheDocument()
  expect(screen.getByText(/United States \(US\)/)).toBeInTheDocument()
  // Card titles follow the Overview page's "Name (Top 10)" style.
  for (const name of ['Countries', 'ASNs', 'Referers', 'Hosts']) {
    expect(screen.getByText((_, el) => el?.getAttribute('data-slot') === 'card-title' && el.textContent === `${name} (Top 10)`)).toBeInTheDocument()
  }

  // bytes per 300 s bucket → bits per second.
  const chart = screen.getByTestId('bandwidth-chart')
  expect(within(chart).getByText('Edge (to viewers): 80,160')).toBeInTheDocument()
  expect(within(chart).getByText('Shield (to edge): 8,0')).toBeInTheDocument()

  expect(postMock).toHaveBeenCalledWith('/api/cmcd/content-security', expect.anything())
  expect(lastBody()).toMatchObject({ content_id: null, bucket_seconds: 300, top_n: 10 })
})

test('?content= deep-links to one piece of content', async () => {
  renderTab('?content=movie-2')

  await screen.findByText('pirate.example')
  expect(lastBody()).toMatchObject({ content_id: 'movie-2' })
  expect(screen.getByRole('combobox', { name: 'Content ID' })).toHaveTextContent('movie-2')
})

test('picking a content ID writes ?content= and refetches for it', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  renderTab('', onUrlUpdate)
  await screen.findByText('pirate.example')

  await userEvent.click(screen.getByRole('combobox', { name: 'Content ID' }))
  await userEvent.click(await screen.findByRole('option', { name: /movie-1/ }))

  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?content=movie-1' }))
  await waitFor(() => expect(lastBody()).toMatchObject({ content_id: 'movie-1' }))

  await userEvent.click(screen.getByRole('button', { name: /clear/i }))
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '' }))
})

test('explains which log fields are missing', async () => {
  postMock.mockResolvedValue({
    data: {
      ...FULL_RESPONSE,
      fields: { ...FULL_RESPONSE.fields, host: false, cs_content_id: false },
      content_ids: [],
      top_hosts: [],
    },
  } as never)
  renderTab()

  expect(await screen.findByText(/Host field \(Group A\)/)).toBeInTheDocument()
  expect(screen.getByText(/Token option with Content ID configured/)).toBeInTheDocument()
  expect(screen.getByRole('combobox', { name: 'Content ID' })).toBeDisabled()
})

test('shows the reason when the service is not supported', async () => {
  postMock.mockResolvedValue({
    data: { available: false, reason: 'Content security metrics are not yet supported for high-scale services.' },
  } as never)
  renderTab()

  expect(await screen.findByText(/not yet supported for high-scale services/)).toBeInTheDocument()
  expect(screen.queryByTestId('bandwidth-chart')).not.toBeInTheDocument()
})

function subscriberCard() {
  const title = screen.getByText((_, el) => el?.getAttribute('data-slot') === 'card-title' && el.textContent === 'Subscriber Usage (Top 10)')
  return title.closest('[data-slot="card"]') as HTMLElement
}

test('lists subscriber IDs with IPs, countries, requests, bandwidth and last seen', async () => {
  renderTab()
  await screen.findByText('acct-shared')

  const card = within(subscriberCard())
  const headers = card.getAllByRole('columnheader').map((h) => h.textContent)
  expect(headers).toEqual(['#', 'Subscriber ID', 'Distinct IPs', 'Countries', 'Requests', 'Bandwidth', 'Last seen', 'Actions'])

  const [, first, second] = card.getAllByRole('row')
  expect(within(first).getAllByRole('cell').map((c) => c.textContent)).toEqual([
    '1', 'acct-shared', '14', '5', '90', '2 MB', '2026-01-01 00:55:00', '',
  ])
  // Server order is kept: fewer IPs ranks lower even with more requests.
  expect(within(second).getAllByRole('cell')[1]).toHaveTextContent('acct-home')
  expect(screen.queryByText(/hidden for shared sessions/)).not.toBeInTheDocument()
})

test('drops the Countries and Bandwidth columns when those fields are not logged', async () => {
  postMock.mockResolvedValue({
    data: {
      ...FULL_RESPONSE,
      fields: { ...FULL_RESPONSE.fields, country: false },
      top_countries: [],
      top_subscribers: [{ subscriber_id: 'acct-1', distinct_ips: 3, distinct_countries: null, requests: 7, bytes: null, last_seen: null }],
    },
  } as never)
  renderTab()
  await screen.findByText('acct-1')

  const headers = within(subscriberCard()).getAllByRole('columnheader').map((h) => h.textContent)
  expect(headers).toEqual(['#', 'Subscriber ID', 'Distinct IPs', 'Requests', 'Last seen', 'Actions'])
})

test('notes masking when the analyst sees rank labels', async () => {
  postMock.mockResolvedValue({
    data: {
      ...FULL_RESPONSE,
      top_subscribers: [{ ...FULL_RESPONSE.top_subscribers[0], subscriber_id: 'Subscriber #1' }],
      subscribers_masked: true,
    },
  } as never)
  renderTab()

  expect(await screen.findByText('Subscriber #1')).toBeInTheDocument()
  expect(screen.getByText(/hidden for shared sessions/)).toBeInTheDocument()
})

test('explains that the Token option is required for the subscriber list', async () => {
  postMock.mockResolvedValue({
    data: { ...FULL_RESPONSE, fields: { ...FULL_RESPONSE.fields, cs_sub_id: false }, top_subscribers: [] },
  } as never)
  renderTab()

  expect(await screen.findByText(/Streaming → Token option \(Subscriber ID\)/)).toBeInTheDocument()
  expect(screen.queryByText('acct-shared')).not.toBeInTheDocument()
})

function cardByTitle(title: string) {
  const el = screen.getByText((_, e) => e?.getAttribute('data-slot') === 'card-title' && e.textContent === title)
  return within(el.closest('[data-slot="card"]') as HTMLElement)
}

test('leaderboards show Users as the first metric', async () => {
  renderTab()
  await screen.findByText('pirate.example')

  for (const [title, valueHeader] of [
    ['Countries (Top 10)', 'Country'],
    ['ASNs (Top 10)', 'ASN'],
    ['Referers (Top 10)', 'Referer'],
    ['Hosts (Top 10)', 'Host'],
  ]) {
    const headers = cardByTitle(title).getAllByRole('columnheader').map((h) => h.textContent)
    expect(headers).toEqual([valueHeader, 'Users', 'Requests', 'Bandwidth'])
  }
  const [, hostRow] = cardByTitle('Hosts (Top 10)').getAllByRole('row')
  expect(within(hostRow).getAllByRole('cell').map((c) => c.textContent)).toEqual([
    'pirate.example',
    '987',
    '1,234',
    '1 MB',
  ])
})

test('ASNs sit beside Countries; Hosts leads the proxy row', async () => {
  renderTab()
  await screen.findByText('pirate.example')

  const order = screen
    .getAllByText((_, el) => el?.getAttribute('data-slot') === 'card-title')
    .map((el) => el.textContent?.replace(/ \(Top 10\)$/, ''))
  expect(order.slice(0, 6)).toEqual(['Countries', 'ASNs', 'Referers', 'Hosts', 'Proxy Types', 'Proxy Descriptions'])
  // The ASN row shows the owner name, not just the number.
  expect(cardByTitle('ASNs (Top 10)').getByText('Comcast Cable (7922)')).toBeInTheDocument()
})

test('leaderboards omit Users when the backend sends none', async () => {
  postMock.mockResolvedValue({
    data: { ...FULL_RESPONSE, top_hosts: [{ value: 'pirate.example', requests: 1234, bytes: 1048576 }] },
  } as never)
  renderTab()
  await screen.findByText('pirate.example')

  const headers = cardByTitle('Hosts (Top 10)').getAllByRole('columnheader').map((h) => h.textContent)
  expect(headers).toEqual(['Host', 'Requests', 'Bandwidth'])
})

test('lists every proxy description with users, requests and bandwidth (no top-N cap)', async () => {
  const many = Array.from({ length: 12 }, (_, i) => ({ value: `proxy-${i}`, users: 1, requests: 12 - i, bytes: 100 }))
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, proxy_descriptions: many } } as never)
  renderTab()
  await screen.findByText('proxy-0')

  const card = cardByTitle('Proxy Descriptions')
  expect(card.getAllByRole('columnheader').map((h) => h.textContent)).toEqual([
    'Proxy description',
    'Users',
    'Requests',
    'Bandwidth',
  ])
  // Header row + all 12 values, more than TOP_N.
  expect(card.getAllByRole('row')).toHaveLength(13)
})

test('proxy description row shows its metrics', async () => {
  renderTab()
  await screen.findByText('vpn')

  const [, vpnRow] = cardByTitle('Proxy Descriptions').getAllByRole('row')
  expect(within(vpnRow).getAllByRole('cell').map((c) => c.textContent)).toEqual(['vpn', '40', '500', '2 MB'])
})

test('explains that Group I is required for proxy descriptions', async () => {
  postMock.mockResolvedValue({
    data: { ...FULL_RESPONSE, fields: { ...FULL_RESPONSE.fields, p_desc: false }, proxy_descriptions: [] },
  } as never)
  renderTab()

  expect(await screen.findByText(/Proxy Description field \(Group I\)/)).toBeInTheDocument()
})

test('lists every proxy type with users, requests and bandwidth (no top-N cap)', async () => {
  const many = Array.from({ length: 11 }, (_, i) => ({ value: `type-${i}`, users: 1, requests: 11 - i, bytes: 100 }))
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, proxy_types: many } } as never)
  renderTab()
  await screen.findByText('type-0')

  const card = cardByTitle('Proxy Types')
  expect(card.getAllByRole('columnheader').map((h) => h.textContent)).toEqual([
    'Proxy type',
    'Users',
    'Requests',
    'Bandwidth',
  ])
  // Header row + all 11 values, more than TOP_N.
  expect(card.getAllByRole('row')).toHaveLength(12)
  const [, firstRow] = card.getAllByRole('row')
  expect(within(firstRow).getAllByRole('cell').map((c) => c.textContent)).toEqual(['type-0', '1', '11', '100 B'])
})

test('explains that Group I is required for proxy types', async () => {
  postMock.mockResolvedValue({
    data: { ...FULL_RESPONSE, fields: { ...FULL_RESPONSE.fields, p_type: false }, proxy_types: [] },
  } as never)
  renderTab()

  expect(await screen.findByText(/Proxy Type field \(Group I\)/)).toBeInTheDocument()
})

test('leaderboard values open the filter / copy menu keyed to their log field', async () => {
  const user = userEvent.setup()
  const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null)
  renderTab()

  const cases: [string, string][] = [
    ['US', 'filter_country=US'],
    ['referring.example', `filter_referer=${encodeURIComponent('*://referring.example/*')}`],
    ['pirate.example', 'filter_host=pirate.example'],
    ['hosting', 'filter_p_type=hosting'],
    ['vpn', 'filter_p_desc=vpn'],
  ]
  for (const [value, qs] of cases) {
    await user.click(await screen.findByRole('button', { name: `Filter actions for ${value}` }))
    expect(await screen.findByText('Filter streaming page')).toBeInTheDocument()
    expect(screen.getByText('Copy value')).toBeInTheDocument()
    await user.click(screen.getByText('Open in dashboard'))
    expect(openSpy).toHaveBeenLastCalledWith(`/dashboard?${qs}`, '_blank', 'noopener,noreferrer')
  }
  openSpy.mockRestore()
})

const SUBSCRIBER_IPS = {
  available: true,
  subscriber_id: 'acct-shared',
  fields: {},
  truncated: false,
  ips: [
    { ip: '203.0.113.3', country: 'DE', host: 'b.example', requests: 3, bytes: 300, last_seen: '2026-01-01T00:58:00+00:00' },
    // Same IP from a second country: its own row.
    { ip: '203.0.113.3', country: 'FR', host: 'b.example', requests: 1, bytes: 100, last_seen: '2026-01-01T00:57:00+00:00' },
    { ip: '203.0.113.2', country: 'US', host: 'a.example', requests: 2, bytes: 200, last_seen: '2026-01-01T00:50:00+00:00' },
    { ip: '203.0.113.1', country: null, host: null, requests: 1, bytes: null, last_seen: '2026-01-01T00:30:00+00:00' },
  ],
}

test('recencyOf buckets last-seen into green / yellow / red', () => {
  const now = Date.parse('2026-01-01T01:00:00Z')
  const ago = (min: number) => new Date(now - min * 60_000).toISOString()
  expect(recencyOf(ago(0), now)).toBe('active')
  expect(recencyOf(ago(5), now)).toBe('active')
  expect(recencyOf(ago(5.5), now)).toBe('recent')
  expect(recencyOf(ago(20), now)).toBe('recent')
  expect(recencyOf(ago(21), now)).toBe('stale')
  expect(recencyOf(null, now)).toBe('stale')
})

test('clicking a subscriber opens a dialog listing its IPs with recency dots', async () => {
  vi.useFakeTimers({ toFake: ['Date'], now: new Date('2026-01-01T01:00:00Z') })
  try {
    postMock.mockImplementation(((path: string) =>
      Promise.resolve({
        data: path === '/api/cmcd/content-security/subscriber' ? SUBSCRIBER_IPS : FULL_RESPONSE,
      })) as never)
    const user = userEvent.setup()
    renderTab('?content=movie-1')

    await user.click(await screen.findByRole('button', { name: 'Show IP addresses for acct-shared' }))

    const dialog = await screen.findByRole('dialog')
    expect(await within(dialog).findByText('Germany (DE)')).toBeInTheDocument()
    expect(within(dialog).getAllByText('203.0.113.3')).toHaveLength(2)
    expect(within(dialog).getAllByText('b.example')).toHaveLength(2)
    expect(within(dialog).getByText('Germany (DE)')).toBeInTheDocument()
    expect(within(dialog).getByText('France (FR)')).toBeInTheDocument()
    const dots = Array.from(dialog.querySelectorAll('[data-recency]')).map((el) => el.getAttribute('data-recency'))
    expect(dots).toEqual(['active', 'active', 'recent', 'stale'])

    const call = postMock.mock.calls.find((c) => c[0] === '/api/cmcd/content-security/subscriber')
    const body = (call?.[1] as unknown as { body: Record<string, unknown> }).body
    expect(body).toMatchObject({ subscriber_id: 'acct-shared', content_id: 'movie-1' })
  } finally {
    vi.useRealTimers()
  }
})

test('masked subscriber ids are not clickable', async () => {
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, subscribers_masked: true } } as never)
  renderTab()

  expect(await screen.findByText('acct-shared')).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /Show IP addresses/ })).not.toBeInTheDocument()
})

// ── Policy rules (admin-only) ────────────────────────────────────────────────

const EMPTY_RULES = { country: [], asn: [], referer: [], host: [], proxy_type: [], proxy_description: [] }

function makePolicy(overrides: Record<string, unknown> = {}, rules: Record<string, unknown[]> = {}) {
  return {
    id: 'pol-1',
    name: 'Playoffs',
    country_regex: null,
    rev: 3,
    rules: { ...EMPTY_RULES, ...rules },
    individual: { subscriber_id: [], ip: [] },
    created_at: '2026-01-01T00:00:00Z',
    created_by: 'admin',
    updated_at: '2026-01-01T00:30:00Z',
    updated_by: 'admin',
    used_by: [],
    ...overrides,
  }
}

const ASSIGNMENT = {
  id: 'asg-1',
  policy_id: 'pol-1',
  policy_name: 'Playoffs',
  scope_type: 'content_id',
  content_id: 'movie-1',
  mode: 'ENFORCE',
  starts_at: null,
  ends_at: null,
  status: 'active',
  created_at: '2026-01-01T00:00:00Z',
  created_by: 'admin',
}

const PUBLISHED = { status: 'published', rev: 4, error: null }

function effectiveWith(policy: ReturnType<typeof makePolicy> | null, extra: Record<string, unknown> = {}) {
  return {
    content_id: null,
    configured: true,
    shared_with: 0,
    policy,
    assignment: policy ? ASSIGNMENT : null,
    ...extra,
  }
}

/** Route the policy GETs; `overrides` replaces the response for a path. */
function routeGets(overrides: Record<string, unknown> = {}) {
  const responses: Record<string, unknown> = {
    '/api/admin/content-security/effective': effectiveWith(null),
    '/api/admin/content-security/policies': { configured: true, policies: [] },
    '/api/admin/content-security/sync-status': {
      configured: true,
      rev: 4,
      published_rev: 4,
      in_sync: true,
      last_ok_at: '2026-01-01T00:59:00Z',
      last_error: null,
    },
    ...overrides,
  }
  getMock.mockImplementation(((path: string) => Promise.resolve({ data: responses[path] })) as never)
}

/** Route POSTs: live-rule echoes a policy holding the new rule; everything else is the tab's data. */
function routePosts(opts: { created_default?: boolean; publish?: Record<string, unknown>; extra?: Record<string, unknown> } = {}) {
  postMock.mockImplementation(((path: string, init: { body: Record<string, unknown> }) => {
    if (path === '/api/admin/content-security/live-rule') {
      const b = init.body as { category: string; value: string; action: string; kbps: number }
      return Promise.resolve({
        data: {
          policy: makePolicy({}, { [b.category]: [{ value: b.value, action: b.action, kbps: b.kbps }] }),
          assignment: ASSIGNMENT,
          created_default: opts.created_default ?? false,
          publish: opts.publish ?? PUBLISHED,
        },
      })
    }
    if (opts.extra && path in opts.extra) return Promise.resolve({ data: opts.extra[path] })
    return Promise.resolve({ data: FULL_RESPONSE })
  }) as never)
}

function liveRuleCalls() {
  return postMock.mock.calls
    .filter((c) => c[0] === '/api/admin/content-security/live-rule')
    .map((c) => (c[1] as unknown as { body: unknown }).body)
}

async function openBlockMenu(user: ReturnType<typeof userEvent.setup>, value: string) {
  await user.click(await screen.findByRole('button', { name: `Filter actions for ${value}` }))
  await user.click(await screen.findByText('Block, throttle or allow…'))
  return screen.findByRole('dialog')
}

test('Block from a leaderboard menu confirms the clicked value, then offers Undo', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(makePolicy()) })
  routePosts()
  vi.mocked(client.DELETE).mockResolvedValue({ data: { policy: makePolicy(), publish: PUBLISHED } } as never)
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  expect(within(dialog).getByText('Block, throttle or allow')).toBeInTheDocument()
  // One category in a leaderboard: no category picker, the clicked value preselected.
  expect(within(dialog).queryByText('Category')).not.toBeInTheDocument()
  expect(within(dialog).getByRole('combobox', { name: 'Country' })).toHaveTextContent('United States (US)')
  // The policy in effect is named, and it is not shared.
  expect(await within(dialog).findByText('Playoffs')).toBeInTheDocument()
  expect(within(dialog).queryByRole('note')).not.toBeInTheDocument()

  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() =>
    expect(liveRuleCalls()).toEqual([{ category: 'country', value: 'US', action: 'block', kbps: 0, content_id: null }]),
  )
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(showToastWithAction).toHaveBeenCalledWith('Blocked Country US', expect.objectContaining({ actionLabel: 'Undo' }))

  // The toast's Undo removes the rule from the policy it was added to.
  vi.mocked(showToastWithAction).mock.calls[0][1].onAction()
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/policies/{policy_id}/rules', {
      params: { path: { policy_id: 'pol-1' }, query: { category: 'country', value: 'US' } },
    }),
  )
})

test('with no policy in effect the dialog says a Default policy will be created, and the toast says it was', async () => {
  routeGets()
  routePosts({ created_default: true })
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  expect(await within(dialog).findByText((_, el) => el?.tagName === 'P' && /A Default policy will be created/.test(el.textContent ?? ''))).toBeInTheDocument()
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() =>
    expect(showToastWithAction).toHaveBeenCalledWith(
      'Blocked Country US. Created a Default policy for all content',
      expect.objectContaining({ actionLabel: 'Undo' }),
    ),
  )
})

test('the dialog warns when the policy in effect is shared', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(makePolicy(), { shared_with: 2 }) })
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  expect(await within(dialog).findByRole('note')).toHaveTextContent('also scheduled for 2 other entries')
})

test('a rule is added to the policy in effect for the selected content', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(makePolicy(), { content_id: 'movie-2' }) })
  routePosts()
  const user = userEvent.setup()
  renderTab('?content=movie-2')

  expect(getMock).toHaveBeenCalledWith(
    '/api/admin/content-security/effective',
    expect.objectContaining({ params: { query: { content_id: 'movie-2' } } }),
  )
  const dialog = await openBlockMenu(user, 'US')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))
  await waitFor(() => expect(liveRuleCalls()[0]).toMatchObject({ content_id: 'movie-2' }))
})

test('an ASN can be blocked from its leaderboard', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, '7922')
  expect(within(dialog).getByRole('combobox', { name: 'ASN' })).toHaveTextContent('Comcast Cable (7922)')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(liveRuleCalls()).toMatchObject([{ category: 'asn', value: '7922', action: 'block' }]))
})

test('Block dialog lets the user pick a different value from the list', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'hosting')
  await user.click(within(dialog).getByRole('combobox', { name: 'Proxy type' }))
  await user.click(await screen.findByRole('option', { name: 'anonymous' }))
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(liveRuleCalls()).toMatchObject([{ category: 'proxy_type', value: 'anonymous' }]))
})

test('Block dialog accepts a custom value', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'pirate.example')
  const blockButton = within(dialog).getByRole('button', { name: 'Block' })
  await user.click(within(dialog).getByRole('combobox', { name: 'Host' }))
  await user.type(screen.getByPlaceholderText('Search or type a value…'), 'mirror.example')
  await user.click(await within(screen.getByRole('listbox')).findByText('Use “mirror.example”'))
  expect(within(dialog).getByRole('combobox', { name: 'Host' })).toHaveTextContent('mirror.example')
  await user.click(blockButton)

  await waitFor(() => expect(liveRuleCalls()).toMatchObject([{ category: 'host', value: 'mirror.example' }]))
})

test('a failed block keeps the dialog open with the server message', async () => {
  routeGets()
  postMock.mockImplementation(((path: string) =>
    path === '/api/admin/content-security/live-rule'
      ? Promise.reject(new Error('Error: policy limit reached'))
      : Promise.resolve({ data: FULL_RESPONSE })) as never)
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'vpn')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  expect(await within(dialog).findByRole('alert')).toHaveTextContent('policy limit reached')
  expect(showToastWithAction).not.toHaveBeenCalled()
})

test('a block that is saved but not yet live offers Retry instead of Undo', async () => {
  routeGets()
  routePosts({ publish: { status: 'failed', rev: 4, error: 'blocker returned HTTP 502' } })
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(showToastWithAction).toHaveBeenCalled())
  const [message, options] = vi.mocked(showToastWithAction).mock.calls[0]
  expect(message).toContain('Blocked Country US')
  expect(message).toContain('not yet live')
  expect(message).toContain('HTTP 502')
  expect(options).toMatchObject({ actionLabel: 'Retry', kind: 'warn' })

  // Retry re-publishes.
  postMock.mockResolvedValue({ data: PUBLISHED } as never)
  options.onAction()
  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/publish'))
})

test('the dialog can throttle instead of block, offering 128 / 256 / 512 kbps', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  const action = within(dialog).getByRole('combobox', { name: 'Action' })
  expect(action).toHaveTextContent('Block (no traffic allowed)')
  await user.click(action)
  const labels = (await screen.findAllByRole('option')).map((o) => o.textContent)
  expect(labels).toEqual([
    'Block (no traffic allowed)',
    'Throttle to: 128 kbps',
    'Throttle to: 256 kbps',
    'Throttle to: 512 kbps',
    "Allow (skip this policy's other rules)",
  ])
  await user.click(screen.getByRole('option', { name: 'Throttle to: 256 kbps' }))
  await user.click(within(dialog).getByRole('button', { name: 'Throttle' }))

  await waitFor(() =>
    expect(liveRuleCalls()).toEqual([{ category: 'country', value: 'US', action: 'pace', kbps: 256, content_id: null }]),
  )
  expect(showToastWithAction).toHaveBeenCalledWith(
    'Throttled Country US to 256 kbps',
    expect.objectContaining({ actionLabel: 'Undo' }),
  )
})

test('Allow adds an allow rule', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  const dialog = await openBlockMenu(user, 'US')
  await user.click(within(dialog).getByRole('combobox', { name: 'Action' }))
  await user.click(await screen.findByRole('option', { name: /^Allow/ }))
  await user.click(within(dialog).getByRole('button', { name: 'Allow' }))

  await waitFor(() => expect(liveRuleCalls()).toMatchObject([{ category: 'country', value: 'US', action: 'exempt', kbps: 0 }]))
  expect(showToastWithAction).toHaveBeenCalledWith('Allowed Country US', expect.objectContaining({ actionLabel: 'Undo' }))
})

test('subscriber IDs can only be blocked', async () => {
  routeGets()
  routePosts()
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Actions for acct-home' }))
  expect(await screen.findByText('Show IP addresses')).toBeInTheDocument()
  await user.click(screen.getByText('Block…'))

  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByRole('combobox', { name: 'Subscriber ID' })).toHaveTextContent('acct-home')
  await user.click(within(dialog).getByRole('combobox', { name: 'Action' }))
  expect((await screen.findAllByRole('option')).map((o) => o.textContent)).toEqual(['Block (no traffic allowed)'])
  await user.keyboard('{Escape}')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() =>
    expect(liveRuleCalls()).toEqual([{ category: 'subscriber_id', value: 'acct-home', action: 'block', kbps: 0, content_id: null }]),
  )
})

test('the subscriber dialog Block button offers the subscriber ID or any of its IPs', async () => {
  routeGets()
  routePosts({ extra: { '/api/cmcd/content-security/subscriber': SUBSCRIBER_IPS } })
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Show IP addresses for acct-shared' }))
  const ipsDialog = await screen.findByRole('dialog')
  await within(ipsDialog).findByText('Germany (DE)')
  // Toolbar summary: distinct IPs (203.0.113.3 appears twice) and distinct non-null countries.
  expect(within(ipsDialog).getByText('3 IP addresses · 3 countries')).toBeInTheDocument()
  await user.click(within(ipsDialog).getByRole('button', { name: /Block/ }))

  const dialogs = await screen.findAllByRole('dialog')
  const dialog = dialogs.find((d) => within(d).queryByText('Block, throttle or allow')) as HTMLElement
  expect(within(dialog).getByRole('combobox', { name: 'Subscriber ID' })).toHaveTextContent('acct-shared')

  await user.click(within(dialog).getByRole('combobox', { name: 'Category' }))
  await user.click(await screen.findByRole('option', { name: 'IP address' }))
  const ipPicker = within(dialog).getByRole('combobox', { name: 'IP address' })
  expect(ipPicker).toHaveTextContent('203.0.113.3')
  await user.click(ipPicker)
  // Duplicate IP rows (one per country) collapse to one choice.
  expect(await screen.findAllByRole('option', { name: /^203\.0\.113\.\d$/ })).toHaveLength(3)
  await user.click(screen.getByRole('option', { name: '203.0.113.2' }))
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(liveRuleCalls()).toMatchObject([{ category: 'ip', value: '203.0.113.2', action: 'block' }]))
})

test('the banner names the policy in effect and Edit policy opens it in the Policies view', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(makePolicy(), { content_id: 'movie-1' }) })
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  const user = userEvent.setup()
  renderTab('?content=movie-1', onUrlUpdate)

  expect(await screen.findByText('Playoffs', { selector: 'strong' })).toBeInTheDocument()
  expect(screen.getByText(/In effect for/)).toHaveTextContent('In effect for movie-1: Playoffs (Enforce)')
  await user.click(screen.getByRole('button', { name: 'Edit policy' }))

  expect(onUrlUpdate).toHaveBeenCalledWith(expect.objectContaining({ queryString: expect.stringContaining('csview=policies') }))
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: expect.stringContaining('policy=pol-1') }))
})

test('the banner says when no policy is in effect', async () => {
  routeGets()
  renderTab()
  expect(await screen.findByText(/No policy is in effect for all content/)).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Edit policy' })).not.toBeInTheDocument()
})

test('a policy that is not yet live shows a Retry notice in the Monitor view', async () => {
  routeGets({
    '/api/admin/content-security/sync-status': {
      configured: true,
      rev: 5,
      published_rev: 4,
      in_sync: false,
      last_ok_at: null,
      last_error: 'blocker returned HTTP 502',
    },
  })
  postMock.mockImplementation(((path: string) =>
    Promise.resolve({ data: path === '/api/admin/content-security/publish' ? PUBLISHED : FULL_RESPONSE })) as never)
  const user = userEvent.setup()
  renderTab()

  const alert = await screen.findByRole('alert')
  expect(alert).toHaveTextContent('Not yet live')
  expect(alert).toHaveTextContent('HTTP 502')
  await user.click(within(alert).getByRole('button', { name: 'Retry' }))
  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/publish'))
})

test('the Blocked card lists the policy rules; values show a mark and offer Unblock', async () => {
  routeGets({
    '/api/admin/content-security/effective': effectiveWith(
      makePolicy({ country_regex: '^(CN|RU)$' }, { country: [{ value: 'US', action: 'block', kbps: 0 }] }),
    ),
  })
  vi.mocked(client.DELETE).mockResolvedValue({ data: { policy: makePolicy(), publish: PUBLISHED } } as never)
  const user = userEvent.setup()
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('United States (US)')).toBeInTheDocument()
  // The regex is shown, but only editable from the policy.
  expect(card.getByText('^(CN|RU)$')).toBeInTheDocument()

  const [, usRow] = cardByTitle('Countries (Top 10)').getAllByRole('row')
  expect(within(usRow).getByTitle('Blocked')).toBeInTheDocument()
  await user.click(within(usRow).getByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Unblock')).toBeInTheDocument()
  expect(screen.queryByText('Block, throttle or allow…')).not.toBeInTheDocument()
  await user.keyboard('{Escape}')

  await user.click(card.getByRole('button', { name: 'Unblock Country US' }))
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/policies/{policy_id}/rules', {
      params: { path: { policy_id: 'pol-1' }, query: { category: 'country', value: 'US' } },
    }),
  )
})

test('a throttle is listed with its rate and offers Remove throttle', async () => {
  routeGets({
    '/api/admin/content-security/effective': effectiveWith(makePolicy({}, { country: [{ value: 'US', action: 'pace', kbps: 512 }] })),
  })
  vi.mocked(client.DELETE).mockResolvedValue({ data: { policy: makePolicy(), publish: PUBLISHED } } as never)
  const user = userEvent.setup()
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('Throttle to 512 kbps')).toBeInTheDocument()

  // The leaderboard value carries a throttle mark, not a block mark.
  const [, usRow] = cardByTitle('Countries (Top 10)').getAllByRole('row')
  expect(within(usRow).getByTitle('Throttled to 512 kbps')).toBeInTheDocument()
  expect(within(usRow).queryByTitle('Blocked')).not.toBeInTheDocument()
  await user.click(within(usRow).getByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Remove throttle')).toBeInTheDocument()
  await user.keyboard('{Escape}')

  await user.click(card.getByRole('button', { name: 'Remove throttle Country US' }))
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/policies/{policy_id}/rules', {
      params: { path: { policy_id: 'pol-1' }, query: { category: 'country', value: 'US' } },
    }),
  )
})

test('an ASN rule shows the owner name and number, or AS<number> when it has no name', async () => {
  routeGets({
    '/api/admin/content-security/effective': effectiveWith(
      makePolicy(
        {},
        {
          asn: [
            { value: '7922', action: 'block', kbps: 0, label: 'Comcast Cable (7922)' },
            { value: '64500', action: 'block', kbps: 0 },
          ],
        },
      ),
    ),
  })
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('Comcast Cable (7922)')).toBeInTheDocument()
  expect(card.getByText('AS64500')).toBeInTheDocument()
})

test('an allow rule is marked on its value and can be removed', async () => {
  routeGets({
    '/api/admin/content-security/effective': effectiveWith(makePolicy({}, { country: [{ value: 'US', action: 'exempt', kbps: 0 }] })),
  })
  const user = userEvent.setup()
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('Allow', { selector: 'span:not(.sr-only)' })).toBeInTheDocument()
  const [, usRow] = cardByTitle('Countries (Top 10)').getAllByRole('row')
  expect(within(usRow).getByTitle('Allowed')).toBeInTheDocument()
  expect(card.getByRole('button', { name: 'Remove allow Country US' })).toBeInTheDocument()
  await user.click(within(usRow).getByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Remove allow')).toBeInTheDocument()
})

test('individual blocks in the policy mark subscriber rows', async () => {
  routeGets({
    '/api/admin/content-security/effective': effectiveWith(
      makePolicy({ individual: { subscriber_id: ['acct-home'], ip: [] } }),
    ),
  })
  renderTab()

  await screen.findByText('acct-shared')
  await waitFor(() => expect(within(subscriberCard()).getByTitle('Blocked')).toBeInTheDocument())
})

test('without a blocklist endpoint the dialog explains and cannot confirm', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(null, { configured: false }) })
  const user = userEvent.setup()
  renderTab()

  expect(await screen.findByText(/Blocking and throttling are off/)).toBeInTheDocument()
  const dialog = await openBlockMenu(user, 'US')
  expect(within(dialog).getByText(/No blocklist endpoint is configured/)).toBeInTheDocument()
  expect(within(dialog).getByRole('button', { name: 'Block' })).toBeDisabled()
})

test('analysts get no block actions and never query policies', async () => {
  vi.mocked(useIsAnalyst).mockReturnValue(true)
  // The backend always masks subscriber ids for analysts.
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, subscribers_masked: true } } as never)
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Copy value')).toBeInTheDocument()
  expect(screen.queryByText('Block, throttle or allow…')).not.toBeInTheDocument()
  expect(screen.queryByText((_, e) => e?.getAttribute('data-slot') === 'card-title' && e.textContent === 'Blocked & Throttled')).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /^Actions for/ })).not.toBeInTheDocument()
  expect(getMock).not.toHaveBeenCalled()
})

test('the old flat blocklist endpoints are never called', async () => {
  routeGets({ '/api/admin/content-security/effective': effectiveWith(makePolicy()) })
  renderTab()
  await screen.findByText('pirate.example')
  const paths = [...getMock.mock.calls, ...postMock.mock.calls, ...vi.mocked(client.DELETE).mock.calls].map((c) => String(c[0]))
  expect(paths.filter((p) => p.startsWith('/api/admin/content-security/blocks'))).toEqual([])
})

// ── Enforcement by policy ────────────────────────────────────────────────────

const REASONS = [
  { policy_id: 'pol-1', reason: 'country_regex', status: 'blocked', requests: 30, enforced: 20 },
  { policy_id: 'pol-1', reason: 'exempt:asn', status: 'allowed', requests: 100, enforced: 0 },
  { policy_id: 'pol-gone', reason: 'subscriber_id', status: 'blocked', requests: 5, enforced: 0 },
]

test('Enforcement breaks down by policy and rule, counting only enforced requests as enforced', async () => {
  routeGets({
    '/api/admin/content-security/policies': { configured: true, policies: [makePolicy()] },
  })
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, enforcement_reasons: REASONS } } as never)
  renderTab()

  const table = await screen.findByRole('table', { name: 'Enforcement by policy and rule' })
  await within(table).findAllByText('Playoffs')
  const rows = within(table).getAllByRole('row').slice(1)
  const text = rows.map((r) => within(r).getAllByRole('cell').map((c) => c.textContent))
  // Sorted by policy name then requests; an unknown policy falls back to its id.
  expect(text).toEqual([
    ['Playoffs', 'Allowed: ASN', 'Allowed', '100', '—', '—'],
    ['Playoffs', 'Country regex', 'Blocked', '30', '20', '10'],
    ['pol-gone', 'Subscriber ID', 'Blocked', '5', '0', '5'],
  ])
})

test('the by-policy breakdown is omitted when the backend sends no reasons', async () => {
  routeGets()
  renderTab()
  await screen.findByText('pirate.example')
  expect(screen.queryByRole('table', { name: 'Enforcement by policy and rule' })).not.toBeInTheDocument()
})

test('Enforcement splits enforced from warned requests, most severe first', async () => {
  renderTab()
  await screen.findByText('pirate.example')

  const table = (await screen.findByText('Rate limited', { selector: 'table *' })).closest('table')!
  const rows = within(table).getAllByRole('row').slice(1)
  expect(rows.map((r) => within(r).getAllByRole('cell')[0].textContent)).toEqual(['Blocked', 'Rate limited', 'Allowed'])

  // blocked: 40 requests, 25 enforced (ENFORCE), the other 15 were WARN-only.
  const blocked = within(rows[0]).getAllByRole('cell').map((c) => c.textContent)
  expect(blocked.slice(1, 4)).toEqual(['40', '25', '15'])
  // Outcomes that don't stop a request have nothing to enforce.
  const allowed = within(rows[2]).getAllByRole('cell').map((c) => c.textContent)
  expect(allowed.slice(1, 4)).toEqual(['900', '—', '—'])
})

test('Enforcement chart stacks one zero-filled series per outcome', async () => {
  renderTab()
  await screen.findByText('pirate.example')

  const enforcement = screen.getByTestId('enforcement-chart')
  expect(within(enforcement).getByText('Blocked: 10,0')).toBeInTheDocument()
  expect(within(enforcement).getByText('Rate limited: 0,12')).toBeInTheDocument()
  expect(within(enforcement).getByText('Allowed: 500,400')).toBeInTheDocument()
})

test('Enforcement says which Log Settings option is missing', async () => {
  postMock.mockResolvedValue({
    data: { ...FULL_RESPONSE, fields: { ...FULL_RESPONSE.fields, cs_status: false }, enforcement: [], enforcement_ts: [] },
  } as never)
  renderTab()
  await screen.findByText('pirate.example')
  expect(screen.getByText(/Log enforcement outcome option to be enabled/)).toBeInTheDocument()
})


test('coarsenEnforcement picks a round bucket for about 48 bars and sums into it', () => {
  const ts = [
    { bucket: '2026-01-01 09:05:00', status: 'blocked', requests: 2 },
    { bucket: '2026-01-01 09:10:00', status: 'blocked', requests: 3 },
    { bucket: '2026-01-01 09:10:00', status: 'allowed', requests: 7 },
    { bucket: '2026-01-01 11:30:00', status: 'blocked', requests: 4 },
  ]
  // 24 h of 5-minute buckets -> 30-minute bars (86400 / 48 = 1800).
  const day = coarsenEnforcement(ts, 300, '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')
  expect(day.bucketSeconds).toBe(1800)
  const at = (iso: string, status: string) =>
    day.points.find((p) => p.bucketMs === Date.parse(iso) && p.status === status)?.requests
  expect(at('2026-01-01T09:00:00Z', 'blocked')).toBe(5)
  expect(at('2026-01-01T09:00:00Z', 'allowed')).toBe(7)
  expect(at('2026-01-01T11:30:00Z', 'blocked')).toBe(4)

  // A short range keeps the page's own bucket; it never goes finer than that.
  expect(coarsenEnforcement(ts, 300, '2026-01-01T09:00:00Z', '2026-01-01T10:00:00Z').bucketSeconds).toBe(300)
  // An open range is sized from the data instead.
  expect(coarsenEnforcement(ts, 300).bucketSeconds).toBe(300)
  // 7 days -> round 4-hour bars.
  expect(coarsenEnforcement(ts, 300, '2026-01-01T00:00:00Z', '2026-01-08T00:00:00Z').bucketSeconds).toBe(14400)
})
