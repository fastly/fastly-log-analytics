import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import React from 'react'
import SecurityTab from '@/app/streaming/_sections/tabs/SecurityTab'
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
    <div data-testid="bandwidth-chart">
      {data.map((t) => (
        <span key={t.name}>{`${t.name}: ${t.y.join(',')}`}</span>
      ))}
    </div>
  ),
}))

const FULL_RESPONSE = {
  available: true,
  fields: { country: true, referer: true, host: true, resp_bytes: true, edge: true, cs_content_id: true, ip: true, cs_sub_id: true, p_type: true, p_desc: true },
  content_id: null,
  has_shield_split: true,
  content_ids: [
    { content_id: 'movie-1', requests: 3 },
    { content_id: 'movie-2', requests: 2 },
  ],
  top_countries: [{ value: 'US', users: 2, requests: 3, bytes: 3072 }],
  top_referers: [{ value: 'https://pirate.example/watch', users: 2, requests: 3, bytes: 3072 }],
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
  getMock.mockResolvedValue({ data: { configured: true, blocks: [] } } as never)
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
  expect(screen.getByText('https://pirate.example/watch')).toBeInTheDocument()
  expect(screen.getByText(/United States \(US\)/)).toBeInTheDocument()
  // Card titles follow the Overview page's "Name (Top 10)" style.
  for (const name of ['Countries', 'Referers', 'Hosts']) {
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
  const title = screen.getByText((_, el) => el?.getAttribute('data-slot') === 'card-title' && el.textContent === 'Subscriber IDs by Distinct IPs (Top 10)')
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
    ['https://pirate.example/watch', `filter_referer=${encodeURIComponent('https://pirate.example/watch')}`],
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

// ── Blocklist (admin-only) ───────────────────────────────────────────────────

const BLOCK = {
  id: 'blk-1',
  conditions: [{ category: 'country', value: 'US' }],
  kbps: 0,
  created_at: '2026-01-01T00:59:00Z',
  created_by: 'admin',
  unblocked_at: null,
  unblocked_by: null,
}

/** Route POSTs: the block endpoint echoes a stored block; everything else is the tab's data. */
function routePosts(extra: Record<string, unknown> = {}) {
  postMock.mockImplementation(((path: string, init: { body: { conditions: unknown[] } }) => {
    if (path === '/api/admin/content-security/blocks') {
      return Promise.resolve({ data: { ...BLOCK, conditions: init.body.conditions } })
    }
    if (path in extra) return Promise.resolve({ data: extra[path] })
    return Promise.resolve({ data: FULL_RESPONSE })
  }) as never)
}

function blockCalls() {
  return postMock.mock.calls
    .filter((c) => c[0] === '/api/admin/content-security/blocks')
    .map((c) => (c[1] as unknown as { body: unknown }).body)
}

test('Block from a leaderboard menu confirms the clicked value, then offers Undo', async () => {
  routePosts()
  vi.mocked(client.DELETE).mockResolvedValue({ data: { ...BLOCK, unblocked_at: '2026-01-01T01:00:00Z' } } as never)
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for US' }))
  await user.click(await screen.findByText('Block or throttle…'))

  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText('Block or throttle access')).toBeInTheDocument()
  // One category in a leaderboard: no category picker, the clicked value preselected.
  expect(within(dialog).queryByText('Category')).not.toBeInTheDocument()
  expect(within(dialog).getByRole('combobox', { name: 'Country' })).toHaveTextContent('United States (US)')

  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(blockCalls()).toEqual([{ conditions: [{ category: 'country', value: 'US' }], kbps: 0 }]))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(showToastWithAction).toHaveBeenCalledWith(
    'Blocked Country US',
    expect.objectContaining({ actionLabel: 'Undo' }),
  )

  // The toast's Undo reverts via DELETE.
  vi.mocked(showToastWithAction).mock.calls[0][1].onAction()
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/blocks/{block_id}', {
      params: { path: { block_id: 'blk-1' } },
    }),
  )
})

test('Block dialog lets the user pick a different value from the list', async () => {
  routePosts()
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for hosting' }))
  await user.click(await screen.findByText('Block or throttle…'))
  const dialog = await screen.findByRole('dialog')

  await user.click(within(dialog).getByRole('combobox', { name: 'Proxy type' }))
  await user.click(await screen.findByRole('option', { name: 'anonymous' }))
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(blockCalls()).toEqual([{ conditions: [{ category: 'proxy_type', value: 'anonymous' }], kbps: 0 }]))
})

test('Block dialog accepts a custom value', async () => {
  routePosts()
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for pirate.example' }))
  await user.click(await screen.findByText('Block or throttle…'))
  const dialog = await screen.findByRole('dialog')

  await user.click(within(dialog).getByRole('combobox', { name: 'Host' }))
  await user.click(await screen.findByRole('option', { name: 'Custom value…' }))
  const blockButton = within(dialog).getByRole('button', { name: 'Block' })
  expect(blockButton).toBeDisabled()
  await user.type(within(dialog).getByRole('textbox', { name: 'Custom host' }), 'mirror.example')
  await user.click(blockButton)

  await waitFor(() => expect(blockCalls()).toEqual([{ conditions: [{ category: 'host', value: 'mirror.example' }], kbps: 0 }]))
})

test('a failed block keeps the dialog open with the error', async () => {
  postMock.mockImplementation(((path: string) =>
    path === '/api/admin/content-security/blocks'
      ? Promise.reject(new Error('Blocklist endpoint returned HTTP 500'))
      : Promise.resolve({ data: FULL_RESPONSE })) as never)
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for vpn' }))
  await user.click(await screen.findByText('Block or throttle…'))
  const dialog = await screen.findByRole('dialog')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  expect(await within(dialog).findByRole('alert')).toHaveTextContent('HTTP 500')
  expect(showToastWithAction).not.toHaveBeenCalled()
})

test('subscriber rows get an actions menu with Block', async () => {
  routePosts()
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Actions for acct-home' }))
  expect(await screen.findByText('Show IP addresses')).toBeInTheDocument()
  await user.click(screen.getByText('Block or throttle…'))

  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByRole('combobox', { name: 'Subscriber ID' })).toHaveTextContent('acct-home')
  await user.click(within(dialog).getByRole('button', { name: 'Block' }))

  await waitFor(() => expect(blockCalls()).toEqual([{ conditions: [{ category: 'subscriber_id', value: 'acct-home' }], kbps: 0 }]))
})

test('the subscriber dialog Block button offers the subscriber ID or any of its IPs', async () => {
  routePosts({ '/api/cmcd/content-security/subscriber': SUBSCRIBER_IPS })
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Show IP addresses for acct-shared' }))
  const ipsDialog = await screen.findByRole('dialog')
  await within(ipsDialog).findByText('Germany (DE)')
  // Toolbar summary: distinct IPs (203.0.113.3 appears twice) and distinct non-null countries.
  expect(within(ipsDialog).getByText('3 IP addresses · 3 countries')).toBeInTheDocument()
  await user.click(within(ipsDialog).getByRole('button', { name: /Block/ }))

  const dialogs = await screen.findAllByRole('dialog')
  const dialog = dialogs.find((d) => within(d).queryByText('Block or throttle access')) as HTMLElement
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

  await waitFor(() => expect(blockCalls()).toEqual([{ conditions: [{ category: 'ip', value: '203.0.113.2' }], kbps: 0 }]))
})

test('Blocked card lists active blocks; values show a mark and offer Unblock', async () => {
  getMock.mockResolvedValue({ data: { configured: true, blocks: [BLOCK] } } as never)
  vi.mocked(client.DELETE).mockResolvedValue({ data: { ...BLOCK, unblocked_at: '2026-01-01T01:00:00Z' } } as never)
  const user = userEvent.setup()
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('United States (US)')).toBeInTheDocument()
  expect(card.getByText('2026-01-01 00:59:00')).toBeInTheDocument()

  const [, usRow] = cardByTitle('Countries (Top 10)').getAllByRole('row')
  expect(within(usRow).getByTitle('Blocked')).toBeInTheDocument()
  await user.click(within(usRow).getByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Unblock')).toBeInTheDocument()
  expect(screen.queryByText('Block or throttle…')).not.toBeInTheDocument()
  await user.keyboard('{Escape}')

  await user.click(card.getByRole('button', { name: 'Unblock Country US' }))
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/blocks/{block_id}', {
      params: { path: { block_id: 'blk-1' } },
    }),
  )
})

test('without a blocklist endpoint the dialog explains and cannot confirm', async () => {
  getMock.mockResolvedValue({ data: { configured: false, blocks: [] } } as never)
  const user = userEvent.setup()
  renderTab()

  expect(await screen.findByText(/Blocking and throttling are off/)).toBeInTheDocument()
  await user.click(await screen.findByRole('button', { name: 'Filter actions for US' }))
  await user.click(await screen.findByText('Block or throttle…'))
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText(/No blocklist endpoint is configured/)).toBeInTheDocument()
  expect(within(dialog).getByRole('button', { name: 'Block' })).toBeDisabled()
})

test('analysts get no block actions and never query the blocklist', async () => {
  vi.mocked(useIsAnalyst).mockReturnValue(true)
  // The backend always masks subscriber ids for analysts.
  postMock.mockResolvedValue({ data: { ...FULL_RESPONSE, subscribers_masked: true } } as never)
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for US' }))
  expect(await screen.findByText('Copy value')).toBeInTheDocument()
  expect(screen.queryByText('Block or throttle…')).not.toBeInTheDocument()
  expect(screen.queryByText((_, e) => e?.getAttribute('data-slot') === 'card-title' && e.textContent === 'Blocked & Throttled')).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /^Actions for/ })).not.toBeInTheDocument()
  expect(getMock).not.toHaveBeenCalledWith('/api/admin/content-security/blocks', expect.anything())
})

test('a multi-condition (AND) block is listed with every condition but marks no single value', async () => {
  const andBlock = {
    ...BLOCK,
    id: 'blk-and',
    conditions: [
      { category: 'country', value: 'US' },
      { category: 'proxy_type', value: 'hosting' },
    ],
  }
  getMock.mockResolvedValue({ data: { configured: true, blocks: [andBlock] } } as never)
  vi.mocked(client.DELETE).mockResolvedValue({ data: { ...andBlock, unblocked_at: '2026-01-01T01:00:00Z' } } as never)
  const user = userEvent.setup()
  renderTab()

  const card = await waitFor(() => cardByTitle('Blocked & Throttled'))
  expect(await card.findByText('United States (US)')).toBeInTheDocument()
  expect(card.getByText('and')).toBeInTheDocument()
  expect(card.getByText('Proxy type')).toBeInTheDocument()

  // Neither value is blocked on its own, so the leaderboards still offer Block.
  const [, usRow] = cardByTitle('Countries (Top 10)').getAllByRole('row')
  expect(within(usRow).queryByTitle('Blocked')).not.toBeInTheDocument()

  await user.click(card.getByRole('button', { name: 'Unblock Country US and Proxy type hosting' }))
  await waitFor(() =>
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/blocks/{block_id}', {
      params: { path: { block_id: 'blk-and' } },
    }),
  )
})

test('the dialog can throttle instead of block, and the list shows the rate', async () => {
  routePosts()
  postMock.mockImplementation(((path: string, init: { body: { conditions: unknown[]; kbps: number } }) =>
    Promise.resolve({
      data:
        path === '/api/admin/content-security/blocks'
          ? { ...BLOCK, conditions: init.body.conditions, kbps: init.body.kbps }
          : FULL_RESPONSE,
    })) as never)
  const user = userEvent.setup()
  renderTab()

  await user.click(await screen.findByRole('button', { name: 'Filter actions for US' }))
  await user.click(await screen.findByText('Block or throttle…'))
  const dialog = await screen.findByRole('dialog')

  const action = within(dialog).getByRole('combobox', { name: 'Action' })
  expect(action).toHaveTextContent('Block (no traffic allowed)')
  await user.click(action)
  const labels = (await screen.findAllByRole('option')).map((o) => o.textContent)
  expect(labels).toEqual([
    'Block (no traffic allowed)',
    'Throttle to: 128 kbps',
    'Throttle to: 256 kbps',
    'Throttle to: 512 kbps',
  ])
  await user.click(screen.getByRole('option', { name: 'Throttle to: 256 kbps' }))
  await user.click(within(dialog).getByRole('button', { name: 'Throttle' }))

  await waitFor(() =>
    expect(blockCalls()).toEqual([{ conditions: [{ category: 'country', value: 'US' }], kbps: 256 }]),
  )
  expect(showToastWithAction).toHaveBeenCalledWith(
    'Throttled Country US to 256 kbps',
    expect.objectContaining({ actionLabel: 'Undo' }),
  )
})

test('a throttle is listed with its rate and offers Remove throttle', async () => {
  const throttle = { ...BLOCK, id: 'thr-1', kbps: 512 }
  getMock.mockResolvedValue({ data: { configured: true, blocks: [throttle] } } as never)
  vi.mocked(client.DELETE).mockResolvedValue({ data: { ...throttle, unblocked_at: '2026-01-01T01:00:00Z' } } as never)
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
    expect(client.DELETE).toHaveBeenCalledWith('/api/admin/content-security/blocks/{block_id}', {
      params: { path: { block_id: 'thr-1' } },
    }),
  )
})
