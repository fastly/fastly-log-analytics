import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach, afterEach } from 'vitest'
import { QueryClientProvider } from '@tanstack/react-query'
import { NuqsTestingAdapter, type UrlUpdateEvent } from 'nuqs/adapters/testing'
import React from 'react'
import PoliciesView from '@/app/streaming/_sections/tabs/PoliciesView'
import { client } from '@/lib/api'
import { showToast, showToastWithAction } from '@/lib/toast'
import { createTestQueryClient } from '../../helpers/query'
import { spyOnConsoleError } from '../../helpers/page-smoke'

vi.mock('@/stores/serviceStore', async () => (await import('../../helpers/page-smoke')).serviceStoreModuleMock())
vi.mock('@/stores/filterStore', async () => (await import('../../helpers/page-smoke')).filterStoreModuleMock())
vi.mock('next/navigation', async () => (await import('../../helpers/page-smoke')).navigationModuleMock('/streaming'))
vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: vi.fn(), PUT: vi.fn(), DELETE: vi.fn(), use: vi.fn() },
  extractApiError: vi.fn((e) => (e instanceof Error ? e.message : String(e))),
  getApiBase: vi.fn(() => 'http://test'),
}))
vi.mock('@/lib/toast', () => ({ showToast: vi.fn(), showToastWithAction: vi.fn() }))

const getMock = vi.mocked(client.GET)
const postMock = vi.mocked(client.POST)
const putMock = vi.mocked(client.PUT)
const deleteMock = vi.mocked(client.DELETE)
const queryClient = createTestQueryClient({ queries: { staleTime: 0 } })
let errorSpy: ReturnType<typeof spyOnConsoleError>

const EMPTY_RULES = { country: [], asn: [], referer: [], host: [], proxy_type: [], proxy_description: [] }

function makePolicy(overrides: Record<string, unknown> = {}, rules: Record<string, unknown[]> = {}) {
  return {
    id: 'pol-1',
    name: 'Playoffs',
    country_regex: '^(CN|RU)$',
    rev: 3,
    rules: { ...EMPTY_RULES, referer: [{ value: 'pirate.example', action: 'block', kbps: 0 }], ...rules },
    individual: { subscriber_id: ['acct-1'], ip: [] },
    created_at: '2026-10-01T00:00:00Z',
    created_by: 'admin',
    updated_at: '2026-10-14T12:00:00Z',
    updated_by: 'admin',
    used_by: [
      { assignment_id: 'asg-1', scope_type: 'content_id', content_id: 'ev-123', mode: 'WARN', status: 'scheduled' },
      { assignment_id: 'asg-2', scope_type: 'content_id', content_id: 'ev-099', mode: 'ENFORCE', status: 'ended' },
    ],
    ...overrides,
  }
}

const DEFAULT_POLICY = makePolicy(
  { id: 'pol-def', name: 'Default', country_regex: null, individual: { subscriber_id: [], ip: [] }, used_by: [{ assignment_id: 'asg-0', scope_type: 'default', content_id: null, mode: 'ENFORCE', status: 'active' }] },
  { referer: [], country: [{ value: 'BR', action: 'pace', kbps: 256 }, { value: 'US', action: 'exempt', kbps: 0 }], asn: [{ value: '7922', action: 'block', kbps: 0 }] },
)

const ASSIGNMENTS = [
  { id: 'asg-1', policy_id: 'pol-1', policy_name: 'Playoffs', scope_type: 'content_id', content_id: 'ev-123', mode: 'WARN', starts_at: '2026-10-20T18:00:00Z', ends_at: '2026-10-21T02:00:00Z', status: 'scheduled', created_at: '2026-10-01T00:00:00Z', created_by: 'admin' },
  { id: 'asg-0', policy_id: 'pol-def', policy_name: 'Default', scope_type: 'default', content_id: null, mode: 'ENFORCE', starts_at: null, ends_at: null, status: 'active', created_at: '2026-10-01T00:00:00Z', created_by: 'admin' },
  { id: 'asg-2', policy_id: 'pol-1', policy_name: 'Playoffs', scope_type: 'content_id', content_id: 'ev-099', mode: 'ENFORCE', starts_at: '2026-10-12T18:00:00Z', ends_at: '2026-10-13T02:00:00Z', status: 'ended', created_at: '2026-10-01T00:00:00Z', created_by: 'admin' },
]

const IN_SYNC = { configured: true, rev: 4, published_rev: 4, in_sync: true, last_ok_at: '2026-10-14T12:00:00Z', last_error: null }
const PUBLISHED = { status: 'published', rev: 5, error: null }

const OBSERVED = {
  available: true,
  fields: {},
  content_ids: [
    { content_id: 'ev-123', requests: 900 },
    { content_id: 'ev-555', requests: 12 },
  ],
  top_countries: [{ value: 'DE', requests: 3 }],
  top_asns: [{ value: '7922', label: 'Comcast Cable (7922)', requests: 3 }],
  top_referers: [{ value: 'referring.example', requests: 3 }],
  top_hosts: [],
  proxy_types: [],
  proxy_descriptions: [],
}

let gets: Record<string, unknown>

function routeAll() {
  gets = {
    '/api/admin/content-security/policies': { configured: true, policies: [makePolicy(), DEFAULT_POLICY] },
    '/api/admin/content-security/assignments': { configured: true, assignments: ASSIGNMENTS },
    '/api/admin/content-security/sync-status': IN_SYNC,
    '/api/admin/content-security/country-regex/preview': { valid: true, error: null, matches: ['CN', 'RU'] },
    '/api/admin/content-security/policies/{policy_id}/revisions': {
      revisions: [
        { rev: 3, changed_by: 'steve', changed_at: '2026-10-14T12:00:00Z', policy: makePolicy() },
        { rev: 2, changed_by: 'admin', changed_at: '2026-10-13T09:00:00Z', policy: makePolicy({ country_regex: null }) },
      ],
    },
  }
  getMock.mockImplementation(((path: string) => Promise.resolve({ data: gets[path] })) as never)
  postMock.mockImplementation(((path: string) => {
    if (path === '/api/cmcd/content-security') return Promise.resolve({ data: OBSERVED })
    if (path === '/api/admin/content-security/publish') return Promise.resolve({ data: PUBLISHED })
    if (path === '/api/admin/content-security/policies') return Promise.resolve({ data: { policy: makePolicy({ id: 'pol-new', name: 'Euro cup' }), publish: PUBLISHED } })
    if (path === '/api/admin/content-security/assignments') return Promise.resolve({ data: { assignment: ASSIGNMENTS[0], publish: PUBLISHED } })
    return Promise.resolve({ data: {} })
  }) as never)
  putMock.mockImplementation((() => Promise.resolve({ data: { policy: makePolicy(), publish: PUBLISHED, assignment: ASSIGNMENTS[0] } })) as never)
  deleteMock.mockImplementation((() => Promise.resolve({ data: { policy: makePolicy(), assignment: ASSIGNMENTS[0], publish: PUBLISHED } })) as never)
}

beforeEach(() => {
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} })
  queryClient.clear()
  getMock.mockReset()
  postMock.mockReset()
  putMock.mockReset()
  deleteMock.mockReset()
  vi.mocked(showToast).mockReset()
  vi.mocked(showToastWithAction).mockReset()
  routeAll()
  errorSpy = spyOnConsoleError()
})

afterEach(() => {
  vi.unstubAllGlobals()
  errorSpy.mockRestore()
})

function renderView(searchParams = '', onUrlUpdate?: (e: UrlUpdateEvent) => void) {
  return render(
    <NuqsTestingAdapter searchParams={searchParams} onUrlUpdate={onUrlUpdate} hasMemory>
      <QueryClientProvider client={queryClient}>
        <PoliciesView
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

const cells = (row: HTMLElement) => within(row).getAllByRole('cell').map((c) => c.textContent)

// ── Tables ───────────────────────────────────────────────────────────────────

test('lists the policy schedule (all-content first) with scope, policy, mode, window and status', async () => {
  renderView()
  const table = await screen.findByRole('table', { name: 'Policy schedule' })
  const rows = within(table).getAllByRole('row').slice(1)
  expect(rows.map(cells)).toEqual([
    ['All content (default)', 'Default', 'Enforce', '(always)', '(none)', 'Active', ''],
    ['Content: ev-123', 'Playoffs', 'Test only', '2026-10-20 18:00 UTC', '2026-10-21 02:00 UTC', 'Scheduled', ''],
    ['Content: ev-099', 'Playoffs', 'Enforce', '2026-10-12 18:00 UTC', '2026-10-13 02:00 UTC', 'Ended', ''],
  ])
})

test('lists policies with a rules summary and where each is assigned', async () => {
  renderView()
  const table = await screen.findByRole('table', { name: 'Policies' })
  const rows = within(table).getAllByRole('row').slice(1)
  expect(rows.map(cells)).toEqual([
    ['Default', '1 country, 1 ASN, 1 allowed', 'Default', '2026-10-14 12:00', ''],
    ['Playoffs', 'regex ^(CN|RU)$, 1 referer, 1 subscriber', 'ev-123, ev-099', '2026-10-14 12:00', ''],
  ])
})

test('says what to do when there is nothing yet', async () => {
  gets['/api/admin/content-security/policies'] = { configured: true, policies: [] }
  gets['/api/admin/content-security/assignments'] = { configured: true, assignments: [] }
  renderView()
  expect(await screen.findByText(/Nothing is scheduled/)).toBeInTheDocument()
  expect(screen.getByText(/No policies yet/)).toBeInTheDocument()
  // Nothing to assign until a policy exists.
  expect(screen.getByRole('button', { name: 'Schedule a policy' })).toBeDisabled()
})

// ── Sync state ───────────────────────────────────────────────────────────────

test('shows the live state, and Sync now publishes', async () => {
  const user = userEvent.setup()
  renderView()
  const status = await screen.findByText(/Live: the blocker has revision 4/)
  expect(status).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: 'Sync now' }))
  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/publish'))
  await waitFor(() => expect(showToast).toHaveBeenCalledWith('Synced with the blocker. Changes are live.', 'success'))
})

test('Not yet live: a banner with Retry, and a chip on assignments that are not ended', async () => {
  gets['/api/admin/content-security/sync-status'] = { configured: true, rev: 5, published_rev: 4, in_sync: false, last_ok_at: null, last_error: 'blocker returned HTTP 502' }
  const user = userEvent.setup()
  renderView()

  const alert = await screen.findByRole('alert')
  expect(alert).toHaveTextContent('Not yet live')
  expect(alert).toHaveTextContent('Saved revision 5')
  expect(alert).toHaveTextContent('HTTP 502')
  const table = screen.getByRole('table', { name: 'Policy schedule' })
  expect(within(table).getAllByText('Not yet live')).toHaveLength(2) // active + scheduled, not ended

  postMock.mockImplementation((() => Promise.resolve({ data: { status: 'failed', rev: 5, error: 'still down' } })) as never)
  await user.click(within(alert).getByRole('button', { name: 'Retry' }))
  await waitFor(() => expect(showToast).toHaveBeenCalledWith('Still not live: still down', 'warn'))
})

test('with no blocklist endpoint it points at Admin settings', async () => {
  gets['/api/admin/content-security/sync-status'] = { configured: false, rev: 1, published_rev: 0, in_sync: false, last_ok_at: null, last_error: null }
  renderView()
  expect(await screen.findByText(/No blocklist endpoint is configured/)).toBeInTheDocument()
  expect(screen.getByRole('link', { name: 'Admin → Overall Settings' })).toHaveAttribute('href', expect.stringContaining('/admin'))
  expect(screen.queryByText('Not yet live')).not.toBeInTheDocument()
})

// ── Policy editor ────────────────────────────────────────────────────────────

test('New policy opens the editor and deep-links with ?policy=new', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  const user = userEvent.setup()
  renderView('', onUrlUpdate)
  await user.click(await screen.findByRole('button', { name: 'New policy' }))
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '?policy=new' }))
  expect(await screen.findByRole('dialog', { name: 'New policy' })).toBeInTheDocument()
})

test('?policy=<id> opens that policy in the editor', async () => {
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog', { name: 'Edit policy: Playoffs' })
  expect(within(dialog).getByLabelText('Name')).toHaveValue('Playoffs')
  expect(within(dialog).getByLabelText('Mass-block countries by regex')).toHaveValue('^(CN|RU)$')
})

test('the regex preview is debounced and lists the matched countries', async () => {
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getByText(/Optional\. Blocks every country/)).toBeInTheDocument()

  await user.type(within(dialog).getByLabelText('Mass-block countries by regex'), '^(CN|RU)$')
  expect(await within(dialog).findByText(/Matches 2 countries/)).toBeInTheDocument()
  expect(within(dialog).getByText('CN')).toBeInTheDocument()
  expect(within(dialog).getByText('RU')).toBeInTheDocument()
  // One request for the finished pattern, not one per keystroke.
  const previews = getMock.mock.calls.filter((c) => c[0] === '/api/admin/content-security/country-regex/preview')
  expect(previews).toHaveLength(1)
  expect(previews[0][1]).toMatchObject({ params: { query: { pattern: '^(CN|RU)$' } } })
})

test('an invalid regex shows the server error and the policy can still be edited', async () => {
  gets['/api/admin/content-security/country-regex/preview'] = { valid: false, error: 'nested quantifiers are not allowed', matches: [] }
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')
  await user.type(within(dialog).getByLabelText('Mass-block countries by regex'), '(a+)+')
  expect(await within(dialog).findByText(/Invalid pattern: nested quantifiers are not allowed/)).toBeInTheDocument()
})

/** Open a picker, type, and use the text as-is (the "Use “…”" row). */
async function pickCustom(user: ReturnType<typeof userEvent.setup>, scope: HTMLElement, name: string, text: string) {
  await user.click(within(scope).getByRole('combobox', { name }))
  await user.type(screen.getByPlaceholderText(/^Search/), text)
  await user.click(await within(screen.getByRole('listbox')).findByText(`Use “${text}”`))
}

async function pickCountry(user: ReturnType<typeof userEvent.setup>, dialog: HTMLElement, name: string) {
  await user.click(within(dialog).getByRole('combobox', { name: 'Add country' }))
  await user.type(screen.getByPlaceholderText('Search countries…'), name)
  await user.click(await within(screen.getByRole('listbox')).findByRole('option', { name: new RegExp(name) }))
}

test('rows are value + Block / Throttle / Allow, with a 128 / 256 / 512 kbps dropdown', async () => {
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')
  await user.type(within(dialog).getByLabelText('Name'), 'Euro cup')

  // Countries tab: add BR as a pace rule.
  await pickCountry(user, dialog, 'Brazil')
  await user.selectOptions(within(dialog).getByLabelText('Action for new country'), 'pace')
  const kbps = within(dialog).getByLabelText('Throttle rate for the new value in kbps')
  expect(kbps).toHaveValue('256')
  expect([...kbps.querySelectorAll('option')].map((o) => o.textContent)).toEqual(['128 kbps', '256 kbps', '512 kbps'])
  await user.selectOptions(kbps, '512')
  await user.click(within(dialog).getByRole('button', { name: 'Add' }))
  expect(within(dialog).getByText('Brazil (BR)')).toBeInTheDocument()
  expect(within(dialog).getByLabelText('Throttle rate for BR in kbps')).toHaveValue('512')

  // The action dropdown says Allow (not Exempt); choosing it removes the rate dropdown.
  const action = within(dialog).getByLabelText('Action for BR')
  expect([...action.querySelectorAll('option')].map((o) => o.textContent)).toEqual(['Block', 'Throttle', 'Allow'])
  await user.selectOptions(action, 'exempt')
  expect(within(dialog).queryByLabelText('Throttle rate for BR in kbps')).not.toBeInTheDocument()

  // Referers tab: a block, then a tab badge counts it.
  await user.click(within(dialog).getByRole('tab', { name: /Referers/ }))
  await pickCustom(user, dialog, 'Add referer', 'Pirate.Example')
  await user.click(within(dialog).getByRole('button', { name: 'Add' }))
  expect(within(dialog).getByText('pirate.example')).toBeInTheDocument()
  expect(within(dialog).getByRole('tab', { name: /Referers/ })).toHaveTextContent('1')

  await user.click(within(dialog).getByRole('button', { name: 'Create policy' }))
  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/policies', expect.anything()))
  const body = (postMock.mock.calls.find((c) => c[0] === '/api/admin/content-security/policies')![1] as unknown as { body: Record<string, unknown> }).body
  expect(body).toEqual({
    name: 'Euro cup',
    country_regex: null,
    rules: {
      ...EMPTY_RULES,
      country: [{ value: 'BR', action: 'exempt', kbps: 0 }],
      referer: [{ value: 'pirate.example', action: 'block', kbps: 0 }],
    },
    individual: { subscriber_id: [], ip: [] },
  })
  expect(showToast).toHaveBeenCalledWith('Created policy Euro cup', 'success')
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
})

test('add-row validates ASN numbers and suggests values already seen in the Security tab', async () => {
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')

  await user.click(within(dialog).getByRole('tab', { name: /ASNs/ }))
  await pickCustom(user, dialog, 'Add ASN', 'abc')
  await user.click(within(dialog).getByRole('button', { name: 'Add' }))
  expect(await within(dialog).findByRole('alert')).toHaveTextContent('ASN number')

  // Suggestions come from the Monitor view's top lists: name on top, AS number and requests below.
  await user.click(within(dialog).getByRole('combobox', { name: 'Add ASN' }))
  expect(await within(screen.getByRole('listbox')).findByText(/^AS7922/)).toBeInTheDocument()
  await user.keyboard('{Escape}')
  await user.click(within(dialog).getByRole('tab', { name: /Referers/ }))
  await user.click(within(dialog).getByRole('combobox', { name: 'Add referer' }))
  expect(await within(screen.getByRole('listbox')).findByText('referring.example')).toBeInTheDocument()
})

test('country picker: every country A to Z, name on top, code and requests underneath, searchable', async () => {
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')
  await user.click(within(dialog).getByRole('combobox', { name: 'Add country' }))

  const options = within(await screen.findByRole('listbox')).getAllByRole('option')
  expect(options.length).toBeGreaterThanOrEqual(249)
  const names = options.map((o) => o.querySelector('.truncate')?.textContent ?? '')
  expect(names[0]).toBe('Afghanistan')
  expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b, 'en')))
  // Germany had traffic: its secondary line carries the code and the request count; others just the code.
  const germany = options[names.indexOf('Germany')]
  expect(germany).toHaveTextContent(/Germany\s*DE · 3 requests/)
  expect(options[names.indexOf('New Zealand')]).toHaveTextContent(/New Zealand\s*NZ$/)
  // The query asks for the API's maximum so the counts cover every country seen.
  expect(JSON.stringify(postMock.mock.calls.filter((c) => c[0] === '/api/cmcd/content-security'))).toContain('"top_n":100')

  // Search by name or by code, pick, and add.
  await user.type(screen.getByPlaceholderText('Search countries…'), 'zealand')
  await user.click(await within(screen.getByRole('listbox')).findByRole('option', { name: /New Zealand/ }))
  expect(within(dialog).getByRole('combobox', { name: 'Add country' })).toHaveTextContent('New Zealand (NZ)')
  await user.click(within(dialog).getByRole('button', { name: 'Add' }))
  expect(within(dialog).getByText('New Zealand (NZ)', { selector: 'span.font-mono' })).toBeInTheDocument()
})

test('ASN rows show the owner name and number', async () => {
  gets['/api/admin/content-security/policies'] = {
    configured: true,
    policies: [
      makePolicy(
        {},
        {
          asn: [
            { value: '7922', action: 'block', kbps: 0, label: 'Comcast Cable (7922)' },
            { value: '64500', action: 'block', kbps: 0 },
          ],
        },
      ),
    ],
  }
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog')
  await user.click(within(dialog).getByRole('tab', { name: /ASNs/ }))
  expect(within(dialog).getByText('Comcast Cable (7922)')).toBeInTheDocument()
  expect(within(dialog).getByText('AS64500')).toBeInTheDocument()
})

test('Individuals: subscriber IDs and IPs, one per line, block only; a bad IP stops the save', async () => {
  const user = userEvent.setup()
  renderView('?policy=new')
  const dialog = await screen.findByRole('dialog')
  await user.type(within(dialog).getByLabelText('Name'), 'Individuals')
  await user.click(within(dialog).getByRole('tab', { name: /Individuals/ }))

  expect(within(dialog).getByText(/cannot be throttled or allowed/)).toBeInTheDocument()
  // Block-only: no action control on this tab.
  expect(within(dialog).queryByLabelText(/^Action for/)).not.toBeInTheDocument()

  await user.type(within(dialog).getByLabelText('Subscriber IDs (one per line)'), 'Acct-1{Enter}Acct-1{Enter}{Enter}acct-2')
  await user.type(within(dialog).getByLabelText('IP addresses (one per line)'), '203.0.113.9{Enter}not-an-ip')
  expect(await within(dialog).findByRole('alert')).toHaveTextContent('Not an IPv4 or IPv6 address: not-an-ip')
  expect(within(dialog).getByRole('button', { name: 'Create policy' })).toBeDisabled()

  await user.clear(within(dialog).getByLabelText('IP addresses (one per line)'))
  await user.type(within(dialog).getByLabelText('IP addresses (one per line)'), '203.0.113.9{Enter}2001:db8::1')
  await user.click(within(dialog).getByRole('button', { name: 'Create policy' }))

  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/policies', expect.anything()))
  const body = (postMock.mock.calls.find((c) => c[0] === '/api/admin/content-security/policies')![1] as unknown as { body: { individual: unknown } }).body
  // Blank lines and duplicates dropped; subscriber IDs keep their case.
  expect(body.individual).toEqual({ subscriber_id: ['Acct-1', 'acct-2'], ip: ['203.0.113.9', '2001:db8::1'] })
})

test('editing a shared policy warns who it applies to, and saves in place with PUT', async () => {
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog')

  const note = within(dialog).getByRole('note')
  expect(note).toHaveTextContent('Used by 2 schedule entries')
  expect(note).toHaveTextContent('applies to all of them immediately')
  const usedBy = within(dialog).getByRole('list', { name: 'Used by' })
  expect(within(usedBy).getByText('Content: ev-123 · Test only · scheduled')).toBeInTheDocument()

  // Saving needs a change.
  expect(within(dialog).getByRole('button', { name: 'Save policy' })).toBeDisabled()
  await user.type(within(dialog).getByLabelText('Name'), ' 2')
  await user.click(within(dialog).getByRole('button', { name: 'Save policy' }))

  await waitFor(() => expect(putMock).toHaveBeenCalled())
  expect(putMock).toHaveBeenCalledWith(
    '/api/admin/content-security/policies/{policy_id}',
    expect.objectContaining({
      params: { path: { policy_id: 'pol-1' } },
      body: expect.objectContaining({ name: 'Playoffs 2', country_regex: '^(CN|RU)$' }),
    }),
  )
})

test('a save that is not yet live closes the editor and offers Retry', async () => {
  putMock.mockImplementation((() => Promise.resolve({ data: { policy: makePolicy(), publish: { status: 'failed', rev: 5, error: 'blocker returned HTTP 502' } } })) as never)
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog')
  await user.type(within(dialog).getByLabelText('Name'), '!')
  await user.click(within(dialog).getByRole('button', { name: 'Save policy' }))

  await waitFor(() => expect(showToastWithAction).toHaveBeenCalled())
  const [message, options] = vi.mocked(showToastWithAction).mock.calls[0]
  expect(message).toContain('Saved but not yet live')
  expect(message).toContain('HTTP 502')
  expect(options).toMatchObject({ actionLabel: 'Retry' })
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())

  options.onAction()
  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/publish'))
})

test('a server error (name taken) is shown inline and the editor stays open', async () => {
  putMock.mockRejectedValue(new Error('A policy named Playoffs already exists'))
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog')
  await user.type(within(dialog).getByLabelText('Name'), 'x')
  await user.click(within(dialog).getByRole('button', { name: 'Save policy' }))
  expect(await within(dialog).findByRole('alert')).toHaveTextContent('already exists')
  expect(screen.getByRole('dialog', { name: /Edit policy/ })).toBeInTheDocument()
})

test('unsaved changes are guarded: Cancel asks, Keep editing stays, Discard closes', async () => {
  const onUrlUpdate = vi.fn<(e: UrlUpdateEvent) => void>()
  const user = userEvent.setup()
  renderView('?policy=pol-1', onUrlUpdate)
  const dialog = await screen.findByRole('dialog', { name: /Edit policy/ })

  // No edits: Cancel closes straight away (covered by the next test); with an edit it asks first.
  await user.type(within(dialog).getByLabelText('Name'), 'x')
  await user.click(within(dialog).getByRole('button', { name: 'Cancel' }))
  const confirm = await screen.findByRole('alertdialog')
  expect(within(confirm).getByText('Discard unsaved changes?')).toBeInTheDocument()

  await user.click(within(confirm).getByRole('button', { name: 'Keep editing' }))
  await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
  expect(screen.getByRole('dialog', { name: /Edit policy/ })).toBeInTheDocument()
  expect(within(screen.getByRole('dialog', { name: /Edit policy/ })).getByLabelText('Name')).toHaveValue('Playoffsx')

  await user.click(within(screen.getByRole('dialog', { name: /Edit policy/ })).getByRole('button', { name: 'Cancel' }))
  await user.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Discard changes' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(onUrlUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ queryString: '' }))
  expect(putMock).not.toHaveBeenCalled()
})

test('an unedited editor closes without asking', async () => {
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog', { name: /Edit policy/ })
  await user.click(within(dialog).getByRole('button', { name: 'Cancel' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
})

test('the revision history lists who changed what, newest first', async () => {
  const user = userEvent.setup()
  renderView('?policy=pol-1')
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).queryByRole('list', { name: 'Revision history' })).not.toBeInTheDocument()

  await user.click(within(dialog).getByRole('button', { name: 'Show history' }))
  const list = await within(dialog).findByRole('list', { name: 'Revision history' })
  const items = within(list).getAllByRole('listitem').map((li) => li.textContent)
  expect(items[0]).toContain('Rev 3')
  expect(items[0]).toContain('steve')
  expect(items[0]).toContain('2026-10-14 12:00:00')
  expect(items[0]).toContain('regex ^(CN|RU)$')
  expect(items[1]).toContain('Rev 2')
  expect(getMock).toHaveBeenCalledWith(
    '/api/admin/content-security/policies/{policy_id}/revisions',
    expect.objectContaining({ params: { path: { policy_id: 'pol-1' } } }),
  )
})

// ── Assignment dialog ────────────────────────────────────────────────────────

async function openAddAssignment(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('button', { name: 'Schedule a policy' }))
  return screen.findByRole('dialog', { name: 'Apply policy to content' })
}

function assignmentBody() {
  const call = postMock.mock.calls.find((c) => c[0] === '/api/admin/content-security/assignments')
  return (call?.[1] as unknown as { body: Record<string, unknown> }).body
}

test('creates a content-ID assignment: observed IDs suggested, free text allowed, UTC sent', async () => {
  const user = userEvent.setup()
  renderView()
  const dialog = await openAddAssignment(user)

  await user.click(within(dialog).getByRole('radio', { name: 'Content ID' }))
  // Observed content IDs are listed (fetched on open) with their request counts, but any text works.
  await user.click(within(dialog).getByRole('combobox', { name: 'Content ID' }))
  expect(await within(screen.getByRole('listbox')).findByRole('option', { name: /ev-555/ })).toBeInTheDocument()
  await user.keyboard('{Escape}')
  await pickCustom(user, dialog, 'Content ID', 'ev-777')

  const starts = within(dialog).getByLabelText('Starts')
  const ends = within(dialog).getByLabelText('Ends')
  fireEvent.change(starts, { target: { value: '2026-10-20T18:00' } })
  fireEvent.change(ends, { target: { value: '2026-10-21T02:00' } })
  const startUtc = new Date('2026-10-20T18:00').toISOString()
  expect(within(dialog).getByTestId('cs-assignment-starts-utc')).toHaveTextContent(`${startUtc.slice(0, 16).replace('T', ' ')} UTC`)

  await user.click(within(dialog).getByRole('button', { name: 'Save' }))
  await waitFor(() => expect(assignmentBody()).toBeDefined())
  expect(assignmentBody()).toEqual({
    policy_id: 'pol-def',
    scope_type: 'content_id',
    content_id: 'ev-777',
    mode: 'ENFORCE',
    starts_at: startUtc,
    ends_at: new Date('2026-10-21T02:00').toISOString(),
  })
  expect(showToast).toHaveBeenCalledWith('Scheduled policy', 'success')
})

test('server errors are shown inline in the assignment dialog and it stays open', async () => {
  postMock.mockImplementation(((path: string) =>
    path === '/api/admin/content-security/assignments'
      ? Promise.reject(new Error('A default assignment already exists'))
      : Promise.resolve({ data: path === '/api/cmcd/content-security' ? OBSERVED : {} })) as never)
  const user = userEvent.setup()
  renderView()
  const dialog = await openAddAssignment(user)
  await user.click(within(dialog).getByRole('button', { name: 'Save' }))

  expect(await within(dialog).findByRole('alert')).toHaveTextContent('A default assignment already exists')
  expect(screen.getByRole('dialog', { name: 'Apply policy to content' })).toBeInTheDocument()
})

test('an overlap error names the content ID and window', async () => {
  postMock.mockImplementation(((path: string) =>
    path === '/api/admin/content-security/assignments'
      ? Promise.reject(new Error('ev-123 already has an assignment overlapping this window'))
      : Promise.resolve({ data: path === '/api/cmcd/content-security' ? OBSERVED : {} })) as never)
  const user = userEvent.setup()
  renderView()
  const dialog = await openAddAssignment(user)
  await user.click(within(dialog).getByRole('radio', { name: 'Content ID' }))
  await user.click(within(dialog).getByRole('combobox', { name: 'Content ID' }))
  await user.click(await within(screen.getByRole('listbox')).findByText('ev-123'))
  await user.click(within(dialog).getByRole('button', { name: 'Save' }))
  expect(await within(dialog).findByRole('alert')).toHaveTextContent('ev-123 already has an assignment overlapping')
})

test('the dialog refuses an end before the start, and a content scope without an ID', async () => {
  const user = userEvent.setup()
  renderView()
  const dialog = await openAddAssignment(user)
  await user.click(within(dialog).getByRole('radio', { name: 'Content ID' }))
  expect(within(dialog).getByRole('button', { name: 'Save' })).toBeDisabled()

  await pickCustom(user, dialog, 'Content ID', 'ev-1')
  fireEvent.change(within(dialog).getByLabelText('Starts'), { target: { value: '2026-10-21T02:00' } })
  fireEvent.change(within(dialog).getByLabelText('Ends'), { target: { value: '2026-10-20T18:00' } })
  expect(within(dialog).getByRole('alert')).toHaveTextContent('The end must be after the start')
  expect(within(dialog).getByRole('button', { name: 'Save' })).toBeDisabled()
})

test('editing an assignment seeds the dialog and saves with PUT', async () => {
  const user = userEvent.setup()
  renderView()
  await user.click(await screen.findByRole('button', { name: 'Edit schedule for Content: ev-123' }))
  const dialog = await screen.findByRole('dialog', { name: 'Edit scheduled policy' })
  expect(within(dialog).getByRole('radio', { name: 'Content ID' })).toBeChecked()
  expect(within(dialog).getByRole('combobox', { name: 'Content ID' })).toHaveTextContent('ev-123')
  expect(within(dialog).getByLabelText('Starts')).toHaveValue(
    (await import('@/app/streaming/_sections/tabs/AssignmentDialog')).isoToLocalInput('2026-10-20T18:00:00Z'),
  )
  expect(within(dialog).getByRole('combobox', { name: 'Mode' })).toHaveTextContent('Test only')

  await user.click(within(dialog).getByRole('button', { name: 'Save' }))
  await waitFor(() => expect(putMock).toHaveBeenCalled())
  expect(putMock).toHaveBeenCalledWith(
    '/api/admin/content-security/assignments/{assignment_id}',
    expect.objectContaining({
      params: { path: { assignment_id: 'asg-1' } },
      body: expect.objectContaining({ policy_id: 'pol-1', scope_type: 'content_id', content_id: 'ev-123', mode: 'WARN', starts_at: '2026-10-20T18:00:00.000Z' }),
    }),
  )
})

// ── Deleting ─────────────────────────────────────────────────────────────────

test('deleting an assignment confirms first', async () => {
  const user = userEvent.setup()
  renderView()
  await user.click(await screen.findByRole('button', { name: 'Remove schedule for Content: ev-099' }))
  const confirm = await screen.findByRole('alertdialog')
  expect(within(confirm).getByText('Remove this scheduled policy?')).toBeInTheDocument()
  expect(deleteMock).not.toHaveBeenCalled()
  await user.click(within(confirm).getByRole('button', { name: 'Delete' }))
  await waitFor(() =>
    expect(deleteMock).toHaveBeenCalledWith('/api/admin/content-security/assignments/{assignment_id}', {
      params: { path: { assignment_id: 'asg-2' } },
    }),
  )
  await waitFor(() => expect(showToast).toHaveBeenCalledWith('Removed from schedule', 'success'))
})

test('deleting a policy confirms first and says an assigned policy cannot be deleted', async () => {
  const user = userEvent.setup()
  renderView()
  await user.click(await screen.findByRole('button', { name: 'Delete policy Playoffs' }))
  const confirm = await screen.findByRole('alertdialog')
  expect(confirm).toHaveTextContent('cannot be deleted')
  await user.click(within(confirm).getByRole('button', { name: 'Delete' }))
  await waitFor(() =>
    expect(deleteMock).toHaveBeenCalledWith('/api/admin/content-security/policies/{policy_id}', {
      params: { path: { policy_id: 'pol-1' } },
    }),
  )
})

test('a refused policy delete (still assigned) is swallowed here; the api client shows the server message', async () => {
  deleteMock.mockRejectedValue(new Error('default_not_active'))
  const user = userEvent.setup()
  renderView()
  await user.click(await screen.findByRole('button', { name: 'Delete policy Playoffs' }))
  await user.click(within(await screen.findByRole('alertdialog')).getByRole('button', { name: 'Delete' }))
  await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
  expect(screen.getByRole('table', { name: 'Policies' })).toBeInTheDocument()
})
