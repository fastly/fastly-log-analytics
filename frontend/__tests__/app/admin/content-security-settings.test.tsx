import { QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, expect, test, vi } from 'vitest'

import { ContentSecuritySettingsPanel } from '@/app/admin/_sections/ContentSecuritySettingsPanel'
import { client } from '@/lib/api'
import { showToast } from '@/lib/toast'
import { createTestQueryClient } from '../../helpers/query'

vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), PUT: vi.fn(), POST: vi.fn() },
  extractApiError: vi.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
}))

vi.mock('@/lib/toast', () => ({ showToast: vi.fn() }))

const getMock = vi.mocked(client.GET)
const putMock = vi.mocked(client.PUT)
const postMock = vi.mocked(client.POST)

function renderPanel() {
  const queryClient = createTestQueryClient({ queries: { retry: false } })
  return render(
    <QueryClientProvider client={queryClient}>
      <ContentSecuritySettingsPanel />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  getMock.mockReset()
  putMock.mockReset()
  postMock.mockReset()
  vi.mocked(showToast).mockReset()
})

test('shows the saved blocklist URL and the policy-set publish contract', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  renderPanel()

  const input = screen.getByLabelText('Content Security')
  await waitFor(() => expect(input).toHaveValue('https://hooks.example.com/block'))
  expect(screen.getByText(/published as one idempotent/)).toHaveTextContent('PUT')
  expect(screen.getByText(/published as one idempotent/)).toHaveTextContent('/v2/services/<service>/policy-set')
  // Nothing to save until the value changes.
  expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
})

test('saves a trimmed URL', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: '' } } as never)
  putMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  const user = userEvent.setup()
  renderPanel()

  await user.type(screen.getByLabelText('Content Security'), '  https://hooks.example.com/block ')
  await user.click(screen.getByRole('button', { name: 'Save' }))

  await waitFor(() =>
    expect(putMock).toHaveBeenCalledWith('/api/admin/content-security/settings', {
      body: { blocklist_url: 'https://hooks.example.com/block' },
    }),
  )
  expect(showToast).toHaveBeenCalledWith('Blocklist endpoint saved', 'success')
})

test('shows the validation error from the backend', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: '' } } as never)
  putMock.mockRejectedValue(new Error('URL must be an absolute http:// or https:// URL'))
  const user = userEvent.setup()
  renderPanel()

  await user.type(screen.getByLabelText('Content Security'), 'ftp://x')
  await user.click(screen.getByRole('button', { name: 'Save' }))

  expect(await screen.findByRole('alert')).toHaveTextContent('absolute http')
  expect(showToast).not.toHaveBeenCalled()
})

test('a saved header shows only its name; blank keeps it on save', async () => {
  getMock.mockResolvedValue({
    data: { blocklist_url: 'https://hooks.example.com/block', auth_header_name: 'Authorization' },
  } as never)
  putMock.mockResolvedValue({
    data: { blocklist_url: 'https://hooks.example.com/v2', auth_header_name: 'Authorization' },
  } as never)
  const user = userEvent.setup()
  renderPanel()

  const headerInput = screen.getByLabelText('Authorization header')
  expect(
    await screen.findByText('Saved header: Authorization (value hidden). Type a new one to replace it.'),
  ).toBeInTheDocument()
  // The value never reaches the browser, so the box starts empty and masked.
  expect(headerInput).toHaveValue('')
  expect(headerInput).toHaveAttribute('type', 'password')

  const urlInput = screen.getByLabelText('Content Security')
  await user.clear(urlInput)
  await user.type(urlInput, 'https://hooks.example.com/v2')
  await user.click(screen.getByRole('button', { name: 'Save' }))

  // No auth_header key at all → the backend keeps the saved one.
  await waitFor(() =>
    expect(putMock).toHaveBeenCalledWith('/api/admin/content-security/settings', {
      body: { blocklist_url: 'https://hooks.example.com/v2' },
    }),
  )
})

test('typing a header sends it, then clears the box', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  putMock.mockResolvedValue({
    data: { blocklist_url: 'https://hooks.example.com/block', auth_header_name: 'Authorization' },
  } as never)
  const user = userEvent.setup()
  renderPanel()

  const headerInput = screen.getByLabelText('Authorization header')
  await user.type(headerInput, ' Authorization: Bearer 12345 ')
  await user.click(screen.getByRole('button', { name: 'Save' }))

  await waitFor(() =>
    expect(putMock).toHaveBeenCalledWith('/api/admin/content-security/settings', {
      body: { blocklist_url: 'https://hooks.example.com/block', auth_header: 'Authorization: Bearer 12345' },
    }),
  )
  await waitFor(() => expect(headerInput).toHaveValue(''))
})

test('Remove sends an empty header', async () => {
  getMock.mockResolvedValue({
    data: { blocklist_url: 'https://hooks.example.com/block', auth_header_name: 'X-Api-Key' },
  } as never)
  putMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  const user = userEvent.setup()
  renderPanel()

  await user.click(await screen.findByRole('button', { name: 'Remove' }))
  expect(screen.getByText('The X-Api-Key header will be removed when you save.')).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: 'Save' }))

  await waitFor(() =>
    expect(putMock).toHaveBeenCalledWith('/api/admin/content-security/settings', {
      body: { blocklist_url: 'https://hooks.example.com/block', auth_header: '' },
    }),
  )
})

test('Sync now publishes and reports the result', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  postMock.mockResolvedValue({ data: { status: 'published', rev: 7, error: null } } as never)
  const user = userEvent.setup()
  renderPanel()

  await user.click(screen.getByRole('button', { name: 'Sync now' }))

  await waitFor(() => expect(postMock).toHaveBeenCalledWith('/api/admin/content-security/publish'))
  expect(await screen.findByRole('status')).toHaveTextContent('Synced: the blocker now has revision 7.')
  expect(showToast).toHaveBeenCalledWith('Synced: the blocker now has revision 7.', 'success')
})

test('Sync now says when the change is saved but not yet live', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  postMock.mockResolvedValue({ data: { status: 'failed', rev: 7, error: 'blocker returned HTTP 502' } } as never)
  const user = userEvent.setup()
  renderPanel()

  await user.click(screen.getByRole('button', { name: 'Sync now' }))

  const status = await screen.findByRole('status')
  expect(status).toHaveTextContent('not yet live')
  expect(status).toHaveTextContent('HTTP 502')
  expect(showToast).toHaveBeenCalledWith(expect.stringContaining('HTTP 502'), 'warn')
})

test('Sync now with no endpoint configured says there is nothing to sync', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: '' } } as never)
  postMock.mockResolvedValue({ data: { status: 'not_configured', rev: 0, error: null } } as never)
  const user = userEvent.setup()
  renderPanel()

  await user.click(screen.getByRole('button', { name: 'Sync now' }))
  expect(await screen.findByRole('status')).toHaveTextContent('no blocklist endpoint is configured')
})

test('Sync now shows a request failure', async () => {
  getMock.mockResolvedValue({ data: { blocklist_url: 'https://hooks.example.com/block' } } as never)
  postMock.mockRejectedValue(new Error('boom'))
  const user = userEvent.setup()
  renderPanel()

  await user.click(screen.getByRole('button', { name: 'Sync now' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('boom')
})
