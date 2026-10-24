import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'

import { server } from '@/__tests__/helpers/msw/server'
import { useServiceStore } from '@/stores/serviceStore'
import { CreateInviteDialog } from '@/components/share-dashboard/CreateInviteDialog'
import type { ShareStatus } from '@/components/share-dashboard/utils'
import { stubMatchMedia } from '@/__tests__/helpers/jsdom-shims'

// Radix primitives (Dialog/Select/Checkbox) need these in jsdom.
stubMatchMedia()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

const services: ShareStatus['services'] = [{ service_id: 'svcA', name: 'Service A' }]

interface InviteBody {
  allow_concurrent_sessions?: boolean
}

// Captures the POSTed invite body over the wire instead of spying on the client.
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

beforeEach(() => {
  // The api-client middleware aborts requests when no service is active.
  useServiceStore.setState({ activeServiceId: 'svcA' })
})

afterEach(() => {
  vi.restoreAllMocks()
})

async function fillRequiredFields(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Name'), 'Jane')
  await user.type(screen.getByLabelText('Email'), 'jane@example.com')
  await user.type(screen.getByLabelText('Passcode'), 'ocean-breeze-cabin-42')
  await user.click(screen.getByRole('checkbox', { name: /Service A/i }))
}

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
  // The wire payload (auth_method / oauth_provider / no passcode) is covered by
  // the backend test_create_oauth_invite_success; here we pin the UI toggle
  // (Radix Select option-selection is unreliable in jsdom).
  server.use(
    http.get('/api/admin/share/oauth-providers', () =>
      HttpResponse.json({ providers: [{ id: 'google', display_name: 'Google Workspace', enabled: true }] }),
    ),
  )
  const user = userEvent.setup()
  render(<CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />)

  // Passcode field present in the default (passcode) mode.
  expect(screen.getByLabelText('Passcode')).toBeInTheDocument()

  // Switch to SSO — passcode field disappears, provider selector appears, and
  // submit stays disabled until a provider is picked.
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
