import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import React from 'react'

import * as apiLib from '@/lib/api'
import { server } from '@/tests/msw/server'
import { CreateInviteDialog } from '@/components/share-dashboard/CreateInviteDialog'

// Radix primitives (Dialog/Select/Checkbox) need these in jsdom.
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
})
window.HTMLElement.prototype.scrollIntoView = vi.fn()

const services = [{ service_id: 'svcA', name: 'Service A' }] as any

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
  const postSpy = vi
    .spyOn(apiLib.client, 'POST')
    .mockResolvedValue({ data: { id: 'i1' } } as any)

  render(
    <CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />,
  )

  await fillRequiredFields(user)
  await user.click(screen.getByRole('checkbox', { name: /multiple people to use this link/i }))
  await user.click(screen.getByRole('button', { name: /create invite/i }))

  await waitFor(() => expect(postSpy).toHaveBeenCalled())
  const body = (postSpy.mock.calls[0][1] as any).body
  expect(body.allow_concurrent_sessions).toBe(true)
})

test('CreateInviteDialog defaults allow_concurrent_sessions to false', async () => {
  const user = userEvent.setup()
  const postSpy = vi
    .spyOn(apiLib.client, 'POST')
    .mockResolvedValue({ data: { id: 'i1' } } as any)

  render(
    <CreateInviteDialog open onOpenChange={vi.fn()} services={services} onCreated={vi.fn()} />,
  )

  await fillRequiredFields(user)
  await user.click(screen.getByRole('button', { name: /create invite/i }))

  await waitFor(() => expect(postSpy).toHaveBeenCalled())
  const body = (postSpy.mock.calls[0][1] as any).body
  expect(body.allow_concurrent_sessions).toBe(false)
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
