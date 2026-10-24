import { act, fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import React from 'react'

import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import type { ShareStatus } from '@/components/share-dashboard/utils'
import { stubMatchMedia } from '@/__tests__/helpers/browser-stubs'

// Radix primitives (Popover/Checkbox) need these in jsdom.
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
