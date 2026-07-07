import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import React from 'react'

import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import type { ShareStatus } from '@/components/share-dashboard/utils'

// Radix primitives (Popover/Checkbox) need these in jsdom.
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
} as unknown as ShareStatus

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
