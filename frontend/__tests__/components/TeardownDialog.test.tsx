/**
 * Migration (TESTING_ANALYSIS_AND_PLAN P1 user-event row): fireEvent → userEvent +
 * getByRole. Teardown is destructive, so the test should fail in exactly
 * the cases a real user would experience a broken button (focus
 * trapped, disabled state, pointer-capture mismatch) — fireEvent.click
 * silently passes through all of those.
 */
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { TeardownDialog } from '@/components/TeardownDialog/TeardownDialog'
import React from 'react'

const mockStart = vi.fn()
vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    status: 'idle',
    isDone: false,
    error: null,
    start: mockStart,
    stop: vi.fn(),
    reset: vi.fn()
  })
}))

test('TeardownDialog starts the SSE stream when the user enters a token and clicks Execute Teardown', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  render(
    <TeardownDialog
      service={{ service_id: "test-svc", name: "Test Service", rum_enabled: true } as any}
      open={true}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
    />
  )

  expect(screen.getByText('Teardown: Test Service')).toBeDefined()

  // Security: the button is disabled until the admin pastes a Fastly
  // token. The teardown URL must include it so the backend can validate
  // scope against /tokens/self before performing any destructive op.
  const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
  expect((teardownBtn as HTMLButtonElement).disabled).toBe(true)

  const tokenInput = screen.getByLabelText(/fastly token with the/i)
  await user.type(tokenInput, 'test-admin-token-value')

  expect((teardownBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(teardownBtn)

  // Security: teardown is POST-only now; the token + service id ride in
  // the body so they don't end up in browser history or access logs.
  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/teardown',
      {
        service_id: 'test-svc',
        remove_logging: true,
        remove_rum: true,
        remove_cdn: true,
        remove_bucket: true,
        remove_cloud_files: true,
        remove_cache: true,
        token: 'test-admin-token-value',
      },
    )
  })
})

test('TeardownDialog allows analyst (cache-only) teardown without a token', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  render(
    <TeardownDialog
      service={{ service_id: "test-svc", name: "Test Service", access_level: "read_only" } as any}
      open={true}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
    />
  )

  const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
  // Cache-only teardown does not call Fastly, so no token is required.
  expect((teardownBtn as HTMLButtonElement).disabled).toBe(false)
  await user.click(teardownBtn)

  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/teardown',
      {
        service_id: 'test-svc',
        remove_logging: false,
        remove_rum: false,
        remove_cdn: false,
        remove_bucket: false,
        remove_cache: true,
      },
    )
  })
})
