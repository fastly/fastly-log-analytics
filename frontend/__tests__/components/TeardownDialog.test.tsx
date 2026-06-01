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

test('TeardownDialog starts the SSE stream when the user clicks Execute Teardown', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()
  const onComplete = vi.fn()

  render(
    <TeardownDialog
      service={{ service_id: "test-svc", name: "Test Service" } as any}
      open={true}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
    />
  )

  expect(screen.getByText('Teardown: Test Service')).toBeDefined()

  // Role-based lookup: this is the actual <button>, not an arbitrary
  // text node, so a label-only refactor (e.g., adding an icon) would
  // not break the test.
  const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
  await user.click(teardownBtn)

  await waitFor(() => {
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/teardown?service_id=test-svc&remove_logging=true&remove_cdn=true&remove_bucket=true&remove_cache=true',
    )
  })
})
