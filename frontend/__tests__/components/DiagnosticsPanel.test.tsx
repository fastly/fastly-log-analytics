/**
 * Tests for the DiagnosticsPanel component, verifying that it loads debug visibility
 * settings from the backend and triggers appropriate PATCH mutations on change.
 */
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, test, vi, beforeEach } from 'vitest'
import { createTestQueryClient, renderWithQuery } from '../helpers/query'
import { selectMockFactory } from '../helpers/ui-mocks'
import React from 'react'

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => ({ data: { debug_state: { debug_responses_enabled: true } } }),
}))

// Mock @/components/ui/select. The real implementation uses base-ui which is
// hard to drive from userEvent in jsdom (portals, native event sequencing).
vi.mock('@/components/ui/select', () => selectMockFactory())

import { DiagnosticsPanel } from '@/app/admin/_sections/DiagnosticsPanel'
import { useServiceStore } from '@/stores/serviceStore'

function makeClient() {
  return createTestQueryClient()
}

describe('DiagnosticsPanel — debug settings API triggers', () => {
  beforeEach(() => {
    // Set an active service ID so the API onRequest middleware does not abort requests with NO_SERVICE
    useServiceStore.setState({ activeServiceId: 'service-1' })
  })

  test('renders with initial settings from the backend', async () => {
    const qc = makeClient()
    renderWithQuery(<DiagnosticsPanel />, { queryClient: qc })

    // Wait for settings to load
    await screen.findByText('Query debugging panel')

    // Check that we render the mock select elements with correct initial values
    const selectContainers = screen.getAllByTestId('mock-select')
    expect(selectContainers).toHaveLength(2)
    expect(selectContainers[0].getAttribute('data-select-value')).toBe('disabled')
    expect(selectContainers[1].getAttribute('data-select-value')).toBe('disabled')
  })

  test('changing query-debug visibility triggers mutation and invalidates queries', async () => {
    const user = userEvent.setup()
    const qc = makeClient()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')

    renderWithQuery(<DiagnosticsPanel />, { queryClient: qc })

    // Wait for initial render
    await screen.findByText('Query debugging panel')

    // Find the first SelectItem with value "admins" (for query debugging)
    const items = screen.getAllByRole('button')
    const queryAdminsItem = items.find(
      (b) => b.getAttribute('data-select-item-value') === 'admins',
    )
    expect(queryAdminsItem).toBeInTheDocument()

    // Click "Admins Only" button on the first Select
    await user.click(queryAdminsItem!)

    // Verify invalidation was called
    await waitFor(() => {
      expect(invalidateSpy).toHaveBeenCalled()
    })
  })

  test('changing api-calls visibility triggers mutation and invalidates queries', async () => {
    const user = userEvent.setup()
    const qc = makeClient()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')

    renderWithQuery(<DiagnosticsPanel />, { queryClient: qc })

    // Wait for initial render
    await screen.findByText('Query debugging panel')

    // Find all buttons with value "both" and select the second one (for API call panel)
    const items = screen.getAllByRole('button')
    const apiBothItems = items.filter(
      (b) => b.getAttribute('data-select-item-value') === 'both',
    )
    expect(apiBothItems).toHaveLength(2)

    // Click "Both Admins & Analysts" button on the second Select (index 1)
    await user.click(apiBothItems[1])

    // Verify invalidation was called
    await waitFor(() => {
      expect(invalidateSpy).toHaveBeenCalled()
    })
  })
})
