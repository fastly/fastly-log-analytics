/**
 * Tests for the DiagnosticsPanel component, verifying that it loads debug visibility
 * settings from the backend and triggers appropriate PATCH mutations on change.
 */
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, test, vi, beforeEach } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { DiagnosticsPanel } from '@/app/admin/_sections/DiagnosticsPanel'
import { useServiceStore } from '@/stores/serviceStore'

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => ({ data: { debug_state: { debug_responses_enabled: true } } }),
}))

// Mock @/components/ui/select. The real implementation uses base-ui which is
// hard to drive from userEvent in jsdom (portals, native event sequencing).
vi.mock('@/components/ui/select', () => {
  const SelectCtx = React.createContext<((v: string) => void) | null>(null)

  const Select = ({
    value,
    onValueChange,
    children,
  }: {
    value?: string
    onValueChange?: (v: string) => void
    children?: React.ReactNode
  }) => {
    return (
      <SelectCtx.Provider value={onValueChange ?? null}>
        <div data-testid="mock-select" data-select-value={value ?? ''}>
          {children}
        </div>
      </SelectCtx.Provider>
    )
  }

  const SelectTrigger = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-trigger">{children}</div>
  )

  const SelectValue = ({ children }: { children?: React.ReactNode }) => (
    <span data-testid="mock-select-value">{children}</span>
  )

  const SelectContent = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-content">{children}</div>
  )

  const SelectItem = ({
    value,
    children,
  }: {
    value: string
    children?: React.ReactNode
  }) => {
    const onValueChange = React.useContext(SelectCtx)
    return (
      <button
        type="button"
        data-select-item-value={value}
        onClick={() => onValueChange?.(value)}
      >
        {children}
      </button>
    )
  }

  return { Select, SelectTrigger, SelectValue, SelectContent, SelectItem }
})

function makeClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } })
}

describe('DiagnosticsPanel — debug settings API triggers', () => {
  beforeEach(() => {
    // Set an active service ID so the API onRequest middleware does not abort requests with NO_SERVICE
    useServiceStore.setState({ activeServiceId: 'service-1' })
  })

  test('renders with initial settings from the backend', async () => {
    const qc = makeClient()
    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

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

    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

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

    render(
      <QueryClientProvider client={qc}>
        <DiagnosticsPanel />
      </QueryClientProvider>,
    )

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
