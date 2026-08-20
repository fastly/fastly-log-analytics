/**
 * C-3 (testing_suite_audit_2026-06-14.md). ReportShell is the layout
 * primitive every analytics page funnels through. Cover the three
 * branches its conditionals encode: no-service fallback, not-ready
 * skeleton, ready content.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Server } from 'lucide-react'
import React from 'react'
import { ReportShell } from '@/components/ReportShell'

let mockActiveServiceId: string | null = 'svc-1'
let mockIsDataReady = true
// Default to "bootstrap has returned" so existing tests keep their
// pre-change behavior. The new flash-defense case sets this to false
// explicitly to verify ReportShell shows the skeleton instead of the
// NoServiceSelected fallback while bootstrap is still in flight.
let mockBootstrapResolved = true

vi.mock('@/hooks/useIsDataReady', () => ({
  useEffectiveServiceId: () => mockActiveServiceId,
  useIsDataReady: () => mockIsDataReady,
  useBootstrapResolved: () => mockBootstrapResolved,
}))

vi.mock('@/components/NoServiceSelected', () => ({
  NoServiceSelected: ({ message }: { message: string }) => (
    <div data-testid="no-service">{message}</div>
  ),
}))

vi.mock('@/components/skeletons/PageSkeleton', () => ({
  DashboardSkeleton: () => <div data-testid="skeleton" />,
}))

describe('ReportShell', () => {
  beforeEach(() => {
    mockActiveServiceId = 'svc-1'
    mockIsDataReady = true
    mockBootstrapResolved = true
  })

  it('renders title + children once the service is selected and data is ready', () => {
    render(
      <ReportShell title="My Report" description="A descriptive line" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByText('My Report')).toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })

  it('falls back to NoServiceSelected when no active service id is set (requireService default)', () => {
    mockActiveServiceId = null
    render(
      <ReportShell title="Security" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('no-service')).toHaveTextContent(/select a service/i)
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
  })

  it('shows the dashboard skeleton when isDataReady is false', () => {
    mockIsDataReady = false
    render(
      <ReportShell title="Logs" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
  })

  it('isReadyOverride wins over the internal isReady computation', () => {
    mockIsDataReady = false
    render(
      <ReportShell title="Logs" icon={Server} isReadyOverride>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('content')).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })

  it('flash defense: bootstrap unresolved AND no active service → skeleton, not NoServiceSelected', () => {
    // Cold load with empty localStorage: useBootstrap hasn't returned
    // yet (bootstrapResolved = false) AND useServiceStore is null
    // (mockActiveServiceId = null). Pre-fix this rendered
    // <NoServiceSelected /> for one tick before HydrationBoundary
    // committed the dehydrated bootstrap or the client-side fetch
    // returned, producing the visible "No service selected" flash on
    // every cold dashboard load. The gate now suppresses that
    // fallback until bootstrap has actually resolved.
    mockActiveServiceId = null
    mockBootstrapResolved = false
    mockIsDataReady = false
    render(
      <ReportShell title="Dashboard" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByTestId('no-service')).not.toBeInTheDocument()
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
  })

  it('requireService=false bypasses the NoServiceSelected gate', () => {
    mockActiveServiceId = null
    render(
      <ReportShell title="Insights" icon={Server} requireService={false}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByTestId('no-service')).not.toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })
})
