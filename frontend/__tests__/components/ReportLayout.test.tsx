/**
 * Consolidated tests for ReportShell, ChartIntervalButtons, NoServiceSelected, and UpdatingBadge.
 *
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { Server } from 'lucide-react'
import React from 'react'
import { ReportShell } from '@/components/ReportShell'
import { ChartIntervalButtons } from '@/components/ChartIntervalButtons'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import { INTERVALS } from '@/lib/constants'

let mockActiveServiceId: string | null = 'svc-1'
let mockIsDataReady = true
let mockBootstrapResolved = true

vi.mock('@/hooks/useIsDataReady', () => ({
  useEffectiveServiceId: () => mockActiveServiceId,
  useIsDataReady: () => mockIsDataReady,
  useBootstrapResolved: () => mockBootstrapResolved,
}))

vi.mock('@/components/skeletons/PageSkeleton', () => ({
  DashboardSkeleton: () => <div data-testid="skeleton" />,
}))

afterEach(() => cleanup())

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
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText(/select a service/i)).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
  })

  it('shows the dashboard skeleton when isDataReady is false and honours isReadyOverride', () => {
    mockIsDataReady = false
    const { unmount } = render(
      <ReportShell title="Logs" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
    expect(screen.queryByTestId('content')).not.toBeInTheDocument()
    unmount()

    render(
      <ReportShell title="Logs" icon={Server} isReadyOverride>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.getByTestId('content')).toBeInTheDocument()
    expect(screen.queryByTestId('skeleton')).not.toBeInTheDocument()
  })

  it('flash defense: bootstrap unresolved AND no active service → skeleton, not NoServiceSelected', () => {
    mockActiveServiceId = null
    mockBootstrapResolved = false
    mockIsDataReady = false
    render(
      <ReportShell title="Dashboard" icon={Server}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('skeleton')).toBeInTheDocument()
  })

  it('requireService=false bypasses the NoServiceSelected gate', () => {
    mockActiveServiceId = null
    render(
      <ReportShell title="Insights" icon={Server} requireService={false}>
        <div data-testid="content">body</div>
      </ReportShell>,
    )
    expect(screen.queryByRole('heading', { name: 'No Service Selected' })).not.toBeInTheDocument()
    expect(screen.getByTestId('content')).toBeInTheDocument()
  })
})

const ALL_VALID = new Set(INTERVALS.map((i) => i.value))

describe('ChartIntervalButtons', () => {
  it('renders one button per INTERVALS entry and marks effective interval with aria-pressed=true', () => {
    render(
      <ChartIntervalButtons
        effectiveInterval="1 hour"
        validIntervals={ALL_VALID}
        onIntervalChange={() => {}}
      />,
    )
    for (const i of INTERVALS) {
      expect(screen.getByRole('button', { name: `Chart bucket size: ${i.label}` })).toBeInTheDocument()
    }
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1h' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1m' })).toHaveAttribute('aria-pressed', 'false')
  })

  it('fires onIntervalChange for valid buttons and disables invalid ones', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    const { unmount } = render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={ALL_VALID}
        onIntervalChange={onChange}
      />,
    )
    await user.click(screen.getByRole('button', { name: 'Chart bucket size: 1h' }))
    expect(onChange).toHaveBeenCalledWith('1 hour')
    unmount()

    const onChangeDisabled = vi.fn()
    render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={new Set(['1 minute'])}
        onIntervalChange={onChangeDisabled}
      />,
    )
    const hourBtn = screen.getByRole('button', { name: 'Chart bucket size: 1h' })
    expect(hourBtn).toBeDisabled()
    await user.click(hourBtn)
    expect(onChangeDisabled).not.toHaveBeenCalled()
  })
})

describe('NoServiceSelected', () => {
  it('renders the provided message, default/explicit title, and lucide icon', () => {
    const { container, unmount } = render(
      <NoServiceSelected icon={Server} message="Pick one from the picker above" />,
    )
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText('Pick one from the picker above')).toBeInTheDocument()
    const svg = container.querySelector('svg')
    expect(svg?.classList.contains('text-muted-foreground')).toBe(true)
    unmount()

    render(<NoServiceSelected icon={Server} message="msg" title="Nothing here yet" />)
    expect(screen.getByRole('heading', { name: 'Nothing here yet' })).toBeInTheDocument()
  })
})

describe('UpdatingBadge', () => {
  it('renders the "Updating" label text and pulses the decorative dot only', () => {
    const { container } = render(<UpdatingBadge />)
    expect(screen.getByText(/updating/i)).toBeInTheDocument()
    expect(container.children).toHaveLength(1)
    const root = container.firstChild as HTMLElement
    expect(root.className).not.toMatch(/animate-pulse/)
    expect(root.querySelector('span')!.className).toMatch(/animate-pulse/)
  })
})
