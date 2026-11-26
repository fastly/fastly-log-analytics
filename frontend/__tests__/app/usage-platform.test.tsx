import { fireEvent, render, screen, within } from '@testing-library/react'
import type { ComponentProps } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { UsageStatCards, type StorageUsageData } from '@/app/usage/_sections/UsageStatCards'
import { costAxisTickFormat, UsageChartsGrid } from '@/app/usage/_sections/UsageChartsGrid'
import { CostCalculator } from '@/components/CostCalculator/CostCalculator'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import { calculate, DEFAULTS, type CalcState } from '@/components/CostCalculator/costCalculatorModel'
import type { PlotlyChart } from '@/components/PlotlyChart'

const plotlyMock = vi.fn<(props: ComponentProps<typeof PlotlyChart>) => void>()

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn() }),
}))

vi.mock('@/components/PlotlyChart', () => ({
  PlotlyChart: (props: ComponentProps<typeof PlotlyChart>) => {
    plotlyMock(props)
    return <div data-testid="plotly-chart" />
  },
}))

const storage: StorageUsageData = {
  _is_cached: false,
  storage_billing_complete: true,
  storage_billing_provenance: ['cloud:inventory'],
  live_billed_gb_hours: 720,
  deleted_billed_gb_hours: 0,
  minimum_billed_gb_hours: 0,
  total_billed_gb_hours: 720,
  live_bytes: 1024 ** 3,
  live_files: 1,
  deleted_bytes: 0,
  deleted_bytes_complete: true,
  deleted_bytes_provenance: 'fos_usage_exact',
  quarantine_bytes: 0,
  total_billed_bytes: 1024 ** 3,
  archive_bytes: 0,
  archive_objects: 0,
  archive_expired_bytes: 0,
  cloud_copy_deleted_bytes: 0,
  total_files: 1,
  total_bytes: 1024 ** 3,
  rum_bytes: 0,
  regular_log_bytes: 1024 ** 3,
  processed_request_bytes: 0,
  processed_rum_bytes: 0,
  processed_objects: 0,
  raw_logs_deleted: true,
  start: '2026-11-01T00:00:00Z',
  end: '2026-11-02T00:00:00Z',
}

const statProps = {
  storage,
  loadingStorage: false,
  errorStorage: null,
  loadingOps: false,
  errorOps: null,
  loadingBw: false,
  errorBw: null,
  isFetchingAny: false,
  isLoadingInitial: false,
  totalClassA: 0,
  totalClassB: 0,
  totalBwGB: 0,
  rateA: 0.005,
  rateB: 0.001,
  rateBPer10k: 0.01,
  rateEgress: 0.12,
  rateStorage: 1,
  activeServiceId: null,
  onNavigateToAdmin: () => {},
}

describe('observed Platform usage', () => {
  it('labels incomplete deletion accounting instead of presenting it as a measured zero', () => {
    render(
      <UsageStatCards
        {...statProps}
        storage={{
          ...storage,
          deleted_bytes: 0,
          deleted_bytes_complete: false,
          deleted_bytes_provenance: 'fos_usage_incomplete',
        } as StorageUsageData}
      />,
    )

    expect(screen.getByText('Incomplete')).toBeInTheDocument()
    expect(screen.queryByText('0 B')).not.toBeInTheDocument()
  })

  it('preserves an explicitly complete measured deletion zero', () => {
    render(<UsageStatCards {...statProps} />)

    expect(screen.getByText('0 B')).toBeInTheDocument()
    expect(screen.queryByText('Incomplete')).not.toBeInTheDocument()
  })

  it('does not guess zero usage or a full cost when operations or bandwidth reads fail', () => {
    render(
      <UsageStatCards
        {...statProps}
        errorOps={new Error('operations unavailable')}
        errorBw={new Error('bandwidth unavailable')}
        totalClassA={0}
        totalClassB={0}
        totalBwGB={0}
      />,
    )

    expect(screen.getAllByText('Unavailable')).toHaveLength(2)
    expect(screen.getAllByText('n/a').length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText(/Partial estimate.*operations and CDN egress.*excluded/)).toBeInTheDocument()
  })

  it('keeps local selected-period storage authoritative and labels it Measured · this install', () => {
    render(<UsageStatCards {...statProps} localStorageGbHours={720} />)

    expect(screen.getByText('720.00 GB-hrs')).toBeInTheDocument()
    expect(screen.getAllByText('$1.00')).toHaveLength(2)
    expect(screen.getAllByText('Measured · this install').length).toBeGreaterThan(0)
    expect(screen.queryByText('Measured · Fastly Platform (account-wide)')).not.toBeInTheDocument()
  })

  it('shows a partial storage warning with provenance when storage billing is incomplete', () => {
    render(
      <UsageStatCards
        {...statProps}
        localStorageGbHours={720}
        storageBillingComplete={false}
        storageBillingProvenance={['cloud:fos_usage']}
      />,
    )

    expect(screen.getByText(/Partial storage cost/)).toBeInTheDocument()
    expect(screen.getByText(/cloud:fos_usage/)).toBeInTheDocument()
  })

  it('does not present incomplete local operations as an exact zero', () => {
    render(
      <UsageStatCards
        {...statProps}
        totalClassA={0}
        totalClassB={0}
        totalBwGB={10}
        operationsSource="local"
        operationsComplete={false}
      />,
    )

    expect(screen.getAllByText('Unavailable').length).toBeGreaterThanOrEqual(2)
    expect(screen.queryByText('$0.00')).not.toBeInTheDocument()
    expect(screen.getByText(/Partial estimate.*operations.*excluded/)).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('uses role="status" for incomplete storage/ops provenance and role="alert" only for API errors', () => {
    const { rerender } = render(
      <UsageStatCards
        {...statProps}
        storage={{ ...storage, storage_billing_complete: false, storage_billing_provenance: ['cloud:inventory_missing'] }}
        operationsComplete={false}
      />,
    )

    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getAllByRole('status').length).toBeGreaterThanOrEqual(2)

    rerender(
      <UsageStatCards
        {...statProps}
        errorStorage={new Error('boom')}
      />,
    )

    expect(screen.getAllByRole('alert').length).toBeGreaterThanOrEqual(1)
  })

  it('preserves authoritative zero Compute usage instead of falling back to logged values', () => {
    render(
      <UsageStatCards
        {...statProps}
        storage={{ ...storage, compute_requests: 7, compute_vcpu_ms: 3 } as StorageUsageData}
        compute={{
          source: 'fastly',
          requests: 0,
          vcpu_ms: 0,
          billed_ms: 0,
          app_requests: 0,
          worker_requests: 0,
          app_vcpu_ms: 0,
          worker_vcpu_ms: 0,
          covered_from: null,
          covered_to: null,
          data: [],
        }}
      />,
    )

    expect(screen.getByText('0 reqs')).toBeInTheDocument()
    expect(screen.queryByText('7 reqs')).not.toBeInTheDocument()
    expect(screen.getByText('Fastly stats · KV logged')).toBeInTheDocument()
  })

  it('keeps High-Scale estimator storage modelled even when local measured storage is provided', () => {
    const { rerender } = render(<CostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CostCalculator localStorageGbHours={24} localStoragePeriodHours={24} />)

    expect(screen.queryByText(/Measured · this install/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Fastly Platform \(account-wide\)/)).not.toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .toBe(reference)
  })

  it('keeps Cloud estimator storage modelled even when local measured storage is provided', () => {
    const { rerender } = render(<CloudCostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CloudCostCalculator localStorageGbHours={720} localStoragePeriodHours={720} />)

    expect(screen.queryByText(/Measured · this install/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Fastly Platform \(account-wide\)/)).not.toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .toBe(reference)
  })

  it('prefills High-Scale line size from configured log fields and defaults to 1320 B matching Cloud', () => {
    const { rerender } = render(<CostCalculator />)
    expect(screen.getByRole('spinbutton', { name: 'Average bytes per log line' })).toHaveValue(1320)

    rerender(<CostCalculator prefillData={{ estimated_bytes_per_line: 2450, avg_log_file_size_kb: 999 } as never} />)
    expect(screen.getByRole('spinbutton', { name: 'Average bytes per log line' })).toHaveValue(2450)
  })

  it('lets users switch the High-Scale Cloud snapshot interval and updates DR state snapshots and total cost', () => {
    render(<CostCalculator />)
    const group = screen.getByRole('group', { name: 'Cloud snapshot interval' })
    const btn5m = within(group).getByRole('button', { name: '5m' })
    const btn1h = within(group).getByRole('button', { name: '1h' })
    expect(btn5m).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByText('DR state snapshots / month').nextElementSibling?.textContent).toBe('8.6K')
    const total5m = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent

    fireEvent.click(btn1h)
    expect(btn1h).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByText('DR state snapshots / month').nextElementSibling?.textContent).toBe('720')
    const total1h = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    expect(total1h).not.toBe(total5m)
  })

  it('increases Estimated Cost y-axis precision when max cost is tiny to avoid duplicate $0.00 ticks', () => {
    expect(costAxisTickFormat(0)).toBe('.2f')
    expect(costAxisTickFormat(0.15)).toBe('.2f')
    expect(costAxisTickFormat(0.05)).toBe('.2f')
    expect(costAxisTickFormat(0.008)).toBe('.3f')
    expect(costAxisTickFormat(0.002)).toBe('.4f')
    expect(costAxisTickFormat(0.0002)).toBe('.5f')
  })

  it('reserves full chart-grid height in Cloud mode and Cloud stat-card height to prevent CLS', () => {    const { container: gridContainer } = render(
      <UsageChartsGrid
        ops={undefined}
        loadingOps={true}
        fetchingOps={false}
        errorOps={null}
        bw={undefined}
        loadingBw={true}
        fetchingBw={false}
        errorBw={null}
        logActivity={undefined}
        loadingActivity={true}
        fetchingActivity={false}
        errorActivity={null}
        rumBreakdown={undefined}
        loadingRumBreakdown={false}
        fetchingRumBreakdown={false}
        errorRumBreakdown={null}
        isRumEnabled={false}
        isFetchingAny={false}
        isLoadingInitial={true}
        isDark={false}
        rateA={0.005}
        rateB={0.001}
        rateEgress={0.12}
        effectiveInterval="1 day"
        setChartInterval={() => {}}
        showCompute={true}
        compute={undefined}
        loadingCompute={true}
      />,
    )
    const grid = gridContainer.firstElementChild as HTMLElement
    expect(grid.className).toContain('min-h-[1128px]')
    expect(grid.className).toContain('[contain-intrinsic-size:1128px]')

    const { container: statsContainer } = render(
      <UsageStatCards {...statProps} loadingStorage={true} cloudMode={true} />,
    )
    const statsGrid = statsContainer.firstElementChild as HTMLElement
    expect(statsGrid.className).toContain('min-h-[420px]')
    expect(statsGrid.className).toContain('[contain-intrinsic-size:420px]')
  })

  it('shows Operations provenance label and status note when using Fastly Platform fallback', () => {
    const { rerender } = render(
      <UsageStatCards
        {...statProps}
        totalClassA={1000}
        totalClassB={2000}
        operationsSource="local"
        operationsComplete={true}
      />,
    )
    expect(screen.getAllByText('Measured · this install').length).toBeGreaterThanOrEqual(2)
    expect(screen.queryByText('Fastly Platform (account-wide)')).not.toBeInTheDocument()

    rerender(
      <UsageStatCards
        {...statProps}
        totalClassA={1000}
        totalClassB={2000}
        operationsSource="fastly"
        operationsComplete={true}
      />,
    )
    expect(screen.getByText('Fastly Platform (account-wide)')).toBeInTheDocument()
    expect(screen.getByRole('status')).toHaveTextContent(/Fastly Platform/i)
  })

  it('updates FOS Operations card description by provenance and decouples Estimated Cost x-axis from ops availability', () => {
    plotlyMock.mockClear()
    const baseGridProps = {
      loadingOps: false,
      fetchingOps: false,
      errorOps: null,
      loadingBw: false,
      fetchingBw: false,
      errorBw: null,
      logActivity: undefined,
      loadingActivity: false,
      fetchingActivity: false,
      errorActivity: null,
      rumBreakdown: undefined,
      loadingRumBreakdown: false,
      fetchingRumBreakdown: false,
      errorRumBreakdown: null,
      isRumEnabled: false,
      isFetchingAny: false,
      isLoadingInitial: false,
      isDark: false,
      rateA: 0.005,
      rateB: 0.001,
      rateEgress: 0.12,
      effectiveInterval: '1 hour' as const,
      setChartInterval: () => {},
    }

    const { rerender } = render(
      <UsageChartsGrid
        {...baseGridProps}
        ops={{
          data: [{ date: '2026-11-22T12:00', class_a: 1000, class_b: 0 }],
          total_class_a: 1000,
          total_class_b: 0,
          granularity: 'hour',
          note: '',
          fos_fields_found: [],
          source: 'local',
          complete: true,
          provenance: ['high-scale:usage_log'],
        } as never}
        bw={{
          data: [
            { time: '2026-11-22T12:00:00.000Z', bandwidth_bytes: 1_000_000_000, requests: 1 },
            { time: '2026-11-22T13:00', bandwidth_bytes: 2_000_000_000, requests: 2 },
          ],
          total_bytes: 3_000_000_000,
          total_log_bytes: 0,
          granularity: 'hour',
        } as never}
      />,
    )
    expect(screen.getByText('Service-local Class A and Class B Object Storage operations')).toBeInTheDocument()
    const costCall = plotlyMock.mock.calls.find(([p]) =>
      p.data.some((t) => t.name === 'CDN Egress'),
    )
    expect(costCall).toBeDefined()
    const [opsTrace, egressTrace] = costCall![0].data
    expect(opsTrace.x).toEqual(['2026-11-22T12:00', '2026-11-22T13:00'])
    expect(opsTrace.y).toEqual([0.005, 0])
    expect(egressTrace.x).toEqual(['2026-11-22T12:00', '2026-11-22T13:00'])
    expect(egressTrace.y).toEqual([0.12, 0.24])

    plotlyMock.mockClear()
    rerender(
      <UsageChartsGrid
        {...baseGridProps}
        ops={{
          data: [{ date: '2026-11-22T12:00', class_a: 1000, class_b: 0 }],
          total_class_a: 1000,
          total_class_b: 0,
          granularity: 'hour',
          note: '',
          fos_fields_found: [],
          source: 'fastly',
          complete: true,
          provenance: [],
        } as never}
        bw={{ data: [], total_bytes: 0, total_log_bytes: 0, granularity: 'hour' } as never}
      />,
    )
    expect(
      screen.getByText('Fallback: account-wide Class A and Class B operations from Fastly Platform Metrics'),
    ).toBeInTheDocument()

    plotlyMock.mockClear()
    rerender(
      <UsageChartsGrid
        {...baseGridProps}
        ops={{
          data: [],
          total_class_a: 0,
          total_class_b: 0,
          granularity: 'hour',
          note: '',
          fos_fields_found: [],
          source: 'local',
          complete: false,
          provenance: [],
        } as never}
        bw={{
          data: [{ time: '2026-11-22T13:00', bandwidth_bytes: 1_000_000_000, requests: 1 }],
          total_bytes: 1_000_000_000,
          total_log_bytes: 0,
          granularity: 'hour',
        } as never}
      />,
    )
    expect(
      screen.getByText('Local Object Storage operation telemetry is unavailable for this period'),
    ).toBeInTheDocument()
    const incompleteCostCall = plotlyMock.mock.calls.find(([p]) =>
      p.data.some((t) => t.name === 'CDN Egress'),
    )
    expect(incompleteCostCall).toBeDefined()
    expect(incompleteCostCall![0].data[0].y).toEqual([0])
    expect(incompleteCostCall![0].data[1].y).toEqual([0.12])
  })
})

describe('High-Scale cost calculator model fixes', () => {
  const base = (over: Partial<CalcState> = {}): CalcState => ({
    ...DEFAULTS,
    edgeOnly: false,
    sampleRate: 100,
    rumBeaconsDay: 0,
    ...over,
  })

  it('defaults FOS operation rates to Fastly published list rates (A $0.0025/1k, B $0.004/10k)', () => {
    expect(DEFAULTS.rateA).toBe(0.0025)
    expect(DEFAULTS.rateB).toBe(0.0004)
    expect(DEFAULTS.rateB * 10).toBeCloseTo(0.004, 6)
  })

  it('caps log files per day with the Poisson coupon-collector formula at low volume', () => {
    const r = calculate(base({ reqDay: 10_000, logNodes: 100, logPeriod: 60 }))
    expect(r.logFilesPerMonth / 30).toBeCloseTo(9660.7, 0)
    expect(r.logFilesPerMonth / 30).toBeLessThan(10_000)
  })

  it('produces zero log files (no NaN) at zero requests', () => {
    const r = calculate(base({ reqDay: 0, logNodes: 100, logPeriod: 60 }))
    expect(r.logFilesPerMonth).toBe(0)
    expect(Number.isNaN(r.totalGBStored)).toBe(false)
    expect(Number.isNaN(r.classBPerMonth)).toBe(false)
    expect(Number.isNaN(r.totalCost)).toBe(false)
  })

  it('computes cdnEgressGB with decimal 1e9 (Fastly egress billing), not binary 1024^3', () => {
    const r = calculate(base({ reqDay: 1_000_000, cdnEnabled: true }))
    expect(r.cdnEgressGB).toBeCloseTo(r.totalGzBytesPerMonth / 1e9, 6)
    expect(r.cdnEgressGB).not.toBeCloseTo(r.totalGzBytesPerMonth / 1024 ** 3, 6)
  })

  it('charges 5 archive Class B verification reads per batch', () => {
    const r = calculate(base({ reqDay: 1_000_000, rumBeaconsDay: 0 }))
    expect(r.classBPerMonth).toBeCloseTo(r.logFilesPerMonth + r.parquetFilesPerMonth * 5, 6)
  })
})

describe('High-Scale cost calculator UI fixes', () => {
  it('shows the corrected ~5× ZSTD ratio (not the stale ~15×)', () => {
    const { container } = render(<CostCalculator />)
    expect(container.textContent).toContain('~5×')
    expect(container.textContent).not.toContain('~15×')
  })

  it('hides log nodes / fan-out from the UI since it is auto-derived', () => {
    render(<CostCalculator />)
    expect(screen.queryByText('Log nodes / fan-out')).not.toBeInTheDocument()
  })
})
