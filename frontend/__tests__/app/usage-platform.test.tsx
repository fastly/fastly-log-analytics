import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { PlatformUsageCards } from '@/app/usage/_sections/PlatformUsageCards'
import { UsageStatCards, type StorageUsageData } from '@/app/usage/_sections/UsageStatCards'
import { CostCalculator } from '@/components/CostCalculator/CostCalculator'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import type { components } from '@/types/api.generated'

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn() }),
}))

type PlatformUsage = components['schemas']['PlatformUsageResponse']

const platform: PlatformUsage = {
  _is_cached: false,
  source: 'partial',
  storage: {
    status: 'ok',
    scope: 'account',
    bytes: 2 * 1024 ** 3,
    buckets: 3,
    gb_hours: 48,
  },
  compute: {
    status: 'ok',
    scope: 'app_and_worker',
    requests: 7,
    billed_time_ms: 80,
    backend_errors: 1,
    cache_operations: 2,
    sandboxes: 3,
    resource_errors: 4,
  },
  ngwaf: {
    status: 'ok',
    scope: 'workspace',
    total: 10,
    allowed: 4,
    blocked: 3,
    challenged: 1,
    logged: 1,
    timeout: 1,
    unknown: 0,
  },
}

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

  it('labels account-wide storage and summarizes counters without adding snapshots', () => {
    render(<PlatformUsageCards data={platform} cloudMode />)

    expect(screen.getByText('Account-wide snapshot')).toBeInTheDocument()
    expect(screen.getByText('2 GB')).toBeInTheDocument()
    expect(screen.getByText('48.00 GB-hours · 3 buckets')).toBeInTheDocument()
    expect(screen.getByText('Cloud Compute')).toBeInTheDocument()
    expect(screen.getByText('7 requests')).toBeInTheDocument()
    expect(screen.getByText('3 blocked / 10')).toBeInTheDocument()
    expect(screen.getByText('Configured workspace')).toBeInTheDocument()
  })

  it('keeps independently unavailable and not configured sections visible', () => {
    const data: PlatformUsage = {
      ...platform,
      compute: { ...platform.compute, status: 'not_configured', scope: null },
      ngwaf: {
        ...platform.ngwaf,
        status: 'unavailable',
        scope: null,
        reason: 'metrics_unavailable',
      },
    }
    const { rerender } = render(<PlatformUsageCards data={data} cloudMode={false} />)

    expect(screen.queryByText('Cloud Compute')).not.toBeInTheDocument()
    expect(screen.getAllByText('Unavailable')).toHaveLength(2)
    expect(screen.getAllByText('Measured · Fastly Platform')).toHaveLength(1)

    rerender(<PlatformUsageCards data={{ ...data, ngwaf: {
      ...data.ngwaf, status: 'not_configured', reason: undefined,
    } }} cloudMode />)
    expect(screen.getAllByText('Not configured')).toHaveLength(4)
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

  it('replaces estimator storage cost and total with local measured storage', () => {
    const { rerender } = render(<CostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CostCalculator localStorageGbHours={720} />)

    expect(screen.getByText(/Measured · this install: 1\.000 GB-mo/)).toBeInTheDocument()
    expect(screen.getByText('Measured · this install')).toBeInTheDocument()
    expect(screen.queryByText(/Fastly Platform \(account-wide\)/)).not.toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .not.toBe(reference)
  })

  it('keeps an authoritative zero local storage value visible', () => {
    render(<CostCalculator localStorageGbHours={0} />)

    expect(screen.getByText(/Measured · this install: 0\.000 GB-mo/)).toBeInTheDocument()
    expect(screen.queryByText(/Measured · this install: 0\.001 GB-mo/)).not.toBeInTheDocument()
  })

  it('replaces only the Cloud Object Storage line and total with local measured storage', () => {
    const { rerender } = render(<CloudCostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CloudCostCalculator localStorageGbHours={720} />)

    expect(screen.getByText(/Measured · this install · 1\.00 GB-months/)).toBeInTheDocument()
    expect(screen.queryByText(/Fastly Platform \(account-wide\)/)).not.toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .not.toBe(reference)
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
})
