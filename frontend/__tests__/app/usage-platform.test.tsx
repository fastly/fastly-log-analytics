import { render, screen } from '@testing-library/react'
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
type StorageUsage = components['schemas']['PlatformStorageUsage']

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
  total_billed_gb_hours: 720,
  live_bytes: 1024 ** 3,
  live_files: 1,
  deleted_bytes: 0,
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
  loadingBw: false,
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

  it('uses Platform gb-hours for the period storage cost before local inventory', () => {
    render(<UsageStatCards {...statProps} platformStorage={platform.storage} />)

    expect(screen.getByText('48.00 GB-hrs')).toBeInTheDocument()
    expect(screen.getAllByText('$0.07')).toHaveLength(2)
    expect(screen.getAllByText('Measured · Fastly Platform (account-wide)').length).toBeGreaterThan(0)
  })

  it('preserves local storage costs when Platform storage is unavailable', () => {
    const unavailable: StorageUsage = {
      status: 'unavailable',
      scope: 'account',
      reason: 'metrics_unavailable',
    }
    const { rerender } = render(<UsageStatCards {...statProps} />)
    const referenceCost = screen.getAllByText('$1.00')[0].textContent

    rerender(<UsageStatCards {...statProps} platformStorage={unavailable} />)

    expect(screen.getAllByText('$1.00')[0].textContent).toBe(referenceCost)
    expect(screen.getByText('720.00 GB-hrs')).toBeInTheDocument()
    expect(screen.queryByText('Measured · Fastly Platform (account-wide)')).not.toBeInTheDocument()
  })

  it('replaces estimator storage cost and total with the measured account-wide source', () => {
    const { rerender } = render(<CostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CostCalculator observedStorageGbHours={720} />)

    expect(screen.getByText(/Fastly Platform \(account-wide\).*1\.000 GB-mo/)).toBeInTheDocument()
    expect(screen.getByText('Measured · Fastly Platform (account-wide)')).toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .not.toBe(reference)
  })

  it('keeps an authoritative zero Platform storage value visible', () => {
    render(<CostCalculator observedStorageGbHours={0} />)

    expect(screen.getByText(/Fastly Platform \(account-wide\).*0\.000 GB-mo/)).toBeInTheDocument()
    expect(screen.queryByText(/Fastly Platform \(account-wide\).*0\.001 GB-mo/)).not.toBeInTheDocument()
  })

  it('replaces only the Cloud Object Storage line and total', () => {
    const { rerender } = render(<CloudCostCalculator />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CloudCostCalculator observedStorageGbHours={720} />)

    expect(screen.getByText(/Measured · Fastly Platform \(account-wide\).*1\.00 GB-months/)).toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent)
      .not.toBe(reference)
  })
})
