'use client'

import { Database, HardDrive, ShieldCheck } from 'lucide-react'
import { StatCard } from '@/components/ui/confirm-dialog'
import { formatBytes, formatCount } from '@/lib/utils'
import type { components } from '@/types/api.generated'

type PlatformUsage = components['schemas']['PlatformUsageResponse']
type PlatformSection = PlatformUsage['storage'] | PlatformUsage['compute'] | PlatformUsage['ngwaf']

const statusValue = (status: PlatformSection['status']) =>
  status === 'not_configured' ? 'Not configured' : 'Unavailable'

function sourceLabel(status: PlatformSection['status']) {
  return status === 'ok' ? 'Measured · Fastly Platform' : statusValue(status)
}

export function PlatformUsageCards({
  data,
  loading = false,
  cloudMode,
}: {
  data?: PlatformUsage
  loading?: boolean
  cloudMode: boolean
}) {
  const storage = data?.storage
  const compute = data?.compute
  const ngwaf = data?.ngwaf
  const showCompute = cloudMode || (compute != null && compute.status !== 'not_configured')

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-3 mb-4">
      <StatCard
        title="Object Storage"
        icon={HardDrive}
        loading={loading}
        loadingMessage="Loading Fastly Platform usage"
        value={
          storage?.status === 'ok' && storage.bytes != null
            ? formatBytes(storage.bytes)
            : statusValue(storage?.status ?? 'unavailable')
        }
        sub={
          storage?.status === 'ok' ? (
            <div className="space-y-0.5">
              <div>Account-wide snapshot</div>
              <div>
                {storage.gb_hours == null ? 'GB-hours unavailable' : `${storage.gb_hours.toFixed(2)} GB-hours`}
                {storage.buckets != null ? ` · ${formatCount(storage.buckets)} buckets` : ''}
              </div>
              <div className="uppercase tracking-wider text-[10px]">Measured · Fastly Platform</div>
            </div>
          ) : (
            <div>{sourceLabel(storage?.status ?? 'unavailable')}</div>
          )
        }
        tooltip="Latest Fastly Platform Object Storage gauge for the whole account. Snapshot values are not summed."
      />

      {showCompute && (
        <StatCard
          title="Cloud Compute"
          icon={Database}
          loading={loading}
          loadingMessage="Loading Fastly Platform usage"
          value={
            compute?.status === 'ok'
              ? `${formatCount(compute.requests)} requests`
              : statusValue(compute?.status ?? 'unavailable')
          }
          sub={
            compute?.status === 'ok' ? (
              <div className="space-y-0.5">
                <div>
                  {formatCount(compute.billed_time_ms)} ms billed · {formatCount(compute.backend_errors)} backend errors
                </div>
                <div>
                  {formatCount(compute.cache_operations)} cache ops · {formatCount(compute.sandboxes)} sandboxes
                  {' · '}{formatCount(compute.resource_errors)} resource errors
                </div>
                <div className="uppercase tracking-wider text-[10px]">Measured · Fastly Platform</div>
              </div>
            ) : (
              <div>{sourceLabel(compute?.status ?? 'unavailable')}</div>
            )
          }
          tooltip="Observed Cloud app and worker Compute usage for the install."
        />
      )}

      <StatCard
        title="NGWAF Outcomes"
        icon={ShieldCheck}
        loading={loading}
        loadingMessage="Loading Fastly Platform usage"
        value={
          ngwaf?.status === 'ok'
            ? `${formatCount(ngwaf.blocked)} blocked / ${formatCount(ngwaf.total)}`
            : statusValue(ngwaf?.status ?? 'unavailable')
        }
        sub={
          ngwaf?.status === 'ok' ? (
            <div className="space-y-0.5">
              <div>Configured workspace</div>
              <div>
                {formatCount(ngwaf.allowed)} allowed · {formatCount(ngwaf.challenged)} challenged
                {' · '}{formatCount(ngwaf.logged)} logged
              </div>
              <div>
                {formatCount(ngwaf.timeout)} timeout · {formatCount(ngwaf.unknown)} unknown
              </div>
              <div className="uppercase tracking-wider text-[10px]">Measured · Fastly Platform</div>
            </div>
          ) : (
            <div>{sourceLabel(ngwaf?.status ?? 'unavailable')}</div>
          )
        }
        tooltip="Observed outcomes for the configured NGWAF workspace, not the whole account."
      />
    </div>
  )
}
