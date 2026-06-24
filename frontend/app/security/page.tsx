'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { useTimezone } from '@/hooks/useTimezone'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { useTimeLayout } from '@/lib/chart-helpers'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { ReportLayout } from '@/components/ReportLayout'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import { Shield } from 'lucide-react'
import { BotsSection } from './_sections/BotsSection'
// HeaderAnomaliesSection + NetworkSection sit below the first-paint
// fold (BotsSection is the anchor). They consume their OWN per-section
// fetch, so deferring their JS parse + React mount until after the first
// commit lets the bots cards above paint without competing for the main
// thread on ~2 Plotly mounts each.
const HeaderAnomaliesSection = dynamic(
  () => import('./_sections/HeaderAnomaliesSection').then(m => ({ default: m.HeaderAnomaliesSection })),
  { ssr: false, loading: () => <Skeleton className="h-80 w-full" /> },
)
const NetworkSection = dynamic(
  () => import('./_sections/NetworkSection').then(m => ({ default: m.NetworkSection })),
  { ssr: false, loading: () => <Skeleton className="h-80 w-full" /> },
)
import type { components } from '@/types/api.generated'

type SecurityData = components['schemas']['SecurityAggregatesResponse']

// Per-section field lists. The backend gates each section block on
// `sections is None or 'name' in sections`; passing only the names a
// given React section needs keeps the other section blocks from
// running. The fingerprint cards + fingerprint_coverage are bundled
// together because security.py:858 reuses a shared scan for them; the
// two ngwaf_verified_bots* fields share a per-conn ngwaf_cache ATTACH.
// BotsSection currently renders the fingerprint cards, so they live
// in its request rather than HeaderAnomaliesSection's.
type SecuritySections = NonNullable<components['schemas']['SecurityAggregatesRequest']['sections']>
const BOTS_SECTIONS: SecuritySections = [
  'ngwaf_verified_bots',
  'ngwaf_verified_bots_ts',
  'wellknown_bots',
  'tls_fingerprints',
  'fingerprint_coverage',
]
const HEADER_ANOMALIES_SECTIONS: SecuritySections = ['req_size_dist', 'top_ips_header']
const NETWORK_SECTIONS: SecuritySections = ['ipv6_adoption', 'proxy_dist', 'conn_reuse_dist']
// Union of all three section groups — fetched in a single POST so the page
// makes one /api/security/aggregates call instead of three concurrent ones.
// On the 4-core prod VM, three parallel DuckDB scans oversubscribe the cores
// (see memory: cpu-bound queries don't parallelize there); one combined scan
// avoids that contention. Trade-off vs. the prior split: the three sections
// now share a single loading state (no progressive above-the-fold paint) and
// a single error boundary.
const SECURITY_SECTIONS: SecuritySections = [...BOTS_SECTIONS, ...HEADER_ANOMALIES_SECTIONS, ...NETWORK_SECTIONS]

export default function SecurityPage() {
  const getFieldLabel = useFieldLabel()
  const timezone = useTimezone()

  const [fingerprintVisibility, setFingerprintVisibility, onFingerprintVisChange] = useColumnVisibility()
  const [topIpVisibility, setTopIpVisibility, onTopIpVisChange] = useColumnVisibility()
  const [botVisibility, setBotVisibility, onBotVisChange] = useColumnVisibility()
  const [ngwafBotVisibility, setNgwafBotVisibility, onNgwafBotVisChange] = useColumnVisibility()

  return (
    <ReportLayout
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
      icon={Shield}
    >
      {({ startTime, endTime, activeServiceId, filterPayload, bucketSeconds, intervalButtons }) => {
        const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)

        // Single combined query for all three section groups (see
        // SECURITY_SECTIONS). One POST → one DuckDB scan, shared loading/error.
        const securityQuery = useServiceQuery<SecurityData | undefined>(
          ['security', 'aggregates', activeServiceId, startTime, endTime, filterPayload, bucketSeconds],
          async ({ signal }) => {
            const { data } = await client.POST('/api/security/aggregates', {
              signal,
              body: {
                start_time: startTime,
                end_time: endTime,
                filters: filterPayload,
                bucket_seconds: bucketSeconds,
                sections: SECURITY_SECTIONS,
              },
            })
            return data
          },
        )

        return (
          <>
            <BotsSection
              data={securityQuery.data}
              isLoading={securityQuery.isLoading}
              isFetching={securityQuery.isFetching}
              error={securityQuery.error ?? null}
              intervalButtons={intervalButtons}
              bucketSeconds={bucketSeconds}
              timezone={timezone}
              commonTimeLayout={commonTimeLayout}
              getFieldLabel={getFieldLabel}
              ngwafBotVisibility={ngwafBotVisibility}
              setNgwafBotVisibility={setNgwafBotVisibility}
              onNgwafBotVisChange={onNgwafBotVisChange}
              botVisibility={botVisibility}
              setBotVisibility={setBotVisibility}
              onBotVisChange={onBotVisChange}
              fingerprintVisibility={fingerprintVisibility}
              setFingerprintVisibility={setFingerprintVisibility}
              onFingerprintVisChange={onFingerprintVisChange}
            />
            <HeaderAnomaliesSection
              data={securityQuery.data}
              isLoading={securityQuery.isLoading}
              isFetching={securityQuery.isFetching}
              error={securityQuery.error ?? null}
              getFieldLabel={getFieldLabel}
              topIpVisibility={topIpVisibility}
              setTopIpVisibility={setTopIpVisibility}
              onTopIpVisChange={onTopIpVisChange}
            />
            <NetworkSection
              data={securityQuery.data}
              isLoading={securityQuery.isLoading}
              isFetching={securityQuery.isFetching}
              error={securityQuery.error ?? null}
              timezone={timezone}
              commonTimeLayout={commonTimeLayout}
            />
          </>
        )
      }}
    </ReportLayout>
  )
}
