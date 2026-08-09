'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { useTimezone } from '@/hooks/useTimezone'
import { useColumnVisibility } from '@/hooks/useColumnVisibility'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { useTimeLayout } from '@/lib/chart-helpers'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveRangeWire } from '@/lib/range-wire'
import { ReportLayout } from '@/components/ReportLayout'
import { Skeleton } from '@/components/ui/skeleton'
import { client } from '@/lib/api'
import { Shield } from 'lucide-react'
import { BotsSection } from './BotsSection'
// HeaderAnomaliesSection + NetworkSection sit below the first-paint
// fold (BotsSection is the anchor). They consume their OWN per-section
// fetch, so deferring their JS parse + React mount until after the first
// commit lets the bots cards above paint without competing for the main
// thread on ~2 Plotly mounts each.
const HeaderAnomaliesSection = dynamic(
  () => import('./HeaderAnomaliesSection').then(m => ({ default: m.HeaderAnomaliesSection })),
  { ssr: false, loading: () => <Skeleton className="h-80 w-full" /> },
)
const NetworkSection = dynamic(
  () => import('./NetworkSection').then(m => ({ default: m.NetworkSection })),
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
//
// MUST stay in lockstep with SECURITY_SSR_SECTIONS in lib/ssr/security.ts —
// the section list is part of the SSR-seed query key, so a divergence here
// would miss the dehydrated cache and double-fetch on first paint.
const SECURITY_SECTIONS: SecuritySections = [...BOTS_SECTIONS, ...HEADER_ANOMALIES_SECTIONS, ...NETWORK_SECTIONS]

// Lifted out of the ReportLayout render-prop so useTimeLayout + useServiceQuery
// live at the top of a STABLE component instead of being called inside the
// `{(ctx) => { …hooks… }}` callback. Mirrors InsightsBody (app/insights/
// page.tsx) and DashboardBody. ReportLayout currently calls children(...)
// unconditionally so the in-callback hooks were safe today, but the moment a
// conditional/early-return appears above them (ReportShell's `!isReady`
// placeholder/toggle re-render path can hit one) React's hook order changes
// across renders and throws #310 ("Rendered fewer/more hooks than the
// previous render"). Hooks at the top of a dedicated child are immune.
type SecurityFilters = components['schemas']['SecurityAggregatesRequest']['filters']

interface SecurityBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: SecurityFilters
  bucketSeconds: number
  intervalButtons: React.ReactNode
  timezone: string
  getFieldLabel: (field: string) => string
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  fingerprintVisibility: ReturnType<typeof useColumnVisibility>[0]
  setFingerprintVisibility: ReturnType<typeof useColumnVisibility>[1]
  onFingerprintVisChange: ReturnType<typeof useColumnVisibility>[2]
  topIpVisibility: ReturnType<typeof useColumnVisibility>[0]
  setTopIpVisibility: ReturnType<typeof useColumnVisibility>[1]
  onTopIpVisChange: ReturnType<typeof useColumnVisibility>[2]
  botVisibility: ReturnType<typeof useColumnVisibility>[0]
  setBotVisibility: ReturnType<typeof useColumnVisibility>[1]
  onBotVisChange: ReturnType<typeof useColumnVisibility>[2]
  ngwafBotVisibility: ReturnType<typeof useColumnVisibility>[0]
  setNgwafBotVisibility: ReturnType<typeof useColumnVisibility>[1]
  onNgwafBotVisChange: ReturnType<typeof useColumnVisibility>[2]
}

function SecurityBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  bucketSeconds,
  intervalButtons,
  timezone,
  getFieldLabel,
  relativeRange,
  isAutoRange,
  anchor,
  fingerprintVisibility,
  setFingerprintVisibility,
  onFingerprintVisChange,
  topIpVisibility,
  setTopIpVisibility,
  onTopIpVisChange,
  botVisibility,
  setBotVisibility,
  onBotVisChange,
  ngwafBotVisibility,
  setNgwafBotVisibility,
  onNgwafBotVisChange,
}: SecurityBodyProps) {
  const commonTimeLayout = useTimeLayout(startTime, endTime, timezone)

  // Single combined query for all three section groups (see SECURITY_SECTIONS).
  // One POST → one DuckDB scan, shared loading/error.
  //
  // SSR-seed key contract (lib/ssr/security.ts + app/security/page.tsx): in token
  // mode keys on the SERVER-REPRODUCIBLE (rangeKey, anchor) pair instead of the
  // client-now()-anchored start/end, so the seed byte-matches and paints from
  // cache. On a cold load rangeKey is "24h". A custom absolute range instead keys
  // on "abs:<start>|<end>" and sends the explicit bounds, so it scans exactly
  // what the chart x-axis (startTime/endTime) displays.
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
  const securityQuery = useServiceQuery<SecurityData | undefined>(
    ['security', 'aggregates', activeServiceId, rangeKey, anchor, filterPayload, bucketSeconds],
    async ({ signal }) => {
      const { data } = await client.POST('/api/security/aggregates', {
        signal,
        body: {
          // Token mode → {range_token, anchor}; custom mode → {start_time,
          // end_time} (backend uses these when range_token is absent).
          filters: filterPayload,
          bucket_seconds: bucketSeconds,
          sections: SECURITY_SECTIONS,
          ...rangeBody,
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
}

export default function SecurityClient() {
  const getFieldLabel = useFieldLabel()
  const timezone = useTimezone()

  // Time-range wire inputs (lib/range-wire.ts; resolved in SecurityBody where
  // startTime/endTime are available). A quick-preset pill / the cold-load default
  // → a server-reproducible token ("24h"), matching the hard-clamped chart x-axis
  // AND keeping the SSR seed key byte-matched. A custom absolute range
  // (relativeRange null + isAutoRange false) → the explicit start/end bounds, so
  // it scans exactly what it displays.
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)
  // Anchor the keyed path to the SELECTED window's end, not to mount time: a
  // preset clicked in a long-lived tab re-anchors at click time so the scan
  // matches the hard-clamped x-axis (a mount-pinned anchor could leave the
  // short 1h..12h presets fully disjoint from the display). Memoized on endTime
  // so a re-render can't advance the key; cold-load endTime ≈ mount now, so the
  // SSR seed (same 60s floor, quantizeAnchor ≡ backend quantize_anchor) still
  // byte-matches within the quantum.
  const anchor = React.useMemo(() => {
    return quantizeAnchor(storeEndTime)
  }, [storeEndTime])

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
      {({ startTime, endTime, activeServiceId, filterPayload, bucketSeconds, intervalButtons }) => (
        <SecurityBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          bucketSeconds={bucketSeconds}
          intervalButtons={intervalButtons}
          timezone={timezone}
          getFieldLabel={getFieldLabel}
          relativeRange={relativeRange}
          isAutoRange={isAutoRange}
          anchor={anchor}
          fingerprintVisibility={fingerprintVisibility}
          setFingerprintVisibility={setFingerprintVisibility}
          onFingerprintVisChange={onFingerprintVisChange}
          topIpVisibility={topIpVisibility}
          setTopIpVisibility={setTopIpVisibility}
          onTopIpVisChange={onTopIpVisChange}
          botVisibility={botVisibility}
          setBotVisibility={setBotVisibility}
          onBotVisChange={onBotVisChange}
          ngwafBotVisibility={ngwafBotVisibility}
          setNgwafBotVisibility={setNgwafBotVisibility}
          onNgwafBotVisChange={onNgwafBotVisChange}
        />
      )}
    </ReportLayout>
  )
}
