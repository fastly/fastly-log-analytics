import React, { Suspense } from 'react'
import { notFound } from 'next/navigation'
import { BackToAdminLink } from '@/components/ReportLayout'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { ClickHousePanelFallback } from '@/components/ui/skeleton'
import {
  ClickHouseAdminClient,
  ClickHouseStatusNotice,
  ClickHouseVitalsCard,
  ClickHouseActiveGenerationCard,
  ClickHouseReplayControlPanel,
  formatAgeSeconds,
  formatTimestamp,
  type ClickHouseAdminClientProps,
  type ClickHouseReplayRequest,
  type ClickHouseReplayResponse,
  type ClickHouseStatusResponse,
} from './_sections/ClickHouseAdminClient'

export const dynamic = 'force-dynamic'

export {
  ClickHouseAdminClient,
  ClickHouseStatusNotice,
  ClickHouseVitalsCard,
  ClickHouseActiveGenerationCard,
  ClickHouseReplayControlPanel,
  formatAgeSeconds,
  formatTimestamp,
  type ClickHouseAdminClientProps,
  type ClickHouseReplayRequest,
  type ClickHouseReplayResponse,
  type ClickHouseStatusResponse,
}

export function ClickHouseAdminPage() {
  const rawMode = (process.env.DEPLOYMENT_MODE || '').trim().toLowerCase()
  const isHighScale =
    rawMode === 'standard'
      ? false
      : rawMode === 'high_scale' ||
        ['1', 'true', 'yes', 'on'].includes(
          (process.env.HIGH_SCALE_ENABLED || '').trim().toLowerCase(),
        )
  if (!isHighScale) notFound()
  return (
    <div className="space-y-6 min-h-[820px]" data-testid="admin-clickhouse-root">
      <PageHeader
        title="ClickHouse Replay & Cluster Operations"
        description="Monitor High-Scale ClickHouse cluster status, publication manifest health, and execute bounded artifact replays."
      >
        <BackToAdminLink />
      </PageHeader>
      <Suspense fallback={<ClickHousePanelFallback />}>
        <ClickHouseAdminClient />
      </Suspense>
    </div>
  )
}

export default ClickHouseAdminPage
