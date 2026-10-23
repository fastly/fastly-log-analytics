import { Suspense } from 'react'
import type { Metadata } from 'next'
import { notFound } from 'next/navigation'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { PageHeader } from '@/components/ui/page-header'
import ClickHouseAdminClient from './_sections/ClickHouseAdminClient'
import { ClickHousePanelFallback } from './loading'

export const metadata: Metadata = {
  title: 'ClickHouse Replay & Cluster Operations',
}

export default function ClickHouseAdminPage() {
  const rawMode = (process.env.DEPLOYMENT_MODE || '').trim().toLowerCase()
  const isHighScale =
    rawMode === 'standard'
      ? false
      : rawMode === 'high_scale' ||
        ['1', 'true', 'yes', 'on'].includes((process.env.HIGH_SCALE_ENABLED || '').trim().toLowerCase())
  if (!isHighScale) {
    notFound()
  }

  return (
    <div className="space-y-6" data-testid="admin-clickhouse-root">
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
