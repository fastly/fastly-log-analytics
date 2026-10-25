import React from 'react'
import { SystemHealthCard } from "@/components/SystemHealthCard"
import { PageHeader } from '@/components/ui/page-header'

import { ServicesTable } from './_sections/ServicesTable'
import { GlobalSettings, PricingSettings } from './_sections/GlobalSettings'
import { OperationsOverview, AdminPrefetchLinks } from './_sections/OperationsOverview'
import { QuarantineSection } from './_sections/QuarantineSection'

export const dynamic = 'force-dynamic'

export default function AdminPage() {
  const rawMode = (process.env.DEPLOYMENT_MODE || '').trim().toLowerCase()
  const isHighScale =
    rawMode === 'standard'
      ? false
      : rawMode === 'high_scale' ||
        ['1', 'true', 'yes', 'on'].includes((process.env.HIGH_SCALE_ENABLED || '').trim().toLowerCase())

  return (
    <div className="space-y-6" data-testid="admin-overview-root">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        <AdminPrefetchLinks isHighScale={isHighScale} />
      </PageHeader>

      <div
        data-testid="admin-operations-reservation"
        className="min-h-[108px] [contain-intrinsic-size:auto_108px] [content-visibility:auto]"
      >
        <OperationsOverview />
      </div>

      <div
        data-testid="admin-services-reservation"
        className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]"
      >
        <ServicesTable />
      </div>

      <div
        data-testid="admin-health-reservation"
        className="min-h-[280px] [contain-intrinsic-size:auto_280px] [content-visibility:auto]"
      >
        <SystemHealthCard />
      </div>

      <div
        data-testid="admin-quarantine-reservation"
        className="min-h-[120px] [contain-intrinsic-size:auto_120px] [content-visibility:auto]"
      >
        <QuarantineSection />
      </div>

      <div
        data-testid="admin-global-settings-reservation"
        className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]"
      >
        <GlobalSettings />
      </div>

      <div
        data-testid="admin-pricing-reservation"
        className="min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]"
      >
        <PricingSettings />
      </div>
    </div>
  )
}
