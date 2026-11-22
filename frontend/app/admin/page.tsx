import React from 'react'
import { SystemHealthCard } from "@/components/SystemHealthCard"
import { PageHeader } from '@/components/ui/confirm-dialog'

import { OperationsOverview, AdminPrefetchLinks, ServicesTable, GlobalSettings, PricingSettings } from './_sections/OperationsOverview'
import { QuarantineSection } from '@/app/control-room/_sections/ControlRoomClient'

export const dynamic = 'force-dynamic'

export default function AdminPage() {
  return (
    <div className="space-y-6" data-testid="admin-overview-root">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        <AdminPrefetchLinks />
      </PageHeader>

      <div
        data-testid="admin-operations-reservation"
        className="min-h-[108px] [contain-intrinsic-size:auto_108px]"
      >
        <OperationsOverview />
      </div>

      <div
        data-testid="admin-services-reservation"
        className="min-h-[296px] [contain-intrinsic-size:auto_296px]"
      >
        <ServicesTable />
      </div>

      <div
        data-testid="admin-health-reservation"
        className="min-h-[280px] [contain-intrinsic-size:auto_280px]"
      >
        <SystemHealthCard />
      </div>

      <div
        data-testid="admin-quarantine-reservation"
        className="min-h-[120px] [contain-intrinsic-size:auto_120px]"
      >
        <QuarantineSection />
      </div>

      <div
        data-testid="admin-global-settings-reservation"
        className="min-h-[240px] [contain-intrinsic-size:auto_240px]"
      >
        <GlobalSettings />
      </div>

      <div
        data-testid="admin-pricing-reservation"
        className="min-h-[220px] [contain-intrinsic-size:auto_220px]"
      >
        <PricingSettings />
      </div>
    </div>
  )
}
