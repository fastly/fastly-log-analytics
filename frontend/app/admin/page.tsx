import React, { Suspense } from 'react'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { Skeleton } from '@/components/ui/skeleton'

import { AdminPrefetchLinks, OperationsOverview } from './_sections/AdminOverviewCards'
import { ServicesTable } from './_sections/AdminServicesTable'
import {
  DeferredGlobalSettings,
  DeferredPricingSettings,
  DeferredQuarantineSection,
  DeferredSystemHealthCard,
} from './_sections/AdminDeferredSections'

export const dynamic = 'force-dynamic'

export default function AdminPage() {
  return (
    <div className="space-y-6" data-testid="admin-overview-root">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        <Suspense fallback={null}>
          <AdminPrefetchLinks />
        </Suspense>
      </PageHeader>

      <div
        data-testid="admin-operations-reservation"
        className="min-h-[108px] [contain-intrinsic-size:auto_108px]"
      >
        <Suspense fallback={<Skeleton className="h-[108px] w-full rounded-xl" />}>
          <OperationsOverview />
        </Suspense>
      </div>

      <div
        data-testid="admin-services-reservation"
        className="min-h-[296px] [contain-intrinsic-size:auto_296px]"
      >
        <Suspense fallback={<Skeleton className="h-[296px] w-full rounded-lg" />}>
          <ServicesTable />
        </Suspense>
      </div>

      <div
        data-testid="admin-health-reservation"
        className="min-h-[280px] [contain-intrinsic-size:auto_280px]"
      >
        <Suspense fallback={<Skeleton className="h-[280px] w-full rounded-xl" />}>
          <DeferredSystemHealthCard />
        </Suspense>
      </div>

      <div
        data-testid="admin-quarantine-reservation"
        className="min-h-[120px] [contain-intrinsic-size:auto_120px]"
      >
        <Suspense fallback={null}>
          <DeferredQuarantineSection />
        </Suspense>
      </div>

      <div
        data-testid="admin-global-settings-reservation"
        className="min-h-[240px] [contain-intrinsic-size:auto_240px]"
      >
        <Suspense fallback={<Skeleton className="h-[240px] w-full rounded-xl" />}>
          <DeferredGlobalSettings />
        </Suspense>
      </div>

      <div
        data-testid="admin-pricing-reservation"
        className="min-h-[220px] [contain-intrinsic-size:auto_220px]"
      >
        <Suspense fallback={<Skeleton className="h-[220px] w-full rounded-xl" />}>
          <DeferredPricingSettings />
        </Suspense>
      </div>
    </div>
  )
}
