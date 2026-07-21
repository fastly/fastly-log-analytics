import React from 'react'
import { SystemHealthCard } from "@/components/SystemHealthCard"
import { PageHeader } from '@/components/ui/page-header'

import { ServicesTable } from './_sections/ServicesTable'
import { GlobalSettings, PricingSettings } from './_sections/GlobalSettings'
import { OperationsOverview } from './_sections/OperationsOverview'
import { QuarantineSection } from './_sections/QuarantineSection'
import { AdminPrefetchLinks } from './AdminPrefetchLinks'

export default function AdminPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        <AdminPrefetchLinks />
      </PageHeader>

      <OperationsOverview />

      <ServicesTable />

      <SystemHealthCard />

      <QuarantineSection />

      <GlobalSettings />

      <PricingSettings />
    </div>
  )
}
