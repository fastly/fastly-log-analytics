import React from 'react'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { BackToAdminLink } from '@/components/ReportLayout'
import { QueueClient } from './_sections/QueueClient'

export default function AdminQueuePage() {
  return (
    <div className="space-y-6" data-testid="admin-queue-root">
      <PageHeader
        title="Queue & Workers"
        description="Monitor background job schedules, active workers, queue depths, and dead-letter / quarantine state."
      >
        <BackToAdminLink />
      </PageHeader>
      <QueueClient />
    </div>
  )
}
