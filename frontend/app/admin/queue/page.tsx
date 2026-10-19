import React from 'react'
import { PageHeader } from '@/components/ui/page-header'
import { BackToAdminLink } from '@/components/BackToAdminLink'
import { QueueClient } from './_sections/QueueClient'

export default function AdminQueuePage() {
  return (
    <div className="space-y-6" data-testid="admin-queue-root">
      <PageHeader
        title="Queue & Workers"
        description="Monitor Celery queue depths, active workers, and scheduled tasks."
      >
        <BackToAdminLink />
      </PageHeader>
      <QueueClient />
    </div>
  )
}
