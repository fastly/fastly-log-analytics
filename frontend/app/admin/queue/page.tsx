import { Suspense } from 'react'

import Loading from './loading'
import AdminQueueClient from './_sections/AdminQueueClient'

export * from './_sections/QueueOverviewPanels'
export * from './_sections/QueueLedgerInspector'
export { QueueClient, AdminQueueClient } from './_sections/AdminQueueClient'

export default function AdminQueuePage() {
  return (
    <Suspense fallback={<Loading />}>
      <AdminQueueClient />
    </Suspense>
  )
}
