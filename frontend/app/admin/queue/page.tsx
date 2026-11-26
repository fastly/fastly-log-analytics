import { Suspense } from 'react'

import Loading from './loading'
import AdminQueueClient from './_sections/AdminQueueClient'

export default function AdminQueuePage() {
  return (
    <Suspense fallback={<Loading />}>
      <AdminQueueClient />
    </Suspense>
  )
}
