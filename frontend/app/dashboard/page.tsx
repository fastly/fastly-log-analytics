import { Suspense } from 'react'

import DashboardPageClient from './_sections/DashboardPageClient'
import DashboardLoading from './loading'

export const dynamic = 'force-dynamic'

export default function DashboardPage() {
  return (
    <Suspense fallback={<DashboardLoading />}>
      <DashboardPageClient nowServerStr={new Date().toISOString()} />
    </Suspense>
  )
}
