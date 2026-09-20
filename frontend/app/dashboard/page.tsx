import { Suspense } from 'react'

import DashboardClient from './_sections/DashboardClient'
import DashboardLoading from './loading'

export default function DashboardPage() {
  return (
    <Suspense fallback={<DashboardLoading />}>
      <DashboardClient nowServerStr={new Date().toISOString()} />
    </Suspense>
  )
}
