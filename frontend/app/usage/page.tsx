import { Suspense } from 'react'

import Loading from './loading'
import UsageClient from './_sections/UsageClient'

export {
  UsageClient,
  UsagePage,
  UsagePageContent,
  BreakdownTable,
  UsageStatCards,
  type BreakdownRow,
  type StorageUsageData,
  type OperationsUsageData,
  type BandwidthUsageData,
  type LogActivityData,
  type RumBreakdownData,
} from './_sections/UsageClient'

export default function UsagePageShell() {
  return (
    <Suspense fallback={<Loading />}>
      <UsageClient />
    </Suspense>
  )
}
