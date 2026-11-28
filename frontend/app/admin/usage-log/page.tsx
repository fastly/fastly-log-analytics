import { Suspense } from 'react'

import Loading from './loading'
import { UsageLogClient } from './_sections/UsageLogClient'

export {
  UsageLogClient,
  usageLogUrlParsers,
  DATE_PRESETS,
  fmtCost,
  buildUsageLogColumns,
  UsageLogFilters,
  type UsageLogEntry,
} from './_sections/UsageLogClient'
export { LogAccountingPanel } from './_sections/LogAccountingPanel'

export default function UsageLogPage() {
  return (
    <Suspense fallback={<Loading />}>
      <UsageLogClient />
    </Suspense>
  )
}
