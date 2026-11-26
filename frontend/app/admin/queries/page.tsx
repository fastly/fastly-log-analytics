import { Suspense } from 'react'

import QueriesLoading from './loading'
import QueryMonitorClient from './_sections/QueryMonitorClient'

export {
  QueryMonitorClient,
  QueryMonitorPage,
} from './_sections/QueryMonitorClient'
export * from './_sections/useQueryMonitorState'
export * from './_sections/QueryMonitorComponents'
export * from './_sections/QueryMonitorSections'

export default function AdminQueriesPage() {
  return (
    <Suspense fallback={<QueriesLoading />}>
      <QueryMonitorClient />
    </Suspense>
  )
}
