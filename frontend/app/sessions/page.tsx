import { Suspense } from 'react'

import Loading from './loading'
import SessionsClient from './_sections/SessionsClient'

export {
  SessionsClient,
  SessionsTable,
  ScoringControls,
  type SessionsTableProps,
  type ScoringControlsProps,
} from './_sections/SessionsClient'

export default function SessionsPage() {
  return (
    <Suspense fallback={<Loading />}>
      <SessionsClient />
    </Suspense>
  )
}
