import { Suspense } from 'react'

import Loading from './loading'
import SessionScoringClient from './_sections/SessionScoringClient'

export {
  SinceHoursPicker,
  SessionScoringClient,
  AdminSessionScoringPage,
} from './_sections/SessionScoringClient'

export default function SessionScoringPage() {
  return (
    <Suspense fallback={<Loading />}>
      <SessionScoringClient />
    </Suspense>
  )
}
