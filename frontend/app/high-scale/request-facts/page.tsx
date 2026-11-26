import { Suspense } from 'react'

import Loading from './loading'
import HighScaleRequestFactsClient from './_sections/HighScaleRequestFactsClient'

export {
  useHighScaleRequestFacts,
  highScaleRequestFactsHooks,
  HighScaleRequestFactsPanel,
  HighScaleRequestFactsClient,
} from './_sections/HighScaleRequestFactsClient'

export default function HighScaleRequestFactsPage() {
  return (
    <Suspense fallback={<Loading />}>
      <HighScaleRequestFactsClient />
    </Suspense>
  )
}
