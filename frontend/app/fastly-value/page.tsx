import { Suspense } from 'react'

import FastlyValueLoading from './loading'
import FastlyValueClient from './_sections/FastlyValueClient'

export { FastlyValueClient, OffloadAndCacheCharts, IOValueTab } from './_sections/FastlyValueClient'

export default function FastlyValuePage() {
  return (
    <Suspense fallback={<FastlyValueLoading />}>
      <FastlyValueClient />
    </Suspense>
  )
}
