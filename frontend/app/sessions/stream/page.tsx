'use client'

import { Suspense } from 'react'
import { StreamDetailClient } from './_sections/StreamDetailClient'

export default function StreamDetailPage() {
  return (
    <Suspense fallback={null}>
      <StreamDetailClient />
    </Suspense>
  )
}
