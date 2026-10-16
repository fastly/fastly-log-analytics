'use client'

import { Database } from 'lucide-react'
import { PageHeader } from '@/components/ui/page-header'
import { HighScaleRequestFactsPanel } from '@/components/high-scale/HighScaleRequestFactsPanel'

export default function HighScaleRequestFactsPage() {
  return (
    <main className="container mx-auto max-w-7xl px-4 py-6 space-y-6">
      <div className="min-h-[60px] [contain-intrinsic-size:auto_60px] [content-visibility:auto]">
        <PageHeader
          title="High-scale request facts"
          description="Explicit opt-in access to the isolated high-scale request-facts API."
          icon={Database}
        />
      </div>
      <div className="min-h-[600px] [contain-intrinsic-size:auto_600px] [content-visibility:auto]">
        <HighScaleRequestFactsPanel />
      </div>
    </main>
  )
}
