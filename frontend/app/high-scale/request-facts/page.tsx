'use client'

import { Database } from 'lucide-react'
import { PageHeader } from '@/components/ui/page-header'
import { HighScaleRequestFactsPanel } from '@/components/high-scale/HighScaleRequestFactsPanel'

export default function HighScaleRequestFactsPage() {
  return (
    <main className="container mx-auto max-w-7xl px-4 py-6">
      <PageHeader
        title="High-scale request facts"
        description="Explicit opt-in access to the isolated high-scale request-facts API."
        icon={Database}
      />
      <HighScaleRequestFactsPanel />
    </main>
  )
}
