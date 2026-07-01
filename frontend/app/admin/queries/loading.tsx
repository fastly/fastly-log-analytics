import { PageHeader } from '@/components/ui/page-header'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

// Stable string keys for placeholder cards/rows (array-index keys trip
// react/no-array-index-key; these never reorder).
const CARD_KEYS = ['active', 'slow'] as const
const ROW_KEYS = Array.from({ length: 5 }, (_, i) => `queries-skel-row-${i}`)

/**
 * Above-the-fold skeleton matching page.tsx (Live Query Monitor) so the
 * skeleton→real swap doesn't reflow. The prior <GenericPageSkeleton/> (one
 * short block) reserved none of the titled header, summary strip, tabs
 * strip, or stacked table cards that pop in — CLS 0.400. The real page's
 * outer container is ``space-y-6``; match it.
 */
export default function Loading() {
  return (
    <div className="space-y-6">
      {/* Titled PageHeader with the BackToAdmin action placeholder. */}
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and SQLite query. Click a row to see the full SQL."
      >
        <Skeleton className="h-9 w-28" />
      </PageHeader>

      {/* SummaryStrip row (badge cluster left, keyboard-help button right). */}
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <Skeleton className="h-6 w-20 rounded-full" />
          <Skeleton className="h-6 w-16 rounded-full" />
        </div>
        <Skeleton className="h-8 w-8" />
      </div>

      {/* Tabs strip (All / Live only / Past only). */}
      <Skeleton className="h-9 w-64" />

      {/* First two stacked table Cards (Active & Just-Finished, then a
          second table). */}
      {CARD_KEYS.map((cardKey) => (
        <Card key={cardKey}>
          <CardHeader className="pb-3">
            <Skeleton className="h-5 w-56" />
          </CardHeader>
          <CardContent className="p-0">
            <Skeleton className="h-10 w-full rounded-none" />
            <div className="divide-y">
              {ROW_KEYS.map((r) => (
                <Skeleton key={`${cardKey}-${r}`} className="h-11 w-full rounded-none" />
              ))}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
