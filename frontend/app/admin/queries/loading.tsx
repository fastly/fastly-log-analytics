import { PageHeader } from '@/components/ui/confirm-dialog'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

// Stable string keys for placeholder cards (array-index keys trip
// react/no-array-index-key; these never reorder).
const CARD_KEYS = ['active', 'slow', 'completed'] as const

/**
 * Above-the-fold skeleton matching page.tsx (Live Query Monitor) so the
 * skeleton→real swap doesn't reflow. Matches the exact geometry of
 * PageHeader + SummaryStrip (36px) + TabsList (36px) + stacked DataTable
 * cards (CardHeader + 40px thead + 96px tbody = ~200px per card).
 */
export default function Loading() {
  return (
    <div className="space-y-6" data-testid="admin-queries-loading">
      {/* Titled PageHeader with the BackToAdmin action placeholder. */}
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and Postgres query. Click a row to see the full SQL."
      >
        <Skeleton className="h-7 w-28" />
      </PageHeader>

      {/* SummaryStrip row (badge cluster left, keyboard-help button right). */}
      <div
        data-testid="admin-queries-summary-skeleton"
        className="flex items-center justify-between gap-3 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
      >
        <div className="flex items-center gap-3">
          <Skeleton className="h-5 w-20 rounded-full" />
          <Skeleton className="h-5 w-16 rounded-full" />
          <span className="text-xs text-muted-foreground">
            Inspecting DuckDB connection pool...
          </span>
        </div>
        <Skeleton className="h-8 w-8" />
      </div>

      {/* Tabs strip (All / Live only / Past only). */}
      <Skeleton className="h-9 w-64" />

      {/* Stacked table Cards matching Active (480px above-the-fold), Notable Slow, and Recently Completed geometry. */}
      {CARD_KEYS.map((cardKey) => {
        const isActiveCard = cardKey === 'active'
        return (
          <div
            key={cardKey}
            className={
              isActiveCard
                ? 'min-h-[480px] [contain-intrinsic-size:auto_480px] [content-visibility:auto]'
                : 'min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]'
            }
          >
            <Card className={isActiveCard ? 'min-h-[480px]' : 'min-h-[200px]'}>
              <CardHeader className="pb-3 flex flex-row items-center justify-between space-y-0">
                <Skeleton className="h-6 w-56" />
                <span className="text-xs text-muted-foreground">
                  {isActiveCard
                    ? 'Inspecting DuckDB connection pool...'
                    : 'Loading query telemetry...'}
                </span>
              </CardHeader>
              <CardContent className="p-0">
                <Skeleton className="h-10 w-full rounded-none" />
                <Skeleton className={`${isActiveCard ? 'h-[400px]' : 'h-24'} w-full rounded-none`} />
              </CardContent>
            </Card>
          </div>
        )
      })}
    </div>
  )
}
