import { PageHeader } from '@/components/ui/page-header'
import { Skeleton } from '@/components/ui/skeleton'

// Stable string keys for the placeholder rows (array-index keys trip
// react/no-array-index-key; these never reorder).
const TABLE_ROW_KEYS = Array.from({ length: 10 }, (_, i) => `usage-log-skel-row-${i}`)

/**
 * Above-the-fold skeleton matching _sections/UsageLogClient.tsx so the
 * skeleton→real swap doesn't reflow (the bare <TableSkeleton/> it replaced
 * scored CLS 0.457 — the worst route — because it reserved none of the
 * header / banner / stat-grid / accounting-panel that pop in above the
 * table). The real page's outer container is ``space-y-6``; match it.
 */
export default function Loading() {
  return (
    <div className="space-y-6">
      {/* Titled PageHeader with the 4 action-button placeholders. */}
      <PageHeader
        title="FOS Usage Log"
        description="Fastly Object Storage and CDN operations captured for cost analysis."
      >
        <Skeleton className="h-8 w-20" />
        <Skeleton className="h-8 w-24" />
        <Skeleton className="h-8 w-28" />
        <Skeleton className="h-8 w-28" />
      </PageHeader>

      {/* Dashed pricing banner stripe. */}
      <Skeleton className="h-11 w-full rounded-lg" />

      {/* 4-up aggregate StatCard grid (~h-20 cards). */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
      </div>

      {/* LogAccountingPanel (header + mini-stat grid + ~240px chart). */}
      <Skeleton className="h-72 w-full rounded-lg" />

      {/* Filters + DataTable card. */}
      <div className="rounded-lg border bg-card overflow-hidden">
        <Skeleton className="h-12 w-full rounded-none" />
        <div className="divide-y">
          {TABLE_ROW_KEYS.map((k) => (
            <Skeleton key={k} className="h-12 w-full rounded-none" />
          ))}
        </div>
      </div>
    </div>
  )
}
