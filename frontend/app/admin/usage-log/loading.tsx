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
    <div data-testid="admin-usage-log-loading" className="space-y-6">
      {/* Titled PageHeader with the 4 action-button placeholders. */}
      <PageHeader
        title="FOS Usage Log"
        description="Fastly Object Storage and CDN operations captured for cost analysis."
      >
        <div
          data-testid="admin-usage-log-toolbar"
          className="flex flex-wrap items-center gap-2 min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]"
        >
          <Skeleton className="h-8 w-20" />
          <Skeleton className="h-8 w-24" />
          <Skeleton className="h-8 w-28" />
          <Skeleton className="h-8 w-28" />
        </div>
      </PageHeader>

      {/* Dashed pricing banner stripe. */}
      <Skeleton className="h-11 w-full rounded-lg" />

      {/* 4-up aggregate StatCard grid (~h-[164px] cards). */}
      <div
        data-testid="admin-usage-log-kpi-reservation"
        className="grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[164px] [contain-intrinsic-size:auto_164px] [content-visibility:auto]"
      >
        <Skeleton className="h-[164px] w-full" />
        <Skeleton className="h-[164px] w-full" />
        <Skeleton className="h-[164px] w-full" />
        <Skeleton className="h-[164px] w-full" />
      </div>

      {/* LogAccountingPanel (header + mini-stat grid + ~240px chart). */}
      <div
        data-testid="admin-usage-log-chart-reservation"
        className="min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
      >
        <Skeleton className="h-[380px] w-full rounded-lg" />
      </div>

      {/* Filters + DataTable card. */}
      <div
        data-testid="admin-usage-log-table-reservation"
        className="rounded-lg border bg-card overflow-hidden min-h-[520px] [contain-intrinsic-size:auto_520px] [content-visibility:auto]"
      >
        <div className="px-4 py-3 border-b flex items-center justify-between">
          <span className="text-xs text-muted-foreground italic">Loading FOS usage ledger...</span>
          <Skeleton className="h-7 w-48" />
        </div>
        <div className="divide-y">
          {TABLE_ROW_KEYS.map((k) => (
            <Skeleton key={k} className="h-12 w-full rounded-none" />
          ))}
        </div>
      </div>
    </div>
  )
}
