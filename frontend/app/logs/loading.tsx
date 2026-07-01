import { PageHeader } from '@/components/ui/page-header'
import { Skeleton } from '@/components/ui/skeleton'

// Stable string keys for placeholder rows (array-index keys trip
// react/no-array-index-key; these never reorder).
const TABLE_ROW_KEYS = Array.from({ length: 10 }, (_, i) => `logs-skel-row-${i}`)

/**
 * Above-the-fold skeleton matching _sections/LogsClient.tsx ("Data
 * Management") so the skeleton→real swap doesn't reflow. The prior bare
 * <TableSkeleton/> reserved none of the titled header, QuickActionsBar
 * band, or tabs strip that pop in above the cron table — CLS 0.378. The
 * real page's outer container is ``space-y-6``; match it.
 */
export default function Loading() {
  return (
    <div className="space-y-6">
      {/* Titled PageHeader ("Data Management"). */}
      <PageHeader
        title="Data Management"
        description="Monitor and manage log ingestion history and active data syncs."
      />

      {/* QuickActionsBar band (bg-muted/30 p-2 rounded-lg border, h-8 buttons). */}
      <Skeleton className="h-12 w-full rounded-lg" />

      {/* Tabs strip (full-width TabsList). */}
      <Skeleton className="h-10 w-full rounded-md" />

      {/* Cron table card. */}
      <div className="border rounded-md overflow-hidden">
        <Skeleton className="h-10 w-full rounded-none" />
        <div className="divide-y">
          {TABLE_ROW_KEYS.map((k) => (
            <Skeleton key={k} className="h-12 w-full rounded-none" />
          ))}
        </div>
      </div>
    </div>
  )
}
