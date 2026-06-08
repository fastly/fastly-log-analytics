/**
 * Route-level loading skeletons.
 *
 * Each route's ``loading.tsx`` imports one of the variants below so
 * clicks render instantly while the real page hydrates + fetches. The
 * Next.js App Router serves loading.tsx the moment a navigation
 * starts — BEFORE the destination page's JS bundle is even on the
 * wire — so a thin skeleton here translates directly into a sub-50ms
 * paint after every sidebar click.
 *
 * Shape conventions:
 *   - Top: ~56px "header row" placeholder (matches AppLayout header).
 *   - Then: a layout-shaped block of ``animate-pulse`` divs sized to
 *     the page's real content so the skeleton-to-real swap doesn't
 *     thrash layout.
 *
 * Add a new variant when an existing one doesn't match the page's
 * dominant layout; keep variant count small (a couple of close-enough
 * skeletons is better than one perfectly-pixel-matched skeleton per
 * page, since the skeleton only shows for ~200ms in practice).
 */

import { Skeleton } from '@/components/ui/skeleton'

/**
 * 4 stat cards over a wide chart + a table. Matches /dashboard,
 * /performance, /security, /origin layout shape.
 */
export function DashboardSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-48" />
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
        <Skeleton className="h-20 w-full" />
      </div>
      <Skeleton className="h-72 w-full" />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <Skeleton className="h-56 w-full" />
        <Skeleton className="h-56 w-full" />
      </div>
    </div>
  )
}

/** Grid of equally-sized chart panels — matches /charts. */
export function ChartsGridSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-32" />
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        {Array.from({ length: 9 }).map((_, i) => (
          <Skeleton key={i} className="h-64 w-full" />
        ))}
      </div>
    </div>
  )
}

/** Tall table with header row + N body rows — matches /logs, /sessions, /alerts. */
export function TableSkeleton({ rows = 12 }: { rows?: number }) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Skeleton className="h-7 w-48" />
        <Skeleton className="h-9 w-32" />
      </div>
      <div className="border rounded-md overflow-hidden">
        <Skeleton className="h-10 w-full rounded-none" />
        <div className="divide-y">
          {Array.from({ length: rows }).map((_, i) => (
            <Skeleton key={i} className="h-12 w-full rounded-none" />
          ))}
        </div>
      </div>
    </div>
  )
}

/** Two-column form / dialog-heavy layout — matches /admin pages. */
export function FormSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-56" />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="space-y-3">
          <Skeleton className="h-32 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
        <div className="space-y-3">
          <Skeleton className="h-32 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      </div>
    </div>
  )
}

/** Two-pane (editor + results) — matches /query. */
export function EditorSplitSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-40" />
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_2fr] gap-3 min-h-[60vh]">
        <Skeleton className="w-full h-full min-h-72" />
        <Skeleton className="w-full h-full min-h-72" />
      </div>
    </div>
  )
}

/** Generic single-column page — fallback for routes without a more
 *  specific layout. */
export function GenericPageSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-7 w-48" />
      <Skeleton className="h-32 w-full" />
      <Skeleton className="h-64 w-full" />
    </div>
  )
}
